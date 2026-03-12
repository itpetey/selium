//! Host-neutral helpers for the first-party control-plane system module.

use std::collections::{BTreeMap, BTreeSet};

use rkyv::{Archive, Deserialize, Serialize};
use selium_control_plane_api::{
    ContractKind, ControlPlaneState, DeploymentSpec, PublicEndpointRef,
};
use selium_control_plane_protocol::{
    EndpointBridgeSemantics, EventBridgeSemantics, EventDeliveryMode, ManagedEndpointBinding,
    ManagedEndpointBindingType, ManagedEndpointRole, ServiceBridgeSemantics,
    ServiceCorrelationMode, StreamBridgeSemantics, StreamLifecycleMode,
};
use selium_control_plane_scheduler::{
    SchedulePlan, ScheduledEndpointBridgeIntent, ScheduledInstance, build_endpoint_bridge_intents,
};

/// Well-known module identifier used by the host when wiring system workloads.
pub const MODULE_ID: &str = "system/control-plane.wasm";
/// Default entrypoint function expected by the runtime.
pub const ENTRYPOINT: &str = "start";
/// Runtime-managed binding granted to the system control-plane service.
pub const INTERNAL_BINDING_NAME: &str = "system-control-plane-internal";
/// Runtime-managed egress profile used for daemon and peer RPCs.
pub const PEER_PROFILE_NAME: &str = "system-control-plane-peers";
/// Runtime-managed durable log used for committed event replay.
pub const EVENT_LOG_NAME: &str = "system-control-plane-events";
/// Runtime-managed blob store used for raft and engine snapshots.
pub const SNAPSHOT_BLOB_STORE_NAME: &str = "system-control-plane-snapshots";

const DEFAULT_CAPABILITIES: &[&str] = &[
    "session_lifecycle",
    "process_lifecycle",
    "time_read",
    "shared_memory",
    "queue_lifecycle",
    "queue_writer",
    "queue_reader",
];

/// Minimal bootstrap output for host-side diagnostics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapReport {
    /// Module identifier registered by the host.
    pub module_id: &'static str,
    /// Entrypoint expected to exist in the guest module.
    pub entrypoint: &'static str,
    /// Runtime-managed capabilities granted to the control-plane module.
    pub managed_capabilities: &'static [&'static str],
}

/// Agent-side state used to reconcile desired instances and bridge routes.
#[derive(Debug, Clone, PartialEq, Eq, Default, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct AgentState {
    /// Running instances keyed by instance id.
    pub running_instances: BTreeMap<String, String>,
    /// Active endpoint bridge identifiers.
    pub active_bridges: BTreeSet<String>,
}

/// Agent actions emitted by reconciliation.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum ReconcileAction {
    /// Start an instance on the local node.
    Start {
        /// Instance identifier.
        instance_id: String,
        /// Deployment key.
        deployment: String,
        /// Managed endpoint bindings the runtime should inject.
        managed_endpoint_bindings: Vec<ManagedEndpointBinding>,
    },
    /// Stop an instance on the local node.
    Stop {
        /// Instance identifier.
        instance_id: String,
    },
    /// Ensure an endpoint bridge exists.
    EnsureEndpointBridge(Box<EnsureEndpointBridgeAction>),
    /// Remove an endpoint bridge.
    RemoveEndpointBridge {
        /// Bridge identifier.
        bridge_id: String,
    },
}

/// Payload for bridge creation actions emitted during reconciliation.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct EnsureEndpointBridgeAction {
    /// Bridge identifier.
    pub bridge_id: String,
    /// Source instance id.
    pub source_instance_id: String,
    /// Source public endpoint reference.
    pub source_endpoint: PublicEndpointRef,
    /// Target instance id.
    pub target_instance_id: String,
    /// Target node id.
    pub target_node: String,
    /// Target public endpoint reference.
    pub target_endpoint: PublicEndpointRef,
}

/// Remote peer metadata required to connect to another control-plane daemon.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct PeerTarget {
    /// Peer node id.
    pub node_id: String,
    /// Public daemon address.
    pub daemon_addr: String,
    /// Expected TLS server name.
    pub daemon_server_name: String,
}

/// Runtime configuration supplied to the first-party control-plane guest module.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ControlPlaneModuleConfig {
    /// Local node id.
    pub node_id: String,
    /// Public daemon address for this node.
    pub public_daemon_addr: String,
    /// Public daemon TLS server name for this node.
    pub public_daemon_server_name: String,
    /// Schedulable slot capacity on this node.
    pub capacity_slots: u32,
    /// Optional allocatable CPU budget in millis.
    pub allocatable_cpu_millis: Option<u32>,
    /// Optional allocatable memory budget in MiB.
    pub allocatable_memory_mib: Option<u32>,
    /// Heartbeat interval used when publishing node liveness.
    pub heartbeat_interval_ms: u64,
    /// Whether this node should bootstrap as leader when the cluster is empty.
    pub bootstrap_leader: bool,
    /// Remote peers in the control-plane cluster.
    pub peers: Vec<PeerTarget>,
}

/// Build the runtime module spec string for a deployment.
pub fn deployment_module_spec(deployment: &DeploymentSpec) -> String {
    let (adaptor, profile) = match deployment.isolation {
        selium_control_plane_api::IsolationProfile::Standard => ("wasmtime", "standard"),
        selium_control_plane_api::IsolationProfile::Hardened => ("wasmtime", "hardened"),
        selium_control_plane_api::IsolationProfile::Microvm => ("microvm", "microvm"),
    };

    build_module_spec(&deployment.module, adaptor, profile, default_capabilities())
}

/// Build a compact Selium module spec string.
pub fn build_module_spec(
    module: &str,
    adaptor: &str,
    profile: &str,
    capabilities: Vec<String>,
) -> String {
    format!(
        "path={};capabilities={};adaptor={};profile={}",
        module,
        capabilities.join(","),
        adaptor,
        profile
    )
}

/// Return the runtime-managed capabilities granted to the control-plane module.
pub fn default_capabilities() -> Vec<String> {
    DEFAULT_CAPABILITIES
        .iter()
        .map(|capability| (*capability).to_string())
        .collect()
}

/// Derive the reconciled actions for a single node.
pub fn reconcile(
    node_name: &str,
    state: &ControlPlaneState,
    desired: &SchedulePlan,
    current: &AgentState,
) -> Vec<ReconcileAction> {
    let desired_on_node = desired
        .instances
        .iter()
        .filter(|instance| instance.node == node_name)
        .collect::<Vec<&ScheduledInstance>>();

    let desired_ids = desired_on_node
        .iter()
        .map(|instance| instance.instance_id.clone())
        .collect::<BTreeSet<_>>();

    let mut actions = Vec::new();
    let all_bridges = build_endpoint_bridge_intents(state, desired);

    for instance in desired_on_node {
        if !current
            .running_instances
            .contains_key(&instance.instance_id)
        {
            actions.push(ReconcileAction::Start {
                instance_id: instance.instance_id.clone(),
                deployment: instance.deployment.clone(),
                managed_endpoint_bindings: managed_endpoint_bindings_for_instance(
                    &instance.instance_id,
                    &all_bridges,
                ),
            });
        }
    }

    for instance_id in current.running_instances.keys() {
        if !desired_ids.contains(instance_id) {
            actions.push(ReconcileAction::Stop {
                instance_id: instance_id.clone(),
            });
        }
    }

    let desired_bridges = all_bridges
        .into_iter()
        .filter(|bridge| bridge.source_node == node_name)
        .collect::<Vec<_>>();
    let desired_bridge_ids = desired_bridges
        .iter()
        .map(|bridge| bridge.bridge_id.clone())
        .collect::<BTreeSet<_>>();

    for bridge in desired_bridges {
        if !current.active_bridges.contains(&bridge.bridge_id) {
            actions.push(ReconcileAction::EnsureEndpointBridge(Box::new(
                EnsureEndpointBridgeAction {
                    bridge_id: bridge.bridge_id,
                    source_instance_id: bridge.source_instance_id,
                    source_endpoint: bridge.source_endpoint,
                    target_instance_id: bridge.target_instance_id,
                    target_node: bridge.target_node,
                    target_endpoint: bridge.target_endpoint,
                },
            )));
        }
    }

    for bridge_id in &current.active_bridges {
        if !desired_bridge_ids.contains(bridge_id) {
            actions.push(ReconcileAction::RemoveEndpointBridge {
                bridge_id: bridge_id.clone(),
            });
        }
    }

    actions.sort_by(|lhs, rhs| {
        action_order(lhs)
            .cmp(&action_order(rhs))
            .then_with(|| format!("{lhs:?}").cmp(&format!("{rhs:?}")))
    });
    actions
}

/// Apply reconciled actions to agent state.
pub fn apply(current: &mut AgentState, actions: &[ReconcileAction]) {
    for action in actions {
        match action {
            ReconcileAction::Start {
                instance_id,
                deployment,
                managed_endpoint_bindings: _,
            } => {
                current
                    .running_instances
                    .insert(instance_id.clone(), deployment.clone());
            }
            ReconcileAction::Stop { instance_id } => {
                current.running_instances.remove(instance_id);
            }
            ReconcileAction::EnsureEndpointBridge(action) => {
                current.active_bridges.insert(action.bridge_id.clone());
            }
            ReconcileAction::RemoveEndpointBridge { bridge_id } => {
                current.active_bridges.remove(bridge_id);
            }
        }
    }
}

/// Return the managed bridge semantics for a contract kind.
pub fn endpoint_bridge_semantics(kind: ContractKind) -> EndpointBridgeSemantics {
    match kind {
        ContractKind::Event => EndpointBridgeSemantics::Event(EventBridgeSemantics {
            delivery: EventDeliveryMode::Frame,
        }),
        ContractKind::Service => EndpointBridgeSemantics::Service(ServiceBridgeSemantics {
            correlation: ServiceCorrelationMode::RequestId,
        }),
        ContractKind::Stream => EndpointBridgeSemantics::Stream(StreamBridgeSemantics {
            lifecycle: StreamLifecycleMode::SessionFrames,
        }),
    }
}

fn action_order(action: &ReconcileAction) -> u8 {
    match action {
        ReconcileAction::RemoveEndpointBridge { .. } => 0,
        ReconcileAction::Stop { .. } => 1,
        ReconcileAction::Start { .. } => 2,
        ReconcileAction::EnsureEndpointBridge(_) => 3,
    }
}

/// Build the managed endpoint bindings required for a reconciled instance.
pub fn managed_endpoint_bindings_for_instance(
    instance_id: &str,
    bridges: &[ScheduledEndpointBridgeIntent],
) -> Vec<ManagedEndpointBinding> {
    let mut bindings = BTreeMap::<(u8, ContractKind, String), ManagedEndpointBinding>::new();
    for bridge in bridges {
        if bridge.source_instance_id == instance_id {
            bindings
                .entry((
                    0,
                    bridge.source_endpoint.kind,
                    bridge.source_endpoint.name.clone(),
                ))
                .or_insert_with(|| ManagedEndpointBinding {
                    endpoint_name: bridge.source_endpoint.name.clone(),
                    endpoint_kind: bridge.source_endpoint.kind,
                    role: ManagedEndpointRole::Egress,
                    binding_type: binding_type_for_kind(bridge.source_endpoint.kind),
                });
        }
        if bridge.target_instance_id == instance_id {
            bindings
                .entry((
                    1,
                    bridge.target_endpoint.kind,
                    bridge.target_endpoint.name.clone(),
                ))
                .or_insert_with(|| ManagedEndpointBinding {
                    endpoint_name: bridge.target_endpoint.name.clone(),
                    endpoint_kind: bridge.target_endpoint.kind,
                    role: ManagedEndpointRole::Ingress,
                    binding_type: binding_type_for_kind(bridge.target_endpoint.kind),
                });
        }
    }
    bindings.into_values().collect()
}

fn binding_type_for_kind(kind: ContractKind) -> ManagedEndpointBindingType {
    match kind {
        ContractKind::Event => ManagedEndpointBindingType::OneWay,
        ContractKind::Service => ManagedEndpointBindingType::RequestResponse,
        ContractKind::Stream => ManagedEndpointBindingType::Session,
    }
}
