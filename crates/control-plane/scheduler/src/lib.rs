//! Placement and scheduling primitives for the control plane.

use std::collections::{BTreeMap, BTreeSet};

use rkyv::{Archive, Deserialize, Serialize};
use selium_control_plane_api::{
    ContractRef, ControlPlaneState, DeploymentSpec, EventEndpointRef, IsolationProfile, NodeSpec,
    PublicEndpointRef, WorkloadRef, collect_contracts_for_workload, ensure_pipeline_consistency,
};
use thiserror::Error;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
struct NodeUsage {
    slots: u32,
    cpu_millis: u32,
    memory_mib: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ScheduledInstance {
    pub deployment: String,
    pub instance_id: String,
    pub node: String,
    pub isolation: IsolationProfile,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct SchedulePlan {
    pub instances: Vec<ScheduledInstance>,
    pub node_slots: BTreeMap<String, u32>,
    pub node_cpu_millis: BTreeMap<String, u32>,
    pub node_memory_mib: BTreeMap<String, u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ScheduledEndpointBridgeIntent {
    pub bridge_id: String,
    pub source_instance_id: String,
    pub source_node: String,
    pub source_endpoint: PublicEndpointRef,
    pub target_instance_id: String,
    pub target_node: String,
    pub target_endpoint: PublicEndpointRef,
    pub contract: ContractRef,
}

#[derive(Debug, PartialEq, Eq, Error)]
pub enum ScheduleError {
    #[error("pipeline is inconsistent: {0}")]
    InconsistentPipeline(String),
    #[error("no nodes are registered in control-plane state")]
    NoNodes,
    #[error("deployment `{0}` has no eligible nodes for its isolation profile")]
    NoEligibleNodes(String),
    #[error(
        "insufficient capacity to place deployment `{deployment}`: required={required}, available={available}"
    )]
    InsufficientCapacity {
        deployment: String,
        required: u32,
        available: u32,
    },
    #[error("insufficient allocatable resources to place deployment `{0}`")]
    InsufficientResources(String),
}

pub fn build_plan(state: &ControlPlaneState) -> Result<SchedulePlan, ScheduleError> {
    ensure_pipeline_consistency(state)
        .map_err(|err| ScheduleError::InconsistentPipeline(err.to_string()))?;

    let schedulable_nodes = schedulable_node_names(state);
    if schedulable_nodes.is_empty() {
        return Err(ScheduleError::NoNodes);
    }

    let mut node_order = schedulable_nodes.iter().cloned().collect::<Vec<_>>();
    node_order.sort_unstable();

    let mut usage: BTreeMap<String, NodeUsage> = node_order
        .iter()
        .map(|node| (node.clone(), NodeUsage::default()))
        .collect();
    let mut instances = Vec::new();

    for deployment in state.deployments.values() {
        let eligible = eligible_nodes(state, deployment.isolation.clone(), &schedulable_nodes);
        if eligible.is_empty() {
            return Err(ScheduleError::NoEligibleNodes(
                deployment.workload.to_string(),
            ));
        }
        for ordinal in 0..deployment.replicas {
            let Some(node) = select_node_for_replica(state, deployment, &eligible, &usage, ordinal)
            else {
                return Err(schedule_failure(state, deployment, &eligible, &usage));
            };

            instances.push(ScheduledInstance {
                deployment: deployment.workload.key(),
                instance_id: internal_instance_id(&deployment.workload, ordinal),
                node: node.clone(),
                isolation: deployment.isolation.clone(),
            });
            record_usage(
                usage.get_mut(&node).expect("tracked node usage"),
                deployment,
            );
        }
    }

    Ok(SchedulePlan {
        instances,
        node_slots: usage
            .iter()
            .map(|(node, used)| (node.clone(), used.slots))
            .collect(),
        node_cpu_millis: usage
            .iter()
            .map(|(node, used)| (node.clone(), used.cpu_millis))
            .collect(),
        node_memory_mib: usage
            .iter()
            .map(|(node, used)| (node.clone(), used.memory_mib))
            .collect(),
    })
}

fn select_node_for_replica(
    state: &ControlPlaneState,
    deployment: &DeploymentSpec,
    eligible: &[String],
    usage: &BTreeMap<String, NodeUsage>,
    ordinal: u32,
) -> Option<String> {
    for offset in 0..eligible.len() {
        let node_name = &eligible[(ordinal as usize + offset) % eligible.len()];
        let node = state.nodes.get(node_name)?;
        let used = usage.get(node_name).copied().unwrap_or_default();
        if node_can_fit(node, deployment, used) {
            return Some(node_name.clone());
        }
    }
    None
}

fn node_can_fit(node: &NodeSpec, deployment: &DeploymentSpec, used: NodeUsage) -> bool {
    let slots_fit = node.capacity_slots == 0 || used.slots < node.capacity_slots;
    let cpu_fit = node
        .allocatable_cpu_millis
        .is_none_or(|capacity| used.cpu_millis + deployment.cpu_millis <= capacity);
    let memory_fit = node
        .allocatable_memory_mib
        .is_none_or(|capacity| used.memory_mib + deployment.memory_mib <= capacity);
    slots_fit && cpu_fit && memory_fit
}

fn record_usage(usage: &mut NodeUsage, deployment: &DeploymentSpec) {
    usage.slots += 1;
    usage.cpu_millis += deployment.cpu_millis;
    usage.memory_mib += deployment.memory_mib;
}

fn schedule_failure(
    state: &ControlPlaneState,
    deployment: &DeploymentSpec,
    eligible: &[String],
    usage: &BTreeMap<String, NodeUsage>,
) -> ScheduleError {
    if deployment.cpu_millis > 0 || deployment.memory_mib > 0 {
        return ScheduleError::InsufficientResources(deployment.workload.to_string());
    }

    let available = eligible
        .iter()
        .map(|node| {
            let capacity = state
                .nodes
                .get(node)
                .map(|node| node.capacity_slots)
                .unwrap_or(0);
            let used = usage.get(node).copied().unwrap_or_default().slots;
            capacity.saturating_sub(used)
        })
        .sum();
    ScheduleError::InsufficientCapacity {
        deployment: deployment.workload.to_string(),
        required: deployment.replicas,
        available,
    }
}

fn schedulable_node_names(state: &ControlPlaneState) -> BTreeSet<String> {
    let live = state
        .nodes
        .values()
        .filter(|node| node.last_heartbeat_ms > 0)
        .map(|node| node.name.clone())
        .collect::<BTreeSet<_>>();
    if live.is_empty() {
        state.nodes.keys().cloned().collect()
    } else {
        live
    }
}

pub fn deployment_contract_usage(state: &ControlPlaneState) -> BTreeMap<String, BTreeSet<String>> {
    state
        .deployments
        .values()
        .map(|deployment| {
            let contracts = collect_contracts_for_workload(state, &deployment.workload)
                .into_iter()
                .map(|contract| {
                    format!(
                        "{}/{}:{}@{}",
                        contract.namespace,
                        contract.kind.as_str(),
                        contract.name,
                        contract.version
                    )
                })
                .collect::<BTreeSet<_>>();
            (deployment.workload.key(), contracts)
        })
        .collect()
}

pub fn build_endpoint_bridge_intents(
    state: &ControlPlaneState,
    plan: &SchedulePlan,
) -> Vec<ScheduledEndpointBridgeIntent> {
    let mut instances_by_workload: BTreeMap<&str, Vec<&ScheduledInstance>> = BTreeMap::new();
    for instance in &plan.instances {
        instances_by_workload
            .entry(instance.deployment.as_str())
            .or_default()
            .push(instance);
    }

    let mut intents = Vec::new();
    for pipeline in state.pipelines.values() {
        for edge in &pipeline.edges {
            let Some(source_instances) =
                instances_by_workload.get(edge.from.endpoint.workload.key().as_str())
            else {
                continue;
            };
            let Some(target_instances) =
                instances_by_workload.get(edge.to.endpoint.workload.key().as_str())
            else {
                continue;
            };
            for instance in source_instances {
                let Some(target) = select_target_instance(instance.node.as_str(), target_instances)
                else {
                    continue;
                };
                intents.push(ScheduledEndpointBridgeIntent {
                    bridge_id: endpoint_bridge_id(
                        &instance.instance_id,
                        &target.instance_id,
                        &public_endpoint_ref(&edge.from.endpoint, &edge.from.contract),
                        &public_endpoint_ref(&edge.to.endpoint, &edge.to.contract),
                    ),
                    source_instance_id: instance.instance_id.clone(),
                    source_node: instance.node.clone(),
                    source_endpoint: public_endpoint_ref(&edge.from.endpoint, &edge.from.contract),
                    target_instance_id: target.instance_id.clone(),
                    target_node: target.node.clone(),
                    target_endpoint: public_endpoint_ref(&edge.to.endpoint, &edge.to.contract),
                    contract: edge.to.contract.clone(),
                });
            }
        }
    }
    intents.sort_by(|lhs, rhs| lhs.bridge_id.cmp(&rhs.bridge_id));
    intents
}

fn internal_instance_id(workload: &WorkloadRef, ordinal: u32) -> String {
    format!(
        "tenant={};namespace={};workload={};replica={ordinal}",
        workload.tenant, workload.namespace, workload.name
    )
}

fn endpoint_bridge_id(
    source_instance_id: &str,
    target_instance_id: &str,
    source_endpoint: &PublicEndpointRef,
    target_endpoint: &PublicEndpointRef,
) -> String {
    format!(
        "{source_instance_id}->{target_instance_id}::{source}->{target}",
        source = source_endpoint.key(),
        target = target_endpoint.key()
    )
}

fn public_endpoint_ref(endpoint: &EventEndpointRef, contract: &ContractRef) -> PublicEndpointRef {
    PublicEndpointRef {
        workload: endpoint.workload.clone(),
        kind: contract.kind,
        name: endpoint.name.clone(),
    }
}

fn select_target_instance<'a>(
    source_node: &str,
    targets: &'a [&'a ScheduledInstance],
) -> Option<&'a ScheduledInstance> {
    targets
        .iter()
        .copied()
        .filter(|candidate| candidate.node == source_node)
        .min_by(|lhs, rhs| lhs.instance_id.cmp(&rhs.instance_id))
        .or_else(|| {
            targets.iter().copied().min_by(|lhs, rhs| {
                lhs.node
                    .cmp(&rhs.node)
                    .then(lhs.instance_id.cmp(&rhs.instance_id))
            })
        })
}

fn eligible_nodes(
    state: &ControlPlaneState,
    isolation: IsolationProfile,
    schedulable_nodes: &BTreeSet<String>,
) -> Vec<String> {
    let mut nodes = state
        .nodes
        .values()
        .filter(|node| {
            schedulable_nodes.contains(&node.name) && node.supported_isolation.contains(&isolation)
        })
        .map(|node| node.name.clone())
        .collect::<Vec<_>>();
    nodes.sort_unstable();
    nodes
}

#[cfg(test)]
mod tests {
    use selium_control_plane_api::{
        BandwidthProfile, ContractRef, DeploymentSpec, PipelineEdge, PipelineEndpoint,
        PipelineSpec, parse_idl,
    };

    use super::*;

    fn sample_state() -> ControlPlaneState {
        let mut state = ControlPlaneState::new_local_default();

        state
            .registry
            .register_package(
                parse_idl(
                    "package media.pipeline.v1;\n\
                     schema Frame { camera_id: string; ts_ms: u64; }\n\
                     event camera.frames(Frame) { replay: enabled; }",
                )
                .expect("parse"),
            )
            .expect("register");

        state
            .upsert_deployment(DeploymentSpec {
                workload: WorkloadRef {
                    tenant: "tenant-a".to_string(),
                    namespace: "media".to_string(),
                    name: "ingest".to_string(),
                },
                module: "ingest.wasm".to_string(),
                replicas: 2,
                contracts: vec![ContractRef {
                    namespace: "media.pipeline".to_string(),
                    kind: selium_control_plane_api::ContractKind::Event,
                    name: "camera.frames".to_string(),
                    version: "v1".to_string(),
                }],
                isolation: IsolationProfile::Standard,
                cpu_millis: 0,
                memory_mib: 0,
                ephemeral_storage_mib: 0,
                bandwidth_profile: BandwidthProfile::Standard,
                volume_mounts: Vec::new(),
                external_account_ref: None,
            })
            .expect("deployment");
        state
            .upsert_deployment(DeploymentSpec {
                workload: WorkloadRef {
                    tenant: "tenant-a".to_string(),
                    namespace: "media".to_string(),
                    name: "detector".to_string(),
                },
                module: "detector.wasm".to_string(),
                replicas: 1,
                contracts: vec![ContractRef {
                    namespace: "media.pipeline".to_string(),
                    kind: selium_control_plane_api::ContractKind::Event,
                    name: "camera.frames".to_string(),
                    version: "v1".to_string(),
                }],
                isolation: IsolationProfile::Standard,
                cpu_millis: 0,
                memory_mib: 0,
                ephemeral_storage_mib: 0,
                bandwidth_profile: BandwidthProfile::Standard,
                volume_mounts: Vec::new(),
                external_account_ref: None,
            })
            .expect("deployment");

        state.upsert_pipeline(PipelineSpec {
            name: "pipeline".to_string(),
            tenant: "tenant-a".to_string(),
            namespace: "media".to_string(),
            edges: vec![PipelineEdge {
                from: PipelineEndpoint {
                    endpoint: selium_control_plane_api::EventEndpointRef {
                        workload: WorkloadRef {
                            tenant: "tenant-a".to_string(),
                            namespace: "media".to_string(),
                            name: "ingest".to_string(),
                        },
                        name: "camera.frames".to_string(),
                    },
                    contract: ContractRef {
                        namespace: "media.pipeline".to_string(),
                        kind: selium_control_plane_api::ContractKind::Event,
                        name: "camera.frames".to_string(),
                        version: "v1".to_string(),
                    },
                },
                to: PipelineEndpoint {
                    endpoint: selium_control_plane_api::EventEndpointRef {
                        workload: WorkloadRef {
                            tenant: "tenant-a".to_string(),
                            namespace: "media".to_string(),
                            name: "detector".to_string(),
                        },
                        name: "camera.frames".to_string(),
                    },
                    contract: ContractRef {
                        namespace: "media.pipeline".to_string(),
                        kind: selium_control_plane_api::ContractKind::Event,
                        name: "camera.frames".to_string(),
                        version: "v1".to_string(),
                    },
                },
            }],
            external_account_ref: None,
        });

        state
    }

    #[test]
    fn builds_plan_for_registered_nodes() {
        let state = sample_state();
        let plan = build_plan(&state).expect("plan");
        assert_eq!(plan.instances.len(), 3);
        assert_eq!(plan.node_cpu_millis.get("local-node"), Some(&0));
        assert_eq!(plan.node_memory_mib.get("local-node"), Some(&0));
        assert!(
            plan.instances
                .iter()
                .all(|instance| instance.node == "local-node")
        );
    }

    #[test]
    fn builds_plan_using_allocatable_resources_without_slots() {
        let mut state = sample_state();
        let node = state.nodes.get_mut("local-node").expect("local node");
        node.capacity_slots = 0;
        node.allocatable_cpu_millis = Some(2_000);
        node.allocatable_memory_mib = Some(4_096);
        for deployment in state.deployments.values_mut() {
            deployment.cpu_millis = 500;
            deployment.memory_mib = 512;
        }

        let plan = build_plan(&state).expect("plan");

        assert_eq!(plan.instances.len(), 3);
        assert_eq!(plan.node_cpu_millis.get("local-node"), Some(&1_500));
        assert_eq!(plan.node_memory_mib.get("local-node"), Some(&1_536));
    }

    #[test]
    fn rejects_when_allocatable_cpu_is_exceeded() {
        let mut state = ControlPlaneState::new_local_default();
        let node = state.nodes.get_mut("local-node").expect("local node");
        node.capacity_slots = 0;
        node.allocatable_cpu_millis = Some(1_000);
        node.allocatable_memory_mib = Some(4_096);

        state
            .upsert_deployment(DeploymentSpec {
                workload: WorkloadRef {
                    tenant: "tenant-a".to_string(),
                    namespace: "media".to_string(),
                    name: "ingest".to_string(),
                },
                module: "ingest.wasm".to_string(),
                replicas: 2,
                contracts: Vec::new(),
                isolation: IsolationProfile::Standard,
                cpu_millis: 600,
                memory_mib: 256,
                ephemeral_storage_mib: 0,
                bandwidth_profile: BandwidthProfile::Standard,
                volume_mounts: Vec::new(),
                external_account_ref: None,
            })
            .expect("deployment");

        let err = build_plan(&state).expect_err("insufficient cpu");

        assert_eq!(
            err,
            ScheduleError::InsufficientResources("tenant-a/media/ingest".to_string())
        );
    }

    #[test]
    fn usage_contains_contract_refs() {
        let state = sample_state();
        let usage = deployment_contract_usage(&state);
        let ingest = usage.get("tenant-a/media/ingest").expect("ingest usage");
        assert!(ingest.contains("media.pipeline/event:camera.frames@v1"));
    }

    #[test]
    fn builds_endpoint_bridge_intents_per_source_instance() {
        let state = sample_state();
        let plan = build_plan(&state).expect("plan");

        let intents = build_endpoint_bridge_intents(&state, &plan);
        assert_eq!(intents.len(), 2);
        assert_eq!(
            intents[0].source_endpoint.key(),
            "tenant-a/media/ingest#event:camera.frames"
        );
        assert_eq!(
            intents[0].target_endpoint.key(),
            "tenant-a/media/detector#event:camera.frames"
        );
        assert_eq!(intents[0].source_node, "local-node");
        assert_eq!(intents[0].target_node, "local-node");
        assert_eq!(
            intents[0].target_instance_id,
            "tenant=tenant-a;namespace=media;workload=detector;replica=0"
        );
        assert!(
            intents[0]
                .bridge_id
                .starts_with(
                    "tenant=tenant-a;namespace=media;workload=ingest;replica=0->tenant=tenant-a;namespace=media;workload=detector;replica=0::"
                )
        );
    }
}
