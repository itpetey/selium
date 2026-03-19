use std::{path::Path, sync::Arc, time::Duration};

use anyhow::{Context, Result, anyhow};
use selium_abi::{InteractionKind, NetworkProtocol};
use selium_control_plane_agent::{
    ControlPlaneModuleConfig, ENTRYPOINT, EVENT_LOG_NAME, INTERNAL_BINDING_NAME, MODULE_ID,
    PEER_PROFILE_NAME, PeerTarget, SNAPSHOT_BLOB_STORE_NAME,
};
use selium_io_durability::RetentionPolicy;
use selium_kernel::{Kernel, registry::Registry};
use selium_runtime_network::{NetworkEgressProfile, NetworkIngressBinding, NetworkService};
use selium_runtime_storage::{StorageBlobStoreDefinition, StorageLogDefinition, StorageService};

use crate::{config::DaemonArgs, modules};

use super::{ControlPlaneAddresses, ControlPlaneTlsPaths, encode_hex, make_abs};

const CONTROL_PLANE_CAPABILITIES: &[&str] = &[
    "time_read",
    "network_lifecycle",
    "network_connect",
    "network_accept",
    "network_stream_read",
    "network_stream_write",
    "storage_lifecycle",
    "storage_log_read",
    "storage_log_write",
    "storage_blob_read",
    "storage_blob_write",
];

const CONTROL_PLANE_RESPONSIBILITIES: &[SystemModuleResponsibility] = &[
    SystemModuleResponsibility {
        area: "execution, isolation, transport termination, and capability enforcement",
        classification: SystemModuleResponsibilityClassification::HostSubstrate,
        rationale: "Trusted execution, isolation, authenticated transport termination, and capability grants remain permanent host substrate responsibilities.",
    },
    SystemModuleResponsibility {
        area: "authentication, authorisation, and gateway enforcement",
        classification: SystemModuleResponsibilityClassification::HostSubstrate,
        rationale: "The daemon stays responsible for verifying callers and enforcing access before any guest-owned control semantics run.",
    },
    SystemModuleResponsibility {
        area: "bridge worker and process realization",
        classification: SystemModuleResponsibilityClassification::HostSubstrate,
        rationale: "Process launches, capability grants, runtime-managed bindings, and bridge worker execution remain trusted host mechanisms.",
    },
    SystemModuleResponsibility {
        area: "control API meaning, bridge topology and lifecycle, and workload orchestration intent",
        classification: SystemModuleResponsibilityClassification::GuestOwnedPolicy,
        rationale: "Desired-state interpretation stays in the control-plane guest so roadmap phases can move product semantics out of the daemon without changing host enforcement primitives.",
    },
];

#[derive(Clone, Debug)]
pub(crate) struct SystemModuleDefinition {
    pub(crate) name: &'static str,
    pub(crate) launch: SystemModuleLaunchSpec,
    pub(crate) resources: SystemModuleResources,
    pub(crate) readiness: SystemModuleReadinessPolicy,
    pub(crate) responsibilities: &'static [SystemModuleResponsibility],
}

#[derive(Clone, Debug)]
pub(crate) struct SystemModuleLaunchSpec {
    pub(crate) module_id: &'static str,
    pub(crate) entrypoint: &'static str,
    pub(crate) capability_labels: &'static [&'static str],
    pub(crate) network_egress_profiles: Vec<String>,
    pub(crate) network_ingress_bindings: Vec<String>,
    pub(crate) storage_logs: Vec<String>,
    pub(crate) storage_blobs: Vec<String>,
    pub(crate) args: Vec<SystemModuleArg>,
}

impl SystemModuleLaunchSpec {
    pub(crate) fn as_runtime_spec(&self) -> Result<String> {
        let mut parts = vec![
            format!("path={}", self.module_id),
            format!("entrypoint={}", self.entrypoint),
            format!("capabilities={}", self.capability_labels.join(",")),
        ];

        if !self.network_egress_profiles.is_empty() {
            parts.push(format!(
                "network-egress-profiles={}",
                self.network_egress_profiles.join(",")
            ));
        }
        if !self.network_ingress_bindings.is_empty() {
            parts.push(format!(
                "network-ingress-bindings={}",
                self.network_ingress_bindings.join(",")
            ));
        }
        if !self.storage_logs.is_empty() {
            parts.push(format!("storage-logs={}", self.storage_logs.join(",")));
        }
        if !self.storage_blobs.is_empty() {
            parts.push(format!("storage-blobs={}", self.storage_blobs.join(",")));
        }
        if !self.args.is_empty() {
            let params = self
                .args
                .iter()
                .map(SystemModuleArg::param_label)
                .collect::<Vec<_>>()
                .join(",");
            let args = self
                .args
                .iter()
                .map(SystemModuleArg::arg_label)
                .collect::<Result<Vec<_>>>()?
                .join(",");
            parts.push(format!("params={params}"));
            parts.push(format!("args={args}"));
        }

        Ok(parts.join(";"))
    }
}

#[derive(Clone, Debug)]
pub(crate) enum SystemModuleArg {
    Buffer(Vec<u8>),
}

impl SystemModuleArg {
    fn param_label(&self) -> &'static str {
        match self {
            Self::Buffer(_) => "buffer",
        }
    }

    fn arg_label(&self) -> Result<String> {
        match self {
            Self::Buffer(bytes) => Ok(format!("buffer:hex:{}", encode_hex(bytes))),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct SystemModuleResources {
    pub(crate) network_egress_profiles: Vec<NetworkEgressProfile>,
    pub(crate) network_ingress_bindings: Vec<NetworkIngressBinding>,
    pub(crate) storage_logs: Vec<StorageLogDefinition>,
    pub(crate) storage_blobs: Vec<StorageBlobStoreDefinition>,
}

#[derive(Clone, Debug)]
pub(crate) struct SystemModuleReadinessPolicy {
    pub(crate) max_attempts: usize,
    pub(crate) retry_delay: Duration,
    pub(crate) description: &'static str,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SystemModuleResponsibilityClassification {
    HostSubstrate,
    GuestOwnedPolicy,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SystemModuleResponsibility {
    pub(crate) area: &'static str,
    pub(crate) classification: SystemModuleResponsibilityClassification,
    pub(crate) rationale: &'static str,
}

#[derive(Clone, Debug)]
pub(crate) struct SystemModuleInstance {
    pub(crate) name: &'static str,
    pub(crate) process_id: usize,
    pub(crate) readiness: SystemModuleReadinessPolicy,
}

pub(crate) async fn bootstrap_system_module(
    kernel: &Kernel,
    registry: &Arc<Registry>,
    work_dir: &Path,
    definition: &SystemModuleDefinition,
) -> Result<SystemModuleInstance> {
    register_system_module_resources(kernel, &definition.resources).await?;
    let process_id = spawn_system_module(kernel, registry, work_dir, &definition.launch).await?;
    Ok(SystemModuleInstance {
        name: definition.name,
        process_id,
        readiness: definition.readiness.clone(),
    })
}

pub(crate) async fn register_system_module_resources(
    kernel: &Kernel,
    resources: &SystemModuleResources,
) -> Result<()> {
    let network = kernel
        .get::<NetworkService>()
        .ok_or_else(|| anyhow!("missing NetworkService in kernel"))?;
    let storage = kernel
        .get::<StorageService>()
        .ok_or_else(|| anyhow!("missing StorageService in kernel"))?;

    for profile in &resources.network_egress_profiles {
        network.register_egress_profile(profile.clone()).await;
    }
    for binding in &resources.network_ingress_bindings {
        network.register_ingress_binding(binding.clone()).await;
    }
    for log in &resources.storage_logs {
        storage.register_log(log.clone()).await;
    }
    for blob in &resources.storage_blobs {
        storage.register_blob_store(blob.clone()).await;
    }

    Ok(())
}

pub(crate) async fn spawn_system_module(
    kernel: &Kernel,
    registry: &Arc<Registry>,
    work_dir: &Path,
    launch: &SystemModuleLaunchSpec,
) -> Result<usize> {
    let module_spec = launch.as_runtime_spec()?;
    let spawned = modules::spawn_from_cli(kernel, registry, work_dir, &[module_spec]).await?;
    spawned
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("system-module spawn returned no process id"))
}

pub(crate) fn build_control_plane_system_module(
    work_dir: &Path,
    args: &DaemonArgs,
    tls_paths: &ControlPlaneTlsPaths<'_>,
    addresses: &ControlPlaneAddresses<'_>,
    peers: Vec<PeerTarget>,
) -> Result<SystemModuleDefinition> {
    let mut allowed_authorities = vec![addresses.public_addr.to_string()];
    allowed_authorities.extend(peers.iter().map(|peer| peer.daemon_addr.clone()));

    let state_dir = make_abs(work_dir, &args.cp_state_dir);
    let config = ControlPlaneModuleConfig {
        node_id: args.cp_node_id.clone(),
        public_daemon_addr: addresses.public_addr.to_string(),
        public_daemon_server_name: args.cp_server_name.clone(),
        capacity_slots: args.cp_capacity_slots,
        allocatable_cpu_millis: args.cp_allocatable_cpu_millis,
        allocatable_memory_mib: args.cp_allocatable_memory_mib,
        reserve_cpu_utilisation_ppm: args.cp_reserve_cpu_utilisation_ppm,
        reserve_memory_utilisation_ppm: args.cp_reserve_memory_utilisation_ppm,
        reserve_slots_utilisation_ppm: args.cp_reserve_slots_utilisation_ppm,
        heartbeat_interval_ms: args.cp_heartbeat_interval_ms,
        bootstrap_leader: args.cp_bootstrap_leader,
        peers,
    };
    let config_bytes =
        selium_abi::encode_rkyv(&config).context("encode control-plane module config")?;

    Ok(SystemModuleDefinition {
        name: "control-plane",
        launch: SystemModuleLaunchSpec {
            module_id: MODULE_ID,
            entrypoint: ENTRYPOINT,
            capability_labels: CONTROL_PLANE_CAPABILITIES,
            network_egress_profiles: vec![PEER_PROFILE_NAME.to_string()],
            network_ingress_bindings: vec![INTERNAL_BINDING_NAME.to_string()],
            storage_logs: vec![EVENT_LOG_NAME.to_string()],
            storage_blobs: vec![SNAPSHOT_BLOB_STORE_NAME.to_string()],
            args: vec![SystemModuleArg::Buffer(config_bytes)],
        },
        resources: SystemModuleResources {
            network_egress_profiles: vec![NetworkEgressProfile {
                name: PEER_PROFILE_NAME.to_string(),
                protocol: NetworkProtocol::Quic,
                interactions: vec![InteractionKind::Stream],
                allowed_authorities,
                ca_cert_path: tls_paths.ca_path.to_path_buf(),
                client_cert_path: tls_paths.peer_cert_path.map(Path::to_path_buf),
                client_key_path: tls_paths.peer_key_path.map(Path::to_path_buf),
            }],
            network_ingress_bindings: vec![NetworkIngressBinding {
                name: INTERNAL_BINDING_NAME.to_string(),
                protocol: NetworkProtocol::Quic,
                interactions: vec![InteractionKind::Stream],
                listen_addr: addresses.internal_addr.to_string(),
                cert_path: tls_paths.cert_path.to_path_buf(),
                key_path: tls_paths.key_path.to_path_buf(),
            }],
            storage_logs: vec![StorageLogDefinition {
                name: EVENT_LOG_NAME.to_string(),
                path: state_dir.join("events.rkyv"),
                retention: RetentionPolicy::default(),
            }],
            storage_blobs: vec![StorageBlobStoreDefinition {
                name: SNAPSHOT_BLOB_STORE_NAME.to_string(),
                path: state_dir.join("snapshots"),
            }],
        },
        readiness: SystemModuleReadinessPolicy {
            max_attempts: 50,
            retry_delay: Duration::from_millis(100),
            description: "control status responds successfully",
        },
        responsibilities: CONTROL_PLANE_RESPONSIBILITIES,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn launch_spec_serialises_buffer_bootstrap_args() {
        let spec = SystemModuleLaunchSpec {
            module_id: "system/test.wasm",
            entrypoint: "start",
            capability_labels: &["time_read"],
            network_egress_profiles: vec!["egress".to_string()],
            network_ingress_bindings: vec!["ingress".to_string()],
            storage_logs: vec!["events".to_string()],
            storage_blobs: vec!["snapshots".to_string()],
            args: vec![SystemModuleArg::Buffer(vec![0x41, 0x42])],
        };

        let rendered = spec.as_runtime_spec().expect("render spec");
        assert!(rendered.contains("path=system/test.wasm"));
        assert!(rendered.contains("params=buffer"));
        assert!(rendered.contains("args=buffer:hex:4142"));
    }

    #[test]
    fn control_plane_responsibility_report_covers_host_and_guest_roles() {
        assert_eq!(
            CONTROL_PLANE_RESPONSIBILITIES
                .iter()
                .filter(|entry| {
                    entry.classification
                        == SystemModuleResponsibilityClassification::HostSubstrate
                })
                .count(),
            3
        );
        assert!(CONTROL_PLANE_RESPONSIBILITIES.iter().any(|entry| {
            entry.classification == SystemModuleResponsibilityClassification::HostSubstrate
        }));
        assert!(CONTROL_PLANE_RESPONSIBILITIES.iter().any(|entry| {
            entry.classification == SystemModuleResponsibilityClassification::GuestOwnedPolicy
        }));
        assert!(CONTROL_PLANE_RESPONSIBILITIES.iter().any(|entry| {
            entry.area
                == "control API meaning, bridge topology and lifecycle, and workload orchestration intent"
        }));
    }
}
