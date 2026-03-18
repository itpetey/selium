use selium_control_plane_api::{
    ApiError, ControlPlaneState, DiscoveryCapabilityScope, DiscoveryOperation, DiscoveryRequest,
    DiscoveryResolution, DiscoveryState, DiscoveryTarget, OperationalProcessRecord,
    OperationalProcessSelector, ResolvedEndpoint, ResolvedWorkload, build_discovery_state,
};
use selium_control_plane_scheduler::build_plan;

use crate::RuntimeError;

#[cfg(test)]
use crate::ControlPlaneEngine;

pub(super) fn authorised_discovery_state(
    state: &ControlPlaneState,
    scope: &DiscoveryCapabilityScope,
) -> Result<DiscoveryState, RuntimeError> {
    let discovery = build_discovery_state(state)?;
    let workloads = discovery
        .workloads
        .into_iter()
        .filter(|record| scope.allows_workload(DiscoveryOperation::Discover, &record.workload))
        .collect();
    let endpoints = discovery
        .endpoints
        .into_iter()
        .filter(|record| scope.allows_endpoint(DiscoveryOperation::Discover, &record.endpoint))
        .collect();

    Ok(DiscoveryState {
        workloads,
        endpoints,
    })
}

pub(super) fn resolve_discovery(
    state: &ControlPlaneState,
    request: DiscoveryRequest,
) -> Result<DiscoveryResolution, RuntimeError> {
    let discovery = build_discovery_state(state)?;
    let plan = build_plan(state).map_err(|err| RuntimeError::Scheduler(err.to_string()))?;

    match request.target {
        DiscoveryTarget::Workload(workload) => {
            if !request.scope.allows_workload(request.operation, &workload) {
                return Err(ApiError::Unauthorised {
                    operation: request.operation.as_str().to_string(),
                    subject: workload.key(),
                }
                .into());
            }

            if !discovery
                .workloads
                .iter()
                .any(|record| record.workload == workload)
            {
                return Err(ApiError::UnknownDeployment(workload.key()).into());
            }
            let endpoints = discovery
                .endpoints
                .iter()
                .filter(|record| record.endpoint.workload == workload)
                .cloned()
                .collect();

            Ok(DiscoveryResolution::Workload(ResolvedWorkload {
                workload,
                endpoints,
            }))
        }
        DiscoveryTarget::Endpoint(endpoint) => {
            if !request.scope.allows_endpoint(request.operation, &endpoint) {
                return Err(ApiError::Unauthorised {
                    operation: request.operation.as_str().to_string(),
                    subject: endpoint.key(),
                }
                .into());
            }

            let endpoint_record = discovery
                .endpoints
                .into_iter()
                .find(|record| record.endpoint == endpoint)
                .ok_or_else(|| ApiError::UnknownEndpoint(endpoint.key()))?;

            Ok(DiscoveryResolution::Endpoint(ResolvedEndpoint {
                contract: endpoint_record.contract,
                endpoint,
            }))
        }
        DiscoveryTarget::RunningProcess(selector) => {
            if request.operation == DiscoveryOperation::Bind {
                return Err(ApiError::InvalidBindTarget(
                    "running process discovery is operational-only".to_string(),
                )
                .into());
            }

            let record =
                resolve_operational_process(&plan.instances, state, &request.scope, selector)?;
            Ok(DiscoveryResolution::RunningProcess(record))
        }
    }
}

fn resolve_operational_process(
    instances: &[selium_control_plane_scheduler::ScheduledInstance],
    state: &ControlPlaneState,
    scope: &DiscoveryCapabilityScope,
    selector: OperationalProcessSelector,
) -> Result<OperationalProcessRecord, RuntimeError> {
    match selector {
        OperationalProcessSelector::ReplicaKey(replica_key) => {
            let instance = instances
                .iter()
                .find(|instance| instance.instance_id == replica_key)
                .ok_or_else(|| ApiError::UnknownDeployment(replica_key.clone()))?;
            let workload = state
                .deployments
                .get(&instance.deployment)
                .map(|deployment| deployment.workload.clone())
                .ok_or_else(|| ApiError::UnknownDeployment(instance.deployment.clone()))?;
            if !scope.allows_operational_process_discovery(&workload) {
                return Err(ApiError::Unauthorised {
                    operation: DiscoveryOperation::Discover.as_str().to_string(),
                    subject: workload.key(),
                }
                .into());
            }

            Ok(OperationalProcessRecord {
                workload,
                replica_key: instance.instance_id.clone(),
                node: instance.node.clone(),
            })
        }
        OperationalProcessSelector::Workload(workload) => {
            if !scope.allows_operational_process_discovery(&workload) {
                return Err(ApiError::Unauthorised {
                    operation: DiscoveryOperation::Discover.as_str().to_string(),
                    subject: workload.key(),
                }
                .into());
            }

            let mut matches = instances
                .iter()
                .filter(|instance| instance.deployment == workload.key())
                .map(|instance| OperationalProcessRecord {
                    workload: workload.clone(),
                    replica_key: instance.instance_id.clone(),
                    node: instance.node.clone(),
                })
                .collect::<Vec<_>>();

            if matches.is_empty() {
                return Err(ApiError::UnknownDeployment(workload.key()).into());
            }
            if matches.len() > 1 {
                return Err(ApiError::AmbiguousProcess(format!(
                    "{} has {} replicas; specify a replica key",
                    workload,
                    matches.len()
                ))
                .into());
            }

            Ok(matches.remove(0))
        }
    }
}

#[allow(dead_code)]
#[cfg(test)]
pub(super) fn discovery_engine() -> ControlPlaneEngine {
    use selium_control_plane_api::{
        BandwidthProfile, ContractKind, ContractRef, DeploymentSpec, IsolationProfile,
        PlacementMode, WorkloadRef, parse_idl,
    };

    fn event_contract() -> ContractRef {
        ContractRef {
            namespace: "media.pipeline".to_string(),
            kind: ContractKind::Event,
            name: "camera.frames".to_string(),
            version: "v1".to_string(),
        }
    }

    let mut state = ControlPlaneState::new_local_default();
    state
        .registry
        .register_package(
            parse_idl(
                "package media.pipeline.v1;\n\
                 schema Frame { camera_id: string; }\n\
                 event camera.frames(Frame) { replay: enabled; }",
            )
            .expect("parse"),
        )
        .expect("register");

    for (tenant, namespace, name, replicas) in [
        ("tenant-a", "media", "ingest", 2),
        ("tenant-a", "other", "ingest", 1),
        ("tenant-b", "media", "ingest", 1),
    ] {
        state
            .upsert_deployment(DeploymentSpec {
                workload: WorkloadRef {
                    tenant: tenant.to_string(),
                    namespace: namespace.to_string(),
                    name: name.to_string(),
                },
                module: format!("{tenant}-{namespace}-{name}.wasm"),
                replicas,
                contracts: vec![event_contract()],
                isolation: IsolationProfile::Standard,
                placement_mode: PlacementMode::ElasticPack,
                cpu_millis: 0,
                memory_mib: 0,
                ephemeral_storage_mib: 0,
                bandwidth_profile: BandwidthProfile::Standard,
                volume_mounts: Vec::new(),
                external_account_ref: None,
            })
            .expect("deployment");
    }

    ControlPlaneEngine::new(state)
}

#[allow(dead_code)]
#[cfg(test)]
pub(super) fn multi_kind_discovery_engine() -> ControlPlaneEngine {
    use selium_control_plane_api::{
        BandwidthProfile, ContractKind, ContractRef, DeploymentSpec, IsolationProfile,
        PlacementMode, WorkloadRef, parse_idl,
    };

    fn event_contract() -> ContractRef {
        ContractRef {
            namespace: "media.pipeline".to_string(),
            kind: ContractKind::Event,
            name: "camera.frames".to_string(),
            version: "v1".to_string(),
        }
    }

    fn service_contract() -> ContractRef {
        ContractRef {
            namespace: "media.pipeline".to_string(),
            kind: ContractKind::Service,
            name: "camera.detect".to_string(),
            version: "v1".to_string(),
        }
    }

    fn stream_contract() -> ContractRef {
        ContractRef {
            namespace: "media.pipeline".to_string(),
            kind: ContractKind::Stream,
            name: "camera.raw".to_string(),
            version: "v1".to_string(),
        }
    }

    let mut state = ControlPlaneState::new_local_default();
    state
        .registry
        .register_package(
            parse_idl(
                "package media.pipeline.v1;\n\
                 schema Frame { camera_id: string; }\n\
                 event camera.frames(Frame) { replay: enabled; }\n\
                 service camera.detect(Frame) -> Frame;\n\
                 stream camera.raw(Frame);",
            )
            .expect("parse"),
        )
        .expect("register");
    state
        .upsert_deployment(DeploymentSpec {
            workload: WorkloadRef {
                tenant: "tenant-a".to_string(),
                namespace: "media".to_string(),
                name: "router".to_string(),
            },
            module: "tenant-a-media-router.wasm".to_string(),
            replicas: 2,
            contracts: vec![event_contract(), service_contract(), stream_contract()],
            isolation: IsolationProfile::Standard,
            placement_mode: PlacementMode::ElasticPack,
            cpu_millis: 0,
            memory_mib: 0,
            ephemeral_storage_mib: 0,
            bandwidth_profile: BandwidthProfile::Standard,
            volume_mounts: Vec::new(),
            external_account_ref: None,
        })
        .expect("deployment");

    ControlPlaneEngine::new(state)
}
