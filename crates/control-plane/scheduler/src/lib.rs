//! Placement and scheduling primitives for the control plane.

use std::collections::{BTreeMap, BTreeSet};

use rkyv::{Archive, Deserialize, Serialize};
use selium_control_plane_api::{
    ContractRef, ControlPlaneState, EventEndpointRef, IsolationProfile, PublicEndpointRef,
    WorkloadRef, collect_contracts_for_workload, ensure_pipeline_consistency,
};
use thiserror::Error;

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

#[derive(Debug, Error)]
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

    let mut used_slots: BTreeMap<String, u32> =
        node_order.iter().map(|node| (node.clone(), 0)).collect();
    let mut instances = Vec::new();

    for deployment in state.deployments.values() {
        let eligible = eligible_nodes(state, deployment.isolation.clone(), &schedulable_nodes);
        if eligible.is_empty() {
            return Err(ScheduleError::NoEligibleNodes(
                deployment.workload.to_string(),
            ));
        }

        let mut remaining = deployment.replicas;
        let mut placed = 0;
        while remaining > 0 {
            let node = &eligible[placed as usize % eligible.len()];
            let used = *used_slots.get(node).unwrap_or(&0);
            let capacity = state
                .nodes
                .get(node)
                .map(|node| node.capacity_slots)
                .unwrap_or(0);

            if used < capacity {
                let ordinal = deployment.replicas - remaining;
                instances.push(ScheduledInstance {
                    deployment: deployment.workload.key(),
                    instance_id: internal_instance_id(&deployment.workload, ordinal),
                    node: node.clone(),
                    isolation: deployment.isolation.clone(),
                });
                used_slots.insert(node.clone(), used + 1);
                remaining -= 1;
            }

            placed += 1;
            if placed > eligible.len() as u32 * deployment.replicas.max(1) {
                let available = eligible
                    .iter()
                    .map(|node| {
                        let cap = state
                            .nodes
                            .get(node)
                            .map(|node| node.capacity_slots)
                            .unwrap_or(0);
                        let used = used_slots.get(node).copied().unwrap_or(0);
                        cap.saturating_sub(used)
                    })
                    .sum();
                return Err(ScheduleError::InsufficientCapacity {
                    deployment: deployment.workload.to_string(),
                    required: deployment.replicas,
                    available,
                });
            }
        }
    }

    Ok(SchedulePlan {
        instances,
        node_slots: used_slots,
    })
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
        ContractRef, DeploymentSpec, PipelineEdge, PipelineEndpoint, PipelineSpec, parse_idl,
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
        });

        state
    }

    #[test]
    fn builds_plan_for_registered_nodes() {
        let state = sample_state();
        let plan = build_plan(&state).expect("plan");
        assert_eq!(plan.instances.len(), 3);
        assert!(
            plan.instances
                .iter()
                .all(|instance| instance.node == "local-node")
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
