//! Placement and scheduling primitives for the control plane.

use std::collections::{BTreeMap, BTreeSet};

use rkyv::{Archive, Deserialize, Serialize};
use selium_control_plane_api::{
    ControlPlaneState, IsolationProfile, collect_contracts_for_app, ensure_pipeline_consistency,
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

    if state.nodes.is_empty() {
        return Err(ScheduleError::NoNodes);
    }

    let mut node_order = state.nodes.keys().cloned().collect::<Vec<_>>();
    node_order.sort_unstable();

    let mut used_slots: BTreeMap<String, u32> =
        node_order.iter().map(|node| (node.clone(), 0)).collect();
    let mut instances = Vec::new();

    for deployment in state.deployments.values() {
        let eligible = eligible_nodes(state, deployment.isolation.clone());
        if eligible.is_empty() {
            return Err(ScheduleError::NoEligibleNodes(deployment.app.clone()));
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
                    deployment: deployment.app.clone(),
                    instance_id: format!("{}-{}", deployment.app, ordinal),
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
                    deployment: deployment.app.clone(),
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

pub fn deployment_contract_usage(state: &ControlPlaneState) -> BTreeMap<String, BTreeSet<String>> {
    state
        .deployments
        .keys()
        .map(|deployment| {
            let contracts = collect_contracts_for_app(state, deployment)
                .into_iter()
                .map(|contract| {
                    format!(
                        "{}/{}@{}",
                        contract.namespace, contract.name, contract.version
                    )
                })
                .collect::<BTreeSet<_>>();
            (deployment.clone(), contracts)
        })
        .collect()
}

fn eligible_nodes(state: &ControlPlaneState, isolation: IsolationProfile) -> Vec<String> {
    let mut nodes = state
        .nodes
        .values()
        .filter(|node| node.supported_isolation.contains(&isolation))
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
                app: "ingest".to_string(),
                module: "ingest.wasm".to_string(),
                replicas: 2,
                contracts: vec![],
                isolation: IsolationProfile::Standard,
            })
            .expect("deployment");
        state
            .upsert_deployment(DeploymentSpec {
                app: "detector".to_string(),
                module: "detector.wasm".to_string(),
                replicas: 1,
                contracts: vec![],
                isolation: IsolationProfile::Standard,
            })
            .expect("deployment");

        state.upsert_pipeline(PipelineSpec {
            name: "pipeline".to_string(),
            namespace: "media".to_string(),
            edges: vec![PipelineEdge {
                from: PipelineEndpoint {
                    app: "ingest".to_string(),
                    contract: ContractRef {
                        namespace: "media.pipeline".to_string(),
                        name: "camera.frames".to_string(),
                        version: "v1".to_string(),
                    },
                },
                to: PipelineEndpoint {
                    app: "detector".to_string(),
                    contract: ContractRef {
                        namespace: "media.pipeline".to_string(),
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
        let ingest = usage.get("ingest").expect("ingest usage");
        assert!(ingest.contains("media.pipeline/camera.frames@v1"));
    }
}
