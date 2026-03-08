use std::collections::{BTreeMap, BTreeSet};

use crate::{
    ApiError, ContractRef, ControlPlaneState, DeploymentSpec, IsolationProfile, NodeSpec,
    PipelineSpec,
};

impl ControlPlaneState {
    pub fn new_local_default() -> Self {
        let mut nodes = BTreeMap::new();
        nodes.insert(
            "local-node".to_string(),
            NodeSpec {
                name: "local-node".to_string(),
                capacity_slots: 64,
                supported_isolation: vec![
                    IsolationProfile::Standard,
                    IsolationProfile::Hardened,
                    IsolationProfile::Microvm,
                ],
                daemon_addr: "127.0.0.1:7100".to_string(),
                daemon_server_name: "localhost".to_string(),
                last_heartbeat_ms: 0,
            },
        );

        Self {
            nodes,
            ..Self::default()
        }
    }

    pub fn upsert_deployment(
        &mut self,
        deployment: DeploymentSpec,
    ) -> std::result::Result<(), ApiError> {
        if deployment.app.trim().is_empty() {
            return Err(ApiError::InvalidDeployment(
                "app name must not be empty".to_string(),
            ));
        }
        if deployment.module.trim().is_empty() {
            return Err(ApiError::InvalidDeployment(
                "module must not be empty".to_string(),
            ));
        }
        if deployment.replicas == 0 {
            return Err(ApiError::InvalidDeployment(
                "replicas must be > 0".to_string(),
            ));
        }

        self.deployments.insert(deployment.app.clone(), deployment);
        Ok(())
    }

    pub fn set_scale(&mut self, app: &str, replicas: u32) -> std::result::Result<(), ApiError> {
        let deployment = self
            .deployments
            .get_mut(app)
            .ok_or_else(|| ApiError::UnknownDeployment(app.to_string()))?;
        deployment.replicas = replicas.max(1);
        Ok(())
    }

    pub fn upsert_pipeline(&mut self, pipeline: PipelineSpec) {
        self.pipelines.insert(pipeline.name.clone(), pipeline);
    }

    pub fn upsert_node(&mut self, node: NodeSpec) -> std::result::Result<(), ApiError> {
        if node.name.trim().is_empty() {
            return Err(ApiError::InvalidNode(
                "node name must not be empty".to_string(),
            ));
        }
        if node.daemon_addr.trim().is_empty() {
            return Err(ApiError::InvalidNode(
                "daemon address must not be empty".to_string(),
            ));
        }

        self.nodes.insert(node.name.clone(), node);
        Ok(())
    }
}

pub fn ensure_pipeline_consistency(state: &ControlPlaneState) -> std::result::Result<(), ApiError> {
    for pipeline in state.pipelines.values() {
        for edge in &pipeline.edges {
            if !state.deployments.contains_key(&edge.from.app) {
                return Err(ApiError::UnknownDeployment(edge.from.app.clone()));
            }
            if !state.deployments.contains_key(&edge.to.app) {
                return Err(ApiError::UnknownDeployment(edge.to.app.clone()));
            }
            if !state.registry.has_contract(&edge.from.contract) {
                return Err(ApiError::InvalidContract);
            }
            if !state.registry.has_contract(&edge.to.contract) {
                return Err(ApiError::InvalidContract);
            }
        }
    }

    Ok(())
}

pub fn collect_contracts_for_app(state: &ControlPlaneState, app: &str) -> BTreeSet<ContractRef> {
    let mut out = BTreeSet::new();
    for pipeline in state.pipelines.values() {
        for edge in &pipeline.edges {
            if edge.from.app == app {
                out.insert(edge.from.contract.clone());
            }
            if edge.to.app == app {
                out.insert(edge.to.contract.clone());
            }
        }
    }
    out
}
