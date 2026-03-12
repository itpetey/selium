use rkyv::{Archive, Deserialize, Serialize};
use selium_abi::DataValue;
use selium_control_plane_api::{
    DeploymentSpec, DiscoveryCapabilityScope, DiscoveryRequest, NodeSpec, PipelineSpec, WorkloadRef,
};
use selium_io_tables::TableCommand;
use std::collections::BTreeSet;

/// Mutation commands applied to the deterministic control-plane engine.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum Mutation {
    /// Register or update an IDL package.
    PublishIdl { idl: String },
    /// Insert or replace a deployment specification.
    UpsertDeployment { spec: DeploymentSpec },
    /// Insert or replace a pipeline specification.
    UpsertPipeline { spec: PipelineSpec },
    /// Insert or replace a node specification.
    UpsertNode { spec: NodeSpec },
    /// Update the desired replica count for a workload.
    SetScale {
        workload: WorkloadRef,
        replicas: u32,
    },
    /// Apply a low-level table command.
    Table { command: TableCommand },
}

/// Query commands served by the deterministic control-plane engine.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum Query {
    /// Read a single table row.
    TableGet { table: String, key: String },
    /// Read a bounded set of table rows.
    TableScan { table: String, limit: usize },
    /// Read a materialized view snapshot.
    ViewGet { name: String },
    /// Return the full serialized control-plane state.
    ControlPlaneState,
    /// Return a machine-readable summary projection of the control plane.
    ControlPlaneSummary,
    /// Return attributed workload, module, pipeline, and node inventory.
    AttributedInfrastructureInventory {
        filter: AttributedInfrastructureFilter,
    },
    /// Return discovery state scoped for the caller.
    DiscoveryState { scope: DiscoveryCapabilityScope },
    /// Resolve a discovery request against the current state.
    ResolveDiscovery { request: DiscoveryRequest },
    /// Return node liveness derived from heartbeat age.
    NodesLive { now_ms: u64, max_staleness_ms: u64 },
}

/// Exact-match filters for attributed infrastructure inventory queries.
#[derive(Debug, Clone, Default, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct AttributedInfrastructureFilter {
    /// Restrict results to this external account reference when present.
    pub external_account_ref: Option<String>,
    /// Restrict results to this workload key when present.
    pub workload: Option<String>,
    /// Restrict results to this module identifier when present.
    pub module: Option<String>,
    /// Restrict results to this pipeline key when present.
    pub pipeline: Option<String>,
    /// Restrict results to this node name when present.
    pub node: Option<String>,
}

impl AttributedInfrastructureFilter {
    /// Match an optional external account reference against the filter.
    pub fn matches_external_account_ref(&self, actual: Option<&str>) -> bool {
        self.external_account_ref
            .as_deref()
            .is_none_or(|expected| actual == Some(expected))
    }

    /// Match a workload key against the filter.
    pub fn matches_workload(&self, actual: &str) -> bool {
        self.workload
            .as_deref()
            .is_none_or(|expected| actual == expected)
    }

    /// Match a module identifier against the filter.
    pub fn matches_module(&self, actual: &str) -> bool {
        self.module
            .as_deref()
            .is_none_or(|expected| actual == expected)
    }

    /// Match a pipeline key against the filter.
    pub fn matches_pipeline(&self, actual: &str) -> bool {
        self.pipeline
            .as_deref()
            .is_none_or(|expected| actual == expected)
    }

    /// Match a node name against the filter.
    pub fn matches_node(&self, actual: &str) -> bool {
        self.node
            .as_deref()
            .is_none_or(|expected| actual == expected)
    }

    /// Match a workload key against an optional pipeline scope.
    pub fn matches_pipeline_scope(
        &self,
        actual_workload: &str,
        scope: Option<&BTreeSet<String>>,
    ) -> bool {
        scope.is_none_or(|scope| scope.contains(actual_workload))
    }
}

/// Durable idempotent mutation envelope written to the control-plane log.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct MutationEnvelope {
    /// Client-supplied idempotency key.
    pub idempotency_key: String,
    /// Mutation to apply.
    pub mutation: Mutation,
}

/// Mutation result returned after a committed entry is applied.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct MutationResponse {
    /// Committed log index.
    pub index: u64,
    /// Machine-readable result payload.
    pub result: DataValue,
}

/// Query response returned by the control-plane engine.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct QueryResponse {
    /// Machine-readable result payload.
    pub result: DataValue,
}
