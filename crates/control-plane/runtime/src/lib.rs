//! Deterministic control-plane state machine built on host-managed consensus + tables.

mod discovery;
mod inventory;
mod values;

use std::collections::BTreeMap;

use rkyv::{Archive, Deserialize, Serialize};
use selium_abi::{DataValue, decode_rkyv, encode_rkyv};
use selium_control_plane_api::{
    ApiError, ControlPlaneState, ensure_pipeline_consistency, parse_idl,
};
use selium_control_plane_core::{
    Mutation, MutationEnvelope, MutationResponse, Query, QueryResponse, serialize_optional_u32,
};
use selium_control_plane_scheduler::{build_plan, deployment_contract_usage};
use selium_io_consensus::LogEntry;
use selium_io_tables::{TableError, TableStore};
use thiserror::Error;

use crate::discovery::{authorised_discovery_state, resolve_discovery};
use crate::inventory::serialize_attributed_infrastructure_inventory;
use crate::values::{
    ok_status, serialize_deployment_spec, serialize_isolation_profiles, serialize_node_spec,
    serialize_pipeline_spec, serialize_schedule_plan, serialize_string_set_map,
    serialize_table_record, serialize_table_result,
};

#[cfg(test)]
use selium_control_plane_api::{
    ContractRegistry, DeploymentSpec, DiscoveryCapabilityScope, DiscoveryOperation,
    DiscoveryRequest, DiscoveryResolution, DiscoveryState, DiscoveryTarget, NodeSpec,
    OperationalProcessSelector, ResolvedEndpoint, ResolvedWorkload, WorkloadRef,
};
#[cfg(test)]
use selium_control_plane_core::AttributedInfrastructureFilter;
#[cfg(test)]
use selium_io_tables::TableCommand;

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct EngineSnapshot {
    pub control_plane: ControlPlaneState,
    pub tables: TableStore,
    pub last_applied: u64,
}

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error(transparent)]
    Api(#[from] ApiError),
    #[error(transparent)]
    Table(#[from] TableError),
    #[error("scheduler error: {0}")]
    Scheduler(String),
    #[error("serialization error: {0}")]
    Serialization(String),
    #[error("invalid snapshot payload: {0}")]
    InvalidSnapshotPayload(String),
    #[error("invalid log payload: {0}")]
    InvalidLogPayload(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ControlPlaneEngine {
    control_plane: ControlPlaneState,
    tables: TableStore,
    last_applied: u64,
}

impl Default for ControlPlaneEngine {
    fn default() -> Self {
        Self::new(ControlPlaneState::new_local_default())
    }
}

impl ControlPlaneEngine {
    pub fn new(control_plane: ControlPlaneState) -> Self {
        Self {
            control_plane,
            tables: TableStore::default(),
            last_applied: 0,
        }
    }

    pub fn from_snapshot(snapshot: EngineSnapshot) -> Self {
        Self {
            control_plane: snapshot.control_plane,
            tables: snapshot.tables,
            last_applied: snapshot.last_applied,
        }
    }

    pub fn snapshot(&self) -> EngineSnapshot {
        EngineSnapshot {
            control_plane: self.control_plane.clone(),
            tables: self.tables.clone(),
            last_applied: self.last_applied,
        }
    }

    pub fn last_applied(&self) -> u64 {
        self.last_applied
    }

    pub fn encode_mutation(envelope: &MutationEnvelope) -> Result<Vec<u8>, RuntimeError> {
        encode_rkyv(envelope).map_err(|err| RuntimeError::Serialization(err.to_string()))
    }

    pub fn decode_snapshot(bytes: &[u8]) -> Result<EngineSnapshot, RuntimeError> {
        decode_rkyv(bytes).map_err(|err| RuntimeError::InvalidSnapshotPayload(err.to_string()))
    }

    pub fn decode_mutation(bytes: &[u8]) -> Result<MutationEnvelope, RuntimeError> {
        decode_rkyv(bytes).map_err(|err| RuntimeError::InvalidLogPayload(err.to_string()))
    }

    pub fn decode_replayed_mutation(bytes: &[u8]) -> Result<MutationEnvelope, RuntimeError> {
        Self::decode_mutation(bytes)
    }

    pub fn apply_committed_entry(
        &mut self,
        entry: &LogEntry,
    ) -> Result<MutationResponse, RuntimeError> {
        let envelope = Self::decode_mutation(&entry.payload)?;
        let result = self.apply_mutation(entry.index, envelope.mutation)?;
        Ok(MutationResponse {
            index: entry.index,
            result,
        })
    }

    pub fn apply_replayed_entry(
        &mut self,
        entry: &LogEntry,
    ) -> Result<MutationResponse, RuntimeError> {
        let envelope = Self::decode_replayed_mutation(&entry.payload)?;
        let result = self.apply_mutation(entry.index, envelope.mutation)?;
        Ok(MutationResponse {
            index: entry.index,
            result,
        })
    }

    pub fn apply_mutation(
        &mut self,
        index: u64,
        mutation: Mutation,
    ) -> Result<DataValue, RuntimeError> {
        let result: Result<DataValue, RuntimeError> = match mutation {
            Mutation::PublishIdl { idl } => {
                let package = parse_idl(&idl)?;
                let report = self.control_plane.registry.register_package(package)?;
                let warnings = report
                    .warnings
                    .into_iter()
                    .map(DataValue::from)
                    .collect::<Vec<_>>();
                Ok(DataValue::Map(BTreeMap::from([
                    (
                        "previous_version".to_string(),
                        DataValue::from(report.previous_version),
                    ),
                    (
                        "current_version".to_string(),
                        DataValue::from(report.current_version),
                    ),
                    ("warnings".to_string(), DataValue::List(warnings)),
                ])))
            }
            Mutation::UpsertDeployment { spec } => {
                self.control_plane.upsert_deployment(spec)?;
                ensure_pipeline_consistency(&self.control_plane)?;
                Ok(ok_status())
            }
            Mutation::UpsertPipeline { spec } => {
                self.control_plane.upsert_pipeline(spec);
                ensure_pipeline_consistency(&self.control_plane)?;
                Ok(ok_status())
            }
            Mutation::SetScale { workload, replicas } => {
                self.control_plane.set_scale(&workload, replicas)?;
                ensure_pipeline_consistency(&self.control_plane)?;
                Ok(ok_status())
            }
            Mutation::UpsertNode { spec } => {
                self.control_plane.upsert_node(spec)?;
                Ok(ok_status())
            }
            Mutation::Table { command } => {
                let applied = self.tables.apply(command, index)?;
                Ok(serialize_table_result(applied))
            }
        };
        let result = result?;
        self.last_applied = self.last_applied.max(index);
        Ok(result)
    }

    pub fn query(&self, query: Query) -> Result<QueryResponse, RuntimeError> {
        let result = match query {
            Query::TableGet { table, key } => self
                .tables
                .get(&table, &key)
                .map(serialize_table_record)
                .unwrap_or(DataValue::Null),
            Query::TableScan { table, limit } => {
                let rows = self.tables.scan(&table, limit);
                DataValue::List(
                    rows.into_iter()
                        .map(|(key, value)| {
                            DataValue::Map(BTreeMap::from([
                                ("key".to_string(), DataValue::from(key)),
                                ("record".to_string(), serialize_table_record(&value)),
                            ]))
                        })
                        .collect::<Vec<_>>(),
                )
            }
            Query::ViewGet { name } => self.tables.view(&name).cloned().unwrap_or(DataValue::Null),
            Query::ControlPlaneState => DataValue::Bytes(
                encode_rkyv(&self.control_plane)
                    .map_err(|err| RuntimeError::Serialization(err.to_string()))?,
            ),
            Query::ControlPlaneSummary => {
                let plan = build_plan(&self.control_plane)
                    .map_err(|err| RuntimeError::Scheduler(err.to_string()))?;
                let usage = deployment_contract_usage(&self.control_plane);
                DataValue::Map(BTreeMap::from([
                    (
                        "deployments".to_string(),
                        DataValue::List(
                            self.control_plane
                                .deployments
                                .values()
                                .map(serialize_deployment_spec)
                                .collect(),
                        ),
                    ),
                    (
                        "pipelines".to_string(),
                        DataValue::List(
                            self.control_plane
                                .pipelines
                                .keys()
                                .cloned()
                                .map(DataValue::from)
                                .collect(),
                        ),
                    ),
                    (
                        "pipeline_specs".to_string(),
                        DataValue::List(
                            self.control_plane
                                .pipelines
                                .values()
                                .map(serialize_pipeline_spec)
                                .collect(),
                        ),
                    ),
                    (
                        "nodes".to_string(),
                        DataValue::List(
                            self.control_plane
                                .nodes
                                .values()
                                .map(serialize_node_spec)
                                .collect(),
                        ),
                    ),
                    ("schedule".to_string(), serialize_schedule_plan(&plan)),
                    (
                        "contract_usage".to_string(),
                        serialize_string_set_map(&usage),
                    ),
                ]))
            }
            Query::AttributedInfrastructureInventory { filter } => {
                let plan = build_plan(&self.control_plane)
                    .map_err(|err| RuntimeError::Scheduler(err.to_string()))?;
                serialize_attributed_infrastructure_inventory(
                    &self.control_plane,
                    &plan,
                    &filter,
                    self.last_applied,
                )
            }
            Query::DiscoveryState { scope } => DataValue::Bytes(
                encode_rkyv(&authorised_discovery_state(&self.control_plane, &scope)?)
                    .map_err(|err| RuntimeError::Serialization(err.to_string()))?,
            ),
            Query::ResolveDiscovery { request } => DataValue::Bytes(
                encode_rkyv(&resolve_discovery(&self.control_plane, request)?)
                    .map_err(|err| RuntimeError::Serialization(err.to_string()))?,
            ),
            Query::NodesLive {
                now_ms,
                max_staleness_ms,
            } => {
                let nodes = self
                    .control_plane
                    .nodes
                    .values()
                    .map(|node| {
                        let age_ms = now_ms.saturating_sub(node.last_heartbeat_ms);
                        DataValue::Map(BTreeMap::from([
                            ("name".to_string(), DataValue::from(node.name.clone())),
                            (
                                "daemon_addr".to_string(),
                                DataValue::from(node.daemon_addr.clone()),
                            ),
                            (
                                "daemon_server_name".to_string(),
                                DataValue::from(node.daemon_server_name.clone()),
                            ),
                            (
                                "capacity_slots".to_string(),
                                DataValue::from(node.capacity_slots),
                            ),
                            (
                                "allocatable_cpu_millis".to_string(),
                                serialize_optional_u32(node.allocatable_cpu_millis),
                            ),
                            (
                                "allocatable_memory_mib".to_string(),
                                serialize_optional_u32(node.allocatable_memory_mib),
                            ),
                            (
                                "reserve_cpu_utilisation_ppm".to_string(),
                                DataValue::from(node.reserve_cpu_utilisation_ppm),
                            ),
                            (
                                "reserve_memory_utilisation_ppm".to_string(),
                                DataValue::from(node.reserve_memory_utilisation_ppm),
                            ),
                            (
                                "reserve_slots_utilisation_ppm".to_string(),
                                DataValue::from(node.reserve_slots_utilisation_ppm),
                            ),
                            (
                                "supported_isolation".to_string(),
                                serialize_isolation_profiles(&node.supported_isolation),
                            ),
                            (
                                "last_heartbeat_ms".to_string(),
                                DataValue::from(node.last_heartbeat_ms),
                            ),
                            ("age_ms".to_string(), DataValue::from(age_ms)),
                            (
                                "live".to_string(),
                                DataValue::from(age_ms <= max_staleness_ms),
                            ),
                        ]))
                    })
                    .collect::<Vec<_>>();
                DataValue::Map(BTreeMap::from([
                    ("now_ms".to_string(), DataValue::from(now_ms)),
                    (
                        "max_staleness_ms".to_string(),
                        DataValue::from(max_staleness_ms),
                    ),
                    ("nodes".to_string(), DataValue::List(nodes)),
                ]))
            }
        };

        Ok(QueryResponse { result })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use selium_control_plane_api::{
        BandwidthProfile, ContractKind, ContractRef, DiscoveryPattern, EventEndpointRef,
        ExternalAccountRef, GUEST_LOG_STDERR_ENDPOINT, GUEST_LOG_STDOUT_ENDPOINT, IsolationProfile,
        PipelineEdge, PipelineEndpoint, PipelineSpec, PublicEndpointRef, VolumeMount, parse_idl,
    };
    use selium_io_consensus::{ConsensusConfig, RaftNode};

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

    fn shared_name_contract(kind: ContractKind) -> ContractRef {
        ContractRef {
            namespace: "media.pipeline".to_string(),
            kind,
            name: "camera.shared".to_string(),
            version: "v1".to_string(),
        }
    }

    fn discovery_engine() -> ControlPlaneEngine {
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
                    placement_mode: selium_control_plane_api::PlacementMode::ElasticPack,
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

    fn multi_kind_discovery_engine() -> ControlPlaneEngine {
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
                placement_mode: selium_control_plane_api::PlacementMode::ElasticPack,
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

    fn same_name_multi_kind_discovery_engine() -> ControlPlaneEngine {
        let mut state = ControlPlaneState::new_local_default();
        state
            .registry
            .register_package(
                parse_idl(
                    "package media.pipeline.v1;\n\
                     schema Frame { camera_id: string; }\n\
                     event camera.shared(Frame) { replay: enabled; }\n\
                     service camera.shared(Frame) -> Frame;\n\
                     stream camera.shared(Frame);",
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
                contracts: vec![
                    shared_name_contract(ContractKind::Event),
                    shared_name_contract(ContractKind::Service),
                    shared_name_contract(ContractKind::Stream),
                ],
                isolation: IsolationProfile::Standard,
                placement_mode: selium_control_plane_api::PlacementMode::ElasticPack,
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

    #[test]
    fn committed_log_entry_applies_to_tables() {
        let mut engine = ControlPlaneEngine::default();
        let envelope = MutationEnvelope {
            idempotency_key: "k1".to_string(),
            mutation: Mutation::Table {
                command: TableCommand::Put {
                    table: "apps".to_string(),
                    key: "echo".to_string(),
                    value: DataValue::Map(BTreeMap::from([(
                        "replicas".to_string(),
                        DataValue::from(1u64),
                    )])),
                },
            },
        };

        let payload = ControlPlaneEngine::encode_mutation(&envelope).expect("encode");
        let entry = selium_io_consensus::LogEntry {
            index: 1,
            term: 1,
            payload,
        };

        let response = engine.apply_committed_entry(&entry).expect("apply");
        assert_eq!(response.index, 1);
        assert_eq!(engine.last_applied(), 1);

        let query = engine
            .query(Query::TableGet {
                table: "apps".to_string(),
                key: "echo".to_string(),
            })
            .expect("query");
        assert_eq!(
            query.result.get("version").and_then(DataValue::as_u64),
            Some(1)
        );
    }

    #[test]
    fn summary_query_runs_scheduler() {
        let mut engine = ControlPlaneEngine::default();
        engine
            .apply_mutation(
                1,
                Mutation::UpsertDeployment {
                    spec: DeploymentSpec {
                        workload: WorkloadRef {
                            tenant: "tenant-a".to_string(),
                            namespace: "default".to_string(),
                            name: "echo".to_string(),
                        },
                        module: "echo.wasm".to_string(),
                        replicas: 1,
                        contracts: vec![],
                        isolation: selium_control_plane_api::IsolationProfile::Standard,
                        placement_mode: selium_control_plane_api::PlacementMode::ElasticPack,
                        cpu_millis: 500,
                        memory_mib: 256,
                        ephemeral_storage_mib: 128,
                        bandwidth_profile: BandwidthProfile::High,
                        volume_mounts: vec![VolumeMount {
                            name: "scratch".to_string(),
                            mount_path: "/data".to_string(),
                            read_only: false,
                        }],
                        external_account_ref: Some(ExternalAccountRef {
                            key: "acct-123".to_string(),
                        }),
                    },
                },
            )
            .expect("upsert");

        engine
            .apply_mutation(
                2,
                Mutation::UpsertNode {
                    spec: NodeSpec {
                        name: "local-node".to_string(),
                        capacity_slots: 0,
                        allocatable_cpu_millis: Some(2_000),
                        allocatable_memory_mib: Some(4_096),
                        reserve_cpu_utilisation_ppm: 800_000,
                        reserve_memory_utilisation_ppm: 800_000,
                        reserve_slots_utilisation_ppm: 800_000,
                        observed_running_instances: Some(1),
                        observed_active_bridges: Some(0),
                        observed_memory_mib: Some(512),
                        observed_workloads: BTreeMap::from([(
                            "tenant-a/default/echo".to_string(),
                            1u32,
                        )]),
                        observed_workload_memory_mib: BTreeMap::from([(
                            "tenant-a/default/echo".to_string(),
                            512u32,
                        )]),
                        supported_isolation: vec![IsolationProfile::Standard],
                        daemon_addr: "127.0.0.1:7100".to_string(),
                        daemon_server_name: "localhost".to_string(),
                        last_heartbeat_ms: 42,
                    },
                },
            )
            .expect("node");

        let query = engine.query(Query::ControlPlaneSummary).expect("summary");
        let deployments = query
            .result
            .get("deployments")
            .and_then(DataValue::as_array)
            .expect("deployments array");
        let resources = deployments[0]
            .get("resources")
            .expect("deployment resources");
        assert_eq!(
            resources.get("cpu_millis").and_then(DataValue::as_u64),
            Some(500)
        );
        assert_eq!(
            resources
                .get("bandwidth_profile")
                .and_then(DataValue::as_str),
            Some("high")
        );
        assert_eq!(
            deployments[0]
                .get("external_account_ref")
                .and_then(DataValue::as_str),
            Some("acct-123")
        );
        assert_eq!(
            query
                .result
                .get("schedule")
                .and_then(|schedule| schedule.get("node_cpu_millis"))
                .and_then(|usage| usage.get("local-node"))
                .and_then(DataValue::as_u64),
            Some(500)
        );
    }

    #[test]
    fn nodes_live_query_preserves_usage_export_connection_fields() {
        let mut engine = ControlPlaneEngine::new(ControlPlaneState::default());
        engine
            .apply_mutation(
                1,
                Mutation::UpsertNode {
                    spec: NodeSpec {
                        name: "node-a".to_string(),
                        capacity_slots: 8,
                        allocatable_cpu_millis: Some(4_000),
                        allocatable_memory_mib: Some(8_192),
                        reserve_cpu_utilisation_ppm: 800_000,
                        reserve_memory_utilisation_ppm: 800_000,
                        reserve_slots_utilisation_ppm: 800_000,
                        observed_running_instances: Some(2),
                        observed_active_bridges: Some(1),
                        observed_memory_mib: Some(1_024),
                        observed_workloads: BTreeMap::new(),
                        observed_workload_memory_mib: BTreeMap::new(),
                        supported_isolation: vec![
                            IsolationProfile::Standard,
                            IsolationProfile::Hardened,
                        ],
                        daemon_addr: "127.0.0.1:7200".to_string(),
                        daemon_server_name: "selium-node-a".to_string(),
                        last_heartbeat_ms: 95,
                    },
                },
            )
            .expect("node");

        let query = engine
            .query(Query::NodesLive {
                now_ms: 100,
                max_staleness_ms: 10,
            })
            .expect("nodes live");
        let nodes = query
            .result
            .get("nodes")
            .and_then(DataValue::as_array)
            .expect("nodes array");
        assert_eq!(
            query.result.get("now_ms").and_then(DataValue::as_u64),
            Some(100)
        );
        assert_eq!(
            query
                .result
                .get("max_staleness_ms")
                .and_then(DataValue::as_u64),
            Some(10)
        );

        let node = nodes
            .iter()
            .find(|node| node.get("name").and_then(DataValue::as_str) == Some("node-a"))
            .expect("node-a record");
        assert_eq!(node.get("name").and_then(DataValue::as_str), Some("node-a"));
        assert_eq!(
            node.get("daemon_addr").and_then(DataValue::as_str),
            Some("127.0.0.1:7200")
        );
        assert_eq!(
            node.get("daemon_server_name").and_then(DataValue::as_str),
            Some("selium-node-a")
        );
        assert_eq!(node.get("live").and_then(DataValue::as_bool), Some(true));
        assert_eq!(node.get("age_ms").and_then(DataValue::as_u64), Some(5));
        assert_eq!(
            node.get("allocatable_cpu_millis")
                .and_then(DataValue::as_u64),
            Some(4_000)
        );
        assert_eq!(
            node.get("allocatable_memory_mib")
                .and_then(DataValue::as_u64),
            Some(8_192)
        );
        let supported_isolation = node
            .get("supported_isolation")
            .and_then(DataValue::as_array)
            .expect("supported isolation array")
            .iter()
            .filter_map(DataValue::as_str)
            .collect::<Vec<_>>();
        assert_eq!(supported_isolation, vec!["Standard", "Hardened"]);
    }

    #[test]
    fn attributed_inventory_query_filters_workloads_modules_pipelines_and_nodes() {
        let mut engine = ControlPlaneEngine::default();
        engine
            .apply_mutation(
                1,
                Mutation::PublishIdl {
                    idl: "package media.pipeline.v1;\n\
                         schema Frame { camera_id: string; }\n\
                         event camera.frames(Frame) { replay: enabled; }"
                        .to_string(),
                },
            )
            .expect("publish idl");

        let account = ExternalAccountRef {
            key: "acct-123".to_string(),
        };
        for (index, workload_name, module) in [
            (2, "ingest", "ingest.wasm"),
            (3, "detector", "detector.wasm"),
        ] {
            engine
                .apply_mutation(
                    index,
                    Mutation::UpsertDeployment {
                        spec: DeploymentSpec {
                            workload: WorkloadRef {
                                tenant: "tenant-a".to_string(),
                                namespace: "media".to_string(),
                                name: workload_name.to_string(),
                            },
                            module: module.to_string(),
                            replicas: 1,
                            contracts: vec![event_contract()],
                            isolation: IsolationProfile::Standard,
                            placement_mode: selium_control_plane_api::PlacementMode::ElasticPack,
                            cpu_millis: 250,
                            memory_mib: 128,
                            ephemeral_storage_mib: 0,
                            bandwidth_profile: BandwidthProfile::Standard,
                            volume_mounts: Vec::new(),
                            external_account_ref: Some(account.clone()),
                        },
                    },
                )
                .expect("deployment");
        }
        engine
            .apply_mutation(
                4,
                Mutation::UpsertDeployment {
                    spec: DeploymentSpec {
                        workload: WorkloadRef {
                            tenant: "tenant-b".to_string(),
                            namespace: "media".to_string(),
                            name: "observer".to_string(),
                        },
                        module: "observer.wasm".to_string(),
                        replicas: 1,
                        contracts: vec![event_contract()],
                        isolation: IsolationProfile::Standard,
                        placement_mode: selium_control_plane_api::PlacementMode::ElasticPack,
                        cpu_millis: 100,
                        memory_mib: 64,
                        ephemeral_storage_mib: 0,
                        bandwidth_profile: BandwidthProfile::Low,
                        volume_mounts: Vec::new(),
                        external_account_ref: Some(ExternalAccountRef {
                            key: "acct-999".to_string(),
                        }),
                    },
                },
            )
            .expect("observer deployment");
        engine
            .apply_mutation(
                5,
                Mutation::UpsertPipeline {
                    spec: PipelineSpec {
                        name: "camera".to_string(),
                        tenant: "tenant-a".to_string(),
                        namespace: "media".to_string(),
                        edges: vec![PipelineEdge {
                            from: PipelineEndpoint {
                                endpoint: EventEndpointRef {
                                    workload: WorkloadRef {
                                        tenant: "tenant-a".to_string(),
                                        namespace: "media".to_string(),
                                        name: "ingest".to_string(),
                                    },
                                    name: "camera.frames".to_string(),
                                },
                                contract: event_contract(),
                            },
                            to: PipelineEndpoint {
                                endpoint: EventEndpointRef {
                                    workload: WorkloadRef {
                                        tenant: "tenant-a".to_string(),
                                        namespace: "media".to_string(),
                                        name: "detector".to_string(),
                                    },
                                    name: "camera.frames".to_string(),
                                },
                                contract: event_contract(),
                            },
                        }],
                        external_account_ref: Some(account.clone()),
                    },
                },
            )
            .expect("pipeline");

        let query = engine
            .query(Query::AttributedInfrastructureInventory {
                filter: AttributedInfrastructureFilter {
                    external_account_ref: Some(account.key.clone()),
                    ..Default::default()
                },
            })
            .expect("inventory query");

        let workloads = query
            .result
            .get("workloads")
            .and_then(DataValue::as_array)
            .expect("workloads");
        assert_eq!(workloads.len(), 2);
        assert!(workloads.iter().all(|workload| {
            workload
                .get("external_account_ref")
                .and_then(DataValue::as_str)
                == Some("acct-123")
        }));

        let modules = query
            .result
            .get("modules")
            .and_then(DataValue::as_array)
            .expect("modules");
        assert_eq!(modules.len(), 2);
        assert!(modules.iter().all(|module| {
            module
                .get("external_account_refs")
                .and_then(DataValue::as_array)
                .is_some_and(|refs| refs.iter().all(|value| value.as_str() == Some("acct-123")))
        }));

        let pipelines = query
            .result
            .get("pipelines")
            .and_then(DataValue::as_array)
            .expect("pipelines");
        assert_eq!(pipelines.len(), 1);
        assert_eq!(
            pipelines[0].get("pipeline").and_then(DataValue::as_str),
            Some("tenant-a/media/camera")
        );

        let nodes = query
            .result
            .get("nodes")
            .and_then(DataValue::as_array)
            .expect("nodes");
        assert_eq!(nodes.len(), 1);
        let scheduled_instances = nodes[0]
            .get("scheduled_instances")
            .and_then(DataValue::as_array)
            .expect("scheduled instances");
        assert_eq!(scheduled_instances.len(), 2);
        assert!(scheduled_instances.iter().all(|instance| {
            instance
                .get("external_account_ref")
                .and_then(DataValue::as_str)
                == Some("acct-123")
        }));

        let snapshot_marker = query
            .result
            .get("snapshot_marker")
            .expect("snapshot marker");
        assert_eq!(
            snapshot_marker
                .get("last_applied")
                .and_then(DataValue::as_u64),
            Some(5)
        );

        let filters = query.result.get("filters").expect("serialized filters");
        assert_eq!(
            filters
                .get("external_account_ref")
                .and_then(DataValue::as_str),
            Some("acct-123")
        );
        assert!(filters.get("workload").is_some());
        assert!(filters.get("module").is_some());
        assert!(filters.get("pipeline").is_some());
        assert!(filters.get("node").is_some());
    }

    #[test]
    fn control_plane_state_query_preserves_resource_fields() {
        let mut engine = ControlPlaneEngine::default();
        engine
            .apply_mutation(
                1,
                Mutation::PublishIdl {
                    idl: "package media.pipeline.v1;\n\
                         schema Frame { camera_id: string; }\n\
                         event camera.frames(Frame) { replay: enabled; }"
                        .to_string(),
                },
            )
            .expect("publish idl");

        let workload = WorkloadRef {
            tenant: "tenant-a".to_string(),
            namespace: "default".to_string(),
            name: "echo".to_string(),
        };
        let external_account_ref = ExternalAccountRef {
            key: "acct-456".to_string(),
        };

        engine
            .apply_mutation(
                2,
                Mutation::UpsertDeployment {
                    spec: DeploymentSpec {
                        workload: workload.clone(),
                        module: "echo.wasm".to_string(),
                        replicas: 1,
                        contracts: vec![event_contract()],
                        isolation: IsolationProfile::Standard,
                        placement_mode: selium_control_plane_api::PlacementMode::ElasticPack,
                        cpu_millis: 750,
                        memory_mib: 512,
                        ephemeral_storage_mib: 64,
                        bandwidth_profile: BandwidthProfile::Low,
                        volume_mounts: vec![VolumeMount {
                            name: "cache".to_string(),
                            mount_path: "/cache".to_string(),
                            read_only: true,
                        }],
                        external_account_ref: Some(external_account_ref.clone()),
                    },
                },
            )
            .expect("deployment");
        engine
            .apply_mutation(
                3,
                Mutation::UpsertPipeline {
                    spec: PipelineSpec {
                        name: "echo".to_string(),
                        tenant: workload.tenant.clone(),
                        namespace: workload.namespace.clone(),
                        edges: vec![PipelineEdge {
                            from: PipelineEndpoint {
                                endpoint: EventEndpointRef {
                                    workload: workload.clone(),
                                    name: "camera.frames".to_string(),
                                },
                                contract: event_contract(),
                            },
                            to: PipelineEndpoint {
                                endpoint: EventEndpointRef {
                                    workload: workload.clone(),
                                    name: "camera.frames".to_string(),
                                },
                                contract: event_contract(),
                            },
                        }],
                        external_account_ref: Some(external_account_ref.clone()),
                    },
                },
            )
            .expect("pipeline");

        let state_bytes = match engine
            .query(Query::ControlPlaneState)
            .expect("state query")
            .result
        {
            DataValue::Bytes(bytes) => bytes,
            other => panic!("expected state bytes, got {other:?}"),
        };
        let state = decode_rkyv::<ControlPlaneState>(&state_bytes).expect("decode state");
        let deployment = state
            .deployments
            .get("tenant-a/default/echo")
            .expect("deployment");

        assert_eq!(deployment.cpu_millis, 750);
        assert_eq!(deployment.memory_mib, 512);
        assert_eq!(deployment.bandwidth_profile, BandwidthProfile::Low);
        assert_eq!(deployment.volume_mounts.len(), 1);
        assert_eq!(
            deployment.external_account_ref,
            Some(external_account_ref.clone())
        );
        assert_eq!(
            state
                .pipelines
                .get("tenant-a/default/echo")
                .and_then(|pipeline| pipeline.external_account_ref.as_ref()),
            Some(&external_account_ref)
        );
    }

    #[test]
    fn encodes_mutation_payload_for_raft_log() {
        let _raft = RaftNode::new(ConsensusConfig::default_for("n1", vec![]), 0);
        let envelope = MutationEnvelope {
            idempotency_key: "id-1".to_string(),
            mutation: Mutation::SetScale {
                workload: WorkloadRef {
                    tenant: "tenant-a".to_string(),
                    namespace: "default".to_string(),
                    name: "echo".to_string(),
                },
                replicas: 2,
            },
        };
        let bytes = ControlPlaneEngine::encode_mutation(&envelope).expect("encode");
        let decoded = ControlPlaneEngine::decode_mutation(&bytes).expect("decode");
        assert_eq!(decoded.idempotency_key, "id-1");
    }

    #[test]
    fn decodes_current_mutation_preserves_live_observed_usage() {
        let bytes = ControlPlaneEngine::encode_mutation(&MutationEnvelope {
            idempotency_key: "id-3".to_string(),
            mutation: Mutation::UpsertNode {
                spec: NodeSpec {
                    name: "node-a".to_string(),
                    capacity_slots: 8,
                    allocatable_cpu_millis: Some(4_000),
                    allocatable_memory_mib: Some(8_192),
                    reserve_cpu_utilisation_ppm: 800_000,
                    reserve_memory_utilisation_ppm: 800_000,
                    reserve_slots_utilisation_ppm: 800_000,
                    observed_running_instances: Some(3),
                    observed_active_bridges: Some(2),
                    observed_memory_mib: Some(2_048),
                    observed_workloads: BTreeMap::from([("tenant-a/default/echo".to_string(), 3)]),
                    observed_workload_memory_mib: BTreeMap::from([(
                        "tenant-a/default/echo".to_string(),
                        2_048u32,
                    )]),
                    supported_isolation: vec![IsolationProfile::Standard],
                    daemon_addr: "127.0.0.1:7100".to_string(),
                    daemon_server_name: "localhost".to_string(),
                    last_heartbeat_ms: 42,
                },
            },
        })
        .expect("encode current mutation");

        let decoded = ControlPlaneEngine::decode_mutation(&bytes).expect("decode mutation");
        let Mutation::UpsertNode { spec } = decoded.mutation else {
            panic!("expected node mutation");
        };

        assert_eq!(decoded.idempotency_key, "id-3");
        assert_eq!(spec.observed_running_instances, Some(3));
        assert_eq!(spec.observed_active_bridges, Some(2));
        assert_eq!(spec.observed_memory_mib, Some(2_048));
        assert_eq!(
            spec.observed_workloads,
            BTreeMap::from([("tenant-a/default/echo".to_string(), 3)])
        );
        assert_eq!(
            spec.observed_workload_memory_mib,
            BTreeMap::from([("tenant-a/default/echo".to_string(), 2_048)])
        );
    }

    #[test]
    fn decodes_current_replayed_mutation_preserves_observed_usage() {
        let bytes = ControlPlaneEngine::encode_mutation(&MutationEnvelope {
            idempotency_key: "id-3".to_string(),
            mutation: Mutation::UpsertNode {
                spec: NodeSpec {
                    name: "node-a".to_string(),
                    capacity_slots: 8,
                    allocatable_cpu_millis: Some(4_000),
                    allocatable_memory_mib: Some(8_192),
                    reserve_cpu_utilisation_ppm: 800_000,
                    reserve_memory_utilisation_ppm: 800_000,
                    reserve_slots_utilisation_ppm: 800_000,
                    observed_running_instances: Some(3),
                    observed_active_bridges: Some(2),
                    observed_memory_mib: Some(2_048),
                    observed_workloads: BTreeMap::from([("tenant-a/default/echo".to_string(), 3)]),
                    observed_workload_memory_mib: BTreeMap::from([(
                        "tenant-a/default/echo".to_string(),
                        2_048u32,
                    )]),
                    supported_isolation: vec![IsolationProfile::Standard],
                    daemon_addr: "127.0.0.1:7100".to_string(),
                    daemon_server_name: "localhost".to_string(),
                    last_heartbeat_ms: 42,
                },
            },
        })
        .expect("encode current mutation");

        let decoded =
            ControlPlaneEngine::decode_replayed_mutation(&bytes).expect("decode mutation");
        let Mutation::UpsertNode { spec } = decoded.mutation else {
            panic!("expected node mutation");
        };

        assert_eq!(decoded.idempotency_key, "id-3");
        assert_eq!(spec.observed_running_instances, Some(3));
        assert_eq!(spec.observed_active_bridges, Some(2));
        assert_eq!(spec.observed_memory_mib, Some(2_048));
        assert_eq!(
            spec.observed_workloads,
            BTreeMap::from([("tenant-a/default/echo".to_string(), 3)])
        );
        assert_eq!(
            spec.observed_workload_memory_mib,
            BTreeMap::from([("tenant-a/default/echo".to_string(), 2_048)])
        );
    }

    #[test]
    fn decodes_current_snapshot_preserves_observed_usage() {
        let bytes = encode_rkyv(&EngineSnapshot {
            control_plane: ControlPlaneState {
                registry: ContractRegistry::default(),
                deployments: BTreeMap::new(),
                pipelines: BTreeMap::new(),
                nodes: BTreeMap::from([(
                    "node-a".to_string(),
                    NodeSpec {
                        name: "node-a".to_string(),
                        capacity_slots: 8,
                        allocatable_cpu_millis: Some(4_000),
                        allocatable_memory_mib: Some(8_192),
                        reserve_cpu_utilisation_ppm: 800_000,
                        reserve_memory_utilisation_ppm: 800_000,
                        reserve_slots_utilisation_ppm: 800_000,
                        observed_running_instances: Some(3),
                        observed_active_bridges: Some(2),
                        observed_memory_mib: Some(2_048),
                        observed_workloads: BTreeMap::from([(
                            "tenant-a/default/echo".to_string(),
                            3,
                        )]),
                        observed_workload_memory_mib: BTreeMap::from([(
                            "tenant-a/default/echo".to_string(),
                            2_048u32,
                        )]),
                        supported_isolation: vec![IsolationProfile::Standard],
                        daemon_addr: "127.0.0.1:7100".to_string(),
                        daemon_server_name: "localhost".to_string(),
                        last_heartbeat_ms: 42,
                    },
                )]),
            },
            tables: TableStore::default(),
            last_applied: 9,
        })
        .expect("encode current snapshot");

        let snapshot = ControlPlaneEngine::decode_snapshot(&bytes).expect("decode snapshot");
        let node = snapshot.control_plane.nodes.get("node-a").expect("node-a");

        assert_eq!(snapshot.last_applied, 9);
        assert_eq!(node.observed_running_instances, Some(3));
        assert_eq!(node.observed_active_bridges, Some(2));
        assert_eq!(node.observed_memory_mib, Some(2_048));
        assert_eq!(
            node.observed_workloads,
            BTreeMap::from([("tenant-a/default/echo".to_string(), 3)])
        );
        assert_eq!(
            node.observed_workload_memory_mib,
            BTreeMap::from([("tenant-a/default/echo".to_string(), 2_048)])
        );
    }

    #[test]
    fn discovery_state_filters_tenant_and_namespace_collisions() {
        let engine = discovery_engine();
        let query = engine
            .query(Query::DiscoveryState {
                scope: DiscoveryCapabilityScope {
                    operations: vec![DiscoveryOperation::Discover],
                    workloads: vec![DiscoveryPattern::Exact("tenant-a/media/ingest".to_string())],
                    endpoints: Vec::new(),
                    allow_operational_processes: false,
                },
            })
            .expect("discovery state");
        let DataValue::Bytes(bytes) = &query.result else {
            panic!("expected bytes result");
        };
        let discovery: DiscoveryState = decode_rkyv(bytes).expect("decode discovery");

        assert_eq!(discovery.workloads.len(), 1);
        assert_eq!(discovery.endpoints.len(), 3);
        assert_eq!(
            discovery.workloads[0].workload.key(),
            "tenant-a/media/ingest"
        );
        assert_eq!(
            discovery
                .endpoints
                .iter()
                .map(|record| record.endpoint.key())
                .collect::<Vec<_>>(),
            vec![
                "tenant-a/media/ingest#event:camera.frames".to_string(),
                "tenant-a/media/ingest#event:stderr".to_string(),
                "tenant-a/media/ingest#event:stdout".to_string(),
            ]
        );
    }

    #[test]
    fn resolve_endpoint_rejects_denied_bind() {
        let engine = discovery_engine();
        let err = engine
            .query(Query::ResolveDiscovery {
                request: DiscoveryRequest {
                    operation: DiscoveryOperation::Bind,
                    target: DiscoveryTarget::Endpoint(PublicEndpointRef {
                        workload: WorkloadRef {
                            tenant: "tenant-a".to_string(),
                            namespace: "media".to_string(),
                            name: "ingest".to_string(),
                        },
                        kind: ContractKind::Event,
                        name: "camera.frames".to_string(),
                    }),
                    scope: DiscoveryCapabilityScope {
                        operations: vec![DiscoveryOperation::Bind],
                        workloads: vec![DiscoveryPattern::Exact(
                            "tenant-b/media/ingest".to_string(),
                        )],
                        endpoints: Vec::new(),
                        allow_operational_processes: false,
                    },
                },
            })
            .expect_err("bind should be denied");

        assert!(err.to_string().contains("unauthorised"));
    }

    #[test]
    fn resolve_workload_discovery_omits_replica_identity() {
        let engine = discovery_engine();
        let query = engine
            .query(Query::ResolveDiscovery {
                request: DiscoveryRequest {
                    operation: DiscoveryOperation::Discover,
                    target: DiscoveryTarget::Workload(WorkloadRef {
                        tenant: "tenant-a".to_string(),
                        namespace: "media".to_string(),
                        name: "ingest".to_string(),
                    }),
                    scope: DiscoveryCapabilityScope {
                        operations: vec![DiscoveryOperation::Discover],
                        workloads: vec![DiscoveryPattern::Exact(
                            "tenant-a/media/ingest".to_string(),
                        )],
                        endpoints: Vec::new(),
                        allow_operational_processes: false,
                    },
                },
            })
            .expect("resolve workload");
        let DataValue::Bytes(bytes) = &query.result else {
            panic!("expected bytes result");
        };
        let resolution: DiscoveryResolution = decode_rkyv(bytes).expect("decode resolution");

        assert_eq!(
            resolution,
            DiscoveryResolution::Workload(ResolvedWorkload {
                workload: WorkloadRef {
                    tenant: "tenant-a".to_string(),
                    namespace: "media".to_string(),
                    name: "ingest".to_string(),
                },
                endpoints: vec![
                    selium_control_plane_api::DiscoverableEndpoint {
                        endpoint: PublicEndpointRef {
                            workload: WorkloadRef {
                                tenant: "tenant-a".to_string(),
                                namespace: "media".to_string(),
                                name: "ingest".to_string(),
                            },
                            kind: ContractKind::Event,
                            name: "camera.frames".to_string(),
                        },
                        contract: Some(event_contract()),
                    },
                    selium_control_plane_api::DiscoverableEndpoint {
                        endpoint: PublicEndpointRef {
                            workload: WorkloadRef {
                                tenant: "tenant-a".to_string(),
                                namespace: "media".to_string(),
                                name: "ingest".to_string(),
                            },
                            kind: ContractKind::Event,
                            name: GUEST_LOG_STDERR_ENDPOINT.to_string(),
                        },
                        contract: None,
                    },
                    selium_control_plane_api::DiscoverableEndpoint {
                        endpoint: PublicEndpointRef {
                            workload: WorkloadRef {
                                tenant: "tenant-a".to_string(),
                                namespace: "media".to_string(),
                                name: "ingest".to_string(),
                            },
                            kind: ContractKind::Event,
                            name: GUEST_LOG_STDOUT_ENDPOINT.to_string(),
                        },
                        contract: None,
                    }
                ],
            })
        );
    }

    #[test]
    fn resolve_event_endpoint_bind_omits_replica_identity() {
        let engine = discovery_engine();
        let query = engine
            .query(Query::ResolveDiscovery {
                request: DiscoveryRequest {
                    operation: DiscoveryOperation::Bind,
                    target: DiscoveryTarget::Endpoint(PublicEndpointRef {
                        workload: WorkloadRef {
                            tenant: "tenant-a".to_string(),
                            namespace: "media".to_string(),
                            name: "ingest".to_string(),
                        },
                        kind: ContractKind::Event,
                        name: "camera.frames".to_string(),
                    }),
                    scope: DiscoveryCapabilityScope {
                        operations: vec![DiscoveryOperation::Bind],
                        workloads: vec![DiscoveryPattern::Exact(
                            "tenant-a/media/ingest".to_string(),
                        )],
                        endpoints: Vec::new(),
                        allow_operational_processes: false,
                    },
                },
            })
            .expect("resolve endpoint bind");
        let DataValue::Bytes(bytes) = &query.result else {
            panic!("expected bytes result");
        };
        let resolution: DiscoveryResolution = decode_rkyv(bytes).expect("decode resolution");

        assert_eq!(
            resolution,
            DiscoveryResolution::Endpoint(ResolvedEndpoint {
                endpoint: PublicEndpointRef {
                    workload: WorkloadRef {
                        tenant: "tenant-a".to_string(),
                        namespace: "media".to_string(),
                        name: "ingest".to_string(),
                    },
                    kind: ContractKind::Event,
                    name: "camera.frames".to_string(),
                },
                contract: Some(event_contract()),
            })
        );
    }

    #[test]
    fn discovery_state_lists_event_service_and_stream_endpoints() {
        let engine = multi_kind_discovery_engine();
        let query = engine
            .query(Query::DiscoveryState {
                scope: DiscoveryCapabilityScope {
                    operations: vec![DiscoveryOperation::Discover],
                    workloads: vec![DiscoveryPattern::Exact("tenant-a/media/router".to_string())],
                    endpoints: Vec::new(),
                    allow_operational_processes: false,
                },
            })
            .expect("discovery state");
        let DataValue::Bytes(bytes) = &query.result else {
            panic!("expected bytes result");
        };
        let discovery: DiscoveryState = decode_rkyv(bytes).expect("decode discovery");
        let endpoint_keys = discovery
            .endpoints
            .iter()
            .map(|record| record.endpoint.key())
            .collect::<Vec<_>>();

        assert_eq!(discovery.workloads.len(), 1);
        assert_eq!(
            endpoint_keys,
            vec![
                "tenant-a/media/router#event:camera.frames".to_string(),
                "tenant-a/media/router#event:stderr".to_string(),
                "tenant-a/media/router#event:stdout".to_string(),
                "tenant-a/media/router#service:camera.detect".to_string(),
                "tenant-a/media/router#stream:camera.raw".to_string(),
            ]
        );
    }

    #[test]
    fn resolve_service_endpoint_bind_omits_replica_identity() {
        let engine = multi_kind_discovery_engine();
        let query = engine
            .query(Query::ResolveDiscovery {
                request: DiscoveryRequest {
                    operation: DiscoveryOperation::Bind,
                    target: DiscoveryTarget::Endpoint(PublicEndpointRef {
                        workload: WorkloadRef {
                            tenant: "tenant-a".to_string(),
                            namespace: "media".to_string(),
                            name: "router".to_string(),
                        },
                        kind: ContractKind::Service,
                        name: "camera.detect".to_string(),
                    }),
                    scope: DiscoveryCapabilityScope {
                        operations: vec![DiscoveryOperation::Bind],
                        workloads: vec![DiscoveryPattern::Exact(
                            "tenant-a/media/router".to_string(),
                        )],
                        endpoints: Vec::new(),
                        allow_operational_processes: false,
                    },
                },
            })
            .expect("resolve service endpoint bind");
        let DataValue::Bytes(bytes) = &query.result else {
            panic!("expected bytes result");
        };
        let resolution: DiscoveryResolution = decode_rkyv(bytes).expect("decode resolution");

        assert_eq!(
            resolution,
            DiscoveryResolution::Endpoint(ResolvedEndpoint {
                endpoint: PublicEndpointRef {
                    workload: WorkloadRef {
                        tenant: "tenant-a".to_string(),
                        namespace: "media".to_string(),
                        name: "router".to_string(),
                    },
                    kind: ContractKind::Service,
                    name: "camera.detect".to_string(),
                },
                contract: Some(service_contract()),
            })
        );
    }

    #[test]
    fn resolve_stream_endpoint_bind_omits_replica_identity() {
        let engine = multi_kind_discovery_engine();
        let query = engine
            .query(Query::ResolveDiscovery {
                request: DiscoveryRequest {
                    operation: DiscoveryOperation::Bind,
                    target: DiscoveryTarget::Endpoint(PublicEndpointRef {
                        workload: WorkloadRef {
                            tenant: "tenant-a".to_string(),
                            namespace: "media".to_string(),
                            name: "router".to_string(),
                        },
                        kind: ContractKind::Stream,
                        name: "camera.raw".to_string(),
                    }),
                    scope: DiscoveryCapabilityScope {
                        operations: vec![DiscoveryOperation::Bind],
                        workloads: vec![DiscoveryPattern::Exact(
                            "tenant-a/media/router".to_string(),
                        )],
                        endpoints: Vec::new(),
                        allow_operational_processes: false,
                    },
                },
            })
            .expect("resolve stream endpoint bind");
        let DataValue::Bytes(bytes) = &query.result else {
            panic!("expected bytes result");
        };
        let resolution: DiscoveryResolution = decode_rkyv(bytes).expect("decode resolution");

        assert_eq!(
            resolution,
            DiscoveryResolution::Endpoint(ResolvedEndpoint {
                endpoint: PublicEndpointRef {
                    workload: WorkloadRef {
                        tenant: "tenant-a".to_string(),
                        namespace: "media".to_string(),
                        name: "router".to_string(),
                    },
                    kind: ContractKind::Stream,
                    name: "camera.raw".to_string(),
                },
                contract: Some(stream_contract()),
            })
        );
    }

    #[test]
    fn resolve_same_name_cross_kind_endpoints_uses_explicit_contract_kind() {
        let engine = same_name_multi_kind_discovery_engine();

        for kind in [
            ContractKind::Event,
            ContractKind::Service,
            ContractKind::Stream,
        ] {
            let query = engine
                .query(Query::ResolveDiscovery {
                    request: DiscoveryRequest {
                        operation: DiscoveryOperation::Bind,
                        target: DiscoveryTarget::Endpoint(PublicEndpointRef {
                            workload: WorkloadRef {
                                tenant: "tenant-a".to_string(),
                                namespace: "media".to_string(),
                                name: "router".to_string(),
                            },
                            kind,
                            name: "camera.shared".to_string(),
                        }),
                        scope: DiscoveryCapabilityScope {
                            operations: vec![DiscoveryOperation::Bind],
                            workloads: vec![DiscoveryPattern::Exact(
                                "tenant-a/media/router".to_string(),
                            )],
                            endpoints: Vec::new(),
                            allow_operational_processes: false,
                        },
                    },
                })
                .expect("resolve endpoint bind");
            let DataValue::Bytes(bytes) = &query.result else {
                panic!("expected bytes result");
            };
            let resolution: DiscoveryResolution = decode_rkyv(bytes).expect("decode resolution");

            assert_eq!(
                resolution,
                DiscoveryResolution::Endpoint(ResolvedEndpoint {
                    endpoint: PublicEndpointRef {
                        workload: WorkloadRef {
                            tenant: "tenant-a".to_string(),
                            namespace: "media".to_string(),
                            name: "router".to_string(),
                        },
                        kind,
                        name: "camera.shared".to_string(),
                    },
                    contract: Some(shared_name_contract(kind)),
                })
            );
        }
    }

    #[test]
    fn discovery_state_prefix_filters_guest_std_endpoints() {
        let engine = discovery_engine();
        let query = engine
            .query(Query::DiscoveryState {
                scope: DiscoveryCapabilityScope {
                    operations: vec![DiscoveryOperation::Discover],
                    workloads: vec![DiscoveryPattern::Prefix(String::new())],
                    endpoints: vec![DiscoveryPattern::Prefix(
                        "tenant-a/media/ingest#event:std".to_string(),
                    )],
                    allow_operational_processes: false,
                },
            })
            .expect("discovery state");
        let DataValue::Bytes(bytes) = &query.result else {
            panic!("expected bytes result");
        };
        let discovery: DiscoveryState = decode_rkyv(bytes).expect("decode discovery");

        assert_eq!(
            discovery
                .endpoints
                .iter()
                .map(|record| record.endpoint.key())
                .collect::<Vec<_>>(),
            vec![
                "tenant-a/media/ingest#event:stderr".to_string(),
                "tenant-a/media/ingest#event:stdout".to_string(),
            ]
        );
    }

    #[test]
    fn resolve_guest_log_endpoint_bind_omits_replica_identity() {
        let engine = discovery_engine();
        let query = engine
            .query(Query::ResolveDiscovery {
                request: DiscoveryRequest {
                    operation: DiscoveryOperation::Bind,
                    target: DiscoveryTarget::Endpoint(PublicEndpointRef {
                        workload: WorkloadRef {
                            tenant: "tenant-a".to_string(),
                            namespace: "media".to_string(),
                            name: "ingest".to_string(),
                        },
                        kind: ContractKind::Event,
                        name: GUEST_LOG_STDOUT_ENDPOINT.to_string(),
                    }),
                    scope: DiscoveryCapabilityScope {
                        operations: vec![DiscoveryOperation::Bind],
                        workloads: vec![DiscoveryPattern::Exact(
                            "tenant-a/media/ingest".to_string(),
                        )],
                        endpoints: Vec::new(),
                        allow_operational_processes: false,
                    },
                },
            })
            .expect("resolve guest log endpoint bind");
        let DataValue::Bytes(bytes) = &query.result else {
            panic!("expected bytes result");
        };
        let resolution: DiscoveryResolution = decode_rkyv(bytes).expect("decode resolution");

        assert_eq!(
            resolution,
            DiscoveryResolution::Endpoint(ResolvedEndpoint {
                endpoint: PublicEndpointRef {
                    workload: WorkloadRef {
                        tenant: "tenant-a".to_string(),
                        namespace: "media".to_string(),
                        name: "ingest".to_string(),
                    },
                    kind: ContractKind::Event,
                    name: GUEST_LOG_STDOUT_ENDPOINT.to_string(),
                },
                contract: None,
            })
        );
    }

    #[test]
    fn resolve_unknown_workload_reports_missing_name() {
        let engine = discovery_engine();
        let err = engine
            .query(Query::ResolveDiscovery {
                request: DiscoveryRequest {
                    operation: DiscoveryOperation::Discover,
                    target: DiscoveryTarget::Workload(WorkloadRef {
                        tenant: "tenant-a".to_string(),
                        namespace: "media".to_string(),
                        name: "missing".to_string(),
                    }),
                    scope: DiscoveryCapabilityScope {
                        operations: vec![DiscoveryOperation::Discover],
                        workloads: vec![DiscoveryPattern::Exact(
                            "tenant-a/media/missing".to_string(),
                        )],
                        endpoints: Vec::new(),
                        allow_operational_processes: false,
                    },
                },
            })
            .expect_err("unknown workload should be rejected");

        assert!(err.to_string().contains("unknown deployment"));
        assert!(err.to_string().contains("tenant-a/media/missing"));
    }

    #[test]
    fn resolve_unknown_endpoint_reports_missing_name() {
        let engine = discovery_engine();
        let err = engine
            .query(Query::ResolveDiscovery {
                request: DiscoveryRequest {
                    operation: DiscoveryOperation::Bind,
                    target: DiscoveryTarget::Endpoint(PublicEndpointRef {
                        workload: WorkloadRef {
                            tenant: "tenant-a".to_string(),
                            namespace: "media".to_string(),
                            name: "ingest".to_string(),
                        },
                        kind: ContractKind::Event,
                        name: "camera.missing".to_string(),
                    }),
                    scope: DiscoveryCapabilityScope {
                        operations: vec![DiscoveryOperation::Bind],
                        workloads: vec![DiscoveryPattern::Exact(
                            "tenant-a/media/ingest".to_string(),
                        )],
                        endpoints: Vec::new(),
                        allow_operational_processes: false,
                    },
                },
            })
            .expect_err("unknown endpoint should be rejected");

        assert!(err.to_string().contains("unknown endpoint"));
        assert!(
            err.to_string()
                .contains("tenant-a/media/ingest#event:camera.missing")
        );
    }

    #[test]
    fn running_process_bind_target_is_rejected() {
        let engine = discovery_engine();
        let err = engine
            .query(Query::ResolveDiscovery {
                request: DiscoveryRequest {
                    operation: DiscoveryOperation::Bind,
                    target: DiscoveryTarget::RunningProcess(
                        OperationalProcessSelector::ReplicaKey(
                            "tenant=tenant-a;namespace=media;workload=ingest;replica=0".to_string(),
                        ),
                    ),
                    scope: DiscoveryCapabilityScope::allow_all(),
                },
            })
            .expect_err("running process bind rejected");

        assert!(err.to_string().contains("operational-only"));
    }

    #[test]
    fn running_process_discovery_reports_ambiguity() {
        let engine = discovery_engine();
        let err = engine
            .query(Query::ResolveDiscovery {
                request: DiscoveryRequest {
                    operation: DiscoveryOperation::Discover,
                    target: DiscoveryTarget::RunningProcess(OperationalProcessSelector::Workload(
                        WorkloadRef {
                            tenant: "tenant-a".to_string(),
                            namespace: "media".to_string(),
                            name: "ingest".to_string(),
                        },
                    )),
                    scope: DiscoveryCapabilityScope {
                        operations: vec![DiscoveryOperation::Discover],
                        workloads: vec![DiscoveryPattern::Exact(
                            "tenant-a/media/ingest".to_string(),
                        )],
                        endpoints: Vec::new(),
                        allow_operational_processes: true,
                    },
                },
            })
            .expect_err("ambiguous running process");

        assert!(err.to_string().contains("specify a replica key"));
    }

    #[test]
    fn running_process_discovery_keeps_operational_replica_identity() {
        let engine = discovery_engine();
        let query = engine
            .query(Query::ResolveDiscovery {
                request: DiscoveryRequest {
                    operation: DiscoveryOperation::Discover,
                    target: DiscoveryTarget::RunningProcess(
                        OperationalProcessSelector::ReplicaKey(
                            "tenant=tenant-a;namespace=media;workload=ingest;replica=0".to_string(),
                        ),
                    ),
                    scope: DiscoveryCapabilityScope {
                        operations: vec![DiscoveryOperation::Discover],
                        workloads: vec![DiscoveryPattern::Exact(
                            "tenant-a/media/ingest".to_string(),
                        )],
                        endpoints: Vec::new(),
                        allow_operational_processes: true,
                    },
                },
            })
            .expect("resolve running process");
        let DataValue::Bytes(bytes) = &query.result else {
            panic!("expected bytes result");
        };
        let resolution: DiscoveryResolution = decode_rkyv(bytes).expect("decode resolution");

        let DiscoveryResolution::RunningProcess(record) = resolution else {
            panic!("expected running-process resolution");
        };
        assert_eq!(record.workload.key(), "tenant-a/media/ingest");
        assert_eq!(
            record.replica_key,
            "tenant=tenant-a;namespace=media;workload=ingest;replica=0"
        );
        assert!(!record.node.is_empty());
    }
}
