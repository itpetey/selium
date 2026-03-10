//! Deterministic control-plane state machine built on host-managed consensus + tables.

use std::collections::BTreeMap;

use rkyv::{Archive, Deserialize, Serialize};
use selium_abi::{DataValue, decode_rkyv, encode_rkyv};
use selium_control_plane_api::{
    ApiError, ControlPlaneState, DeploymentSpec, DiscoveryCapabilityScope, DiscoveryOperation,
    DiscoveryRequest, DiscoveryResolution, DiscoveryState, DiscoveryTarget, NodeSpec,
    OperationalProcessRecord, OperationalProcessSelector, PipelineSpec, ResolvedEndpoint,
    ResolvedWorkload, WorkloadRef, build_discovery_state, ensure_pipeline_consistency, parse_idl,
};
use selium_control_plane_scheduler::{build_plan, deployment_contract_usage};
use selium_io_consensus::LogEntry;
use selium_io_tables::{TableApplyResult, TableCommand, TableError, TableRecord, TableStore};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum Mutation {
    PublishIdl {
        idl: String,
    },
    UpsertDeployment {
        spec: DeploymentSpec,
    },
    UpsertPipeline {
        spec: PipelineSpec,
    },
    UpsertNode {
        spec: NodeSpec,
    },
    SetScale {
        workload: WorkloadRef,
        replicas: u32,
    },
    Table {
        command: TableCommand,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum Query {
    TableGet { table: String, key: String },
    TableScan { table: String, limit: usize },
    ViewGet { name: String },
    ControlPlaneState,
    ControlPlaneSummary,
    DiscoveryState { scope: DiscoveryCapabilityScope },
    ResolveDiscovery { request: DiscoveryRequest },
    NodesLive { now_ms: u64, max_staleness_ms: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct MutationEnvelope {
    pub idempotency_key: String,
    pub mutation: Mutation,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct MutationResponse {
    pub index: u64,
    pub result: DataValue,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct QueryResponse {
    pub result: DataValue,
}

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

    pub fn decode_mutation(bytes: &[u8]) -> Result<MutationEnvelope, RuntimeError> {
        decode_rkyv(bytes).map_err(|err| RuntimeError::InvalidLogPayload(err.to_string()))
    }

    pub fn apply_committed_entry(
        &mut self,
        entry: &LogEntry,
    ) -> Result<MutationResponse, RuntimeError> {
        let envelope = Self::decode_mutation(&entry.payload)?;
        let result = self.apply_mutation(entry.index, envelope.mutation)?;
        self.last_applied = self.last_applied.max(entry.index);
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
        match mutation {
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
        }
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
                                .keys()
                                .cloned()
                                .map(DataValue::from)
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
                        "nodes".to_string(),
                        DataValue::List(
                            self.control_plane
                                .nodes
                                .keys()
                                .cloned()
                                .map(DataValue::from)
                                .collect(),
                        ),
                    ),
                    (
                        "schedule".to_string(),
                        DataValue::Map(BTreeMap::from([
                            (
                                "instances".to_string(),
                                DataValue::from(format!("{:?}", plan.instances)),
                            ),
                            (
                                "node_slots".to_string(),
                                DataValue::from(format!("{:?}", plan.node_slots)),
                            ),
                        ])),
                    ),
                    (
                        "contract_usage".to_string(),
                        DataValue::from(format!("{:?}", usage)),
                    ),
                ]))
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
                                "supported_isolation".to_string(),
                                DataValue::from(format!("{:?}", node.supported_isolation)),
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

fn authorised_discovery_state(
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

fn resolve_discovery(
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

fn ok_status() -> DataValue {
    DataValue::Map(BTreeMap::from([(
        "status".to_string(),
        DataValue::from("ok"),
    )]))
}

fn serialize_table_record(record: &TableRecord) -> DataValue {
    DataValue::Map(BTreeMap::from([
        ("value".to_string(), record.value.clone()),
        ("version".to_string(), DataValue::from(record.version)),
        (
            "updated_index".to_string(),
            DataValue::from(record.updated_index),
        ),
    ]))
}

fn serialize_table_result(result: TableApplyResult) -> DataValue {
    match result {
        TableApplyResult::Updated {
            table,
            key,
            version,
        } => DataValue::Map(BTreeMap::from([
            ("status".to_string(), DataValue::from("updated")),
            ("table".to_string(), DataValue::from(table)),
            ("key".to_string(), DataValue::from(key)),
            ("version".to_string(), DataValue::from(version)),
        ])),
        TableApplyResult::Deleted { table, key } => DataValue::Map(BTreeMap::from([
            ("status".to_string(), DataValue::from("deleted")),
            ("table".to_string(), DataValue::from(table)),
            ("key".to_string(), DataValue::from(key)),
        ])),
        TableApplyResult::ViewCreated { name } => DataValue::Map(BTreeMap::from([(
            "status".to_string(),
            DataValue::from(format!("view_created:{name}")),
        )])),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use selium_control_plane_api::{
        ContractKind, ContractRef, DiscoveryPattern, IsolationProfile, PublicEndpointRef, parse_idl,
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
                    },
                },
            )
            .expect("upsert");

        let query = engine.query(Query::ControlPlaneSummary).expect("summary");
        assert!(
            query
                .result
                .get("deployments")
                .and_then(DataValue::as_array)
                .is_some()
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
        assert_eq!(discovery.endpoints.len(), 1);
        assert_eq!(
            discovery.workloads[0].workload.key(),
            "tenant-a/media/ingest"
        );
        assert_eq!(
            discovery.endpoints[0].endpoint.key(),
            "tenant-a/media/ingest#event:camera.frames"
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
                endpoints: vec![selium_control_plane_api::DiscoverableEndpoint {
                    endpoint: PublicEndpointRef {
                        workload: WorkloadRef {
                            tenant: "tenant-a".to_string(),
                            namespace: "media".to_string(),
                            name: "ingest".to_string(),
                        },
                        kind: ContractKind::Event,
                        name: "camera.frames".to_string(),
                    },
                    contract: event_contract(),
                }],
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
                contract: event_contract(),
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
                contract: service_contract(),
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
                contract: stream_contract(),
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
                    contract: shared_name_contract(kind),
                })
            );
        }
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
