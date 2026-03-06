//! Deterministic control-plane state machine built on host-managed consensus + tables.

use std::collections::BTreeMap;

use rkyv::{Archive, Deserialize, Serialize};
use selium_abi::{DataValue, decode_rkyv, encode_rkyv};
use selium_control_plane_api::{
    ApiError, ControlPlaneState, DeploymentSpec, NodeSpec, PipelineSpec,
    ensure_pipeline_consistency, parse_idl,
};
use selium_control_plane_scheduler::{build_plan, deployment_contract_usage};
use selium_io_consensus::LogEntry;
use selium_io_tables::{TableApplyResult, TableCommand, TableError, TableRecord, TableStore};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum Mutation {
    PublishIdl { idl: String },
    UpsertDeployment { spec: DeploymentSpec },
    UpsertPipeline { spec: PipelineSpec },
    UpsertNode { spec: NodeSpec },
    SetScale { app: String, replicas: u32 },
    Table { command: TableCommand },
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum Query {
    TableGet { table: String, key: String },
    TableScan { table: String, limit: usize },
    ViewGet { name: String },
    ControlPlaneState,
    ControlPlaneSummary,
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
            Mutation::SetScale { app, replicas } => {
                self.control_plane.set_scale(&app, replicas)?;
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
    use selium_io_consensus::{ConsensusConfig, RaftNode};

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
                        app: "echo".to_string(),
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
                app: "echo".to_string(),
                replicas: 2,
            },
        };
        let bytes = ControlPlaneEngine::encode_mutation(&envelope).expect("encode");
        let decoded = ControlPlaneEngine::decode_mutation(&bytes).expect("decode");
        assert_eq!(decoded.idempotency_key, "id-1");
    }
}
