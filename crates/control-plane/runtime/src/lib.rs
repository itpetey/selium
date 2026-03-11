//! Deterministic control-plane state machine built on host-managed consensus + tables.

use std::collections::{BTreeMap, BTreeSet};

use rkyv::{Archive, Deserialize, Serialize};
use selium_abi::{DataValue, decode_rkyv, encode_rkyv};
use selium_control_plane_api::{
    ApiError, ControlPlaneState, DeploymentSpec, DiscoveryCapabilityScope, DiscoveryOperation,
    DiscoveryRequest, DiscoveryResolution, DiscoveryState, DiscoveryTarget, NodeSpec,
    OperationalProcessRecord, OperationalProcessSelector, PipelineSpec, ResolvedEndpoint,
    ResolvedWorkload, WorkloadRef, build_discovery_state, ensure_pipeline_consistency, parse_idl,
};
use selium_control_plane_scheduler::{SchedulePlan, build_plan, deployment_contract_usage};
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
    TableGet {
        table: String,
        key: String,
    },
    TableScan {
        table: String,
        limit: usize,
    },
    ViewGet {
        name: String,
    },
    ControlPlaneState,
    ControlPlaneSummary,
    /// Return attributed workload, module, pipeline, and node inventory for external consumers.
    AttributedInfrastructureInventory {
        filter: AttributedInfrastructureFilter,
    },
    DiscoveryState {
        scope: DiscoveryCapabilityScope,
    },
    ResolveDiscovery {
        request: DiscoveryRequest,
    },
    NodesLive {
        now_ms: u64,
        max_staleness_ms: u64,
    },
}

/// Exact-match filters for attributed infrastructure inventory queries.
#[derive(Debug, Clone, Default, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct AttributedInfrastructureFilter {
    pub external_account_ref: Option<String>,
    pub workload: Option<String>,
    pub module: Option<String>,
    pub pipeline: Option<String>,
    pub node: Option<String>,
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

fn serialize_deployment_spec(spec: &DeploymentSpec) -> DataValue {
    DataValue::Map(BTreeMap::from([
        ("workload".to_string(), DataValue::from(spec.workload.key())),
        ("module".to_string(), DataValue::from(spec.module.clone())),
        ("replicas".to_string(), DataValue::from(spec.replicas)),
        (
            "external_account_ref".to_string(),
            serialize_external_account_ref(&spec.external_account_ref),
        ),
        (
            "isolation".to_string(),
            DataValue::from(format!("{:?}", spec.isolation)),
        ),
        (
            "resources".to_string(),
            serialize_deployment_resources(spec),
        ),
    ]))
}

fn serialize_deployment_resources(spec: &DeploymentSpec) -> DataValue {
    DataValue::Map(BTreeMap::from([
        ("cpu_millis".to_string(), DataValue::from(spec.cpu_millis)),
        ("memory_mib".to_string(), DataValue::from(spec.memory_mib)),
        (
            "ephemeral_storage_mib".to_string(),
            DataValue::from(spec.ephemeral_storage_mib),
        ),
        (
            "bandwidth_profile".to_string(),
            DataValue::from(spec.bandwidth_profile.as_str()),
        ),
        (
            "volume_mounts".to_string(),
            DataValue::List(
                spec.volume_mounts
                    .iter()
                    .map(|mount| {
                        DataValue::Map(BTreeMap::from([
                            ("name".to_string(), DataValue::from(mount.name.clone())),
                            (
                                "mount_path".to_string(),
                                DataValue::from(mount.mount_path.clone()),
                            ),
                            ("read_only".to_string(), DataValue::from(mount.read_only)),
                        ]))
                    })
                    .collect(),
            ),
        ),
    ]))
}

fn serialize_pipeline_spec(spec: &PipelineSpec) -> DataValue {
    DataValue::Map(BTreeMap::from([
        ("pipeline".to_string(), DataValue::from(spec.key())),
        (
            "external_account_ref".to_string(),
            serialize_external_account_ref(&spec.external_account_ref),
        ),
        ("edge_count".to_string(), DataValue::from(spec.edges.len())),
    ]))
}

fn serialize_node_spec(spec: &NodeSpec) -> DataValue {
    DataValue::Map(BTreeMap::from([
        ("name".to_string(), DataValue::from(spec.name.clone())),
        (
            "capacity_slots".to_string(),
            DataValue::from(spec.capacity_slots),
        ),
        (
            "allocatable_cpu_millis".to_string(),
            serialize_optional_u32(spec.allocatable_cpu_millis),
        ),
        (
            "allocatable_memory_mib".to_string(),
            serialize_optional_u32(spec.allocatable_memory_mib),
        ),
        (
            "supported_isolation".to_string(),
            serialize_isolation_profiles(&spec.supported_isolation),
        ),
        (
            "daemon_addr".to_string(),
            DataValue::from(spec.daemon_addr.clone()),
        ),
        (
            "daemon_server_name".to_string(),
            DataValue::from(spec.daemon_server_name.clone()),
        ),
        (
            "last_heartbeat_ms".to_string(),
            DataValue::from(spec.last_heartbeat_ms),
        ),
    ]))
}

fn serialize_schedule_plan(plan: &SchedulePlan) -> DataValue {
    DataValue::Map(BTreeMap::from([
        (
            "instances".to_string(),
            DataValue::List(
                plan.instances
                    .iter()
                    .map(|instance| {
                        DataValue::Map(BTreeMap::from([
                            (
                                "deployment".to_string(),
                                DataValue::from(instance.deployment.clone()),
                            ),
                            (
                                "instance_id".to_string(),
                                DataValue::from(instance.instance_id.clone()),
                            ),
                            ("node".to_string(), DataValue::from(instance.node.clone())),
                            (
                                "isolation".to_string(),
                                DataValue::from(format!("{:?}", instance.isolation)),
                            ),
                        ]))
                    })
                    .collect(),
            ),
        ),
        (
            "node_slots".to_string(),
            serialize_string_u32_map(&plan.node_slots),
        ),
        (
            "node_cpu_millis".to_string(),
            serialize_string_u32_map(&plan.node_cpu_millis),
        ),
        (
            "node_memory_mib".to_string(),
            serialize_string_u32_map(&plan.node_memory_mib),
        ),
    ]))
}

#[derive(Debug, Clone)]
struct InventoryInstance {
    deployment: String,
    workload: String,
    module: String,
    external_account_ref: Option<String>,
    node: String,
    instance_id: String,
    isolation: String,
}

#[derive(Debug, Default)]
struct ModuleInventory {
    deployments: usize,
    workloads: BTreeSet<String>,
    external_account_refs: BTreeSet<String>,
    nodes: BTreeSet<String>,
}

fn serialize_attributed_infrastructure_inventory(
    state: &ControlPlaneState,
    plan: &SchedulePlan,
    filter: &AttributedInfrastructureFilter,
    last_applied: u64,
) -> DataValue {
    let instances = build_inventory_instances(state, plan);
    let mut instances_by_workload = BTreeMap::<String, Vec<InventoryInstance>>::new();
    let mut instances_by_node = BTreeMap::<String, Vec<InventoryInstance>>::new();
    for instance in instances {
        instances_by_workload
            .entry(instance.workload.clone())
            .or_default()
            .push(instance.clone());
        instances_by_node
            .entry(instance.node.clone())
            .or_default()
            .push(instance);
    }

    let pipeline_scope = if let Some(pipeline_key) = filter.pipeline.as_deref() {
        Some(
            state
                .pipelines
                .get(pipeline_key)
                .map(pipeline_workload_keys)
                .unwrap_or_default(),
        )
    } else {
        None
    };

    let workloads = state
        .deployments
        .values()
        .filter(|deployment| {
            let scheduled = instances_by_workload
                .get(&deployment.workload.key())
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            workload_matches_filter(deployment, scheduled, filter, pipeline_scope.as_ref())
        })
        .collect::<Vec<_>>();

    let workload_values = workloads
        .iter()
        .map(|deployment| {
            let scheduled = instances_by_workload
                .get(&deployment.workload.key())
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            serialize_inventory_workload(deployment, scheduled)
        })
        .collect::<Vec<_>>();

    let mut modules = BTreeMap::<String, ModuleInventory>::new();
    for deployment in &workloads {
        let scheduled = instances_by_workload
            .get(&deployment.workload.key())
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let module = modules.entry(deployment.module.clone()).or_default();
        module.deployments += 1;
        module.workloads.insert(deployment.workload.key());
        if let Some(reference) = &deployment.external_account_ref {
            module.external_account_refs.insert(reference.key.clone());
        }
        for instance in scheduled {
            module.nodes.insert(instance.node.clone());
        }
    }

    let pipeline_values = state
        .pipelines
        .values()
        .filter(|pipeline| pipeline_matches_filter(pipeline, state, &instances_by_workload, filter))
        .map(serialize_inventory_pipeline)
        .collect::<Vec<_>>();

    let module_values = modules
        .into_iter()
        .map(|(module_name, module)| {
            DataValue::Map(BTreeMap::from([
                ("module".to_string(), DataValue::from(module_name)),
                (
                    "deployment_count".to_string(),
                    DataValue::from(module.deployments as u64),
                ),
                (
                    "workloads".to_string(),
                    DataValue::List(module.workloads.into_iter().map(DataValue::from).collect()),
                ),
                (
                    "external_account_refs".to_string(),
                    DataValue::List(
                        module
                            .external_account_refs
                            .into_iter()
                            .map(DataValue::from)
                            .collect(),
                    ),
                ),
                (
                    "nodes".to_string(),
                    DataValue::List(module.nodes.into_iter().map(DataValue::from).collect()),
                ),
            ]))
        })
        .collect::<Vec<_>>();

    let node_values = state
        .nodes
        .values()
        .filter_map(|node| {
            let scheduled = instances_by_node
                .get(&node.name)
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            let filtered_instances = scheduled
                .iter()
                .filter(|instance| {
                    instance_matches_filter(instance, filter, pipeline_scope.as_ref())
                })
                .cloned()
                .collect::<Vec<_>>();
            if node_matches_filter(node, &filtered_instances, filter) {
                Some(serialize_inventory_node(node, &filtered_instances))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    DataValue::Map(BTreeMap::from([
        (
            "snapshot_marker".to_string(),
            DataValue::Map(BTreeMap::from([(
                "last_applied".to_string(),
                DataValue::from(last_applied),
            )])),
        ),
        ("filters".to_string(), serialize_inventory_filter(filter)),
        ("workloads".to_string(), DataValue::List(workload_values)),
        ("pipelines".to_string(), DataValue::List(pipeline_values)),
        ("modules".to_string(), DataValue::List(module_values)),
        ("nodes".to_string(), DataValue::List(node_values)),
    ]))
}

fn build_inventory_instances(
    state: &ControlPlaneState,
    plan: &SchedulePlan,
) -> Vec<InventoryInstance> {
    plan.instances
        .iter()
        .filter_map(|instance| {
            let deployment = state.deployments.get(&instance.deployment)?;
            Some(InventoryInstance {
                deployment: instance.deployment.clone(),
                workload: deployment.workload.key(),
                module: deployment.module.clone(),
                external_account_ref: deployment
                    .external_account_ref
                    .as_ref()
                    .map(|reference| reference.key.clone()),
                node: instance.node.clone(),
                instance_id: instance.instance_id.clone(),
                isolation: format!("{:?}", instance.isolation),
            })
        })
        .collect()
}

fn serialize_inventory_filter(filter: &AttributedInfrastructureFilter) -> DataValue {
    DataValue::Map(BTreeMap::from([
        (
            "external_account_ref".to_string(),
            filter
                .external_account_ref
                .clone()
                .map_or(DataValue::Null, DataValue::from),
        ),
        (
            "workload".to_string(),
            filter
                .workload
                .clone()
                .map_or(DataValue::Null, DataValue::from),
        ),
        (
            "module".to_string(),
            filter
                .module
                .clone()
                .map_or(DataValue::Null, DataValue::from),
        ),
        (
            "pipeline".to_string(),
            filter
                .pipeline
                .clone()
                .map_or(DataValue::Null, DataValue::from),
        ),
        (
            "node".to_string(),
            filter.node.clone().map_or(DataValue::Null, DataValue::from),
        ),
    ]))
}

fn serialize_inventory_workload(
    spec: &DeploymentSpec,
    instances: &[InventoryInstance],
) -> DataValue {
    let mut node_names = BTreeSet::new();
    for instance in instances {
        node_names.insert(instance.node.clone());
    }
    DataValue::Map(BTreeMap::from([
        ("workload".to_string(), DataValue::from(spec.workload.key())),
        ("module".to_string(), DataValue::from(spec.module.clone())),
        (
            "external_account_ref".to_string(),
            serialize_external_account_ref(&spec.external_account_ref),
        ),
        ("replicas".to_string(), DataValue::from(spec.replicas)),
        (
            "isolation".to_string(),
            DataValue::from(format!("{:?}", spec.isolation)),
        ),
        (
            "resources".to_string(),
            serialize_deployment_resources(spec),
        ),
        (
            "contracts".to_string(),
            DataValue::List(
                spec.contracts
                    .iter()
                    .map(|contract| {
                        DataValue::Map(BTreeMap::from([
                            (
                                "namespace".to_string(),
                                DataValue::from(contract.namespace.clone()),
                            ),
                            ("kind".to_string(), DataValue::from(contract.kind.as_str())),
                            ("name".to_string(), DataValue::from(contract.name.clone())),
                            (
                                "version".to_string(),
                                DataValue::from(contract.version.clone()),
                            ),
                        ]))
                    })
                    .collect(),
            ),
        ),
        (
            "nodes".to_string(),
            DataValue::List(node_names.into_iter().map(DataValue::from).collect()),
        ),
        (
            "scheduled_instances".to_string(),
            DataValue::List(instances.iter().map(serialize_inventory_instance).collect()),
        ),
    ]))
}

fn serialize_inventory_pipeline(spec: &PipelineSpec) -> DataValue {
    let workloads = pipeline_workload_keys(spec);
    DataValue::Map(BTreeMap::from([
        ("pipeline".to_string(), DataValue::from(spec.key())),
        (
            "external_account_ref".to_string(),
            serialize_external_account_ref(&spec.external_account_ref),
        ),
        (
            "edge_count".to_string(),
            DataValue::from(spec.edges.len() as u64),
        ),
        (
            "workloads".to_string(),
            DataValue::List(workloads.into_iter().map(DataValue::from).collect()),
        ),
        (
            "edges".to_string(),
            DataValue::List(
                spec.edges
                    .iter()
                    .map(|edge| {
                        DataValue::Map(BTreeMap::from([
                            (
                                "from".to_string(),
                                DataValue::from(edge.from.endpoint.key()),
                            ),
                            ("to".to_string(), DataValue::from(edge.to.endpoint.key())),
                            (
                                "contract".to_string(),
                                DataValue::Map(BTreeMap::from([
                                    (
                                        "namespace".to_string(),
                                        DataValue::from(edge.from.contract.namespace.clone()),
                                    ),
                                    (
                                        "kind".to_string(),
                                        DataValue::from(edge.from.contract.kind.as_str()),
                                    ),
                                    (
                                        "name".to_string(),
                                        DataValue::from(edge.from.contract.name.clone()),
                                    ),
                                    (
                                        "version".to_string(),
                                        DataValue::from(edge.from.contract.version.clone()),
                                    ),
                                ])),
                            ),
                        ]))
                    })
                    .collect(),
            ),
        ),
    ]))
}

fn serialize_inventory_node(node: &NodeSpec, instances: &[InventoryInstance]) -> DataValue {
    let mut external_account_refs = BTreeSet::new();
    for instance in instances {
        if let Some(reference) = &instance.external_account_ref {
            external_account_refs.insert(reference.clone());
        }
    }
    DataValue::Map(BTreeMap::from([
        ("name".to_string(), DataValue::from(node.name.clone())),
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
            "supported_isolation".to_string(),
            serialize_isolation_profiles(&node.supported_isolation),
        ),
        (
            "daemon_addr".to_string(),
            DataValue::from(node.daemon_addr.clone()),
        ),
        (
            "daemon_server_name".to_string(),
            DataValue::from(node.daemon_server_name.clone()),
        ),
        (
            "last_heartbeat_ms".to_string(),
            DataValue::from(node.last_heartbeat_ms),
        ),
        (
            "external_account_refs".to_string(),
            DataValue::List(
                external_account_refs
                    .into_iter()
                    .map(DataValue::from)
                    .collect(),
            ),
        ),
        (
            "scheduled_instances".to_string(),
            DataValue::List(instances.iter().map(serialize_inventory_instance).collect()),
        ),
    ]))
}

fn serialize_inventory_instance(instance: &InventoryInstance) -> DataValue {
    DataValue::Map(BTreeMap::from([
        (
            "deployment".to_string(),
            DataValue::from(instance.deployment.clone()),
        ),
        (
            "workload".to_string(),
            DataValue::from(instance.workload.clone()),
        ),
        (
            "module".to_string(),
            DataValue::from(instance.module.clone()),
        ),
        (
            "external_account_ref".to_string(),
            instance
                .external_account_ref
                .clone()
                .map_or(DataValue::Null, DataValue::from),
        ),
        ("node".to_string(), DataValue::from(instance.node.clone())),
        (
            "instance_id".to_string(),
            DataValue::from(instance.instance_id.clone()),
        ),
        (
            "isolation".to_string(),
            DataValue::from(instance.isolation.clone()),
        ),
    ]))
}

fn pipeline_workload_keys(spec: &PipelineSpec) -> BTreeSet<String> {
    let mut workloads = BTreeSet::new();
    for edge in &spec.edges {
        workloads.insert(edge.from.endpoint.workload.key());
        workloads.insert(edge.to.endpoint.workload.key());
    }
    workloads
}

fn workload_matches_filter(
    spec: &DeploymentSpec,
    instances: &[InventoryInstance],
    filter: &AttributedInfrastructureFilter,
    pipeline_scope: Option<&BTreeSet<String>>,
) -> bool {
    if let Some(reference) = filter.external_account_ref.as_deref() {
        if spec
            .external_account_ref
            .as_ref()
            .map(|value| value.key.as_str())
            != Some(reference)
        {
            return false;
        }
    }
    if let Some(workload) = filter.workload.as_deref() {
        if spec.workload.key() != workload {
            return false;
        }
    }
    if let Some(module) = filter.module.as_deref() {
        if spec.module != module {
            return false;
        }
    }
    if let Some(node) = filter.node.as_deref() {
        if !instances.iter().any(|instance| instance.node == node) {
            return false;
        }
    }
    if let Some(scope) = pipeline_scope {
        if !scope.contains(&spec.workload.key()) {
            return false;
        }
    }
    true
}

fn pipeline_matches_filter(
    spec: &PipelineSpec,
    state: &ControlPlaneState,
    instances_by_workload: &BTreeMap<String, Vec<InventoryInstance>>,
    filter: &AttributedInfrastructureFilter,
) -> bool {
    if let Some(pipeline) = filter.pipeline.as_deref() {
        if spec.key() != pipeline {
            return false;
        }
    }
    if let Some(reference) = filter.external_account_ref.as_deref() {
        if spec
            .external_account_ref
            .as_ref()
            .map(|value| value.key.as_str())
            != Some(reference)
        {
            return false;
        }
    }
    if let Some(workload) = filter.workload.as_deref() {
        if !spec.edges.iter().any(|edge| {
            edge.from.endpoint.workload.key() == workload
                || edge.to.endpoint.workload.key() == workload
        }) {
            return false;
        }
    }
    if let Some(module) = filter.module.as_deref() {
        let has_module = spec.edges.iter().any(|edge| {
            [
                edge.from.endpoint.workload.key(),
                edge.to.endpoint.workload.key(),
            ]
            .into_iter()
            .any(|workload_key| {
                state
                    .deployments
                    .get(&workload_key)
                    .map(|deployment| deployment.module == module)
                    .unwrap_or(false)
            })
        });
        if !has_module {
            return false;
        }
    }
    if let Some(node) = filter.node.as_deref() {
        let has_node = spec.edges.iter().any(|edge| {
            [
                edge.from.endpoint.workload.key(),
                edge.to.endpoint.workload.key(),
            ]
            .into_iter()
            .any(|workload_key| {
                instances_by_workload
                    .get(&workload_key)
                    .map(|instances| instances.iter().any(|instance| instance.node == node))
                    .unwrap_or(false)
            })
        });
        if !has_node {
            return false;
        }
    }
    true
}

fn instance_matches_filter(
    instance: &InventoryInstance,
    filter: &AttributedInfrastructureFilter,
    pipeline_scope: Option<&BTreeSet<String>>,
) -> bool {
    if let Some(reference) = filter.external_account_ref.as_deref() {
        if instance.external_account_ref.as_deref() != Some(reference) {
            return false;
        }
    }
    if let Some(workload) = filter.workload.as_deref() {
        if instance.workload != workload {
            return false;
        }
    }
    if let Some(module) = filter.module.as_deref() {
        if instance.module != module {
            return false;
        }
    }
    if let Some(node) = filter.node.as_deref() {
        if instance.node != node {
            return false;
        }
    }
    if let Some(scope) = pipeline_scope {
        if !scope.contains(&instance.workload) {
            return false;
        }
    }
    true
}

fn node_matches_filter(
    node: &NodeSpec,
    filtered_instances: &[InventoryInstance],
    filter: &AttributedInfrastructureFilter,
) -> bool {
    if let Some(node_name) = filter.node.as_deref() {
        if node.name != node_name {
            return false;
        }
    }
    if filter.external_account_ref.is_some()
        || filter.workload.is_some()
        || filter.module.is_some()
        || filter.pipeline.is_some()
    {
        return !filtered_instances.is_empty();
    }
    true
}

fn serialize_string_u32_map(values: &BTreeMap<String, u32>) -> DataValue {
    DataValue::Map(
        values
            .iter()
            .map(|(key, value)| (key.clone(), DataValue::from(*value)))
            .collect(),
    )
}

fn serialize_string_set_map(
    values: &BTreeMap<String, std::collections::BTreeSet<String>>,
) -> DataValue {
    DataValue::Map(
        values
            .iter()
            .map(|(key, value)| {
                (
                    key.clone(),
                    DataValue::List(value.iter().cloned().map(DataValue::from).collect()),
                )
            })
            .collect(),
    )
}

fn serialize_isolation_profiles(
    profiles: &[selium_control_plane_api::IsolationProfile],
) -> DataValue {
    DataValue::List(
        profiles
            .iter()
            .map(|profile| DataValue::from(format!("{:?}", profile)))
            .collect(),
    )
}

fn serialize_optional_u32(value: Option<u32>) -> DataValue {
    value.map_or(DataValue::Null, DataValue::from)
}

fn serialize_external_account_ref(
    external_account_ref: &Option<selium_control_plane_api::ExternalAccountRef>,
) -> DataValue {
    external_account_ref
        .as_ref()
        .map_or(DataValue::Null, |reference| {
            DataValue::from(reference.key.clone())
        })
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
        BandwidthProfile, ContractKind, ContractRef, DiscoveryPattern, EventEndpointRef,
        ExternalAccountRef, GUEST_LOG_STDERR_ENDPOINT, GUEST_LOG_STDOUT_ENDPOINT,
        IsolationProfile, PipelineEdge, PipelineEndpoint, PipelineSpec, PublicEndpointRef,
        VolumeMount, parse_idl,
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
