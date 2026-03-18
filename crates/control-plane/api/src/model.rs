use std::{collections::BTreeMap, fmt};

use rkyv::{Archive, Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ContractRef {
    pub namespace: String,
    pub kind: ContractKind,
    pub name: String,
    pub version: String,
}

/// User-facing workload identity for control-plane-managed workloads.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct WorkloadRef {
    pub tenant: String,
    pub namespace: String,
    pub name: String,
}

impl WorkloadRef {
    pub fn key(&self) -> String {
        format!("{}/{}/{}", self.tenant, self.namespace, self.name)
    }

    pub fn validate(&self, subject: &str) -> std::result::Result<(), ApiError> {
        validate_identity_segment(
            &self.tenant,
            &format!("{subject} tenant"),
            ApiError::InvalidWorkload,
        )?;
        validate_identity_segment(
            &self.namespace,
            &format!("{subject} namespace"),
            ApiError::InvalidWorkload,
        )?;
        validate_identity_segment(
            &self.name,
            &format!("{subject} workload"),
            ApiError::InvalidWorkload,
        )
    }
}

impl fmt::Display for WorkloadRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.key())
    }
}

/// User-facing contract-defined event endpoint identity.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct EventEndpointRef {
    pub workload: WorkloadRef,
    pub name: String,
}

impl EventEndpointRef {
    pub fn key(&self) -> String {
        format!("{}#{}", self.workload.key(), self.name)
    }

    pub fn validate(&self, subject: &str) -> std::result::Result<(), ApiError> {
        self.workload.validate(subject)?;
        validate_identity_segment(
            &self.name,
            &format!("{subject} endpoint"),
            ApiError::InvalidEndpoint,
        )
    }
}

impl fmt::Display for EventEndpointRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.key())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum ContractKind {
    Event,
    Service,
    Stream,
}

impl ContractKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Event => "event",
            Self::Service => "service",
            Self::Stream => "stream",
        }
    }
}

pub const GUEST_LOG_STDOUT_ENDPOINT: &str = "stdout";
pub const GUEST_LOG_STDERR_ENDPOINT: &str = "stderr";

/// User-facing public endpoint identity for discovery across contract kinds.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct PublicEndpointRef {
    pub workload: WorkloadRef,
    pub kind: ContractKind,
    pub name: String,
}

impl PublicEndpointRef {
    pub fn key(&self) -> String {
        format!(
            "{}#{}:{}",
            self.workload.key(),
            self.kind.as_str(),
            self.name
        )
    }

    pub fn validate(&self, subject: &str) -> std::result::Result<(), ApiError> {
        self.workload.validate(subject)?;
        validate_identity_segment(
            &self.name,
            &format!("{subject} endpoint"),
            ApiError::InvalidEndpoint,
        )
    }
}

impl fmt::Display for PublicEndpointRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.key())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum DeliveryGuarantee {
    AtLeastOnce,
    AtMostOnce,
    ExactlyOnce,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct SchemaField {
    pub name: String,
    pub ty: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct SchemaDef {
    pub name: String,
    pub fields: Vec<SchemaField>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct EventDef {
    pub name: String,
    pub payload_schema: String,
    pub partitions: u16,
    pub retention: String,
    pub delivery: DeliveryGuarantee,
    pub replay_enabled: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum ServiceBodyMode {
    None,
    Buffered,
    Stream,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ServiceBodyDef {
    pub mode: ServiceBodyMode,
    pub schema: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ServiceFieldBinding {
    pub field: String,
    pub target: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ServiceDef {
    pub name: String,
    pub request_schema: String,
    pub response_schema: String,
    pub protocol: Option<String>,
    pub method: Option<String>,
    pub path: Option<String>,
    pub request_headers: Vec<ServiceFieldBinding>,
    pub request_body: ServiceBodyDef,
    pub response_body: ServiceBodyDef,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct StreamDef {
    pub name: String,
    pub payload_schema: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ContractPackage {
    pub package: String,
    pub namespace: String,
    pub version: String,
    pub schemas: Vec<SchemaDef>,
    pub events: Vec<EventDef>,
    pub services: Vec<ServiceDef>,
    pub streams: Vec<StreamDef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ResolvedContract {
    pub kind: ContractKind,
    pub schema: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize, Default)]
#[rkyv(bytecheck())]
pub struct ContractRegistry {
    /// namespace -> version -> package
    pub packages: BTreeMap<String, BTreeMap<String, ContractPackage>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum IsolationProfile {
    Standard,
    Hardened,
    Microvm,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum BandwidthProfile {
    Low,
    #[default]
    Standard,
    High,
}

impl BandwidthProfile {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Standard => "standard",
            Self::High => "high",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum PlacementMode {
    #[default]
    ElasticPack,
    Balanced,
    Spread,
}

impl PlacementMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ElasticPack => "elastic_pack",
            Self::Balanced => "balanced",
            Self::Spread => "spread",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct VolumeMount {
    pub name: String,
    pub mount_path: String,
    pub read_only: bool,
}

/// Stable opaque reference to an account record owned outside Selium.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ExternalAccountRef {
    /// Opaque account key supplied by an external attribution system.
    pub key: String,
}

impl ExternalAccountRef {
    pub(crate) fn validate(
        &self,
        subject: &str,
        invalid: fn(String) -> ApiError,
    ) -> std::result::Result<(), ApiError> {
        validate_identity_segment(&self.key, &format!("{subject} key"), invalid)
    }
}

impl fmt::Display for ExternalAccountRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.key)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct DeploymentSpec {
    pub workload: WorkloadRef,
    pub module: String,
    pub replicas: u32,
    pub contracts: Vec<ContractRef>,
    pub isolation: IsolationProfile,
    pub placement_mode: PlacementMode,
    pub cpu_millis: u32,
    pub memory_mib: u32,
    pub ephemeral_storage_mib: u32,
    pub bandwidth_profile: BandwidthProfile,
    pub volume_mounts: Vec<VolumeMount>,
    /// Stable opaque external account reference used for attribution hand-off.
    pub external_account_ref: Option<ExternalAccountRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct PipelineEndpoint {
    pub endpoint: EventEndpointRef,
    pub contract: ContractRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct PipelineEdge {
    pub from: PipelineEndpoint,
    pub to: PipelineEndpoint,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct PipelineSpec {
    pub name: String,
    pub tenant: String,
    pub namespace: String,
    pub edges: Vec<PipelineEdge>,
    /// Stable opaque external account reference used for attribution hand-off.
    pub external_account_ref: Option<ExternalAccountRef>,
}

impl PipelineSpec {
    pub fn key(&self) -> String {
        format!("{}/{}/{}", self.tenant, self.namespace, self.name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct NodeSpec {
    pub name: String,
    pub capacity_slots: u32,
    pub allocatable_cpu_millis: Option<u32>,
    pub allocatable_memory_mib: Option<u32>,
    pub reserve_cpu_utilisation_ppm: u32,
    pub reserve_memory_utilisation_ppm: u32,
    pub reserve_slots_utilisation_ppm: u32,
    pub observed_running_instances: Option<u32>,
    pub observed_active_bridges: Option<u32>,
    pub observed_memory_mib: Option<u32>,
    pub observed_workloads: BTreeMap<String, u32>,
    pub observed_workload_memory_mib: BTreeMap<String, u32>,
    pub supported_isolation: Vec<IsolationProfile>,
    pub daemon_addr: String,
    pub daemon_server_name: String,
    pub last_heartbeat_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize, Default)]
#[rkyv(bytecheck())]
pub struct ControlPlaneState {
    pub registry: ContractRegistry,
    pub deployments: BTreeMap<String, DeploymentSpec>,
    pub pipelines: BTreeMap<String, PipelineSpec>,
    pub nodes: BTreeMap<String, NodeSpec>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum DiscoveryOperation {
    Discover,
    Bind,
}

impl DiscoveryOperation {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Discover => "discover",
            Self::Bind => "bind",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum DiscoveryPattern {
    Exact(String),
    Prefix(String),
}

impl DiscoveryPattern {
    pub fn matches(&self, candidate: &str) -> bool {
        match self {
            Self::Exact(pattern) => pattern == candidate,
            Self::Prefix(pattern) => candidate.starts_with(pattern),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize, Default)]
#[rkyv(bytecheck())]
pub struct DiscoveryCapabilityScope {
    pub operations: Vec<DiscoveryOperation>,
    pub workloads: Vec<DiscoveryPattern>,
    pub endpoints: Vec<DiscoveryPattern>,
    pub allow_operational_processes: bool,
}

impl DiscoveryCapabilityScope {
    pub fn allow_all() -> Self {
        Self {
            operations: vec![DiscoveryOperation::Discover, DiscoveryOperation::Bind],
            workloads: vec![DiscoveryPattern::Prefix(String::new())],
            endpoints: vec![DiscoveryPattern::Prefix(String::new())],
            allow_operational_processes: true,
        }
    }

    pub fn allows_workload(&self, operation: DiscoveryOperation, workload: &WorkloadRef) -> bool {
        self.allows_operation(operation) && self.matches_any(&self.workloads, &workload.key())
    }

    pub fn allows_endpoint(
        &self,
        operation: DiscoveryOperation,
        endpoint: &PublicEndpointRef,
    ) -> bool {
        self.allows_operation(operation)
            && if self.endpoints.is_empty() {
                self.matches_any(&self.workloads, &endpoint.workload.key())
            } else {
                self.matches_any(&self.endpoints, &endpoint.key())
            }
    }

    pub fn allows_operational_process_discovery(&self, workload: &WorkloadRef) -> bool {
        self.allow_operational_processes
            && self.allows_operation(DiscoveryOperation::Discover)
            && self.matches_any(&self.workloads, &workload.key())
    }

    fn allows_operation(&self, operation: DiscoveryOperation) -> bool {
        self.operations.contains(&operation)
    }

    fn matches_any(&self, patterns: &[DiscoveryPattern], candidate: &str) -> bool {
        patterns.iter().any(|pattern| pattern.matches(candidate))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct DiscoverableWorkload {
    pub workload: WorkloadRef,
    pub endpoints: Vec<PublicEndpointRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct DiscoverableEndpoint {
    pub endpoint: PublicEndpointRef,
    pub contract: Option<ContractRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize, Default)]
#[rkyv(bytecheck())]
pub struct DiscoveryState {
    pub workloads: Vec<DiscoverableWorkload>,
    pub endpoints: Vec<DiscoverableEndpoint>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ResolvedWorkload {
    pub workload: WorkloadRef,
    pub endpoints: Vec<DiscoverableEndpoint>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ResolvedEndpoint {
    pub endpoint: PublicEndpointRef,
    pub contract: Option<ContractRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct OperationalProcessRecord {
    pub workload: WorkloadRef,
    pub replica_key: String,
    pub node: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum OperationalProcessSelector {
    ReplicaKey(String),
    Workload(WorkloadRef),
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum DiscoveryTarget {
    Workload(WorkloadRef),
    Endpoint(PublicEndpointRef),
    RunningProcess(OperationalProcessSelector),
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct DiscoveryRequest {
    pub operation: DiscoveryOperation,
    pub target: DiscoveryTarget,
    pub scope: DiscoveryCapabilityScope,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum DiscoveryResolution {
    Workload(ResolvedWorkload),
    Endpoint(ResolvedEndpoint),
    RunningProcess(OperationalProcessRecord),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompatibilityReport {
    pub previous_version: String,
    pub current_version: String,
    pub warnings: Vec<String>,
}

#[derive(Debug, Error)]
pub enum ApiError {
    #[error("invalid contract reference")]
    InvalidContract,
    #[error("invalid workload `{0}`")]
    InvalidWorkload(String),
    #[error("invalid endpoint `{0}`")]
    InvalidEndpoint(String),
    #[error("parse error: {0}")]
    Parse(String),
    #[error("duplicate package version `{version}` for namespace `{namespace}`")]
    DuplicateVersion { namespace: String, version: String },
    #[error("incompatible package `{namespace}` {from} -> {to}: {reason}")]
    Incompatible {
        namespace: String,
        from: String,
        to: String,
        reason: String,
    },
    #[error("unknown deployment `{0}`")]
    UnknownDeployment(String),
    #[error("unknown node `{0}`")]
    UnknownNode(String),
    #[error("invalid deployment `{0}`")]
    InvalidDeployment(String),
    #[error("invalid pipeline `{0}`")]
    InvalidPipeline(String),
    #[error("invalid node `{0}`")]
    InvalidNode(String),
    #[error("unknown endpoint `{0}`")]
    UnknownEndpoint(String),
    #[error("unauthorised to {operation} `{subject}`")]
    Unauthorised { operation: String, subject: String },
    #[error("invalid bind target `{0}`")]
    InvalidBindTarget(String),
    #[error("ambiguous running process `{0}`")]
    AmbiguousProcess(String),
}

fn validate_identity_segment(
    value: &str,
    label: &str,
    invalid: fn(String) -> ApiError,
) -> std::result::Result<(), ApiError> {
    if value.trim().is_empty() {
        return Err(invalid(format!("{label} must not be empty")));
    }

    Ok(())
}

pub(crate) fn schema_fields<'a>(
    package: &'a ContractPackage,
    schema_name: &str,
) -> Option<&'a [SchemaField]> {
    package
        .schemas
        .iter()
        .find(|schema| schema.name == schema_name)
        .map(|schema| schema.fields.as_slice())
}

pub(crate) fn schema_field_type<'a>(
    package: &'a ContractPackage,
    schema_name: &str,
    field_name: &str,
) -> Option<&'a str> {
    schema_fields(package, schema_name)?
        .iter()
        .find(|field| field.name == field_name)
        .map(|field| field.ty.as_str())
}
