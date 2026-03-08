use std::collections::BTreeMap;

use rkyv::{Archive, Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ContractRef {
    pub namespace: String,
    pub name: String,
    pub version: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum ContractKind {
    Event,
    Service,
    Stream,
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

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct DeploymentSpec {
    pub app: String,
    pub module: String,
    pub replicas: u32,
    pub contracts: Vec<ContractRef>,
    pub isolation: IsolationProfile,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct PipelineEndpoint {
    pub app: String,
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
    pub namespace: String,
    pub edges: Vec<PipelineEdge>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct NodeSpec {
    pub name: String,
    pub capacity_slots: u32,
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
    #[error("invalid node `{0}`")]
    InvalidNode(String),
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
