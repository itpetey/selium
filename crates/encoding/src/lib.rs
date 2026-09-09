//! Flatbuffers-centric encoding helpers.
//!
//! Selium guests use Flatbuffers on public wires. This module defines small traits for bridging
//! between idiomatic Rust types and Flatbuffers payloads.

use flatbuffers::{FlatBufferBuilder, InvalidFlatbuffer};
use selium_guest_macros::schema;
use thiserror::Error;

pub mod codec;
#[allow(warnings)]
#[rustfmt::skip]
pub mod fbs;
pub mod log;

// Allow generated schema bindings to refer to this crate by name.
extern crate self as selium_encoding;

/// Helper for encoding schema fields into Flatbuffers-ready values.
pub trait FieldEncoder {
    /// Output type written into Flatbuffers args or vectors.
    type Output<'bldr>;

    /// Encode the field for Flatbuffers builders.
    fn encode_field<'bldr, A: flatbuffers::Allocator + 'bldr>(
        &self,
        builder: &mut FlatBufferBuilder<'bldr, A>,
    ) -> Self::Output<'bldr>;
}

/// Flatbuffers-backed message that can be transmitted over an endpoint.
pub trait FlatMsg: Sized {
    /// Encode the owned value into Flatbuffer bytes.
    fn encode(value: &Self) -> Vec<u8>;
    /// Decode the owned value from Flatbuffer bytes.
    fn decode(bytes: &[u8]) -> Result<Self, InvalidFlatbuffer>;
}

/// Marker trait linking a Rust type to a Flatbuffers schema.
pub trait HasSchema {
    /// Static schema descriptor used for port metadata.
    const SCHEMA: SchemaDescriptor;
}

/// Helper for converting Flatbuffer string accessors into owned `String`s.
pub trait StringFieldValue {
    /// Convert the accessor into an owned `String`.
    fn into_owned(self) -> String;
}

/// Error type for encoding/framing operations.
#[derive(Debug, Error)]
pub enum EncodingError {
    /// ABI framing error.
    #[error("framing error: {0:?}")]
    Framing(selium_abi::AbiError),
    /// Flatbuffers decode error.
    #[error("flatbuffer decode error: {0}")]
    Decode(flatbuffers::InvalidFlatbuffer),
}

/// Static descriptor describing the schema carried by an endpoint.
#[derive(Clone, Copy, Debug)]
pub struct SchemaDescriptor {
    /// Fully qualified schema name (used for human-friendly diagnostics).
    pub fqname: &'static str,
    /// 16-byte content hash identifying the schema.
    pub hash: [u8; 16],
}

/// Wire type for InterfaceMetadata, backed by Flatbuffers.
#[derive(Debug, Clone, PartialEq)]
#[schema(
    path = "schemas/discovery.fbs",
    ty = "selium.discovery.InterfaceMetadata",
    binding = "selium_encoding::fbs::selium::discovery::InterfaceMetadata"
)]
pub struct InterfaceMetadataWire {
    /// Interface name.
    pub name: String,
    /// Method names exposed by the interface.
    pub methods: Vec<String>,
}

/// Wire type for a classification label pair, backed by Flatbuffers.
#[derive(Debug, Clone, PartialEq)]
#[schema(
    path = "schemas/discovery.fbs",
    ty = "selium.discovery.Label",
    binding = "selium_encoding::fbs::selium::discovery::Label"
)]
pub struct LabelWire {
    /// Label key.
    pub key: String,
    /// Label value.
    pub value: String,
}

/// Wire type for ResourceTarget, backed by Flatbuffers.
#[derive(Debug, Clone, PartialEq)]
#[schema(
    path = "schemas/discovery.fbs",
    ty = "selium.discovery.ResourceTarget",
    binding = "selium_encoding::fbs::selium::discovery::ResourceTarget"
)]
pub struct ResourceTargetWire {
    /// URI of the resource.
    pub uri: String,
    /// Host id where the resource resides.
    pub host_id: String,
    /// Resource identifier.
    pub resource_id: u64,
    /// Optional interface metadata.
    pub interface: Option<InterfaceMetadataWire>,
    /// Optional tenant identifier for multi-tenant isolation.
    pub tenant: Option<String>,
    /// Resource class segment (`proc`, `region`, `queue`, …).
    pub class: String,
    /// Classification label pairs.
    pub labels: Vec<LabelWire>,
}

/// Wire type for a single advisory domain-to-tenant mapping, backed by Flatbuffers.
#[derive(Debug, Clone, PartialEq)]
#[schema(
    path = "schemas/discovery.fbs",
    ty = "selium.discovery.DomainEntry",
    binding = "selium_encoding::fbs::selium::discovery::DomainEntry"
)]
pub struct DomainEntryWire {
    /// Domain (e.g. `example.com`).
    pub domain: String,
    /// Tenant the domain maps to (e.g. `acme`).
    pub tenant: String,
}

/// Wire type for DiscoveryRequest, backed by Flatbuffers.
#[derive(Debug, Clone, PartialEq)]
#[schema(
    path = "schemas/discovery.fbs",
    ty = "selium.discovery.DiscoveryRequest",
    binding = "selium_encoding::fbs::selium::discovery::DiscoveryRequest"
)]
pub struct DiscoveryRequestWire {
    /// Variant discriminator (0 = Resolve, 1 = Register, 2 = Revoke,
    /// 3 = ResolvePrefix, 4 = ResolveLabels, 5 = ListDomains).
    pub variant: u8,
    /// URI to resolve or register (Resolve, ResolvePrefix, Register, Revoke).
    pub uri: String,
    /// Label key (ResolveLabels variant).
    pub key: String,
    /// Label value (ResolveLabels variant).
    pub value: String,
    /// Target resource for registration (used by Register variant).
    pub target: Option<ResourceTargetWire>,
    /// Root-service designation for the registration (Register variant).
    pub root_service: bool,
}

/// Wire type for DiscoveryResponse, backed by Flatbuffers.
#[derive(Debug, Clone, PartialEq)]
#[schema(
    path = "schemas/discovery.fbs",
    ty = "selium.discovery.DiscoveryResponse",
    binding = "selium_encoding::fbs::selium::discovery::DiscoveryResponse"
)]
pub struct DiscoveryResponseWire {
    /// Variant discriminator (0 = Found, 1 = NotFound, 2 = Registered,
    /// 3 = Revoked, 4 = Forbidden, 5 = Resolved, 6 = Domains).
    pub variant: u8,
    /// The discovered resource (used by Found variant).
    pub target: Option<ResourceTargetWire>,
    /// The matched resources (used by Resolved variant).
    pub targets: Vec<ResourceTargetWire>,
    /// The provisioned domain table (used by Domains variant).
    pub domains: Vec<DomainEntryWire>,
}

/// Wire type for a control-plane resolved target, backed by Flatbuffers.
#[derive(Debug, Clone, PartialEq)]
#[schema(
    path = "schemas/control.fbs",
    ty = "selium.control.ResolvedTarget",
    binding = "selium_encoding::fbs::selium::control::ResolvedTarget"
)]
pub struct ResolvedTargetWire {
    /// URI of the resource.
    pub uri: String,
    /// Host id where the resource resides.
    pub host_id: String,
    /// Resource identifier.
    pub resource_id: u64,
}

/// Wire type for ControlRequest, backed by Flatbuffers.
#[derive(Debug, Clone, PartialEq)]
#[schema(
    path = "schemas/control.fbs",
    ty = "selium.control.ControlRequest",
    binding = "selium_encoding::fbs::selium::control::ControlRequest"
)]
pub struct ControlRequestWire {
    /// Variant discriminator (0 = Deploy, 1 = Scale, 2 = Stop, 3 = Resolve,
    /// 4 = Upload, 5 = Status).
    pub variant: u8,
    /// Workload identifier (Deploy, Scale, Stop, Status variants).
    pub workload_id: String,
    /// Replica count (Deploy, Scale variants).
    pub replicas: u32,
    /// Module reference (Deploy variant).
    pub module: String,
    /// URI to resolve (Resolve variant).
    pub uri: String,
    /// Manifest name for an uploaded module (Upload variant).
    pub manifest: String,
    /// Uploaded module bytes (Upload variant).
    pub bytes: Vec<u8>,
}

/// Wire type for a control-plane deployment desired state, backed by
/// Flatbuffers.
#[derive(Debug, Clone, PartialEq)]
#[schema(
    path = "schemas/control.fbs",
    ty = "selium.control.Deployment",
    binding = "selium_encoding::fbs::selium::control::Deployment"
)]
pub struct DeploymentWire {
    /// Workload identifier.
    pub workload_id: String,
    /// Desired replica count.
    pub replicas: u32,
    /// Module reference.
    pub module: String,
}

/// Wire type for ControlResponse, backed by Flatbuffers.
#[derive(Debug, Clone, PartialEq)]
#[schema(
    path = "schemas/control.fbs",
    ty = "selium.control.ControlResponse",
    binding = "selium_encoding::fbs::selium::control::ControlResponse"
)]
pub struct ControlResponseWire {
    /// Variant discriminator (0 = Uploaded, 1 = Accepted, 2 = StatusFound,
    /// 3 = StatusNotFound, 4 = ResolvedFound, 5 = ResolvedNotFound, 6 = Error).
    pub variant: u8,
    /// Deployment desired state (Accepted, StatusFound variants).
    pub deployment: Option<DeploymentWire>,
    /// Manifest name for an uploaded module (Uploaded variant).
    pub manifest: String,
    /// Whether the primary delegation was applied (Accepted variant).
    pub applied: bool,
    /// Delegation step name (Accepted, Error variants).
    pub step: String,
    /// Delegation or error context (Accepted, Error variants).
    pub context: String,
    /// Resolved target (ResolvedFound variant).
    pub target: Option<ResolvedTargetWire>,
}

/// Wire type for SchedulerRequest, backed by Flatbuffers.
#[derive(Debug, Clone, PartialEq)]
#[schema(
    path = "schemas/scheduler.fbs",
    ty = "selium.scheduler.SchedulerRequest",
    binding = "selium_encoding::fbs::selium::scheduler::SchedulerRequest"
)]
pub struct SchedulerRequestWire {
    /// Variant discriminator (0 = Place, 1 = Scale, 2 = Stop).
    pub variant: u8,
    /// Workload identifier.
    pub workload_id: String,
    /// Replica count (Place, Scale variants).
    pub replicas: u32,
}

/// Wire type for SchedulerResponse, backed by Flatbuffers.
#[derive(Debug, Clone, PartialEq)]
#[schema(
    path = "schemas/scheduler.fbs",
    ty = "selium.scheduler.SchedulerResponse",
    binding = "selium_encoding::fbs::selium::scheduler::SchedulerResponse"
)]
pub struct SchedulerResponseWire {
    /// Variant discriminator (0 = Applied, 1 = Deferred, 2 = Rejected).
    pub variant: u8,
    /// Human-readable reason (Deferred, Rejected variants).
    pub reason: String,
}

impl From<selium_abi::AbiError> for EncodingError {
    fn from(error: selium_abi::AbiError) -> Self {
        Self::Framing(error)
    }
}

impl From<flatbuffers::InvalidFlatbuffer> for EncodingError {
    fn from(error: flatbuffers::InvalidFlatbuffer) -> Self {
        Self::Decode(error)
    }
}

impl From<&selium_abi::InterfaceMetadata> for InterfaceMetadataWire {
    fn from(value: &selium_abi::InterfaceMetadata) -> Self {
        Self::new(value.name.clone(), value.methods.clone())
    }
}

impl From<&(String, String)> for LabelWire {
    fn from(value: &(String, String)) -> Self {
        Self::new(value.0.clone(), value.1.clone())
    }
}

impl From<&selium_abi::ResourceTarget> for ResourceTargetWire {
    fn from(value: &selium_abi::ResourceTarget) -> Self {
        Self::new(
            value.uri.clone(),
            value.host_id.clone(),
            value.resource_id,
            value.interface.as_ref().map(InterfaceMetadataWire::from),
            value.tenant.clone(),
            value.class.uri_segment().to_string(),
            value.labels.iter().map(LabelWire::from).collect(),
        )
    }
}

impl From<&(String, String)> for DomainEntryWire {
    fn from(value: &(String, String)) -> Self {
        Self::new(value.0.clone(), value.1.clone())
    }
}

impl From<&selium_abi::DiscoveryRequest> for DiscoveryRequestWire {
    fn from(value: &selium_abi::DiscoveryRequest) -> Self {
        match value {
            selium_abi::DiscoveryRequest::Resolve(uri) => {
                Self::new(0, uri.clone(), String::new(), String::new(), None, false)
            }
            selium_abi::DiscoveryRequest::ResolvePrefix(uri) => {
                Self::new(3, uri.clone(), String::new(), String::new(), None, false)
            }
            selium_abi::DiscoveryRequest::ResolveLabels {
                key,
                value: value_str,
            } => Self::new(
                4,
                String::new(),
                key.clone(),
                value_str.clone(),
                None,
                false,
            ),
            selium_abi::DiscoveryRequest::Register {
                uri,
                target,
                root_service,
                ..
            } => Self::new(
                1,
                uri.clone(),
                String::new(),
                String::new(),
                Some(ResourceTargetWire::from(target)),
                *root_service,
            ),
            selium_abi::DiscoveryRequest::Revoke { uri } => {
                Self::new(2, uri.clone(), String::new(), String::new(), None, false)
            }
            selium_abi::DiscoveryRequest::ListDomains => {
                Self::new(5, String::new(), String::new(), String::new(), None, false)
            }
            // Tier-1-only operations (published over the runtime→discovery
            // rkyv feed, never the flatbuffers RPC wire) map to Resolve with
            // no URI; they still round-trip structurally.
            selium_abi::DiscoveryRequest::RevokeByOwner { .. }
            | selium_abi::DiscoveryRequest::SeedDomain { .. } => {
                Self::new(0, String::new(), String::new(), String::new(), None, false)
            }
        }
    }
}

impl From<&selium_abi::DiscoveryResponse> for DiscoveryResponseWire {
    fn from(value: &selium_abi::DiscoveryResponse) -> Self {
        match value {
            selium_abi::DiscoveryResponse::Found(target) => Self::new(
                0,
                Some(ResourceTargetWire::from(target)),
                Vec::new(),
                Vec::new(),
            ),
            selium_abi::DiscoveryResponse::NotFound => Self::new(1, None, Vec::new(), Vec::new()),
            selium_abi::DiscoveryResponse::Registered => Self::new(2, None, Vec::new(), Vec::new()),
            selium_abi::DiscoveryResponse::Revoked => Self::new(3, None, Vec::new(), Vec::new()),
            selium_abi::DiscoveryResponse::Forbidden => Self::new(4, None, Vec::new(), Vec::new()),
            selium_abi::DiscoveryResponse::Resolved(targets) => Self::new(
                5,
                None,
                targets.iter().map(ResourceTargetWire::from).collect(),
                Vec::new(),
            ),
            selium_abi::DiscoveryResponse::Domains(domains) => Self::new(
                6,
                None,
                Vec::new(),
                domains.iter().map(DomainEntryWire::from).collect(),
            ),
        }
    }
}

impl From<&selium_abi::ResolvedTarget> for ResolvedTargetWire {
    fn from(value: &selium_abi::ResolvedTarget) -> Self {
        Self::new(value.uri.clone(), value.host_id.clone(), value.resource_id)
    }
}

impl From<&selium_abi::ControlRequest> for ControlRequestWire {
    fn from(value: &selium_abi::ControlRequest) -> Self {
        match value {
            selium_abi::ControlRequest::Deploy {
                workload_id,
                replicas,
                module,
            } => Self::new(
                0,
                workload_id.clone(),
                *replicas,
                module.clone(),
                String::new(),
                String::new(),
                Vec::new(),
            ),
            selium_abi::ControlRequest::Scale {
                workload_id,
                replicas,
            } => Self::new(
                1,
                workload_id.clone(),
                *replicas,
                String::new(),
                String::new(),
                String::new(),
                Vec::new(),
            ),
            selium_abi::ControlRequest::Stop { workload_id } => Self::new(
                2,
                workload_id.clone(),
                0,
                String::new(),
                String::new(),
                String::new(),
                Vec::new(),
            ),
            selium_abi::ControlRequest::Resolve { uri } => Self::new(
                3,
                String::new(),
                0,
                String::new(),
                uri.clone(),
                String::new(),
                Vec::new(),
            ),
            selium_abi::ControlRequest::Upload { manifest, bytes } => Self::new(
                4,
                String::new(),
                0,
                String::new(),
                String::new(),
                manifest.clone(),
                bytes.clone(),
            ),
            selium_abi::ControlRequest::Status { workload_id } => Self::new(
                5,
                workload_id.clone(),
                0,
                String::new(),
                String::new(),
                String::new(),
                Vec::new(),
            ),
        }
    }
}

impl From<&selium_abi::Deployment> for DeploymentWire {
    fn from(value: &selium_abi::Deployment) -> Self {
        Self::new(
            value.workload_id.clone(),
            value.replicas,
            value.module.clone(),
        )
    }
}

impl From<&selium_abi::ControlResponse> for ControlResponseWire {
    fn from(value: &selium_abi::ControlResponse) -> Self {
        match value {
            selium_abi::ControlResponse::Uploaded { manifest } => Self::new(
                0,
                None,
                manifest.clone(),
                false,
                String::new(),
                String::new(),
                None,
            ),
            selium_abi::ControlResponse::Accepted {
                workload_id,
                replicas,
                module,
                delegated,
            } => Self::new(
                1,
                Some(DeploymentWire::from(&selium_abi::Deployment {
                    workload_id: workload_id.clone(),
                    replicas: *replicas,
                    module: module.clone(),
                })),
                String::new(),
                delegated.applied,
                delegated.step.clone(),
                delegated.context.clone(),
                None,
            ),
            selium_abi::ControlResponse::Status {
                deployment: Some(deployment),
            } => Self::new(
                2,
                Some(DeploymentWire::from(deployment)),
                String::new(),
                false,
                String::new(),
                String::new(),
                None,
            ),
            selium_abi::ControlResponse::Status { deployment: None } => Self::new(
                3,
                None,
                String::new(),
                false,
                String::new(),
                String::new(),
                None,
            ),
            selium_abi::ControlResponse::Resolved {
                target: Some(target),
            } => Self::new(
                4,
                None,
                String::new(),
                false,
                String::new(),
                String::new(),
                Some(ResolvedTargetWire::from(target)),
            ),
            selium_abi::ControlResponse::Resolved { target: None } => Self::new(
                5,
                None,
                String::new(),
                false,
                String::new(),
                String::new(),
                None,
            ),
            selium_abi::ControlResponse::Error { step, context } => Self::new(
                6,
                None,
                String::new(),
                false,
                step.clone(),
                context.clone(),
                None,
            ),
        }
    }
}

impl From<&selium_abi::SchedulerRequest> for SchedulerRequestWire {
    fn from(value: &selium_abi::SchedulerRequest) -> Self {
        match value {
            selium_abi::SchedulerRequest::Place {
                workload_id,
                replicas,
            } => Self::new(0, workload_id.clone(), *replicas),
            selium_abi::SchedulerRequest::Scale {
                workload_id,
                replicas,
            } => Self::new(1, workload_id.clone(), *replicas),
            selium_abi::SchedulerRequest::Stop { workload_id } => {
                Self::new(2, workload_id.clone(), 0)
            }
        }
    }
}

impl From<&selium_abi::SchedulerResponse> for SchedulerResponseWire {
    fn from(value: &selium_abi::SchedulerResponse) -> Self {
        match value {
            selium_abi::SchedulerResponse::Applied => Self::new(0, String::new()),
            selium_abi::SchedulerResponse::Deferred { reason } => Self::new(1, reason.clone()),
            selium_abi::SchedulerResponse::Rejected { reason } => Self::new(2, reason.clone()),
        }
    }
}

impl From<&DomainEntryWire> for (String, String) {
    fn from(value: &DomainEntryWire) -> Self {
        (value.domain.clone(), value.tenant.clone())
    }
}

impl From<LabelWire> for (String, String) {
    fn from(value: LabelWire) -> Self {
        (value.key, value.value)
    }
}

impl From<ResolvedTargetWire> for selium_abi::ResolvedTarget {
    fn from(value: ResolvedTargetWire) -> Self {
        Self {
            uri: value.uri,
            host_id: value.host_id,
            resource_id: value.resource_id,
        }
    }
}

impl From<DeploymentWire> for selium_abi::Deployment {
    fn from(value: DeploymentWire) -> Self {
        Self {
            workload_id: value.workload_id,
            replicas: value.replicas,
            module: value.module,
        }
    }
}

impl FlatMsg for () {
    fn encode(_value: &Self) -> Vec<u8> {
        Vec::new()
    }

    fn decode(_bytes: &[u8]) -> Result<Self, InvalidFlatbuffer> {
        Ok(())
    }
}

impl HasSchema for () {
    const SCHEMA: SchemaDescriptor = SchemaDescriptor {
        fqname: "empty_tuple",
        hash: [0; 16],
    };
}

impl FlatMsg for u32 {
    fn encode(value: &Self) -> Vec<u8> {
        value.to_le_bytes().into()
    }

    fn decode(bytes: &[u8]) -> Result<Self, InvalidFlatbuffer> {
        Ok(u32::from_le_bytes(
            bytes
                .try_into()
                .map_err(|_e| InvalidFlatbuffer::ApparentSizeTooLarge)?,
        ))
    }
}

impl HasSchema for u32 {
    const SCHEMA: SchemaDescriptor = SchemaDescriptor {
        fqname: "unsigned_thirty_two_bit_int",
        hash: [0, 3, 2, 3, 2, 3, 2, 3, 2, 3, 2, 3, 2, 3, 2, 3],
    };
}

impl FlatMsg for i32 {
    fn encode(value: &Self) -> Vec<u8> {
        value.to_le_bytes().into()
    }

    fn decode(bytes: &[u8]) -> Result<Self, InvalidFlatbuffer> {
        Ok(i32::from_le_bytes(
            bytes
                .try_into()
                .map_err(|_e| InvalidFlatbuffer::ApparentSizeTooLarge)?,
        ))
    }
}

impl HasSchema for i32 {
    const SCHEMA: SchemaDescriptor = SchemaDescriptor {
        fqname: "signed_thirty_two_bit_int",
        hash: [1, 3, 2, 3, 2, 3, 2, 3, 2, 3, 2, 3, 2, 3, 2, 3],
    };
}

impl FlatMsg for u64 {
    fn encode(value: &Self) -> Vec<u8> {
        value.to_le_bytes().into()
    }

    fn decode(bytes: &[u8]) -> Result<Self, InvalidFlatbuffer> {
        Ok(u64::from_le_bytes(
            bytes
                .try_into()
                .map_err(|_e| InvalidFlatbuffer::ApparentSizeTooLarge)?,
        ))
    }
}

impl HasSchema for u64 {
    const SCHEMA: SchemaDescriptor = SchemaDescriptor {
        fqname: "unsigned_sixty_four_bit_int",
        hash: [0, 6, 4, 6, 4, 6, 4, 6, 4, 6, 4, 6, 4, 6, 4, 6],
    };
}

impl FlatMsg for String {
    fn encode(value: &Self) -> Vec<u8> {
        value.as_bytes().to_owned()
    }

    fn decode(bytes: &[u8]) -> Result<Self, InvalidFlatbuffer> {
        Ok(str::from_utf8(bytes)
            .map_err(|e| InvalidFlatbuffer::Utf8Error {
                error: e,
                range: 0..bytes.len(),
                error_trace: Default::default(),
            })?
            .to_owned())
    }
}

impl HasSchema for String {
    const SCHEMA: SchemaDescriptor = SchemaDescriptor {
        fqname: "string",
        hash: [1; 16],
    };
}

impl FlatMsg for Vec<u8> {
    fn encode(value: &Self) -> Vec<u8> {
        value.clone()
    }

    fn decode(bytes: &[u8]) -> Result<Self, InvalidFlatbuffer> {
        Ok(bytes.to_vec())
    }
}

impl HasSchema for Vec<u8> {
    const SCHEMA: SchemaDescriptor = SchemaDescriptor {
        fqname: "byte_vector",
        hash: [2; 16],
    };
}

impl StringFieldValue for &str {
    fn into_owned(self) -> String {
        self.to_string()
    }
}

impl StringFieldValue for Option<&str> {
    fn into_owned(self) -> String {
        self.unwrap_or_default().to_string()
    }
}

impl From<InterfaceMetadataWire> for selium_abi::InterfaceMetadata {
    fn from(wire: InterfaceMetadataWire) -> Self {
        Self {
            name: wire.name,
            methods: wire.methods,
        }
    }
}

impl FlatMsg for selium_abi::InterfaceMetadata {
    fn encode(value: &Self) -> Vec<u8> {
        let wire = InterfaceMetadataWire::from(value);
        FlatMsg::encode(&wire)
    }

    fn decode(bytes: &[u8]) -> ::std::result::Result<Self, InvalidFlatbuffer> {
        let wire: InterfaceMetadataWire = FlatMsg::decode(bytes)?;
        Ok(Self::from(wire))
    }
}

impl HasSchema for selium_abi::InterfaceMetadata {
    const SCHEMA: SchemaDescriptor = InterfaceMetadataWireSchema;
}

impl FlatMsg for selium_abi::ResourceTarget {
    fn encode(value: &Self) -> Vec<u8> {
        let wire = ResourceTargetWire::from(value);
        FlatMsg::encode(&wire)
    }

    fn decode(bytes: &[u8]) -> ::std::result::Result<Self, InvalidFlatbuffer> {
        let wire: ResourceTargetWire = FlatMsg::decode(bytes)?;
        resource_target_try_from_wire(wire)
    }
}

impl HasSchema for selium_abi::ResourceTarget {
    const SCHEMA: SchemaDescriptor = ResourceTargetWireSchema;
}

impl FlatMsg for selium_abi::DiscoveryRequest {
    fn encode(value: &Self) -> Vec<u8> {
        let wire = DiscoveryRequestWire::from(value);
        FlatMsg::encode(&wire)
    }

    fn decode(bytes: &[u8]) -> ::std::result::Result<Self, InvalidFlatbuffer> {
        let wire: DiscoveryRequestWire = FlatMsg::decode(bytes)?;
        discovery_request_try_from_wire(wire)
    }
}

impl HasSchema for selium_abi::DiscoveryRequest {
    const SCHEMA: SchemaDescriptor = DiscoveryRequestWireSchema;
}

impl FlatMsg for selium_abi::DiscoveryResponse {
    fn encode(value: &Self) -> Vec<u8> {
        let wire = DiscoveryResponseWire::from(value);
        FlatMsg::encode(&wire)
    }

    fn decode(bytes: &[u8]) -> ::std::result::Result<Self, InvalidFlatbuffer> {
        let wire: DiscoveryResponseWire = FlatMsg::decode(bytes)?;
        discovery_response_try_from_wire(wire)
    }
}

impl HasSchema for selium_abi::DiscoveryResponse {
    const SCHEMA: SchemaDescriptor = DiscoveryResponseWireSchema;
}

impl FlatMsg for selium_abi::ResolvedTarget {
    fn encode(value: &Self) -> Vec<u8> {
        let wire = ResolvedTargetWire::from(value);
        FlatMsg::encode(&wire)
    }

    fn decode(bytes: &[u8]) -> ::std::result::Result<Self, InvalidFlatbuffer> {
        let wire: ResolvedTargetWire = FlatMsg::decode(bytes)?;
        Ok(Self::from(wire))
    }
}

impl HasSchema for selium_abi::ResolvedTarget {
    const SCHEMA: SchemaDescriptor = ResolvedTargetWireSchema;
}

impl FlatMsg for selium_abi::ControlRequest {
    fn encode(value: &Self) -> Vec<u8> {
        let wire = ControlRequestWire::from(value);
        FlatMsg::encode(&wire)
    }

    fn decode(bytes: &[u8]) -> ::std::result::Result<Self, InvalidFlatbuffer> {
        let wire: ControlRequestWire = FlatMsg::decode(bytes)?;
        control_request_try_from_wire(wire)
    }
}

impl HasSchema for selium_abi::ControlRequest {
    const SCHEMA: SchemaDescriptor = ControlRequestWireSchema;
}

impl FlatMsg for selium_abi::ControlResponse {
    fn encode(value: &Self) -> Vec<u8> {
        let wire = ControlResponseWire::from(value);
        FlatMsg::encode(&wire)
    }

    fn decode(bytes: &[u8]) -> ::std::result::Result<Self, InvalidFlatbuffer> {
        let wire: ControlResponseWire = FlatMsg::decode(bytes)?;
        control_response_try_from_wire(wire)
    }
}

impl HasSchema for selium_abi::ControlResponse {
    const SCHEMA: SchemaDescriptor = ControlResponseWireSchema;
}

impl FlatMsg for selium_abi::SchedulerRequest {
    fn encode(value: &Self) -> Vec<u8> {
        let wire = SchedulerRequestWire::from(value);
        FlatMsg::encode(&wire)
    }

    fn decode(bytes: &[u8]) -> ::std::result::Result<Self, InvalidFlatbuffer> {
        let wire: SchedulerRequestWire = FlatMsg::decode(bytes)?;
        scheduler_request_try_from_wire(wire)
    }
}

impl HasSchema for selium_abi::SchedulerRequest {
    const SCHEMA: SchemaDescriptor = SchedulerRequestWireSchema;
}

impl FlatMsg for selium_abi::SchedulerResponse {
    fn encode(value: &Self) -> Vec<u8> {
        let wire = SchedulerResponseWire::from(value);
        FlatMsg::encode(&wire)
    }

    fn decode(bytes: &[u8]) -> ::std::result::Result<Self, InvalidFlatbuffer> {
        let wire: SchedulerResponseWire = FlatMsg::decode(bytes)?;
        scheduler_response_try_from_wire(wire)
    }
}

impl HasSchema for selium_abi::SchedulerResponse {
    const SCHEMA: SchemaDescriptor = SchedulerResponseWireSchema;
}

/// Converts a control request wire strictly: unknown variant tags are decode
/// errors rather than silently reinterpreted defaults.
fn control_request_try_from_wire(
    wire: ControlRequestWire,
) -> ::std::result::Result<selium_abi::ControlRequest, InvalidFlatbuffer> {
    match wire.variant {
        0 => Ok(selium_abi::ControlRequest::Deploy {
            workload_id: wire.workload_id,
            replicas: wire.replicas,
            module: wire.module,
        }),
        1 => Ok(selium_abi::ControlRequest::Scale {
            workload_id: wire.workload_id,
            replicas: wire.replicas,
        }),
        2 => Ok(selium_abi::ControlRequest::Stop {
            workload_id: wire.workload_id,
        }),
        3 => Ok(selium_abi::ControlRequest::Resolve { uri: wire.uri }),
        4 => Ok(selium_abi::ControlRequest::Upload {
            manifest: wire.manifest,
            bytes: wire.bytes,
        }),
        5 => Ok(selium_abi::ControlRequest::Status {
            workload_id: wire.workload_id,
        }),
        _ => invalid_wire("known control request variant"),
    }
}

/// Converts a control response wire strictly: unknown variant tags and a
/// `ResolvedFound` without a target are decode errors rather than silently
/// reinterpreted defaults.
fn control_response_try_from_wire(
    wire: ControlResponseWire,
) -> ::std::result::Result<selium_abi::ControlResponse, InvalidFlatbuffer> {
    match wire.variant {
        0 => Ok(selium_abi::ControlResponse::Uploaded {
            manifest: wire.manifest,
        }),
        1 => {
            let deployment = match wire.deployment {
                Some(deployment) => deployment,
                None => return invalid_wire("Accepted deployment"),
            };
            Ok(selium_abi::ControlResponse::Accepted {
                workload_id: deployment.workload_id,
                replicas: deployment.replicas,
                module: deployment.module,
                delegated: selium_abi::DelegationStatus {
                    step: wire.step,
                    applied: wire.applied,
                    context: wire.context,
                },
            })
        }
        2 => match wire.deployment {
            Some(deployment) => Ok(selium_abi::ControlResponse::Status {
                deployment: Some(selium_abi::Deployment::from(deployment)),
            }),
            None => invalid_wire("StatusFound deployment"),
        },
        3 => Ok(selium_abi::ControlResponse::Status { deployment: None }),
        4 => {
            let target = match wire.target {
                Some(target) => target,
                None => return invalid_wire("ResolvedFound target"),
            };
            Ok(selium_abi::ControlResponse::Resolved {
                target: Some(selium_abi::ResolvedTarget::from(target)),
            })
        }
        5 => Ok(selium_abi::ControlResponse::Resolved { target: None }),
        6 => Ok(selium_abi::ControlResponse::Error {
            step: wire.step,
            context: wire.context,
        }),
        _ => invalid_wire("known control response variant"),
    }
}

/// Converts a discovery request wire strictly: unknown variant tags and a
/// `Register` without a target are decode errors rather than silently
/// reinterpreted defaults.
fn discovery_request_try_from_wire(
    wire: DiscoveryRequestWire,
) -> ::std::result::Result<selium_abi::DiscoveryRequest, InvalidFlatbuffer> {
    match wire.variant {
        0 => Ok(selium_abi::DiscoveryRequest::Resolve(wire.uri)),
        1 => {
            let target = match wire.target {
                Some(target) => target,
                None => return invalid_wire("Register target"),
            };
            Ok(selium_abi::DiscoveryRequest::Register {
                uri: wire.uri,
                target: resource_target_try_from_wire(target)?,
                // The RPC wire never carries a Tier-1 owner; guests always
                // register on their own behalf.
                owner: None,
                root_service: wire.root_service,
            })
        }
        2 => Ok(selium_abi::DiscoveryRequest::Revoke { uri: wire.uri }),
        3 => Ok(selium_abi::DiscoveryRequest::ResolvePrefix(wire.uri)),
        4 => Ok(selium_abi::DiscoveryRequest::ResolveLabels {
            key: wire.key,
            value: wire.value,
        }),
        5 => Ok(selium_abi::DiscoveryRequest::ListDomains),
        _ => invalid_wire("known discovery request variant"),
    }
}

/// Converts a discovery response wire strictly: unknown variant tags and a
/// `Found` without a target are decode errors rather than silently
/// reinterpreted defaults.
fn discovery_response_try_from_wire(
    wire: DiscoveryResponseWire,
) -> ::std::result::Result<selium_abi::DiscoveryResponse, InvalidFlatbuffer> {
    match wire.variant {
        0 => {
            let target = match wire.target {
                Some(target) => target,
                None => return invalid_wire("Found target"),
            };
            Ok(selium_abi::DiscoveryResponse::Found(
                resource_target_try_from_wire(target)?,
            ))
        }
        1 => Ok(selium_abi::DiscoveryResponse::NotFound),
        2 => Ok(selium_abi::DiscoveryResponse::Registered),
        3 => Ok(selium_abi::DiscoveryResponse::Revoked),
        4 => Ok(selium_abi::DiscoveryResponse::Forbidden),
        5 => {
            let targets = wire
                .targets
                .into_iter()
                .map(resource_target_try_from_wire)
                .collect::<::std::result::Result<Vec<_>, _>>()?;
            Ok(selium_abi::DiscoveryResponse::Resolved(targets))
        }
        6 => {
            let domains = wire
                .domains
                .into_iter()
                .map(|entry| (entry.domain, entry.tenant))
                .collect();
            Ok(selium_abi::DiscoveryResponse::Domains(domains))
        }
        _ => invalid_wire("known discovery response variant"),
    }
}

/// Strict-decode failure naming a required property of the wire payload
/// that is missing or invalid.
fn invalid_wire<T>(required: &'static str) -> ::std::result::Result<T, InvalidFlatbuffer> {
    InvalidFlatbuffer::new_missing_required(required)
}

/// Converts a wire resource target strictly: the class segment vocabulary
/// is closed, so an unknown segment is a decode error rather than a silent
/// default. Both in-fabric endpoints emit the closed set, so strictness
/// never rejects legitimate traffic — it only surfaces foreign or buggy
/// producers instead of silently misclassifying them.
fn resource_target_try_from_wire(
    wire: ResourceTargetWire,
) -> ::std::result::Result<selium_abi::ResourceTarget, InvalidFlatbuffer> {
    let class = match selium_abi::ResourceClass::from_uri_segment(&wire.class) {
        Some(class) => class,
        None => return invalid_wire("known resource class segment"),
    };
    Ok(selium_abi::ResourceTarget {
        uri: wire.uri,
        host_id: wire.host_id,
        resource_id: wire.resource_id,
        interface: wire.interface.map(selium_abi::InterfaceMetadata::from),
        tenant: wire.tenant,
        class,
        labels: wire
            .labels
            .into_iter()
            .map(<(String, String)>::from)
            .collect(),
    })
}

/// Converts a scheduler request wire strictly: unknown variant tags are decode
/// errors rather than silently reinterpreted defaults.
fn scheduler_request_try_from_wire(
    wire: SchedulerRequestWire,
) -> ::std::result::Result<selium_abi::SchedulerRequest, InvalidFlatbuffer> {
    match wire.variant {
        0 => Ok(selium_abi::SchedulerRequest::Place {
            workload_id: wire.workload_id,
            replicas: wire.replicas,
        }),
        1 => Ok(selium_abi::SchedulerRequest::Scale {
            workload_id: wire.workload_id,
            replicas: wire.replicas,
        }),
        2 => Ok(selium_abi::SchedulerRequest::Stop {
            workload_id: wire.workload_id,
        }),
        _ => invalid_wire("known scheduler request variant"),
    }
}

/// Converts a scheduler response wire strictly: unknown variant tags are decode
/// errors rather than silently reinterpreted defaults.
fn scheduler_response_try_from_wire(
    wire: SchedulerResponseWire,
) -> ::std::result::Result<selium_abi::SchedulerResponse, InvalidFlatbuffer> {
    match wire.variant {
        0 => Ok(selium_abi::SchedulerResponse::Applied),
        1 => Ok(selium_abi::SchedulerResponse::Deferred {
            reason: wire.reason,
        }),
        2 => Ok(selium_abi::SchedulerResponse::Rejected {
            reason: wire.reason,
        }),
        _ => invalid_wire("known scheduler response variant"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unit_round_trips() {
        let bytes = FlatMsg::encode(&());
        let decoded: () = FlatMsg::decode(&bytes).expect("decode");
        assert_eq!(decoded, ());
    }

    #[test]
    fn u32_round_trips() {
        let bytes = FlatMsg::encode(&42u32);
        let decoded: u32 = FlatMsg::decode(&bytes).expect("decode");
        assert_eq!(decoded, 42u32);
    }

    #[test]
    fn i32_round_trips() {
        let bytes = FlatMsg::encode(&-42i32);
        let decoded: i32 = FlatMsg::decode(&bytes).expect("decode");
        assert_eq!(decoded, -42i32);
    }

    #[test]
    fn u64_round_trips() {
        let bytes = FlatMsg::encode(&12345678901234u64);
        let decoded: u64 = FlatMsg::decode(&bytes).expect("decode");
        assert_eq!(decoded, 12345678901234u64);
    }

    #[test]
    fn string_round_trips() {
        let bytes = FlatMsg::encode(&"hello".to_string());
        let decoded: String = FlatMsg::decode(&bytes).expect("decode");
        assert_eq!(decoded, "hello".to_string());
    }

    #[test]
    fn vec_u8_round_trips() {
        let bytes = FlatMsg::encode(&vec![1u8, 2, 3]);
        let decoded: Vec<u8> = FlatMsg::decode(&bytes).expect("decode");
        assert_eq!(decoded, vec![1u8, 2, 3]);
    }

    #[test]
    fn discovery_request_round_trips() {
        let request = selium_abi::DiscoveryRequest::Resolve("sel://tenant/app/api".to_string());
        let bytes = FlatMsg::encode(&request);
        let decoded: selium_abi::DiscoveryRequest = FlatMsg::decode(&bytes).expect("decode");
        assert_eq!(decoded, request);
    }

    #[test]
    fn discovery_response_not_found_round_trips() {
        let response = selium_abi::DiscoveryResponse::NotFound;
        let bytes = FlatMsg::encode(&response);
        let decoded: selium_abi::DiscoveryResponse = FlatMsg::decode(&bytes).expect("decode");
        assert_eq!(decoded, response);
    }

    #[test]
    fn discovery_response_found_round_trips() {
        let response = selium_abi::DiscoveryResponse::Found(selium_abi::ResourceTarget {
            uri: "sel://acme/region/7".to_string(),
            host_id: "host-1".to_string(),
            resource_id: 42,
            interface: Some(selium_abi::InterfaceMetadata {
                name: "MyInterface".to_string(),
                methods: vec!["method_a".to_string(), "method_b".to_string()],
            }),
            tenant: Some("acme".to_string()),
            class: selium_abi::ResourceClass::SharedRegion,
            labels: vec![("app".to_string(), "web".to_string())],
        });
        let bytes = FlatMsg::encode(&response);
        let decoded: selium_abi::DiscoveryResponse = FlatMsg::decode(&bytes).expect("decode");
        assert_eq!(decoded, response);
    }

    #[test]
    fn discovery_response_found_without_interface_round_trips() {
        let response = selium_abi::DiscoveryResponse::Found(selium_abi::ResourceTarget {
            uri: "sel://acme/region/7".to_string(),
            host_id: "host-1".to_string(),
            resource_id: 42,
            interface: None,
            tenant: None,
            class: selium_abi::ResourceClass::SharedRegion,
            labels: Vec::new(),
        });
        let bytes = FlatMsg::encode(&response);
        let decoded: selium_abi::DiscoveryResponse = FlatMsg::decode(&bytes).expect("decode");
        assert_eq!(decoded, response);
    }

    #[test]
    fn discovery_prefix_query_round_trips() {
        let request =
            selium_abi::DiscoveryRequest::ResolvePrefix("sel://acme/region/*".to_string());
        let bytes = FlatMsg::encode(&request);
        let decoded: selium_abi::DiscoveryRequest = FlatMsg::decode(&bytes).expect("decode");
        assert_eq!(decoded, request);
    }

    #[test]
    fn discovery_label_query_round_trips() {
        let request = selium_abi::DiscoveryRequest::ResolveLabels {
            key: "app".to_string(),
            value: "web".to_string(),
        };
        let bytes = FlatMsg::encode(&request);
        let decoded: selium_abi::DiscoveryRequest = FlatMsg::decode(&bytes).expect("decode");
        assert_eq!(decoded, request);
    }

    #[test]
    fn discovery_multi_target_response_round_trips() {
        let targets = vec![
            selium_abi::ResourceTarget {
                uri: "sel://acme/proc/1".to_string(),
                host_id: String::new(),
                resource_id: 1,
                interface: None,
                tenant: Some("acme".to_string()),
                class: selium_abi::ResourceClass::Process,
                labels: vec![("app".to_string(), "web".to_string())],
            },
            selium_abi::ResourceTarget {
                uri: "sel://acme/proc/2".to_string(),
                host_id: String::new(),
                resource_id: 2,
                interface: None,
                tenant: Some("acme".to_string()),
                class: selium_abi::ResourceClass::Process,
                labels: vec![("app".to_string(), "web".to_string())],
            },
        ];
        let response = selium_abi::DiscoveryResponse::Resolved(targets);
        let bytes = FlatMsg::encode(&response);
        let decoded: selium_abi::DiscoveryResponse = FlatMsg::decode(&bytes).expect("decode");
        assert_eq!(decoded, response);
    }

    /// Strict decode: an unknown resource class segment is a decode error,
    /// not a silent default.
    #[test]
    fn unknown_resource_class_segment_is_decode_error() {
        let target = selium_abi::ResourceTarget {
            uri: "sel://acme/region/7".to_string(),
            host_id: String::new(),
            resource_id: 7,
            interface: None,
            tenant: Some("acme".to_string()),
            class: selium_abi::ResourceClass::SharedRegion,
            labels: Vec::new(),
        };
        let mut wire = ResourceTargetWire::from(&target);
        wire.class = "not-a-class".to_string();
        let bytes = FlatMsg::encode(&wire);
        let result: ::std::result::Result<selium_abi::ResourceTarget, InvalidFlatbuffer> =
            FlatMsg::decode(&bytes);
        assert!(result.is_err(), "unknown class segment must fail decode");
    }

    /// Strict decode: an unknown request variant tag is a decode error,
    /// not a silently reinterpreted default variant.
    #[test]
    fn unknown_request_variant_is_decode_error() {
        let wire = DiscoveryRequestWire {
            variant: 42,
            uri: "sel://acme/region/7".to_string(),
            key: String::new(),
            value: String::new(),
            target: None,
            root_service: false,
        };
        let bytes = FlatMsg::encode(&wire);
        let result: ::std::result::Result<selium_abi::DiscoveryRequest, InvalidFlatbuffer> =
            FlatMsg::decode(&bytes);
        assert!(result.is_err(), "unknown variant tag must fail decode");
    }

    /// Strict decode: a Register without a target is a decode error, not a
    /// fabricated default target.
    #[test]
    fn register_without_target_is_decode_error() {
        let wire = DiscoveryRequestWire {
            variant: 1,
            uri: "sel://acme/proxy".to_string(),
            key: String::new(),
            value: String::new(),
            target: None,
            root_service: false,
        };
        let bytes = FlatMsg::encode(&wire);
        let result: ::std::result::Result<selium_abi::DiscoveryRequest, InvalidFlatbuffer> =
            FlatMsg::decode(&bytes);
        assert!(result.is_err(), "Register without target must fail decode");
    }

    /// Strict decode: an unknown response variant tag is a decode error,
    /// not a silently reinterpreted default variant.
    #[test]
    fn unknown_response_variant_is_decode_error() {
        let wire = DiscoveryResponseWire {
            variant: 77,
            target: None,
            targets: Vec::new(),
            domains: Vec::new(),
        };
        let bytes = FlatMsg::encode(&wire);
        let result: ::std::result::Result<selium_abi::DiscoveryResponse, InvalidFlatbuffer> =
            FlatMsg::decode(&bytes);
        assert!(result.is_err(), "unknown variant tag must fail decode");
    }

    /// Strict decode: a Found response without a target is a decode error,
    /// not a silent NotFound.
    #[test]
    fn found_without_target_is_decode_error() {
        let wire = DiscoveryResponseWire {
            variant: 0,
            target: None,
            targets: Vec::new(),
            domains: Vec::new(),
        };
        let bytes = FlatMsg::encode(&wire);
        let result: ::std::result::Result<selium_abi::DiscoveryResponse, InvalidFlatbuffer> =
            FlatMsg::decode(&bytes);
        assert!(result.is_err(), "Found without target must fail decode");
    }

    #[test]
    fn control_request_deploy_round_trips() {
        let request = selium_abi::ControlRequest::Deploy {
            workload_id: "api".to_string(),
            replicas: 3,
            module: "api/v1".to_string(),
        };
        let bytes = FlatMsg::encode(&request);
        let decoded: selium_abi::ControlRequest = FlatMsg::decode(&bytes).expect("decode");
        assert_eq!(decoded, request);
    }

    #[test]
    fn control_request_scale_round_trips() {
        let request = selium_abi::ControlRequest::Scale {
            workload_id: "api".to_string(),
            replicas: 5,
        };
        let bytes = FlatMsg::encode(&request);
        let decoded: selium_abi::ControlRequest = FlatMsg::decode(&bytes).expect("decode");
        assert_eq!(decoded, request);
    }

    #[test]
    fn control_request_upload_round_trips_bytes() {
        let request = selium_abi::ControlRequest::Upload {
            manifest: "api/v1".to_string(),
            bytes: vec![0x00, 0x61, 0x73, 0x6d],
        };
        let bytes = FlatMsg::encode(&request);
        let decoded: selium_abi::ControlRequest = FlatMsg::decode(&bytes).expect("decode");
        assert_eq!(decoded, request);
    }

    #[test]
    fn control_request_status_round_trips() {
        let request = selium_abi::ControlRequest::Status {
            workload_id: "api".to_string(),
        };
        let bytes = FlatMsg::encode(&request);
        let decoded: selium_abi::ControlRequest = FlatMsg::decode(&bytes).expect("decode");
        assert_eq!(decoded, request);
    }

    #[test]
    fn control_response_accepted_round_trips() {
        let response = selium_abi::ControlResponse::Accepted {
            workload_id: "api".to_string(),
            replicas: 3,
            module: "api/v1".to_string(),
            delegated: selium_abi::DelegationStatus {
                step: "scheduler".to_string(),
                applied: false,
                context: "scheduler service not yet online".to_string(),
            },
        };
        let bytes = FlatMsg::encode(&response);
        let decoded: selium_abi::ControlResponse = FlatMsg::decode(&bytes).expect("decode");
        assert_eq!(decoded, response);
    }

    #[test]
    fn control_response_resolved_round_trips() {
        let response = selium_abi::ControlResponse::Resolved {
            target: Some(selium_abi::ResolvedTarget {
                uri: "sel://acme/bridge".to_string(),
                host_id: "host-a".to_string(),
                resource_id: 42,
            }),
        };
        let bytes = FlatMsg::encode(&response);
        let decoded: selium_abi::ControlResponse = FlatMsg::decode(&bytes).expect("decode");
        assert_eq!(decoded, response);
    }

    #[test]
    fn control_response_error_round_trips() {
        let response = selium_abi::ControlResponse::Error {
            step: "discovery".to_string(),
            context: "delegation failed".to_string(),
        };
        let bytes = FlatMsg::encode(&response);
        let decoded: selium_abi::ControlResponse = FlatMsg::decode(&bytes).expect("decode");
        assert_eq!(decoded, response);
    }

    #[test]
    fn scheduler_request_place_round_trips() {
        let request = selium_abi::SchedulerRequest::Place {
            workload_id: "api".to_string(),
            replicas: 3,
        };
        let bytes = FlatMsg::encode(&request);
        let decoded: selium_abi::SchedulerRequest = FlatMsg::decode(&bytes).expect("decode");
        assert_eq!(decoded, request);
    }

    #[test]
    fn scheduler_response_deferred_round_trips() {
        let response = selium_abi::SchedulerResponse::Deferred {
            reason: "scheduler not yet online".to_string(),
        };
        let bytes = FlatMsg::encode(&response);
        let decoded: selium_abi::SchedulerResponse = FlatMsg::decode(&bytes).expect("decode");
        assert_eq!(decoded, response);
    }

    /// Strict decode: an unknown control request variant tag is a decode
    /// error, not a silently reinterpreted default variant.
    #[test]
    fn unknown_control_request_variant_is_decode_error() {
        let wire = ControlRequestWire {
            variant: 42,
            workload_id: String::new(),
            replicas: 0,
            module: String::new(),
            uri: String::new(),
            manifest: String::new(),
            bytes: Vec::new(),
        };
        let bytes = FlatMsg::encode(&wire);
        let result: ::std::result::Result<selium_abi::ControlRequest, InvalidFlatbuffer> =
            FlatMsg::decode(&bytes);
        assert!(result.is_err(), "unknown variant tag must fail decode");
    }

    /// Strict decode: an unknown scheduler response variant tag is a decode
    /// error, not a silently reinterpreted default variant.
    #[test]
    fn unknown_scheduler_response_variant_is_decode_error() {
        let wire = SchedulerResponseWire {
            variant: 77,
            reason: String::new(),
        };
        let bytes = FlatMsg::encode(&wire);
        let result: ::std::result::Result<selium_abi::SchedulerResponse, InvalidFlatbuffer> =
            FlatMsg::decode(&bytes);
        assert!(result.is_err(), "unknown variant tag must fail decode");
    }
}
