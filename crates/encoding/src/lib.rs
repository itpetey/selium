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

/// Wire type for DiscoveryRequest, backed by Flatbuffers.
#[derive(Debug, Clone, PartialEq)]
#[schema(
    path = "schemas/discovery.fbs",
    ty = "selium.discovery.DiscoveryRequest",
    binding = "selium_encoding::fbs::selium::discovery::DiscoveryRequest"
)]
pub struct DiscoveryRequestWire {
    /// Variant discriminator (0 = Resolve, 1 = Register, 2 = Revoke,
    /// 3 = ResolvePrefix, 4 = ResolveLabels).
    pub variant: u8,
    /// URI to resolve or register (Resolve, ResolvePrefix, Register, Revoke).
    pub uri: String,
    /// Label key (ResolveLabels variant).
    pub key: String,
    /// Label value (ResolveLabels variant).
    pub value: String,
    /// Target resource for registration (used by Register variant).
    pub target: Option<ResourceTargetWire>,
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
    /// 3 = Revoked, 4 = Forbidden, 5 = Resolved).
    pub variant: u8,
    /// The discovered resource (used by Found variant).
    pub target: Option<ResourceTargetWire>,
    /// The matched resources (used by Resolved variant).
    pub targets: Vec<ResourceTargetWire>,
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

impl From<&selium_abi::DiscoveryRequest> for DiscoveryRequestWire {
    fn from(value: &selium_abi::DiscoveryRequest) -> Self {
        match value {
            selium_abi::DiscoveryRequest::Resolve(uri) => {
                Self::new(0, uri.clone(), String::new(), String::new(), None)
            }
            selium_abi::DiscoveryRequest::ResolvePrefix(uri) => {
                Self::new(3, uri.clone(), String::new(), String::new(), None)
            }
            selium_abi::DiscoveryRequest::ResolveLabels {
                key,
                value: value_str,
            } => Self::new(4, String::new(), key.clone(), value_str.clone(), None),
            selium_abi::DiscoveryRequest::Register { uri, target, .. } => Self::new(
                1,
                uri.clone(),
                String::new(),
                String::new(),
                Some(ResourceTargetWire::from(target)),
            ),
            selium_abi::DiscoveryRequest::Revoke { uri } => {
                Self::new(2, uri.clone(), String::new(), String::new(), None)
            }
        }
    }
}

impl From<&selium_abi::DiscoveryResponse> for DiscoveryResponseWire {
    fn from(value: &selium_abi::DiscoveryResponse) -> Self {
        match value {
            selium_abi::DiscoveryResponse::Found(target) => {
                Self::new(0, Some(ResourceTargetWire::from(target)), Vec::new())
            }
            selium_abi::DiscoveryResponse::NotFound => Self::new(1, None, Vec::new()),
            selium_abi::DiscoveryResponse::Registered => Self::new(2, None, Vec::new()),
            selium_abi::DiscoveryResponse::Revoked => Self::new(3, None, Vec::new()),
            selium_abi::DiscoveryResponse::Forbidden => Self::new(4, None, Vec::new()),
            selium_abi::DiscoveryResponse::Resolved(targets) => Self::new(
                5,
                None,
                targets.iter().map(ResourceTargetWire::from).collect(),
            ),
        }
    }
}

impl From<LabelWire> for (String, String) {
    fn from(value: LabelWire) -> Self {
        (value.key, value.value)
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
            })
        }
        2 => Ok(selium_abi::DiscoveryRequest::Revoke { uri: wire.uri }),
        3 => Ok(selium_abi::DiscoveryRequest::ResolvePrefix(wire.uri)),
        4 => Ok(selium_abi::DiscoveryRequest::ResolveLabels {
            key: wire.key,
            value: wire.value,
        }),
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
        };
        let bytes = FlatMsg::encode(&wire);
        let result: ::std::result::Result<selium_abi::DiscoveryResponse, InvalidFlatbuffer> =
            FlatMsg::decode(&bytes);
        assert!(result.is_err(), "Found without target must fail decode");
    }
}
