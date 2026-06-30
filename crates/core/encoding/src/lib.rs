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
}

/// Wire type for DiscoveryRequest, backed by Flatbuffers.
#[derive(Debug, Clone, PartialEq)]
#[schema(
    path = "schemas/discovery.fbs",
    ty = "selium.discovery.DiscoveryRequest",
    binding = "selium_encoding::fbs::selium::discovery::DiscoveryRequest"
)]
pub struct DiscoveryRequestWire {
    /// Variant discriminator (0 = Resolve, 1 = Register, 2 = Revoke).
    pub variant: u8,
    /// URI to resolve or register.
    pub uri: String,
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
    /// Variant discriminator (0 = Found, 1 = NotFound, 2 = Registered, 3 = Revoked, 4 = Forbidden).
    pub variant: u8,
    /// The discovered resource (used by Found variant).
    pub target: Option<ResourceTargetWire>,
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

impl From<&selium_abi::ResourceTarget> for ResourceTargetWire {
    fn from(value: &selium_abi::ResourceTarget) -> Self {
        Self::new(
            value.uri.clone(),
            value.host_id.clone(),
            value.resource_id,
            value.interface.as_ref().map(InterfaceMetadataWire::from),
            value.tenant.clone(),
        )
    }
}

impl From<&selium_abi::DiscoveryRequest> for DiscoveryRequestWire {
    fn from(value: &selium_abi::DiscoveryRequest) -> Self {
        match value {
            selium_abi::DiscoveryRequest::Resolve(uri) => Self::new(0, uri.clone(), None),
            selium_abi::DiscoveryRequest::Register { uri, target } => {
                Self::new(1, uri.clone(), Some(ResourceTargetWire::from(target)))
            }
            selium_abi::DiscoveryRequest::Revoke { uri } => Self::new(2, uri.clone(), None),
        }
    }
}

impl From<&selium_abi::DiscoveryResponse> for DiscoveryResponseWire {
    fn from(value: &selium_abi::DiscoveryResponse) -> Self {
        match value {
            selium_abi::DiscoveryResponse::Found(target) => {
                Self::new(0, Some(ResourceTargetWire::from(target)))
            }
            selium_abi::DiscoveryResponse::NotFound => Self::new(1, None),
            selium_abi::DiscoveryResponse::Registered => Self::new(2, None),
            selium_abi::DiscoveryResponse::Revoked => Self::new(3, None),
            selium_abi::DiscoveryResponse::Forbidden => Self::new(4, None),
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

impl From<ResourceTargetWire> for selium_abi::ResourceTarget {
    fn from(wire: ResourceTargetWire) -> Self {
        Self {
            uri: wire.uri,
            host_id: wire.host_id,
            resource_id: wire.resource_id,
            interface: wire.interface.map(selium_abi::InterfaceMetadata::from),
            tenant: wire.tenant,
        }
    }
}

impl FlatMsg for selium_abi::ResourceTarget {
    fn encode(value: &Self) -> Vec<u8> {
        let wire = ResourceTargetWire::from(value);
        FlatMsg::encode(&wire)
    }

    fn decode(bytes: &[u8]) -> ::std::result::Result<Self, InvalidFlatbuffer> {
        let wire: ResourceTargetWire = FlatMsg::decode(bytes)?;
        Ok(Self::from(wire))
    }
}

impl HasSchema for selium_abi::ResourceTarget {
    const SCHEMA: SchemaDescriptor = ResourceTargetWireSchema;
}

impl From<DiscoveryRequestWire> for selium_abi::DiscoveryRequest {
    fn from(wire: DiscoveryRequestWire) -> Self {
        match wire.variant {
            0 => selium_abi::DiscoveryRequest::Resolve(wire.uri),
            1 => {
                let target = wire.target.map(selium_abi::ResourceTarget::from).unwrap_or(
                    selium_abi::ResourceTarget {
                        uri: String::new(),
                        host_id: String::new(),
                        resource_id: 0,
                        interface: None,
                        tenant: None,
                    },
                );
                selium_abi::DiscoveryRequest::Register {
                    uri: wire.uri,
                    target,
                }
            }
            2 => selium_abi::DiscoveryRequest::Revoke { uri: wire.uri },
            _ => selium_abi::DiscoveryRequest::Resolve(String::new()),
        }
    }
}

impl FlatMsg for selium_abi::DiscoveryRequest {
    fn encode(value: &Self) -> Vec<u8> {
        let wire = DiscoveryRequestWire::from(value);
        FlatMsg::encode(&wire)
    }

    fn decode(bytes: &[u8]) -> ::std::result::Result<Self, InvalidFlatbuffer> {
        let wire: DiscoveryRequestWire = FlatMsg::decode(bytes)?;
        Ok(Self::from(wire))
    }
}

impl HasSchema for selium_abi::DiscoveryRequest {
    const SCHEMA: SchemaDescriptor = DiscoveryRequestWireSchema;
}

impl From<DiscoveryResponseWire> for selium_abi::DiscoveryResponse {
    fn from(wire: DiscoveryResponseWire) -> Self {
        match wire.variant {
            0 => {
                if let Some(target) = wire.target {
                    selium_abi::DiscoveryResponse::Found(selium_abi::ResourceTarget::from(target))
                } else {
                    selium_abi::DiscoveryResponse::NotFound
                }
            }
            1 => selium_abi::DiscoveryResponse::NotFound,
            2 => selium_abi::DiscoveryResponse::Registered,
            3 => selium_abi::DiscoveryResponse::Revoked,
            4 => selium_abi::DiscoveryResponse::Forbidden,
            _ => selium_abi::DiscoveryResponse::NotFound,
        }
    }
}

impl FlatMsg for selium_abi::DiscoveryResponse {
    fn encode(value: &Self) -> Vec<u8> {
        let wire = DiscoveryResponseWire::from(value);
        FlatMsg::encode(&wire)
    }

    fn decode(bytes: &[u8]) -> ::std::result::Result<Self, InvalidFlatbuffer> {
        let wire: DiscoveryResponseWire = FlatMsg::decode(bytes)?;
        Ok(Self::from(wire))
    }
}

impl HasSchema for selium_abi::DiscoveryResponse {
    const SCHEMA: SchemaDescriptor = DiscoveryResponseWireSchema;
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
            uri: "sel://tenant/app/api".to_string(),
            host_id: "host-1".to_string(),
            resource_id: 42,
            interface: Some(selium_abi::InterfaceMetadata {
                name: "MyInterface".to_string(),
                methods: vec!["method_a".to_string(), "method_b".to_string()],
            }),
            tenant: None,
        });
        let bytes = FlatMsg::encode(&response);
        let decoded: selium_abi::DiscoveryResponse = FlatMsg::decode(&bytes).expect("decode");
        assert_eq!(decoded, response);
    }

    #[test]
    fn discovery_response_found_without_interface_round_trips() {
        let response = selium_abi::DiscoveryResponse::Found(selium_abi::ResourceTarget {
            uri: "sel://tenant/app/api".to_string(),
            host_id: "host-1".to_string(),
            resource_id: 42,
            interface: None,
            tenant: None,
        });
        let bytes = FlatMsg::encode(&response);
        let decoded: selium_abi::DiscoveryResponse = FlatMsg::decode(&bytes).expect("decode");
        assert_eq!(decoded, response);
    }
}
