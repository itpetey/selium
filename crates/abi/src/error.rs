//! Structured ABI errors and hostcall preamble errors.

use rkyv::{Archive, Deserialize, Serialize};
use thiserror::Error;

/// Stable error code returned across the host-guest ABI.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum AbiErrorCode {
    /// A supplied handle or operation id is invalid.
    InvalidHandle,
    /// A resource was used after being detached or closed.
    DetachedResource,
    /// The caller does not have the required capability.
    PermissionDenied,
    /// The caller's tenant has exhausted its quota ceiling for the resource
    /// class named in the error message.
    QuotaExceeded,
    /// Payload bytes could not be decoded or framed correctly.
    MalformedPayload,
    /// Requested resource was not found.
    NotFound,
    /// Operation timed out.
    Timeout,
    /// Host-side internal failure.
    Internal,
}

/// Error value returned over the Selium ABI.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct AbiError {
    /// Stable machine-readable error code.
    pub code: AbiErrorCode,
    /// Human-readable error details.
    pub message: String,
}

/// Error returned by rkyv encoding or decoding helpers.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum RkyvError {
    #[error("encode error: {0}")]
    /// Encoding failed.
    Encode(String),
    #[error("decode error: {0}")]
    /// Decoding failed.
    Decode(String),
}

/// Error returned when a hostcall request preamble is missing or incompatible.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum HostcallPreambleError {
    /// The request is too short to carry the fixed preamble.
    #[error("hostcall request is missing the ABI preamble")]
    Missing,
    /// The leading magic value is not the Selium hostcall magic.
    #[error("hostcall request has invalid magic {found:#010x}")]
    BadMagic {
        /// Magic value supplied by the sender.
        found: u32,
    },
    /// The sender's ABI version is not the host's supported version.
    #[error("host ABI version {host} is incompatible with guest ABI version {guest}")]
    VersionMismatch {
        /// Version supported by the host.
        host: u32,
        /// Version declared by the guest request.
        guest: u32,
    },
}

impl HostcallPreambleError {
    /// The structured ABI error this rejection is reported as.
    ///
    /// The message names both ABI versions, so a refused guest can report why
    /// it was refused (and against which host version) without reading the
    /// host's log.
    pub fn to_abi_error(&self) -> AbiError {
        AbiError::new(AbiErrorCode::MalformedPayload, self.to_string())
    }
}

impl AbiError {
    /// Creates an ABI error with the supplied code and message.
    pub fn new(code: AbiErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}
