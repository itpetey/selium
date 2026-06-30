use selium_abi::{AbiError, Capability};
use thiserror::Error;

use std::io;

/// Result type used by the Selium guest SDK.
pub type Result<T> = std::result::Result<T, GuestError>;

/// Error returned by guest SDK operations.
#[derive(Debug, Error)]
pub enum GuestError {
    #[error("host error: {0}")]
    /// Host-side operation failed.
    Host(String),
    #[error("codec error: {0}")]
    /// Encoding or decoding failed.
    Codec(#[from] selium_abi::RkyvError),
    #[error("permission denied for capability {0:?}")]
    /// The host denied access to a capability.
    PermissionDenied(Capability),
    #[error("unexpected hostcall output")]
    /// The host returned a response variant that did not match the request.
    UnexpectedHostcallOutput,
    #[error("builder is sealed")]
    /// Builder does not accept further modifications.
    BuilderSealed,
    #[error("capacity exceeded")]
    /// Capacity limit exceeded.
    CapacityExceeded,
    #[error("I/O error: {0}")]
    Io(io::Error),
}

pub(crate) fn abi_error_to_guest_error(error: AbiError) -> GuestError {
    if error.code == selium_abi::AbiErrorCode::PermissionDenied {
        GuestError::PermissionDenied(Capability::SharedMemory)
    } else {
        GuestError::Host(format!("{:?}: {}", error.code, error.message))
    }
}
