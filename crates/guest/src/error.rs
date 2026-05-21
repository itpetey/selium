use selium_abi::{AbiError, Capability};
use thiserror::Error;

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
}

pub(crate) fn abi_error_to_guest_error(error: AbiError) -> GuestError {
    if error.code == selium_abi::AbiErrorCode::PermissionDenied {
        GuestError::PermissionDenied(Capability::Signal)
    } else {
        GuestError::Host(format!("{:?}: {}", error.code, error.message))
    }
}
