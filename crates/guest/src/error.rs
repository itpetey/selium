use selium_abi::{AbiError, Capability};
use thiserror::Error;

pub type Result<T> = std::result::Result<T, GuestError>;

#[derive(Debug, Error)]
pub enum GuestError {
    #[error("host error: {0}")]
    Host(String),
    #[error("codec error: {0}")]
    Codec(#[from] selium_abi::RkyvError),
    #[error("permission denied for capability {0:?}")]
    PermissionDenied(Capability),
    #[error("unexpected hostcall output")]
    UnexpectedHostcallOutput,
}

pub(crate) fn abi_error_to_guest_error(error: AbiError) -> GuestError {
    if error.code == selium_abi::AbiErrorCode::PermissionDenied {
        GuestError::PermissionDenied(Capability::Signal)
    } else {
        GuestError::Host(format!("{:?}: {}", error.code, error.message))
    }
}
