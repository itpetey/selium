use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum GuestError {
    #[error("invalid argument")]
    InvalidArgument,
    #[error("resource not found")]
    NotFound,
    #[error("permission denied")]
    PermissionDenied,
    #[error("operation would block")]
    WouldBlock,
    #[error("internal error: {0}")]
    Kernel(String),
}

#[allow(dead_code)]
pub type GuestResult<T, E = GuestError> = Result<T, E>;

#[allow(dead_code)]
#[derive(Debug, Error)]
pub enum EncodeError {
    #[error("encode failed: {0}")]
    Encode(String),
    #[error("decode failed: {0}")]
    Decode(String),
}
