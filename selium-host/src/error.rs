//! Error types for Selium Host.

use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Guest error: {0}")]
    Guest(String),

    #[error("Capability not found: {0}")]
    CapabilityNotFound(String),

    #[error("Invalid handle: {0}")]
    InvalidHandle(String),

    #[error("Guest exited with: {0:?}")]
    GuestExited(GuestExitStatus),

    #[error("Hostcall error: {0}")]
    Hostcall(String),

    #[error("Deprecated hostcall: {name} (removed in v{removed_version})")]
    DeprecatedHostcall { name: String, removed_version: u32 },

    #[error("WASM error: {0}")]
    Wasm(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Capability revoked")]
    CapabilityRevoked,
}

pub type Result<T> = std::result::Result<T, Error>;

/// Guest exit status types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GuestExitStatus {
    /// Guest completed successfully
    Ok,
    /// Guest returned an error
    Error(String),
    /// Guest requested hot-swap (drain → replace → resume)
    HotSwap,
    /// Guest requested restart
    Restart,
}

impl From<GuestExitStatus> for i32 {
    fn from(status: GuestExitStatus) -> Self {
        match status {
            GuestExitStatus::Ok => 0,
            GuestExitStatus::Error(_) => 1,
            GuestExitStatus::HotSwap => 42,
            GuestExitStatus::Restart => 43,
        }
    }
}
