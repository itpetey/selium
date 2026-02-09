//! Guest-facing error types shared across hostcall contracts.

use thiserror::Error;

use crate::{KernelError, registry::RegistryError};

/// Result type used for guest-visible operations.
pub type GuestResult<T, E = GuestError> = Result<T, E>;

/// Errors surfaced to guest callers through hostcall contracts.
#[derive(Error, Debug)]
pub enum GuestError {
    /// Invalid argument received from guest input.
    #[error("invalid argument")]
    InvalidArgument,
    /// Guest-provided UTF-8 payload is invalid.
    #[error("Invalid UTF-8 in guest input")]
    InvalidUtf8,
    /// Guest memory slice is invalid.
    #[error("Invalid guest memory slice")]
    MemorySlice,
    /// Requested resource was not found.
    #[error("resource not found")]
    NotFound,
    /// Caller does not have sufficient permissions.
    #[error("permission denied")]
    PermissionDenied,
    /// Internal kernel error.
    #[error("The kernel encountered an error. Please report this to your administrator.")]
    Kernel(#[from] KernelError),
    /// Internal registry error.
    #[error("The kernel Registry encountered an error. Please report this to your administrator.")]
    Registry(#[from] RegistryError),
    /// Stable identifier already exists.
    #[error("Stable identifier already exists")]
    StableIdExists,
    /// Subsystem-specific internal error.
    #[error("internal error: {0}")]
    Subsystem(String),
    /// Requested operation would block.
    #[error("This function would block")]
    WouldBlock,
}
