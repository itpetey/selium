//! Module repository traits and errors for host-side module loading.

use std::path::PathBuf;

use thiserror::Error;

/// Capability for reading module bytes from a host-side repository.
pub trait ModuleRepositoryReadCapability {
    /// Read a module by identifier.
    fn read(&self, module_id: &str) -> Result<Vec<u8>, ModuleRepositoryError>;
}

#[derive(Error, Debug)]
/// Error type returned by module repository implementations.
pub enum ModuleRepositoryError {
    #[error("Path validation failed for {0:?}: {1}")]
    InvalidPath(PathBuf, String),
    #[error("Error reading filesystem: {0}")]
    Filesystem(String),
}

impl From<ModuleRepositoryError> for i32 {
    fn from(value: ModuleRepositoryError) -> Self {
        match value {
            ModuleRepositoryError::InvalidPath(_, _) => -300,
            ModuleRepositoryError::Filesystem(_) => -301,
        }
    }
}
