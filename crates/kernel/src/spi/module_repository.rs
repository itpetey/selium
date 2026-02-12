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

#[cfg(test)]
mod tests {
    use super::*;

    struct Repo;

    impl ModuleRepositoryReadCapability for Repo {
        fn read(&self, _module_id: &str) -> Result<Vec<u8>, ModuleRepositoryError> {
            Ok(vec![1, 2, 3])
        }
    }

    #[test]
    fn error_codes_match_contract() {
        assert_eq!(
            i32::from(ModuleRepositoryError::InvalidPath(
                PathBuf::from("."),
                "x".into()
            )),
            -300
        );
        assert_eq!(
            i32::from(ModuleRepositoryError::Filesystem("x".into())),
            -301
        );
    }

    #[test]
    fn repository_trait_can_be_used_via_trait_object() {
        let repo: &dyn ModuleRepositoryReadCapability = &Repo;
        let bytes = repo.read("module.wasm").expect("read");
        assert_eq!(bytes, vec![1, 2, 3]);
    }
}
