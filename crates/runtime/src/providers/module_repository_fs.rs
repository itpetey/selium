//! Host-side module repository backed by the runtime filesystem.

use std::{
    fs::File,
    io::Read,
    path::{Path, PathBuf},
};

use path_security::validate_path;
use selium_kernel::spi::module_repository::{
    ModuleRepositoryError, ModuleRepositoryReadCapability,
};

/// Filesystem implementation of [`ModuleRepositoryReadCapability`].
pub struct FilesystemModuleRepository {
    base_dir: PathBuf,
}

impl FilesystemModuleRepository {
    /// Create a new filesystem-backed module repository.
    pub fn new(base_dir: impl AsRef<Path>) -> Self {
        Self {
            base_dir: base_dir.as_ref().into(),
        }
    }
}

impl ModuleRepositoryReadCapability for FilesystemModuleRepository {
    fn read(&self, module_id: &str) -> Result<Vec<u8>, ModuleRepositoryError> {
        let module_path = Path::new(module_id);
        let full_path = validate_path(module_path, &self.base_dir).map_err(|err| {
            ModuleRepositoryError::InvalidPath(self.base_dir.join(module_path), err.to_string())
        })?;

        let mut file = File::open(full_path)
            .map_err(|err| ModuleRepositoryError::Filesystem(err.to_string()))?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)
            .map_err(|err| ModuleRepositoryError::Filesystem(err.to_string()))?;
        Ok(bytes)
    }
}
