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

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    fn temp_dir() -> PathBuf {
        let id = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("selium-module-repo-{id}"));
        fs::create_dir_all(&path).expect("create temp dir");
        path
    }

    #[test]
    fn reads_existing_module_bytes() {
        let dir = temp_dir();
        let module_path = dir.join("module.wasm");
        fs::write(&module_path, [1u8, 2, 3]).expect("write module");

        let repo = FilesystemModuleRepository::new(&dir);
        let bytes = repo.read("module.wasm").expect("read module");
        assert_eq!(bytes, vec![1, 2, 3]);
    }

    #[test]
    fn rejects_path_escape_attempts() {
        let dir = temp_dir();
        let repo = FilesystemModuleRepository::new(&dir);

        let err = repo
            .read("../outside.wasm")
            .expect_err("expected invalid path");
        assert!(matches!(err, ModuleRepositoryError::InvalidPath(_, _)));
    }

    #[test]
    fn surfaces_filesystem_errors_for_missing_file() {
        let dir = temp_dir();
        let repo = FilesystemModuleRepository::new(&dir);

        let err = repo.read("missing.wasm").expect_err("missing file");
        assert!(matches!(err, ModuleRepositoryError::Filesystem(_)));
    }
}
