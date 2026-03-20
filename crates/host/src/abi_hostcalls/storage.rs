use selium_abi::{Capability, GuestContext};
use wasmtime::Linker;

pub struct StorageCapability {
    data: std::collections::HashMap<Vec<u8>, Vec<u8>>,
}

impl StorageCapability {
    pub fn new() -> Self {
        Self {
            data: std::collections::HashMap::new(),
        }
    }

    pub fn get(&self, key: &[u8]) -> Result<&Vec<u8>, selium_abi::GuestError> {
        self.data.get(key).ok_or(selium_abi::GuestError::NotFound)
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), selium_abi::GuestError> {
        self.data.insert(key, value);
        Ok(())
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<(), selium_abi::GuestError> {
        self.data.remove(key);
        Ok(())
    }

    pub fn list_keys(&self, prefix: Option<&[u8]>) -> Vec<Vec<u8>> {
        match prefix {
            Some(p) => self
                .data
                .keys()
                .filter(|k| k.starts_with(p))
                .cloned()
                .collect(),
            None => self.data.keys().cloned().collect(),
        }
    }
}

impl Default for StorageCapability {
    fn default() -> Self {
        Self::new()
    }
}

use std::sync::OnceLock;
static STORAGE: OnceLock<StorageCapability> = OnceLock::new();

pub fn global_storage() -> &'static StorageCapability {
    STORAGE.get_or_init(StorageCapability::new)
}

pub fn storage_get(_ctx: &GuestContext, key: &[u8]) -> Result<Vec<u8>, selium_abi::GuestError> {
    if !_ctx.has_capability(Capability::StorageLogRead)
        && !_ctx.has_capability(Capability::StorageBlobRead)
    {
        return Err(selium_abi::GuestError::PermissionDenied);
    }
    global_storage().get(key).cloned()
}

pub fn storage_put(
    _ctx: &GuestContext,
    key: Vec<u8>,
    value: Vec<u8>,
) -> Result<(), selium_abi::GuestError> {
    if !_ctx.has_capability(Capability::StorageLogWrite)
        && !_ctx.has_capability(Capability::StorageBlobWrite)
    {
        return Err(selium_abi::GuestError::PermissionDenied);
    }
    let storage = global_storage() as *const StorageCapability as *mut StorageCapability;
    unsafe { (*storage).put(key, value) }
}

pub fn storage_delete(_ctx: &GuestContext, key: &[u8]) -> Result<(), selium_abi::GuestError> {
    if !_ctx.has_capability(Capability::StorageLogWrite)
        && !_ctx.has_capability(Capability::StorageBlobWrite)
    {
        return Err(selium_abi::GuestError::PermissionDenied);
    }
    let storage = global_storage() as *const StorageCapability as *mut StorageCapability;
    unsafe { (*storage).delete(key) }
}

pub fn storage_list_keys(_ctx: &GuestContext, prefix: Option<&[u8]>) -> Vec<Vec<u8>> {
    if !_ctx.has_capability(Capability::StorageLogRead)
        && !_ctx.has_capability(Capability::StorageBlobRead)
    {
        return Vec::new();
    }
    global_storage().list_keys(prefix)
}

pub fn add_to_linker(_linker: &mut Linker<GuestContext>) -> anyhow::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_ctx(caps: impl IntoIterator<Item = Capability>) -> GuestContext {
        GuestContext::with_capabilities(1, caps)
    }

    #[test]
    fn test_storage_put_and_get() {
        let ctx = test_ctx([Capability::StorageLogWrite, Capability::StorageLogRead]);
        storage_put(&ctx, b"key1".to_vec(), b"value1".to_vec()).unwrap();
        let result = storage_get(&ctx, b"key1").unwrap();
        assert_eq!(result, b"value1");
    }

    #[test]
    fn test_storage_get_not_found() {
        let ctx = test_ctx([Capability::StorageLogRead]);
        let result = storage_get(&ctx, b"nonexistent");
        assert!(matches!(result, Err(selium_abi::GuestError::NotFound)));
    }

    #[test]
    fn test_storage_delete() {
        let ctx = test_ctx([Capability::StorageLogWrite, Capability::StorageLogRead]);
        storage_put(&ctx, b"key1".to_vec(), b"value1".to_vec()).unwrap();
        storage_delete(&ctx, b"key1").unwrap();
        let result = storage_get(&ctx, b"key1");
        assert!(matches!(result, Err(selium_abi::GuestError::NotFound)));
    }

    #[test]
    fn test_storage_list_keys() {
        let ctx = test_ctx([Capability::StorageLogWrite, Capability::StorageLogRead]);
        storage_put(&ctx, b"a:1".to_vec(), b"v1".to_vec()).unwrap();
        storage_put(&ctx, b"a:2".to_vec(), b"v2".to_vec()).unwrap();
        storage_put(&ctx, b"b:1".to_vec(), b"v3".to_vec()).unwrap();

        let keys = storage_list_keys(&ctx, Some(b"a:"));
        assert_eq!(keys.len(), 2);
    }
}
