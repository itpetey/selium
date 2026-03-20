//! Storage capability for async key-value operations.
//!
//! Provides async storage operations that yield to the host,
//! allowing other guests to make progress.

use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;

use crate::kernel::Capability;

pub type Key = Vec<u8>;
pub type Value = Vec<u8>;

#[derive(Debug, Clone)]
pub enum StorageError {
    NotFound,
    PermissionDenied,
    IoError(String),
    Cancelled,
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::NotFound => write!(f, "Key not found"),
            StorageError::PermissionDenied => write!(f, "Permission denied"),
            StorageError::IoError(msg) => write!(f, "IO error: {}", msg),
            StorageError::Cancelled => write!(f, "Operation cancelled"),
        }
    }
}

impl std::error::Error for StorageError {}

pub type StorageResult<T> = std::result::Result<T, StorageError>;

pub struct StorageCapability {
    data: Arc<RwLock<HashMap<Key, Value>>>,
}

impl StorageCapability {
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn get(&self, key: Key) -> StorageResult<Value> {
        let data = self.data.clone();
        
        tokio::task::spawn_blocking(move || {
            let data = data.read();
            data.get(&key)
                .cloned()
                .ok_or(StorageError::NotFound)
        })
        .await
        .map_err(|_| StorageError::IoError("Task cancelled".to_string()))?
    }

    pub async fn put(&self, key: Key, value: Value) -> StorageResult<()> {
        let data = self.data.clone();
        
        tokio::task::spawn_blocking(move || {
            let mut data = data.write();
            data.insert(key, value);
            Ok(())
        })
        .await
        .map_err(|_| StorageError::IoError("Task cancelled".to_string()))?
    }

    pub async fn delete(&self, key: Key) -> StorageResult<()> {
        let data = self.data.clone();
        
        tokio::task::spawn_blocking(move || {
            let mut data = data.write();
            data.remove(&key);
            Ok(())
        })
        .await
        .map_err(|_| StorageError::IoError("Task cancelled".to_string()))?
    }

    pub async fn list_keys(&self, prefix: Option<Key>) -> StorageResult<Vec<Key>> {
        let data = self.data.clone();
        
        tokio::task::spawn_blocking(move || {
            let data = data.read();
            let keys: Vec<Key> = match prefix {
                Some(ref p) => data.keys()
                    .filter(|k| k.starts_with(p))
                    .cloned()
                    .collect(),
                None => data.keys().cloned().collect(),
            };
            Ok(keys)
        })
        .await
        .map_err(|_| StorageError::IoError("Task cancelled".to_string()))?
    }
}

impl Default for StorageCapability {
    fn default() -> Self {
        Self::new()
    }
}

impl Capability for StorageCapability {
    fn name(&self) -> &'static str {
        "selium::storage"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_storage_put_and_get() {
        let storage = StorageCapability::new();
        
        storage.put(b"key1".to_vec(), b"value1".to_vec()).await.unwrap();
        
        let result = storage.get(b"key1".to_vec()).await.unwrap();
        assert_eq!(result, b"value1");
    }

    #[tokio::test]
    async fn test_storage_get_not_found() {
        let storage = StorageCapability::new();
        
        let result = storage.get(b"nonexistent".to_vec()).await;
        assert!(matches!(result, Err(StorageError::NotFound)));
    }

    #[tokio::test]
    async fn test_storage_delete() {
        let storage = StorageCapability::new();
        
        storage.put(b"key1".to_vec(), b"value1".to_vec()).await.unwrap();
        storage.delete(b"key1".to_vec()).await.unwrap();
        
        let result = storage.get(b"key1".to_vec()).await;
        assert!(matches!(result, Err(StorageError::NotFound)));
    }

    #[tokio::test]
    async fn test_storage_list_keys() {
        let storage = StorageCapability::new();
        
        storage.put(b"a:1".to_vec(), b"v1".to_vec()).await.unwrap();
        storage.put(b"a:2".to_vec(), b"v2".to_vec()).await.unwrap();
        storage.put(b"b:1".to_vec(), b"v3".to_vec()).await.unwrap();
        
        let keys = storage.list_keys(Some(b"a:".to_vec())).await.unwrap();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&b"a:1".to_vec()));
        assert!(keys.contains(&b"a:2".to_vec()));
    }
}
