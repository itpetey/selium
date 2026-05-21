use selium_abi::{BlobStoreDescriptor, DurableLogDescriptor, SharedResourceId, StorageRecord};
use sha2::{Digest, Sha256};

use crate::{
    Error, Result,
    state::{BlobStoreState, DurableLogState, Kernel},
};

fn hex_char(value: u8) -> char {
    const HEX: [char; 16] = [
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f',
    ];
    // SAFETY: `value & 0x0f` is always in 0..=15, within bounds of `HEX`.
    unsafe { *HEX.get_unchecked((value & 0x0f) as usize) }
}

impl Kernel {
    /// Opens or creates a named durable log.
    pub fn open_log(&self, name: impl Into<String>) -> DurableLogDescriptor {
        let name = name.into();
        let mut logs = self.inner.durable_logs_by_shared.lock();
        let mut local_logs = self.inner.local_logs.lock();
        let shared_id =
            if let Some((shared_id, _)) = logs.iter().find(|(_, state)| state.name == name) {
                *shared_id
            } else {
                let shared_id = self.next_shared_id();
                logs.insert(
                    shared_id,
                    DurableLogState {
                        name: name.clone(),
                        next_sequence: 1,
                        ..DurableLogState::default()
                    },
                );
                shared_id
            };
        let local_id = self.next_local_id();
        local_logs.insert(local_id, shared_id);
        DurableLogDescriptor {
            local_id,
            shared_id,
            name,
        }
    }

    /// Appends a record to a durable log and returns its sequence number.
    pub fn append_log(
        &self,
        local_id: u64,
        timestamp_ms: u64,
        headers: Vec<(String, String)>,
        payload: Vec<u8>,
    ) -> Result<u64> {
        let shared_id = self.log_shared_id(local_id)?;
        let mut logs = self.inner.durable_logs_by_shared.lock();
        let log = logs
            .get_mut(&shared_id)
            .ok_or_else(|| Error::NotFound(format!("durable log {shared_id}")))?;
        let sequence = log.next_sequence;
        log.next_sequence += 1;
        log.records.push(StorageRecord {
            sequence,
            timestamp_ms,
            headers,
            payload,
        });
        Ok(sequence)
    }

    /// Replays records from a durable log.
    pub fn replay_log(
        &self,
        local_id: u64,
        from_sequence: Option<u64>,
        limit: usize,
    ) -> Result<Vec<StorageRecord>> {
        let shared_id = self.log_shared_id(local_id)?;
        let logs = self.inner.durable_logs_by_shared.lock();
        let log = logs
            .get(&shared_id)
            .ok_or_else(|| Error::NotFound(format!("durable log {shared_id}")))?;
        Ok(log
            .records
            .iter()
            .filter(|record| from_sequence.is_none_or(|from| record.sequence >= from))
            .take(limit)
            .cloned()
            .collect())
    }

    /// Stores a named checkpoint for a durable log.
    pub fn checkpoint_log(
        &self,
        local_id: u64,
        name: impl Into<String>,
        sequence: u64,
    ) -> Result<()> {
        let shared_id = self.log_shared_id(local_id)?;
        let mut logs = self.inner.durable_logs_by_shared.lock();
        let log = logs
            .get_mut(&shared_id)
            .ok_or_else(|| Error::NotFound(format!("durable log {shared_id}")))?;
        if !log.records.iter().any(|record| record.sequence == sequence) {
            return Err(Error::NotFound(format!("log sequence {sequence}")));
        }
        log.checkpoints.insert(name.into(), sequence);
        Ok(())
    }

    /// Reads a named checkpoint sequence from a durable log.
    pub fn checkpoint_sequence(&self, local_id: u64, name: &str) -> Result<Option<u64>> {
        let shared_id = self.log_shared_id(local_id)?;
        let logs = self.inner.durable_logs_by_shared.lock();
        let log = logs
            .get(&shared_id)
            .ok_or_else(|| Error::NotFound(format!("durable log {shared_id}")))?;
        Ok(log.checkpoints.get(name).copied())
    }

    /// Opens or creates a named blob store.
    pub fn open_blob_store(&self, name: impl Into<String>) -> BlobStoreDescriptor {
        let name = name.into();
        let mut stores = self.inner.blob_stores_by_shared.lock();
        let mut local_blob_stores = self.inner.local_blob_stores.lock();
        let shared_id =
            if let Some((shared_id, _)) = stores.iter().find(|(_, state)| state.name == name) {
                *shared_id
            } else {
                let shared_id = self.next_shared_id();
                stores.insert(
                    shared_id,
                    BlobStoreState {
                        name: name.clone(),
                        ..BlobStoreState::default()
                    },
                );
                shared_id
            };
        let local_id = self.next_local_id();
        local_blob_stores.insert(local_id, shared_id);
        BlobStoreDescriptor {
            local_id,
            shared_id,
            name,
        }
    }

    /// Stores bytes in a blob store and returns the blob id.
    pub fn put_blob(&self, local_id: u64, bytes: Vec<u8>) -> Result<String> {
        let shared_id = self.blob_store_shared_id(local_id)?;
        let mut stores = self.inner.blob_stores_by_shared.lock();
        let store = stores
            .get_mut(&shared_id)
            .ok_or_else(|| Error::NotFound(format!("blob store {shared_id}")))?;
        let digest = Sha256::digest(&bytes);
        let mut blob_id = String::with_capacity(digest.len() * 2);
        for byte in digest {
            blob_id.push(hex_char(byte >> 4));
            blob_id.push(hex_char(byte & 0x0f));
        }
        store.blobs.insert(blob_id.clone(), bytes);
        Ok(blob_id)
    }

    /// Reads a blob from a blob store by id.
    pub fn get_blob(&self, local_id: u64, blob_id: &str) -> Result<Option<Vec<u8>>> {
        let shared_id = self.blob_store_shared_id(local_id)?;
        let stores = self.inner.blob_stores_by_shared.lock();
        let store = stores
            .get(&shared_id)
            .ok_or_else(|| Error::NotFound(format!("blob store {shared_id}")))?;
        Ok(store.blobs.get(blob_id).cloned())
    }

    /// Sets a named manifest to reference an existing blob id.
    pub fn set_manifest(
        &self,
        local_id: u64,
        name: impl Into<String>,
        blob_id: impl Into<String>,
    ) -> Result<()> {
        let shared_id = self.blob_store_shared_id(local_id)?;
        let mut stores = self.inner.blob_stores_by_shared.lock();
        let store = stores
            .get_mut(&shared_id)
            .ok_or_else(|| Error::NotFound(format!("blob store {shared_id}")))?;
        let blob_id = blob_id.into();
        if !store.blobs.contains_key(&blob_id) {
            return Err(Error::NotFound(format!("blob {blob_id}")));
        }
        store.manifests.insert(name.into(), blob_id);
        Ok(())
    }

    /// Reads the blob id associated with a named manifest.
    pub fn get_manifest(&self, local_id: u64, name: &str) -> Result<Option<String>> {
        let shared_id = self.blob_store_shared_id(local_id)?;
        let stores = self.inner.blob_stores_by_shared.lock();
        let store = stores
            .get(&shared_id)
            .ok_or_else(|| Error::NotFound(format!("blob store {shared_id}")))?;
        Ok(store.manifests.get(name).cloned())
    }

    /// Closes a local durable log handle.
    pub fn close_log(&self, local_id: u64) -> Result<()> {
        let mut local_logs = self.inner.local_logs.lock();
        local_logs
            .remove(&local_id)
            .ok_or_else(|| Error::NotFound(format!("durable log {local_id}")))?;
        Ok(())
    }

    /// Closes a local blob store handle.
    pub fn close_blob_store(&self, local_id: u64) -> Result<()> {
        let mut local_blob_stores = self.inner.local_blob_stores.lock();
        local_blob_stores
            .remove(&local_id)
            .ok_or_else(|| Error::NotFound(format!("blob store {local_id}")))?;
        Ok(())
    }

    /// Returns the shared log id associated with a local log handle.
    pub fn log_shared_id_public(&self, local_id: u64) -> Result<SharedResourceId> {
        self.log_shared_id(local_id)
    }

    /// Returns the shared blob store id associated with a local blob store handle.
    pub fn blob_store_shared_id_public(&self, local_id: u64) -> Result<SharedResourceId> {
        self.blob_store_shared_id(local_id)
    }

    pub(crate) fn log_shared_id(&self, local_id: u64) -> Result<SharedResourceId> {
        self.inner
            .local_logs
            .lock()
            .get(&local_id)
            .copied()
            .ok_or_else(|| Error::NotFound(format!("durable log {local_id}")))
    }

    pub(crate) fn blob_store_shared_id(&self, local_id: u64) -> Result<SharedResourceId> {
        self.inner
            .local_blob_stores
            .lock()
            .get(&local_id)
            .copied()
            .ok_or_else(|| Error::NotFound(format!("blob store {local_id}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durable_log_replay_and_checkpoint_work() {
        let kernel = Kernel::default();
        let log = kernel.open_log("audit");
        let sequence = kernel
            .append_log(
                log.local_id,
                7,
                vec![("kind".to_string(), "test".to_string())],
                b"hello".to_vec(),
            )
            .expect("append log");
        kernel
            .checkpoint_log(log.local_id, "boot", sequence)
            .expect("checkpoint log");

        let replay = kernel
            .replay_log(log.local_id, Some(sequence), 10)
            .expect("replay log");
        assert_eq!(replay.len(), 1);
        assert_eq!(replay[0].payload, b"hello".to_vec());
        assert_eq!(
            kernel
                .checkpoint_sequence(log.local_id, "boot")
                .expect("checkpoint read"),
            Some(sequence)
        );
        assert!(matches!(
            kernel.checkpoint_log(log.local_id, "missing", sequence + 1),
            Err(Error::NotFound(_))
        ));
    }

    #[test]
    fn blob_store_put_get_and_manifest_work() {
        let kernel = Kernel::default();
        let store = kernel.open_blob_store("assets");
        let blob_id = kernel
            .put_blob(store.local_id, b"blob".to_vec())
            .expect("put blob");
        kernel
            .set_manifest(store.local_id, "latest", blob_id.clone())
            .expect("set manifest");

        assert_eq!(
            kernel.get_blob(store.local_id, &blob_id).expect("get blob"),
            Some(b"blob".to_vec())
        );
        assert_eq!(
            kernel
                .get_manifest(store.local_id, "latest")
                .expect("get manifest"),
            Some(blob_id)
        );
        assert!(matches!(
            kernel.set_manifest(store.local_id, "broken", "missing"),
            Err(Error::NotFound(_))
        ));
    }
}
