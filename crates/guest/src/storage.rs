use selium_abi::{
    BlobStoreDescriptor, DurableLogDescriptor, HostcallOutput, HostcallRequest, StorageRecord,
};

use crate::{GuestError, Result, hostcall::hostcall_ready};

#[derive(Clone, Debug)]
pub struct DurableLog {
    descriptor: DurableLogDescriptor,
}

#[derive(Clone, Debug)]
pub struct BlobStore {
    descriptor: BlobStoreDescriptor,
}

impl DurableLog {
    pub fn open(name: impl Into<String>) -> Result<Self> {
        match hostcall_ready(HostcallRequest::StorageOpenLog { name: name.into() })? {
            HostcallOutput::DurableLog(descriptor) => Ok(Self { descriptor }),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }

    pub fn descriptor(&self) -> &DurableLogDescriptor {
        &self.descriptor
    }

    pub fn append(
        &self,
        timestamp_ms: u64,
        headers: Vec<(String, String)>,
        payload: Vec<u8>,
    ) -> Result<u64> {
        match hostcall_ready(HostcallRequest::StorageLogAppend {
            local_id: self.descriptor.local_id,
            timestamp_ms,
            headers,
            payload,
        })? {
            HostcallOutput::Sequence(Some(sequence)) => Ok(sequence),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }

    pub fn replay(&self, from_sequence: Option<u64>, limit: u32) -> Result<Vec<StorageRecord>> {
        match hostcall_ready(HostcallRequest::StorageLogReplay {
            local_id: self.descriptor.local_id,
            from_sequence,
            limit,
        })? {
            HostcallOutput::StorageRecords(records) => Ok(records),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }

    pub fn checkpoint(&self, name: impl Into<String>, sequence: u64) -> Result<()> {
        match hostcall_ready(HostcallRequest::StorageLogCheckpoint {
            local_id: self.descriptor.local_id,
            name: name.into(),
            sequence,
        })? {
            HostcallOutput::Empty => Ok(()),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }

    pub fn checkpoint_sequence(&self, name: impl Into<String>) -> Result<Option<u64>> {
        match hostcall_ready(HostcallRequest::StorageLogCheckpointRead {
            local_id: self.descriptor.local_id,
            name: name.into(),
        })? {
            HostcallOutput::Sequence(sequence) => Ok(sequence),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }

    pub fn close(self) -> Result<()> {
        match hostcall_ready(HostcallRequest::StorageLogClose {
            local_id: self.descriptor.local_id,
        })? {
            HostcallOutput::Empty => Ok(()),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }
}

impl BlobStore {
    pub fn open(name: impl Into<String>) -> Result<Self> {
        match hostcall_ready(HostcallRequest::StorageOpenBlobStore { name: name.into() })? {
            HostcallOutput::BlobStore(descriptor) => Ok(Self { descriptor }),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }

    pub fn descriptor(&self) -> &BlobStoreDescriptor {
        &self.descriptor
    }

    pub fn put(&self, bytes: Vec<u8>) -> Result<String> {
        match hostcall_ready(HostcallRequest::StorageBlobPut {
            local_id: self.descriptor.local_id,
            bytes,
        })? {
            HostcallOutput::BlobId(blob_id) => Ok(blob_id),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }

    pub fn get(&self, blob_id: impl Into<String>) -> Result<Option<Vec<u8>>> {
        match hostcall_ready(HostcallRequest::StorageBlobGet {
            local_id: self.descriptor.local_id,
            blob_id: blob_id.into(),
        })? {
            HostcallOutput::Bytes(bytes) => Ok(Some(bytes)),
            HostcallOutput::Empty => Ok(None),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }

    pub fn set_manifest(&self, name: impl Into<String>, blob_id: impl Into<String>) -> Result<()> {
        match hostcall_ready(HostcallRequest::StorageBlobSetManifest {
            local_id: self.descriptor.local_id,
            name: name.into(),
            blob_id: blob_id.into(),
        })? {
            HostcallOutput::Empty => Ok(()),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }

    pub fn manifest(&self, name: impl Into<String>) -> Result<Option<String>> {
        match hostcall_ready(HostcallRequest::StorageBlobGetManifest {
            local_id: self.descriptor.local_id,
            name: name.into(),
        })? {
            HostcallOutput::BlobId(blob_id) => Ok(Some(blob_id)),
            HostcallOutput::Empty => Ok(None),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }

    pub fn close(self) -> Result<()> {
        match hostcall_ready(HostcallRequest::StorageBlobStoreClose {
            local_id: self.descriptor.local_id,
        })? {
            HostcallOutput::Empty => Ok(()),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }
}
