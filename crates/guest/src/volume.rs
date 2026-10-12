use selium_abi::{HostcallOutput, HostcallRequest, LocalResourceId};

use crate::{GuestError, Result, hostcall::hostcall_ready};

/// Guest handle for an open durable volume.
///
/// A volume is an opaque byte container owned by the host: the guest chooses
/// the bytes and their meaning. The guest SDK exposes it for system guests
/// (notably `selium-store`) that own durable semantics themselves.
#[derive(Clone, Debug)]
pub struct Volume {
    local_id: LocalResourceId,
    len: u64,
}

impl Volume {
    /// Opens or creates a volume, optionally under a serving tenant.
    ///
    /// `serving_tenant` is the tenant the volume is minted under; `None` mints
    /// it under the calling process's own tenant.
    pub fn open(name: impl Into<String>, serving_tenant: Option<&str>) -> Result<Self> {
        match hostcall_ready(HostcallRequest::VolumeOpen {
            name: name.into(),
            serving_tenant: serving_tenant.map(str::to_string),
        })? {
            HostcallOutput::Volume(descriptor) => Ok(Self {
                local_id: descriptor.local_id,
                len: descriptor.len,
            }),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }

    /// Returns the local volume handle id.
    pub fn local_id(&self) -> LocalResourceId {
        self.local_id
    }

    /// Returns the volume's length at open time.
    pub fn opened_len(&self) -> u64 {
        self.len
    }

    /// Appends bytes and returns the offset they landed at.
    pub fn append(&self, bytes: &[u8]) -> Result<u64> {
        match hostcall_ready(HostcallRequest::VolumeAppend {
            local_id: self.local_id,
            bytes: bytes.to_vec(),
        })? {
            HostcallOutput::U64(offset) => Ok(offset),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }

    /// Reads up to `len` bytes at `offset`.
    pub fn read_at(&self, offset: u64, len: u32) -> Result<Vec<u8>> {
        match hostcall_ready(HostcallRequest::VolumeReadAt {
            local_id: self.local_id,
            offset,
            len,
        })? {
            HostcallOutput::Bytes(bytes) => Ok(bytes),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }

    /// Reads the whole volume.
    pub fn read_all(&self) -> Result<Vec<u8>> {
        let len = self.len()?;
        let mut bytes = Vec::new();
        let mut offset = 0u64;
        while offset < len {
            let chunk = self.read_at(offset, CHUNK_BYTES)?;
            if chunk.is_empty() {
                break;
            }
            offset += chunk.len() as u64;
            bytes.extend_from_slice(&chunk);
        }
        Ok(bytes)
    }

    /// Returns the volume's current length.
    pub fn len(&self) -> Result<u64> {
        match hostcall_ready(HostcallRequest::VolumeLen {
            local_id: self.local_id,
        })? {
            HostcallOutput::U64(len) => Ok(len),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }

    /// Returns whether the volume is empty.
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    /// Flushes the volume to stable storage (the fsync barrier).
    pub fn sync(&self) -> Result<()> {
        match hostcall_ready(HostcallRequest::VolumeSync {
            local_id: self.local_id,
        })? {
            HostcallOutput::Empty => Ok(()),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }

    /// Closes the local volume handle.
    pub fn close(self) -> Result<()> {
        match hostcall_ready(HostcallRequest::VolumeClose {
            local_id: self.local_id,
        })? {
            HostcallOutput::Empty => Ok(()),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }
}

/// Read chunk size used by [`Volume::read_all`].
const CHUNK_BYTES: u32 = 64 * 1024;

/// Lists the volume names recorded under a tenant.
///
/// `serving_tenant` scopes the listing; `None` lists the calling process's own
/// tenant's volumes.
pub fn list(serving_tenant: Option<&str>) -> Result<Vec<String>> {
    match hostcall_ready(HostcallRequest::VolumeList {
        serving_tenant: serving_tenant.map(str::to_string),
    })? {
        HostcallOutput::VolumeNames(names) => Ok(names),
        _ => Err(GuestError::UnexpectedHostcallOutput),
    }
}

/// Removes a volume under a tenant, returning the bytes it occupied.
pub fn remove(name: impl Into<String>, serving_tenant: Option<&str>) -> Result<u64> {
    match hostcall_ready(HostcallRequest::VolumeRemove {
        name: name.into(),
        serving_tenant: serving_tenant.map(str::to_string),
    })? {
        HostcallOutput::U64(freed) => Ok(freed),
        _ => Err(GuestError::UnexpectedHostcallOutput),
    }
}

/// Returns a tenant's resident durable-storage bytes.
///
/// Gated by the `QuotaWrite` authority: only the accounting guest may read it.
pub fn tenant_usage(tenant: &str) -> Result<u64> {
    match hostcall_ready(HostcallRequest::VolumeUsage {
        tenant: tenant.to_string(),
    })? {
        HostcallOutput::U64(usage) => Ok(usage),
        _ => Err(GuestError::UnexpectedHostcallOutput),
    }
}
