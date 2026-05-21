use selium_abi::{
    HostcallOutput, HostcallRequest, SharedMappingDescriptor, SharedRegionDescriptor,
};

use crate::{GuestError, Result, hostcall::hostcall_ready};

/// Owned shared memory region allocated through the host.
#[derive(Clone, Copy, Debug)]
pub struct SharedRegion {
    descriptor: SharedRegionDescriptor,
}

/// Local mapping of a shared memory region.
#[derive(Clone, Copy, Debug)]
pub struct SharedMemory {
    descriptor: SharedMappingDescriptor,
}

impl SharedRegion {
    /// Allocates a shared memory region with the requested size and alignment.
    pub fn allocate(size: u32, alignment: u32) -> Result<Self> {
        match hostcall_ready(HostcallRequest::SharedMemoryAllocate { size, alignment })? {
            HostcallOutput::SharedRegion(descriptor) => Ok(Self { descriptor }),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }

    /// Returns the region descriptor.
    pub fn descriptor(&self) -> SharedRegionDescriptor {
        self.descriptor
    }

    /// Returns the shared region id.
    pub fn shared_id(&self) -> u64 {
        self.descriptor.shared_id
    }

    /// Returns the region length in bytes.
    pub fn len(&self) -> u32 {
        self.descriptor.len
    }

    /// Returns whether the region has zero length.
    pub fn is_empty(&self) -> bool {
        self.descriptor.len == 0
    }

    /// Destroys the shared region.
    pub fn destroy(self) -> Result<()> {
        match hostcall_ready(HostcallRequest::SharedMemoryDestroy {
            shared_id: self.descriptor.shared_id,
        })? {
            HostcallOutput::Empty => Ok(()),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }
}

impl SharedMemory {
    /// Attaches a local mapping to a shared region descriptor.
    pub fn attach(region: SharedRegionDescriptor, offset: u32, len: u32) -> Result<Self> {
        Self::attach_shared(region.shared_id, offset, len)
    }

    /// Attaches a local mapping to a shared region id.
    pub fn attach_shared(shared_id: u64, offset: u32, len: u32) -> Result<Self> {
        match hostcall_ready(HostcallRequest::SharedMemoryAttach {
            shared_id,
            offset,
            len,
        })? {
            HostcallOutput::SharedMapping(descriptor) => Ok(Self { descriptor }),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }

    /// Returns the mapping descriptor.
    pub fn descriptor(&self) -> SharedMappingDescriptor {
        self.descriptor
    }

    /// Returns the local mapping id.
    pub fn local_id(&self) -> u64 {
        self.descriptor.local_id
    }

    /// Returns the shared region id backing this mapping.
    pub fn shared_id(&self) -> u64 {
        self.descriptor.shared_id
    }

    /// Reads bytes from the mapping at the supplied offset.
    pub fn read(&self, offset: u32, len: u32) -> Result<Vec<u8>> {
        match hostcall_ready(HostcallRequest::SharedMemoryRead {
            local_id: self.descriptor.local_id,
            offset,
            len,
        })? {
            HostcallOutput::Bytes(bytes) => Ok(bytes),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }

    /// Writes bytes into the mapping at the supplied offset.
    pub fn write(&self, offset: u32, bytes: Vec<u8>) -> Result<()> {
        match hostcall_ready(HostcallRequest::SharedMemoryWrite {
            local_id: self.descriptor.local_id,
            offset,
            bytes,
        })? {
            HostcallOutput::Empty => Ok(()),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }

    /// Atomically adds to a little-endian `u64` at the supplied mapping offset.
    pub fn fetch_add_u64(&self, offset: u32, value: u64) -> Result<u64> {
        match hostcall_ready(HostcallRequest::SharedMemoryFetchAddU64 {
            local_id: self.descriptor.local_id,
            offset,
            value,
        })? {
            HostcallOutput::U64(previous) => Ok(previous),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }

    /// Atomically compares and exchanges a little-endian `u64` at the supplied mapping offset.
    pub fn compare_exchange_u64(&self, offset: u32, current: u64, new: u64) -> Result<u64> {
        match hostcall_ready(HostcallRequest::SharedMemoryCompareExchangeU64 {
            local_id: self.descriptor.local_id,
            offset,
            current,
            new,
        })? {
            HostcallOutput::U64(previous) => Ok(previous),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }

    /// Detaches the local mapping.
    pub fn detach(self) -> Result<()> {
        match hostcall_ready(HostcallRequest::SharedMemoryDetach {
            local_id: self.descriptor.local_id,
        })? {
            HostcallOutput::Empty => Ok(()),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }
}
