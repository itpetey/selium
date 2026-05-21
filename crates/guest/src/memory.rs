use selium_abi::{
    HostcallOutput, HostcallRequest, SharedMappingDescriptor, SharedRegionDescriptor,
};

use crate::{GuestError, Result, hostcall::hostcall_ready};

#[derive(Clone, Copy, Debug)]
pub struct SharedRegion {
    descriptor: SharedRegionDescriptor,
}

#[derive(Clone, Copy, Debug)]
pub struct SharedMemory {
    descriptor: SharedMappingDescriptor,
}

impl SharedRegion {
    pub fn allocate(size: u32, alignment: u32) -> Result<Self> {
        match hostcall_ready(HostcallRequest::SharedMemoryAllocate { size, alignment })? {
            HostcallOutput::SharedRegion(descriptor) => Ok(Self { descriptor }),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }

    pub fn descriptor(&self) -> SharedRegionDescriptor {
        self.descriptor
    }

    pub fn shared_id(&self) -> u64 {
        self.descriptor.shared_id
    }

    pub fn len(&self) -> u32 {
        self.descriptor.len
    }

    pub fn is_empty(&self) -> bool {
        self.descriptor.len == 0
    }

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
    pub fn attach(region: SharedRegionDescriptor, offset: u32, len: u32) -> Result<Self> {
        Self::attach_shared(region.shared_id, offset, len)
    }

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

    pub fn descriptor(&self) -> SharedMappingDescriptor {
        self.descriptor
    }

    pub fn local_id(&self) -> u64 {
        self.descriptor.local_id
    }

    pub fn shared_id(&self) -> u64 {
        self.descriptor.shared_id
    }

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

    pub fn detach(self) -> Result<()> {
        match hostcall_ready(HostcallRequest::SharedMemoryDetach {
            local_id: self.descriptor.local_id,
        })? {
            HostcallOutput::Empty => Ok(()),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }
}
