use selium_abi::{
    HostcallOutput, HostcallRequest, SharedMappingDescriptor, SharedRegionDescriptor,
};

use crate::{GuestError, Result, hostcall::hostcall_ready};

const SHARED_REGION_HEADER_CAPACITY_OFFSET: u64 = 8;
const SHARED_REGION_HEADER_COUNT_OFFSET: u64 = 16;
const SHARED_REGION_HEADER_ENTRY_OFFSET: u64 = 24;
const SHARED_REGION_HEADER_ENTRY_SIZE: u64 = 8;
/// Magic value for multi-memory shared region layout headers.
pub const SHARED_REGION_MAGIC: u64 = 0x53454C49554D454D;

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

/// Builder for constructing a multi-memory shared region.
///
/// Sub-memories are stored contiguously with 8-byte alignment padding.
/// The region header records `memory_count` and `(offset, len)` pairs.
/// After `seal()`, no further modifications are permitted.
pub struct SharedRegionBuilder {
    capacity: u64,
    memories: Vec<u64>,
    sealed: bool,
}

impl SharedRegion {
    /// Allocates a shared memory region with the requested size and alignment.
    pub fn allocate(size: u64, alignment: u64) -> Result<Self> {
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
    pub fn len(&self) -> u64 {
        self.descriptor.len
    }

    /// Returns whether the region has zero length.
    pub fn is_empty(&self) -> bool {
        self.descriptor.len == 0
    }

    /// Attaches to an existing shared region by its shared id and length.
    pub fn attach(shared_id: u64, len: u64) -> Self {
        Self {
            descriptor: SharedRegionDescriptor { shared_id, len },
        }
    }

    /// Reads the number of sub-memories from the region layout header.
    pub fn memory_count(&self) -> Result<usize> {
        let mapping = SharedMemory::attach_shared(self.shared_id(), 0, self.len())?;
        let count = mapping.memory_count()?;
        mapping.detach()?;
        Ok(count)
    }

    /// Reads the offset and length of the sub-memory at the given index.
    pub fn memory(&self, index: usize) -> Result<(u64, u64)> {
        let mapping = SharedMemory::attach_shared(self.shared_id(), 0, self.len())?;
        let result = mapping.memory(index)?;
        mapping.detach()?;
        Ok(result)
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
    pub fn attach(region: SharedRegionDescriptor, offset: u64, len: u64) -> Result<Self> {
        Self::attach_shared(region.shared_id, offset, len)
    }

    /// Attaches a local mapping to a shared region id.
    pub fn attach_shared(shared_id: u64, offset: u64, len: u64) -> Result<Self> {
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
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
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
    pub fn write(&self, offset: u64, bytes: Vec<u8>) -> Result<()> {
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
    pub fn fetch_add_u64(&self, offset: u64, value: u64) -> Result<u64> {
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
    pub fn compare_exchange_u64(&self, offset: u64, current: u64, new: u64) -> Result<u64> {
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

    /// Reads the number of sub-memories from the shared region layout header.
    pub fn memory_count(&self) -> Result<usize> {
        let bytes = self.read(SHARED_REGION_HEADER_COUNT_OFFSET, 4)?;
        let count = usize::from_le_bytes(
            bytes
                .try_into()
                .map_err(|_error| GuestError::Host("invalid memory count".to_string()))?,
        );
        Ok(count)
    }

    /// Reads the offset and length of the sub-memory at the given index.
    pub fn memory(&self, index: usize) -> Result<(u64, u64)> {
        let count = self.memory_count()?;
        if index >= count {
            return Err(GuestError::Host("memory index out of bounds".to_string()));
        }
        let entry_offset =
            SHARED_REGION_HEADER_ENTRY_OFFSET + index as u64 * SHARED_REGION_HEADER_ENTRY_SIZE;
        let offset_bytes = self.read(entry_offset, 4)?;
        let len_bytes = self.read(entry_offset + 4, 4)?;
        let offset = u64::from_le_bytes(
            offset_bytes
                .try_into()
                .map_err(|_error| GuestError::Host("invalid memory offset".to_string()))?,
        );
        let len = u64::from_le_bytes(
            len_bytes
                .try_into()
                .map_err(|_error| GuestError::Host("invalid memory length".to_string()))?,
        );
        Ok((offset, len))
    }
}

impl SharedRegionBuilder {
    /// Creates a new builder with the given total region capacity.
    pub fn new(capacity: u64) -> Self {
        Self {
            capacity,
            memories: Vec::new(),
            sealed: false,
        }
    }

    /// Adds a sub-memory of the given length to the layout.
    pub fn add_memory(&mut self, len: u64) -> Result<&mut Self> {
        if self.sealed {
            return Err(GuestError::BuilderSealed);
        }
        self.memories.push(len);
        Ok(self)
    }

    /// Finalises the layout, allocates the shared region, writes the header,
    /// and returns the region descriptor.
    pub fn seal(&mut self) -> Result<SharedRegion> {
        if self.sealed {
            return Err(GuestError::BuilderSealed);
        }

        let header_size = Self::header_size(self.memories.len());
        let mut total = header_size;
        for &len in &self.memories {
            total = Self::align_up(total, 8) + len;
        }

        if total > self.capacity {
            return Err(GuestError::CapacityExceeded);
        }

        let region = SharedRegion::allocate(self.capacity, 8)?;
        let mapping = SharedMemory::attach(region.descriptor(), 0, self.capacity)?;

        mapping.write(0, SHARED_REGION_MAGIC.to_le_bytes().to_vec())?;
        mapping.write(
            SHARED_REGION_HEADER_CAPACITY_OFFSET,
            (self.capacity).to_le_bytes().to_vec(),
        )?;
        mapping.write(
            SHARED_REGION_HEADER_COUNT_OFFSET,
            (self.memories.len() as u32).to_le_bytes().to_vec(),
        )?;
        mapping.write(
            SHARED_REGION_HEADER_COUNT_OFFSET + 4,
            0u32.to_le_bytes().to_vec(),
        )?;

        let mut offset = header_size;
        for (i, &len) in self.memories.iter().enumerate() {
            offset = Self::align_up(offset, 8);
            let entry_offset =
                SHARED_REGION_HEADER_ENTRY_OFFSET + i as u64 * SHARED_REGION_HEADER_ENTRY_SIZE;
            mapping.write(entry_offset, offset.to_le_bytes().to_vec())?;
            mapping.write(entry_offset + 4, len.to_le_bytes().to_vec())?;
            offset += len;
        }

        self.sealed = true;
        Ok(region)
    }

    fn header_size(memory_count: usize) -> u64 {
        SHARED_REGION_HEADER_ENTRY_OFFSET + memory_count as u64 * SHARED_REGION_HEADER_ENTRY_SIZE
    }

    fn align_up(value: u64, alignment: u64) -> u64 {
        let rem = value % alignment;
        if rem == 0 {
            value
        } else {
            value + alignment - rem
        }
    }
}

#[cfg(test)]
mod builder_tests {
    use super::*;

    #[test]
    fn add_memory_after_seal_returns_error() {
        let mut builder = SharedRegionBuilder::new(1024);
        builder.sealed = true;
        assert!(matches!(
            builder.add_memory(64),
            Err(GuestError::BuilderSealed)
        ));
    }

    #[test]
    fn seal_after_seal_returns_error() {
        let mut builder = SharedRegionBuilder::new(1024);
        builder.sealed = true;
        assert!(matches!(builder.seal(), Err(GuestError::BuilderSealed)));
    }

    #[test]
    fn total_exceeding_capacity_returns_error() {
        let mut builder = SharedRegionBuilder::new(32);
        builder.add_memory(16).unwrap();
        builder.add_memory(32).unwrap();
        assert!(matches!(builder.seal(), Err(GuestError::CapacityExceeded)));
    }
}
