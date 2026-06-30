//! Runtime-backed [`RegionProvider`] and [`MappingBackend`].
//!
//! The runtime manages shared memory regions through `wasmtiny` and the
//! `selium-kernel` crate. This module lets the runtime itself participate in
//! shared-memory I/O (e.g. discovery pub/sub) using the same `selium-shm` and
//! `selium-memory` abstractions as guests.

use std::{
    collections::HashMap,
    fmt::Debug,
    sync::{Arc, Mutex, atomic::Ordering},
};

use selium_abi::{RegionAllocation, RegionProt, ResourceKind};
use selium_kernel::Kernel;
use selium_memory::{MemoryError, MappingBackend, Region, RegionProvider, Result};

/// Region provider backed by the runtime kernel's shared region table.
#[derive(Clone)]
pub struct RuntimeRegionProvider {
    kernel: Kernel,
    /// Local mapping ids created by this provider, keyed by shared region id.
    /// Detached in `free` so the kernel can destroy the region.
    local_ids: Arc<Mutex<HashMap<u64, u64>>>,
}

impl Debug for RuntimeRegionProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimeRegionProvider").finish()
    }
}

impl RuntimeRegionProvider {
    /// Creates a new provider backed by the given kernel.
    pub fn new(kernel: Kernel) -> Self {
        Self {
            kernel,
            local_ids: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl RegionProvider for RuntimeRegionProvider {
    fn allocate(&self, pages: u32, _prot: RegionProt, _purpose: ResourceKind) -> Result<Region> {
        let size_bytes = pages as u64 * selium_memory::PAGE_SIZE;
        let size_u32 = u32::try_from(size_bytes)
            .map_err(|_| MemoryError::Other("region size exceeds u32".to_string()))?;
        let (shared_id, len) = self
            .kernel
            .allocate_shared_region(size_u32)
            .map_err(|error| MemoryError::Other(error.to_string()))?;
        let local_id = self
            .kernel
            .attach_shared_region(shared_id)
            .map_err(|error| MemoryError::Other(error.to_string()))?;
        self.local_ids.lock().unwrap().insert(shared_id, local_id);
        let backend = Arc::new(KernelBackend::new(self.kernel.clone(), local_id, shared_id, len as u64));
        Ok(Region::with_backend(
            RegionAllocation {
                region_id: shared_id,
                page_offset: 0,
            },
            backend,
        ))
    }

    fn attach(
        &self,
        region_id: u64,
        _reader_slot: Option<u32>,
        _prot: RegionProt,
    ) -> Result<Region> {
        let local_id = self
            .kernel
            .attach_shared_region(region_id)
            .map_err(|error| MemoryError::Other(error.to_string()))?;
        let len = self
            .kernel
            .shared_region_len(region_id)
            .map_err(|error| MemoryError::Other(error.to_string()))?;
        let backend = Arc::new(KernelBackend::new(self.kernel.clone(), local_id, region_id, len as u64));
        Ok(Region::with_backend(
            RegionAllocation {
                region_id,
                page_offset: 0,
            },
            backend,
        ))
    }

    fn free(&self, region_id: u64) -> Result<()> {
        if let Some(local_id) = self.local_ids.lock().unwrap().remove(&region_id) {
            let _ = self.kernel.detach_shared_region(local_id);
        }
        self.kernel
            .destroy_shared_region(region_id)
            .map_err(|error| MemoryError::Other(error.to_string()))
    }
}

/// Mapping backend that delegates reads, writes, and atomics to the kernel.
#[derive(Clone)]
struct KernelBackend {
    kernel: Kernel,
    /// Kernel local mapping id used for read/write/atomic calls.
    local_id: u64,
    /// Selium shared region id.
    shared_id: u64,
    /// Byte offset within the shared region that this mapping starts at.
    base_offset: u64,
    /// Size of this mapping in bytes.
    size: u64,
}

impl Debug for KernelBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KernelBackend")
            .field("local_id", &self.local_id)
            .field("shared_id", &self.shared_id)
            .field("base_offset", &self.base_offset)
            .field("size", &self.size)
            .finish()
    }
}

impl KernelBackend {
    fn new(kernel: Kernel, local_id: u64, shared_id: u64, size: u64) -> Self {
        Self {
            kernel,
            local_id,
            shared_id,
            base_offset: 0,
            size,
        }
    }

    fn offset(&self, offset: u64) -> Result<u64> {
        offset
            .checked_add(self.base_offset)
            .ok_or(MemoryError::CapacityExceeded)
    }
}

impl MappingBackend for KernelBackend {
    fn size(&self) -> u64 {
        self.size
    }

    fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        let offset = self.offset(offset)?;
        self.kernel
            .read_shared_memory(self.local_id, offset, len as usize)
            .map_err(|error| MemoryError::Other(error.to_string()))
    }

    fn write(&self, offset: u64, bytes: &[u8]) -> Result<()> {
        let offset = self.offset(offset)?;
        self.kernel
            .write_shared_memory(self.local_id, offset, bytes)
            .map_err(|error| MemoryError::Other(error.to_string()))
    }

    fn atomic_load_u64(&self, offset: u64, _ordering: Ordering) -> Result<u64> {
        let bytes = self.read(offset, 8)?;
        Ok(u64::from_le_bytes(
            bytes
                .try_into()
                .map_err(|_| MemoryError::InvalidLayout)?,
        ))
    }

    fn atomic_store_u64(&self, offset: u64, value: u64, _ordering: Ordering) -> Result<()> {
        self.write(offset, &value.to_le_bytes())
    }

    fn fetch_add_u64(&self, offset: u64, value: u64, _ordering: Ordering) -> Result<u64> {
        let offset = self.offset(offset)?;
        self.kernel
            .fetch_add_shared_memory_u64(self.local_id, offset, value)
            .map_err(|error| MemoryError::Other(error.to_string()))
    }

    fn compare_exchange_u64(&self, offset: u64, current: u64, new: u64) -> Result<u64> {
        let offset = self.offset(offset)?;
        self.kernel
            .compare_exchange_shared_memory_u64(self.local_id, offset, current, new)
            .map_err(|error| MemoryError::Other(error.to_string()))
    }

    fn atomic_notify(&self, _offset: u64, _count: u32) -> Result<u32> {
        // The kernel does not expose host-side notify/wait; guests use native
        // atomic instructions on the mapped memory.
        Ok(0)
    }

    fn atomic_wait32(&self, _offset: u64, _expected: u32, _timeout_ms: u64) -> Result<()> {
        Ok(())
    }

    fn sub_region(&self, offset: u64, size: u64) -> Result<Arc<dyn MappingBackend>> {
        if offset.checked_add(size).ok_or(MemoryError::CapacityExceeded)? > self.size {
            return Err(MemoryError::IndexOutOfBounds);
        }
        let base_offset = self.offset(offset)?;
        Ok(Arc::new(KernelBackend {
            kernel: self.kernel.clone(),
            local_id: self.local_id,
            shared_id: self.shared_id,
            base_offset,
            size,
        }))
    }

    fn as_debug(&self) -> &dyn Debug {
        self
    }
}

