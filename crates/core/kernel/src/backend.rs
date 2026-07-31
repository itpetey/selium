//! `KernelBackend`: a [`MappingBackend`] backed by the kernel's
//! Store-mediated shared memory.
//!
//! This lets the kernel participate in shared-memory ring I/O using the same
//! [`crate`] ring protocol primitives as guests, without depending on the
//! runtime (which would create a cycle: runtime → kernel → runtime). The
//! runtime re-uses this implementation via its dependency on `selium-kernel`.

use std::{
    fmt::Debug,
    sync::{Arc, atomic::Ordering},
};

use selium_memory::{MappingBackend, MemoryError, Result};

use crate::Kernel;

/// Mapping backend that delegates reads, writes, and atomics to the kernel's
/// shared-memory store. All operations go through the kernel's mutex-protected
/// accessor methods, providing a consistent (if coarser-grained) atomicity
/// domain for host-side ring operations.
#[derive(Clone)]
pub struct KernelBackend {
    kernel: Kernel,
    /// Kernel local mapping id used for read/write/atomic calls.
    pub(crate) local_id: u64,
    /// Selium shared region id.
    pub(crate) shared_id: u64,
    /// Byte offset within the shared region that this mapping starts at.
    base_offset: u64,
    /// Size of this mapping in bytes.
    size: u64,
}

impl KernelBackend {
    /// Creates a new backend wrapping the given kernel mapping.
    pub fn new(kernel: Kernel, local_id: u64, shared_id: u64, size: u64) -> Self {
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
                .map_err(|_error| MemoryError::InvalidLayout)?,
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

    fn atomic_notify(&self, offset: u64, count: u32) -> Result<u32> {
        let effective_offset = self.offset(offset)?;
        let key = shared_offset_key(self.shared_id, effective_offset);
        Ok(selium_memory::host_notify(key, count))
    }

    fn atomic_wait32(&self, offset: u64, expected: u32, timeout_ms: u64) -> Result<()> {
        let effective_offset = self.offset(offset)?;
        let bytes = self
            .kernel
            .read_shared_memory(self.local_id, effective_offset, 4)
            .map_err(|error| MemoryError::Other(error.to_string()))?;
        let actual = u32::from_le_bytes(
            bytes
                .try_into()
                .map_err(|_error| MemoryError::InvalidLayout)?,
        );
        if actual != expected {
            return Ok(());
        }

        let key = shared_offset_key(self.shared_id, effective_offset);
        selium_memory::host_wait(key, timeout_ms)
    }

    fn sub_region(&self, offset: u64, size: u64) -> Result<Arc<dyn MappingBackend>> {
        if offset
            .checked_add(size)
            .ok_or(MemoryError::CapacityExceeded)?
            > self.size
        {
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

/// Helper: allocates a shared region and creates a `KernelBackend` for it.
impl Kernel {
    /// Allocates a shared region and returns a `KernelBackend` covering the
    /// full region, plus the shared region id.
    pub fn allocate_backend(&self, size: u32) -> crate::Result<(KernelBackend, u64)> {
        let (shared_id, len) = self.allocate_shared_region(size)?;
        let local_id = self.attach_shared_region(shared_id)?;
        Ok((
            KernelBackend::new(self.clone(), local_id, shared_id, len as u64),
            shared_id,
        ))
    }

    /// Attaches to an existing shared region and returns a `KernelBackend`
    /// covering the full region.
    pub fn attach_backend(&self, shared_id: u64) -> crate::Result<KernelBackend> {
        let local_id = self.attach_shared_region(shared_id)?;
        let len = self.shared_region_len(shared_id)?;
        Ok(KernelBackend::new(
            self.clone(),
            local_id,
            shared_id,
            len as u64,
        ))
    }
}

/// Creates a unique waiters key from a shared region id and offset.
fn shared_offset_key(shared_id: u64, offset: u64) -> usize {
    ((shared_id as usize).wrapping_mul(31)) ^ (offset as usize)
}
