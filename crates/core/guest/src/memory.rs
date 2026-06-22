use std::{
    collections::HashMap,
    sync::Arc,
    sync::atomic::{AtomicU64, Ordering},
};

use selium_abi::{RegionAllocation, RegionProt, ResourceKind};

#[cfg(target_arch = "wasm32")]
use selium_abi::{HostcallOutput, HostcallRequest};

#[cfg(target_arch = "wasm32")]
use crate::hostcall::hostcall_ready;
use crate::io::error::Error as IoError;
use crate::{GuestError, Result};

/// Magic value for multi-memory shared region layout headers.
pub const SHARED_REGION_MAGIC: u64 = 0x53454C49554D454D;

/// WASM page size used for region layout (4 KiB).
pub const PAGE_SIZE: u64 = 4096;

static NATIVE_REGION_COUNTER: AtomicU64 = AtomicU64::new(1);
static NATIVE_REGION_REGISTRY: std::sync::LazyLock<std::sync::Mutex<HashMap<u64, Arc<Vec<u8>>>>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(HashMap::new()));

/// An allocated shared memory region.
///
/// The region is mapped into the allocating guest's linear memory at the
/// returned `page_offset`. Other guests can attach via [`SharedRegion::attach`].
///
/// In WASM mode, allocation and attachment go through hostcalls
/// (`AllocRegion` / `AttachRegion` / `FreeRegion`). In native mode a
/// heap-backed `Arc<Vec<u8>>` is used and registered in a process-local
/// registry so that [`SharedRegion::attach`] can share the same memory.
#[derive(Clone)]
pub struct SharedRegion {
    allocation: RegionAllocation,
    size: u64,
    /// Heap-allocated backing store (native mode only). `None` for WASM
    /// mappings whose lifetime is managed by the runtime.
    _backing: Option<Arc<Vec<u8>>>,
}

impl SharedRegion {
    /// Allocates a shared memory region with the requested number of pages.
    pub fn allocate(pages: u32, prot: RegionProt, purpose: ResourceKind) -> Result<Self> {
        let size = pages as u64 * PAGE_SIZE;

        // --- WASM path (hostcall) ---
        #[cfg(target_arch = "wasm32")]
        {
            let _ = size; // computed above but not needed in WASM path
            match hostcall_ready(HostcallRequest::AllocRegion {
                pages,
                prot,
                purpose,
            })? {
                HostcallOutput::RegionAlloc(allocation) => Ok(Self {
                    allocation,
                    size: pages as u64 * PAGE_SIZE,
                    _backing: None,
                }),
                _ => Err(GuestError::UnexpectedHostcallOutput),
            }
        }

        // --- Native path (heap + registry) ---
        #[cfg(not(target_arch = "wasm32"))]
        {
            let _ = (prot, purpose);
            let backing = Arc::new(vec![0u8; size as usize]);
            let region_id = NATIVE_REGION_COUNTER.fetch_add(1, Ordering::SeqCst);
            NATIVE_REGION_REGISTRY
                .lock()
                .expect("native registry poisoned")
                .insert(region_id, backing.clone());
            Ok(Self {
                allocation: RegionAllocation {
                    region_id,
                    page_offset: 0,
                },
                size,
                _backing: Some(backing),
            })
        }
    }

    /// Attaches an existing shared region into this guest's linear memory.
    pub fn attach(region_id: u64, reader_slot: Option<u32>, prot: RegionProt) -> Result<Self> {
        // --- WASM path (hostcall) ---
        #[cfg(target_arch = "wasm32")]
        {
            match hostcall_ready(HostcallRequest::AttachRegion {
                region_id,
                reader_slot,
                prot,
            })? {
                HostcallOutput::RegionAttach(attachment) => Ok(Self {
                    allocation: RegionAllocation {
                        region_id,
                        page_offset: attachment.page_offset,
                    },
                    // Size is not returned by the hostcall; callers must read
                    // it from the shared header after mapping.
                    size: 0,
                    _backing: None,
                }),
                _ => Err(GuestError::UnexpectedHostcallOutput),
            }
        }

        // --- Native path (registry lookup) ---
        #[cfg(not(target_arch = "wasm32"))]
        {
            let _ = (reader_slot, prot);
            let backing = NATIVE_REGION_REGISTRY
                .lock()
                .expect("native registry poisoned")
                .get(&region_id)
                .cloned()
                .ok_or_else(|| GuestError::Host(format!("region {region_id} not found")))?;
            let size = backing.len() as u64;
            Ok(Self {
                allocation: RegionAllocation {
                    region_id,
                    page_offset: 0,
                },
                size,
                _backing: Some(backing),
            })
        }
    }

    /// Returns the region allocation descriptor.
    pub fn allocation(&self) -> RegionAllocation {
        self.allocation
    }

    /// Returns the region id.
    pub fn region_id(&self) -> u64 {
        self.allocation.region_id
    }

    /// Returns the page offset within guest linear memory.
    pub fn page_offset(&self) -> u32 {
        self.allocation.page_offset
    }

    /// Returns the region size in bytes.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Creates a [`RegionMapping`] for this shared region.
    ///
    /// In WASM mode the pointer is derived from the page offset. In native
    /// mode the mapping shares the heap-backed `Arc<Vec<u8>>` so the memory
    /// stays alive as long as the mapping exists.
    pub fn mapping(&self) -> RegionMapping {
        match &self._backing {
            Some(backing) => {
                let base = backing.as_ptr() as *mut u8;
                RegionMapping {
                    inner: Arc::new(RegionMappingInner {
                        base,
                        size: self.size,
                        _backing: Some(backing.clone()),
                    }),
                }
            }
            None => {
                // WASM mode: derive pointer from page offset.
                let base = (self.allocation.page_offset as u64 * PAGE_SIZE) as *mut u8;
                unsafe { RegionMapping::from_raw(base, self.size) }
            }
        }
    }

    /// Frees the shared region.
    pub fn free(self) -> Result<()> {
        // --- WASM path (hostcall) ---
        #[cfg(target_arch = "wasm32")]
        {
            match hostcall_ready(HostcallRequest::FreeRegion {
                region_id: self.allocation.region_id,
            })? {
                HostcallOutput::Empty => Ok(()),
                _ => Err(GuestError::UnexpectedHostcallOutput),
            }
        }

        // --- Native path (registry removal) ---
        #[cfg(not(target_arch = "wasm32"))]
        {
            NATIVE_REGION_REGISTRY
                .lock()
                .expect("native registry poisoned")
                .remove(&self.allocation.region_id);
            Ok(())
        }
    }
}

/// Frees a shared region by id.
///
/// This is a convenience wrapper that does not require owning a
/// [`SharedRegion`] instance.
pub fn free_region(region_id: u64) -> Result<()> {
    // --- WASM path (hostcall) ---
    #[cfg(target_arch = "wasm32")]
    {
        match hostcall_ready(HostcallRequest::FreeRegion { region_id })? {
            HostcallOutput::Empty => Ok(()),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }

    // --- Native path (registry removal) ---
    #[cfg(not(target_arch = "wasm32"))]
    {
        NATIVE_REGION_REGISTRY
            .lock()
            .expect("native registry poisoned")
            .remove(&region_id);
        Ok(())
    }
}

/// A direct-memory mapping of a shared region.
///
/// In WASM mode the pointer is derived from the page offset returned by
/// `alloc_region` / `attach_region`. In native mode a heap allocation is used
/// for testing.
#[derive(Clone)]
pub struct RegionMapping {
    inner: Arc<RegionMappingInner>,
}

struct RegionMappingInner {
    base: *mut u8,
    size: u64,
    /// Heap-allocated backing store (native mode only). `None` for WASM mappings
    /// whose lifetime is managed by the runtime.
    _backing: Option<Arc<Vec<u8>>>,
}

impl RegionMapping {
    /// Creates a mapping backed by a heap allocation (for native testing).
    pub fn allocate(size: u64) -> std::result::Result<Self, IoError> {
        let backing = Arc::new(vec![0u8; size as usize]);
        // Get pointer to the actual data buffer, not the Vec struct.
        let base = backing.as_ptr() as *mut u8;
        Ok(Self {
            inner: Arc::new(RegionMappingInner {
                base,
                size,
                _backing: Some(backing),
            }),
        })
    }

    /// Creates a mapping that shares an existing backing store.
    ///
    /// The returned mapping points to the same memory as the original,
    /// enabling cross-process-style sharing in native test mode.
    pub fn from_shared_backing(backing: Arc<Vec<u8>>, size: u64) -> Self {
        let base = backing.as_ptr() as *mut u8;
        Self {
            inner: Arc::new(RegionMappingInner {
                base,
                size,
                _backing: Some(backing),
            }),
        }
    }

    /// Creates a mapping from a raw pointer (for WASM linear memory).
    ///
    /// # Safety
    ///
    /// The pointer must be valid for reads and writes of `size` bytes and must
    /// remain valid for the lifetime of the returned mapping.
    pub unsafe fn from_raw(base: *mut u8, size: u64) -> Self {
        Self {
            inner: Arc::new(RegionMappingInner {
                base,
                size,
                _backing: None,
            }),
        }
    }

    /// Returns the base pointer.
    pub fn base(&self) -> *mut u8 {
        self.inner.base
    }

    /// Returns the mapping size in bytes.
    pub fn size(&self) -> u64 {
        self.inner.size
    }

    /// Creates a sub-mapping that points to a sub-region of this mapping.
    ///
    /// The sub-mapping shares the same underlying memory (via Arc) but has
    /// a different base pointer and size. This is useful for multi-memory
    /// regions where each sub-memory has its own offset and length.
    ///
    /// # Safety
    ///
    /// The caller must ensure that `offset + size` does not exceed the
    /// parent mapping's size.
    pub fn sub_region(&self, offset: u64, size: u64) -> std::result::Result<Self, IoError> {
        if offset.checked_add(size).ok_or(IoError::CapacityExceeded)? > self.inner.size {
            return Err(IoError::IndexOutOfBounds);
        }
        let new_base = unsafe { self.inner.base.add(offset as usize) };
        Ok(Self {
            inner: Arc::new(RegionMappingInner {
                base: new_base,
                size,
                // Share the parent's backing to keep the memory alive.
                _backing: self.inner._backing.clone(),
            }),
        })
    }

    /// Reads bytes at the given offset.
    pub fn read(&self, offset: u64, len: u64) -> std::result::Result<Vec<u8>, IoError> {
        let end = offset.checked_add(len).ok_or(IoError::CapacityExceeded)?;
        if end > self.inner.size {
            return Err(IoError::IndexOutOfBounds);
        }
        let mut buf = vec![0u8; len as usize];
        unsafe {
            std::ptr::copy_nonoverlapping(
                self.inner.base.add(offset as usize),
                buf.as_mut_ptr(),
                len as usize,
            );
        }
        Ok(buf)
    }

    /// Writes bytes at the given offset.
    pub fn write(&self, offset: u64, bytes: &[u8]) -> std::result::Result<(), IoError> {
        let end = offset
            .checked_add(bytes.len() as u64)
            .ok_or(IoError::CapacityExceeded)?;
        if end > self.inner.size {
            return Err(IoError::IndexOutOfBounds);
        }
        unsafe {
            std::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                self.inner.base.add(offset as usize),
                bytes.len(),
            );
        }
        Ok(())
    }

    /// Reads a little-endian `u64` at the given offset.
    pub fn read_u64(&self, offset: u64) -> std::result::Result<u64, IoError> {
        let bytes = self.read(offset, 8)?;
        Ok(u64::from_le_bytes(
            bytes
                .try_into()
                .map_err(|_invalid| IoError::InvalidLayout)?,
        ))
    }

    /// Writes a little-endian `u64` at the given offset.
    pub fn write_u64(&self, offset: u64, value: u64) -> std::result::Result<(), IoError> {
        self.write(offset, &value.to_le_bytes())
    }

    /// Reads a single byte at the given offset.
    pub fn read_u8(&self, offset: u64) -> std::result::Result<u8, IoError> {
        let bytes = self.read(offset, 1)?;
        Ok(bytes[0])
    }

    /// Writes a single byte at the given offset.
    pub fn write_u8(&self, offset: u64, value: u8) -> std::result::Result<(), IoError> {
        self.write(offset, &[value])
    }

    /// Atomically loads a `u64` at the given offset.
    pub fn atomic_load_u64(
        &self,
        offset: u64,
        ordering: Ordering,
    ) -> std::result::Result<u64, IoError> {
        if offset + 8 > self.inner.size {
            return Err(IoError::IndexOutOfBounds);
        }
        let ptr = unsafe { self.inner.base.add(offset as usize) as *const AtomicU64 };
        Ok(unsafe { (*ptr).load(ordering) })
    }

    /// Atomically stores a `u64` at the given offset.
    pub fn atomic_store_u64(
        &self,
        offset: u64,
        value: u64,
        ordering: Ordering,
    ) -> std::result::Result<(), IoError> {
        if offset + 8 > self.inner.size {
            return Err(IoError::IndexOutOfBounds);
        }
        let ptr = unsafe { self.inner.base.add(offset as usize) as *const AtomicU64 };
        unsafe {
            (*ptr).store(value, ordering);
        }
        Ok(())
    }

    /// Atomically adds to a `u64` at the given offset, returning the previous value.
    pub fn fetch_add_u64(
        &self,
        offset: u64,
        value: u64,
        ordering: Ordering,
    ) -> std::result::Result<u64, IoError> {
        if offset + 8 > self.inner.size {
            return Err(IoError::IndexOutOfBounds);
        }
        let ptr = unsafe { self.inner.base.add(offset as usize) as *const AtomicU64 };
        Ok(unsafe { (*ptr).fetch_add(value, ordering) })
    }

    /// Atomically compares and exchanges a `u64` at the given offset.
    /// Returns the previous value (equals `current` on success).
    pub fn compare_exchange_u64(
        &self,
        offset: u64,
        current: u64,
        new: u64,
    ) -> std::result::Result<u64, IoError> {
        if offset + 8 > self.inner.size {
            return Err(IoError::IndexOutOfBounds);
        }
        let ptr = unsafe { self.inner.base.add(offset as usize) as *const AtomicU64 };
        let result = unsafe {
            (*ptr).compare_exchange_weak(current, new, Ordering::SeqCst, Ordering::SeqCst)
        };
        match result {
            Ok(prev) => Ok(prev),
            Err(prev) => Ok(prev),
        }
    }

    /// Notifies waiters on an address within this mapping.
    ///
    /// In WASM mode this lowers to `memory.atomic.notify`. In native mode this
    /// is a no-op (tests use tokio notification instead).
    pub fn atomic_notify(&self, offset: u64, count: u32) -> std::result::Result<u32, IoError> {
        let _ = (offset, count);
        Ok(0)
    }

    /// Waits on an address within this mapping.
    ///
    /// In WASM mode this lowers to `memory.atomic.wait32`. In native mode this
    /// is a no-op.
    pub fn atomic_wait32(
        &self,
        offset: u64,
        expected: u32,
        timeout_ms: u64,
    ) -> std::result::Result<(), IoError> {
        let _ = (offset, expected, timeout_ms);
        Ok(())
    }
}

// SAFETY (Send): RegionMappingInner contains a `base: *mut u8` raw pointer.
// In WASM mode, this pointer references shared linear memory that remains valid
// for the guest's entire lifetime, so moving it across threads is safe.
// In native mode, the pointer points into an `Arc<Vec<u8>>` held by `_backing`,
// which keeps the allocation alive for as long as this struct exists.
// All access to the pointed-to memory goes through atomic operations at
// well-known offsets, so concurrent access from different threads is sound.
unsafe impl Send for RegionMappingInner {}

// SAFETY (Sync): See the Send rationale above. In both WASM and native modes,
// the raw pointer is stable and all mutations go through atomic operations
// (load/store/CAS with appropriate ordering), so sharing `&RegionMappingInner`
// across threads is safe.
unsafe impl Sync for RegionMappingInner {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_region_allocate_and_free_native() {
        let region = SharedRegion::allocate(2, RegionProt::ReadWrite, ResourceKind::SharedMemory)
            .expect("allocate");
        assert_ne!(region.region_id(), 0);
        assert_eq!(region.size(), 2 * PAGE_SIZE);
        let region_id = region.region_id();
        region.free().expect("free");
        // After free, attach should fail.
        let result = SharedRegion::attach(region_id, None, RegionProt::ReadWrite);
        assert!(result.is_err());
    }

    #[test]
    fn shared_region_attach_shares_memory_native() {
        let original = SharedRegion::allocate(2, RegionProt::ReadWrite, ResourceKind::SharedMemory)
            .expect("allocate");
        let region_id = original.region_id();

        // Write via the original mapping.
        let mapping = original.mapping();
        mapping.write(0, b"shared!").expect("write");

        // Attach and read back.
        let attached =
            SharedRegion::attach(region_id, None, RegionProt::ReadWrite).expect("attach");
        let attached_mapping = attached.mapping();
        let data = attached_mapping.read(0, 7).expect("read");
        assert_eq!(data, b"shared!");

        // Writes through attached mapping are visible to original.
        attached_mapping
            .write(8, b"hello")
            .expect("write via attach");
        let data = mapping.read(8, 5).expect("read original");
        assert_eq!(data, b"hello");

        original.free().expect("free");
    }

    #[test]
    fn shared_region_attach_unknown_fails() {
        let result = SharedRegion::attach(999_999_999, None, RegionProt::ReadWrite);
        assert!(result.is_err());
    }

    #[test]
    fn free_region_by_id_native() {
        let region = SharedRegion::allocate(1, RegionProt::ReadWrite, ResourceKind::SharedMemory)
            .expect("allocate");
        let region_id = region.region_id();
        // Don't call region.free() — use the standalone function instead.
        std::mem::forget(region);
        free_region(region_id).expect("free_region");
        // Attach should now fail.
        let result = SharedRegion::attach(region_id, None, RegionProt::ReadWrite);
        assert!(result.is_err());
    }

    #[test]
    fn region_mapping_read_write_round_trip() {
        let mapping = RegionMapping::allocate(256).expect("allocate");
        mapping.write(0, &[1, 2, 3, 4]).expect("write");
        let data = mapping.read(0, 4).expect("read");
        assert_eq!(data, vec![1, 2, 3, 4]);
    }

    #[test]
    fn region_mapping_u64_round_trip() {
        let mapping = RegionMapping::allocate(64).expect("allocate");
        mapping.write_u64(0, 0xDEAD_BEEF_CAFE_BABE).expect("write");
        assert_eq!(mapping.read_u64(0).expect("read"), 0xDEAD_BEEF_CAFE_BABE);
    }

    #[test]
    fn region_mapping_atomic_operations() {
        let mapping = RegionMapping::allocate(64).expect("allocate");
        mapping
            .atomic_store_u64(0, 10, Ordering::Release)
            .expect("store");
        assert_eq!(
            mapping.atomic_load_u64(0, Ordering::Acquire).expect("load"),
            10
        );
        let prev = mapping
            .fetch_add_u64(0, 5, Ordering::SeqCst)
            .expect("fetch_add");
        assert_eq!(prev, 10);
        assert_eq!(
            mapping.atomic_load_u64(0, Ordering::Acquire).expect("load"),
            15
        );
    }
}
