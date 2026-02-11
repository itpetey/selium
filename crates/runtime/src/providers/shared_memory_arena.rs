//! Runtime-backed shared memory allocator capability.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use selium_abi::{GuestUint, ShmAlloc, ShmRegion};
use selium_kernel::{guest_error::GuestError, spi::shared_memory::SharedMemoryCapability};

/// Default shared memory arena size for host allocations.
const DEFAULT_ARENA_BYTES: u64 = 256 * 1024 * 1024;

struct Allocation {
    len: GuestUint,
    bytes: Vec<u8>,
}

/// Shared-memory allocator used by kernel SHM hostcalls.
pub struct SharedMemoryDriver {
    next_offset: AtomicU64,
    arena_bytes: u64,
    allocations: Mutex<BTreeMap<GuestUint, Allocation>>,
}

impl SharedMemoryDriver {
    /// Create a shared memory driver with the default arena size.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            next_offset: AtomicU64::new(0),
            arena_bytes: DEFAULT_ARENA_BYTES,
            allocations: Mutex::new(BTreeMap::new()),
        })
    }
}

impl SharedMemoryCapability for SharedMemoryDriver {
    type Error = GuestError;

    fn alloc(&self, request: ShmAlloc) -> Result<ShmRegion, Self::Error> {
        if request.size == 0 {
            return Err(GuestError::InvalidArgument);
        }

        if request.align == 0 || !request.align.is_power_of_two() {
            return Err(GuestError::InvalidArgument);
        }

        let size = u64::from(request.size);
        let align = u64::from(request.align);

        loop {
            let current = self.next_offset.load(Ordering::Acquire);
            let aligned = align_up(current, align).ok_or(GuestError::InvalidArgument)?;
            let end = aligned
                .checked_add(size)
                .ok_or(GuestError::InvalidArgument)?;
            if end > self.arena_bytes {
                return Err(GuestError::WouldBlock);
            }

            if self
                .next_offset
                .compare_exchange(current, end, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                let offset =
                    GuestUint::try_from(aligned).map_err(|_| GuestError::InvalidArgument)?;
                let len = GuestUint::try_from(size).map_err(|_| GuestError::InvalidArgument)?;
                let allocation_size =
                    usize::try_from(size).map_err(|_| GuestError::InvalidArgument)?;
                let mut allocations = self.allocations.lock().map_err(|_| {
                    GuestError::Subsystem("shared memory allocation lock poisoned".to_string())
                })?;
                allocations.insert(
                    offset,
                    Allocation {
                        len,
                        bytes: vec![0; allocation_size],
                    },
                );
                return Ok(ShmRegion { offset, len });
            }
        }
    }

    fn read(
        &self,
        region: ShmRegion,
        offset: GuestUint,
        len: GuestUint,
    ) -> Result<Vec<u8>, Self::Error> {
        let (start, end) = checked_bounds(region.len, offset, len)?;
        let allocations = self.allocations.lock().map_err(|_| {
            GuestError::Subsystem("shared memory allocation lock poisoned".to_string())
        })?;
        let allocation = allocations
            .get(&region.offset)
            .ok_or(GuestError::NotFound)?;
        if allocation.len != region.len {
            return Err(GuestError::InvalidArgument);
        }

        Ok(allocation.bytes[start..end].to_vec())
    }

    fn write(&self, region: ShmRegion, offset: GuestUint, bytes: &[u8]) -> Result<(), Self::Error> {
        let len = GuestUint::try_from(bytes.len()).map_err(|_| GuestError::InvalidArgument)?;
        let (start, end) = checked_bounds(region.len, offset, len)?;
        let mut allocations = self.allocations.lock().map_err(|_| {
            GuestError::Subsystem("shared memory allocation lock poisoned".to_string())
        })?;
        let allocation = allocations
            .get_mut(&region.offset)
            .ok_or(GuestError::NotFound)?;
        if allocation.len != region.len {
            return Err(GuestError::InvalidArgument);
        }

        allocation.bytes[start..end].copy_from_slice(bytes);
        Ok(())
    }
}

fn align_up(value: u64, align: u64) -> Option<u64> {
    let mask = align.checked_sub(1)?;
    value.checked_add(mask).map(|aligned| aligned & !mask)
}

fn checked_bounds(
    region_len: GuestUint,
    offset: GuestUint,
    len: GuestUint,
) -> Result<(usize, usize), GuestError> {
    let end = offset.checked_add(len).ok_or(GuestError::InvalidArgument)?;
    if end > region_len {
        return Err(GuestError::InvalidArgument);
    }

    let start = usize::try_from(offset).map_err(|_| GuestError::InvalidArgument)?;
    let end = usize::try_from(end).map_err(|_| GuestError::InvalidArgument)?;
    Ok((start, end))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocation_round_trip_write_and_read() {
        let driver = SharedMemoryDriver::new();
        let region = driver
            .alloc(ShmAlloc { size: 32, align: 8 })
            .expect("allocate region");
        driver
            .write(region, 4, b"ping")
            .expect("write bytes into region");
        let data = driver.read(region, 4, 4).expect("read bytes from region");
        assert_eq!(data, b"ping");
    }

    #[test]
    fn write_rejects_out_of_bounds() {
        let driver = SharedMemoryDriver::new();
        let region = driver
            .alloc(ShmAlloc { size: 8, align: 8 })
            .expect("allocate region");
        let error = driver
            .write(region, 6, b"abcd")
            .expect_err("write must fail");
        assert!(matches!(error, GuestError::InvalidArgument));
    }
}
