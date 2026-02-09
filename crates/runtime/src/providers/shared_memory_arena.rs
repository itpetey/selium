//! Runtime-backed shared memory allocator capability.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use selium_abi::{GuestUint, ShmAlloc, ShmRegion};
use selium_kernel::{guest_error::GuestError, spi::shared_memory::SharedMemoryCapability};

/// Default shared memory arena size for host allocations.
const DEFAULT_ARENA_BYTES: u64 = 256 * 1024 * 1024;

/// Shared-memory allocator used by kernel SHM hostcalls.
pub struct SharedMemoryDriver {
    next_offset: AtomicU64,
    arena_bytes: u64,
}

impl SharedMemoryDriver {
    /// Create a shared memory driver with the default arena size.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            next_offset: AtomicU64::new(0),
            arena_bytes: DEFAULT_ARENA_BYTES,
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
                return Ok(ShmRegion { offset, len });
            }
        }
    }
}

fn align_up(value: u64, align: u64) -> Option<u64> {
    let mask = align.checked_sub(1)?;
    value.checked_add(mask).map(|aligned| aligned & !mask)
}
