//! Host-managed shared memory allocator capability.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use selium_abi::{
    GuestResourceId, GuestUint, SHM_RING_CAPACITY_OFFSET, SHM_RING_FLAGS_OFFSET,
    SHM_RING_HEAD_OFFSET, SHM_RING_HEADER_BYTES, SHM_RING_MAGIC, SHM_RING_MAGIC_OFFSET,
    SHM_RING_SEQUENCE_OFFSET, SHM_RING_TAIL_OFFSET, SHM_RING_VERSION, SHM_RING_VERSION_OFFSET,
    ShmAlloc, ShmNotify, ShmReady, ShmRegion, ShmWaitCondition,
};
use tokio::sync::Notify;

use crate::{
    guest_error::GuestError,
    spi::shared_memory::{SharedMemoryBindingContext, SharedMemoryCapability},
};

/// Default shared memory arena size for host allocations.
const DEFAULT_ARENA_BYTES: u64 = 256 * 1024 * 1024;

struct Allocation {
    len: GuestUint,
    bytes: Vec<u8>,
    notify: Arc<Notify>,
}

#[derive(Debug, Clone, Copy)]
struct RingSnapshot {
    head: GuestUint,
    tail: GuestUint,
    sequence: GuestUint,
    readable: GuestUint,
    writable: GuestUint,
}

/// Shared-memory allocator used by kernel SHM hostcalls.
pub struct SharedMemoryDriver {
    next_offset: AtomicU64,
    arena_bytes: u64,
    allocations: Arc<Mutex<BTreeMap<GuestUint, Allocation>>>,
}

impl SharedMemoryDriver {
    /// Create a shared memory driver with the default arena size.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            next_offset: AtomicU64::new(0),
            arena_bytes: DEFAULT_ARENA_BYTES,
            allocations: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }

    fn snapshot_locked(
        allocation: &Allocation,
        region: ShmRegion,
    ) -> Result<RingSnapshot, GuestError> {
        if usize::try_from(region.len).map_err(|_| GuestError::InvalidArgument)?
            != allocation.bytes.len()
        {
            return Err(GuestError::InvalidArgument);
        }

        let magic = read_field(&allocation.bytes, SHM_RING_MAGIC_OFFSET)?;
        if magic != SHM_RING_MAGIC {
            return Err(GuestError::InvalidArgument);
        }

        let version = read_field(&allocation.bytes, SHM_RING_VERSION_OFFSET)?;
        if version != SHM_RING_VERSION {
            return Err(GuestError::InvalidArgument);
        }

        let capacity = read_field(&allocation.bytes, SHM_RING_CAPACITY_OFFSET)?;
        let payload_len = region
            .len
            .checked_sub(
                GuestUint::try_from(SHM_RING_HEADER_BYTES)
                    .map_err(|_| GuestError::InvalidArgument)?,
            )
            .ok_or(GuestError::InvalidArgument)?;
        if capacity == 0 || capacity > payload_len {
            return Err(GuestError::InvalidArgument);
        }

        let head = read_field(&allocation.bytes, SHM_RING_HEAD_OFFSET)?;
        let tail = read_field(&allocation.bytes, SHM_RING_TAIL_OFFSET)?;
        if tail < head {
            return Err(GuestError::InvalidArgument);
        }
        let readable = tail.checked_sub(head).ok_or(GuestError::InvalidArgument)?;
        if readable > capacity {
            return Err(GuestError::InvalidArgument);
        }

        let writable = capacity
            .checked_sub(readable)
            .ok_or(GuestError::InvalidArgument)?;
        let sequence = read_field(&allocation.bytes, SHM_RING_SEQUENCE_OFFSET)?;
        let _flags = read_field(&allocation.bytes, SHM_RING_FLAGS_OFFSET)?;

        Ok(RingSnapshot {
            head,
            tail,
            sequence,
            readable,
            writable,
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

        let minimum = GuestUint::try_from(SHM_RING_HEADER_BYTES + 1)
            .map_err(|_| GuestError::InvalidArgument)?;
        if request.size < minimum {
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
                let mut bytes =
                    vec![0; usize::try_from(size).map_err(|_| GuestError::InvalidArgument)?];
                write_field(&mut bytes, SHM_RING_MAGIC_OFFSET, SHM_RING_MAGIC)?;
                write_field(&mut bytes, SHM_RING_VERSION_OFFSET, SHM_RING_VERSION)?;
                write_field(
                    &mut bytes,
                    SHM_RING_CAPACITY_OFFSET,
                    len.checked_sub(
                        GuestUint::try_from(SHM_RING_HEADER_BYTES)
                            .map_err(|_| GuestError::InvalidArgument)?,
                    )
                    .ok_or(GuestError::InvalidArgument)?,
                )?;
                write_field(&mut bytes, SHM_RING_HEAD_OFFSET, 0)?;
                write_field(&mut bytes, SHM_RING_TAIL_OFFSET, 0)?;
                write_field(&mut bytes, SHM_RING_SEQUENCE_OFFSET, 0)?;
                write_field(&mut bytes, SHM_RING_FLAGS_OFFSET, 0)?;

                let mut allocations = self.allocations.lock().map_err(|_| {
                    GuestError::Subsystem("shared memory allocation lock poisoned".to_string())
                })?;
                allocations.insert(
                    offset,
                    Allocation {
                        len,
                        bytes,
                        notify: Arc::new(Notify::new()),
                    },
                );
                return Ok(ShmRegion { offset, len });
            }
        }
    }

    fn attach_mapping(
        &self,
        binding: &mut dyn SharedMemoryBindingContext,
        shared_id: GuestResourceId,
        region: ShmRegion,
    ) -> Result<GuestUint, Self::Error> {
        let allocations = self.allocations.lock().map_err(|_| {
            GuestError::Subsystem("shared memory allocation lock poisoned".to_string())
        })?;
        let allocation = allocations
            .get(&region.offset)
            .ok_or(GuestError::NotFound)?;
        if allocation.len != region.len {
            return Err(GuestError::InvalidArgument);
        }

        binding.attach_mapping(shared_id)
    }

    fn detach_mapping(
        &self,
        binding: &mut dyn SharedMemoryBindingContext,
        shared_id: GuestResourceId,
        region: ShmRegion,
    ) -> Result<(), Self::Error> {
        let allocations = self.allocations.lock().map_err(|_| {
            GuestError::Subsystem("shared memory allocation lock poisoned".to_string())
        })?;
        let allocation = allocations
            .get(&region.offset)
            .ok_or(GuestError::NotFound)?;
        if allocation.len != region.len {
            return Err(GuestError::InvalidArgument);
        }

        binding.detach_mapping(shared_id)
    }

    fn wait(
        &self,
        _shared_id: GuestResourceId,
        region: ShmRegion,
        condition: ShmWaitCondition,
    ) -> impl std::future::Future<Output = Result<ShmReady, Self::Error>> + Send {
        let allocations = Arc::clone(&self.allocations);

        async move {
            loop {
                let (snapshot, notifier) = {
                    let allocations = allocations.lock().map_err(|_| {
                        GuestError::Subsystem("shared memory allocation lock poisoned".to_string())
                    })?;
                    let allocation = allocations
                        .get(&region.offset)
                        .ok_or(GuestError::NotFound)?;
                    if allocation.len != region.len {
                        return Err(GuestError::InvalidArgument);
                    }
                    (
                        SharedMemoryDriver::snapshot_locked(allocation, region)?,
                        Arc::clone(&allocation.notify),
                    )
                };

                if condition_ready(snapshot, condition) {
                    return Ok(ShmReady {
                        head: snapshot.head,
                        tail: snapshot.tail,
                        sequence: snapshot.sequence,
                        readable: snapshot.readable,
                        writable: snapshot.writable,
                    });
                }

                notifier.notified().await;
            }
        }
    }

    fn notify(
        &self,
        _shared_id: GuestResourceId,
        region: ShmRegion,
        notify: ShmNotify,
    ) -> Result<ShmReady, Self::Error> {
        let (snapshot, notifier) = {
            let allocations = self.allocations.lock().map_err(|_| {
                GuestError::Subsystem("shared memory allocation lock poisoned".to_string())
            })?;
            let allocation = allocations
                .get(&region.offset)
                .ok_or(GuestError::NotFound)?;
            if allocation.len != region.len {
                return Err(GuestError::InvalidArgument);
            }

            let snapshot = Self::snapshot_locked(allocation, region)?;
            if notify.sequence < snapshot.sequence {
                return Err(GuestError::InvalidArgument);
            }

            (snapshot, Arc::clone(&allocation.notify))
        };

        notifier.notify_waiters();

        Ok(ShmReady {
            head: snapshot.head,
            tail: snapshot.tail,
            sequence: snapshot.sequence,
            readable: snapshot.readable,
            writable: snapshot.writable,
        })
    }
}

fn condition_ready(snapshot: RingSnapshot, condition: ShmWaitCondition) -> bool {
    match condition {
        ShmWaitCondition::DataAvailable => snapshot.readable > 0,
        ShmWaitCondition::SpaceAvailable => snapshot.writable > 0,
        ShmWaitCondition::SequenceAtLeast(sequence) => snapshot.sequence >= sequence,
    }
}

fn align_up(value: u64, align: u64) -> Option<u64> {
    let mask = align.checked_sub(1)?;
    value.checked_add(mask).map(|aligned| aligned & !mask)
}

fn read_field(bytes: &[u8], offset: usize) -> Result<GuestUint, GuestError> {
    let end = offset
        .checked_add(core::mem::size_of::<GuestUint>())
        .ok_or(GuestError::InvalidArgument)?;
    let slice = bytes.get(offset..end).ok_or(GuestError::InvalidArgument)?;
    let mut value = [0u8; core::mem::size_of::<GuestUint>()];
    value.copy_from_slice(slice);
    Ok(GuestUint::from_le_bytes(value))
}

fn write_field(bytes: &mut [u8], offset: usize, value: GuestUint) -> Result<(), GuestError> {
    let end = offset
        .checked_add(core::mem::size_of::<GuestUint>())
        .ok_or(GuestError::InvalidArgument)?;
    let slot = bytes
        .get_mut(offset..end)
        .ok_or(GuestError::InvalidArgument)?;
    slot.copy_from_slice(&value.to_le_bytes());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct Binding {
        attached: Vec<GuestResourceId>,
        detached: Vec<GuestResourceId>,
    }

    impl SharedMemoryBindingContext for Binding {
        fn attach_mapping(&mut self, shared_id: GuestResourceId) -> Result<GuestUint, GuestError> {
            self.attached.push(shared_id);
            Ok(2048)
        }

        fn detach_mapping(&mut self, shared_id: GuestResourceId) -> Result<(), GuestError> {
            self.detached.push(shared_id);
            Ok(())
        }
    }

    #[tokio::test]
    async fn allocation_round_trip_attach_wait_notify() {
        let driver = SharedMemoryDriver::new();
        let region = driver
            .alloc(ShmAlloc { size: 64, align: 8 })
            .expect("allocate region");

        let mut binding = Binding::default();
        let mapping = driver
            .attach_mapping(&mut binding, 1, region)
            .expect("attach mapping");
        assert_eq!(mapping, 2048);

        let ready = driver
            .wait(1, region, ShmWaitCondition::SpaceAvailable)
            .await
            .expect("wait for writable space");
        assert!(ready.writable > 0);

        let notified = driver
            .notify(
                1,
                region,
                ShmNotify {
                    resource_id: 0,
                    sequence: 0,
                },
            )
            .expect("notify");
        assert_eq!(notified.sequence, 0);

        driver
            .detach_mapping(&mut binding, 1, region)
            .expect("detach mapping");
    }

    #[test]
    fn allocation_rejects_too_small_region() {
        let driver = SharedMemoryDriver::new();
        let error = driver
            .alloc(ShmAlloc {
                size: GuestUint::try_from(SHM_RING_HEADER_BYTES).expect("header fits in u32"),
                align: 8,
            })
            .expect_err("allocation must fail");
        assert!(matches!(error, GuestError::InvalidArgument));
    }
}
