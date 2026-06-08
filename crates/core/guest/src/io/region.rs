use std::{
    sync::Arc,
    sync::atomic::{AtomicU64, Ordering},
};

use crate::io::error::{Error, Result};

/// Byte offset where ring buffer data begins (page 1).
pub const DATA_OFFSET: u64 = PAGE_SIZE;
/// Byte offset of the generation counter within the shared region.
pub const GENERATION_COUNTER_OFFSET: u64 = 0;
/// Maximum number of strong reader slots available in the shared region.
pub const MAX_READER_SLOTS: usize = 128;
/// Minimum region size that can hold a ring buffer (header page + one data page).
pub const MIN_REGION_BYTES: u64 = PAGE_SIZE * 2;
/// Byte offset of the shared `next_tail` cursor (writers CAS to reserve space).
pub const NEXT_TAIL_OFFSET: u64 = 8;
/// Byte offset of the shared `next_writer_id` counter (fetch_add for unique writer IDs).
pub const NEXT_WRITER_ID_OFFSET: u64 = 1048;
/// WASM page size used for region layout (4 KiB).
pub const PAGE_SIZE: u64 = 4096;
/// Byte offset where the shared `reader_slots` array begins (128 × u64).
pub const READER_SLOTS_OFFSET: u64 = 24;
/// Byte offset of the shared `reader_slot_counter` (fetch_add for unique reader slot indices).
pub const READER_SLOT_COUNTER_OFFSET: u64 = 1056;
/// Byte offset of the shared `writer_count` (incremented/decremented atomically).
pub const WRITER_COUNT_OFFSET: u64 = 16;

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

/// A builder for creating or attaching to a shared memory ring buffer region.
pub struct RegionBuilder;

/// A shared memory region allocated for ring buffer I/O.
///
/// The shared region contains cross-process coordination fields in page 0:
/// generation counter (offset 0), `next_tail` (offset 8), `writer_count`
/// (offset 16), `reader_slots` (128 × u64 at offset 24), `next_writer_id`
/// (offset 1048), and `reader_slot_counter` (offset 1056). Ring buffer data
/// starts at page 1 (offset 4096).
///
/// Process-local optimisation fields (`tail_cache`, `next_mutation_id`) live
/// in per-guest private memory via `ChannelPrivateState`.
#[derive(Clone)]
pub struct ChannelRegion {
    region_id: u64,
    mapping: RegionMapping,
    private: Arc<ChannelPrivateState>,
    capacity: u64,
    size: u64,
}

/// Per-guest private channel state. Not stored in shared memory.
///
/// Only process-local optimisation fields remain here. Cross-process
/// coordination metadata (`next_tail`, `writer_count`, `reader_slots`,
/// `next_writer_id`) lives in the shared region at well-known offsets.
struct ChannelPrivateState {
    tail_cache: AtomicU64,
    next_mutation_id: AtomicU64,
}

impl RegionMapping {
    /// Creates a mapping backed by a heap allocation (for native testing).
    pub fn allocate(size: u64) -> Result<Self> {
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
    pub fn sub_region(&self, offset: u64, size: u64) -> Result<Self> {
        if offset.checked_add(size).ok_or(Error::CapacityExceeded)? > self.inner.size {
            return Err(Error::IndexOutOfBounds);
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
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        let end = offset.checked_add(len).ok_or(Error::CapacityExceeded)?;
        if end > self.inner.size {
            return Err(Error::IndexOutOfBounds);
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
    pub fn write(&self, offset: u64, bytes: &[u8]) -> Result<()> {
        let end = offset
            .checked_add(bytes.len() as u64)
            .ok_or(Error::CapacityExceeded)?;
        if end > self.inner.size {
            return Err(Error::IndexOutOfBounds);
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
    pub fn read_u64(&self, offset: u64) -> Result<u64> {
        let bytes = self.read(offset, 8)?;
        Ok(u64::from_le_bytes(
            bytes.try_into().map_err(|_invalid| Error::InvalidLayout)?,
        ))
    }

    /// Writes a little-endian `u64` at the given offset.
    pub fn write_u64(&self, offset: u64, value: u64) -> Result<()> {
        self.write(offset, &value.to_le_bytes())
    }

    /// Atomically loads a `u64` at the given offset.
    pub fn atomic_load_u64(&self, offset: u64, ordering: Ordering) -> Result<u64> {
        if offset + 8 > self.inner.size {
            return Err(Error::IndexOutOfBounds);
        }
        let ptr = unsafe { self.inner.base.add(offset as usize) as *const AtomicU64 };
        Ok(unsafe { (*ptr).load(ordering) })
    }

    /// Atomically stores a `u64` at the given offset.
    pub fn atomic_store_u64(&self, offset: u64, value: u64, ordering: Ordering) -> Result<()> {
        if offset + 8 > self.inner.size {
            return Err(Error::IndexOutOfBounds);
        }
        let ptr = unsafe { self.inner.base.add(offset as usize) as *const AtomicU64 };
        unsafe {
            (*ptr).store(value, ordering);
        }
        Ok(())
    }

    /// Atomically adds to a `u64` at the given offset, returning the previous value.
    pub fn fetch_add_u64(&self, offset: u64, value: u64, ordering: Ordering) -> Result<u64> {
        if offset + 8 > self.inner.size {
            return Err(Error::IndexOutOfBounds);
        }
        let ptr = unsafe { self.inner.base.add(offset as usize) as *const AtomicU64 };
        Ok(unsafe { (*ptr).fetch_add(value, ordering) })
    }

    /// Atomically compares and exchanges a `u64` at the given offset.
    /// Returns the previous value (equals `current` on success).
    pub fn compare_exchange_u64(&self, offset: u64, current: u64, new: u64) -> Result<u64> {
        if offset + 8 > self.inner.size {
            return Err(Error::IndexOutOfBounds);
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
    pub fn atomic_notify(&self, offset: u64, count: u32) -> Result<u32> {
        let _ = (offset, count);
        Ok(0)
    }

    /// Waits on an address within this mapping.
    ///
    /// In WASM mode this lowers to `memory.atomic.wait32`. In native mode this
    /// is a no-op.
    pub fn atomic_wait32(&self, offset: u64, expected: u32, timeout_ms: u64) -> Result<()> {
        let _ = (offset, expected, timeout_ms);
        Ok(())
    }
}

unsafe impl Send for RegionMappingInner {}

unsafe impl Sync for RegionMappingInner {}

impl RegionBuilder {
    /// Creates a new shared memory region for a ring buffer of the given capacity.
    ///
    /// In native mode this uses a heap allocation. In WASM mode it would call
    /// the `alloc_region` hostcall.
    pub fn create(capacity: u64) -> Result<ChannelRegion> {
        let total_aligned = aligned_region_size(capacity)?;
        let mapping = RegionMapping::allocate(total_aligned)?;
        Ok(ChannelRegion {
            region_id: 0,
            mapping,
            private: Arc::new(ChannelPrivateState::default()),
            capacity,
            size: total_aligned,
        })
    }

    /// Attaches to an existing shared memory region by its region id.
    ///
    /// In native mode this creates a new heap allocation (for testing). In WASM
    /// mode it would call the `attach_region` hostcall.
    pub fn attach(region_id: u64, capacity: u64) -> Result<ChannelRegion> {
        let total_aligned = aligned_region_size(capacity)?;
        let mapping = RegionMapping::allocate(total_aligned)?;
        Ok(ChannelRegion {
            region_id,
            mapping,
            private: Arc::new(ChannelPrivateState::default()),
            capacity,
            size: total_aligned,
        })
    }
}

impl ChannelRegion {
    /// Wraps an existing region mapping as a channel region.
    pub fn from_mapping(mapping: RegionMapping, capacity: u64) -> Self {
        let size = DATA_OFFSET + capacity;
        Self {
            region_id: 0,
            mapping,
            private: Arc::new(ChannelPrivateState::default()),
            capacity,
            size,
        }
    }

    /// Returns the shared region id.
    pub fn region_id(&self) -> u64 {
        self.region_id
    }

    /// Returns the ring data capacity in bytes.
    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    /// Returns the total region size in bytes.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Returns the data offset within the region where ring bytes start.
    pub fn data_offset(&self) -> u64 {
        DATA_OFFSET
    }

    /// Returns a reference to the underlying region mapping.
    pub fn mapping(&self) -> &RegionMapping {
        &self.mapping
    }

    /// Loads the generation counter from shared memory.
    pub fn load_generation(&self) -> Result<u64> {
        self.mapping
            .atomic_load_u64(GENERATION_COUNTER_OFFSET, Ordering::Acquire)
    }

    /// Increments the generation counter in shared memory and notifies waiters.
    pub fn bump_generation(&self) -> Result<u64> {
        let new_gen =
            self.mapping
                .fetch_add_u64(GENERATION_COUNTER_OFFSET, 1, Ordering::Release)?;
        let _ = self
            .mapping
            .atomic_notify(GENERATION_COUNTER_OFFSET, u32::MAX);
        Ok(new_gen + 1)
    }

    /// Loads the shared `next_tail` cursor from shared memory.
    pub fn load_next_tail(&self) -> Result<u64> {
        self.mapping
            .atomic_load_u64(NEXT_TAIL_OFFSET, Ordering::Acquire)
    }

    /// Atomically CAS on the shared `next_tail` cursor.
    /// Returns the previous value (equals `current` on success).
    pub fn cas_next_tail(&self, current: u64, new: u64) -> Result<u64> {
        self.mapping
            .compare_exchange_u64(NEXT_TAIL_OFFSET, current, new)
    }

    /// Reads the next_tail cursor from shared memory (alias for `load_next_tail`).
    pub fn read_next_tail(&self) -> Result<u64> {
        self.load_next_tail()
    }

    /// Writes the next_tail cursor to shared memory.
    pub fn write_next_tail(&self, value: u64) -> Result<()> {
        self.mapping
            .atomic_store_u64(NEXT_TAIL_OFFSET, value, Ordering::Release)
    }

    /// Reads the tail_cache from private state.
    pub fn read_tail_cache(&self) -> Result<u64> {
        Ok(self.private.tail_cache.load(Ordering::Acquire))
    }

    /// Loads the shared `writer_count` from shared memory.
    pub fn load_writer_count(&self) -> Result<u64> {
        self.mapping
            .atomic_load_u64(WRITER_COUNT_OFFSET, Ordering::Acquire)
    }

    /// Atomically adds to the shared `writer_count`, returning the previous value.
    pub fn fetch_add_writer_count(&self, delta: u64) -> Result<u64> {
        self.mapping
            .fetch_add_u64(WRITER_COUNT_OFFSET, delta, Ordering::SeqCst)
    }

    /// Reads the writer count from shared memory.
    pub fn read_writer_count(&self) -> Result<u64> {
        self.load_writer_count()
    }

    /// Increments the shared writer count and returns the previous value.
    pub fn increment_writer_count(&self) -> Result<u64> {
        self.fetch_add_writer_count(1)
    }

    /// Decrements the shared writer count.
    pub fn decrement_writer_count(&self) -> Result<()> {
        self.fetch_add_writer_count(u64::MAX)?;
        Ok(())
    }

    /// Loads a reader slot value from the shared `reader_slots` array.
    pub fn load_reader_slot(&self, slot: u32) -> Result<u64> {
        if slot as usize >= MAX_READER_SLOTS {
            return Err(Error::InvalidLayout);
        }
        let offset = READER_SLOTS_OFFSET + slot as u64 * 8;
        self.mapping.atomic_load_u64(offset, Ordering::Acquire)
    }

    /// Stores a value into a reader slot in the shared `reader_slots` array.
    pub fn store_reader_slot(&self, slot: u32, value: u64) -> Result<()> {
        if slot as usize >= MAX_READER_SLOTS {
            return Err(Error::InvalidLayout);
        }
        let offset = READER_SLOTS_OFFSET + slot as u64 * 8;
        self.mapping
            .atomic_store_u64(offset, value, Ordering::Release)
    }

    /// Atomically increments the shared `next_writer_id` counter, returning the previous value.
    pub fn fetch_add_next_writer_id(&self) -> Result<u64> {
        self.mapping
            .fetch_add_u64(NEXT_WRITER_ID_OFFSET, 1, Ordering::SeqCst)
    }

    /// Atomically increments the shared `reader_slot_counter`, returning the previous value.
    pub fn fetch_add_reader_slot_counter(&self) -> Result<u64> {
        self.mapping
            .fetch_add_u64(READER_SLOT_COUNTER_OFFSET, 1, Ordering::SeqCst)
    }

    /// Allocates a stable writer id from the shared counter.
    pub fn allocate_writer_id(&self) -> Result<u32> {
        let id = self.fetch_add_next_writer_id()?;
        if id > u64::from(u32::MAX) {
            return Err(Error::CapacityExceeded);
        }
        Ok(id as u32)
    }

    /// Allocates a globally unique mutation id from private state.
    pub fn allocate_mutation_id(&self) -> Result<u64> {
        Ok(self.private.next_mutation_id.fetch_add(1, Ordering::SeqCst) + 1)
    }

    /// Reads the strong reader count by scanning shared reader slots.
    pub fn read_reader_count(&self) -> Result<u64> {
        let mut count = 0;
        for i in 0..MAX_READER_SLOTS as u32 {
            if self.load_reader_slot(i)? != 0 {
                count += 1;
            }
        }
        Ok(count)
    }

    /// Allocates a reader cursor slot via the shared `reader_slot_counter`
    /// (fetch_add for global uniqueness) and initialises it to `position`.
    pub fn allocate_reader_slot(&self, position: u64) -> Result<u32> {
        let slot_index = self.fetch_add_reader_slot_counter()?;
        if slot_index >= MAX_READER_SLOTS as u64 {
            return Err(Error::CapacityExceeded);
        }
        let encoded_position = encode_reader_position(position)?;
        self.store_reader_slot(slot_index as u32, encoded_position)?;
        Ok(slot_index as u32)
    }

    /// Updates an allocated reader cursor slot in the shared `reader_slots` array.
    pub fn update_reader_slot(&self, slot: u32, position: u64) -> Result<()> {
        let encoded = encode_reader_position(position)?;
        self.store_reader_slot(slot, encoded)
    }

    /// Releases an allocated reader cursor slot in the shared `reader_slots` array.
    pub fn release_reader_slot(&self, slot: u32) -> Result<()> {
        self.store_reader_slot(slot, 0)
    }

    /// Returns the minimum active strong-reader cursor from shared reader slots.
    pub fn minimum_reader_position(&self) -> Result<Option<u64>> {
        let mut minimum = None;
        for i in 0..MAX_READER_SLOTS as u32 {
            let encoded_position = self.load_reader_slot(i)?;
            if encoded_position == 0 {
                continue;
            }
            let position = encoded_position - 1;
            minimum = Some(minimum.map_or(position, |current: u64| current.min(position)));
        }
        Ok(minimum)
    }

    /// Atomically reserves `len` bytes at the tail, returning the reservation position.
    ///
    /// Uses exponential backoff on CAS contention (1→2→4→…→64 spin-loop iterations).
    /// CAS operates on the shared `next_tail` field for cross-process coordination.
    pub fn reserve_tail(&self, len: u64, protect_readers: bool) -> Result<u64> {
        if len == 0 || len > self.capacity {
            return Err(Error::CapacityExceeded);
        }

        let mut delay: usize = 1;
        loop {
            let tail = self.load_next_tail()?;
            let minimum_reader_position = if protect_readers {
                self.minimum_reader_position()?
            } else {
                None
            };
            let next = reserve_tail_next(
                tail,
                len,
                self.capacity,
                minimum_reader_position,
                protect_readers,
            )?;

            let prev = self.cas_next_tail(tail, next)?;
            if prev == tail {
                return Ok(tail);
            }

            // Exponential backoff on contention.
            for _ in 0..delay {
                std::hint::spin_loop();
            }
            delay = (delay * 2).min(64);
        }
    }

    /// Reads bytes from the ring data area.
    pub fn read_data(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        let data_offset = DATA_OFFSET + offset;
        self.mapping.read(data_offset, len)
    }

    /// Writes bytes to the ring data area.
    pub fn write_data(&self, offset: u64, bytes: &[u8]) -> Result<()> {
        let data_offset = DATA_OFFSET + offset;
        self.mapping.write(data_offset, bytes)
    }

    /// Initialises a fresh region with all shared coordination fields zeroed.
    pub fn initialise(&self) -> Result<()> {
        self.mapping
            .atomic_store_u64(GENERATION_COUNTER_OFFSET, 0, Ordering::Release)?;
        self.mapping
            .atomic_store_u64(NEXT_TAIL_OFFSET, 0, Ordering::Release)?;
        self.mapping
            .atomic_store_u64(WRITER_COUNT_OFFSET, 0, Ordering::Release)?;
        for i in 0..MAX_READER_SLOTS as u32 {
            self.store_reader_slot(i, 0)?;
        }
        self.mapping
            .atomic_store_u64(NEXT_WRITER_ID_OFFSET, 0, Ordering::Release)?;
        self.mapping
            .atomic_store_u64(READER_SLOT_COUNTER_OFFSET, 0, Ordering::Release)
    }
}

impl Default for ChannelPrivateState {
    fn default() -> Self {
        Self {
            tail_cache: AtomicU64::new(0),
            next_mutation_id: AtomicU64::new(0),
        }
    }
}

fn aligned_region_size(capacity: u64) -> Result<u64> {
    let total = DATA_OFFSET
        .checked_add(capacity)
        .ok_or(Error::CapacityExceeded)?;
    Ok(total
        .checked_next_power_of_two()
        .ok_or(Error::CapacityExceeded)?
        .max(MIN_REGION_BYTES))
}

fn encode_reader_position(position: u64) -> Result<u64> {
    position.checked_add(1).ok_or(Error::CapacityExceeded)
}

fn reserve_tail_next(
    tail: u64,
    len: u64,
    capacity: u64,
    minimum_reader_position: Option<u64>,
    protect_readers: bool,
) -> Result<u64> {
    if len == 0 || len > capacity {
        return Err(Error::CapacityExceeded);
    }
    let next = tail
        .checked_add(len)
        .filter(|next| *next < u64::MAX)
        .ok_or(Error::CapacityExceeded)?;
    if protect_readers {
        let head = minimum_reader_position.unwrap_or(tail);
        if next.saturating_sub(head) > capacity {
            return Err(Error::BufferFull);
        }
    }
    Ok(next)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reader_position_encoding_reserves_zero_for_empty_slots() {
        assert_eq!(encode_reader_position(0), Ok(1));
        assert_eq!(
            encode_reader_position(u64::MAX),
            Err(Error::CapacityExceeded)
        );
    }

    #[test]
    fn aligned_region_size_accounts_for_header_and_limits() {
        assert_eq!(aligned_region_size(1), Ok(MIN_REGION_BYTES));
        assert_eq!(aligned_region_size(4096), Ok(8192));
        assert_eq!(aligned_region_size(u64::MAX), Err(Error::CapacityExceeded));
        // u32::MAX capacity + header fits in u64 but next_power_of_two overflows
        assert_eq!(
            aligned_region_size(u64::MAX - DATA_OFFSET),
            Err(Error::CapacityExceeded)
        );
    }

    #[test]
    fn reserve_tail_next_checks_capacity_and_overflow() {
        assert_eq!(reserve_tail_next(10, 8, 64, None, false), Ok(18));
        assert_eq!(
            reserve_tail_next(10, 0, 64, None, false),
            Err(Error::CapacityExceeded)
        );
        assert_eq!(
            reserve_tail_next(u64::MAX - 4, 4, 64, None, false),
            Err(Error::CapacityExceeded)
        );
        assert_eq!(
            reserve_tail_next(100, 20, 64, Some(40), true),
            Err(Error::BufferFull)
        );
        assert_eq!(reserve_tail_next(100, 20, 64, Some(60), true), Ok(120));
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

    #[test]
    fn channel_region_reserve_tail_with_backoff() {
        let region = RegionBuilder::create(64).expect("create");
        region.initialise().expect("init");
        let pos = region.reserve_tail(8, false).expect("reserve");
        assert_eq!(pos, 0);
        let pos2 = region.reserve_tail(8, false).expect("reserve");
        assert_eq!(pos2, 8);
    }

    #[test]
    fn exponential_backoff_under_concurrent_contention() {
        let region = std::sync::Arc::new(RegionBuilder::create(4096).expect("create"));
        region.initialise().expect("init");

        let mut handles = Vec::new();
        let thread_count = 8;
        let reservations_per_thread = 32;

        for _ in 0..thread_count {
            let r = region.clone();
            handles.push(std::thread::spawn(move || {
                for _ in 0..reservations_per_thread {
                    // Each reservation of 8 bytes should succeed despite contention.
                    let pos = r.reserve_tail(8, false).expect("reserve under contention");
                    // Positions must be unique and aligned.
                    assert_eq!(pos % 8, 0, "reservation {pos} not aligned");
                }
            }));
        }

        for h in handles {
            h.join().expect("thread panicked");
        }
    }

    #[test]
    fn two_writers_coordinate_on_shared_next_tail() {
        // Two ChannelRegion clones sharing the same underlying mapping
        // simulate cross-process writers.
        let region_a = RegionBuilder::create(4096).expect("create");
        region_a.initialise().expect("init");
        let region_b = region_a.clone();

        let pos_a = region_a.reserve_tail(16, false).expect("reserve a");
        let pos_b = region_b.reserve_tail(16, false).expect("reserve b");

        // Positions must be unique and non-overlapping.
        assert_ne!(pos_a, pos_b);
        assert!(
            pos_a + 16 <= pos_b || pos_b + 16 <= pos_a,
            "reservations must not overlap"
        );

        // Both regions see the same next_tail.
        assert_eq!(
            region_a.load_next_tail().expect("tail a"),
            region_b.load_next_tail().expect("tail b"),
        );
    }

    #[test]
    fn reader_sees_writer_count_from_cloned_region() {
        let region_a = RegionBuilder::create(64).expect("create");
        region_a.initialise().expect("init");
        let region_b = region_a.clone();

        // Writer count is shared.
        assert_eq!(region_a.load_writer_count().expect("wc a"), 0);
        region_a.increment_writer_count().expect("inc");
        assert_eq!(region_b.load_writer_count().expect("wc b"), 1);

        region_a.decrement_writer_count().expect("dec");
        assert_eq!(region_b.load_writer_count().expect("wc b after dec"), 0);
    }

    #[test]
    fn writer_backpressure_from_shared_reader_slots() {
        let region = RegionBuilder::create(64).expect("create");
        region.initialise().expect("init");

        // Allocate a reader slot at position 0.
        let slot = region.allocate_reader_slot(0).expect("alloc slot");

        // Fill the ring up to capacity.
        let mut total_reserved = 0u64;
        loop {
            match region.reserve_tail(8, true) {
                Ok(_pos) => total_reserved += 8,
                Err(Error::BufferFull) => break,
                Err(e) => panic!("unexpected error: {e}"),
            }
        }

        // The ring should be full because the reader slot is at position 0.
        assert!(
            total_reserved <= 64,
            "reserved {total_reserved} bytes but capacity is 64"
        );

        // Advance the reader slot to free space.
        region
            .update_reader_slot(slot, total_reserved)
            .expect("update slot");

        // Now we should be able to reserve more space.
        let pos = region.reserve_tail(8, true).expect("reserve after advance");
        assert!(pos >= total_reserved);
    }

    #[test]
    fn reader_detects_eof_when_writer_count_reaches_zero() {
        let region = RegionBuilder::create(64).expect("create");
        region.initialise().expect("init");

        // Initially no writers.
        assert_eq!(region.load_writer_count().expect("wc"), 0);

        // Add a writer.
        region.increment_writer_count().expect("inc");
        assert_eq!(region.load_writer_count().expect("wc"), 1);

        // Writer disconnects.
        region.decrement_writer_count().expect("dec");
        assert_eq!(region.load_writer_count().expect("wc after dec"), 0);
    }

    #[test]
    fn shared_reader_slot_counter_allocates_unique_indices() {
        let region_a = RegionBuilder::create(4096).expect("create");
        region_a.initialise().expect("init");
        let region_b = region_a.clone();

        let slot_a = region_a.allocate_reader_slot(0).expect("alloc a");
        let slot_b = region_b.allocate_reader_slot(0).expect("alloc b");

        assert_ne!(slot_a, slot_b, "slot indices must be unique");
    }

    #[test]
    fn shared_writer_id_allocation() {
        let region_a = RegionBuilder::create(64).expect("create");
        region_a.initialise().expect("init");
        let region_b = region_a.clone();

        let id_a = region_a.allocate_writer_id().expect("id a");
        let id_b = region_b.allocate_writer_id().expect("id b");

        assert_ne!(id_a, id_b, "writer IDs must be unique");
    }
}
