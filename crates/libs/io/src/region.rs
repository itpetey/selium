use crate::error::{Error, Result};

pub const CAPACITY_OFFSET: u64 = 8;
pub const MAGIC_OFFSET: u64 = 0;
/// Minimum region size that can hold a ring buffer.
pub const MIN_REGION_BYTES: u64 = 8192;
pub const NEXT_MUTATION_ID_OFFSET: u64 = 64;
pub const NEXT_TAIL_OFFSET: u64 = 32;
pub const NEXT_WRITER_ID_OFFSET: u64 = 56;
pub const READER_ACTIVE_OFFSET: u64 = 0;
pub const READER_COUNT_OFFSET: u64 = 24;
pub const READER_SLOT_BYTES: u64 = 16;
pub const READER_SLOTS_OFFSET: u64 = 72;
/// Layout constants for a shared-memory ring buffer region.
pub const REGION_HEADER_BYTES: u64 = 4096;
pub const SIGNAL_SHARED_ID_OFFSET: u64 = 48;
pub const TAIL_CACHE_OFFSET: u64 = 40;
pub const WRITER_COUNT_OFFSET: u64 = 16;
const MAX_READER_SLOTS: u16 = 128;
const RESERVE_SPIN_LIMIT: usize = 1024;

/// A builder for creating or attaching to a shared memory ring buffer region.
pub struct RegionBuilder;

/// A shared memory region allocated for ring buffer I/O.
#[derive(Clone)]
pub struct ChannelRegion {
    shared_id: u64,
    mapping: selium_guest::SharedMemory,
    capacity: u64,
    size: u64,
}

impl RegionBuilder {
    /// Creates a new shared memory region for a ring buffer of the given capacity.
    pub fn create(capacity: u32) -> Result<ChannelRegion> {
        let total = REGION_HEADER_BYTES + capacity as u64;
        let total_aligned = total.next_power_of_two().max(MIN_REGION_BYTES);
        let region = selium_guest::SharedRegion::allocate(total_aligned as u32, 8)
            .map_err(|e| Error::Guest(e.to_string()))?;
        let mapping =
            selium_guest::SharedMemory::attach(region.descriptor(), 0, total_aligned as u32)
                .map_err(|e| Error::Guest(e.to_string()))?;
        Ok(ChannelRegion {
            shared_id: region.shared_id(),
            mapping,
            capacity: capacity as u64,
            size: total_aligned,
        })
    }

    /// Attaches to an existing shared memory region by its shared id.
    pub fn attach(shared_id: u64, capacity: u64) -> Result<ChannelRegion> {
        let total = REGION_HEADER_BYTES + capacity;
        let total_aligned = total.next_power_of_two().max(MIN_REGION_BYTES);
        let mapping = selium_guest::SharedMemory::attach_shared(shared_id, 0, total_aligned as u32)
            .map_err(|e| Error::Guest(e.to_string()))?;
        Ok(ChannelRegion {
            shared_id,
            mapping,
            capacity,
            size: total_aligned,
        })
    }
}

impl ChannelRegion {
    /// Returns the shared region id.
    pub fn shared_id(&self) -> u64 {
        self.shared_id
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
        REGION_HEADER_BYTES
    }

    /// Reads a u64 from the region header at the given offset.
    pub fn read_header_u64(&self, offset: u64) -> Result<u64> {
        let bytes = self
            .mapping
            .read(offset as u32, 8)
            .map_err(|e| Error::Guest(e.to_string()))?;
        Ok(u64::from_le_bytes(
            bytes.try_into().map_err(|_| Error::InvalidLayout)?,
        ))
    }

    /// Writes a u64 to the region header at the given offset.
    pub fn write_header_u64(&self, offset: u64, value: u64) -> Result<()> {
        self.mapping
            .write(offset as u32, value.to_le_bytes().to_vec())
            .map_err(|e| Error::Guest(e.to_string()))
    }

    /// Atomically increments a header u64 and returns the previous value.
    pub fn fetch_add_header_u64(&self, offset: u64, add: u64) -> Result<u64> {
        self.mapping
            .fetch_add_u64(offset as u32, add)
            .map_err(|e| Error::Guest(e.to_string()))
    }

    /// Atomically compares and exchanges a header u64, returning the previous value.
    pub fn compare_exchange_header_u64(&self, offset: u64, current: u64, new: u64) -> Result<u64> {
        self.mapping
            .compare_exchange_u64(offset as u32, current, new)
            .map_err(|e| Error::Guest(e.to_string()))
    }

    /// Reads bytes from the ring data area.
    pub fn read_data(&self, offset: u64, len: u32) -> Result<Vec<u8>> {
        let data_offset = REGION_HEADER_BYTES + offset;
        self.mapping
            .read(data_offset as u32, len)
            .map_err(|e| Error::Guest(e.to_string()))
    }

    /// Writes bytes to the ring data area.
    pub fn write_data(&self, offset: u64, bytes: &[u8]) -> Result<()> {
        let data_offset = REGION_HEADER_BYTES + offset;
        self.mapping
            .write(data_offset as u32, bytes.to_vec())
            .map_err(|e| Error::Guest(e.to_string()))
    }

    /// Returns a reference to the underlying shared memory mapping.
    pub fn data_slice(&self) -> &selium_guest::SharedMemory {
        &self.mapping
    }

    /// Reads the region magic identifier.
    pub fn read_magic(&self) -> Result<u64> {
        self.read_header_u64(MAGIC_OFFSET)
    }

    /// Writes the region magic identifier.
    pub fn write_magic(&self, magic: u64) -> Result<()> {
        self.write_header_u64(MAGIC_OFFSET, magic)
    }

    /// Reads the capacity stored in the shared region header.
    pub fn read_capacity(&self) -> Result<u64> {
        self.read_header_u64(CAPACITY_OFFSET)
    }

    /// Writes the capacity stored in the shared region header.
    pub fn write_capacity(&self, capacity: u64) -> Result<()> {
        self.write_header_u64(CAPACITY_OFFSET, capacity)
    }

    /// Reads the next_tail cursor.
    pub fn read_next_tail(&self) -> Result<u64> {
        self.read_header_u64(NEXT_TAIL_OFFSET)
    }

    /// Writes the next_tail cursor.
    pub fn write_next_tail(&self, value: u64) -> Result<()> {
        self.write_header_u64(NEXT_TAIL_OFFSET, value)
    }

    /// Atomically reserves `len` bytes at the tail, returning the reservation position.
    pub fn reserve_tail(&self, len: u64, protect_readers: bool) -> Result<u64> {
        if len == 0 || len > self.capacity {
            return Err(Error::CapacityExceeded);
        }

        for _ in 0..RESERVE_SPIN_LIMIT {
            let tail = self.read_next_tail()?;
            let Some(next) = tail.checked_add(len).filter(|next| *next < u64::MAX) else {
                return Err(Error::CapacityExceeded);
            };
            if protect_readers {
                let head = self.minimum_reader_position()?.unwrap_or(tail);
                if next.saturating_sub(head) > self.capacity {
                    return Err(Error::BufferFull);
                }
            }

            if self.compare_exchange_header_u64(NEXT_TAIL_OFFSET, tail, next)? == tail {
                return Ok(tail);
            }
        }

        Err(Error::BufferFull)
    }

    /// Reads the tail_cache (minimum writer position).
    pub fn read_tail_cache(&self) -> Result<u64> {
        self.read_header_u64(TAIL_CACHE_OFFSET)
    }

    /// Reads the writer count.
    pub fn read_writer_count(&self) -> Result<u64> {
        self.read_header_u64(WRITER_COUNT_OFFSET)
    }

    /// Increments the writer count and returns the previous value.
    pub fn increment_writer_count(&self) -> Result<u64> {
        self.fetch_add_header_u64(WRITER_COUNT_OFFSET, 1)
    }

    /// Decrements the writer count.
    pub fn decrement_writer_count(&self) -> Result<()> {
        self.fetch_add_header_u64(WRITER_COUNT_OFFSET, u64::MAX)?;
        Ok(())
    }

    /// Allocates a stable writer id.
    pub fn allocate_writer_id(&self) -> Result<u16> {
        let id = self.fetch_add_header_u64(NEXT_WRITER_ID_OFFSET, 1)?;
        Ok(id as u16)
    }

    /// Allocates a globally unique mutation id for stream-level acknowledgements.
    pub fn allocate_mutation_id(&self) -> Result<u64> {
        Ok(self.fetch_add_header_u64(NEXT_MUTATION_ID_OFFSET, 1)? + 1)
    }

    /// Reads the strong reader count.
    pub fn read_reader_count(&self) -> Result<u64> {
        let mut count = 0;
        for slot in 0..MAX_READER_SLOTS {
            if self.read_header_u64(reader_slot_offset(slot, READER_ACTIVE_OFFSET))? != 0 {
                count += 1;
            }
        }
        Ok(count)
    }

    /// Allocates a reader cursor slot and initialises it to `position`.
    pub fn allocate_reader_slot(&self, position: u64) -> Result<u16> {
        let encoded_position = encode_reader_position(position)?;
        for slot in 0..MAX_READER_SLOTS {
            let active_offset = reader_slot_offset(slot, READER_ACTIVE_OFFSET);
            if self.compare_exchange_header_u64(active_offset, 0, encoded_position)? == 0 {
                return Ok(slot);
            }
        }

        Err(Error::CapacityExceeded)
    }

    /// Updates an allocated reader cursor slot.
    pub fn update_reader_slot(&self, slot: u16, position: u64) -> Result<()> {
        if slot >= MAX_READER_SLOTS {
            return Err(Error::InvalidLayout);
        }
        self.write_header_u64(
            reader_slot_offset(slot, READER_ACTIVE_OFFSET),
            encode_reader_position(position)?,
        )
    }

    /// Releases an allocated reader cursor slot.
    pub fn release_reader_slot(&self, slot: u16) -> Result<()> {
        if slot >= MAX_READER_SLOTS {
            return Err(Error::InvalidLayout);
        }
        let active_offset = reader_slot_offset(slot, READER_ACTIVE_OFFSET);
        let current = self.read_header_u64(active_offset)?;
        if current != 0 {
            self.compare_exchange_header_u64(active_offset, current, 0)?;
        }
        Ok(())
    }

    /// Returns the minimum active strong-reader cursor.
    pub fn minimum_reader_position(&self) -> Result<Option<u64>> {
        let mut minimum = None;
        for slot in 0..MAX_READER_SLOTS {
            let encoded_position =
                self.read_header_u64(reader_slot_offset(slot, READER_ACTIVE_OFFSET))?;
            if encoded_position == 0 {
                continue;
            }
            let position = encoded_position - 1;
            minimum = Some(minimum.map_or(position, |current: u64| current.min(position)));
        }
        Ok(minimum)
    }
}

fn encode_reader_position(position: u64) -> Result<u64> {
    position.checked_add(1).ok_or(Error::CapacityExceeded)
}

fn reader_slot_offset(slot: u16, field_offset: u64) -> u64 {
    READER_SLOTS_OFFSET + u64::from(slot) * READER_SLOT_BYTES + field_offset
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
}
