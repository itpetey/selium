use crate::io::region::{
    NEXT_MUTATION_ID_OFFSET, NEXT_WRITER_ID_OFFSET, READER_COUNT_OFFSET, SIGNAL_SHARED_ID_OFFSET,
    WRITER_COUNT_OFFSET,
};
use crate::signal::Signal;

use crate::io::{
    ChannelRegion, Cursor, RegionBuilder,
    cursor::mask_for_capacity,
    error::{Error, Result},
    frame::FrameHeader,
    region::TAIL_CACHE_OFFSET,
};

const MAGIC_PREFIX: u64 = 0x53454C494F524E47;

/// A lock-free ring buffer log on shared memory.
///
/// This is the primitive building block for all selium-io patterns.
/// It stores framed messages sequentially in a shared memory region.
/// A `Signal` may be used to notify readers that new data is available.
pub struct RingBuf {
    region: ChannelRegion,
    mask: u64,
    capacity: u64,
    signal: Option<Signal>,
}

impl RingBuf {
    /// Creates a new ring buffer with the given data capacity, backed by a fresh shared memory region.
    pub fn create(capacity: u32) -> Result<(Self, Signal)> {
        let region = RegionBuilder::create(capacity)?;
        let mask = mask_for_capacity(capacity as u64)?;
        region.write_magic(MAGIC_PREFIX)?;
        region.write_capacity(capacity as u64)?;
        region.write_header_u64(WRITER_COUNT_OFFSET, 0)?;
        region.write_header_u64(READER_COUNT_OFFSET, 0)?;
        region.write_next_tail(0)?;
        region.write_header_u64(TAIL_CACHE_OFFSET, 0)?;
        region.write_header_u64(NEXT_WRITER_ID_OFFSET, 0)?;
        region.write_header_u64(NEXT_MUTATION_ID_OFFSET, 0)?;
        let signal = Signal::create().map_err(|e| Error::Guest(e.to_string()))?;
        region.write_header_u64(SIGNAL_SHARED_ID_OFFSET, signal.shared_id())?;
        let ring_signal =
            Signal::attach(signal.shared_id()).map_err(|e| Error::Guest(e.to_string()))?;
        Ok((
            Self {
                region,
                mask,
                capacity: capacity as u64,
                signal: Some(ring_signal),
            },
            signal,
        ))
    }

    /// Attaches to an existing ring buffer by shared region id.
    pub fn attach(shared_id: u64, capacity: u64) -> Result<Self> {
        let region = RegionBuilder::attach(shared_id, capacity)?;
        Self::wrap_region(region, None)
    }

    /// Wraps an existing channel region as a ring buffer.
    pub fn wrap_region(region: ChannelRegion, signal: Option<Signal>) -> Result<Self> {
        let capacity = region.capacity();
        let mask = mask_for_capacity(capacity)?;
        let magic = region.read_magic()?;
        if magic != MAGIC_PREFIX {
            return Err(Error::InvalidLayout);
        }
        if region.read_capacity()? != capacity {
            return Err(Error::InvalidLayout);
        }
        Ok(Self {
            region,
            mask,
            capacity,
            signal,
        })
    }

    /// Attaches with a signal for wake notification.
    pub fn attach_with_signal(shared_id: u64, capacity: u64, signal: Signal) -> Result<Self> {
        let mut buf = Self::attach(shared_id, capacity)?;
        buf.signal = Some(signal);
        Ok(buf)
    }

    /// Sets the notification signal for this ring buffer handle.
    pub(crate) fn set_signal(&mut self, signal: Signal) {
        self.signal = Some(signal);
    }

    /// Returns a reference to the underlying region.
    pub fn region(&self) -> &ChannelRegion {
        &self.region
    }

    /// Returns the ring buffer capacity in bytes.
    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    /// Returns the mask for wrapping positions.
    pub fn mask(&self) -> u64 {
        self.mask
    }

    /// Returns the shared region id.
    pub fn shared_id(&self) -> u64 {
        self.region.shared_id()
    }

    /// Returns a reference to the notification signal, if set.
    pub fn signal(&self) -> Option<&Signal> {
        self.signal.as_ref()
    }

    /// Reads the current next_tail from shared memory.
    pub fn read_next_tail(&self) -> Result<u64> {
        self.region.read_next_tail()
    }

    /// Reads the tail_cache from shared memory.
    pub fn read_tail_cache(&self) -> Result<u64> {
        self.region.read_tail_cache()
    }

    /// Atomically reserves space at the write tail. Returns the start position.
    pub fn reserve(&self, len: u64) -> Result<u64> {
        self.region.reserve_tail(len, true)
    }

    /// Writes data at a logical position, handling wraparound.
    pub fn write_at(&self, pos: u64, data: &[u8]) -> Result<()> {
        let cursor = Cursor::new(pos);
        let (tail_len, head_len) = cursor.split_wraparound(data.len() as u64, self.mask);
        let raw_pos = cursor.masked(self.mask);
        if tail_len > 0 {
            self.region
                .write_data(raw_pos, data.get(..tail_len as usize).unwrap_or_default())?;
        }
        if head_len > 0 {
            self.region
                .write_data(0, data.get(tail_len as usize..).unwrap_or_default())?;
        }
        Ok(())
    }

    /// Reads up to `len` bytes from a logical position, handling wraparound.
    pub fn read_at(&self, pos: u64, len: u64) -> Result<Vec<u8>> {
        let cursor = Cursor::new(pos);
        let (tail_len, head_len) = cursor.split_wraparound(len, self.mask);
        let raw_pos = cursor.masked(self.mask);
        let mut result = Vec::with_capacity(len as usize);
        if tail_len > 0 {
            let data = self.region.read_data(raw_pos, tail_len as u32)?;
            result.extend_from_slice(&data);
        }
        if head_len > 0 {
            let data = self.region.read_data(0, head_len as u32)?;
            result.extend_from_slice(&data);
        }
        Ok(result)
    }

    /// Writes a framed message at the given position.
    pub fn write_frame(&self, pos: u64, payload: &[u8], tag: u32, flags: u8) -> Result<()> {
        let frame_size = FrameHeader::ENCODED_SIZE as u64 + payload.len() as u64;
        if frame_size > self.capacity {
            return Err(Error::CapacityExceeded);
        }
        let pending_header = FrameHeader {
            len: payload.len() as u32,
            tag,
            flags: flags & !FrameHeader::FLAG_READY,
            _reserved: [0; 3],
        };
        let header_bytes = pending_header.encode();
        self.write_at(pos, &header_bytes)?;
        let payload_pos = pos
            .checked_add(FrameHeader::ENCODED_SIZE as u64)
            .ok_or(Error::InvalidFrame)?;
        self.write_at(payload_pos, payload)?;
        let ready_header = FrameHeader {
            flags: flags | FrameHeader::FLAG_READY,
            ..pending_header
        };
        self.write_at(pos, &ready_header.encode())?;

        if let Some(signal) = &self.signal {
            signal.notify().map_err(|e| Error::Guest(e.to_string()))?;
        }
        Ok(())
    }

    /// Reads a frame header from a position.
    pub fn read_frame_header(&self, pos: u64) -> Result<FrameHeader> {
        let bytes = self.read_at(pos, FrameHeader::ENCODED_SIZE as u64)?;
        FrameHeader::decode(&bytes)
    }

    /// Reads a full framed message from a position.
    pub fn read_frame(&self, pos: u64) -> Result<(FrameHeader, Vec<u8>)> {
        let header = self.read_frame_header(pos)?;
        let payload_pos = pos
            .checked_add(FrameHeader::ENCODED_SIZE as u64)
            .ok_or(Error::InvalidFrame)?;
        let payload = self.read_at(payload_pos, header.len as u64)?;
        Ok((header, payload))
    }
}

/// Rounds a byte capacity to the next power of two for use as a ring buffer.
pub fn round_capacity(capacity: u32) -> Result<u32> {
    capacity
        .checked_next_power_of_two()
        .map(|rounded| rounded.max(64))
        .ok_or(Error::CapacityExceeded)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capacity_rounds_to_power_of_two() {
        assert_eq!(round_capacity(64), Ok(64));
        assert_eq!(round_capacity(100), Ok(128));
        assert_eq!(round_capacity(1), Ok(64));
        assert_eq!(round_capacity(u32::MAX), Err(Error::CapacityExceeded));
    }

    #[test]
    #[expect(
        clippy::assertions_on_result_states,
        reason = "unwrap_used lint conflicts with clippy's suggested fix"
    )]
    fn mask_for_power_of_two_is_correct() {
        assert!(mask_for_capacity(512).is_ok());
        assert!(mask_for_capacity(3).is_err());
    }
}
