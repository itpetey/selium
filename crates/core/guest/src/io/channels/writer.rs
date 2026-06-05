use std::sync::atomic::{Ordering, fence};

use crate::io::{
    channels::{Error, Result},
    frame::FrameHeader,
    region::ChannelRegion,
};

/// Writer that is tracked in the channel's private metadata, preventing
/// buffer overwrite until the slowest strong reader has consumed the data.
pub struct StrongWriter {
    region: ChannelRegion,
    writer_id: u32,
}

/// Writer that is not tracked; may overwrite slow readers.
pub struct WeakWriter {
    region: ChannelRegion,
    writer_id: u32,
}

/// A channel writer. Supports both strong and weak variants.
pub enum Writer {
    /// Strong writer tracked in channel metadata.
    Strong(StrongWriter),
    /// Weak writer not tracked in channel metadata.
    Weak(WeakWriter),
}

impl StrongWriter {
    pub(crate) fn new(region: ChannelRegion, writer_id: u32) -> Self {
        Self { region, writer_id }
    }

    /// Writes a framed payload using single-phase write with release fencing.
    pub fn write(&mut self, payload: &[u8]) -> Result<()> {
        if payload.len() > u32::MAX as usize {
            return Err(Error::InvalidFrame);
        }
        let frame_size = FrameHeader::ENCODED_SIZE as u64 + payload.len() as u64;
        if frame_size > self.region.capacity() {
            return Err(Error::BufferFull);
        }
        let pos = self.region.reserve_tail(frame_size, true)?;
        write_frame_single_phase(&self.region, pos, payload, self.writer_id)?;
        Ok(())
    }

    /// Returns a reference to the underlying channel region.
    pub fn region(&self) -> &ChannelRegion {
        &self.region
    }

    /// Returns the writer id stored in emitted frames.
    pub fn writer_id(&self) -> u32 {
        self.writer_id
    }

    /// Allocates a globally unique mutation id for this writer's channel.
    pub fn allocate_mutation_id(&self) -> Result<u64> {
        self.region.allocate_mutation_id()
    }
}

impl Drop for StrongWriter {
    fn drop(&mut self) {
        let _ = self.region.decrement_writer_count();
    }
}

impl WeakWriter {
    pub(crate) fn new(region: ChannelRegion, writer_id: u32) -> Self {
        Self { region, writer_id }
    }

    /// Writes payload data without applying strong-reader backpressure.
    pub fn write(&mut self, payload: &[u8]) -> Result<()> {
        if payload.len() > u32::MAX as usize {
            return Err(Error::InvalidFrame);
        }
        let frame_size = FrameHeader::ENCODED_SIZE as u64 + payload.len() as u64;
        if frame_size > self.region.capacity() {
            return Err(Error::BufferFull);
        }
        let pos = self.region.reserve_tail(frame_size, false)?;
        write_frame_single_phase(&self.region, pos, payload, self.writer_id)?;
        Ok(())
    }

    /// Returns the writer id stored in emitted frames.
    pub fn writer_id(&self) -> u32 {
        self.writer_id
    }

    /// Allocates a globally unique mutation id for this writer's channel.
    pub fn allocate_mutation_id(&self) -> Result<u64> {
        self.region.allocate_mutation_id()
    }
}

impl Writer {
    /// Writes payload data into the channel.
    pub fn write(&mut self, payload: &[u8]) -> Result<()> {
        match self {
            Self::Strong(w) => w.write(payload),
            Self::Weak(w) => w.write(payload),
        }
    }

    /// Returns the writer id.
    pub fn writer_id(&self) -> u32 {
        match self {
            Self::Strong(w) => w.writer_id,
            Self::Weak(w) => w.writer_id,
        }
    }

    /// Allocates a globally unique mutation id for this writer's channel.
    pub fn allocate_mutation_id(&self) -> Result<u64> {
        match self {
            Self::Strong(w) => w.allocate_mutation_id(),
            Self::Weak(w) => w.allocate_mutation_id(),
        }
    }
}

/// Single-phase frame write with release/acquire fencing.
///
/// 1. Write payload at `pos + ENCODED_SIZE`
/// 2. Release fence
/// 3. Write header with READY flag at `pos`
/// 4. Bump generation counter
fn write_frame_single_phase(
    region: &ChannelRegion,
    pos: u64,
    payload: &[u8],
    tag: u32,
) -> Result<()> {
    let mask = region.capacity() - 1;

    // Step 1: Write payload.
    let payload_pos = pos
        .checked_add(FrameHeader::ENCODED_SIZE as u64)
        .ok_or(Error::InvalidFrame)?;
    write_raw(region, payload_pos, payload, mask)?;

    // Step 2: Release fence ensures payload is visible before the header.
    fence(Ordering::Release);

    // Step 3: Write header with READY flag (single write).
    let header = FrameHeader {
        len: payload.len() as u32,
        tag,
        flags: FrameHeader::FLAG_READY,
        _reserved: [0; 3],
    };
    write_raw(region, pos, &header.encode(), mask)?;

    // Step 4: Bump generation counter and notify waiters.
    region.bump_generation()?;

    Ok(())
}

fn write_raw(region: &ChannelRegion, pos: u64, data: &[u8], mask: u64) -> Result<()> {
    if data.len() as u64 > region.capacity() {
        return Err(Error::BufferFull);
    }
    let raw_start = (pos & mask) as usize;
    let tail = data.len().min(region.capacity() as usize - raw_start);
    let head = data.len() - tail;

    if tail > 0 {
        let offset = region.data_offset() + raw_start as u64;
        region
            .mapping()
            .write(offset, data.get(..tail).unwrap_or_default())?;
    }
    if head > 0 {
        let offset = region.data_offset();
        region
            .mapping()
            .write(offset, data.get(tail..).unwrap_or_default())?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::RegionBuilder;
    use crate::io::channels::reader::read_raw;

    #[test]
    fn single_phase_write_produces_ready_frame() {
        let region = RegionBuilder::create(64).expect("create");
        region.initialise().expect("init");
        let pos = region.reserve_tail(12 + 5, true).expect("reserve");
        write_frame_single_phase(&region, pos, b"hello", 1).expect("write");

        // Read back the header and verify READY flag.
        let mask = region.capacity() - 1;
        let header_bytes =
            read_raw(&region, pos, FrameHeader::ENCODED_SIZE as u64, mask).expect("read header");
        let header = FrameHeader::decode(&header_bytes).expect("decode");
        assert!(header.is_ready());
        assert_eq!(header.len, 5);
        assert_eq!(header.tag, 1);
    }
}
