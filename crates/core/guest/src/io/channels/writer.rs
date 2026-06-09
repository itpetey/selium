use std::{
    pin::Pin,
    sync::atomic::{Ordering, fence},
    task::{Context, Poll},
};

use tokio::io::AsyncWrite;

use crate::io::{
    channels::{Error, Result},
    frame::FrameHeader,
    region::ChannelRegion,
};

/// Strong byte-stream writer tracked in the channel metadata, preventing
/// buffer overwrite until the slowest strong reader has consumed the data.
///
/// Implements [`AsyncWrite`] for byte-stream consumption. Each `poll_write`
/// call creates one frame with `tag = 0` and `protect_readers = true`.
pub struct Writer {
    region: ChannelRegion,
    writer_id: u32,
}

/// Weak byte-stream writer not tracked in channel metadata; may overwrite
/// slow readers. Implements [`AsyncWrite`].
pub struct WeakWriter {
    region: ChannelRegion,
    writer_id: u32,
}

impl Writer {
    /// Creates a new strong writer. Increments writer_count and allocates a writer_id.
    pub(crate) fn new(region: ChannelRegion) -> Result<Self> {
        region.increment_writer_count()?;
        let writer_id = match region.allocate_writer_id() {
            Ok(id) => id,
            Err(error) => {
                let _ = region.decrement_writer_count();
                return Err(error);
            }
        };
        Ok(Self { region, writer_id })
    }

    /// Creates a strong writer from a pre-allocated writer_id.
    /// Caller is responsible for having already incremented writer_count.
    pub(crate) fn from_writer_id(region: ChannelRegion, writer_id: u32) -> Self {
        Self { region, writer_id }
    }

    /// Writes a framed payload using single-phase write with release fencing.
    ///
    /// This is the frame-level write operation. Each call writes one complete
    /// frame with the given tag.
    pub fn write_frame(&mut self, payload: &[u8], tag: u32) -> Result<()> {
        if payload.len() > u32::MAX as usize {
            return Err(Error::InvalidFrame);
        }
        let frame_size = FrameHeader::ENCODED_SIZE as u64 + payload.len() as u64;
        if frame_size > self.region.capacity() {
            return Err(Error::BufferFull);
        }
        let pos = self.region.reserve_tail(frame_size, true)?;
        write_frame_single_phase(&self.region, pos, payload, tag)?;
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

    /// Downgrade this strong writer to a weak writer.
    /// Decrements writer_count to compensate for the lost Drop decrement.
    pub fn downgrade(self) -> WeakWriter {
        let _ = self.region.decrement_writer_count();
        let weak = WeakWriter {
            region: self.region.clone(),
            writer_id: self.writer_id,
        };
        // Prevent Drop from decrementing again (we already did it).
        // We can't set writer_count back, so we use mem::forget on self
        // after extracting what we need. But since Drop only decrements
        // writer_count and we already did that, we need to prevent the
        // double decrement. Use a manual approach:
        std::mem::forget(self);
        weak
    }
}

impl AsyncWrite for Writer {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        match self.write_frame(buf, 0) {
            Ok(()) => Poll::Ready(Ok(buf.len())),
            Err(Error::BufferFull) => Poll::Pending,
            Err(e) => Poll::Ready(Err(std::io::Error::other(e))),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        let _ = self.region.decrement_writer_count();
    }
}

impl WeakWriter {
    pub(crate) fn new(region: ChannelRegion, writer_id: u32) -> Self {
        Self { region, writer_id }
    }

    /// Writes payload data without applying strong-reader backpressure.
    pub fn write_frame(&mut self, payload: &[u8], tag: u32) -> Result<()> {
        if payload.len() > u32::MAX as usize {
            return Err(Error::InvalidFrame);
        }
        let frame_size = FrameHeader::ENCODED_SIZE as u64 + payload.len() as u64;
        if frame_size > self.region.capacity() {
            return Err(Error::BufferFull);
        }
        let pos = self.region.reserve_tail(frame_size, false)?;
        write_frame_single_phase(&self.region, pos, payload, tag)?;
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

    /// Returns a reference to the underlying channel region.
    pub fn region(&self) -> &ChannelRegion {
        &self.region
    }

    /// Upgrade this weak writer to a strong writer. Increments writer_count.
    pub fn upgrade(self) -> Result<Writer> {
        self.region.increment_writer_count()?;
        Ok(Writer::from_writer_id(self.region, self.writer_id))
    }
}

impl AsyncWrite for WeakWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        match self.write_frame(buf, 0) {
            Ok(()) => Poll::Ready(Ok(buf.len())),
            Err(Error::BufferFull) => Poll::Pending,
            Err(e) => Poll::Ready(Err(std::io::Error::other(e))),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
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
