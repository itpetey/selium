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

/// Non-blocking byte-stream writer not tracked in channel metadata; may overwrite
/// slow readers. Implements [`AsyncWrite`].
pub struct Writer {
    region: ChannelRegion,
    writer_id: u32,
}

/// Blocking byte-stream writer tracked in the channel metadata, preventing
/// buffer overwrite until the slowest blocking reader has consumed the data.
///
/// Implements [`AsyncWrite`] for raw byte-stream production. Each
/// `poll_write` call takes a buffer of raw frame bytes (`[header:12][payload:N]`)
/// and writes them to the ring buffer using a two-phase protocol (payload
/// first, release fence, header) to guarantee reader consistency.
///
/// Use [`FramedWrite`](crate::io::framed::FramedWrite) for frame-level
/// operations with automatic header encoding.
pub struct BlockingWriter {
    region: ChannelRegion,
    writer_id: u32,
}

impl Writer {
    pub(crate) fn new(region: ChannelRegion, writer_id: u32) -> Self {
        Self { region, writer_id }
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

    /// Upgrade this non-blocking writer to a blocking writer. Increments writer_count.
    pub fn upgrade(self) -> Result<BlockingWriter> {
        self.region.increment_writer_count()?;
        Ok(BlockingWriter::from_writer_id(self.region, self.writer_id))
    }
}

impl AsyncWrite for Writer {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let len = buf.len() as u64;
        if len > self.region.capacity() {
            return Poll::Ready(Err(std::io::Error::other(Error::BufferFull)));
        }

        let pos = match self.region.reserve_tail(len, false) {
            Ok(p) => p,
            Err(Error::BufferFull) => return Poll::Pending,
            Err(e) => return Poll::Ready(Err(std::io::Error::other(e))),
        };

        if let Err(e) = write_frame_bytes(&self.region, pos, buf) {
            return Poll::Ready(Err(std::io::Error::other(e)));
        }

        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl BlockingWriter {
    /// Creates a new blocking writer. Increments writer_count and allocates a writer_id.
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

    /// Creates a blocking writer from a pre-allocated writer_id.
    /// Caller is responsible for having already incremented writer_count.
    pub(crate) fn from_writer_id(region: ChannelRegion, writer_id: u32) -> Self {
        Self { region, writer_id }
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

    /// Downgrade this blocking writer to a non-blocking writer.
    /// Decrements writer_count to compensate for the lost Drop decrement.
    pub fn downgrade(self) -> Writer {
        let _ = self.region.decrement_writer_count();
        let writer = Writer {
            region: self.region.clone(),
            writer_id: self.writer_id,
        };
        // Prevent Drop from decrementing again (we already did it).
        std::mem::forget(self);
        writer
    }
}

impl AsyncWrite for BlockingWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let len = buf.len() as u64;
        if len > self.region.capacity() {
            return Poll::Ready(Err(std::io::Error::other(Error::BufferFull)));
        }

        let pos = match self.region.reserve_tail(len, true) {
            Ok(p) => p,
            Err(Error::BufferFull) => return Poll::Pending,
            Err(e) => return Poll::Ready(Err(std::io::Error::other(e))),
        };

        if let Err(e) = write_frame_bytes(&self.region, pos, buf) {
            return Poll::Ready(Err(std::io::Error::other(e)));
        }

        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl Drop for BlockingWriter {
    fn drop(&mut self) {
        let _ = self.region.decrement_writer_count();
    }
}

/// Two-phase write of an already-encoded frame (header + payload) to the ring buffer.
///
/// Writes payload first (bytes after the 12-byte header), then a release
/// fence, then the header. This ensures readers never observe a READY
/// header before the completed payload. Finally bumps the generation
/// counter to notify waiters.
fn write_frame_bytes(region: &ChannelRegion, pos: u64, buf: &[u8]) -> Result<()> {
    let mask = region.capacity() - 1;

    let header_size = FrameHeader::ENCODED_SIZE as u64;
    let payload = buf.get(FrameHeader::ENCODED_SIZE..).unwrap_or_default();

    // Step 1: Write payload at pos + ENCODED_SIZE.
    let payload_pos = pos.checked_add(header_size).ok_or(Error::InvalidFrame)?;
    write_raw(region, payload_pos, payload, mask)?;

    // Step 2: Release fence ensures payload is visible before the header.
    fence(Ordering::Release);

    // Step 3: Write header at pos (already has FLAG_READY from the codec).
    write_raw(
        region,
        pos,
        buf.get(..FrameHeader::ENCODED_SIZE).unwrap_or_default(),
        mask,
    )?;

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
    fn two_phase_write_produces_ready_frame() {
        let region = RegionBuilder::create(64).expect("create");
        region.initialise().expect("init");

        // Encode a frame manually: header + payload.
        let header = FrameHeader {
            len: 5,
            tag: 1,
            flags: FrameHeader::FLAG_READY,
            _reserved: [0; 3],
        };
        let mut buf = Vec::new();
        buf.extend_from_slice(&header.encode());
        buf.extend_from_slice(b"hello");

        let pos = region
            .reserve_tail(buf.len() as u64, true)
            .expect("reserve");
        write_frame_bytes(&region, pos, &buf).expect("write");

        // Read back the header and verify READY flag.
        let mask = region.capacity() - 1;
        let header_bytes =
            read_raw(&region, pos, FrameHeader::ENCODED_SIZE as u64, mask).expect("read header");
        let decoded = FrameHeader::decode(&header_bytes).expect("decode");
        assert!(decoded.is_ready());
        assert_eq!(decoded.len, 5);
        assert_eq!(decoded.tag, 1);

        // Read back the payload.
        let payload_pos = pos + FrameHeader::ENCODED_SIZE as u64;
        let payload = read_raw(&region, payload_pos, 5, mask).expect("read payload");
        assert_eq!(payload, b"hello");
    }
}
