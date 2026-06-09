use std::{
    pin::Pin,
    sync::atomic::{Ordering, fence},
    task::{Context, Poll},
};

use tokio::io::{AsyncRead, ReadBuf};

use crate::io::{
    channels::{Error, Result},
    frame::FrameHeader,
    region::ChannelRegion,
};

/// Strong byte-stream reader that tracks its position via a reader slot,
/// preventing the ring buffer from overwriting unread data.
///
/// Implements [`AsyncRead`] for byte-stream consumption. Also exposes
/// frame-level operations (`read_frame`, `poll_ready`) for use with
/// [`FramedRead`](crate::io::framed::FramedRead).
pub struct Reader {
    region: ChannelRegion,
    pos: u64,
    reader_id: u32,
    terminated: bool,
    last_generation: u64,
    /// Buffered payload bytes from a frame partially copied to the caller.
    pending_payload: Vec<u8>,
    /// Offset into `pending_payload` for the next copy.
    pending_offset: usize,
}

/// Weak byte-stream reader that does not prevent buffer overwrite.
///
/// If writers overtake this reader, it reports [`Error::Overwritten`] and
/// resumes at the live tail. Implements [`AsyncRead`].
pub struct WeakReader {
    region: ChannelRegion,
    pos: u64,
    terminated: bool,
    last_generation: u64,
    /// Buffered payload bytes from a frame partially copied to the caller.
    pending_payload: Vec<u8>,
    /// Offset into `pending_payload` for the next copy.
    pending_offset: usize,
}

impl Reader {
    pub(crate) fn new(region: ChannelRegion, start_pos: u64, reader_id: u32) -> Self {
        Self {
            region,
            pos: start_pos,
            reader_id,
            terminated: false,
            last_generation: 0,
            pending_payload: Vec::new(),
            pending_offset: 0,
        }
    }

    /// Reads the next complete frame. Returns `(payload, tag)`.
    ///
    /// This is the frame-level read operation, used by
    /// [`FramedRead`](crate::io::framed::FramedRead) and other frame-aware types.
    pub fn read_frame(&mut self) -> Result<(Vec<u8>, u32)> {
        if self.terminated {
            return Err(Error::Terminated);
        }

        let capacity = self.region.capacity();
        let mask = capacity - 1;
        let tail = self.region.read_next_tail()?;

        if self.pos >= tail {
            return Err(Error::BufferEmpty);
        }
        if self.pos.wrapping_add(capacity) < tail {
            return Err(Error::Overwritten);
        }

        // Acquire fence ensures we see the writer's payload before the header.
        fence(Ordering::Acquire);

        let header = read_header(&self.region, self.pos, mask)?;
        let frame_size = header.frame_size();

        if !header.is_ready() {
            return Err(Error::BufferEmpty);
        }

        let frame_end = self
            .pos
            .checked_add(frame_size)
            .ok_or(Error::InvalidFrame)?;
        if frame_size > capacity || frame_end > tail {
            return Err(Error::InvalidFrame);
        }

        let payload_pos = self
            .pos
            .checked_add(FrameHeader::ENCODED_SIZE as u64)
            .ok_or(Error::InvalidFrame)?;
        let payload = read_raw(&self.region, payload_pos, header.len as u64, mask)?;

        self.advance(frame_size)?;
        self.last_generation = self.region.load_generation().unwrap_or(0);
        Ok((payload, header.tag))
    }

    /// Non-blocking check for frame readiness.
    ///
    /// Returns `Ok(true)` if a complete frame with the READY flag set is
    /// immediately readable at the current cursor position, `Ok(false)` if
    /// no frame is ready.
    pub fn poll_ready(&mut self) -> Result<bool> {
        if self.terminated {
            return Err(Error::Terminated);
        }

        let capacity = self.region.capacity();
        let mask = capacity - 1;
        let tail = self.region.read_next_tail()?;

        if self.pos >= tail {
            return Ok(false);
        }
        if self.pos.wrapping_add(capacity) < tail {
            return Err(Error::Overwritten);
        }

        fence(Ordering::Acquire);

        let header = read_header(&self.region, self.pos, mask)?;
        let frame_size = header.frame_size();
        let frame_end = self
            .pos
            .checked_add(frame_size)
            .ok_or(Error::InvalidFrame)?;
        if frame_size > capacity || frame_end > tail {
            return Err(Error::InvalidFrame);
        }
        if !header.is_ready() {
            return Ok(false);
        }
        Ok(true)
    }

    /// Returns the current generation counter from the underlying ring buffer.
    pub fn generation(&self) -> Result<u64> {
        self.region.load_generation()
    }

    fn advance(&mut self, frame_size: u64) -> Result<()> {
        self.pos = self
            .pos
            .checked_add(frame_size)
            .ok_or(Error::InvalidFrame)?;
        self.region.update_reader_slot(self.reader_id, self.pos)
    }

    /// Returns the current read position.
    pub fn position(&self) -> u64 {
        self.pos
    }

    /// Returns a reference to the underlying channel region.
    pub fn region(&self) -> &ChannelRegion {
        &self.region
    }

    /// Downgrade this strong reader to a weak reader, releasing the reader slot.
    pub fn downgrade(mut self) -> WeakReader {
        let _ = self.region.release_reader_slot(self.reader_id);
        let weak = WeakReader {
            region: self.region.clone(),
            pos: self.pos,
            terminated: self.terminated,
            last_generation: self.last_generation,
            pending_payload: std::mem::take(&mut self.pending_payload),
            pending_offset: self.pending_offset,
        };
        self.terminated = true; // prevent Drop from releasing again
        weak
    }

    /// Close this reader and release its reader slot.
    pub fn close(&mut self) {
        if !self.terminated {
            let _ = self.region.release_reader_slot(self.reader_id);
            self.terminated = true;
        }
    }
}

impl AsyncRead for Reader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        // Drain any buffered payload from a previous partial read.
        if self.pending_offset < self.pending_payload.len() {
            let remaining = &self.pending_payload[self.pending_offset..];
            let to_copy = remaining.len().min(buf.remaining());
            buf.put_slice(&remaining[..to_copy]);
            self.pending_offset += to_copy;
            if self.pending_offset >= self.pending_payload.len() {
                self.pending_payload.clear();
                self.pending_offset = 0;
            }
            return Poll::Ready(Ok(()));
        }

        if self.terminated {
            return Poll::Ready(Ok(())); // EOF
        }

        let capacity = self.region.capacity();
        let mask = capacity - 1;

        let tail = match self.region.read_next_tail() {
            Ok(t) => t,
            Err(e) => return Poll::Ready(Err(std::io::Error::other(e))),
        };

        if self.pos >= tail {
            // No data. Check if writers are still connected.
            match self.region.load_writer_count() {
                Ok(0) => return Poll::Ready(Ok(())), // EOF
                Ok(_) => return Poll::Pending,
                Err(e) => return Poll::Ready(Err(std::io::Error::other(e))),
            }
        }

        // Check for overwrite using generation counter delta.
        let current_gen = match self.region.load_generation() {
            Ok(g) => g,
            Err(e) => return Poll::Ready(Err(std::io::Error::other(e))),
        };
        let delta = current_gen.wrapping_sub(self.last_generation);
        if delta > capacity {
            return Poll::Ready(Err(std::io::Error::other(Error::Overwritten)));
        }

        fence(Ordering::Acquire);

        let header = match read_header(&self.region, self.pos, mask) {
            Ok(h) => h,
            Err(e) => return Poll::Ready(Err(std::io::Error::other(e))),
        };

        if !header.is_ready() {
            return Poll::Pending;
        }

        let frame_size = header.frame_size();
        if frame_size > capacity {
            return Poll::Ready(Err(std::io::Error::other(Error::InvalidFrame)));
        }

        let payload_pos = match self.pos.checked_add(FrameHeader::ENCODED_SIZE as u64) {
            Some(p) => p,
            None => return Poll::Ready(Err(std::io::Error::other(Error::InvalidFrame))),
        };

        let payload = match read_raw(&self.region, payload_pos, header.len as u64, mask) {
            Ok(p) => p,
            Err(e) => return Poll::Ready(Err(std::io::Error::other(e))),
        };

        if let Err(e) = self.advance(frame_size) {
            return Poll::Ready(Err(std::io::Error::other(e)));
        }
        self.last_generation = self.region.load_generation().unwrap_or(0);

        let to_copy = payload.len().min(buf.remaining());
        buf.put_slice(&payload[..to_copy]);

        if to_copy < payload.len() {
            self.pending_payload = payload;
            self.pending_offset = to_copy;
        }

        Poll::Ready(Ok(()))
    }
}

impl Drop for Reader {
    fn drop(&mut self) {
        self.close();
    }
}

impl WeakReader {
    pub(crate) fn new(region: ChannelRegion, start_pos: u64) -> Self {
        Self {
            region,
            pos: start_pos,
            terminated: false,
            last_generation: 0,
            pending_payload: Vec::new(),
            pending_offset: 0,
        }
    }

    /// Reads the next complete frame. Returns `(payload, tag)`.
    pub fn read_frame(&mut self) -> Result<(Vec<u8>, u32)> {
        if self.terminated {
            return Err(Error::Terminated);
        }
        let capacity = self.region.capacity();
        let mask = capacity - 1;
        let tail = self.region.read_next_tail()?;

        if self.pos >= tail {
            return Err(Error::BufferEmpty);
        }
        if self.pos.wrapping_add(capacity) < tail {
            self.pos = tail;
            return Err(Error::Overwritten);
        }

        fence(Ordering::Acquire);

        let header = read_header(&self.region, self.pos, mask)?;
        let frame_size = header.frame_size();

        if !header.is_ready() {
            return Err(Error::BufferEmpty);
        }

        let frame_end = self
            .pos
            .checked_add(frame_size)
            .ok_or(Error::InvalidFrame)?;
        if frame_size > capacity || frame_end > tail {
            return Err(Error::InvalidFrame);
        }

        let payload_pos = self
            .pos
            .checked_add(FrameHeader::ENCODED_SIZE as u64)
            .ok_or(Error::InvalidFrame)?;
        let payload = read_raw(&self.region, payload_pos, header.len as u64, mask)?;

        self.pos = self
            .pos
            .checked_add(frame_size)
            .ok_or(Error::InvalidFrame)?;
        self.last_generation = self.region.load_generation().unwrap_or(0);
        Ok((payload, header.tag))
    }

    /// Non-blocking check for frame readiness.
    pub fn poll_ready(&mut self) -> Result<bool> {
        if self.terminated {
            return Err(Error::Terminated);
        }

        let capacity = self.region.capacity();
        let mask = capacity - 1;
        let tail = self.region.read_next_tail()?;

        if self.pos >= tail {
            return Ok(false);
        }
        if self.pos.wrapping_add(capacity) < tail {
            return Err(Error::Overwritten);
        }

        fence(Ordering::Acquire);

        let header = read_header(&self.region, self.pos, mask)?;
        let frame_size = header.frame_size();
        let frame_end = self
            .pos
            .checked_add(frame_size)
            .ok_or(Error::InvalidFrame)?;
        if frame_size > capacity || frame_end > tail {
            return Err(Error::InvalidFrame);
        }
        if !header.is_ready() {
            return Ok(false);
        }
        Ok(true)
    }

    /// Returns the current generation counter from the underlying ring buffer.
    pub fn generation(&self) -> Result<u64> {
        self.region.load_generation()
    }

    /// Returns the current read position.
    pub fn position(&self) -> u64 {
        self.pos
    }

    /// Returns a reference to the underlying channel region.
    pub fn region(&self) -> &ChannelRegion {
        &self.region
    }

    /// Upgrade this weak reader to a strong reader, allocating a reader slot.
    pub fn upgrade(self) -> Result<Reader> {
        let reader_id = self.region.allocate_reader_slot(self.pos)?;
        let mut reader = Reader::new(self.region, self.pos, reader_id);
        reader.last_generation = self.last_generation;
        // Prevent WeakReader's drop from doing anything meaningful
        // (it has no drop impl, so this is automatic)
        Ok(reader)
    }
}

impl AsyncRead for WeakReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        // Drain any buffered payload from a previous partial read.
        if self.pending_offset < self.pending_payload.len() {
            let remaining = &self.pending_payload[self.pending_offset..];
            let to_copy = remaining.len().min(buf.remaining());
            buf.put_slice(&remaining[..to_copy]);
            self.pending_offset += to_copy;
            if self.pending_offset >= self.pending_payload.len() {
                self.pending_payload.clear();
                self.pending_offset = 0;
            }
            return Poll::Ready(Ok(()));
        }

        if self.terminated {
            return Poll::Ready(Ok(())); // EOF
        }

        let capacity = self.region.capacity();
        let mask = capacity - 1;

        let tail = match self.region.read_next_tail() {
            Ok(t) => t,
            Err(e) => return Poll::Ready(Err(std::io::Error::other(e))),
        };

        if self.pos >= tail {
            match self.region.load_writer_count() {
                Ok(0) => return Poll::Ready(Ok(())), // EOF
                Ok(_) => return Poll::Pending,
                Err(e) => return Poll::Ready(Err(std::io::Error::other(e))),
            }
        }

        // Weak reader: if overtaken, jump to tail and report error.
        if self.pos.wrapping_add(capacity) < tail {
            self.pos = tail;
            return Poll::Ready(Err(std::io::Error::other(Error::Overwritten)));
        }

        fence(Ordering::Acquire);

        let header = match read_header(&self.region, self.pos, mask) {
            Ok(h) => h,
            Err(e) => return Poll::Ready(Err(std::io::Error::other(e))),
        };

        if !header.is_ready() {
            return Poll::Pending;
        }

        let frame_size = header.frame_size();
        if frame_size > capacity {
            return Poll::Ready(Err(std::io::Error::other(Error::InvalidFrame)));
        }

        let payload_pos = match self.pos.checked_add(FrameHeader::ENCODED_SIZE as u64) {
            Some(p) => p,
            None => return Poll::Ready(Err(std::io::Error::other(Error::InvalidFrame))),
        };

        let payload = match read_raw(&self.region, payload_pos, header.len as u64, mask) {
            Ok(p) => p,
            Err(e) => return Poll::Ready(Err(std::io::Error::other(e))),
        };

        self.pos = match self.pos.checked_add(frame_size) {
            Some(p) => p,
            None => return Poll::Ready(Err(std::io::Error::other(Error::InvalidFrame))),
        };
        self.last_generation = self.region.load_generation().unwrap_or(0);

        let to_copy = payload.len().min(buf.remaining());
        buf.put_slice(&payload[..to_copy]);

        if to_copy < payload.len() {
            self.pending_payload = payload;
            self.pending_offset = to_copy;
        }

        Poll::Ready(Ok(()))
    }
}

pub(crate) fn read_raw(region: &ChannelRegion, pos: u64, len: u64, mask: u64) -> Result<Vec<u8>> {
    if len == 0 {
        return Ok(Vec::new());
    }
    if len > region.capacity() {
        return Err(Error::InvalidFrame);
    }

    let raw_pos = region.data_offset() + (pos & mask);
    let ring_end = region.data_offset() + region.capacity();
    let wrap = raw_pos + len > ring_end;
    if !wrap {
        return region.mapping().read(raw_pos, len);
    }

    let tail_len = ring_end.saturating_sub(raw_pos);
    let head_len = len - tail_len;
    let mut buf = Vec::with_capacity(len as usize);
    if tail_len > 0 {
        let part = region.mapping().read(raw_pos, tail_len)?;
        buf.extend_from_slice(&part);
    }
    if head_len > 0 {
        let part = region.mapping().read(region.data_offset(), head_len)?;
        buf.extend_from_slice(&part);
    }
    Ok(buf)
}

fn read_header(region: &ChannelRegion, pos: u64, mask: u64) -> Result<FrameHeader> {
    let header_bytes = read_raw(region, pos, FrameHeader::ENCODED_SIZE as u64, mask)?;
    FrameHeader::decode(&header_bytes).map_err(|_invalid_frame| Error::InvalidFrame)
}
