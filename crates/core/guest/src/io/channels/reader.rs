use crate::io::{
    channels::{Error, Result},
    frame::FrameHeader,
    region::ChannelRegion,
};

/// Reader that tracks its position, preventing the ring buffer from
/// overwriting unread data.
pub struct StrongReader {
    region: ChannelRegion,
    pos: u64,
    reader_id: u32,
    terminated: bool,
}

/// Reader that does not prevent buffer overwrite.
///
/// If writers overtake this reader, it reports [`Error::ReaderBehind`] and
/// resumes at the live tail. The ring does not store frame-boundary metadata
/// for the retained suffix, so weak readers cannot safely recover partial
/// backlog after an overrun.
pub struct WeakReader {
    region: ChannelRegion,
    pos: u64,
    terminated: bool,
}

/// A channel reader. Both strong and weak variants supported.
pub enum Reader {
    /// Strong reader that prevents buffer overwrite.
    Strong(StrongReader),
    /// Weak reader that may lose data.
    Weak(WeakReader),
}

impl StrongReader {
    pub(crate) fn new(region: ChannelRegion, start_pos: u64, reader_id: u32) -> Self {
        Self {
            region,
            pos: start_pos,
            reader_id,
            terminated: false,
        }
    }

    /// Reads the next frame. Returns `(payload, tag)`.
    pub fn read(&mut self) -> Result<(Vec<u8>, u32)> {
        if self.terminated {
            return Err(Error::Terminated);
        }

        let capacity = self.region.capacity();
        let mask = capacity - 1;

        loop {
            let tail = self
                .region
                .read_next_tail()
                .map_err(|_channel_empty| Error::ChannelEmpty)?;

            if self.pos >= tail {
                // Positions beyond the end of the buffer tail should be impossible,
                // though if they happen in production it is not enough to justify
                // process termination.
                debug_assert!(self.pos == tail);

                return Err(Error::ChannelEmpty);
            }
            if self.pos.wrapping_add(capacity) < tail {
                return Err(Error::ReaderBehind);
            }

            let header = read_header(&self.region, self.pos, mask)?;
            let frame_size = header.frame_size();
            if !header.is_ready() {
                return Err(Error::ChannelEmpty);
            }
            let frame_end = self
                .pos
                .checked_add(frame_size)
                .ok_or(Error::InvalidFrame)?;
            if frame_size > capacity || frame_end > tail {
                return Err(Error::InvalidFrame);
            }

            if header.is_aborted() {
                self.advance(frame_size)?;
                continue;
            }

            let payload_pos = self
                .pos
                .checked_add(FrameHeader::ENCODED_SIZE as u64)
                .ok_or(Error::InvalidFrame)?;
            let payload = self.read_payload(payload_pos, header.len as u64, mask)?;

            self.advance(frame_size)?;
            return Ok((payload, header.tag));
        }
    }

    /// Returns whether the current cursor points at a complete readable frame.
    pub(crate) fn has_ready_frame(&mut self) -> Result<bool> {
        if self.terminated {
            return Err(Error::Terminated);
        }

        loop {
            let capacity = self.region.capacity();
            let mask = capacity - 1;
            let tail = self
                .region
                .read_next_tail()
                .map_err(|_channel_empty| Error::ChannelEmpty)?;

            if self.pos >= tail {
                return Ok(false);
            }
            if self.pos.wrapping_add(capacity) < tail {
                return Err(Error::ReaderBehind);
            }

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
            if !header.is_aborted() {
                return Ok(true);
            }
            self.advance(frame_size)?;
        }
    }

    fn read_payload(&self, pos: u64, len: u64, mask: u64) -> Result<Vec<u8>> {
        read_raw(&self.region, pos, len, mask)
    }

    fn advance(&mut self, frame_size: u64) -> Result<()> {
        self.pos = self
            .pos
            .checked_add(frame_size)
            .ok_or(Error::InvalidFrame)?;
        self.region
            .update_reader_slot(self.reader_id, self.pos)
            .map_err(Error::Core)
    }

    /// Returns the current read position.
    pub fn position(&self) -> u64 {
        self.pos
    }

    /// Returns a reference to the underlying channel region.
    pub fn region(&self) -> &ChannelRegion {
        &self.region
    }
}

impl StrongReader {
    /// Close this reader and release its strong-reader cursor slot.
    pub fn close(&mut self) {
        if !self.terminated {
            if let Err(_error) = self.region.release_reader_slot(self.reader_id) {}
            self.terminated = true;
        }
    }
}

impl Drop for StrongReader {
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
        }
    }

    /// Reads the next frame. Returns `(payload, tag)`.
    pub fn read(&mut self) -> Result<(Vec<u8>, u32)> {
        if self.terminated {
            return Err(Error::Terminated);
        }
        loop {
            let capacity = self.region.capacity();
            let mask = capacity - 1;
            let tail = self
                .region
                .read_next_tail()
                .map_err(|_channel_empty| Error::ChannelEmpty)?;

            if self.pos >= tail {
                return Err(Error::ChannelEmpty);
            }
            if self.pos.wrapping_add(capacity) < tail {
                self.pos = tail;
                return Err(Error::ReaderBehind);
            }

            let header = read_header(&self.region, self.pos, mask)?;
            let frame_size = header.frame_size();
            if !header.is_ready() {
                return Err(Error::ChannelEmpty);
            }
            let frame_end = self
                .pos
                .checked_add(frame_size)
                .ok_or(Error::InvalidFrame)?;
            if frame_size > capacity || frame_end > tail {
                return Err(Error::InvalidFrame);
            }

            if header.is_aborted() {
                self.pos = self
                    .pos
                    .checked_add(frame_size)
                    .ok_or(Error::InvalidFrame)?;
                continue;
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
            return Ok((payload, header.tag));
        }
    }

    /// Returns the current read position.
    pub fn position(&self) -> u64 {
        self.pos
    }
}

impl Reader {
    /// Reads the next available frame from the channel.
    pub fn read(&mut self) -> Result<(Vec<u8>, u32)> {
        match self {
            Self::Strong(r) => r.read(),
            Self::Weak(r) => r.read(),
        }
    }

    /// Close this reader.
    pub fn close(&mut self) {
        match self {
            Self::Strong(r) => r.close(),
            Self::Weak(r) => r.terminated = true,
        }
    }
}

fn read_header(region: &ChannelRegion, pos: u64, mask: u64) -> Result<FrameHeader> {
    let header_bytes = read_raw(region, pos, FrameHeader::ENCODED_SIZE as u64, mask)?;
    FrameHeader::decode(&header_bytes).map_err(|_invalid_frame| Error::InvalidFrame)
}

fn read_raw(region: &ChannelRegion, pos: u64, len: u64, mask: u64) -> Result<Vec<u8>> {
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
        return region
            .data_slice()
            .read(raw_pos as u32, len as u32)
            .map_err(|_invalid_frame| Error::InvalidFrame);
    }

    let tail_len = ring_end.saturating_sub(raw_pos);
    let head_len = len - tail_len;
    let mut buf = Vec::with_capacity(len as usize);
    if tail_len > 0 {
        let part = region
            .data_slice()
            .read(raw_pos as u32, tail_len as u32)
            .map_err(|_invalid_frame| Error::InvalidFrame)?;
        buf.extend_from_slice(&part);
    }
    if head_len > 0 {
        let part = region
            .data_slice()
            .read(region.data_offset() as u32, head_len as u32)
            .map_err(|_invalid_frame| Error::InvalidFrame)?;
        buf.extend_from_slice(&part);
    }
    Ok(buf)
}
