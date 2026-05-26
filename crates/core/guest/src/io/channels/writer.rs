use crate::{
    Signal,
    io::{
        self,
        channels::{Error, Result},
        frame::FrameHeader,
        region::ChannelRegion,
    },
};

/// Writer that is tracked in the channel's shared metadata, preventing
/// buffer overwrite until the slowest strong reader has consumed the data.
pub struct StrongWriter {
    region: ChannelRegion,
    writer_id: u32,
    signal: Option<Signal>,
}

/// Writer that is not tracked; may overwrite slow readers.
pub struct WeakWriter {
    region: ChannelRegion,
    writer_id: u32,
    signal: Option<Signal>,
}

/// A channel writer. Supports both strong and weak variants.
pub enum Writer {
    /// Strong writer tracked in channel metadata.
    Strong(StrongWriter),
    /// Weak writer not tracked in channel metadata.
    Weak(WeakWriter),
}

impl StrongWriter {
    pub(crate) fn new(region: ChannelRegion, writer_id: u32, signal: Option<Signal>) -> Self {
        Self {
            region,
            writer_id,
            signal,
        }
    }

    /// Writes a framed payload to the channel.
    pub fn write(&mut self, payload: &[u8]) -> Result<()> {
        if payload.len() > u32::MAX as usize {
            return Err(Error::InvalidFrame);
        }
        let frame_size = FrameHeader::ENCODED_SIZE as u64 + payload.len() as u64;
        if frame_size > self.region.capacity() {
            return Err(Error::ChannelFull);
        }
        let pos = self
            .region
            .reserve_tail(frame_size, true)
            .map_err(map_core_error)?;

        let header = FrameHeader {
            len: payload.len() as u32,
            tag: self.writer_id,
            flags: 0,
            _reserved: [0; 3],
        };
        let mask = self.region.capacity() - 1;
        if let Err(error) = write_reserved_frame(&self.region, pos, payload, header, mask) {
            write_aborted_frame(&self.region, pos, payload.len(), self.writer_id, mask)?;
            return Err(error);
        }
        if let Some(signal) = &self.signal {
            signal
                .notify()
                .map_err(|e| Error::Core(io::Error::Guest(e.to_string())))?;
        }

        Ok(())
    }

    /// Returns the writer id stored in emitted frames.
    pub fn writer_id(&self) -> u32 {
        self.writer_id
    }

    /// Allocates a globally unique mutation id for this writer's channel.
    pub fn allocate_mutation_id(&self) -> Result<u64> {
        self.region.allocate_mutation_id().map_err(Error::Core)
    }
}

impl Drop for StrongWriter {
    fn drop(&mut self) {
        if let Err(_error) = self.region.decrement_writer_count() {}
    }
}

impl WeakWriter {
    pub(crate) fn new(region: ChannelRegion, writer_id: u32, signal: Option<Signal>) -> Self {
        Self {
            region,
            writer_id,
            signal,
        }
    }

    /// Writes payload data without applying strong-reader backpressure.
    pub fn write(&mut self, payload: &[u8]) -> Result<()> {
        if payload.len() > u32::MAX as usize {
            return Err(Error::InvalidFrame);
        }
        let frame_size = FrameHeader::ENCODED_SIZE as u64 + payload.len() as u64;
        if frame_size > self.region.capacity() {
            return Err(Error::ChannelFull);
        }
        let pos = self
            .region
            .reserve_tail(frame_size, false)
            .map_err(map_core_error)?;

        let header = FrameHeader {
            len: payload.len() as u32,
            tag: self.writer_id,
            flags: 0,
            _reserved: [0; 3],
        };
        let mask = self.region.capacity() - 1;
        if let Err(error) = write_reserved_frame(&self.region, pos, payload, header, mask) {
            write_aborted_frame(&self.region, pos, payload.len(), self.writer_id, mask)?;
            return Err(error);
        }
        if let Some(signal) = &self.signal {
            signal
                .notify()
                .map_err(|e| Error::Core(io::Error::Guest(e.to_string())))?;
        }
        Ok(())
    }

    /// Returns the writer id stored in emitted frames.
    pub fn writer_id(&self) -> u32 {
        self.writer_id
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
}

fn map_core_error(error: io::Error) -> Error {
    match error {
        io::Error::BufferFull | io::Error::CapacityExceeded => Error::ChannelFull,
        io::Error::ReservationContended => Error::ReservationContended,
        other => Error::Core(other),
    }
}

fn write_aborted_frame(
    region: &ChannelRegion,
    pos: u64,
    payload_len: usize,
    tag: u32,
    mask: u64,
) -> Result<()> {
    let header = FrameHeader {
        len: payload_len as u32,
        tag,
        flags: FrameHeader::FLAG_READY | FrameHeader::FLAG_ABORTED,
        _reserved: [0; 3],
    };
    write_raw(region, pos, &header.encode(), mask)
}

fn write_raw(region: &ChannelRegion, pos: u64, data: &[u8], mask: u64) -> Result<()> {
    if data.len() as u64 > region.capacity() {
        return Err(Error::ChannelFull);
    }
    let raw_start = (pos & mask) as usize;
    let tail = data.len().min(region.capacity() as usize - raw_start);
    let head = data.len() - tail;

    if tail > 0 {
        let offset = region.data_offset() + raw_start as u64;
        region
            .data_slice()
            .write(offset as u32, data.get(..tail).unwrap_or_default().to_vec())
            .map_err(|e| Error::Core(io::Error::Guest(e.to_string())))?;
    }
    if head > 0 {
        let offset = region.data_offset();
        region
            .data_slice()
            .write(offset as u32, data.get(tail..).unwrap_or_default().to_vec())
            .map_err(|e| Error::Core(io::Error::Guest(e.to_string())))?;
    }
    Ok(())
}

fn write_reserved_frame(
    region: &ChannelRegion,
    pos: u64,
    payload: &[u8],
    header: FrameHeader,
    mask: u64,
) -> Result<()> {
    write_raw(region, pos, &header.encode(), mask)?;
    let payload_pos = pos
        .checked_add(FrameHeader::ENCODED_SIZE as u64)
        .ok_or(Error::InvalidFrame)?;
    write_raw(region, payload_pos, payload, mask)?;
    let ready_header = FrameHeader {
        flags: header.flags | FrameHeader::FLAG_READY,
        ..header
    };
    write_raw(region, pos, &ready_header.encode(), mask)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn core_reservation_contention_maps_to_channel_contention() {
        assert_eq!(
            map_core_error(io::Error::ReservationContended),
            Error::ReservationContended
        );
    }

    #[test]
    fn core_capacity_errors_map_to_channel_full() {
        assert_eq!(map_core_error(io::Error::BufferFull), Error::ChannelFull);
        assert_eq!(
            map_core_error(io::Error::CapacityExceeded),
            Error::ChannelFull
        );
    }
}
