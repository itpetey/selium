use selium_guest::Signal;

use self::error::Result;
use crate::ring_buf::{RingBuf, round_capacity};

pub use self::{
    error::Error,
    reader::{Reader, StrongReader, WeakReader},
    writer::{StrongWriter, WeakWriter, Writer},
};

mod error;
pub mod reader;
pub mod writer;

/// A shared-memory-backed channel with strong/weak reader and writer semantics.
///
/// The channel stores framed messages in a lock-free ring buffer. Strong readers
/// prevent buffer overwrite until they have consumed data; weak readers may lose
/// data if they fall behind.
pub struct Channel {
    ring: RingBuf,
}

impl Channel {
    /// Creates a new channel with the given ring buffer data capacity.
    ///
    /// Returns the channel and its notification signal.
    pub fn create(capacity: u32) -> Result<(Self, Signal)> {
        let capacity = round_capacity(capacity);
        let (ring, signal) = RingBuf::create(capacity).map_err(Error::Core)?;
        Ok((Self { ring }, signal))
    }

    /// Attaches to an existing channel by shared region id.
    pub fn attach(shared_id: u64, capacity: u64) -> Result<Self> {
        let mut ring = RingBuf::attach(shared_id, capacity).map_err(Error::Core)?;
        let signal_shared_id = ring
            .region()
            .read_header_u64(crate::region::SIGNAL_SHARED_ID_OFFSET)
            .map_err(Error::Core)?;
        let signal = Signal::attach(signal_shared_id)
            .map_err(|e| Error::Core(crate::Error::Guest(e.to_string())))?;
        ring.set_signal(signal);
        Ok(Self { ring })
    }

    /// Creates a strong writer for this channel.
    pub fn writer(&self) -> Result<Writer> {
        Ok(Writer::Strong(self.strong_writer()?))
    }

    /// Creates a strong writer tracked in the channel metadata.
    pub fn strong_writer(&self) -> Result<StrongWriter> {
        self.ring
            .region()
            .increment_writer_count()
            .map_err(Error::Core)?;
        let writer_id = match self.ring.region().allocate_writer_id().map_err(Error::Core) {
            Ok(writer_id) => writer_id,
            Err(error) => {
                if let Err(_rollback_error) = self.ring.region().decrement_writer_count() {}
                return Err(error);
            }
        };
        let signal = match self.ring.signal().map(attach_signal).transpose() {
            Ok(signal) => signal,
            Err(error) => {
                if let Err(_rollback_error) = self.ring.region().decrement_writer_count() {}
                return Err(error);
            }
        };
        Ok(StrongWriter::new(
            self.ring.region().clone(),
            writer_id,
            signal,
        ))
    }

    /// Creates a weak writer that acquires positions on demand.
    pub fn weak_writer(&self) -> Result<WeakWriter> {
        let writer_id = self
            .ring
            .region()
            .allocate_writer_id()
            .map_err(Error::Core)?;
        Ok(WeakWriter::new(
            self.ring.region().clone(),
            writer_id,
            self.ring.signal().map(attach_signal).transpose()?,
        ))
    }

    /// Creates a strong reader that prevents buffer overwrite.
    pub fn strong_reader(&self) -> Result<StrongReader> {
        let tail = self.ring.read_next_tail().map_err(Error::Core)?;
        let start_pos = tail;
        let reader_id = self
            .ring
            .region()
            .allocate_reader_slot(start_pos)
            .map_err(Error::Core)?;
        Ok(StrongReader::new(
            self.ring.region().clone(),
            start_pos,
            reader_id,
        ))
    }

    /// Creates a weak reader that may lose data if slow.
    pub fn weak_reader(&self) -> WeakReader {
        let tail = self.ring.read_next_tail().unwrap_or(0);
        WeakReader::new(self.ring.region().clone(), tail)
    }

    /// Returns the underlying ring buffer.
    pub fn ring(&self) -> &RingBuf {
        &self.ring
    }

    /// Returns the channel notification signal when this handle has one attached.
    pub fn signal(&self) -> Option<&Signal> {
        self.ring.signal()
    }
}

fn attach_signal(signal: &Signal) -> Result<Signal> {
    Signal::attach(signal.shared_id()).map_err(|e| Error::Core(crate::Error::Guest(e.to_string())))
}
