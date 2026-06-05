use crate::io::{
    error::{Error, Result},
    ring_buf::{RingBuf, round_capacity},
};

pub use self::{
    reader::{Reader, StrongReader, WeakReader},
    writer::{StrongWriter, WeakWriter, Writer},
};

pub mod reader;
pub mod writer;

/// A shared-memory-backed channel with strong/weak reader and writer semantics.
///
/// The channel stores framed messages in a lock-free ring buffer. Strong readers
/// prevent buffer overwrite until they have consumed data; weak readers may lose
/// data if they fall behind.
///
/// Notification uses the generation counter in the shared region with native
/// atomic wait/notify instead of signals.
pub struct Channel {
    ring: RingBuf,
}

impl Channel {
    /// Creates a new channel with the given ring buffer data capacity.
    pub fn create(capacity: u64) -> Result<Self> {
        let capacity = round_capacity(capacity)?;
        let ring = RingBuf::create(capacity)?;
        Ok(Self { ring })
    }

    /// Attaches to an existing channel by shared region id.
    pub fn attach(region_id: u64, capacity: u64) -> Result<Self> {
        let capacity = round_capacity(capacity)?;
        let ring = RingBuf::attach(region_id, capacity)?;
        Ok(Self { ring })
    }

    /// Creates a strong writer for this channel.
    pub fn writer(&self) -> Result<Writer> {
        Ok(Writer::Strong(self.strong_writer()?))
    }

    /// Creates a strong writer tracked in the channel metadata.
    pub fn strong_writer(&self) -> Result<StrongWriter> {
        self.ring.region().increment_writer_count()?;
        let writer_id = match self.ring.region().allocate_writer_id() {
            Ok(writer_id) => writer_id,
            Err(error) => {
                let _ = self.ring.region().decrement_writer_count();
                return Err(error);
            }
        };
        Ok(StrongWriter::new(self.ring.region().clone(), writer_id))
    }

    /// Creates a weak writer that acquires positions on demand.
    pub fn weak_writer(&self) -> Result<WeakWriter> {
        let writer_id = self.ring.region().allocate_writer_id()?;
        Ok(WeakWriter::new(self.ring.region().clone(), writer_id))
    }

    /// Creates a strong reader that prevents buffer overwrite.
    pub fn strong_reader(&self) -> Result<StrongReader> {
        let tail = self.ring.read_next_tail()?;
        let reader_id = self.ring.region().allocate_reader_slot(tail)?;
        Ok(StrongReader::new(
            self.ring.region().clone(),
            tail,
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

    /// Returns the shared region id.
    pub fn region_id(&self) -> u64 {
        self.ring.region_id()
    }
}
