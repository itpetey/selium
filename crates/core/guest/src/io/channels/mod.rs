use crate::io::{
    error::{Error, Result},
    ring_buf::{RingBuf, round_capacity},
};

pub use self::{
    reader::{BlockingReader, HasGeneration, Reader},
    writer::{BlockingWriter, Writer},
};

pub mod reader;
pub mod writer;

/// A shared-memory-backed channel with [non-]blocking reader and writer semantics.
///
/// The channel stores framed messages in a lock-free ring buffer. Blocking readers
/// prevent buffer overwrite until they have consumed data; non-blocking readers may lose
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

    /// Creates a blocking writer for this channel.
    ///
    /// The writer increments `writer_count` and allocates a `writer_id`.
    pub fn blocking_writer(&self) -> Result<BlockingWriter> {
        BlockingWriter::new(self.ring.region().clone())
    }

    /// Creates a non-blocking writer that does not contribute to `writer_count`.
    pub fn writer(&self) -> Result<Writer> {
        let writer_id = self.ring.region().allocate_writer_id()?;
        Ok(Writer::new(self.ring.region().clone(), writer_id))
    }

    /// Creates a non-blocking reader that may lose data if slow.
    pub fn reader(&self) -> Reader {
        let tail = self.ring.read_next_tail().unwrap_or(0);
        Reader::new(self.ring.region().clone(), tail)
    }

    /// Creates a blocking reader that prevents buffer overwrite.
    pub fn blocking_reader(&self) -> Result<BlockingReader> {
        let tail = self.ring.read_next_tail()?;
        let reader_id = self.ring.region().allocate_reader_slot(tail)?;
        Ok(BlockingReader::new(
            self.ring.region().clone(),
            tail,
            reader_id,
        ))
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
