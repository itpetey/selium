use selium_abi::ResourceKind;

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
    backpressure: ChannelBackpressure,
}

/// Backpressure strategy for channel writers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelBackpressure {
    /// Writers respect blocking reader positions; writes block when consumers fall behind.
    Park,
    /// Writers never block; slow consumers may lose data.
    Drop,
}

impl Channel {
    /// Creates a new channel with the given ring buffer data capacity and backpressure strategy.
    ///
    /// Uses `ResourceKind::SharedMemory` as the default purpose. For channels with
    /// a specific purpose (e.g., log channels, RPC rings), use
    /// [`Channel::create_with_backpressure`] instead.
    pub fn create(capacity: u64, backpressure: ChannelBackpressure) -> Result<Self> {
        Self::create_with_backpressure(capacity, backpressure, ResourceKind::SharedMemory)
    }

    /// Creates a new channel with the given ring buffer data capacity, backpressure
    /// strategy, and resource purpose.
    ///
    /// The `purpose` is threaded through to the underlying `AllocRegion` hostcall
    /// for runtime discovery registration.
    pub fn create_with_backpressure(
        capacity: u64,
        backpressure: ChannelBackpressure,
        purpose: ResourceKind,
    ) -> Result<Self> {
        let capacity = round_capacity(capacity)?;
        let ring = RingBuf::create(capacity, purpose)?;
        Ok(Self { ring, backpressure })
    }

    /// Attaches to an existing channel by shared region id.
    pub fn attach(region_id: u64, capacity: u64) -> Result<Self> {
        let capacity = round_capacity(capacity)?;
        let ring = RingBuf::attach(region_id, capacity)?;
        Ok(Self {
            ring,
            backpressure: ChannelBackpressure::Park,
        })
    }

    /// Creates a blocking writer for this channel.
    ///
    /// Returns `Err(Error::BackpressureNotSupported)` on Drop channels.
    pub fn blocking_writer(&self) -> Result<BlockingWriter> {
        if self.backpressure == ChannelBackpressure::Drop {
            return Err(Error::BackpressureNotSupported);
        }
        BlockingWriter::new(self.ring.region().clone())
    }

    /// Creates a non-blocking writer that does not contribute to `writer_count`.
    pub fn writer(&self) -> Result<Writer> {
        let writer_id = self.ring.region().allocate_writer_id()?;
        Ok(Writer::new(
            self.ring.region().clone(),
            writer_id,
            self.backpressure,
        ))
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

    /// Returns the backpressure strategy for this channel.
    pub fn backpressure(&self) -> ChannelBackpressure {
        self.backpressure
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drop_channel_rejects_blocking_writer() {
        let channel = Channel::create(64, ChannelBackpressure::Drop).expect("create");
        let result = channel.blocking_writer();
        assert!(matches!(result, Err(Error::BackpressureNotSupported)));
    }

    #[test]
    fn park_channel_accepts_blocking_writer() {
        let channel = Channel::create(64, ChannelBackpressure::Park).expect("create");
        let result = channel.blocking_writer();
        assert!(result.is_ok());
    }

    #[test]
    fn drop_channel_writer_never_blocks() {
        let channel = Channel::create(64, ChannelBackpressure::Drop).expect("create");
        let writer = channel.writer().expect("writer");
        assert_eq!(writer.backpressure(), ChannelBackpressure::Drop);
    }

    #[test]
    fn park_channel_writer_uses_park_backpressure() {
        let channel = Channel::create(64, ChannelBackpressure::Park).expect("create");
        let writer = channel.writer().expect("writer");
        assert_eq!(writer.backpressure(), ChannelBackpressure::Park);
    }

    #[test]
    fn create_with_backpressure_is_alias() {
        let channel = Channel::create_with_backpressure(
            64,
            ChannelBackpressure::Drop,
            ResourceKind::LogChannel,
        )
        .expect("create");
        assert_eq!(channel.backpressure(), ChannelBackpressure::Drop);
    }
}
