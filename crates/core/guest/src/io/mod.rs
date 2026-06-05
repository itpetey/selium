//! Selium I/O pattern library for guests.
//!
//! Provides shared-memory-backed ring buffers, typed channels with
//! strong/weak readers and writers, and pub/sub with `Publisher` and `Subscriber`.
//!
//! The shared region layout contains only a generation counter (for atomic
//! wait/notify) and ring buffer data. All other metadata lives in per-guest
//! private memory.

pub use cursor::Cursor;
pub use error::{Error, Result};
pub use frame::FrameHeader;
pub use region::{ChannelRegion, DATA_OFFSET, RegionBuilder};
pub use ring_buf::RingBuf;

pub mod channels;
mod cursor;
pub mod error;
mod frame;
pub mod pubsub;
mod region;
mod ring_buf;
