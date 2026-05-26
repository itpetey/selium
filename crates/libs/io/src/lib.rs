//! Selium I/O pattern library for guests.
//!
//! Provides shared-memory-backed ring buffers, typed channels with
//! strong/weak readers and writers, versioned live tables with CAS,
//! and pub/sub with `Publisher` and `Subscriber`.

pub use cursor::Cursor;
pub use error::{Error, Result};
pub use frame::FrameHeader;
pub use region::{ChannelRegion, RegionBuilder};
pub use ring_buf::RingBuf;

pub mod channels;
mod cursor;
mod error;
mod frame;
pub mod pubsub;
mod region;
mod ring_buf;
pub mod rpc;
pub mod tables;
