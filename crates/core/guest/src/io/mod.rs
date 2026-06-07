//! Selium I/O pattern library for guests.
//!
//! Provides shared-memory-backed ring buffers, typed channels with
//! strong/weak readers and writers, and pub/sub with `Publisher` and `Subscriber`.
//!
//! The shared region layout contains cross-process coordination fields in
//! page 0 (generation counter, `next_tail`, `writer_count`, `reader_slots`,
//! `next_writer_id`, `reader_slot_counter`) and ring buffer data starting at
//! page 1. Process-local optimisation fields (`tail_cache`, `next_mutation_id`)
//! live in per-guest private memory.

pub use cursor::Cursor;
pub use error::{Error, Result};
pub use frame::FrameHeader;
pub use region::{
    ChannelRegion, DATA_OFFSET, MAX_READER_SLOTS, NEXT_TAIL_OFFSET, NEXT_WRITER_ID_OFFSET,
    PAGE_SIZE, READER_SLOT_COUNTER_OFFSET, READER_SLOTS_OFFSET, RegionBuilder, RegionMapping,
    WRITER_COUNT_OFFSET,
};
pub use ring_buf::RingBuf;

pub mod channels;
mod cursor;
pub mod error;
mod frame;
pub mod pubsub;
mod region;
mod ring_buf;
