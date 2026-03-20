//! Mailbox definitions shared between host and guest.

#[cfg(target_arch = "wasm32")]
pub mod abi {
    pub const FLAG_OFFSET: usize = 4; // After head (u32)
    pub const HEAD_OFFSET: usize = 0;
    pub const TAIL_OFFSET: usize = 8; // After head (u32) + flag (AtomicU32)
    pub const CAPACITY: usize = 32;
    pub const SLOT_SIZE: usize = 4;
    pub const RING_OFFSET: usize = 16; // After head + flag + tail
}
