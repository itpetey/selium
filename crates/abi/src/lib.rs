//! Low-level Application Binary Interface helpers for Selium host ↔ guest calls.
//!
//! This crate provides shared types for type-safe host-guest communication.

use std::fmt::{Display, Formatter};

mod context;
mod error;
mod handles;
mod ptr;

pub use context::GuestContext;
pub use error::GuestError;
pub use handles::{NetworkHandle, QueueHandle, StorageHandle};
pub use ptr::GuestPtr;

pub type GuestId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Capability {
    TimeRead,
    StorageLifecycle,
    StorageLogRead,
    StorageLogWrite,
    StorageBlobRead,
    StorageBlobWrite,
    QueueLifecycle,
    QueueWriter,
    QueueReader,
    NetworkLifecycle,
    NetworkConnect,
    NetworkStreamRead,
    NetworkStreamWrite,
}

impl Capability {
    pub fn name(&self) -> &'static str {
        match self {
            Capability::TimeRead => "TimeRead",
            Capability::StorageLifecycle => "StorageLifecycle",
            Capability::StorageLogRead => "StorageLogRead",
            Capability::StorageLogWrite => "StorageLogWrite",
            Capability::StorageBlobRead => "StorageBlobRead",
            Capability::StorageBlobWrite => "StorageBlobWrite",
            Capability::QueueLifecycle => "QueueLifecycle",
            Capability::QueueWriter => "QueueWriter",
            Capability::QueueReader => "QueueReader",
            Capability::NetworkLifecycle => "NetworkLifecycle",
            Capability::NetworkConnect => "NetworkConnect",
            Capability::NetworkStreamRead => "NetworkStreamRead",
            Capability::NetworkStreamWrite => "NetworkStreamWrite",
        }
    }
}

impl Display for Capability {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capability_name() {
        assert_eq!(Capability::TimeRead.name(), "TimeRead");
        assert_eq!(Capability::StorageLogRead.name(), "StorageLogRead");
    }

    #[test]
    fn capability_display() {
        assert_eq!(format!("{}", Capability::NetworkConnect), "NetworkConnect");
    }

    #[test]
    fn capability_equality() {
        let a = Capability::TimeRead;
        let b = Capability::TimeRead;
        let c = Capability::StorageLogRead;
        assert_eq!(a, b);
        assert_ne!(a, c);
    }
}
