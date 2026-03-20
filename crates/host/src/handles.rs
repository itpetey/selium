//! Handle types for capability delegation.
//!
//! Handles are opaque references to capabilities that can be granted to guests.
//! Each handle has a unique ID and is bound to the guest namespace that created it.

use std::fmt;

pub use crate::queue::QueueHandle;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StorageHandle {
    id: u64,
}

impl StorageHandle {
    pub fn new(id: u64) -> Self {
        Self { id }
    }

    pub fn id(&self) -> u64 {
        self.id
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NetworkHandle {
    id: u64,
}

impl NetworkHandle {
    pub fn new(id: u64) -> Self {
        Self { id }
    }

    pub fn id(&self) -> u64 {
        self.id
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct HandleId(pub u64);

static NEXT_HANDLE_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

pub fn next_handle_id() -> HandleId {
    HandleId(NEXT_HANDLE_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed))
}

#[derive(Debug, Clone)]
pub enum AnyHandle {
    Storage(StorageHandle),
    Network(NetworkHandle),
    Queue(QueueHandle),
}

impl AnyHandle {
    pub fn storage(id: u64) -> Self {
        AnyHandle::Storage(StorageHandle::new(id))
    }

    pub fn network(id: u64) -> Self {
        AnyHandle::Network(NetworkHandle::new(id))
    }

    pub fn queue(handle: QueueHandle) -> Self {
        AnyHandle::Queue(handle)
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            AnyHandle::Storage(_) => "Storage",
            AnyHandle::Network(_) => "Network",
            AnyHandle::Queue(_) => "Queue",
        }
    }
}

impl fmt::Display for AnyHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AnyHandle::Storage(h) => write!(f, "Storage({})", h.id()),
            AnyHandle::Network(h) => write!(f, "Network({})", h.id()),
            AnyHandle::Queue(h) => write!(f, "Queue({})", h.id()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_handle_new() {
        let handle = StorageHandle::new(42);
        assert_eq!(handle.id(), 42);
    }

    #[test]
    fn test_network_handle_new() {
        let handle = NetworkHandle::new(99);
        assert_eq!(handle.id(), 99);
    }

    #[test]
    fn test_next_handle_id_increments() {
        let id1 = next_handle_id();
        let id2 = next_handle_id();
        assert!(id2.0 > id1.0);
    }

    #[test]
    fn test_any_handle_type_name() {
        assert_eq!(AnyHandle::storage(1).type_name(), "Storage");
        assert_eq!(AnyHandle::network(1).type_name(), "Network");
        assert_eq!(AnyHandle::queue(QueueHandle::new(1)).type_name(), "Queue");
    }

    #[test]
    fn test_any_handle_display() {
        assert_eq!(format!("{}", AnyHandle::storage(1)), "Storage(1)");
        assert_eq!(format!("{}", AnyHandle::network(2)), "Network(2)");
    }
}
