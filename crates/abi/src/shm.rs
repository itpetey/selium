//! Shared memory hostcall payload types.

use rkyv::{Archive, Deserialize, Serialize};

use crate::{GuestResourceId, GuestUint};

/// Parameters for allocating a shared memory region.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ShmAlloc {
    /// Number of bytes requested.
    pub size: GuestUint,
    /// Required alignment in bytes (must be non-zero and a power of two).
    pub align: GuestUint,
}

/// A byte range inside the runtime shared memory arena.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ShmRegion {
    /// Byte offset from the start of the shared memory arena.
    pub offset: GuestUint,
    /// Length in bytes.
    pub len: GuestUint,
}

/// Descriptor returned for an attached shared memory resource.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ShmDescriptor {
    /// Instance-local resource table id.
    pub resource_id: GuestUint,
    /// Shared handle suitable for cross-instance transfer.
    pub shared_id: GuestResourceId,
    /// Region in the shared memory arena.
    pub region: ShmRegion,
}

/// Request to share an instance-local shared memory resource.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ShmShare {
    /// Instance-local resource table id.
    pub resource_id: GuestUint,
}

/// Request to attach a shared memory resource by shared handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ShmAttach {
    /// Shared handle returned by `shm_share`.
    pub shared_id: GuestResourceId,
}

/// Request to detach an instance-local shared memory resource.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ShmDetach {
    /// Instance-local resource table id.
    pub resource_id: GuestUint,
}

/// Request to read bytes from a shared memory resource.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ShmRead {
    /// Instance-local resource table id.
    pub resource_id: GuestUint,
    /// Start offset relative to the region base.
    pub offset: GuestUint,
    /// Number of bytes to read.
    pub len: GuestUint,
}

/// Request to write bytes into a shared memory resource.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ShmWrite {
    /// Instance-local resource table id.
    pub resource_id: GuestUint,
    /// Start offset relative to the region base.
    pub offset: GuestUint,
    /// Bytes to write.
    pub bytes: Vec<u8>,
}
