//! Resource descriptors shared by hostcall requests and outputs.

use crate::{LocalResourceId, SharedResourceId};
use rkyv::{Archive, Deserialize, Serialize};

/// Descriptor for an allocated shared memory region.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct SharedRegionDescriptor {
    /// Shared region id.
    pub shared_id: SharedResourceId,
    /// Region length in bytes.
    pub len: u64,
}

/// Descriptor for a local mapping of a shared memory region.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct SharedMappingDescriptor {
    /// Local mapping id.
    pub local_id: LocalResourceId,
    /// Shared region id backing the mapping.
    pub shared_id: SharedResourceId,
    /// Mapping length in bytes.
    pub len: u64,
}

/// Descriptor for a signal handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct SignalDescriptor {
    /// Local signal handle id.
    pub local_id: LocalResourceId,
    /// Shared signal id.
    pub shared_id: SharedResourceId,
}

/// Memory protection level for a shared region mapping.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
#[repr(u8)]
pub enum RegionProt {
    /// Read-only mapping (`PROT_READ`).
    ReadOnly = 0,
    /// Read-write mapping (`PROT_READ | PROT_WRITE`).
    ReadWrite = 1,
}

/// Descriptor for a shared region allocation returned by `AllocRegion`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct RegionAllocation {
    /// Shared region id.
    pub region_id: u64,
    /// Page offset within guest linear memory where the region is mapped.
    pub page_offset: u32,
}

/// Descriptor for a shared region attachment returned by `AttachRegion`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct RegionAttachment {
    /// Page offset within guest linear memory where the region is mapped.
    pub page_offset: u32,
    /// Total size of the attached region in bytes.
    pub len: u32,
}

/// Descriptor for a host-mediated connection queue handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct HostQueueDescriptor {
    /// Local queue handle id.
    pub local_id: LocalResourceId,
    /// Shared queue id.
    pub shared_id: SharedResourceId,
}

/// Descriptor for an opened durable volume handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct VolumeDescriptor {
    /// Local volume handle id.
    pub local_id: LocalResourceId,
    /// Current length of the volume in bytes.
    pub len: u64,
}

/// Informational tag for the intended use of an allocated shared memory region.
///
/// **Not** used for AAA decisions — a guest may spoof this value; the only effect
/// is cosmetic (e.g. discovery URI alias, UI icon).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum ResourceKind {
    /// Shared memory region for tracing log transport.
    LogChannel,
    /// Shared memory region for a live table.
    LiveTable,
    /// Shared memory region for RPC request/reply rings.
    RpcRing,
    /// Shared memory region for pub/sub topic.
    PubSubTopic,
    /// Shared memory region for network socket buffers.
    NetworkBuffer,
    /// Generic/unknown shared memory region.
    SharedMemory,
}
