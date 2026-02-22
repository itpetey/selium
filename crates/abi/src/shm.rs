//! Shared memory hostcall payload types.

use rkyv::{Archive, Deserialize, Serialize};

use crate::{GuestResourceId, GuestUint};

/// Byte width of a single ring header field.
pub const SHM_RING_FIELD_BYTES: usize = core::mem::size_of::<GuestUint>();
/// Magic marker stored in ring headers.
pub const SHM_RING_MAGIC: GuestUint = 0x5348_4d52;
/// Ring header version supported by this ABI.
pub const SHM_RING_VERSION: GuestUint = 1;
/// Offset of the magic field in the ring header.
pub const SHM_RING_MAGIC_OFFSET: usize = 0;
/// Offset of the version field in the ring header.
pub const SHM_RING_VERSION_OFFSET: usize = SHM_RING_FIELD_BYTES;
/// Offset of the capacity field in the ring header.
pub const SHM_RING_CAPACITY_OFFSET: usize = SHM_RING_FIELD_BYTES * 2;
/// Offset of the head cursor field in the ring header.
pub const SHM_RING_HEAD_OFFSET: usize = SHM_RING_FIELD_BYTES * 3;
/// Offset of the tail cursor field in the ring header.
pub const SHM_RING_TAIL_OFFSET: usize = SHM_RING_FIELD_BYTES * 4;
/// Offset of the sequence field in the ring header.
pub const SHM_RING_SEQUENCE_OFFSET: usize = SHM_RING_FIELD_BYTES * 5;
/// Offset of the flags field in the ring header.
pub const SHM_RING_FLAGS_OFFSET: usize = SHM_RING_FIELD_BYTES * 6;
/// Total size of the ring header in bytes.
pub const SHM_RING_HEADER_BYTES: usize = SHM_RING_FIELD_BYTES * 7;
/// Offset where ring payload bytes begin.
pub const SHM_RING_PAYLOAD_OFFSET: usize = SHM_RING_HEADER_BYTES;

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
    /// Guest-visible base offset of the attached shared heap mapping.
    pub mapping_offset: GuestUint,
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

/// Conditions that can satisfy a shared-memory wait operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum ShmWaitCondition {
    /// Wake when the ring contains at least one readable byte.
    DataAvailable,
    /// Wake when the ring contains at least one writable byte.
    SpaceAvailable,
    /// Wake when the ring sequence reaches or exceeds the requested value.
    SequenceAtLeast(GuestUint),
}

/// Request to wait for ring readiness.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ShmWait {
    /// Instance-local resource table id.
    pub resource_id: GuestUint,
    /// Condition that must be met before the wait completes.
    pub condition: ShmWaitCondition,
}

/// Request to notify waiters after mutating ring state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ShmNotify {
    /// Instance-local resource table id.
    pub resource_id: GuestUint,
    /// New ring sequence value observed by the caller.
    pub sequence: GuestUint,
}

/// Ring readiness snapshot returned by wait/notify hostcalls.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ShmReady {
    /// Current ring head cursor.
    pub head: GuestUint,
    /// Current ring tail cursor.
    pub tail: GuestUint,
    /// Current ring sequence.
    pub sequence: GuestUint,
    /// Number of readable payload bytes in the ring.
    pub readable: GuestUint,
    /// Number of writable payload bytes in the ring.
    pub writable: GuestUint,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{decode_rkyv, encode_rkyv};

    #[test]
    fn descriptor_round_trips_with_rkyv() {
        let payload = ShmDescriptor {
            resource_id: 3,
            shared_id: 9,
            mapping_offset: 32,
            region: ShmRegion {
                offset: 16,
                len: 64,
            },
        };

        let encoded = encode_rkyv(&payload).expect("encode");
        let decoded = decode_rkyv::<ShmDescriptor>(&encoded).expect("decode");
        assert_eq!(decoded, payload);
    }

    #[test]
    fn wait_payload_preserves_condition() {
        let payload = ShmWait {
            resource_id: 1,
            condition: ShmWaitCondition::SequenceAtLeast(7),
        };

        let encoded = encode_rkyv(&payload).expect("encode");
        let decoded = decode_rkyv::<ShmWait>(&encoded).expect("decode");
        assert_eq!(decoded, payload);
    }

    #[test]
    fn ring_header_offsets_are_contiguous() {
        assert_eq!(
            SHM_RING_VERSION_OFFSET,
            SHM_RING_MAGIC_OFFSET + SHM_RING_FIELD_BYTES
        );
        assert_eq!(
            SHM_RING_CAPACITY_OFFSET,
            SHM_RING_VERSION_OFFSET + SHM_RING_FIELD_BYTES
        );
        assert_eq!(SHM_RING_PAYLOAD_OFFSET, SHM_RING_HEADER_BYTES);
    }
}
