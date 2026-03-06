//! Queue control-plane payload types for guest messaging.

use rkyv::{Archive, Deserialize, Serialize};

use crate::{GuestResourceId, GuestUint};

/// Monotonic sequence number assigned to committed queue frames.
pub type QueueSeq = u64;
/// Identifier returned by reserve calls and consumed by commit/cancel.
pub type QueueReservationId = u64;

/// Endpoint role used when attaching to an existing queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum QueueRole {
    /// Reader endpoint.
    Reader,
    /// Writer endpoint with stable writer identifier.
    Writer {
        /// Identifier attached to committed frames.
        writer_id: u32,
    },
}

/// Delivery semantics enforced by the queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum QueueDelivery {
    /// Readers must observe every frame or receive a `ReaderBehind` status.
    Lossless,
    /// Readers may skip frames when queue pressure requires it.
    BestEffort,
}

/// Overflow policy used when no capacity is available.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum QueueOverflow {
    /// Block writers until capacity is available.
    Block,
    /// Reject new writes while full.
    DropNewest,
    /// Discard oldest frames to create capacity.
    DropOldest,
}

/// Queue operation status code.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum QueueStatusCode {
    /// Operation succeeded.
    Ok = 0,
    /// Operation would block and was not allowed to wait.
    WouldBlock = 1,
    /// Wait operation exceeded timeout.
    Timeout = 2,
    /// Queue is closed.
    Closed = 3,
    /// Queue is full.
    Full = 4,
    /// Queue is empty.
    Empty = 5,
    /// Reader cursor has fallen behind retained frames.
    ReaderBehind = 6,
    /// Resource or endpoint could not be resolved.
    NotFound = 7,
    /// Caller lacks permission for this operation.
    PermissionDenied = 8,
    /// Input arguments are invalid.
    InvalidArgument = 9,
    /// Reservation is stale or owned by another writer.
    StaleReservation = 10,
    /// SHM payload bounds are invalid.
    PayloadOutOfBounds = 11,
    /// Payload exceeds queue limits.
    TooLarge = 12,
    /// Internal error.
    Internal = 255,
}

/// Parameters for creating a queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct QueueCreate {
    /// Maximum number of committed/reserved frames tracked by the queue.
    pub capacity_frames: u32,
    /// Maximum frame payload length in bytes.
    pub max_frame_bytes: u32,
    /// Delivery guarantees.
    pub delivery: QueueDelivery,
    /// Overflow behaviour.
    pub overflow: QueueOverflow,
}

/// Local + shared queue handle returned by queue creation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct QueueDescriptor {
    /// Instance-local queue resource identifier.
    pub resource_id: GuestUint,
    /// Cross-instance shared queue handle.
    pub shared_id: GuestResourceId,
}

/// Payload for sharing a local queue handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct QueueShare {
    /// Instance-local queue resource identifier.
    pub resource_id: GuestUint,
}

/// Attach to a shared queue handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct QueueAttach {
    /// Shared queue handle.
    pub shared_id: GuestResourceId,
    /// Requested endpoint role.
    pub role: QueueRole,
}

/// Local endpoint descriptor returned by attach.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct QueueEndpoint {
    /// Instance-local endpoint resource identifier.
    pub resource_id: GuestUint,
}

/// Reserve writer capacity for one frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct QueueReserve {
    /// Writer endpoint resource identifier.
    pub endpoint_id: GuestUint,
    /// Payload length in bytes.
    pub len: u32,
    /// Wait timeout in milliseconds (`0` for non-blocking).
    pub timeout_ms: u32,
}

/// Reservation metadata returned by reserve.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct QueueReservation {
    /// Reservation identifier for commit/cancel.
    pub reservation_id: QueueReservationId,
    /// Sequence number assigned to this future frame.
    pub seq: QueueSeq,
}

/// Commit a reserved frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct QueueCommit {
    /// Writer endpoint resource identifier.
    pub endpoint_id: GuestUint,
    /// Reservation identifier returned by `queue_reserve`.
    pub reservation_id: QueueReservationId,
    /// Shared SHM handle containing payload bytes.
    pub shm_shared_id: GuestResourceId,
    /// Start offset inside SHM payload.
    pub offset: u32,
    /// Payload length in bytes.
    pub len: u32,
}

/// Cancel an outstanding reservation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct QueueCancel {
    /// Writer endpoint resource identifier.
    pub endpoint_id: GuestUint,
    /// Reservation identifier returned by `queue_reserve`.
    pub reservation_id: QueueReservationId,
}

/// Wait for a readable frame on a reader endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct QueueWait {
    /// Reader endpoint resource identifier.
    pub endpoint_id: GuestUint,
    /// Wait timeout in milliseconds (`0` for non-blocking).
    pub timeout_ms: u32,
}

/// Reference to a committed frame payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct QueueFrameRef {
    /// Frame sequence number.
    pub seq: QueueSeq,
    /// Writer identifier supplied at writer attach time.
    pub writer_id: u32,
    /// Shared SHM handle containing payload bytes.
    pub shm_shared_id: GuestResourceId,
    /// Start offset inside SHM payload.
    pub offset: u32,
    /// Payload length in bytes.
    pub len: u32,
}

/// Ack a previously observed frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct QueueAck {
    /// Reader endpoint resource identifier.
    pub endpoint_id: GuestUint,
    /// Sequence number to acknowledge.
    pub seq: QueueSeq,
}

/// Close a queue or endpoint handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct QueueClose {
    /// Local resource identifier for queue or endpoint.
    pub resource_id: GuestUint,
}

/// Request queue stats by local queue resource id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct QueueStats {
    /// Local queue resource identifier.
    pub queue_id: GuestUint,
}

/// Common status result payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct QueueStatus {
    /// Status code describing operation outcome.
    pub code: QueueStatusCode,
}

/// Reserve operation result payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct QueueReserveResult {
    /// Operation status code.
    pub code: QueueStatusCode,
    /// Reservation details when successful.
    pub reservation: Option<QueueReservation>,
}

/// Wait operation result payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct QueueWaitResult {
    /// Operation status code.
    pub code: QueueStatusCode,
    /// Frame reference when a frame is available.
    pub frame: Option<QueueFrameRef>,
}

/// Queue state snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct QueueStatsData {
    /// Number of committed frames currently retained.
    pub depth_frames: u32,
    /// Number of active writer reservations.
    pub reservations: u32,
    /// Number of attached reader endpoints.
    pub readers: u32,
    /// Number of attached writer endpoints.
    pub writers: u32,
    /// Whether queue is closed.
    pub closed: bool,
}

/// Queue stats operation result payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct QueueStatsResult {
    /// Operation status code.
    pub code: QueueStatusCode,
    /// Stats payload when successful.
    pub stats: Option<QueueStatsData>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{decode_rkyv, encode_rkyv};

    #[test]
    fn queue_reserve_result_round_trips_with_rkyv() {
        let payload = QueueReserveResult {
            code: QueueStatusCode::Ok,
            reservation: Some(QueueReservation {
                reservation_id: 7,
                seq: 11,
            }),
        };

        let encoded = encode_rkyv(&payload).expect("encode");
        let decoded = decode_rkyv::<QueueReserveResult>(&encoded).expect("decode");
        assert_eq!(decoded, payload);
    }

    #[test]
    fn queue_wait_result_round_trips_with_rkyv() {
        let payload = QueueWaitResult {
            code: QueueStatusCode::Ok,
            frame: Some(QueueFrameRef {
                seq: 12,
                writer_id: 3,
                shm_shared_id: 44,
                offset: 5,
                len: 9,
            }),
        };

        let encoded = encode_rkyv(&payload).expect("encode");
        let decoded = decode_rkyv::<QueueWaitResult>(&encoded).expect("decode");
        assert_eq!(decoded, payload);
    }
}
