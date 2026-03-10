//! Low-level queue hostcalls for guest modules.
//!
//! These functions expose the underlying queue primitives used by higher-level helpers such as
//! [`crate::io`]. Reach for this module when you need explicit control over queue creation,
//! reservations, acknowledgements, or stats.

use rkyv::Archive;
use selium_abi::{
    GuestResourceId, GuestUint, QueueAck, QueueAttach, QueueClose, QueueCommit, QueueCreate,
    QueueDescriptor, QueueEndpoint, QueueReserve, QueueReserveResult, QueueRole, QueueShare,
    QueueStats, QueueStatsResult, QueueStatus, QueueWait, QueueWaitResult,
};

use crate::driver::{DriverError, DriverFuture, RkyvDecoder, encode_args};

const RESOURCE_ID_CAPACITY: usize = core::mem::size_of::<<GuestResourceId as Archive>::Archived>();
const QUEUE_DESCRIPTOR_CAPACITY: usize =
    core::mem::size_of::<<QueueDescriptor as Archive>::Archived>();
const QUEUE_ENDPOINT_CAPACITY: usize = core::mem::size_of::<<QueueEndpoint as Archive>::Archived>();
const QUEUE_STATUS_CAPACITY: usize = core::mem::size_of::<<QueueStatus as Archive>::Archived>();
const QUEUE_STATS_RESULT_CAPACITY: usize =
    core::mem::size_of::<<QueueStatsResult as Archive>::Archived>();
const QUEUE_RESERVE_RESULT_CAPACITY: usize =
    core::mem::size_of::<<QueueReserveResult as Archive>::Archived>();
const QUEUE_WAIT_RESULT_CAPACITY: usize =
    core::mem::size_of::<<QueueWaitResult as Archive>::Archived>();

/// Create a queue and return both the local resource id and the shared id.
///
/// The shared id can be passed to other guests or stored in bindings so they can call [`attach`].
pub async fn create(input: QueueCreate) -> Result<QueueDescriptor, DriverError> {
    let args = encode_args(&input)?;
    DriverFuture::<queue_create::Module, RkyvDecoder<QueueDescriptor>>::new(
        &args,
        QUEUE_DESCRIPTOR_CAPACITY,
        RkyvDecoder::new(),
    )?
    .await
}

/// Convert an existing local queue resource id into a shareable queue identifier.
pub async fn share(resource_id: GuestUint) -> Result<GuestResourceId, DriverError> {
    let args = encode_args(&QueueShare { resource_id })?;
    DriverFuture::<queue_share::Module, RkyvDecoder<GuestResourceId>>::new(
        &args,
        RESOURCE_ID_CAPACITY,
        RkyvDecoder::new(),
    )?
    .await
}

/// Attach to a shared queue as the requested reader or writer role.
pub async fn attach(
    shared_id: GuestResourceId,
    role: QueueRole,
) -> Result<QueueEndpoint, DriverError> {
    let args = encode_args(&QueueAttach { shared_id, role })?;
    DriverFuture::<queue_attach::Module, RkyvDecoder<QueueEndpoint>>::new(
        &args,
        QUEUE_ENDPOINT_CAPACITY,
        RkyvDecoder::new(),
    )?
    .await
}

/// Close a local queue or endpoint handle.
///
/// This does not revoke any previously-issued shared ids.
pub async fn close(resource_id: GuestUint) -> Result<QueueStatus, DriverError> {
    let args = encode_args(&QueueClose { resource_id })?;
    DriverFuture::<queue_close::Module, RkyvDecoder<QueueStatus>>::new(
        &args,
        QUEUE_STATUS_CAPACITY,
        RkyvDecoder::new(),
    )?
    .await
}

/// Retrieve runtime-reported statistics for a local queue handle.
pub async fn stats(queue_id: GuestUint) -> Result<QueueStatsResult, DriverError> {
    let args = encode_args(&QueueStats { queue_id })?;
    DriverFuture::<queue_stats::Module, RkyvDecoder<QueueStatsResult>>::new(
        &args,
        QUEUE_STATS_RESULT_CAPACITY,
        RkyvDecoder::new(),
    )?
    .await
}

/// Reserve capacity for one outbound frame on a writer endpoint.
///
/// The returned reservation id must be committed with [`commit`] or released with [`cancel`].
pub async fn reserve(
    endpoint_id: GuestUint,
    len: u32,
    timeout_ms: u32,
) -> Result<QueueReserveResult, DriverError> {
    let args = encode_args(&QueueReserve {
        endpoint_id,
        len,
        timeout_ms,
    })?;
    DriverFuture::<queue_reserve::Module, RkyvDecoder<QueueReserveResult>>::new(
        &args,
        QUEUE_RESERVE_RESULT_CAPACITY,
        RkyvDecoder::new(),
    )?
    .await
}

/// Commit a previously reserved frame after writing its payload to shared memory.
pub async fn commit(input: QueueCommit) -> Result<QueueStatus, DriverError> {
    let args = encode_args(&input)?;
    DriverFuture::<queue_commit::Module, RkyvDecoder<QueueStatus>>::new(
        &args,
        QUEUE_STATUS_CAPACITY,
        RkyvDecoder::new(),
    )?
    .await
}

/// Cancel a previously reserved frame without committing payload data.
pub async fn cancel(
    endpoint_id: GuestUint,
    reservation_id: u64,
) -> Result<QueueStatus, DriverError> {
    let args = encode_args(&selium_abi::QueueCancel {
        endpoint_id,
        reservation_id,
    })?;
    DriverFuture::<queue_cancel::Module, RkyvDecoder<QueueStatus>>::new(
        &args,
        QUEUE_STATUS_CAPACITY,
        RkyvDecoder::new(),
    )?
    .await
}

/// Wait for the next readable frame on a reader endpoint.
///
/// Inspect the returned status code to distinguish successful delivery from timeout or other
/// runtime conditions.
pub async fn wait(endpoint_id: GuestUint, timeout_ms: u32) -> Result<QueueWaitResult, DriverError> {
    let args = encode_args(&QueueWait {
        endpoint_id,
        timeout_ms,
    })?;
    DriverFuture::<queue_wait::Module, RkyvDecoder<QueueWaitResult>>::new(
        &args,
        QUEUE_WAIT_RESULT_CAPACITY,
        RkyvDecoder::new(),
    )?
    .await
}

/// Acknowledge one delivered frame sequence so the runtime can release reader-side state.
pub async fn ack(endpoint_id: GuestUint, seq: u64) -> Result<QueueStatus, DriverError> {
    let args = encode_args(&QueueAck { endpoint_id, seq })?;
    DriverFuture::<queue_ack::Module, RkyvDecoder<QueueStatus>>::new(
        &args,
        QUEUE_STATUS_CAPACITY,
        RkyvDecoder::new(),
    )?
    .await
}

driver_module!(queue_create, "selium::queue::create");
driver_module!(queue_share, "selium::queue::share");
driver_module!(queue_attach, "selium::queue::attach");
driver_module!(queue_close, "selium::queue::close");
driver_module!(queue_stats, "selium::queue::stats");
driver_module!(queue_reserve, "selium::queue::reserve");
driver_module!(queue_commit, "selium::queue::commit");
driver_module!(queue_cancel, "selium::queue::cancel");
driver_module!(queue_wait, "selium::queue::wait");
driver_module!(queue_ack, "selium::queue::ack");

#[cfg(test)]
mod tests {
    use super::*;
    use selium_abi::{
        QueueDelivery, QueueFrameRef, QueueOverflow, QueueReservation, QueueStatsData,
        QueueStatusCode, encode_rkyv,
    };

    #[test]
    fn create_returns_kernel_error_with_native_stub_driver() {
        let err = crate::block_on(create(QueueCreate {
            capacity_frames: 8,
            max_frame_bytes: 256,
            delivery: QueueDelivery::Lossless,
            overflow: QueueOverflow::Block,
        }))
        .expect_err("stub should fail");
        assert!(matches!(err, DriverError::Kernel(2)));
    }

    #[test]
    fn descriptor_capacity_covers_archived_payload() {
        let descriptor = QueueDescriptor {
            resource_id: 7,
            shared_id: 11,
        };
        let encoded = encode_rkyv(&descriptor).expect("encode descriptor");
        assert!(encoded.len() <= QUEUE_DESCRIPTOR_CAPACITY);
    }

    #[test]
    fn reserve_result_capacity_covers_archived_payload() {
        let result = QueueReserveResult {
            code: QueueStatusCode::Ok,
            reservation: Some(QueueReservation {
                reservation_id: 99,
                seq: 101,
            }),
        };
        let encoded = encode_rkyv(&result).expect("encode reserve result");
        assert!(encoded.len() <= QUEUE_RESERVE_RESULT_CAPACITY);
    }

    #[test]
    fn wait_result_capacity_covers_archived_payload() {
        let result = QueueWaitResult {
            code: QueueStatusCode::Ok,
            frame: Some(QueueFrameRef {
                seq: 12,
                writer_id: 2,
                shm_shared_id: 33,
                offset: 4,
                len: 5,
            }),
        };
        let encoded = encode_rkyv(&result).expect("encode wait result");
        assert!(encoded.len() <= QUEUE_WAIT_RESULT_CAPACITY);
    }

    #[test]
    fn stats_result_capacity_covers_archived_payload() {
        let result = QueueStatsResult {
            code: QueueStatusCode::Ok,
            stats: Some(QueueStatsData {
                depth_frames: 1,
                reservations: 2,
                readers: 3,
                writers: 4,
                closed: false,
            }),
        };
        let encoded = encode_rkyv(&result).expect("encode stats result");
        assert!(encoded.len() <= QUEUE_STATS_RESULT_CAPACITY);
    }
}
