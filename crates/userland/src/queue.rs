//! Guest-facing helpers for queue control-plane hostcalls.

use selium_abi::{
    GuestResourceId, GuestUint, QueueAck, QueueAttach, QueueClose, QueueCommit, QueueCreate,
    QueueDescriptor, QueueEndpoint, QueueReserve, QueueReserveResult, QueueRole, QueueShare,
    QueueStats, QueueStatsResult, QueueStatus, QueueWait, QueueWaitResult,
};

use crate::driver::{DriverError, DriverFuture, RkyvDecoder, encode_args};

/// Create a queue and return local + shared handles.
pub async fn create(input: QueueCreate) -> Result<QueueDescriptor, DriverError> {
    let args = encode_args(&input)?;
    DriverFuture::<queue_create::Module, RkyvDecoder<QueueDescriptor>>::new(
        &args,
        16,
        RkyvDecoder::new(),
    )?
    .await
}

/// Share a local queue handle and return shared id.
pub async fn share(resource_id: GuestUint) -> Result<GuestResourceId, DriverError> {
    let args = encode_args(&QueueShare { resource_id })?;
    DriverFuture::<queue_share::Module, RkyvDecoder<GuestResourceId>>::new(
        &args,
        8,
        RkyvDecoder::new(),
    )?
    .await
}

/// Attach to a shared queue as reader or writer.
pub async fn attach(
    shared_id: GuestResourceId,
    role: QueueRole,
) -> Result<QueueEndpoint, DriverError> {
    let args = encode_args(&QueueAttach { shared_id, role })?;
    DriverFuture::<queue_attach::Module, RkyvDecoder<QueueEndpoint>>::new(
        &args,
        8,
        RkyvDecoder::new(),
    )?
    .await
}

/// Close a queue or endpoint local handle.
pub async fn close(resource_id: GuestUint) -> Result<QueueStatus, DriverError> {
    let args = encode_args(&QueueClose { resource_id })?;
    DriverFuture::<queue_close::Module, RkyvDecoder<QueueStatus>>::new(
        &args,
        8,
        RkyvDecoder::new(),
    )?
    .await
}

/// Retrieve queue stats for a local queue handle.
pub async fn stats(queue_id: GuestUint) -> Result<QueueStatsResult, DriverError> {
    let args = encode_args(&QueueStats { queue_id })?;
    DriverFuture::<queue_stats::Module, RkyvDecoder<QueueStatsResult>>::new(
        &args,
        64,
        RkyvDecoder::new(),
    )?
    .await
}

/// Reserve writer capacity for one frame.
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
        32,
        RkyvDecoder::new(),
    )?
    .await
}

/// Commit a previously reserved writer frame.
pub async fn commit(input: QueueCommit) -> Result<QueueStatus, DriverError> {
    let args = encode_args(&input)?;
    DriverFuture::<queue_commit::Module, RkyvDecoder<QueueStatus>>::new(
        &args,
        8,
        RkyvDecoder::new(),
    )?
    .await
}

/// Cancel a previously reserved writer frame.
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
        8,
        RkyvDecoder::new(),
    )?
    .await
}

/// Wait for the next readable frame on a reader endpoint.
pub async fn wait(endpoint_id: GuestUint, timeout_ms: u32) -> Result<QueueWaitResult, DriverError> {
    let args = encode_args(&QueueWait {
        endpoint_id,
        timeout_ms,
    })?;
    DriverFuture::<queue_wait::Module, RkyvDecoder<QueueWaitResult>>::new(
        &args,
        64,
        RkyvDecoder::new(),
    )?
    .await
}

/// Acknowledge one delivered frame sequence.
pub async fn ack(endpoint_id: GuestUint, seq: u64) -> Result<QueueStatus, DriverError> {
    let args = encode_args(&QueueAck { endpoint_id, seq })?;
    DriverFuture::<queue_ack::Module, RkyvDecoder<QueueStatus>>::new(&args, 8, RkyvDecoder::new())?
        .await
}

driver_module!(queue_create, QUEUE_CREATE, "selium::queue::create");
driver_module!(queue_share, QUEUE_SHARE, "selium::queue::share");
driver_module!(queue_attach, QUEUE_ATTACH, "selium::queue::attach");
driver_module!(queue_close, QUEUE_CLOSE, "selium::queue::close");
driver_module!(queue_stats, QUEUE_STATS, "selium::queue::stats");
driver_module!(queue_reserve, QUEUE_RESERVE, "selium::queue::reserve");
driver_module!(queue_commit, QUEUE_COMMIT, "selium::queue::commit");
driver_module!(queue_cancel, QUEUE_CANCEL, "selium::queue::cancel");
driver_module!(queue_wait, QUEUE_WAIT, "selium::queue::wait");
driver_module!(queue_ack, QUEUE_ACK, "selium::queue::ack");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_returns_kernel_error_with_native_stub_driver() {
        let err = crate::block_on(create(QueueCreate {
            capacity_frames: 8,
            max_frame_bytes: 256,
            delivery: selium_abi::QueueDelivery::Lossless,
            overflow: selium_abi::QueueOverflow::Block,
        }))
        .expect_err("stub should fail");
        assert!(matches!(err, DriverError::Kernel(2)));
    }
}
