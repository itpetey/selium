//! Guest-side channel I/O facade built atop queue + shared-memory hostcalls.

use std::collections::BTreeMap;

use selium_abi::{
    GuestResourceId, QueueCommit, QueueCreate, QueueDelivery, QueueOverflow, QueueRole,
    QueueStatusCode,
};
use thiserror::Error;

use crate::{queue, shm};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelDescriptor {
    pub queue_shared_id: GuestResourceId,
    pub max_frame_bytes: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceivedMessage {
    pub seq: u64,
    pub writer_id: u32,
    pub payload: Vec<u8>,
}

#[derive(Debug, Error)]
pub enum IoError {
    #[error(transparent)]
    Driver(#[from] crate::driver::DriverError),
    #[error("queue operation `{operation}` failed with status {status:?}")]
    QueueStatus {
        operation: &'static str,
        status: QueueStatusCode,
    },
    #[error("payload length {actual} exceeds max {max}")]
    PayloadTooLarge { actual: usize, max: u32 },
    #[error("queue operation `{0}` returned missing payload")]
    MissingPayload(&'static str),
}

pub struct ChannelWriter {
    endpoint_id: u32,
    shm_resource_id: u32,
    shm_shared_id: GuestResourceId,
    max_frame_bytes: u32,
}

pub struct ChannelReader {
    endpoint_id: u32,
    attached_shm: BTreeMap<GuestResourceId, u32>,
}

pub async fn create_channel(
    capacity_frames: u32,
    max_frame_bytes: u32,
) -> Result<ChannelDescriptor, IoError> {
    let queue = queue::create(QueueCreate {
        capacity_frames,
        max_frame_bytes,
        delivery: QueueDelivery::Lossless,
        overflow: QueueOverflow::Block,
    })
    .await?;

    Ok(ChannelDescriptor {
        queue_shared_id: queue.shared_id,
        max_frame_bytes,
    })
}

pub async fn attach_writer(
    channel: &ChannelDescriptor,
    writer_id: u32,
) -> Result<ChannelWriter, IoError> {
    let endpoint = queue::attach(channel.queue_shared_id, QueueRole::Writer { writer_id }).await?;
    let shm_descriptor = shm::alloc(channel.max_frame_bytes, 8).await?;

    Ok(ChannelWriter {
        endpoint_id: endpoint.resource_id,
        shm_resource_id: shm_descriptor.resource_id,
        shm_shared_id: shm_descriptor.shared_id,
        max_frame_bytes: channel.max_frame_bytes,
    })
}

pub async fn attach_reader(channel: &ChannelDescriptor) -> Result<ChannelReader, IoError> {
    let endpoint = queue::attach(channel.queue_shared_id, QueueRole::Reader).await?;
    Ok(ChannelReader {
        endpoint_id: endpoint.resource_id,
        attached_shm: BTreeMap::new(),
    })
}

impl ChannelWriter {
    pub async fn send(&mut self, payload: &[u8], timeout_ms: u32) -> Result<u64, IoError> {
        let len = payload.len();
        if len > self.max_frame_bytes as usize {
            return Err(IoError::PayloadTooLarge {
                actual: len,
                max: self.max_frame_bytes,
            });
        }

        let reserved = queue::reserve(self.endpoint_id, len as u32, timeout_ms).await?;
        ensure_queue_ok("reserve", reserved.code)?;
        let reservation = reserved
            .reservation
            .ok_or(IoError::MissingPayload("reserve"))?;

        shm::write(self.shm_resource_id, 0, payload.to_vec()).await?;

        let committed = queue::commit(QueueCommit {
            endpoint_id: self.endpoint_id,
            reservation_id: reservation.reservation_id,
            shm_shared_id: self.shm_shared_id,
            offset: 0,
            len: len as u32,
        })
        .await?;
        ensure_queue_ok("commit", committed.code)?;

        Ok(reservation.seq)
    }

    pub async fn close(self) -> Result<(), IoError> {
        let endpoint = queue::close(self.endpoint_id).await?;
        ensure_queue_ok("close(writer endpoint)", endpoint.code)?;
        shm::detach(self.shm_resource_id).await?;
        Ok(())
    }
}

impl ChannelReader {
    pub async fn recv(&mut self, timeout_ms: u32) -> Result<Option<ReceivedMessage>, IoError> {
        let waited = queue::wait(self.endpoint_id, timeout_ms).await?;
        match waited.code {
            QueueStatusCode::Timeout => Ok(None),
            QueueStatusCode::Ok => {
                let frame = waited.frame.ok_or(IoError::MissingPayload("wait"))?;
                let shm_resource_id = match self.attached_shm.get(&frame.shm_shared_id) {
                    Some(existing) => *existing,
                    None => {
                        let attached = shm::attach(frame.shm_shared_id).await?;
                        self.attached_shm
                            .insert(frame.shm_shared_id, attached.resource_id);
                        attached.resource_id
                    }
                };

                let payload = shm::read(shm_resource_id, frame.offset, frame.len).await?;

                let acked = queue::ack(self.endpoint_id, frame.seq).await?;
                ensure_queue_ok("ack", acked.code)?;

                Ok(Some(ReceivedMessage {
                    seq: frame.seq,
                    writer_id: frame.writer_id,
                    payload,
                }))
            }
            status => Err(IoError::QueueStatus {
                operation: "wait",
                status,
            }),
        }
    }

    pub async fn close(self) -> Result<(), IoError> {
        let endpoint = queue::close(self.endpoint_id).await?;
        ensure_queue_ok("close(reader endpoint)", endpoint.code)?;
        for resource_id in self.attached_shm.into_values() {
            shm::detach(resource_id).await?;
        }
        Ok(())
    }
}

fn ensure_queue_ok(operation: &'static str, status: QueueStatusCode) -> Result<(), IoError> {
    if status == QueueStatusCode::Ok {
        Ok(())
    } else {
        Err(IoError::QueueStatus { operation, status })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_channel_returns_kernel_error_with_native_stub_driver() {
        let err = crate::block_on(create_channel(8, 256)).expect_err("stub should fail");
        assert!(matches!(
            err,
            IoError::Driver(crate::driver::DriverError::Kernel(2))
        ));
    }

    #[test]
    fn payload_too_large_is_rejected() {
        let mut writer = ChannelWriter {
            endpoint_id: 1,
            shm_resource_id: 2,
            shm_shared_id: 3,
            max_frame_bytes: 2,
        };

        let err = crate::block_on(writer.send(&[1, 2, 3], 0)).expect_err("too large");
        assert!(matches!(err, IoError::PayloadTooLarge { .. }));
    }
}
