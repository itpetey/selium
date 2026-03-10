//! Guest-side channel I/O facade built atop queue + shared-memory hostcalls.

use std::collections::BTreeMap;

use selium_abi::{
    DataValue, GuestResourceId, QueueCommit, QueueCreate, QueueDelivery, QueueOverflow, QueueRole,
    QueueStatusCode, decode_rkyv,
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
    #[error("invalid managed endpoint bindings: {0}")]
    InvalidManagedBindings(String),
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

pub async fn managed_event_writer(
    bindings: &[u8],
    endpoint: &str,
    writer_id: u32,
) -> Result<ChannelWriter, IoError> {
    let channel = managed_endpoint_channel(bindings, "writers", "event", endpoint)?;
    attach_writer(&channel, writer_id).await
}

pub async fn managed_event_reader(
    bindings: &[u8],
    endpoint: &str,
) -> Result<ChannelReader, IoError> {
    let channel = managed_endpoint_channel(bindings, "readers", "event", endpoint)?;
    attach_reader(&channel).await
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

fn managed_endpoint_channel(
    bindings: &[u8],
    section: &str,
    endpoint_kind: &str,
    endpoint: &str,
) -> Result<ChannelDescriptor, IoError> {
    let value = decode_rkyv::<DataValue>(bindings)
        .map_err(|err| IoError::InvalidManagedBindings(err.to_string()))?;
    let binding = value
        .get(section)
        .and_then(|section| section.get(endpoint_kind))
        .and_then(|kind_bindings| kind_bindings.get(endpoint))
        .ok_or_else(|| {
            IoError::InvalidManagedBindings(format!(
                "missing binding for `{endpoint_kind}:{endpoint}`"
            ))
        })?;

    let queue_shared_id = binding
        .get("queue_shared_id")
        .and_then(DataValue::as_u64)
        .ok_or_else(|| IoError::InvalidManagedBindings("missing queue_shared_id".to_string()))?;
    let max_frame_bytes = binding
        .get("max_frame_bytes")
        .and_then(DataValue::as_u64)
        .and_then(|value| u32::try_from(value).ok())
        .ok_or_else(|| IoError::InvalidManagedBindings("missing max_frame_bytes".to_string()))?;

    Ok(ChannelDescriptor {
        queue_shared_id,
        max_frame_bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use selium_abi::encode_rkyv;

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

    #[test]
    fn managed_event_channel_reads_from_event_kind_partition() {
        let bindings = managed_bindings_fixture();

        let channel = managed_endpoint_channel(&bindings, "writers", "event", "shared")
            .expect("event binding present");

        assert_eq!(
            channel,
            ChannelDescriptor {
                queue_shared_id: 11,
                max_frame_bytes: 256,
            }
        );
    }

    #[test]
    fn managed_event_channel_keeps_same_name_cross_kind_bindings_distinct() {
        let bindings = managed_bindings_fixture();

        let event = managed_endpoint_channel(&bindings, "writers", "event", "shared")
            .expect("event binding present");
        let service = managed_endpoint_channel(&bindings, "writers", "service", "shared")
            .expect("service binding present");
        let stream = managed_endpoint_channel(&bindings, "writers", "stream", "shared")
            .expect("stream binding present");

        assert_ne!(event.queue_shared_id, service.queue_shared_id);
        assert_ne!(event.queue_shared_id, stream.queue_shared_id);
        assert_ne!(service.queue_shared_id, stream.queue_shared_id);
        assert_eq!(event.max_frame_bytes, 256);
        assert_eq!(service.max_frame_bytes, 1024);
        assert_eq!(stream.max_frame_bytes, 512);
    }

    #[test]
    fn managed_endpoint_channel_reports_missing_binding_for_unknown_endpoint() {
        let bindings = managed_bindings_fixture();

        let err = managed_endpoint_channel(&bindings, "readers", "stream", "missing")
            .expect_err("stream binding should be absent");

        assert!(matches!(
            err,
            IoError::InvalidManagedBindings(message)
                if message.contains("stream:missing")
        ));
    }

    fn managed_bindings_fixture() -> Vec<u8> {
        encode_rkyv(&DataValue::Map(BTreeMap::from([
            (
                "writers".to_string(),
                DataValue::Map(BTreeMap::from([
                    (
                        "event".to_string(),
                        DataValue::Map(BTreeMap::from([(
                            "shared".to_string(),
                            descriptor_value(11, 256),
                        )])),
                    ),
                    (
                        "service".to_string(),
                        DataValue::Map(BTreeMap::from([(
                            "shared".to_string(),
                            descriptor_value(22, 1024),
                        )])),
                    ),
                    (
                        "stream".to_string(),
                        DataValue::Map(BTreeMap::from([(
                            "shared".to_string(),
                            descriptor_value(44, 512),
                        )])),
                    ),
                ])),
            ),
            (
                "readers".to_string(),
                DataValue::Map(BTreeMap::from([
                    (
                        "event".to_string(),
                        DataValue::Map(BTreeMap::from([(
                            "shared".to_string(),
                            descriptor_value(33, 256),
                        )])),
                    ),
                    (
                        "stream".to_string(),
                        DataValue::Map(BTreeMap::from([(
                            "shared".to_string(),
                            descriptor_value(55, 512),
                        )])),
                    ),
                ])),
            ),
        ])))
        .expect("encode fixture")
    }

    fn descriptor_value(queue_shared_id: u64, max_frame_bytes: u64) -> DataValue {
        DataValue::Map(BTreeMap::from([
            (
                "queue_shared_id".to_string(),
                DataValue::U64(queue_shared_id),
            ),
            (
                "max_frame_bytes".to_string(),
                DataValue::U64(max_frame_bytes),
            ),
        ]))
    }
}
