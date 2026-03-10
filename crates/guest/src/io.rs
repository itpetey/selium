//! High-level queue-backed messaging helpers for guest modules.
//!
//! This module combines [`crate::queue`] and [`crate::shm`] into a simpler send/receive API for
//! framed messages. Use it when you want byte or typed payload delivery without manually managing
//! queue reservations and shared-memory attachments yourself.
//!
//! For applications that receive runtime-provided endpoint bindings, the managed endpoint helpers
//! in this module can resolve channel descriptors directly from a bindings payload.

use std::collections::BTreeMap;

use selium_abi::{
    CanonicalDeserialize, CanonicalSerialize, DataValue, GuestResourceId, QueueCommit, QueueCreate,
    QueueDelivery, QueueOverflow, QueueRole, QueueStatusCode, decode_canonical, decode_rkyv,
    encode_canonical,
};
use thiserror::Error;

use crate::{queue, shm};

/// Identifies a queue-backed channel that readers and writers can attach to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelDescriptor {
    /// Shared queue identifier passed to [`attach_writer`] or [`attach_reader`].
    pub queue_shared_id: GuestResourceId,
    /// Maximum number of payload bytes accepted by a writer on this channel.
    pub max_frame_bytes: u32,
}

/// A message received from a channel reader.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceivedMessage {
    /// Monotonic queue sequence assigned by the runtime.
    pub seq: u64,
    /// Writer identifier supplied when the sender attached to the channel.
    pub writer_id: u32,
    /// Raw payload bytes copied out of shared memory.
    pub payload: Vec<u8>,
}

/// A received message whose payload has already been decoded into a typed value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypedReceivedMessage<T> {
    /// Monotonic queue sequence assigned by the runtime.
    pub seq: u64,
    /// Writer identifier supplied when the sender attached to the channel.
    pub writer_id: u32,
    /// Decoded payload value.
    pub payload: T,
}

/// Guest-facing abstraction for managed endpoint bindings used by high-level I/O helpers.
///
/// This is implemented for structured `DataValue` bindings and the encoded byte form, allowing
/// guest code to pass managed bindings around without exposing the underlying codec choice.
pub trait ManagedBindings {
    /// Resolve one channel binding from a managed bindings payload.
    ///
    /// Implementations are expected to look up the requested section/kind/name triple and return
    /// the queue descriptor needed to attach a reader or writer.
    fn lookup_channel(
        &self,
        section: &str,
        endpoint_kind: &str,
        endpoint: &str,
    ) -> Result<ChannelDescriptor, IoError>;
}

/// Encodes an application value before it is sent over a [`ChannelWriter`].
pub trait PayloadEncode {
    /// Serialize this payload into the byte representation written to the channel.
    fn encode_payload(&self) -> Result<Vec<u8>, IoError>;
}

/// Decodes a typed payload from bytes received over a [`ChannelReader`].
pub trait PayloadDecode: Sized {
    /// Deserialize one payload from the bytes returned by the runtime.
    fn decode_payload(bytes: &[u8]) -> Result<Self, IoError>;
}

/// Error returned by high-level guest I/O helpers.
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
    #[error("payload encode failed: {0}")]
    EncodePayload(String),
    #[error("payload decode failed: {0}")]
    DecodePayload(String),
    #[error("managed bindings decode failed: {0}")]
    DecodeManagedBindings(String),
    #[error("invalid managed endpoint bindings: {0}")]
    InvalidManagedBindings(String),
}

/// Sends framed payloads to one queue-backed channel.
pub struct ChannelWriter {
    endpoint_id: u32,
    shm_resource_id: u32,
    shm_shared_id: GuestResourceId,
    max_frame_bytes: u32,
}

/// Receives framed payloads from one queue-backed channel.
pub struct ChannelReader {
    endpoint_id: u32,
    attached_shm: BTreeMap<GuestResourceId, u32>,
}

/// Create a new queue-backed channel with the given capacity and frame size.
///
/// The returned descriptor can be shared with readers and writers in the same guest module or
/// stored in managed bindings for later attachment.
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

/// Attach a writer endpoint to an existing channel.
///
/// The writer allocates a shared-memory buffer sized for the channel's maximum frame length and
/// reuses it across sends.
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

/// Attach a reader endpoint to an existing channel.
pub async fn attach_reader(channel: &ChannelDescriptor) -> Result<ChannelReader, IoError> {
    let endpoint = queue::attach(channel.queue_shared_id, QueueRole::Reader).await?;
    Ok(ChannelReader {
        endpoint_id: endpoint.resource_id,
        attached_shm: BTreeMap::new(),
    })
}

/// Resolve a managed event-writer endpoint and attach a [`ChannelWriter`] to it.
pub async fn managed_event_writer<B>(
    bindings: &B,
    endpoint: &str,
    writer_id: u32,
) -> Result<ChannelWriter, IoError>
where
    B: ManagedBindings + ?Sized,
{
    let channel = managed_endpoint_channel(bindings, "writers", "event", endpoint)?;
    attach_writer(&channel, writer_id).await
}

/// Resolve a managed event-reader endpoint and attach a [`ChannelReader`] to it.
pub async fn managed_event_reader<B>(bindings: &B, endpoint: &str) -> Result<ChannelReader, IoError>
where
    B: ManagedBindings + ?Sized,
{
    let channel = managed_endpoint_channel(bindings, "readers", "event", endpoint)?;
    attach_reader(&channel).await
}

impl ChannelWriter {
    /// Send one raw payload and return the queue sequence assigned to it.
    ///
    /// Returns [`IoError::PayloadTooLarge`] when `payload` exceeds the channel's configured frame
    /// size. Queue timeout or backpressure conditions are surfaced as [`IoError::QueueStatus`].
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

    /// Encode and send one typed payload.
    pub async fn send_typed<T: PayloadEncode>(
        &mut self,
        payload: &T,
        timeout_ms: u32,
    ) -> Result<u64, IoError> {
        let payload = payload.encode_payload()?;
        self.send(&payload, timeout_ms).await
    }

    /// Close the writer endpoint and detach its shared-memory staging buffer.
    pub async fn close(self) -> Result<(), IoError> {
        let endpoint = queue::close(self.endpoint_id).await?;
        ensure_queue_ok("close(writer endpoint)", endpoint.code)?;
        shm::detach(self.shm_resource_id).await?;
        Ok(())
    }
}

impl ChannelReader {
    /// Wait for the next message on the channel.
    ///
    /// Returns `Ok(None)` when the wait times out. Successful reads automatically acknowledge the
    /// delivered frame after copying its bytes out of shared memory.
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

    /// Wait for the next message and decode it into a typed payload.
    pub async fn recv_typed<T: PayloadDecode>(
        &mut self,
        timeout_ms: u32,
    ) -> Result<Option<TypedReceivedMessage<T>>, IoError> {
        self.recv(timeout_ms)
            .await?
            .map(ReceivedMessage::into_typed)
            .transpose()
    }

    /// Close the reader endpoint and detach any shared-memory regions attached while receiving.
    pub async fn close(self) -> Result<(), IoError> {
        let endpoint = queue::close(self.endpoint_id).await?;
        ensure_queue_ok("close(reader endpoint)", endpoint.code)?;
        for resource_id in self.attached_shm.into_values() {
            shm::detach(resource_id).await?;
        }
        Ok(())
    }
}

impl ReceivedMessage {
    /// Decode the raw payload into a typed value without consuming the message.
    pub fn decode<T: PayloadDecode>(&self) -> Result<T, IoError> {
        T::decode_payload(&self.payload)
    }

    /// Decode the raw payload into a typed value and return a typed message envelope.
    pub fn into_typed<T: PayloadDecode>(self) -> Result<TypedReceivedMessage<T>, IoError> {
        Ok(TypedReceivedMessage {
            seq: self.seq,
            writer_id: self.writer_id,
            payload: T::decode_payload(&self.payload)?,
        })
    }
}

impl<T> PayloadEncode for T
where
    T: CanonicalSerialize,
{
    fn encode_payload(&self) -> Result<Vec<u8>, IoError> {
        encode_payload_value(self)
    }
}

impl<T> PayloadDecode for T
where
    T: CanonicalDeserialize,
{
    fn decode_payload(bytes: &[u8]) -> Result<Self, IoError> {
        decode_payload_value(bytes)
    }
}

impl ManagedBindings for DataValue {
    fn lookup_channel(
        &self,
        section: &str,
        endpoint_kind: &str,
        endpoint: &str,
    ) -> Result<ChannelDescriptor, IoError> {
        managed_endpoint_channel_from_value(self, section, endpoint_kind, endpoint)
    }
}

impl ManagedBindings for [u8] {
    fn lookup_channel(
        &self,
        section: &str,
        endpoint_kind: &str,
        endpoint: &str,
    ) -> Result<ChannelDescriptor, IoError> {
        let value = decode_managed_bindings(self)?;
        managed_endpoint_channel_from_value(&value, section, endpoint_kind, endpoint)
    }
}

impl ManagedBindings for Vec<u8> {
    fn lookup_channel(
        &self,
        section: &str,
        endpoint_kind: &str,
        endpoint: &str,
    ) -> Result<ChannelDescriptor, IoError> {
        self.as_slice()
            .lookup_channel(section, endpoint_kind, endpoint)
    }
}

fn ensure_queue_ok(operation: &'static str, status: QueueStatusCode) -> Result<(), IoError> {
    if status == QueueStatusCode::Ok {
        Ok(())
    } else {
        Err(IoError::QueueStatus { operation, status })
    }
}

fn managed_endpoint_channel<B>(
    bindings: &B,
    section: &str,
    endpoint_kind: &str,
    endpoint: &str,
) -> Result<ChannelDescriptor, IoError>
where
    B: ManagedBindings + ?Sized,
{
    bindings.lookup_channel(section, endpoint_kind, endpoint)
}

fn managed_endpoint_channel_from_value(
    value: &DataValue,
    section: &str,
    endpoint_kind: &str,
    endpoint: &str,
) -> Result<ChannelDescriptor, IoError> {
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

fn decode_managed_bindings(bytes: &[u8]) -> Result<DataValue, IoError> {
    decode_canonical::<DataValue>(bytes).or_else(|canonical_err| {
        decode_rkyv::<DataValue>(bytes).map_err(|rkyv_err| {
            IoError::DecodeManagedBindings(format!(
                "{canonical_err}; rkyv fallback also failed: {rkyv_err}"
            ))
        })
    })
}

fn encode_payload_value<T: CanonicalSerialize>(value: &T) -> Result<Vec<u8>, IoError> {
    encode_canonical(value).map_err(|err| IoError::EncodePayload(err.to_string()))
}

fn decode_payload_value<T: CanonicalDeserialize>(bytes: &[u8]) -> Result<T, IoError> {
    decode_canonical(bytes).map_err(|err| IoError::DecodePayload(err.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rkyv::{Archive, Deserialize, Serialize};

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

    #[test]
    fn managed_event_channel_accepts_structured_bindings() {
        let bindings = managed_bindings_value();

        let channel = managed_endpoint_channel(&bindings, "writers", "event", "shared")
            .expect("event binding present");

        assert_eq!(channel.queue_shared_id, 11);
        assert_eq!(channel.max_frame_bytes, 256);
    }

    #[test]
    fn managed_event_channel_accepts_legacy_rkyv_encoded_bindings() {
        let bindings = selium_abi::encode_rkyv(&managed_bindings_value()).expect("encode bindings");

        let channel = managed_endpoint_channel(&bindings, "writers", "event", "shared")
            .expect("event binding present");

        assert_eq!(channel.queue_shared_id, 11);
        assert_eq!(channel.max_frame_bytes, 256);
    }

    #[test]
    fn received_message_decodes_typed_payload() {
        let payload = TestPayload {
            topic: "inventory".to_string(),
            version: 3,
        }
        .encode_payload()
        .expect("encode payload");
        let message = ReceivedMessage {
            seq: 7,
            writer_id: 9,
            payload,
        };

        let typed = message.into_typed::<TestPayload>().expect("decode payload");

        assert_eq!(typed.seq, 7);
        assert_eq!(typed.writer_id, 9);
        assert_eq!(typed.payload.topic, "inventory");
        assert_eq!(typed.payload.version, 3);
    }

    #[test]
    fn received_message_reports_decode_errors() {
        let message = ReceivedMessage {
            seq: 1,
            writer_id: 2,
            payload: vec![1, 2, 3],
        };

        let err = message
            .into_typed::<TestPayload>()
            .expect_err("payload should be invalid");

        assert!(matches!(err, IoError::DecodePayload(_)));
    }

    #[test]
    fn send_typed_checks_encoded_payload_size() {
        let mut writer = ChannelWriter {
            endpoint_id: 1,
            shm_resource_id: 2,
            shm_shared_id: 3,
            max_frame_bytes: 2,
        };

        let err = crate::block_on(writer.send_typed(&OversizedPayload, 0)).expect_err("too large");

        assert!(matches!(err, IoError::PayloadTooLarge { .. }));
    }

    fn managed_bindings_fixture() -> Vec<u8> {
        managed_bindings_value()
            .encode_payload()
            .expect("encode bindings fixture")
    }

    fn managed_bindings_value() -> DataValue {
        DataValue::Map(BTreeMap::from([
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
        ]))
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

    #[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
    #[rkyv(bytecheck())]
    struct TestPayload {
        topic: String,
        version: u32,
    }

    impl CanonicalSerialize for TestPayload {
        fn encode_to(
            &self,
            encoder: &mut selium_abi::CanonicalEncoder,
        ) -> Result<(), selium_abi::ContractCodecError> {
            encoder.encode_value(&self.topic)?;
            encoder.encode_value(&self.version)?;
            Ok(())
        }
    }

    impl CanonicalDeserialize for TestPayload {
        fn decode_from(
            decoder: &mut selium_abi::CanonicalDecoder<'_>,
        ) -> Result<Self, selium_abi::ContractCodecError> {
            Ok(Self {
                topic: decoder.decode_value()?,
                version: decoder.decode_value()?,
            })
        }
    }

    struct OversizedPayload;

    impl PayloadEncode for OversizedPayload {
        fn encode_payload(&self) -> Result<Vec<u8>, IoError> {
            Ok(vec![1, 2, 3])
        }
    }
}
