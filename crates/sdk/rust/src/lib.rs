//! Rust SDK facade for Selium communication primitives.

use std::{collections::BTreeMap, marker::PhantomData, sync::Arc};

use parking_lot::RwLock;
use rkyv::{
    Archive,
    api::high::{HighDeserializer, HighValidator},
};
use selium_abi::{RkyvEncode, decode_rkyv, encode_rkyv};
use selium_io_core::{
    ChannelConfig, ChannelKind, CoreIo, DataPlaneError, PublishAck, Subscription,
};
use selium_io_durability::{ReplayStart, RetentionPolicy};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CodecError {
    #[error("serialization failed: {0}")]
    Serialize(String),
    #[error("deserialization failed: {0}")]
    Deserialize(String),
}

#[derive(Debug, Error)]
pub enum SdkError {
    #[error(transparent)]
    DataPlane(#[from] DataPlaneError),
    #[error(transparent)]
    Codec(#[from] CodecError),
}

#[derive(Clone)]
pub struct Context {
    inner: Arc<ContextInner>,
}

struct ContextInner {
    io: CoreIo,
    runtime: RwLock<RuntimeSettings>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeSettings {
    pub enforced_encoding: String,
}

impl Default for RuntimeSettings {
    fn default() -> Self {
        Self {
            enforced_encoding: "rkyv".to_string(),
        }
    }
}

pub struct ContextBuilder {
    io: Option<CoreIo>,
}

pub struct Publisher<T> {
    context: Context,
    channel: String,
    _marker: PhantomData<T>,
}

pub struct Subscriber<T> {
    inner: Subscription,
    _marker: PhantomData<T>,
}

pub struct BytePublisher {
    context: Context,
    channel: String,
}

pub struct ByteSubscriber {
    inner: Subscription,
}

impl Context {
    pub fn builder() -> ContextBuilder {
        ContextBuilder { io: None }
    }

    pub fn new() -> Self {
        Self::builder().build()
    }

    pub fn runtime_settings(&self) -> RuntimeSettings {
        self.inner.runtime.read().clone()
    }

    pub fn set_runtime_encoding(&self, encoding: impl Into<String>) {
        self.inner.runtime.write().enforced_encoding = encoding.into();
    }

    pub fn create_channel(
        &self,
        name: impl Into<String>,
        kind: ChannelKind,
        retention: RetentionPolicy,
    ) -> Result<(), SdkError> {
        self.inner
            .io
            .create_channel(name, ChannelConfig { kind, retention })?;
        Ok(())
    }

    pub fn ensure_channel(
        &self,
        name: impl Into<String>,
        kind: ChannelKind,
        retention: RetentionPolicy,
    ) -> Result<(), SdkError> {
        self.inner
            .io
            .ensure_channel(name, ChannelConfig { kind, retention })?;
        Ok(())
    }

    pub fn publisher<T>(&self, channel: impl Into<String>) -> Publisher<T> {
        Publisher {
            context: self.clone(),
            channel: channel.into(),
            _marker: PhantomData,
        }
    }

    pub fn byte_publisher(&self, channel: impl Into<String>) -> BytePublisher {
        BytePublisher {
            context: self.clone(),
            channel: channel.into(),
        }
    }

    pub fn subscriber<T>(
        &self,
        channel: &str,
        start: ReplayStart,
    ) -> Result<Subscriber<T>, SdkError> {
        Ok(Subscriber {
            inner: self.inner.io.subscribe(channel, start)?,
            _marker: PhantomData,
        })
    }

    pub fn byte_subscriber(
        &self,
        channel: &str,
        start: ReplayStart,
    ) -> Result<ByteSubscriber, SdkError> {
        Ok(ByteSubscriber {
            inner: self.inner.io.subscribe(channel, start)?,
        })
    }

    pub fn replay_bytes(
        &self,
        channel: &str,
        start: ReplayStart,
        limit: usize,
    ) -> Result<Vec<Vec<u8>>, SdkError> {
        Ok(self
            .inner
            .io
            .replay(channel, start, limit)?
            .into_iter()
            .map(|frame| frame.payload)
            .collect())
    }
}

impl Default for Context {
    fn default() -> Self {
        Self::new()
    }
}

impl ContextBuilder {
    pub fn with_core_io(mut self, io: CoreIo) -> Self {
        self.io = Some(io);
        self
    }

    pub fn build(self) -> Context {
        Context {
            inner: Arc::new(ContextInner {
                io: self.io.unwrap_or_default(),
                runtime: RwLock::new(RuntimeSettings {
                    enforced_encoding: "rkyv".to_string(),
                }),
            }),
        }
    }
}

impl<T> Publisher<T>
where
    T: RkyvEncode,
{
    pub fn publish(&self, message: T) -> Result<PublishAck, SdkError> {
        let payload =
            encode_rkyv(&message).map_err(|err| CodecError::Serialize(err.to_string()))?;
        let mut headers = BTreeMap::new();
        headers.insert("content-type".to_string(), "application/rkyv".to_string());

        Ok(self
            .context
            .inner
            .io
            .publish(&self.channel, headers, payload)?)
    }
}

impl BytePublisher {
    pub fn publish(&self, payload: Vec<u8>) -> Result<PublishAck, SdkError> {
        Ok(self
            .context
            .inner
            .io
            .publish(&self.channel, BTreeMap::new(), payload)?)
    }
}

impl<T> Subscriber<T>
where
    T: Archive + Sized,
    for<'a> T::Archived: rkyv::Deserialize<T, HighDeserializer<rkyv::rancor::Error>>
        + rkyv::bytecheck::CheckBytes<HighValidator<'a, rkyv::rancor::Error>>,
{
    pub async fn recv(&mut self) -> Result<T, SdkError> {
        let frame = self.inner.recv().await?;
        decode_rkyv(&frame.payload).map_err(|err| CodecError::Deserialize(err.to_string()).into())
    }
}

impl ByteSubscriber {
    pub async fn recv(&mut self) -> Result<Vec<u8>, SdkError> {
        let frame = self.inner.recv().await?;
        Ok(frame.payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rkyv::{Archive, Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
    #[rkyv(bytecheck())]
    struct DemoEvent {
        id: u64,
    }

    #[test]
    fn typed_publish_replay_round_trip() {
        let context = Context::new();
        context
            .create_channel(
                "demo.events",
                ChannelKind::Event,
                RetentionPolicy::default(),
            )
            .expect("create channel");

        let publisher = context.publisher::<DemoEvent>("demo.events");
        publisher.publish(DemoEvent { id: 7 }).expect("publish");

        let replay = context
            .replay_bytes("demo.events", ReplayStart::Earliest, 10)
            .expect("replay");
        let value: DemoEvent = decode_rkyv(&replay[0]).expect("decode");
        assert_eq!(value.id, 7);
    }

    #[test]
    fn runtime_encoding_defaults_to_rkyv() {
        let context = Context::new();
        assert_eq!(context.runtime_settings().enforced_encoding, "rkyv");
    }
}
