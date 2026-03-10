//! Host-side Rust SDK for Selium publish, subscribe, and replay flows.
//!
//! Use `selium-sdk-rust` when your Rust code runs **outside** a Selium WASM
//! guest and still needs to interact with Selium channels. Typical users
//! include tests, sidecars, adapters, ingest tools, and operator utilities.
//!
//! If your code is running inside a Selium guest module, use `selium-guest`
//! instead. `selium-guest` exposes guest hostcalls and managed bindings inside
//! the runtime; this crate exposes a host/client [`Context`] for creating
//! channels, publishing messages, subscribing, and replaying retained frames.
//!
//! Typed publishers and subscribers use `rkyv`-encoded payloads. When you need
//! raw frame access instead, use [`BytePublisher`] and [`ByteSubscriber`].
//!
//! # Example
//!
//! ```
//! use rkyv::{Archive, Deserialize, Serialize};
//! use selium_abi::decode_rkyv;
//! use selium_io_core::ChannelKind;
//! use selium_io_durability::{ReplayStart, RetentionPolicy};
//! use selium_sdk_rust::Context;
//!
//! #[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
//! #[rkyv(bytecheck())]
//! struct DemoEvent {
//!     id: u64,
//! }
//!
//! let context = Context::new();
//! context.ensure_channel(
//!     "demo.events",
//!     ChannelKind::Event,
//!     RetentionPolicy::default(),
//! )?;
//!
//! context
//!     .publisher::<DemoEvent>("demo.events")
//!     .publish(DemoEvent { id: 7 })?;
//!
//! let replay = context.replay_bytes("demo.events", ReplayStart::Earliest, 1)?;
//! let event: DemoEvent = decode_rkyv(&replay[0])?;
//! assert_eq!(event.id, 7);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

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
/// Errors produced while encoding or decoding typed SDK payloads.
pub enum CodecError {
    /// Serializing a typed value into bytes failed.
    #[error("serialization failed: {0}")]
    Serialize(String),
    /// Deserializing a typed value from bytes failed.
    #[error("deserialization failed: {0}")]
    Deserialize(String),
}

#[derive(Debug, Error)]
/// Top-level error type for host-side SDK operations.
///
/// This wraps both transport/data-plane failures and typed codec failures.
pub enum SdkError {
    /// The underlying channel or transport operation failed.
    #[error(transparent)]
    DataPlane(#[from] DataPlaneError),
    /// Typed payload encoding or decoding failed.
    #[error(transparent)]
    Codec(#[from] CodecError),
}

#[derive(Clone)]
/// Entry point for host-side channel management, publishing, subscribing, and replay.
///
/// [`Context::new`] creates an in-memory SDK context that is convenient for tests
/// and local tools. Use [`Context::builder`] when you need to inject a specific
/// [`CoreIo`] implementation.
pub struct Context {
    inner: Arc<ContextInner>,
}

struct ContextInner {
    io: CoreIo,
    runtime: RwLock<RuntimeSettings>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Runtime-related settings recorded on a [`Context`].
pub struct RuntimeSettings {
    /// Name of the payload encoding policy the surrounding runtime expects.
    ///
    /// Typed publishers in this crate currently emit `rkyv` payloads.
    pub enforced_encoding: String,
}

impl Default for RuntimeSettings {
    fn default() -> Self {
        Self {
            enforced_encoding: "rkyv".to_string(),
        }
    }
}

/// Builder for constructing a [`Context`] with a custom [`CoreIo`].
///
/// Most callers can use [`Context::new`] or [`Context::builder`]. This builder
/// is mainly useful when tests or host-side tools need to inject a specific
/// transport configuration.
pub struct ContextBuilder {
    io: Option<CoreIo>,
}

/// Publishes typed messages to a channel using `rkyv` encoding.
///
/// Use this when both producer and consumer agree on a typed payload shape.
pub struct Publisher<T> {
    context: Context,
    channel: String,
    _marker: PhantomData<T>,
}

/// Receives typed messages from a channel by decoding `rkyv` payloads.
///
/// Construct instances with [`Context::subscriber`].
pub struct Subscriber<T> {
    inner: Subscription,
    _marker: PhantomData<T>,
}

/// Publishes raw bytes to a channel without applying a typed codec.
///
/// Use this for already-encoded frames or interoperability with non-Rust
/// producers.
pub struct BytePublisher {
    context: Context,
    channel: String,
}

/// Receives raw channel payloads as bytes.
///
/// Construct instances with [`Context::byte_subscriber`].
pub struct ByteSubscriber {
    inner: Subscription,
}

impl Context {
    /// Starts building a [`Context`] with optional custom I/O configuration.
    pub fn builder() -> ContextBuilder {
        ContextBuilder { io: None }
    }

    /// Creates a [`Context`] backed by the default in-memory [`CoreIo`].
    pub fn new() -> Self {
        Self::builder().build()
    }

    /// Returns a snapshot of the runtime settings currently stored on this context.
    pub fn runtime_settings(&self) -> RuntimeSettings {
        self.inner.runtime.read().clone()
    }

    /// Records the payload encoding policy expected by the surrounding runtime.
    ///
    /// This updates the metadata stored in [`RuntimeSettings`]. Typed publishers
    /// in this crate continue to emit `rkyv` payloads today.
    pub fn set_runtime_encoding(&self, encoding: impl Into<String>) {
        self.inner.runtime.write().enforced_encoding = encoding.into();
    }

    /// Creates a new channel and returns an error if it already exists.
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

    /// Ensures that a channel exists, creating it if needed.
    ///
    /// If the channel is already present, this validates that the existing
    /// [`ChannelKind`] matches the requested one.
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

    /// Creates a typed publisher for the given channel.
    ///
    /// Messages published through the returned [`Publisher`] are encoded with
    /// `rkyv` and tagged with the `application/rkyv` content type.
    pub fn publisher<T>(&self, channel: impl Into<String>) -> Publisher<T> {
        Publisher {
            context: self.clone(),
            channel: channel.into(),
            _marker: PhantomData,
        }
    }

    /// Creates a raw-byte publisher for the given channel.
    pub fn byte_publisher(&self, channel: impl Into<String>) -> BytePublisher {
        BytePublisher {
            context: self.clone(),
            channel: channel.into(),
        }
    }

    /// Subscribes to typed messages on a channel, starting from the requested replay point.
    ///
    /// Use [`ReplayStart`] to choose whether to begin from retained history or
    /// from the live tail of the stream.
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

    /// Subscribes to raw payloads on a channel, starting from the requested replay point.
    pub fn byte_subscriber(
        &self,
        channel: &str,
        start: ReplayStart,
    ) -> Result<ByteSubscriber, SdkError> {
        Ok(ByteSubscriber {
            inner: self.inner.io.subscribe(channel, start)?,
        })
    }

    /// Replays retained payloads from a channel as raw bytes.
    ///
    /// This is useful for backfilling host-side state before switching to live
    /// subscription.
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
    /// Injects a preconfigured [`CoreIo`] instance into the [`Context`].
    pub fn with_core_io(mut self, io: CoreIo) -> Self {
        self.io = Some(io);
        self
    }

    /// Builds the [`Context`].
    ///
    /// If no [`CoreIo`] was provided, the context uses the default in-memory transport.
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
    /// Encodes and publishes a typed message to the configured channel.
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
    /// Publishes a raw payload to the configured channel.
    ///
    /// ```
    /// use selium_io_core::ChannelKind;
    /// use selium_io_durability::RetentionPolicy;
    /// use selium_sdk_rust::Context;
    ///
    /// let context = Context::new();
    /// context.ensure_channel(
    ///     "demo.raw",
    ///     ChannelKind::Event,
    ///     RetentionPolicy::default(),
    /// )?;
    ///
    /// let ack = context.byte_publisher("demo.raw").publish(vec![1, 2, 3])?;
    /// assert_eq!(ack.sequence, 1);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
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
    /// Waits for the next message and decodes it into `T`.
    ///
    /// ```no_run
    /// # use rkyv::{Archive, Deserialize, Serialize};
    /// # use selium_io_core::ChannelKind;
    /// # use selium_io_durability::{ReplayStart, RetentionPolicy};
    /// # use selium_sdk_rust::Context;
    /// # #[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
    /// # #[rkyv(bytecheck())]
    /// # struct DemoEvent {
    /// #     id: u64,
    /// # }
    /// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
    /// let context = Context::new();
    /// context.ensure_channel(
    ///     "demo.events",
    ///     ChannelKind::Event,
    ///     RetentionPolicy::default(),
    /// )?;
    ///
    /// let mut subscriber =
    ///     context.subscriber::<DemoEvent>("demo.events", ReplayStart::Earliest)?;
    /// let event = subscriber.recv().await?;
    /// # let _ = event;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn recv(&mut self) -> Result<T, SdkError> {
        let frame = self.inner.recv().await?;
        decode_rkyv(&frame.payload).map_err(|err| CodecError::Deserialize(err.to_string()).into())
    }
}

impl ByteSubscriber {
    /// Waits for the next payload and returns its raw bytes.
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
