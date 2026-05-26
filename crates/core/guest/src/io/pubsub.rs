use std::{cell::RefCell, marker::PhantomData};

use rkyv::{
    Deserialize,
    api::high::{HighDeserializer, HighValidator},
    bytecheck::CheckBytes,
    rancor::Error as RancorError,
};

use crate::{
    io::{
        Error, RingBuf,
        channels::{self, StrongReader, StrongWriter},
        error::Result,
        region::SIGNAL_SHARED_ID_OFFSET,
        ring_buf::round_capacity,
    },
    signal::Signal,
};

/// A codec that converts between typed values and raw bytes.
///
/// Implementations handle serialisation and deserialisation for use
/// with `TypedPublisher` and `TypedSubscriber`.
pub trait Codec {
    /// The application-level message type.
    type Item;
    /// Serialises `item` into a byte vector.
    fn encode(&self, item: &Self::Item) -> Result<Vec<u8>>;
    /// Deserialises a byte slice into a value.
    fn decode(&self, bytes: &[u8]) -> Result<Self::Item>;
}

/// A publisher that writes framed messages into a shared-memory topic.
pub struct Publisher {
    writer: RefCell<StrongWriter>,
    signal: Signal,
    shared_id: u64,
    capacity: u64,
}

/// A subscriber that reads framed messages from a shared-memory topic.
pub struct Subscriber {
    reader: StrongReader,
    signal: Signal,
    shared_id: u64,
    capacity: u64,
    observed_generation: u64,
}

/// An rkyv-backed codec that serialises without extra framing.
///
/// The ring buffer's `FrameHeader` already handles framing at the
/// shared memory layer, so this codec writes raw rkyv bytes directly
/// (unlike `selium_guest::codec` which adds a redundant length prefix).
pub struct RkyvCodec<T>(PhantomData<T>);

/// A typed publisher that serialises messages through a `Codec`.
pub struct TypedPublisher<C: Codec> {
    inner: Publisher,
    codec: C,
}

/// A typed subscriber that deserialises messages through a `Codec`.
pub struct TypedSubscriber<C: Codec> {
    inner: Subscriber,
    codec: C,
}

impl Publisher {
    /// Creates a new pub/sub topic.
    pub fn create(capacity: u32) -> Result<Self> {
        let (ring, signal) = create_topic(capacity)?;
        let writer = writer_from_ring(&ring)?;
        Ok(Self {
            writer,
            signal,
            shared_id: ring.shared_id(),
            capacity: ring.capacity(),
        })
    }

    /// Attaches to an existing pub/sub topic by shared region id.
    pub fn attach(shared_id: u64, capacity: u64) -> Result<Self> {
        let (ring, signal) = attach_topic(shared_id, capacity)?;
        let writer = writer_from_ring(&ring)?;
        Ok(Self {
            writer,
            signal,
            shared_id: ring.shared_id(),
            capacity: ring.capacity(),
        })
    }

    /// Publishes a raw byte payload to all subscribers.
    pub fn publish(&self, payload: &[u8]) -> Result<()> {
        self.writer
            .borrow_mut()
            .write(payload)
            .map_err(|e| match e {
                channels::Error::ChannelFull => Error::BufferFull,
                channels::Error::ReservationContended => Error::ReservationContended,
                channels::Error::Core(e) => e,
                other => Error::Guest(other.to_string()),
            })?;
        self.signal
            .notify()
            .map_err(|e| Error::Guest(e.to_string()))?;
        Ok(())
    }

    /// Returns the shared region id for attaching other publishers or subscribers.
    pub fn shared_id(&self) -> u64 {
        self.shared_id
    }

    /// Returns the ring capacity required when attaching to this topic.
    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    /// Returns the writer id stored in frames published by this publisher.
    pub fn writer_id(&self) -> u32 {
        self.writer.borrow().writer_id()
    }

    /// Allocates a globally unique mutation id for this topic.
    pub fn allocate_mutation_id(&self) -> Result<u64> {
        self.writer
            .borrow()
            .allocate_mutation_id()
            .map_err(|e| match e {
                channels::Error::Core(e) => e,
                other => Error::Guest(other.to_string()),
            })
    }
}

impl Subscriber {
    /// Creates a new pub/sub topic from the subscriber side.
    pub fn create(capacity: u32) -> Result<Self> {
        let (ring, signal) = create_topic(capacity)?;
        let reader = reader_from_ring(&ring)?;
        let observed_generation = signal
            .generation()
            .map_err(|e| Error::Guest(e.to_string()))?;
        Ok(Self {
            reader,
            signal,
            shared_id: ring.shared_id(),
            capacity: ring.capacity(),
            observed_generation,
        })
    }

    /// Attaches to an existing pub/sub topic by shared region id.
    pub fn attach(shared_id: u64, capacity: u64) -> Result<Self> {
        let (ring, signal) = attach_topic(shared_id, capacity)?;
        let reader = reader_from_ring(&ring)?;
        let observed_generation = signal
            .generation()
            .map_err(|e| Error::Guest(e.to_string()))?;
        Ok(Self {
            reader,
            signal,
            shared_id: ring.shared_id(),
            capacity: ring.capacity(),
            observed_generation,
        })
    }

    /// Reads the next available raw message. Returns `(payload, tag)`.
    pub fn read(&mut self) -> Result<(Vec<u8>, u32)> {
        let frame = self.reader.read().map_err(map_channel_error)?;
        if !self.reader.has_ready_frame().map_err(map_channel_error)? {
            self.observed_generation = self
                .signal
                .generation()
                .map_err(|e| Error::Guest(e.to_string()))?;
        }
        Ok(frame)
    }

    /// Waits for new data using the notification signal.
    pub async fn wait(&mut self, timeout_ms: u64) -> Result<()> {
        if self.reader.has_ready_frame().map_err(map_channel_error)? {
            return Ok(());
        }

        let generation = self
            .signal
            .wait(self.observed_generation, timeout_ms)
            .await
            .map_err(|e| Error::Guest(e.to_string()))?;
        self.observed_generation = generation;
        Ok(())
    }

    /// Returns the shared region id for attaching other publishers or subscribers.
    pub fn shared_id(&self) -> u64 {
        self.shared_id
    }

    /// Returns the ring capacity required when attaching to this topic.
    pub fn capacity(&self) -> u64 {
        self.capacity
    }
}

impl<T> RkyvCodec<T> {
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<T> Codec for RkyvCodec<T>
where
    T: selium_abi::RkyvEncode + Sized,
    for<'a> T::Archived:
        Deserialize<T, HighDeserializer<RancorError>> + CheckBytes<HighValidator<'a, RancorError>>,
{
    type Item = T;

    fn encode(&self, item: &T) -> Result<Vec<u8>> {
        selium_abi::encode_rkyv(item).map_err(|e| Error::Guest(e.to_string()))
    }

    fn decode(&self, bytes: &[u8]) -> Result<T> {
        selium_abi::decode_rkyv(bytes).map_err(|e| Error::Guest(e.to_string()))
    }
}

impl<T> Default for RkyvCodec<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<C: Codec + Default> TypedPublisher<C> {
    /// Creates a new typed pub/sub topic.
    pub fn create(capacity: u32) -> Result<Self> {
        let inner = Publisher::create(capacity)?;
        Ok(Self {
            inner,
            codec: C::default(),
        })
    }

    /// Attaches to an existing typed pub/sub topic.
    pub fn attach(shared_id: u64, capacity: u64) -> Result<Self> {
        let inner = Publisher::attach(shared_id, capacity)?;
        Ok(Self {
            inner,
            codec: C::default(),
        })
    }
}

impl<C: Codec> TypedPublisher<C> {
    /// Publishes a typed message to all subscribers.
    pub fn publish(&self, msg: &C::Item) -> Result<()> {
        let bytes = self.codec.encode(msg)?;
        self.inner.publish(&bytes)
    }

    /// Returns the shared region id for attaching other typed endpoints.
    pub fn shared_id(&self) -> u64 {
        self.inner.shared_id()
    }

    /// Returns the ring capacity required when attaching to this topic.
    pub fn capacity(&self) -> u64 {
        self.inner.capacity()
    }

    /// Returns the writer id stored in frames published by this publisher.
    pub fn writer_id(&self) -> u32 {
        self.inner.writer_id()
    }

    /// Allocates a globally unique mutation id for this topic.
    pub fn allocate_mutation_id(&self) -> Result<u64> {
        self.inner.allocate_mutation_id()
    }
}

impl<C: Codec> TypedPublisher<C> {
    pub(crate) fn from_raw(inner: Publisher, codec: C) -> Self {
        Self { inner, codec }
    }
}

impl<C: Codec + Default> TypedSubscriber<C> {
    /// Creates a new typed pub/sub topic from the subscriber side.
    pub fn create(capacity: u32) -> Result<Self> {
        let inner = Subscriber::create(capacity)?;
        Ok(Self {
            inner,
            codec: C::default(),
        })
    }

    /// Attaches to an existing typed pub/sub topic.
    pub fn attach(shared_id: u64, capacity: u64) -> Result<Self> {
        let inner = Subscriber::attach(shared_id, capacity)?;
        Ok(Self {
            inner,
            codec: C::default(),
        })
    }
}

impl<C: Codec> TypedSubscriber<C> {
    /// Reads and deserialises the next available message.
    pub fn read(&mut self) -> Result<C::Item> {
        let (bytes, _writer_id) = self.inner.read()?;
        self.codec.decode(&bytes)
    }

    /// Reads and deserialises the next available message with its writer id.
    pub fn read_with_writer_id(&mut self) -> Result<(C::Item, u32)> {
        let (bytes, tag) = self.inner.read()?;
        let item = self.codec.decode(&bytes)?;
        Ok((item, tag))
    }

    /// Waits for new data using the notification signal.
    pub async fn wait(&mut self, timeout_ms: u64) -> Result<()> {
        self.inner.wait(timeout_ms).await
    }

    /// Returns the shared region id for attaching other typed endpoints.
    pub fn shared_id(&self) -> u64 {
        self.inner.shared_id()
    }

    /// Returns the ring capacity required when attaching to this topic.
    pub fn capacity(&self) -> u64 {
        self.inner.capacity()
    }
}

impl<C: Codec> TypedSubscriber<C> {
    pub(crate) fn from_raw(inner: Subscriber, codec: C) -> Self {
        Self { inner, codec }
    }
}

pub(crate) fn attach_pair(shared_id: u64, capacity: u64) -> Result<(Publisher, Subscriber)> {
    let (ring, signal) = attach_topic(shared_id, capacity)?;
    let tail = ring.read_next_tail()?;
    if tail > ring.capacity() {
        return Err(Error::ReaderBehind);
    }
    let writer = writer_from_ring(&ring)?;
    let publisher_signal = attach_signal(&signal)?;
    let publisher = Publisher {
        writer,
        signal: publisher_signal,
        shared_id: ring.shared_id(),
        capacity: ring.capacity(),
    };
    let reader = reader_from_ring_at(&ring, 0)?;
    let observed_generation = signal
        .generation()
        .map_err(|e| Error::Guest(e.to_string()))?;
    let subscriber = Subscriber {
        reader,
        signal,
        shared_id: ring.shared_id(),
        capacity: ring.capacity(),
        observed_generation,
    };
    Ok((publisher, subscriber))
}

pub(crate) fn create_pair() -> Result<(Publisher, Subscriber)> {
    // Default capacity of 64 KB for table topics
    let (ring, signal) = create_topic(64 * 1024)?;
    let writer = writer_from_ring(&ring)?;
    let publisher_signal = attach_signal(&signal)?;
    let publisher = Publisher {
        writer,
        signal: publisher_signal,
        shared_id: ring.shared_id(),
        capacity: ring.capacity(),
    };
    let reader = reader_from_ring_at(&ring, 0)?;
    let observed_generation = signal
        .generation()
        .map_err(|e| Error::Guest(e.to_string()))?;
    let subscriber = Subscriber {
        reader,
        signal,
        shared_id: ring.shared_id(),
        capacity: ring.capacity(),
        observed_generation,
    };
    Ok((publisher, subscriber))
}

fn attach_signal(signal: &Signal) -> Result<Signal> {
    Signal::attach(signal.shared_id()).map_err(|e| Error::Guest(e.to_string()))
}

fn attach_topic(shared_id: u64, capacity: u64) -> Result<(RingBuf, Signal)> {
    let ring = RingBuf::attach(shared_id, capacity)?;
    let signal_shared_id = ring
        .region()
        .read_header_u64(SIGNAL_SHARED_ID_OFFSET)
        .map_err(|_invalid_layout| Error::InvalidLayout)?;
    let signal = Signal::attach(signal_shared_id).map_err(|e| Error::Guest(e.to_string()))?;
    Ok((ring, signal))
}

fn create_topic(capacity: u32) -> Result<(RingBuf, Signal)> {
    let capacity = round_capacity(capacity)?;
    RingBuf::create(capacity)
}

fn map_channel_error(error: channels::Error) -> Error {
    match error {
        channels::Error::ChannelEmpty => Error::BufferEmpty,
        channels::Error::ReaderBehind => Error::ReaderBehind,
        channels::Error::ReservationContended => Error::ReservationContended,
        channels::Error::InvalidFrame => Error::InvalidFrame,
        channels::Error::Core(error) => error,
        other => Error::Guest(other.to_string()),
    }
}

fn reader_from_ring(ring: &RingBuf) -> Result<StrongReader> {
    let tail = ring.read_next_tail()?;
    reader_from_ring_at(ring, tail)
}

fn reader_from_ring_at(ring: &RingBuf, start_pos: u64) -> Result<StrongReader> {
    let reader_id = ring.region().allocate_reader_slot(start_pos)?;
    Ok(StrongReader::new(
        ring.region().clone(),
        start_pos,
        reader_id,
    ))
}

fn writer_from_ring(ring: &RingBuf) -> Result<RefCell<StrongWriter>> {
    ring.region().increment_writer_count()?;
    let writer_id = match ring.region().allocate_writer_id() {
        Ok(writer_id) => writer_id,
        Err(error) => {
            if let Err(_rollback_error) = ring.region().decrement_writer_count() {}
            return Err(error);
        }
    };
    Ok(RefCell::new(StrongWriter::new(
        ring.region().clone(),
        writer_id,
        None,
    )))
}
