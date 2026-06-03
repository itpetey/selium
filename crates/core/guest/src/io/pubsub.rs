use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{Sink, Stream, future::BoxFuture};
use rkyv::{
    api::high::{HighDeserializer, HighValidator},
    rancor::Error as RancorError,
};
use selium_abi::{RkyvEncode, decode_rkyv, encode_rkyv};

use crate::{
    io::{
        Error, RingBuf,
        channels::{self, WeakReader, WeakWriter},
        error::Result,
        region::SIGNAL_SHARED_ID_OFFSET,
        ring_buf::round_capacity,
    },
    signal::Signal,
};

pub struct Publisher<T> {
    writer: WeakWriter,
    signal: Signal,
    shared_id: u64,
    capacity: u64,
    _t: PhantomData<T>,
}

pub struct Subscriber<T> {
    reader: WeakReader,
    signal: Signal,
    shared_id: u64,
    capacity: u64,
    observed_generation: u64,
    pending_wait: Option<BoxFuture<'static, crate::Result<u64>>>,
    _t: PhantomData<T>,
}

impl<T> Publisher<T> {
    pub fn create(capacity: u64) -> Result<Self> {
        let (ring, signal) = create_topic(capacity)?;
        let writer = writer_from_ring(&ring)?;
        Ok(Self {
            writer,
            signal,
            shared_id: ring.shared_id(),
            capacity: ring.capacity(),
            _t: PhantomData,
        })
    }

    pub fn attach(shared_id: u64, capacity: u64) -> Result<Self> {
        let (ring, signal) = attach_topic(shared_id, capacity)?;
        let writer = writer_from_ring(&ring)?;
        Ok(Self {
            writer,
            signal,
            shared_id: ring.shared_id(),
            capacity: ring.capacity(),
            _t: PhantomData,
        })
    }

    pub fn shared_id(&self) -> u64 {
        self.shared_id
    }

    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    pub fn writer_id(&self) -> u32 {
        self.writer.writer_id()
    }

    pub fn allocate_mutation_id(&self) -> Result<u64> {
        self.writer.allocate_mutation_id().map_err(|e| match e {
            channels::Error::Core(e) => e,
            other => Error::Guest(other.to_string()),
        })
    }
}

impl<T> Sink<T> for Publisher<T>
where
    T: RkyvEncode + Unpin,
{
    type Error = Error;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: T) -> Result<()> {
        let this = self.get_mut();
        let bytes = encode_rkyv(&item).map_err(|e| Error::Guest(format!("encode error: {e}")))?;
        this.writer.write(&bytes).map_err(|e| match e {
            channels::Error::ChannelFull => Error::BufferFull,
            channels::Error::ReservationContended => Error::ReservationContended,
            channels::Error::Core(e) => e,
            other => Error::Guest(other.to_string()),
        })?;
        this.signal
            .notify()
            .map_err(|e| Error::Guest(e.to_string()))?;
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl<T> Subscriber<T> {
    pub fn create(capacity: u64) -> Result<Self> {
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
            pending_wait: None,
            _t: PhantomData,
        })
    }

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
            pending_wait: None,
            _t: PhantomData,
        })
    }

    pub fn shared_id(&self) -> u64 {
        self.shared_id
    }

    pub fn capacity(&self) -> u64 {
        self.capacity
    }
}

impl<T> Stream for Subscriber<T>
where
    T: rkyv::Archive + Sized + Unpin,
    for<'a> T::Archived: rkyv::Deserialize<T, HighDeserializer<RancorError>>
        + rkyv::bytecheck::CheckBytes<HighValidator<'a, RancorError>>,
{
    type Item = Result<T>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        let has_frame = match this.reader.has_ready_frame() {
            Ok(ready) => ready,
            Err(e) => return Poll::Ready(Some(Err(map_channel_error(e)))),
        };

        if has_frame {
            this.pending_wait = None;
            let (payload, _tag) = match this.reader.read() {
                Ok(frame) => frame,
                Err(e) => return Poll::Ready(Some(Err(map_channel_error(e)))),
            };
            if !this.reader.has_ready_frame().unwrap_or(false)
                && let Ok(generation) = this.signal.generation()
            {
                this.observed_generation = generation;
            }
            match decode_rkyv(&payload) {
                Ok(value) => return Poll::Ready(Some(Ok(value))),
                Err(e) => {
                    return Poll::Ready(Some(Err(Error::Guest(format!("decode error: {e}")))));
                }
            }
        }

        if this.pending_wait.is_none() {
            let signal = this.signal.clone();
            let observed = this.observed_generation;
            this.pending_wait = Some(Box::pin(
                async move { signal.wait(observed, u64::MAX).await },
            ));
        }

        if let Some(fut) = this.pending_wait.as_mut() {
            match fut.as_mut().poll(cx) {
                Poll::Ready(Ok(generation)) => {
                    this.pending_wait = None;
                    this.observed_generation = generation;
                    cx.waker().wake_by_ref();
                }
                Poll::Ready(Err(e)) => {
                    this.pending_wait = None;
                    return Poll::Ready(Some(Err(Error::Guest(e.to_string()))));
                }
                Poll::Pending => return Poll::Pending,
            }
        }

        Poll::Pending
    }
}

pub fn create_pair<T>(capacity: u64) -> Result<(Publisher<T>, Subscriber<T>)> {
    let publisher = Publisher::create(capacity)?;
    let subscriber = Subscriber::attach(publisher.shared_id(), publisher.capacity())?;
    Ok((publisher, subscriber))
}

pub fn attach_pair<T>(shared_id: u64, capacity: u64) -> Result<(Publisher<T>, Subscriber<T>)> {
    let publisher = Publisher::attach(shared_id, capacity)?;
    let subscriber = Subscriber::attach(shared_id, capacity)?;
    Ok((publisher, subscriber))
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

fn create_topic(capacity: u64) -> Result<(RingBuf, Signal)> {
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

fn reader_from_ring(ring: &RingBuf) -> Result<WeakReader> {
    let tail = ring.read_next_tail()?;
    reader_from_ring_at(ring, tail)
}

fn reader_from_ring_at(ring: &RingBuf, start_pos: u64) -> Result<WeakReader> {
    Ok(WeakReader::new(ring.region().clone(), start_pos))
}

fn writer_from_ring(ring: &RingBuf) -> Result<WeakWriter> {
    ring.region().increment_writer_count()?;
    let writer_id = match ring.region().allocate_writer_id() {
        Ok(writer_id) => writer_id,
        Err(error) => {
            if let Err(_rollback_error) = ring.region().decrement_writer_count() {}
            return Err(error);
        }
    };
    Ok(WeakWriter::new(ring.region().clone(), writer_id, None))
}
