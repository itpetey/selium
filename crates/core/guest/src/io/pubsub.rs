use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{Sink, Stream};
use rkyv::{
    api::high::{HighDeserializer, HighValidator},
    rancor::Error as RancorError,
};
use selium_abi::{RkyvEncode, decode_rkyv, encode_rkyv};

use crate::io::{
    Error, RingBuf,
    channels::{WeakReader, WeakWriter},
    error::Result,
    ring_buf::round_capacity,
};

pub struct Publisher<T> {
    writer: WeakWriter,
    region_id: u64,
    capacity: u64,
    _t: PhantomData<T>,
}

pub struct Subscriber<T> {
    reader: WeakReader,
    region_id: u64,
    capacity: u64,
    last_generation: u64,
    _t: PhantomData<T>,
}

impl<T> Publisher<T> {
    pub fn create(capacity: u64) -> Result<Self> {
        let ring = create_topic(capacity)?;
        let writer = writer_from_ring(&ring)?;
        Ok(Self {
            writer,
            region_id: ring.region_id(),
            capacity: ring.capacity(),
            _t: PhantomData,
        })
    }

    pub fn attach(region_id: u64, capacity: u64) -> Result<Self> {
        let ring = attach_topic(region_id, capacity)?;
        let writer = writer_from_ring(&ring)?;
        Ok(Self {
            writer,
            region_id: ring.region_id(),
            capacity: ring.capacity(),
            _t: PhantomData,
        })
    }

    pub fn region_id(&self) -> u64 {
        self.region_id
    }

    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    pub fn writer_id(&self) -> u32 {
        self.writer.writer_id()
    }

    pub fn allocate_mutation_id(&self) -> Result<u64> {
        self.writer.allocate_mutation_id()
    }

    /// Publishes a typed message synchronously (convenience wrapper around Sink).
    pub fn publish(&mut self, item: &T) -> Result<()>
    where
        T: RkyvEncode,
    {
        let bytes = encode_rkyv(item).map_err(|e| Error::SerializationFailed(format!("{e}")))?;
        self.writer.write(&bytes)
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
        let bytes = encode_rkyv(&item).map_err(|e| Error::SerializationFailed(format!("{e}")))?;
        this.writer.write(&bytes)?;
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
        let ring = create_topic(capacity)?;
        let reader = reader_from_ring(&ring)?;
        let last_generation = ring.generation().unwrap_or(0);
        Ok(Self {
            reader,
            region_id: ring.region_id(),
            capacity: ring.capacity(),
            last_generation,
            _t: PhantomData,
        })
    }

    pub fn attach(region_id: u64, capacity: u64) -> Result<Self> {
        let ring = attach_topic(region_id, capacity)?;
        let reader = reader_from_ring(&ring)?;
        let last_generation = ring.generation().unwrap_or(0);
        Ok(Self {
            reader,
            region_id: ring.region_id(),
            capacity: ring.capacity(),
            last_generation,
            _t: PhantomData,
        })
    }

    pub fn region_id(&self) -> u64 {
        self.region_id
    }

    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    /// Checks whether the publisher has overwritten data past the ring capacity.
    ///
    /// Computes `delta = current_generation - last_generation`. If `delta > capacity`,
    /// the subscriber's read position has been overwritten and data is lost.
    fn check_overwritten(&self) -> Result<()> {
        let current = self.reader.generation()?;
        let delta = current.wrapping_sub(self.last_generation);
        if delta > self.capacity {
            return Err(Error::Overwritten);
        }
        Ok(())
    }

    /// Reads the next raw message with its writer_id (tag).
    ///
    /// Returns `(decoded_message, writer_id)`. This is used by pattern crates
    /// like `selium-tables` that need to identify which publisher wrote a message.
    pub fn read_with_writer_id(&mut self) -> Result<(T, u32)>
    where
        T: rkyv::Archive + Sized,
        for<'a> T::Archived: rkyv::Deserialize<T, HighDeserializer<RancorError>>
            + rkyv::bytecheck::CheckBytes<HighValidator<'a, RancorError>>,
    {
        self.check_overwritten()?;
        let (payload, writer_id) = self.reader.read()?;
        // Update last_generation after successful read.
        self.last_generation = self.reader.generation()?;
        let value: T =
            decode_rkyv(&payload).map_err(|e| Error::SerializationFailed(format!("{e}")))?;
        Ok((value, writer_id))
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

        // Check for overwritten data before attempting to read.
        if let Err(e) = this.check_overwritten() {
            return Poll::Ready(Some(Err(e)));
        }

        let has_frame = match this.reader.poll_ready() {
            Ok(ready) => ready,
            Err(Error::BufferEmpty) => false,
            Err(e) => return Poll::Ready(Some(Err(e))),
        };

        if has_frame {
            let (payload, _tag) = match this.reader.read() {
                Ok(frame) => frame,
                Err(Error::BufferEmpty) => {
                    // Spurious readiness; yield and retry.
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                Err(e) => return Poll::Ready(Some(Err(e))),
            };
            // Update last_generation after successful read.
            // Propagate the error: if generation() fails, the shared region is
            // in a bad state and the subscriber should surface it rather than
            // continuing with stale overwrite-detection state.
            this.last_generation = match this.reader.generation() {
                Ok(g) => g,
                Err(e) => return Poll::Ready(Some(Err(e))),
            };
            match decode_rkyv(&payload) {
                Ok(value) => return Poll::Ready(Some(Ok(value))),
                Err(e) => {
                    return Poll::Ready(Some(Err(Error::SerializationFailed(format!("{e}")))));
                }
            }
        }

        // No data available. In WASM mode this would use memory.atomic.wait32
        // on the generation counter. In native mode, yield and let the runtime
        // re-poll.
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

pub fn attach_pair<T>(region_id: u64, capacity: u64) -> Result<(Publisher<T>, Subscriber<T>)> {
    let publisher = Publisher::attach(region_id, capacity)?;
    let subscriber = Subscriber::attach(region_id, capacity)?;
    Ok((publisher, subscriber))
}

pub fn create_pair<T>(capacity: u64) -> Result<(Publisher<T>, Subscriber<T>)> {
    let publisher = Publisher::create(capacity)?;
    let subscriber = Subscriber::attach(publisher.region_id(), publisher.capacity())?;
    Ok((publisher, subscriber))
}

fn attach_topic(region_id: u64, capacity: u64) -> Result<RingBuf> {
    RingBuf::attach(region_id, capacity)
}

fn create_topic(capacity: u64) -> Result<RingBuf> {
    let capacity = round_capacity(capacity)?;
    RingBuf::create(capacity)
}

fn reader_from_ring(ring: &RingBuf) -> Result<WeakReader> {
    let tail = ring.read_next_tail()?;
    Ok(WeakReader::new(ring.region().clone(), tail))
}

fn writer_from_ring(ring: &RingBuf) -> Result<WeakWriter> {
    ring.region().increment_writer_count()?;
    let writer_id = match ring.region().allocate_writer_id() {
        Ok(writer_id) => writer_id,
        Err(error) => {
            let _ = ring.region().decrement_writer_count();
            return Err(error);
        }
    };
    Ok(WeakWriter::new(ring.region().clone(), writer_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Creates a publisher/subscriber pair sharing the same underlying ring.
    /// In native test mode, `create_pair` doesn't share memory because
    /// `RegionBuilder::attach` allocates a new heap region. This helper
    /// constructs both from the same ring directly.
    fn test_pair<T>(capacity: u64) -> Result<(Publisher<T>, Subscriber<T>)> {
        let capacity = round_capacity(capacity)?;
        let ring = RingBuf::create(capacity)?;
        let writer = writer_from_ring(&ring)?;
        let publisher = Publisher {
            writer,
            region_id: ring.region_id(),
            capacity: ring.capacity(),
            _t: PhantomData,
        };
        let reader = reader_from_ring(&ring)?;
        let last_generation = ring.generation().unwrap_or(0);
        let subscriber = Subscriber {
            reader,
            region_id: ring.region_id(),
            capacity: ring.capacity(),
            last_generation,
            _t: PhantomData,
        };
        Ok((publisher, subscriber))
    }

    #[test]
    fn overwrite_detected_when_publisher_advances_past_capacity() {
        let (mut publisher, mut subscriber) = test_pair::<u64>(64).expect("create pair");

        // Record the generation at subscription time.
        let gen_start = subscriber.last_generation;

        // Write enough frames to advance the generation counter past the ring
        // byte capacity (64). Each write bumps generation by 1.
        for i in 0..70u64 {
            publisher.publish(&i).expect("publish");
        }

        // The subscriber should detect that data was overwritten.
        let result = subscriber.read_with_writer_id();
        // The overwrite check runs before the read, so Overwritten is expected.
        // ReaderBehind is also acceptable if the weak reader detects the overrun first.
        assert!(
            matches!(result, Err(Error::Overwritten) | Err(Error::ReaderBehind)),
            "expected Overwritten or ReaderBehind, got {result:?}"
        );

        // Verify the generation counter actually advanced.
        let gen_now = subscriber.reader.generation().expect("gen");
        assert!(
            gen_now.wrapping_sub(gen_start) > subscriber.capacity,
            "generation delta should exceed capacity"
        );
    }

    #[test]
    fn normal_publishing_does_not_trigger_overwrite_detection() {
        let (mut publisher, mut subscriber) = test_pair::<u64>(64).expect("create pair");

        // Write a few frames (well within capacity).
        for i in 0..3u64 {
            publisher.publish(&i).expect("publish");
        }

        // Read all frames — should succeed without overwrite errors.
        for i in 0..3u64 {
            let (value, _writer_id) = subscriber.read_with_writer_id().expect("read");
            assert_eq!(value, i);
        }

        // Verify last_generation was updated after reads.
        assert!(
            subscriber.last_generation > 0,
            "last_generation should be updated after reads"
        );
    }

    #[test]
    fn poll_ready_returns_true_when_frame_available() {
        let (mut publisher, subscriber) = test_pair::<u64>(64).expect("create pair");

        // No frame yet — poll_ready should return false.
        let mut reader = subscriber.reader;
        assert_eq!(reader.poll_ready(), Ok(false));

        // Write a frame.
        publisher.publish(&42u64).expect("publish");

        // Now poll_ready should return true.
        assert_eq!(reader.poll_ready(), Ok(true));
    }

    #[test]
    fn poll_ready_returns_false_on_empty_ring() {
        let (_publisher, subscriber) = test_pair::<u64>(64).expect("create pair");
        let mut reader = subscriber.reader;
        assert_eq!(reader.poll_ready(), Ok(false));
    }
}
