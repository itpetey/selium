use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{Sink, Stream};

use crate::{
    encoding::FlatMsg,
    io::{
        Error, RingBuf,
        channels::{BlockingReader, BlockingWriter, ChannelBackpressure, Reader, Writer},
        error::Result,
        framed::{FramedRead, FramedWrite},
        ring_buf::round_capacity,
    },
};

pub struct Publisher<T, W = Writer> {
    writer: FramedWrite<W>,
    region_id: u64,
    capacity: u64,
    _t: PhantomData<T>,
}

pub struct Subscriber<T, R = Reader> {
    reader: FramedRead<R>,
    region_id: u64,
    capacity: u64,
    last_generation: u64,
    _t: PhantomData<T>,
}

impl<T> Publisher<T, Writer> {
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

    pub fn attach(region_id: u64) -> Result<Self> {
        let ring = attach_topic(region_id)?;
        let writer = writer_from_ring(&ring)?;
        Ok(Self {
            writer,
            region_id: ring.region_id(),
            capacity: ring.capacity(),
            _t: PhantomData,
        })
    }
}

impl<T, W> Publisher<T, W> {
    pub fn region_id(&self) -> u64 {
        self.region_id
    }

    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    /// Publishes a typed message synchronously (convenience wrapper).
    pub fn publish(&mut self, item: &T) -> Result<()>
    where
        T: FlatMsg,
        W: tokio::io::AsyncWrite + Unpin,
    {
        let bytes = FlatMsg::encode(item);
        self.writer.write_frame(&bytes, 0)
    }
}

impl<T> Publisher<T, Writer> {
    pub fn writer_id(&self) -> u32 {
        self.writer.inner().writer_id()
    }

    pub fn allocate_mutation_id(&self) -> Result<u64> {
        self.writer.inner().allocate_mutation_id()
    }
}

impl<T> Publisher<T, BlockingWriter> {
    pub fn writer_id(&self) -> u32 {
        self.writer.inner().writer_id()
    }

    pub fn allocate_mutation_id(&self) -> Result<u64> {
        self.writer.inner().allocate_mutation_id()
    }
}

impl<T> Publisher<T, Writer> {
    /// Upgrade the inner non-blocking writer to a blocking writer.
    pub fn upgrade(self) -> Result<Publisher<T, BlockingWriter>> {
        let writer = self.writer.upgrade()?;
        Ok(Publisher {
            writer,
            region_id: self.region_id,
            capacity: self.capacity,
            _t: PhantomData,
        })
    }
}

impl<T> Publisher<T, BlockingWriter> {
    /// Downgrade the inner blocking writer to a non-blocking writer.
    pub fn downgrade(self) -> Publisher<T, Writer> {
        let writer = self.writer.downgrade();
        Publisher {
            writer,
            region_id: self.region_id,
            capacity: self.capacity,
            _t: PhantomData,
        }
    }
}

impl<T, W> Sink<T> for Publisher<T, W>
where
    T: FlatMsg + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    type Error = Error;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: T) -> Result<()> {
        let this = self.get_mut();
        let bytes = FlatMsg::encode(&item);
        this.writer.write_frame(&bytes, 0)?;
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl<T> Subscriber<T, Reader> {
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

    pub fn attach(region_id: u64) -> Result<Self> {
        let ring = attach_topic(region_id)?;
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
}

impl<T, R> Subscriber<T, R> {
    pub fn region_id(&self) -> u64 {
        self.region_id
    }

    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    /// Reads the next raw message with its writer_id (tag).
    ///
    /// Returns `(decoded_message, writer_id)`. This is used by pattern crates
    /// like `selium-tables` that need to identify which publisher wrote a message.
    pub fn read_with_writer_id(&mut self) -> Result<(T, u32)>
    where
        T: FlatMsg,
        R: crate::io::channels::reader::HasGeneration + tokio::io::AsyncRead + Unpin,
    {
        let (payload, writer_id) = self.reader.read_frame()?;
        // Update last_generation after successful read.
        self.last_generation = self.reader.generation()?;
        let value: T =
            FlatMsg::decode(&payload).map_err(|e| Error::SerializationFailed(format!("{e}")))?;
        Ok((value, writer_id))
    }
}

impl<T> Subscriber<T, Reader> {
    /// Upgrade the inner non-blocking reader to a blocking reader.
    pub fn upgrade(self) -> Result<Subscriber<T, BlockingReader>> {
        let reader = self.reader.upgrade()?;
        Ok(Subscriber {
            reader,
            region_id: self.region_id,
            capacity: self.capacity,
            last_generation: self.last_generation,
            _t: PhantomData,
        })
    }
}

impl<T> Subscriber<T, BlockingReader> {
    /// Downgrade the inner blocking reader to a non-blocking reader.
    pub fn downgrade(self) -> Subscriber<T, Reader> {
        let reader = self.reader.downgrade();
        Subscriber {
            reader,
            region_id: self.region_id,
            capacity: self.capacity,
            last_generation: self.last_generation,
            _t: PhantomData,
        }
    }
}

impl<T, R> Stream for Subscriber<T, R>
where
    T: FlatMsg + Unpin,
    R: crate::io::channels::reader::HasGeneration + tokio::io::AsyncRead + Unpin,
{
    type Item = Result<T>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        let has_frame = match this.reader.poll_ready() {
            Ok(ready) => ready,
            Err(Error::BufferEmpty) => false,
            Err(e) => return Poll::Ready(Some(Err(e))),
        };

        if has_frame {
            let (payload, _tag) = match this.reader.read_frame() {
                Ok(frame) => frame,
                Err(Error::BufferEmpty) => {
                    // Spurious readiness; yield and retry.
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                Err(e) => return Poll::Ready(Some(Err(e))),
            };
            // Update last_generation after successful read.
            this.last_generation = match this.reader.generation() {
                Ok(g) => g,
                Err(e) => return Poll::Ready(Some(Err(e))),
            };
            match FlatMsg::decode(&payload) {
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

pub fn attach_pair<T>(region_id: u64) -> Result<(Publisher<T>, Subscriber<T>)> {
    let publisher = Publisher::attach(region_id)?;
    let subscriber = Subscriber::attach(region_id)?;
    Ok((publisher, subscriber))
}

pub fn create_pair<T>(capacity: u64) -> Result<(Publisher<T>, Subscriber<T>)> {
    let publisher = Publisher::create(capacity)?;
    let subscriber = Subscriber::attach(publisher.region_id())?;
    Ok((publisher, subscriber))
}

fn attach_topic(region_id: u64) -> Result<RingBuf> {
    RingBuf::attach(region_id)
}

fn create_topic(capacity: u64) -> Result<RingBuf> {
    let capacity = round_capacity(capacity)?;
    RingBuf::create(capacity, selium_abi::ResourceKind::PubSubTopic)
}

fn reader_from_ring(ring: &RingBuf) -> Result<FramedRead<Reader>> {
    let tail = ring.read_next_tail()?;
    let reader = Reader::new(ring.region().clone(), tail, ChannelBackpressure::Park);
    Ok(FramedRead::new(reader))
}

fn writer_from_ring(ring: &RingBuf) -> Result<FramedWrite<Writer>> {
    let writer_id = ring.region().allocate_writer_id()?;
    let writer = Writer::new(ring.region().clone(), writer_id, ChannelBackpressure::Park);
    Ok(FramedWrite::new(writer))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Creates a publisher/subscriber pair sharing the same underlying ring.
    /// In native test mode, `create_pair` doesn't share memory because
    /// `ChannelRegion::attach` looks up the native registry. This helper
    /// constructs both from the same ring directly.
    fn test_pair<T>(capacity: u64) -> Result<(Publisher<T, Writer>, Subscriber<T, Reader>)> {
        let capacity = round_capacity(capacity)?;
        let ring = RingBuf::create(capacity, selium_abi::ResourceKind::PubSubTopic)?;
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
        assert!(
            matches!(result, Err(Error::Overwritten)),
            "expected Overwritten, got {result:?}"
        );

        // Verify the generation counter actually advanced.
        let gen_now = subscriber.reader.inner().generation().expect("gen");
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
