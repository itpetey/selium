//! Framed read/write wrappers for raw byte-stream readers and writers.
//!
//! Uses `tokio_util::codec::FramedRead` / `FramedWrite` internally, with a
//! [`FrameCodec`] that handles [`FrameHeader`](crate::io::FrameHeader) encoding
//! and decoding. `FramedRead<R>` and `FramedWrite<W>` add convenience methods
//! (`read_frame`, `write_frame`, `poll_ready`, `generation`) and upgrade/downgrade
//! support for strong/weak channel types.

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use bytes::{Buf, BytesMut};
use futures::{Sink, Stream};
use tokio_util::codec::{
    Decoder, Encoder, FramedRead as TokioFramedRead, FramedWrite as TokioFramedWrite,
};

use crate::io::{
    channels::{Reader, WeakReader, WeakWriter, Writer},
    error::{Error, Result},
    frame::FrameHeader,
};

/// Codec that decodes/encodes [`FrameHeader`] + payload frames over a raw
/// byte stream.
///
/// Implements [`tokio_util::codec::Decoder`] and [`Encoder`] so that
/// [`FramedRead`] and [`FramedWrite`] can wrap any [`tokio::io::AsyncRead`]
/// or [`tokio::io::AsyncWrite`] that transports raw frame bytes.
pub struct FrameCodec;

/// A framed reader that wraps a raw byte-stream reader to provide frame-level
/// read operations with [`FrameHeader`] decoding and tag extraction.
///
/// Internally uses `tokio_util::codec::FramedRead<R, FrameCodec>`.
pub struct FramedRead<R> {
    inner: TokioFramedRead<R, FrameCodec>,
    /// A frame that was peeked by `poll_ready` but not yet consumed.
    peeked: Option<(Vec<u8>, u32)>,
}

/// A framed writer that wraps a raw byte-stream writer to provide frame-level
/// write operations with [`FrameHeader`] encoding.
///
/// Internally uses `tokio_util::codec::FramedWrite<W, FrameCodec>`.
pub struct FramedWrite<W> {
    inner: TokioFramedWrite<W, FrameCodec>,
}

impl Decoder for FrameCodec {
    type Item = (Vec<u8>, u32); // (payload, tag)
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
        if src.len() < FrameHeader::ENCODED_SIZE {
            return Ok(None);
        }

        let header_bytes = &src[..FrameHeader::ENCODED_SIZE];
        let header = FrameHeader::decode(header_bytes)?;

        let frame_size = FrameHeader::ENCODED_SIZE + header.len as usize;
        if src.len() < frame_size {
            return Ok(None);
        }

        if !header.is_ready() {
            // Not yet committed — leave bytes in buffer and retry.
            return Ok(None);
        }

        let payload = src[FrameHeader::ENCODED_SIZE..frame_size].to_vec();
        src.advance(frame_size);

        Ok(Some((payload, header.tag)))
    }
}

impl Encoder<(Vec<u8>, u32)> for FrameCodec {
    type Error = Error;

    fn encode(&mut self, item: (Vec<u8>, u32), dst: &mut BytesMut) -> Result<()> {
        let (payload, tag) = item;
        let header = FrameHeader {
            len: payload.len() as u32,
            tag,
            flags: FrameHeader::FLAG_READY,
            _reserved: [0; 3],
        };
        dst.reserve(FrameHeader::ENCODED_SIZE + payload.len());
        dst.extend_from_slice(&header.encode());
        dst.extend_from_slice(&payload);
        Ok(())
    }
}

impl<R> FramedRead<R> {
    /// Creates a new `FramedRead` wrapping the given raw reader.
    pub fn new(reader: R) -> Self {
        Self {
            inner: TokioFramedRead::new(reader, FrameCodec),
            peeked: None,
        }
    }

    /// Returns a reference to the inner reader.
    pub fn inner(&self) -> &R {
        self.inner.get_ref()
    }

    /// Returns a mutable reference to the inner reader.
    pub fn inner_mut(&mut self) -> &mut R {
        self.inner.get_mut()
    }

    /// Consumes this `FramedRead` and returns the inner reader.
    pub fn into_inner(self) -> R {
        self.inner.into_inner()
    }
}

impl<R: crate::io::channels::reader::HasGeneration + tokio::io::AsyncRead + Unpin> FramedRead<R> {
    /// Returns the current generation counter from the underlying ring buffer.
    pub fn generation(&self) -> Result<u64> {
        self.inner.get_ref().generation()
    }

    /// Reads the next complete frame synchronously, returning `(payload, tag)`.
    ///
    /// Returns [`Error::BufferEmpty`] if no complete frame is immediately available.
    pub fn read_frame(&mut self) -> Result<(Vec<u8>, u32)> {
        // Return a previously peeked frame if present.
        if let Some(frame) = self.peeked.take() {
            return Ok(frame);
        }

        match poll_framed_read(&mut self.inner) {
            Poll::Ready(Some(item)) => item,
            Poll::Ready(None) => Err(Error::Terminated),
            Poll::Pending => Err(Error::BufferEmpty),
        }
    }

    /// Non-blocking check for frame readiness.
    ///
    /// Returns `Ok(true)` if a complete frame is immediately readable,
    /// `Ok(false)` if no frame is ready. The frame is buffered and will
    /// be returned by the next [`read_frame`](Self::read_frame) call.
    pub fn poll_ready(&mut self) -> Result<bool> {
        // If we already have a peeked frame, we're ready.
        if self.peeked.is_some() {
            return Ok(true);
        }

        // Try to read a frame from the inner reader.
        match poll_framed_read(&mut self.inner) {
            Poll::Ready(Some(Ok(frame))) => {
                self.peeked = Some(frame);
                Ok(true)
            }
            Poll::Ready(Some(Err(e))) => Err(e),
            Poll::Ready(None) => Ok(false),
            Poll::Pending => Ok(false),
        }
    }
}

// Upgrade/downgrade support
impl FramedRead<WeakReader> {
    /// Upgrade the inner weak reader to a strong reader.
    pub fn upgrade(self) -> Result<FramedRead<Reader>> {
        let weak = self.into_inner();
        let strong = weak.upgrade()?;
        Ok(FramedRead::new(strong))
    }
}

impl FramedRead<Reader> {
    /// Downgrade the inner strong reader to a weak reader.
    pub fn downgrade(self) -> FramedRead<WeakReader> {
        let strong = self.into_inner();
        let weak = strong.downgrade();
        FramedRead::new(weak)
    }
}

impl<W> FramedWrite<W> {
    /// Creates a new `FramedWrite` wrapping the given raw writer.
    pub fn new(writer: W) -> Self {
        Self {
            inner: TokioFramedWrite::new(writer, FrameCodec),
        }
    }

    /// Returns a reference to the inner writer.
    pub fn inner(&self) -> &W {
        self.inner.get_ref()
    }

    /// Returns a mutable reference to the inner writer.
    pub fn inner_mut(&mut self) -> &mut W {
        self.inner.get_mut()
    }

    /// Consumes this `FramedWrite` and returns the inner writer.
    pub fn into_inner(self) -> W {
        self.inner.into_inner()
    }

    /// Writes a framed payload with the given correlation tag synchronously.
    ///
    /// Encodes a [`FrameHeader`] with the payload length and tag, writes
    /// the frame to the underlying writer.
    pub fn write_frame(&mut self, payload: &[u8], tag: u32) -> Result<()>
    where
        W: tokio::io::AsyncWrite + Unpin,
    {
        if payload.len() > u32::MAX as usize {
            return Err(Error::InvalidFrame);
        }

        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        // Poll readiness (should always be ready since our writers don't
        // have backpressure at the codec level).
        match Pin::new(&mut self.inner).poll_ready(&mut cx) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(e)) => return Err(e),
            Poll::Pending => return Err(Error::BufferFull),
        }

        // Encode the item into the internal buffer.
        Pin::new(&mut self.inner).start_send((payload.to_vec(), tag))?;

        // Flush to the underlying writer.
        match Pin::new(&mut self.inner).poll_flush(&mut cx) {
            Poll::Ready(Ok(())) => Ok(()),
            Poll::Ready(Err(e)) => Err(e),
            Poll::Pending => Err(Error::BufferFull),
        }
    }
}

// Upgrade/downgrade support
impl FramedWrite<WeakWriter> {
    /// Upgrade the inner weak writer to a strong writer.
    pub fn upgrade(self) -> Result<FramedWrite<Writer>> {
        let weak = self.into_inner();
        let strong = weak.upgrade()?;
        Ok(FramedWrite::new(strong))
    }
}

impl FramedWrite<Writer> {
    /// Downgrade the inner strong writer to a weak writer.
    pub fn downgrade(self) -> FramedWrite<WeakWriter> {
        let strong = self.into_inner();
        let weak = strong.downgrade();
        FramedWrite::new(weak)
    }
}

/// Polls a `TokioFramedRead` once with a noop waker for synchronous use.
fn poll_framed_read<R>(
    framed: &mut TokioFramedRead<R, FrameCodec>,
) -> Poll<Option<Result<(Vec<u8>, u32)>>>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    Pin::new(framed).poll_next(&mut cx)
}
