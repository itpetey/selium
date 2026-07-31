//! Framed read/write wrappers over a [`MessageTransport`].
//!
//! Uses `tokio_util::codec::FramedRead` / `FramedWrite` internally, with a
//! [`FrameCodec`] that handles [`FrameHeader`] encoding and decoding.

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use bytes::{Buf, BytesMut};
use futures::{Sink, Stream};
use selium_memory::FrameHeader;
use tokio_util::codec::{
    Decoder, Encoder, FramedRead as TokioFramedRead, FramedWrite as TokioFramedWrite,
};

use crate::{
    MessageTransport,
    error::{Error, Result},
};

type FramePollResult = Poll<Option<Result<(Vec<u8>, u32)>>>;

/// Codec that decodes/encodes [`FrameHeader`] + payload frames over a raw
/// byte stream.
///
/// Implements [`tokio_util::codec::Decoder`] and [`Encoder`] so that
/// [`FramedRead`] and [`FramedWrite`] can wrap any [`MessageTransport`].
pub struct FrameCodec;

/// A framed reader that wraps a [`MessageTransport`] to provide frame-level
/// read operations with [`FrameHeader`] decoding and tag extraction.
pub struct FramedRead<M> {
    inner: TokioFramedRead<M, FrameCodec>,
    /// A frame that was peeked by `poll_ready` but not yet consumed.
    peeked: Option<(Vec<u8>, u32)>,
}

/// A framed writer that wraps a [`MessageTransport`] to provide frame-level
/// write operations with [`FrameHeader`] encoding.
pub struct FramedWrite<M> {
    inner: TokioFramedWrite<M, FrameCodec>,
}

impl Decoder for FrameCodec {
    type Item = (Vec<u8>, u32); // (payload, tag)
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
        if src.len() < FrameHeader::ENCODED_SIZE {
            return Ok(None);
        }

        let header_bytes = src
            .get(..FrameHeader::ENCODED_SIZE)
            .ok_or_else(|| Error::InvalidFrame("header bytes slice out of bounds".to_string()))?;
        let header = FrameHeader::decode(header_bytes)?;

        let frame_size = FrameHeader::ENCODED_SIZE + header.len as usize;
        if src.len() < frame_size {
            return Ok(None);
        }

        if !header.is_ready() {
            // Not yet committed — leave bytes in buffer and retry.
            return Ok(None);
        }

        let payload = src
            .get(FrameHeader::ENCODED_SIZE..frame_size)
            .ok_or_else(|| Error::InvalidFrame("payload bytes slice out of bounds".to_string()))?
            .to_vec();
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

impl<M: MessageTransport> FramedRead<M> {
    /// Creates a new `FramedRead` wrapping the given transport.
    pub fn new(transport: M) -> Self {
        Self {
            inner: TokioFramedRead::new(transport, FrameCodec),
            peeked: None,
        }
    }

    /// Returns a reference to the inner transport.
    pub fn inner(&self) -> &M {
        self.inner.get_ref()
    }

    /// Returns a mutable reference to the inner transport.
    pub fn inner_mut(&mut self) -> &mut M {
        self.inner.get_mut()
    }

    /// Consumes this `FramedRead` and returns the inner transport.
    pub fn into_inner(self) -> M {
        self.inner.into_inner()
    }

    /// Returns the current generation counter from the underlying transport.
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

    /// Non-blocking check for peer-closed state.
    pub fn poll_peer_closed(&mut self) -> Result<bool> {
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        match Pin::new(self.inner.get_mut()).poll_peer_closed(&mut cx) {
            Poll::Ready(Ok(closed)) => Ok(closed),
            Poll::Ready(Err(e)) => Err(map_transport_error(e)),
            Poll::Pending => Ok(false),
        }
    }
}

impl<M: MessageTransport> FramedWrite<M> {
    /// Creates a new `FramedWrite` wrapping the given transport.
    pub fn new(transport: M) -> Self {
        Self {
            inner: TokioFramedWrite::new(transport, FrameCodec),
        }
    }

    /// Returns a reference to the inner transport.
    pub fn inner(&self) -> &M {
        self.inner.get_ref()
    }

    /// Returns a mutable reference to the inner transport.
    pub fn inner_mut(&mut self) -> &mut M {
        self.inner.get_mut()
    }

    /// Consumes this `FramedWrite` and returns the inner transport.
    pub fn into_inner(self) -> M {
        self.inner.into_inner()
    }

    /// Writes a framed payload with the given correlation tag synchronously.
    ///
    /// Encodes a [`FrameHeader`] with the payload length and tag, writes
    /// the frame to the underlying transport.
    pub fn write_frame(&mut self, payload: &[u8], tag: u32) -> Result<()> {
        if payload.len() > u32::MAX as usize {
            return Err(Error::InvalidFrame(format!("payload length {} exceeds u32::MAX", payload.len())));
        }

        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        // Poll readiness.
        match Pin::new(&mut self.inner).poll_ready(&mut cx) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(e)) => return Err(map_transport_error(e)),
            Poll::Pending => return Err(Error::BufferFull),
        }

        // Encode the item into the internal buffer.
        Pin::new(&mut self.inner).start_send((payload.to_vec(), tag))?;

        // Flush to the underlying transport.
        match Pin::new(&mut self.inner).poll_flush(&mut cx) {
            Poll::Ready(Ok(())) => Ok(()),
            Poll::Ready(Err(e)) => Err(map_transport_error(e)),
            Poll::Pending => Err(Error::BufferFull),
        }
    }
}

fn map_transport_error<E: std::error::Error>(err: E) -> Error {
    if let Some(our_err) = err.source().and_then(|e| e.downcast_ref::<Error>()) {
        return our_err.clone();
    }
    Error::Transport(err.to_string())
}

/// Polls a `TokioFramedRead` once with a noop waker for synchronous use.
fn poll_framed_read<M>(framed: &mut TokioFramedRead<M, FrameCodec>) -> FramePollResult
where
    M: MessageTransport,
{
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    Pin::new(framed).poll_next(&mut cx)
}
