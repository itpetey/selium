//! Selium QUIC stream transport implementing [`selium_wire::MessageTransport`].
//!
//! This crate wraps a quinn bidirectional stream (`SendStream` + `RecvStream`)
//! as a [`MessageTransport`] so that the transport-agnostic patterns in
//! `selium-wire` (pub/sub, RPC) can run over QUIC streams.

use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};

use selium_wire::{
    MessageTransport,
    error::Result,
};
use tokio::io::{
    AsyncRead,
    AsyncWrite,
    ReadBuf,
};

/// A [`MessageTransport`] implementation over a single QUIC stream.
///
/// Frames written through this transport are sent as-is on the QUIC stream;
/// frames read are delivered verbatim. A bidirectional stream can be split
/// into separate read and write transports via [`Self::split`].
#[derive(Debug)]
pub struct QuicTransport {
    send: Option<quinn::SendStream>,
    recv: Option<quinn::RecvStream>,
    /// Bytes successfully received so far. Used as a generation counter so that
    /// `selium-wire` patterns know when new data may be available.
    read_bytes: u64,
    /// Set when the receive stream signals EOF or an error.
    recv_closed: bool,
}

impl QuicTransport {
    /// Creates a new bidirectional transport from a quinn send/receive stream pair.
    pub fn new(send: quinn::SendStream, recv: quinn::RecvStream) -> Self {
        Self {
            send: Some(send),
            recv: Some(recv),
            read_bytes: 0,
            recv_closed: false,
        }
    }

    /// Creates a read-only transport from a quinn receive stream.
    pub fn read_only(recv: quinn::RecvStream) -> Self {
        Self {
            send: None,
            recv: Some(recv),
            read_bytes: 0,
            recv_closed: false,
        }
    }

    /// Creates a write-only transport from a quinn send stream.
    pub fn write_only(send: quinn::SendStream) -> Self {
        Self {
            send: Some(send),
            recv: None,
            read_bytes: 0,
            recv_closed: true,
        }
    }

    /// Splits a bidirectional transport into separate read and write transports.
    ///
    /// # Panics
    ///
    /// Panics if this transport is missing either the send or receive half.
    pub fn split(mut self) -> (Self, Self) {
        let send = self.send.take().expect("QuicTransport send half missing");
        let recv = self.recv.take().expect("QuicTransport recv half missing");
        (Self::write_only(send), Self::read_only(recv))
    }

    /// Returns a reference to the underlying receive stream, if present.
    pub fn recv_stream(&self) -> Option<&quinn::RecvStream> {
        self.recv.as_ref()
    }

    /// Returns a reference to the underlying send stream, if present.
    pub fn send_stream(&self) -> Option<&quinn::SendStream> {
        self.send.as_ref()
    }

    /// Records a successful read of `n` bytes, advancing the generation counter.
    fn record_read(&mut self, n: usize) {
        if n > 0 {
            self.read_bytes = self.read_bytes.wrapping_add(n as u64);
        }
    }
}

impl AsyncRead for QuicTransport {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let Some(recv) = self.recv.as_mut() else {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "QuicTransport read half not available",
            )));
        };
        let before = buf.filled().len();
        let result = Pin::new(recv).poll_read(cx, buf);
        match &result {
            Poll::Ready(Ok(())) => {
                let after = buf.filled().len();
                let n = after.saturating_sub(before);
                if n == 0 && before < buf.capacity() {
                    // Zero bytes read into a non-empty buffer means EOF.
                    self.recv_closed = true;
                } else {
                    self.record_read(n);
                }
            }
            Poll::Ready(Err(_)) => self.recv_closed = true,
            Poll::Pending => {}
        }
        result
    }
}

impl AsyncWrite for QuicTransport {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let Some(send) = self.send.as_mut() else {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "QuicTransport write half not available",
            )));
        };
        AsyncWrite::poll_write(Pin::new(send), cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // Quinn send streams have no application-level flush.
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let Some(send) = self.send.as_mut() else {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "QuicTransport write half not available",
            )));
        };
        AsyncWrite::poll_shutdown(Pin::new(send), cx)
    }
}

impl MessageTransport for QuicTransport {
    type Error = io::Error;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<bool>> {
        // QUIC streams provide a byte stream; data availability is determined by
        // the underlying AsyncRead. Report readiness optimistically unless the
        // receive side is closed or missing.
        Poll::Ready(Ok(self.recv.is_some() && !self.recv_closed))
    }

    fn poll_peer_closed(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<bool>> {
        Poll::Ready(Ok(self.recv_closed))
    }

    fn generation(&self) -> Result<u64> {
        // QUIC has no native generation counter. Use the cumulative received
        // byte count so that polling loops see a change whenever new bytes
        // arrive.
        Ok(self.read_bytes)
    }
}
