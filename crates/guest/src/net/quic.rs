//! Byte-transport QUIC serve API for application guests.
//!
//! This module is the app-guest side of the QUIC connector: register a
//! `sel-quic://<name>` URI subtree with discovery and accept per-stream byte
//! channels from the connector, then frame the bytes with any user schema.
//!
//! ## Capability model
//!
//! App guests served by the QUIC connector require **zero `Network` grants**
//! and **zero quinn dependency**. QUIC is terminated at the edge by the
//! connector, and only capability-gated shared-memory byte channels reach the
//! app guest. The entire attack surface is channel attach.
//!
//! The recommended grant is [`ExplicitResource`](selium_abi::CapabilityGrant)
//! scoped to each per-stream channel region. Broad shared-memory `UriPrefix`
//! grants widen exposure to *every* connector-served channel and are
//! documented here as an anti-pattern: each stream's channel SHOULD carry its
//! own `ExplicitResource` grant so streams on one connection cannot attach to
//! another stream's region (see `selium-runtime`'s `quic_connector` substrate
//! tests).
//!
//! [`ExplicitResource`]: selium_abi::ResourceSelector::ExplicitResource
//!
//! ## Example
//!
//! ```ignore
//! use selium_guest::{net::quic::QuicServe, entrypoint, Context};
//! use tokio::io::{AsyncReadExt, AsyncWriteExt};
//!
//! #[entrypoint]
//! async fn my_app(mut ctx: Context) {
//!     let mut serve = QuicServe::bind(&mut ctx, "sel-quic://my-app")
//!         .await
//!         .expect("bind failed");
//!
//!     while let Ok(mut stream) = serve.accept().await {
//!         let mut buf = vec![0u8; 1024];
//!         let n = stream.read(&mut buf).await.expect("read");
//!         stream.write_all(&buf[..n]).await.expect("echo");
//!         drop(stream);
//!     }
//! }
//! ```

use std::{
    pin::Pin,
    task::{Context as TaskContext, Poll},
};

use super::bytes::ByteStream;
use selium_abi::{InterfaceMetadata, ResourceTarget, uri};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::{Context, GuestError, ResourceListener};

/// Protocol scheme for QUIC routes (`sel-quic://…`).
pub const QUIC_SCHEME: &str = "sel-quic";
/// Interface marker registered by app guests that serve QUIC byte channels.
pub const QUIC_STREAM_INTERFACE: &str = "selium.quic/stream";

/// A byte-transport QUIC serve handle.
///
/// Wraps a [`ResourceListener`] and a discovery registration for a
/// `sel-quic://<name>` URI. Each accepted stream is a [`QuicStream`] byte
/// channel from the connector.
pub struct QuicServe {
    listener: ResourceListener,
    uri: String,
}

/// A single per-stream byte channel from the QUIC connector.
///
/// Presents the relayed stream as `AsyncRead` + `AsyncWrite`: bytes read are
/// the external client's bytes (in order), and bytes written are relayed back
/// to the client. Zero `Network` grants are required — only the channel attach
/// grant for this stream's region.
pub struct QuicStream {
    inner: ByteStream,
}

/// Errors that can occur while serving QUIC byte channels.
#[derive(Debug, Error)]
pub enum QuicServeError {
    /// Failed to accept an incoming stream.
    #[error("accept: {0}")]
    Accept(String),
    /// The remote (connector) closed the listener.
    #[error("listener closed")]
    Closed,
}

impl QuicServe {
    /// Binds to a `sel-quic://<name>` URI and registers it with discovery.
    ///
    /// The `uri` must be protocol-aware: `sel-quic://<name>` (e.g.
    /// `sel-quic://my-app`). The runtime allocates a host queue for the
    /// listener and registers the URI→queue mapping with discovery.
    ///
    /// The guest requires a channel attach grant but **no `Network` grant** —
    /// QUIC is terminated and relayed by the connector.
    pub async fn bind(ctx: &mut Context, uri: &str) -> Result<Self, GuestError> {
        require_quic_scheme(uri)?;

        let listener = ResourceListener::create()
            .map_err(|e| GuestError::Host(format!("create listener: {e}")))?;

        let target = quic_target(&listener, uri);
        ctx.register(uri, target).await?;

        Ok(Self {
            listener,
            uri: uri.to_string(),
        })
    }

    /// Accepts the next delivered stream region from the connector.
    ///
    /// Attaches the delivered two-ring region as a [`QuicStream`] byte
    /// channel. The connector delivers one region per accepted bidirectional
    /// QUIC stream.
    pub async fn accept(&mut self) -> Result<QuicStream, QuicServeError> {
        let incoming = self
            .listener
            .recv()
            .await
            .map_err(|e| QuicServeError::Accept(format!("recv: {e}")))?;

        let stream = ByteStream::attach_blocking(incoming.shared_id)
            .map_err(|e| QuicServeError::Accept(format!("attach stream: {e}")))?;

        Ok(QuicStream { inner: stream })
    }

    /// Returns the URI subtree this handle is bound to.
    pub fn uri(&self) -> &str {
        &self.uri
    }
}

impl QuicStream {
    /// Builds a `QuicStream` from a delivered region's shared id.
    ///
    /// Separate from [`ByteStream::attach`] because the blocking writer is
    /// required for peer-to-peer close semantics (the connector observes EOF
    /// when this stream drops its write half).
    pub fn from_shared_id(shared_id: u64) -> Result<Self, GuestError> {
        Ok(Self {
            inner: ByteStream::attach_blocking(shared_id)?,
        })
    }
}

impl AsyncRead for QuicStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for QuicStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

fn quic_target(listener: &ResourceListener, uri: &str) -> ResourceTarget {
    ResourceTarget {
        uri: uri.to_string(),
        host_id: String::new(),
        resource_id: listener.descriptor().shared_id,
        interface: Some(InterfaceMetadata {
            name: QUIC_STREAM_INTERFACE.to_string(),
            methods: Vec::new(),
        }),
        tenant: None,
    }
}

fn require_quic_scheme(uri: &str) -> Result<(), GuestError> {
    if uri::scheme_of(uri) == Some(QUIC_SCHEME) {
        Ok(())
    } else {
        Err(GuestError::Host(format!(
            "QUIC serve requires a `{QUIC_SCHEME}://` URI, got: {uri}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use selium_shm::byte_channel;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn setup() {
        drop(selium_memory::set_region_provider(Box::new(
            selium_memory::HeapRegionProvider::new(),
        )));
    }

    #[test]
    fn bind_requires_quic_scheme() {
        assert!(require_quic_scheme("sel-quic://my-app").is_ok());
        assert!(require_quic_scheme("sel-http://my-app").is_err());
        assert!(require_quic_scheme("my-app").is_err());
    }

    #[tokio::test]
    async fn quic_stream_round_trips_bytes_with_connector_peer() {
        setup();

        // Allocate a region pair the way the connector would, then attach the
        // app-guest half as a QuicStream.
        let (ring_to_guest, ring_from_guest, shared_id, _region) =
            byte_channel::create(4096, 4096).expect("create");
        let region = selium_memory::region_provider()
            .expect("provider")
            .attach(shared_id, None, selium_abi::RegionProt::ReadWrite)
            .expect("attach");

        // The connector reads the guest's outbound ring and writes the
        // guest's inbound ring (the mirror half).
        let mut peer =
            ByteStream::from_ring_channels(&ring_from_guest, &ring_to_guest, region, true)
                .expect("peer");
        let mut stream = QuicStream::from_shared_id(shared_id).expect("quic stream");

        peer.write_all(b"request").await.expect("peer write");
        let mut buf = [0u8; 7];
        stream.read_exact(&mut buf).await.expect("guest read");
        assert_eq!(&buf, b"request");

        stream.write_all(b"response").await.expect("guest write");
        let mut buf = [0u8; 8];
        peer.read_exact(&mut buf).await.expect("peer read");
        assert_eq!(&buf, b"response");
    }
}
