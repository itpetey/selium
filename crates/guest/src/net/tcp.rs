//! TCP byte-stream socket handles over shared-memory ring buffers.
//!
//! `TcpStream` implements `tokio::io::AsyncRead + AsyncWrite` by delegating to
//! `Reader`/`Writer` over a two-ring multi-memory region. Frame headers are
//! stripped / prepended transparently so the caller sees a continuous byte stream.

use std::{
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
};

use selium_abi::{HostcallOutput, HostcallRequest, RegionProt, SharedRegionDescriptor};
use selium_memory::MultiMemoryHeader;
use selium_shm::{
    Channel, ChannelBackpressure, ChannelRegion,
    channels::{Reader, Writer},
    ring_buf::RingBuf,
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::{Accept, GuestError, IncomingConnection, Result, hostcall::hostcall_async};

/// A TCP byte stream backed by shared-memory ring buffers.
///
/// Implements [`AsyncRead`] and [`AsyncWrite`] with correct waker registration
/// via `register_generation_wait`. Can be wrapped in `hyper_util::rt::TokioIo`
/// for BYO-framework use.
///
/// Each `poll_write` encodes the data as a single frame (header + payload) on
/// the outbound ring; each `poll_read` decodes frames from the inbound ring
/// and strips headers, presenting a continuous byte stream.
pub struct TcpStream {
    reader: Reader,
    writer: Writer,
    /// Buffered data from a partial frame read (header already stripped).
    read_buf: Vec<u8>,
    /// Offset into `read_buf` for the next copy.
    read_offset: usize,
    /// Keeps the parent shared region alive while the stream is in use.
    _region: selium_memory::Region,
}

/// A TCP listener that yields [`TcpStream`] handles for incoming connections.
///
/// Wraps a [`ResourceListener`] over a host-mediated queue; the kernel accept
/// loop pushes connection `shared_id`s onto this queue.
pub struct TcpListener {
    listener: crate::ResourceListener,
}

impl TcpStream {
    /// Connects to a remote TCP endpoint identified by an IP-literal address.
    ///
    /// The address is validated early for ergonomics; the runtime is the
    /// enforcement point for literals-only addressing.
    pub async fn connect(addr: &str) -> Result<Self> {
        // Early validation — the runtime rejects names with MalformedPayload.
        let _: SocketAddr = addr
            .parse()
            .map_err(|_e| GuestError::Host(format!("invalid IP literal address: {addr}")))?;

        let output = hostcall_async(HostcallRequest::TcpConnect {
            address: addr.to_string(),
        })
        .await?;

        let descriptor = match output {
            HostcallOutput::SharedRegion(d) => d,
            _ => return Err(GuestError::UnexpectedHostcallOutput),
        };

        Self::attach_region(descriptor)
    }

    /// Attaches to a stream region by `shared_id` (used by both `connect` and `accept`).
    fn attach_region(descriptor: SharedRegionDescriptor) -> Result<Self> {
        let shared_id = descriptor.shared_id;

        let region_provider = selium_memory::region_provider()
            .map_err(|e| GuestError::Host(format!("region provider unavailable: {e}")))?;

        let region = region_provider
            .attach(shared_id, None, RegionProt::ReadWrite)
            .map_err(|e| GuestError::Host(format!("attach region failed: {e}")))?;

        let mapping = region.mapping();
        let header = MultiMemoryHeader::parse(mapping.backend(), 0)
            .map_err(|e| GuestError::Host(format!("parse header failed: {e}")))?;

        let inbound = header
            .entry(0)
            .map_err(|e| GuestError::Host(format!("inbound entry missing: {e}")))?;
        let outbound = header
            .entry(1)
            .map_err(|e| GuestError::Host(format!("outbound entry missing: {e}")))?;

        let inbound_mapping = mapping
            .sub_region(inbound.offset, inbound.length)
            .map_err(|e| GuestError::Host(format!("inbound sub-region failed: {e}")))?;
        let outbound_mapping = mapping
            .sub_region(outbound.offset, outbound.length)
            .map_err(|e| GuestError::Host(format!("outbound sub-region failed: {e}")))?;

        // Ring data capacity = region length minus header overhead.
        let ring_cap = inbound
            .length
            .saturating_sub(selium_shm::layout::DATA_OFFSET);

        let inbound_region =
            ChannelRegion::from_mapping_with_id(inbound_mapping, ring_cap, shared_id);
        let outbound_region =
            ChannelRegion::from_mapping_with_id(outbound_mapping, ring_cap, shared_id);

        let inbound_ring = RingBuf::wrap_region(inbound_region)
            .map_err(|e| GuestError::Host(format!("wrap inbound ring failed: {e}")))?;
        let outbound_ring = RingBuf::wrap_region(outbound_region)
            .map_err(|e| GuestError::Host(format!("wrap outbound ring failed: {e}")))?;

        let inbound_channel = Channel::from_ring(inbound_ring, ChannelBackpressure::Park);
        let outbound_channel = Channel::from_ring(outbound_ring, ChannelBackpressure::Park);

        let reader = inbound_channel.reader();
        let writer = outbound_channel
            .writer()
            .map_err(|e| GuestError::Host(format!("create writer failed: {e}")))?;

        Ok(Self {
            reader,
            writer,
            read_buf: Vec::new(),
            read_offset: 0,
            _region: region,
        })
    }

    /// Project `Pin<&mut Self>` → `Pin<&mut Reader>`.
    ///
    /// # Safety
    /// The caller must ensure no other field of `self` is accessed while the
    /// returned pin is live (standard pin projection rules).
    unsafe fn project_reader(self: Pin<&mut Self>) -> Pin<&mut Reader> {
        // SAFETY: caller ensures no other field of `self` is accessed while the
        // returned pin is live (standard pin projection rules).
        let this = unsafe { self.get_unchecked_mut() };
        // SAFETY: we project only through the `reader` field; the caller
        // upholds the pin invariants.
        unsafe { Pin::new_unchecked(&mut this.reader) }
    }

    /// Project `Pin<&mut Self>` → `Pin<&mut Writer>`.
    ///
    /// # Safety
    /// Same constraints as [`project_reader`].
    unsafe fn project_writer(self: Pin<&mut Self>) -> Pin<&mut Writer> {
        // SAFETY: caller ensures no other field of `self` is accessed while the
        // returned pin is live (standard pin projection rules).
        let this = unsafe { self.get_unchecked_mut() };
        // SAFETY: we project only through the `writer` field; the caller
        // upholds the pin invariants.
        unsafe { Pin::new_unchecked(&mut this.writer) }
    }
}

impl AsyncRead for TcpStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        // Drain any previously buffered payload bytes (header already stripped).
        if self.read_offset < self.read_buf.len() {
            let remaining = self.read_buf.get(self.read_offset..).unwrap_or(&[]);
            let to_copy = remaining.len().min(buf.remaining());
            buf.put_slice(remaining.get(..to_copy).unwrap_or(remaining));
            self.read_offset += to_copy;
            if self.read_offset >= self.read_buf.len() {
                self.read_buf.clear();
                self.read_offset = 0;
            }
            return Poll::Ready(Ok(()));
        }

        // Read a full frame into a temporary buffer, then strip the header.
        let mut frame_buf = vec![0u8; 65536];
        let mut inner_buf = ReadBuf::new(&mut frame_buf);

        // SAFETY: we only access `reader`, no other pinned fields are borrowed across this call.
        let reader = unsafe { self.as_mut().project_reader() };
        match reader.poll_read(cx, &mut inner_buf) {
            Poll::Ready(Ok(())) => {
                let filled = inner_buf.filled().len();
                if filled == 0 {
                    return Poll::Ready(Ok(())); // EOF
                }
                let header_size = selium_memory::FrameHeader::ENCODED_SIZE;
                if filled <= header_size {
                    return Poll::Ready(Ok(()));
                }
                let payload = frame_buf.get(header_size..filled).unwrap_or(&[]).to_vec();

                // Store stripped payload in read_buf for potential partial copy.
                self.read_buf = payload;
                self.read_offset = 0;

                let to_copy = self.read_buf.len().min(buf.remaining());
                buf.put_slice(self.read_buf.get(..to_copy).unwrap_or(&[]));
                self.read_offset = to_copy;
                if self.read_offset >= self.read_buf.len() {
                    self.read_buf.clear();
                    self.read_offset = 0;
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl AsyncWrite for TcpStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let payload_len = buf.len() as u32;
        let header = selium_memory::FrameHeader {
            len: payload_len,
            tag: 0,
            flags: 0,
            _reserved: [0; 3],
        };
        let header_bytes = header.encode();

        // Build the framed buffer: [FrameHeader 12 bytes][user payload].
        let mut write_buf = Vec::with_capacity(header_bytes.len() + buf.len());
        write_buf.extend_from_slice(&header_bytes);
        write_buf.extend_from_slice(buf);

        // Pin-project to writer and delegate.
        // SAFETY: we project only to the `writer` field; no other pinned fields
        // are accessed concurrently. The caller upholds pin invariants.
        let writer = unsafe { self.as_mut().project_writer() };
        match writer.poll_write(cx, &write_buf) {
            Poll::Ready(Ok(n)) => {
                // Report payload bytes written (not header+payload).
                let payload_written = n.saturating_sub(header_bytes.len());
                Poll::Ready(Ok(payload_written))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        // SAFETY: we project only to the `writer` field; caller upholds pin invariants.
        let writer = unsafe { self.as_mut().project_writer() };
        writer.poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        // SAFETY: we project only to the `writer` field; caller upholds pin invariants.
        let writer = unsafe { self.as_mut().project_writer() };
        writer.poll_shutdown(cx)
    }
}

impl Accept for TcpStream {
    type Item = Self;

    fn accept(connection: IncomingConnection) -> Result<Self> {
        let descriptor = SharedRegionDescriptor {
            shared_id: connection.shared_id,
            len: 0, // length not needed for attachment; header carries capacity
        };
        Self::attach_region(descriptor)
    }
}

// SAFETY: Reader and Writer are safe to send across threads (they encapsulate
// shared memory regions which are backed by process-level mappings).
unsafe impl Send for TcpStream {}

impl TcpListener {
    /// Binds to an IP-literal address and returns a listener.
    ///
    /// The address is validated early for ergonomics; the runtime is the
    /// enforcement point.
    pub fn bind(addr: &str) -> Result<Self> {
        let _: SocketAddr = addr
            .parse()
            .map_err(|_e| GuestError::Host(format!("invalid IP literal address: {addr}")))?;

        let output = crate::hostcall::hostcall_ready(HostcallRequest::TcpBind {
            address: addr.to_string(),
        })?;

        let descriptor = match output {
            HostcallOutput::HostQueue(d) => d,
            _ => return Err(GuestError::UnexpectedHostcallOutput),
        };

        Ok(Self {
            listener: crate::ResourceListener::from_queue(descriptor),
        })
    }

    /// Accepts the next incoming connection, returning a [`TcpStream`].
    pub async fn accept(&self) -> Result<TcpStream> {
        self.listener.accept::<TcpStream>().await
    }
}
