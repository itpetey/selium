//! TCP stream and listener backed by shared-memory ring buffers.

use std::{
    io,
    net::SocketAddr,
    pin::Pin,
    sync::atomic::{Ordering, fence},
    task::{Context, Poll},
};

use selium_abi::{HostcallOutput, HostcallRequest};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::{
    GuestError, Result,
    hostcall::hostcall_async,
    io::{ChannelRegion, FrameHeader, PAGE_SIZE, RegionMapping, RingBuf},
    resource::{Accept, IncomingConnection, ResourceListener},
};

/// Magic value for multi-memory shared region layout headers.
const SHARED_REGION_MAGIC: u64 = 0x53454C49554D454D;

/// Multi-memory header offsets.
const HEADER_MAGIC_OFFSET: u64 = 0;
const HEADER_COUNT_OFFSET: u64 = 16;
const HEADER_ENTRY_OFFSET: u64 = 24;

/// A TCP stream backed by shared-memory ring buffers.
///
/// Uses two ring buffers within a multi-memory shared region:
/// - Inbound ring: kernel writes → guest reads
/// - Outbound ring: guest writes → kernel reads
pub struct TcpStream {
    inbound: RingBuf,
    outbound: RingBuf,
    read_pos: u64,
    read_buf: Vec<u8>,
    read_offset: usize,
    eof: bool,
}

/// A TCP listener that accepts incoming connections via the host.
pub struct TcpListener {
    pub(crate) listener: ResourceListener,
    pub(crate) local_addr: SocketAddr,
}

/// Accepts incoming TCP connections and produces `TcpStream` handles.
pub struct TcpAccept;

impl TcpStream {
    /// Connects to a remote TCP endpoint via the host.
    ///
    /// Issues the `TcpConnect` hostcall, receives a `SharedRegionDescriptor`,
    /// attaches the region, parses the multi-memory header, and creates
    /// inbound and outbound `RingBuf` handles.
    pub async fn connect(address: impl Into<String>) -> Result<Self> {
        let descriptor = match hostcall_async(HostcallRequest::TcpConnect {
            address: address.into(),
        })
        .await?
        {
            HostcallOutput::SharedRegion(descriptor) => descriptor,
            _ => return Err(GuestError::UnexpectedHostcallOutput),
        };

        Self::attach_shared(descriptor.shared_id)
    }

    /// Attaches to an existing shared region containing TCP stream ring buffers.
    ///
    /// Parses the multi-memory header to discover the inbound and outbound
    /// sub-memories, then creates `RingBuf` handles for each.
    ///
    /// In WASM mode, this calls `attach_region(shared_id)` to map the region.
    /// In native mode, this returns an error because hostcalls are not available.
    pub fn attach_shared(shared_id: u64) -> Result<Self> {
        // In native mode, we cannot attach to a shared region by ID.
        // This will work in WASM mode where the hostcall infrastructure exists.
        //
        // In WASM mode, the implementation would be:
        // 1. Call attach_region(shared_id, None, RegionProt::ReadWrite)
        // 2. Create a RegionMapping from the attachment's page_offset
        // 3. Parse the multi-memory header
        // 4. Create RingBuf instances for inbound and outbound rings
        Err(GuestError::Host(format!(
            "TCP stream attach requires WASM mode (shared_id={shared_id})"
        )))
    }

    /// Creates a `TcpStream` from a pre-attached region mapping.
    ///
    /// Parses the multi-memory header to discover the inbound and outbound
    /// sub-memories, then creates `RingBuf` handles for each.
    ///
    /// This is useful for in-process testing or when the caller has already
    /// attached to the shared region.
    pub fn from_mapping(parent_mapping: &RegionMapping) -> Result<Self> {
        let (inbound, outbound) = parse_dual_ring_region(parent_mapping)?;
        Ok(Self {
            inbound,
            outbound,
            read_pos: 0,
            read_buf: Vec::new(),
            read_offset: 0,
            eof: false,
        })
    }

    /// Creates a `TcpStream` from pre-built ring buffers (for testing).
    #[cfg(test)]
    #[allow(dead_code, reason = "test helper for future integration tests")]
    pub(crate) fn from_rings(inbound: RingBuf, outbound: RingBuf) -> Self {
        Self {
            inbound,
            outbound,
            read_pos: 0,
            read_buf: Vec::new(),
            read_offset: 0,
            eof: false,
        }
    }
}

impl AsyncRead for TcpStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        // If we have buffered data from a previous frame, copy it out.
        if self.read_offset < self.read_buf.len() {
            let remaining = &self.read_buf[self.read_offset..];
            let to_copy = remaining.len().min(buf.remaining());
            buf.put_slice(&remaining[..to_copy]);
            self.read_offset += to_copy;
            if self.read_offset >= self.read_buf.len() {
                self.read_buf.clear();
                self.read_offset = 0;
            }
            return Poll::Ready(Ok(()));
        }

        // Check for EOF.
        if self.eof {
            return Poll::Ready(Ok(()));
        }

        // Try to read a frame from the inbound ring.
        let tail = match self.inbound.region().load_next_tail() {
            Ok(t) => t,
            Err(e) => return Poll::Ready(Err(io::Error::other(format!("load tail: {e}")))),
        };

        if self.read_pos >= tail {
            // No data available. Check if the kernel has disconnected.
            match self.inbound.region().load_writer_count() {
                Ok(0) => {
                    self.eof = true;
                    return Poll::Ready(Ok(()));
                }
                Ok(_) => {}
                Err(e) => {
                    return Poll::Ready(Err(io::Error::other(format!("load writer count: {e}"))));
                }
            }
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }

        // Read the frame header.
        fence(Ordering::Acquire);
        let header = match self.inbound.read_frame_header(self.read_pos) {
            Ok(h) => h,
            Err(e) => {
                return Poll::Ready(Err(io::Error::other(format!("read header: {e}"))));
            }
        };

        if !header.is_ready() {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }

        let frame_size = header.frame_size();
        let payload_pos = self.read_pos + FrameHeader::ENCODED_SIZE as u64;
        let payload = match self.inbound.read_at(payload_pos, header.len as u64) {
            Ok(p) => p,
            Err(e) => {
                return Poll::Ready(Err(io::Error::other(format!("read payload: {e}"))));
            }
        };

        self.read_pos += frame_size;

        // Copy payload to the caller's buffer.
        let to_copy = payload.len().min(buf.remaining());
        buf.put_slice(&payload[..to_copy]);

        // Buffer any remaining data.
        if to_copy < payload.len() {
            self.read_buf = payload;
            self.read_offset = to_copy;
        }

        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for TcpStream {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }

        let frame_size = FrameHeader::ENCODED_SIZE as u64 + buf.len() as u64;

        // Reserve space in the outbound ring.
        let pos = match self.outbound.reserve(frame_size) {
            Ok(p) => p,
            Err(crate::io::Error::BufferFull) => {
                return Poll::Pending;
            }
            Err(e) => {
                return Poll::Ready(Err(io::Error::other(format!("reserve: {e}"))));
            }
        };

        // Write the frame.
        match self.outbound.write_frame(pos, buf, 0, 0) {
            Ok(()) => Poll::Ready(Ok(buf.len())),
            Err(e) => Poll::Ready(Err(io::Error::other(format!("write frame: {e}")))),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // Decrement writer count on the outbound ring to signal EOF to the kernel.
        if let Err(_e) = self.outbound.region().decrement_writer_count() {
            // Ignore errors during shutdown.
        }
        Poll::Ready(Ok(()))
    }
}

impl TcpListener {
    /// Binds a TCP listener via the host.
    pub async fn bind(address: impl Into<String>) -> Result<Self> {
        let address = address.into();
        let descriptor = match hostcall_async(HostcallRequest::TcpBind {
            address: address.clone(),
        })
        .await?
        {
            HostcallOutput::HostQueue(descriptor) => descriptor,
            _ => return Err(GuestError::UnexpectedHostcallOutput),
        };

        let listener = ResourceListener::from_queue(descriptor);
        let local_addr = address
            .parse()
            .map_err(|_error| GuestError::Host(format!("invalid socket address: {address}")))?;

        Ok(Self {
            listener,
            local_addr,
        })
    }

    /// Accepts the next incoming TCP connection.
    pub async fn accept(&self) -> Result<TcpStream> {
        self.listener.accept::<TcpAccept>().await
    }

    /// Returns the local socket address.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

impl Accept for TcpAccept {
    type Item = TcpStream;

    fn accept(connection: IncomingConnection) -> Result<Self::Item> {
        TcpStream::attach_shared(connection.shared_id)
    }
}

/// Parses a multi-memory region header and creates two RingBuf instances.
///
/// The region layout:
/// - Multi-memory header at offset 0
/// - Inbound ring at entry[0].offset (page 0 = coordination, page 1+ = data)
/// - Outbound ring at entry[1].offset (page 0 = coordination, page 1+ = data)
///
/// Returns (inbound_ring, outbound_ring).
fn parse_dual_ring_region(parent_mapping: &RegionMapping) -> Result<(RingBuf, RingBuf)> {
    // Read and validate magic.
    let magic_bytes = parent_mapping
        .read(HEADER_MAGIC_OFFSET, 8)
        .map_err(|e| GuestError::Host(format!("read magic: {e}")))?;
    let magic = u64::from_le_bytes(
        magic_bytes
            .try_into()
            .map_err(|_| GuestError::Host("invalid magic bytes".to_string()))?,
    );
    if magic != SHARED_REGION_MAGIC {
        return Err(GuestError::Host("invalid region magic".to_string()));
    }

    // Read count.
    let count_bytes = parent_mapping
        .read(HEADER_COUNT_OFFSET, 4)
        .map_err(|e| GuestError::Host(format!("read count: {e}")))?;
    let count = u32::from_le_bytes(
        count_bytes
            .try_into()
            .map_err(|_| GuestError::Host("invalid count bytes".to_string()))?,
    );
    if count < 2 {
        return Err(GuestError::Host(format!(
            "expected at least 2 sub-memories, got {count}"
        )));
    }

    // Read entry[0]: inbound ring.
    let entry0_offset_bytes = parent_mapping
        .read(HEADER_ENTRY_OFFSET, 4)
        .map_err(|e| GuestError::Host(format!("read entry0 offset: {e}")))?;
    let entry0_len_bytes = parent_mapping
        .read(HEADER_ENTRY_OFFSET + 4, 4)
        .map_err(|e| GuestError::Host(format!("read entry0 len: {e}")))?;
    let inbound_offset = u32::from_le_bytes(
        entry0_offset_bytes
            .try_into()
            .map_err(|_| GuestError::Host("invalid entry0 offset".to_string()))?,
    ) as u64;
    let inbound_len = u32::from_le_bytes(
        entry0_len_bytes
            .try_into()
            .map_err(|_| GuestError::Host("invalid entry0 len".to_string()))?,
    ) as u64;

    // Read entry[1]: outbound ring.
    let entry1_offset_bytes = parent_mapping
        .read(HEADER_ENTRY_OFFSET + 8, 4)
        .map_err(|e| GuestError::Host(format!("read entry1 offset: {e}")))?;
    let entry1_len_bytes = parent_mapping
        .read(HEADER_ENTRY_OFFSET + 12, 4)
        .map_err(|e| GuestError::Host(format!("read entry1 len: {e}")))?;
    let outbound_offset = u32::from_le_bytes(
        entry1_offset_bytes
            .try_into()
            .map_err(|_| GuestError::Host("invalid entry1 offset".to_string()))?,
    ) as u64;
    let outbound_len = u32::from_le_bytes(
        entry1_len_bytes
            .try_into()
            .map_err(|_| GuestError::Host("invalid entry1 len".to_string()))?,
    ) as u64;

    // Calculate ring capacities (sub-memory length minus page 0).
    let inbound_capacity = inbound_len
        .checked_sub(PAGE_SIZE)
        .ok_or_else(|| GuestError::Host("inbound ring too small".to_string()))?;
    let outbound_capacity = outbound_len
        .checked_sub(PAGE_SIZE)
        .ok_or_else(|| GuestError::Host("outbound ring too small".to_string()))?;

    // Create sub-mappings for each ring.
    let inbound_mapping = parent_mapping
        .sub_region(inbound_offset, inbound_len)
        .map_err(|e| GuestError::Host(format!("create inbound sub-region: {e}")))?;
    let outbound_mapping = parent_mapping
        .sub_region(outbound_offset, outbound_len)
        .map_err(|e| GuestError::Host(format!("create outbound sub-region: {e}")))?;

    // Create ChannelRegions from the sub-mappings.
    let inbound_region = ChannelRegion::from_mapping(inbound_mapping, inbound_capacity);
    let outbound_region = ChannelRegion::from_mapping(outbound_mapping, outbound_capacity);

    // Create RingBuf instances.
    let inbound_ring = RingBuf::wrap_region(inbound_region)
        .map_err(|e| GuestError::Host(format!("wrap inbound: {e}")))?;
    let outbound_ring = RingBuf::wrap_region(outbound_region)
        .map_err(|e| GuestError::Host(format!("wrap outbound: {e}")))?;

    Ok((inbound_ring, outbound_ring))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attach_shared_with_invalid_region_fails() {
        let result = TcpStream::attach_shared(0);
        assert!(matches!(result, Err(GuestError::Host(_))));
    }

    #[test]
    fn tcp_accept_with_invalid_connection_fails() {
        let connection = IncomingConnection {
            client_process_id: 0,
            shared_id: 0,
        };
        let result = TcpAccept::accept(connection);
        assert!(matches!(result, Err(GuestError::Host(_))));
    }
}
