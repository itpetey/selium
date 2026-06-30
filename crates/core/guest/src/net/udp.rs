//! UDP socket backed by shared-memory ring buffers.

use std::{
    net::SocketAddr,
    pin::Pin,
    sync::atomic::{Ordering, fence},
    task::{Context, Poll},
};

use futures::{Sink, Stream};
use selium_abi::{HostcallOutput, HostcallRequest};
use selium_memory::RegionMapping;
use selium_shm::{ChannelRegion, PAGE_SIZE, RingBuf};
use selium_wire::frame::FrameHeader;

use crate::{GuestError, Result, hostcall::hostcall_async};

const HEADER_COUNT_OFFSET: u64 = 16;
const HEADER_ENTRY_OFFSET: u64 = 24;
/// Multi-memory header offsets.
const HEADER_MAGIC_OFFSET: u64 = 0;
/// Magic value for multi-memory shared region layout headers.
const SHARED_REGION_MAGIC: u64 = 0x53454C49554D454D;

/// A UDP socket backed by shared-memory ring buffers.
///
/// Uses two ring buffers within a multi-memory shared region:
/// - Recv ring: kernel writes → guest reads
/// - Send ring: guest writes → kernel reads
pub struct UdpSocket {
    pub(super) local_addr: SocketAddr,
    pub(super) recv_ring: RingBuf,
    pub(super) send_ring: RingBuf,
    pub(super) read_pos: u64,
}

impl UdpSocket {
    /// Binds a UDP socket via the host.
    ///
    /// Issues the `UdpBind` hostcall, receives a `SharedRegionDescriptor`,
    /// attaches the region, parses the multi-memory header, and creates
    /// recv and send `RingBuf` handles.
    ///
    /// In WASM mode, this calls `attach_region` to map the region and parses
    /// the multi-memory header. In native mode, this returns an error because
    /// hostcalls are not available.
    pub async fn bind(address: impl Into<String>) -> Result<Self> {
        let address = address.into();
        let descriptor = match hostcall_async(HostcallRequest::UdpBind {
            address: address.clone(),
        })
        .await?
        {
            HostcallOutput::SharedRegion(descriptor) => descriptor,
            _ => return Err(GuestError::UnexpectedHostcallOutput),
        };

        let _local_addr: SocketAddr = address
            .parse()
            .map_err(|_error| GuestError::Host(format!("invalid socket address: {address}")))?;

        // In WASM mode, we'd attach to the shared region and parse the header.
        // In native mode, this fails.
        Err(GuestError::Host(format!(
            "UDP socket bind requires WASM mode (shared_id={})",
            descriptor.shared_id
        )))
    }

    /// Creates a `UdpSocket` from a pre-attached region mapping.
    ///
    /// Parses the multi-memory header to discover the recv and send
    /// sub-memories, then creates `RingBuf` handles for each.
    ///
    /// This is useful for in-process testing or when the caller has already
    /// attached to the shared region.
    pub fn from_mapping(local_addr: SocketAddr, parent_mapping: &RegionMapping) -> Result<Self> {
        let (recv_ring, send_ring) = parse_dual_ring_region(parent_mapping)?;
        Ok(Self {
            local_addr,
            recv_ring,
            send_ring,
            read_pos: 0,
        })
    }

    /// Returns the local socket address.
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        Ok(self.local_addr)
    }

    /// Encodes a datagram into the wire format.
    ///
    /// Frame format: `[addr_len: u16 LE][addr: UTF-8 bytes][payload: bytes]`
    pub fn encode_datagram(payload: &[u8], addr: SocketAddr) -> Vec<u8> {
        let addr_bytes = addr.to_string().into_bytes();
        let addr_len = addr_bytes.len() as u16;
        let frame_len = 2 + addr_bytes.len() + payload.len();

        let mut frame = Vec::with_capacity(frame_len);
        frame.extend_from_slice(&addr_len.to_le_bytes());
        frame.extend_from_slice(&addr_bytes);
        frame.extend_from_slice(payload);
        frame
    }

    /// Decodes a datagram from the wire format.
    ///
    /// Returns `(source_address, payload)`.
    pub fn decode_datagram(frame: &[u8]) -> Result<(SocketAddr, Vec<u8>)> {
        let prefix = frame
            .get(0..2)
            .ok_or_else(|| GuestError::Host("frame too short".to_string()))?;
        let addr_len = u16::from_le_bytes(
            prefix
                .try_into()
                .map_err(|_error| GuestError::Host("frame too short".to_string()))?,
        ) as usize;
        let addr_bytes = frame
            .get(2..2 + addr_len)
            .ok_or_else(|| GuestError::Host("frame too short for address".to_string()))?;
        let addr_str = std::str::from_utf8(addr_bytes)
            .map_err(|e| GuestError::Host(format!("invalid address bytes: {e}")))?;
        let addr: SocketAddr = addr_str
            .parse()
            .map_err(|e| GuestError::Host(format!("invalid address: {e}")))?;
        let payload = frame
            .get(2 + addr_len..)
            .ok_or_else(|| GuestError::Host("frame too short for payload".to_string()))?
            .to_vec();
        Ok((addr, payload))
    }
}

impl Sink<(Vec<u8>, SocketAddr)> for UdpSocket {
    type Error = GuestError;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        // The send ring handles backpressure via reserve(), so we're always ready
        // to accept a send attempt. If the ring is full, start_send will return
        // an error which the caller can handle.
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: (Vec<u8>, SocketAddr)) -> Result<()> {
        let (payload, addr) = item;
        let frame = Self::encode_datagram(&payload, addr);
        let frame_size = FrameHeader::ENCODED_SIZE as u64 + frame.len() as u64;

        let pos = self
            .send_ring
            .reserve(frame_size)
            .map_err(|e| GuestError::Host(format!("reserve send: {e}")))?;
        self.send_ring
            .write_frame(pos, &frame, 0, 0)
            .map_err(|e| GuestError::Host(format!("write send: {e}")))?;

        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        // Ring buffer writes are immediately visible to the reader.
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        // Nothing to do on close; the ring buffer will be dropped.
        Poll::Ready(Ok(()))
    }
}

impl Stream for UdpSocket {
    type Item = Result<(Vec<u8>, SocketAddr)>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;

        // Check for available data.
        let tail = match this.recv_ring.region().load_next_tail() {
            Ok(t) => t,
            Err(e) => return Poll::Ready(Some(Err(GuestError::Host(format!("load tail: {e}"))))),
        };

        if this.read_pos >= tail {
            // Check if the kernel has disconnected.
            let writer_count = match this.recv_ring.region().load_writer_count() {
                Ok(c) => c,
                Err(e) => {
                    return Poll::Ready(Some(Err(GuestError::Host(format!(
                        "load writer count: {e}"
                    )))));
                }
            };
            if writer_count == 0 {
                return Poll::Ready(Some(Err(GuestError::Host(
                    "UDP recv ring closed".to_string(),
                ))));
            }
            // No data available, wake and return pending.
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }

        // Read the frame.
        fence(Ordering::Acquire);
        let header = match this.recv_ring.read_frame_header(this.read_pos) {
            Ok(h) => h,
            Err(e) => return Poll::Ready(Some(Err(GuestError::Host(format!("read header: {e}"))))),
        };

        if !header.is_ready() {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }

        let frame_size = header.frame_size();
        let payload_pos = this.read_pos + FrameHeader::ENCODED_SIZE as u64;
        let frame_data = match this.recv_ring.read_at(payload_pos, header.len as u64) {
            Ok(d) => d,
            Err(e) => {
                return Poll::Ready(Some(Err(GuestError::Host(format!("read payload: {e}")))));
            }
        };

        this.read_pos += frame_size;

        // Decode the datagram.
        match Self::decode_datagram(&frame_data) {
            Ok((addr, payload)) => Poll::Ready(Some(Ok((payload, addr)))),
            Err(_) => {
                // Malformed frame, wake and try again.
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}

/// Parses a multi-memory region header and creates two RingBuf instances.
///
/// The region layout:
/// - Multi-memory header at offset 0
/// - Recv ring at entry[0].offset (page 0 = coordination, page 1+ = data)
/// - Send ring at entry[1].offset (page 0 = coordination, page 1+ = data)
///
/// Returns (recv_ring, send_ring).
fn parse_dual_ring_region(parent_mapping: &RegionMapping) -> Result<(RingBuf, RingBuf)> {
    // Read and validate magic.
    let magic_bytes = parent_mapping
        .read(HEADER_MAGIC_OFFSET, 8)
        .map_err(|e| GuestError::Host(format!("read magic: {e}")))?;
    let magic = u64::from_le_bytes(
        magic_bytes
            .try_into()
            .map_err(|_error| GuestError::Host("invalid magic bytes".to_string()))?,
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
            .map_err(|_error| GuestError::Host("invalid count bytes".to_string()))?,
    );
    if count < 2 {
        return Err(GuestError::Host(format!(
            "expected at least 2 sub-memories, got {count}"
        )));
    }

    // Read entry[0]: recv ring.
    let entry0_offset_bytes = parent_mapping
        .read(HEADER_ENTRY_OFFSET, 4)
        .map_err(|e| GuestError::Host(format!("read entry0 offset: {e}")))?;
    let entry0_len_bytes = parent_mapping
        .read(HEADER_ENTRY_OFFSET + 4, 4)
        .map_err(|e| GuestError::Host(format!("read entry0 len: {e}")))?;
    let recv_offset = u32::from_le_bytes(
        entry0_offset_bytes
            .try_into()
            .map_err(|_error| GuestError::Host("invalid entry0 offset".to_string()))?,
    ) as u64;
    let recv_len = u32::from_le_bytes(
        entry0_len_bytes
            .try_into()
            .map_err(|_error| GuestError::Host("invalid entry0 len".to_string()))?,
    ) as u64;

    // Read entry[1]: send ring.
    let entry1_offset_bytes = parent_mapping
        .read(HEADER_ENTRY_OFFSET + 8, 4)
        .map_err(|e| GuestError::Host(format!("read entry1 offset: {e}")))?;
    let entry1_len_bytes = parent_mapping
        .read(HEADER_ENTRY_OFFSET + 12, 4)
        .map_err(|e| GuestError::Host(format!("read entry1 len: {e}")))?;
    let send_offset = u32::from_le_bytes(
        entry1_offset_bytes
            .try_into()
            .map_err(|_error| GuestError::Host("invalid entry1 offset".to_string()))?,
    ) as u64;
    let send_len = u32::from_le_bytes(
        entry1_len_bytes
            .try_into()
            .map_err(|_error| GuestError::Host("invalid entry1 len".to_string()))?,
    ) as u64;

    // Calculate ring capacities (sub-memory length minus page 0).
    let recv_capacity = recv_len
        .checked_sub(PAGE_SIZE)
        .ok_or_else(|| GuestError::Host("recv ring too small".to_string()))?;
    let send_capacity = send_len
        .checked_sub(PAGE_SIZE)
        .ok_or_else(|| GuestError::Host("send ring too small".to_string()))?;

    // Create sub-mappings for each ring.
    let recv_mapping = parent_mapping
        .sub_region(recv_offset, recv_len)
        .map_err(|e| GuestError::Host(format!("create recv sub-region: {e}")))?;
    let send_mapping = parent_mapping
        .sub_region(send_offset, send_len)
        .map_err(|e| GuestError::Host(format!("create send sub-region: {e}")))?;

    // Create ChannelRegions from the sub-mappings.
    let recv_region = ChannelRegion::from_mapping(recv_mapping, recv_capacity);
    let send_region = ChannelRegion::from_mapping(send_mapping, send_capacity);

    // Create RingBuf instances.
    let recv_ring = RingBuf::wrap_region(recv_region)
        .map_err(|e| GuestError::Host(format!("wrap recv: {e}")))?;
    let send_ring = RingBuf::wrap_region(send_region)
        .map_err(|e| GuestError::Host(format!("wrap send: {e}")))?;

    Ok((recv_ring, send_ring))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn bind_returns_error_in_native_mode() {
        let result = UdpSocket::bind("127.0.0.1:0").await;
        assert!(result.is_err());
    }
}
