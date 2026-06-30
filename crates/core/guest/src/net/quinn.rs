use std::{
    fmt::{self, Debug},
    io::{self, IoSliceMut},
    net::SocketAddr,
    pin::Pin,
    sync::{
        Arc,
        atomic::{Ordering, fence},
    },
    task::{Context, Poll},
};

use quinn::{AsyncTimer, AsyncUdpSocket, Runtime, UdpSender, udp::RecvMeta};
use selium_shm::RingBuf;
use selium_wire::{error::Error, frame::FrameHeader};

use crate::net::udp::UdpSocket;

pub struct QuinnUdpSocket {
    inner: Arc<UdpSocketInner>,
    read_pos: u64,
}

/// Shared socket state that can be cheaply cloned between the async socket
/// and any number of senders.
///
/// The read cursor (`read_pos`) is intentionally NOT in this struct — it is
/// owned exclusively by [`QuinnUdpSocket`] so that only one task drives
/// `poll_recv`.
struct UdpSocketInner {
    local_addr: SocketAddr,
    recv_ring: RingBuf,
    send_ring: RingBuf,
}

#[derive(Debug)]
pub struct SeliumQuinnRuntime;

struct QuinnUdpSender {
    inner: Arc<UdpSocketInner>,
}

impl AsyncUdpSocket for QuinnUdpSocket {
    fn create_sender(&self) -> Pin<Box<dyn UdpSender>> {
        Box::pin(QuinnUdpSender {
            inner: self.inner.clone(),
        })
    }

    fn poll_recv(
        &mut self,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [RecvMeta],
    ) -> Poll<io::Result<usize>> {
        let recv_ring = &self.inner.recv_ring;
        let read_pos = &mut self.read_pos;

        // Check if writer is still connected
        let writer_count = recv_ring
            .region()
            .load_writer_count()
            .map_err(|e| io::Error::other(format!("load writer count: {e}")))?;

        if writer_count == 0 && *read_pos >= recv_ring.region().load_next_tail().unwrap_or(0) {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "UDP recv ring closed",
            )));
        }

        // Check for available data
        let tail = recv_ring
            .region()
            .load_next_tail()
            .map_err(|e| io::Error::other(format!("load tail: {e}")))?;

        if *read_pos >= tail {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }

        // Read frame header
        fence(Ordering::Acquire);
        let header = recv_ring
            .read_frame_header(*read_pos)
            .map_err(|e| io::Error::other(format!("read header: {e}")))?;

        if !header.is_ready() {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }

        let frame_size = header.frame_size();
        let payload_pos = *read_pos + FrameHeader::ENCODED_SIZE as u64;
        let frame_data = recv_ring
            .read_at(payload_pos, header.len as u64)
            .map_err(|e| io::Error::other(format!("read payload: {e}")))?;

        *read_pos += frame_size;

        // Decode the datagram.
        let (addr, payload) = match UdpSocket::decode_datagram(&frame_data) {
            Ok(result) => result,
            Err(_) => {
                // Malformed frame, wake and try again.
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
        };

        // Copy payload to the first buffer
        if bufs.is_empty() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "no buffers provided",
            )));
        }

        let to_copy = payload.len().min(bufs[0].len());
        bufs[0][..to_copy].copy_from_slice(&payload[..to_copy]);

        // Populate metadata
        if !meta.is_empty() {
            let mut m = <RecvMeta as std::default::Default>::default();
            m.len = to_copy;
            m.stride = to_copy;
            m.addr = addr;
            m.ecn = None;
            m.dst_ip = None;
            meta[0] = m;
        }

        Poll::Ready(Ok(1))
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.inner.local_addr)
    }

    fn max_receive_segments(&self) -> usize {
        1
    }

    fn may_fragment(&self) -> bool {
        false
    }
}

impl From<UdpSocket> for QuinnUdpSocket {
    fn from(socket: UdpSocket) -> Self {
        Self {
            inner: Arc::new(UdpSocketInner {
                local_addr: socket.local_addr,
                recv_ring: socket.recv_ring,
                send_ring: socket.send_ring,
            }),
            read_pos: socket.read_pos,
        }
    }
}

impl Debug for QuinnUdpSocket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuinnUdpSocket")
            .field("local_addr", &self.inner.local_addr)
            .finish()
    }
}

impl Runtime for SeliumQuinnRuntime {
    fn spawn(&self, future: Pin<Box<dyn std::future::Future<Output = ()> + Send>>) {
        // Bridge Send-bound future to the guest's single-threaded runtime.
        // SAFETY: The guest runtime is single-threaded and cooperative.
        // The Send bound is required by Quinn's trait but is a no-op in
        // our single-threaded environment.
        let fut: Pin<Box<dyn std::future::Future<Output = ()>>> =
            unsafe { std::mem::transmute(future) };
        crate::async_runtime::spawn(fut);
    }

    fn new_timer(&self, deadline: web_time::Instant) -> Pin<Box<dyn AsyncTimer>> {
        Box::pin(crate::time::Timer::new(from_quinn_instant(deadline)))
    }

    fn now(&self) -> web_time::Instant {
        let i = crate::time::Instant::now().expect("failed to get monotonic time from host");
        let d = std::time::Duration::from_nanos(i.as_nanos());
        web_time::Instant::from(d)
    }
}

impl UdpSender for QuinnUdpSender {
    fn poll_send(
        self: Pin<&mut Self>,
        transmit: &quinn::udp::Transmit<'_>,
        _cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        let send_ring = &self.inner.send_ring;

        // Encode the datagram.
        let frame = UdpSocket::encode_datagram(transmit.contents, transmit.destination);
        let frame_size = FrameHeader::ENCODED_SIZE as u64 + frame.len() as u64;

        // Reserve space on send ring
        let pos = match send_ring.reserve(frame_size) {
            Ok(p) => p,
            Err(Error::BufferFull) => {
                return Poll::Pending;
            }
            Err(e) => {
                return Poll::Ready(Err(io::Error::other(format!("reserve: {e}"))));
            }
        };

        // Write frame
        match send_ring.write_frame(pos, &frame, 0, 0) {
            Ok(()) => Poll::Ready(Ok(())),
            Err(e) => Poll::Ready(Err(io::Error::other(format!("write frame: {e}")))),
        }
    }

    fn max_transmit_segments(&self) -> usize {
        1
    }
}

impl Debug for QuinnUdpSender {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuinnUdpSender").finish()
    }
}

impl quinn::AsyncTimer for crate::time::Timer {
    fn reset(self: Pin<&mut Self>, deadline: web_time::Instant) {
        self.get_mut().set_deadline(from_quinn_instant(deadline));
    }

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<()> {
        std::future::Future::poll(self, cx)
    }
}

fn from_quinn_instant(qi: web_time::Instant) -> crate::time::Instant {
    let zero = web_time::Instant::from(std::time::Duration::ZERO);
    let duration = qi.duration_since(zero);
    crate::time::Instant::from_nanos(duration.as_nanos() as u64)
}
