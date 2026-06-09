use std::{
    fmt::{self, Debug},
    io::{self, IoSliceMut},
    net::SocketAddr,
    pin::Pin,
    sync::atomic::{Ordering, fence},
    task::{Context, Poll},
};

use quinn::{AsyncTimer, AsyncUdpSocket, Runtime, UdpSender, udp::RecvMeta};

use crate::{
    Instant,
    io::{FrameHeader, RingBuf},
    net::udp::UdpSocket,
};

pub struct QuinnUdpSocket(UdpSocket);

#[derive(Debug)]
pub struct SeliumQuinnRuntime;

struct QuinnUdpSender {
    inner: UdpSocket,
}

impl QuinnUdpSocket {
    pub(crate) fn new(sock: UdpSocket) -> Self {
        Self(sock)
    }
}

impl AsyncUdpSocket for QuinnUdpSocket {
    fn create_sender(&self) -> Pin<Box<dyn UdpSender>> {
        Box::pin(QuinnUdpSender {
            inner: self.0.clone(),
        })
    }

    fn poll_recv(
        &mut self,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [RecvMeta],
    ) -> Poll<io::Result<usize>> {
        let recv_ring = &self.0.recv_ring;
        let read_pos = &mut self.0.read_pos;

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
            meta[0] = RecvMeta {
                len: to_copy,
                stride: to_copy,
                addr,
                ecn: None,
                dst_ip: None,
            };
        }

        Poll::Ready(Ok(1))
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.0.local_addr)
    }

    fn max_receive_segments(&self) -> usize {
        1
    }

    fn may_fragment(&self) -> bool {
        false
    }
}

impl Debug for QuinnUdpSocket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuinnUdpSocket")
            .field("local_addr", &self.0.local_addr)
            .finish()
    }
}

impl Runtime for SeliumQuinnRuntime {
    type Instant = Instant;

    fn spawn(&self, future: Pin<Box<dyn std::future::Future<Output = ()> + Send>>) {
        // Bridge Send-bound future to the guest's single-threaded runtime.
        // SAFETY: The guest runtime is single-threaded and cooperative.
        // The Send bound is required by Quinn's trait but is a no-op in
        // our single-threaded environment.
        let fut: Pin<Box<dyn std::future::Future<Output = ()>>> =
            unsafe { std::mem::transmute(future) };
        crate::async_runtime::spawn(fut);
    }

    fn new_timer(
        &self,
        deadline: Self::Instant,
    ) -> Pin<Box<dyn AsyncTimer<Instant = Self::Instant>>> {
        Box::pin(crate::time::Timer::new(deadline))
    }

    #[cfg(not(target_family = "wasm"))]
    fn wrap_udp_socket(&self, _socket: std::net::UdpSocket) -> io::Result<Box<dyn AsyncUdpSocket>> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "use new_with_abstract_socket for QuinnUdpSocket",
        ))
    }

    fn now(&self) -> Instant {
        Instant::now()
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
            Err(crate::io::Error::BufferFull) => {
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

impl quinn::RuntimeInstant for Instant {
    type Duration = Duration;

    fn now() -> Self {
        Instant::now()
    }

    fn duration_since(&self, earlier: Self) -> Self::Duration {
        Instant::duration_since(self, earlier)
    }

    fn checked_duration_since(&self, earlier: Self) -> Option<Self::Duration> {
        Instant::checked_duration_since(self, earlier)
    }

    fn saturating_duration_since(&self, earlier: Self) -> Self::Duration {
        Instant::saturating_duration_since(self, earlier)
    }

    fn elapsed(&self) -> Self::Duration {
        Instant::elapsed(self)
    }

    fn checked_add(&self, duration: Self::Duration) -> Option<Self> {
        Instant::checked_add(self, duration)
    }

    fn checked_sub(&self, duration: Self::Duration) -> Option<Self> {
        Instant::checked_sub(self, duration)
    }
}

impl quinn::AsyncTimer for Timer {
    type Instant = Instant;

    fn reset(self: std::pin::Pin<&mut Self>, deadline: Instant) {
        let this = self.get_mut();
        this.cancel_wait();
        this.deadline = deadline;
    }

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        std::future::Future::poll(self, cx)
    }
}
