use std::{
    fmt::{self, Debug},
    io::{self, IoSliceMut},
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
};

use quinn::{AsyncTimer, AsyncUdpSocket, Runtime, UdpSender, udp::RecvMeta};

use crate::{Instant, net::udp::UdpSocket};

pub struct QuinnUdpSocket(UdpSocket);

#[derive(Debug)]
pub struct SeliumQuinnRuntime;

struct QuinnUdpSender {
    _inner: UdpSocket,
}

impl QuinnUdpSocket {
    pub(crate) fn new(sock: UdpSocket) -> Self {
        Self(sock)
    }
}

impl AsyncUdpSocket for QuinnUdpSocket {
    fn create_sender(&self) -> Pin<Box<dyn UdpSender>> {
        Box::pin(QuinnUdpSender {
            _inner: self.0.clone(),
        })
    }

    fn poll_recv(
        &mut self,
        _cx: &mut Context<'_>,
        _bufs: &mut [IoSliceMut<'_>],
        _meta: &mut [RecvMeta],
    ) -> Poll<io::Result<usize>> {
        // TODO(networking-follow-up): Implement recv from shared-memory channel
        // using atomic wait instead of the removed SignalWait hostcall.
        Poll::Pending
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
        _transmit: &quinn::udp::Transmit<'_>,
        _cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        // TODO(networking-followup): Implement send to shared-memory channel
        // using atomic wait instead of the removed SignalWait hostcall.
        Poll::Pending
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
