use std::{
    cell::RefCell,
    fmt::{self, Debug},
    io::{self, IoSliceMut},
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use quinn::{
    AsyncTimer, AsyncUdpSocket, Runtime, UdpSender,
    udp::{RecvMeta, Transmit},
};
use selium_abi::{HostcallOutput, HostcallRequest};

use crate::{
    Instant, Signal,
    hostcall::{HostcallFuture, hostcall_async},
    io::channels::{StrongReader, StrongWriter},
    net::udp::UdpSocket,
};

pub struct QuinnUdpSocket(UdpSocket);

struct QuinnUdpSender {
    inner: Arc<UdpSocketInner>,
    pending_signal: Option<HostcallFuture>,
}

#[derive(Debug)]
pub struct SeliumQuinnRuntime;

impl QuinnUdpSocket {
    pub(crate) fn new(sock: UdpSocket) -> Self {
        Self(sock)
    }
}

impl AsyncUdpSocket for QuinnUdpSocket {
    fn create_sender(&self) -> Pin<Box<dyn UdpSender>> {
        Box::pin(QuinnUdpSender {
            inner: self.0.clone(),
            pending_signal: None,
        })
    }

    fn poll_recv(
        &mut self,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [RecvMeta],
    ) -> Poll<io::Result<usize>> {
        if bufs.is_empty() || meta.is_empty() {
            return Poll::Ready(Ok(0));
        }

        let inner = &mut *self.0;

        // Try to read a frame from the recv channel.
        match inner.recv_reader.read() {
            Ok((frame, _tag)) => {
                // Parse frame: [addr_len 2 bytes][addr bytes][ecn 1 byte][payload]
                if frame.len() < 2 {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "invalid udp frame: too short for addr_len",
                    )));
                }
                let addr_len = u16::from_le_bytes([frame[0], frame[1]]) as usize;
                if frame.len() < 2 + addr_len + 1 {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "invalid udp frame: too short for addr + ecn",
                    )));
                }
                let addr_str = match std::str::from_utf8(&frame[2..2 + addr_len]) {
                    Ok(s) => s,
                    Err(_) => {
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "invalid udp frame: addr is not utf8",
                        )));
                    }
                };
                let addr: SocketAddr = match addr_str.parse() {
                    Ok(a) => a,
                    Err(_) => {
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "invalid udp frame: addr parse failed",
                        )));
                    }
                };
                let payload = &frame[2 + addr_len + 1..];
                let to_copy = payload.len().min(bufs[0].len());
                bufs[0][..to_copy].copy_from_slice(&payload[..to_copy]);

                meta[0] = RecvMeta::default();
                meta[0].addr = addr;
                meta[0].len = to_copy;
                meta[0].stride = to_copy;

                Poll::Ready(Ok(1))
            }
            Err(crate::io::channels::Error::ChannelEmpty) => {
                // Start a SignalWait hostcall and return Pending.
                let observed = match inner.recv_signal.generation() {
                    Ok(g) => g,
                    Err(e) => {
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!("signal generation error: {e}"),
                        )));
                    }
                };

                let mut fut = hostcall_async(HostcallRequest::SignalWait {
                    local_id: inner.recv_signal.local_id(),
                    observed_generation: observed,
                    timeout_ms: 30_000,
                });

                // Poll the future once.
                match Pin::new(&mut fut).poll(cx) {
                    Poll::Ready(Ok(HostcallOutput::SignalGeneration(_))) => {
                        // Signal fired; wake and try again.
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                    Poll::Ready(Ok(_)) => Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::Other,
                        "unexpected hostcall output during recv wait",
                    ))),
                    Poll::Ready(Err(e)) => Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!("hostcall error: {e}"),
                    ))),
                    Poll::Pending => Poll::Pending,
                }
            }
            Err(e) => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::Other,
                format!("recv read error: {e}"),
            ))),
        }
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

impl UdpSender for QuinnUdpSender {
    fn poll_send(
        self: Pin<&mut Self>,
        transmit: &Transmit<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();

        // If a previous signal wait is pending, poll it.
        if let Some(ref mut fut) = this.pending_signal {
            let poll = Pin::new(fut).poll(cx);
            match poll {
                Poll::Ready(Ok(HostcallOutput::SignalGeneration(_))) => {
                    this.pending_signal = None;
                }
                Poll::Ready(Ok(_)) => {
                    this.pending_signal = None;
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::Other,
                        "unexpected hostcall output during send wait",
                    )));
                }
                Poll::Ready(Err(e)) => {
                    this.pending_signal = None;
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!("hostcall error: {e}"),
                    )));
                }
                Poll::Pending => return Poll::Pending,
            }
        }

        // Encode frame: [addr_len 2 bytes][addr bytes][payload]
        let addr_bytes = transmit.destination.to_string().into_bytes();
        let addr_len = addr_bytes.len();
        let mut frame = Vec::with_capacity(2 + addr_len + transmit.contents.len());
        frame.extend_from_slice(&(addr_len as u16).to_le_bytes());
        frame.extend_from_slice(&addr_bytes);
        frame.extend_from_slice(transmit.contents);

        match this.inner.send_writer.borrow_mut().write(&frame) {
            Ok(()) => Poll::Ready(Ok(())),
            Err(crate::io::channels::Error::ChannelFull) => {
                // Channel full. Start a signal wait and return Pending.
                let observed = match this.inner.send_signal.generation() {
                    Ok(g) => g,
                    Err(e) => {
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!("signal generation error: {e}"),
                        )));
                    }
                };

                let mut fut = hostcall_async(HostcallRequest::SignalWait {
                    local_id: this.inner.send_signal.local_id(),
                    observed_generation: observed,
                    timeout_ms: 30_000,
                });

                match Pin::new(&mut fut).poll(cx) {
                    Poll::Ready(Ok(HostcallOutput::SignalGeneration(_))) => {
                        // Signal fired; channel likely has space. Wake and retry.
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                    Poll::Ready(Ok(_)) => Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::Other,
                        "unexpected hostcall output during send wait",
                    ))),
                    Poll::Ready(Err(e)) => Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!("hostcall error: {e}"),
                    ))),
                    Poll::Pending => {
                        this.pending_signal = Some(fut);
                        Poll::Pending
                    }
                }
            }
            Err(e) => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::Other,
                format!("send write error: {e}"),
            ))),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quinn_udp_socket_impls_async_udp_socket() {
        fn assert_bound<S: quinn::AsyncUdpSocket>() {}
        assert_bound::<quinn_impl::QuinnUdpSocket>();
    }

    #[test]
    fn selium_quinn_runtime_impls_runtime() {
        fn assert_bound<R: quinn::Runtime>() {}
        assert_bound::<quinn_impl::SeliumQuinnRuntime>();
    }

    #[test]
    fn into_quinn_socket_exists_and_returns_quinn_udp_socket() {
        fn assert_signature<F>(_f: F)
        where
            F: FnOnce(UdpSocket) -> quinn_impl::QuinnUdpSocket,
        {
        }
        assert_signature(UdpSocket::into_quinn_socket);
    }
}
