use std::{
    future::Future,
    io,
    net::SocketAddr,
    pin::Pin,
    task::Poll,
};

use selium_abi::{HostcallOutput, HostcallRequest};

use crate::{
    GuestError, Result,
    hostcall::{HostcallFuture, hostcall_async},
    io::{
        ChannelRegion, REGION_HEADER_BYTES, SIGNAL_SHARED_ID_OFFSET,
        channels::{StrongReader, StrongWriter},
    },
    memory::{SHARED_REGION_MAGIC, SharedMemory},
    signal::Signal,
};

/// A UDP socket backed by shared-memory ring buffers.
pub struct UdpSocket {
    recv_reader: StrongReader,
    recv_signal: Signal,
    send_writer: StrongWriter,
    send_signal: Signal,
    local_addr: SocketAddr,
    pending_recv_wait: Option<HostcallFuture>,
    pending_send_wait: Option<HostcallFuture>,
}

impl UdpSocket {
    /// Binds a UDP socket via the host.
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

        let local_addr: SocketAddr = address
            .parse()
            .map_err(|_error| GuestError::Host(format!("invalid socket address: {address}")))?;

        let mut socket = Self::attach_shared(descriptor.shared_id)?;
        socket.local_addr = local_addr;
        Ok(socket)
    }

    /// Attaches to an existing shared region containing UDP socket ring buffers.
    pub fn attach_shared(shared_id: u64) -> Result<Self> {
        let layout = attach_udp_channels(shared_id)?;
        Ok(Self {
            recv_reader: layout.recv_reader,
            recv_signal: layout.recv_signal,
            send_writer: layout.send_writer,
            send_signal: layout.send_signal,
            local_addr: SocketAddr::from(([0, 0, 0, 0], 0)),
            pending_recv_wait: None,
            pending_send_wait: None,
        })
    }

    /// Attempts to receive a single datagram without blocking.
    pub fn try_recv_from(&mut self, buf: &mut [u8]) -> Result<Option<(usize, SocketAddr)>> {
        match self.recv_reader.read() {
            Ok((frame, _tag)) => {
                // Parse frame: [addr_len 2 bytes][addr bytes][ecn 1 byte][payload]
                if frame.len() < 2 {
                    return Err(GuestError::Io(crate::io::Error::Guest("invalid udp frame".to_string())));
                }
                let addr_len = u16::from_le_bytes([frame[0], frame[1]]) as usize;
                if frame.len() < 2 + addr_len + 1 {
                    return Err(GuestError::Io(crate::io::Error::Guest("invalid udp frame".to_string())));
                }
                let addr_str = std::str::from_utf8(&frame[2..2 + addr_len])
                    .map_err(|e| GuestError::Io(crate::io::Error::Guest(format!("invalid address: {e}"))))?;
                let addr: SocketAddr = addr_str
                    .parse()
                    .map_err(|e| GuestError::Io(crate::io::Error::Guest(format!("invalid address: {e}"))))?;
                let payload = &frame[2 + addr_len + 1..];
                let to_copy = payload.len().min(buf.len());
                buf[..to_copy].copy_from_slice(&payload[..to_copy]);
                Ok(Some((to_copy, addr)))
            }
            Err(crate::io::channels::Error::ChannelEmpty) => Ok(None),
            Err(e) => Err(GuestError::Io(crate::io::Error::Guest(e.to_string()))),
        }
    }

    /// Attempts to send a single datagram without blocking.
    pub fn try_send_to(&mut self, buf: &[u8], addr: SocketAddr) -> Result<()> {
        // Frame format: [addr_len 2 bytes][addr bytes][payload]
        let addr_bytes = addr.to_string().into_bytes();
        let addr_len = addr_bytes.len();
        let mut frame = Vec::with_capacity(2 + addr_len + buf.len());
        frame.extend_from_slice(&(addr_len as u16).to_le_bytes());
        frame.extend_from_slice(&addr_bytes);
        frame.extend_from_slice(buf);

        self.send_writer.write(&frame).map_err(|e| {
            GuestError::Io(crate::io::Error::Guest(format!("send write error: {e}")))
        })
    }

    /// Receives a single datagram asynchronously.
    pub async fn recv_from(&mut self, buf: &mut [u8]) -> Result<(usize, SocketAddr)> {
        std::future::poll_fn(|cx| {
            // If a previous signal wait is pending, poll it.
            if let Some(ref mut fut) = self.pending_recv_wait {
                let poll = Pin::new(fut).poll(cx);
                match poll {
                    Poll::Ready(Ok(HostcallOutput::SignalGeneration(_))) => {
                        self.pending_recv_wait = None;
                    }
                    Poll::Ready(Ok(_)) => {
                        self.pending_recv_wait = None;
                        return Poll::Ready(Err(GuestError::Io(crate::io::Error::Guest(
                            "unexpected hostcall output during recv wait".to_string(),
                        ))));
                    }
                    Poll::Ready(Err(e)) => {
                        self.pending_recv_wait = None;
                        return Poll::Ready(Err(GuestError::Io(crate::io::Error::Guest(e.to_string()))));
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }

            // Try to receive a datagram.
            match self.try_recv_from(buf) {
                Ok(Some(result)) => Poll::Ready(Ok(result)),
                Ok(None) => {
                    // Channel empty. Start a signal wait and return Pending.
                    let observed = self
                        .recv_signal
                        .generation()
                        .map_err(|e| GuestError::Io(crate::io::Error::Guest(e.to_string())))?;
                    self.pending_recv_wait =
                        Some(hostcall_async(HostcallRequest::SignalWait {
                            local_id: self.recv_signal.local_id(),
                            observed_generation: observed,
                            timeout_ms: 30_000,
                        }));

                    // Poll the newly created future once.
                    if let Some(ref mut fut) = self.pending_recv_wait {
                        match Pin::new(fut).poll(cx) {
                            Poll::Ready(Ok(HostcallOutput::SignalGeneration(_))) => {
                                self.pending_recv_wait = None;
                                // Signal already fired; try reading again.
                                cx.waker().wake_by_ref();
                                Poll::Pending
                            }
                            Poll::Ready(Ok(_)) => Poll::Ready(Err(GuestError::Io(
                                crate::io::Error::Guest("unexpected hostcall output".to_string()),
                            ))),
                            Poll::Ready(Err(e)) => Poll::Ready(Err(GuestError::Io(
                                crate::io::Error::Guest(e.to_string()),
                            ))),
                            Poll::Pending => Poll::Pending,
                        }
                    } else {
                        Poll::Pending
                    }
                }
                Err(e) => Poll::Ready(Err(e)),
            }
        })
        .await
    }

    /// Sends a single datagram asynchronously.
    pub async fn send_to(&mut self, buf: &[u8], addr: SocketAddr) -> Result<usize> {
        std::future::poll_fn(|cx| {
            // If a previous signal wait is pending, poll it.
            if let Some(ref mut fut) = self.pending_send_wait {
                let poll = Pin::new(fut).poll(cx);
                match poll {
                    Poll::Ready(Ok(HostcallOutput::SignalGeneration(_))) => {
                        self.pending_send_wait = None;
                    }
                    Poll::Ready(Ok(_)) => {
                        self.pending_send_wait = None;
                        return Poll::Ready(Err(GuestError::Io(crate::io::Error::Guest(
                            "unexpected hostcall output during send wait".to_string(),
                        ))));
                    }
                    Poll::Ready(Err(e)) => {
                        self.pending_send_wait = None;
                        return Poll::Ready(Err(GuestError::Io(crate::io::Error::Guest(e.to_string()))));
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }

            // Try to send the datagram.
            match self.try_send_to(buf, addr) {
                Ok(()) => Poll::Ready(Ok(buf.len())),
                Err(GuestError::Io(ref e))
                    if e.to_string().contains("BufferFull")
                        || e.to_string().contains("ChannelFull") =>
                {
                    // Channel full. Start a signal wait and return Pending.
                    let observed = self
                        .send_signal
                        .generation()
                        .map_err(|e| GuestError::Io(crate::io::Error::Guest(e.to_string())))?;
                    self.pending_send_wait =
                        Some(hostcall_async(HostcallRequest::SignalWait {
                            local_id: self.send_signal.local_id(),
                            observed_generation: observed,
                            timeout_ms: 30_000,
                        }));

                    // Poll the newly created future once.
                    if let Some(ref mut fut) = self.pending_send_wait {
                        match Pin::new(fut).poll(cx) {
                            Poll::Ready(Ok(HostcallOutput::SignalGeneration(_))) => {
                                self.pending_send_wait = None;
                                // Signal already fired; try sending again.
                                cx.waker().wake_by_ref();
                                Poll::Pending
                            }
                            Poll::Ready(Ok(_)) => Poll::Ready(Err(GuestError::Io(
                                crate::io::Error::Guest("unexpected hostcall output".to_string()),
                            ))),
                            Poll::Ready(Err(e)) => Poll::Ready(Err(GuestError::Io(
                                crate::io::Error::Guest(e.to_string()),
                            ))),
                            Poll::Pending => Poll::Pending,
                        }
                    } else {
                        Poll::Pending
                    }
                }
                Err(e) => Poll::Ready(Err(e)),
            }
        })
        .await
    }

    /// Returns the local socket address.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.local_addr)
    }
}

struct UdpChannelLayout {
    recv_reader: StrongReader,
    recv_signal: Signal,
    send_writer: StrongWriter,
    send_signal: Signal,
}

fn attach_udp_channels(shared_id: u64) -> Result<UdpChannelLayout> {
    let header = SharedMemory::attach_shared(shared_id, 0, 256)
        .map_err(|e| GuestError::Host(e.to_string()))?;

    let magic_bytes = header
        .read(0, 8)
        .map_err(|e| GuestError::Host(e.to_string()))?;
    let magic = u64::from_le_bytes(
        magic_bytes
            .try_into()
            .map_err(|_error| GuestError::Host("invalid region magic".to_string()))?,
    );
    if magic != SHARED_REGION_MAGIC {
        return Err(GuestError::Host("invalid region magic".to_string()));
    }

    let count = header
        .memory_count()
        .map_err(|e| GuestError::Host(e.to_string()))?;
    if count != 2 {
        return Err(GuestError::Host("invalid memory count".to_string()));
    }

    let (recv_offset, recv_len) = header
        .memory(0)
        .map_err(|e| GuestError::Host(e.to_string()))?;
    let (send_offset, send_len) = header
        .memory(1)
        .map_err(|e| GuestError::Host(e.to_string()))?;

    header
        .detach()
        .map_err(|e| GuestError::Host(e.to_string()))?;

    // Recv channel (kernel writes, guest reads)
    let recv_mapping = SharedMemory::attach_shared(shared_id, recv_offset, recv_len)
        .map_err(|e| GuestError::Host(e.to_string()))?;
    let recv_region = ChannelRegion::from_mapping(
        recv_mapping,
        (recv_len as u64).saturating_sub(REGION_HEADER_BYTES),
    );

    // Send channel (guest writes, kernel reads)
    let send_mapping = SharedMemory::attach_shared(shared_id, send_offset, send_len)
        .map_err(|e| GuestError::Host(e.to_string()))?;
    let send_region = ChannelRegion::from_mapping(
        send_mapping,
        (send_len as u64).saturating_sub(REGION_HEADER_BYTES),
    );

    // Read signal ids from region headers
    let recv_signal_id = recv_region
        .read_header_u64(SIGNAL_SHARED_ID_OFFSET)
        .map_err(|e| GuestError::Host(e.to_string()))?;
    let send_signal_id = send_region
        .read_header_u64(SIGNAL_SHARED_ID_OFFSET)
        .map_err(|e| GuestError::Host(e.to_string()))?;

    let recv_signal =
        Signal::attach(recv_signal_id).map_err(|e| GuestError::Host(e.to_string()))?;
    let send_signal =
        Signal::attach(send_signal_id).map_err(|e| GuestError::Host(e.to_string()))?;

    // Create recv StrongReader
    let recv_tail = recv_region
        .read_next_tail()
        .map_err(|e| GuestError::Host(e.to_string()))?;
    let reader_id = recv_region
        .allocate_reader_slot(recv_tail)
        .map_err(|e| GuestError::Host(e.to_string()))?;
    let recv_reader = StrongReader::new(recv_region, recv_tail, reader_id);

    // Create send StrongWriter with auto-notify signal
    send_region
        .increment_writer_count()
        .map_err(|e| GuestError::Host(e.to_string()))?;
    let writer_id = send_region
        .allocate_writer_id()
        .map_err(|e| GuestError::Host(e.to_string()))?;
    let send_writer = StrongWriter::new(send_region, writer_id, Some(send_signal.clone()));

    Ok(UdpChannelLayout {
        recv_reader,
        recv_signal,
        send_writer,
        send_signal,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attach_shared_with_invalid_region_fails() {
        let result = UdpSocket::attach_shared(0);
        assert!(matches!(result, Err(GuestError::Host(_))));
    }
}
