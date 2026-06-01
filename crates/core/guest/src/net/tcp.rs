use std::{
    future::Future,
    io,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
};

use selium_abi::{HostcallOutput, HostcallRequest};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::{
    GuestError, Result,
    hostcall::{HostcallFuture, hostcall_async},
    io::{
        ChannelRegion, REGION_HEADER_BYTES, SIGNAL_SHARED_ID_OFFSET,
        channels::{StrongReader, StrongWriter},
    },
    memory::{SHARED_REGION_MAGIC, SharedMemory},
    resource::{Accept, IncomingConnection, ResourceListener},
    signal::Signal,
};

#[cfg(feature = "axum")]
mod axum_impl {
    use super::TcpListener;
    use std::future::Future;
    use tokio::io;

    impl axum::serve::Listener for TcpListener {
        type Io = super::TcpStream;
        type Addr = std::net::SocketAddr;

        fn accept(&mut self) -> impl Future<Output = (Self::Io, Self::Addr)> + Send {
            async {
                loop {
                    match self.listener.accept::<super::TcpAccept>().await {
                        Ok(stream) => return (stream, self.local_addr()),
                        Err(_e) => {
                            // Sleep briefly and retry on error
                            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        }
                    }
                }
            }
        }

        fn local_addr(&self) -> io::Result<Self::Addr> {
            Ok(self.local_addr())
        }
    }
}

/// A TCP stream backed by shared-memory ring buffers.
pub struct TcpStream {
    inbound_reader: StrongReader,
    inbound_signal: Signal,
    outbound_writer: Option<StrongWriter>,
    outbound_signal: Signal,
    read_buffer: Vec<u8>,
    read_offset: usize,
    pending_read_wait: Option<HostcallFuture>,
    pending_write_wait: Option<HostcallFuture>,
}

/// A TCP listener that accepts incoming connections via the host.
pub struct TcpListener {
    pub(crate) listener: ResourceListener,
    pub(crate) local_addr: SocketAddr,
}

/// Accepts incoming TCP connections and produces `TcpStream` handles.
pub struct TcpAccept;

struct TcpChannelLayout {
    inbound_reader: StrongReader,
    inbound_signal: Signal,
    outbound_writer: StrongWriter,
    outbound_signal: Signal,
}

impl TcpStream {
    /// Connects to a remote TCP endpoint via the host.
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
    pub fn attach_shared(shared_id: u64) -> Result<Self> {
        let layout = attach_tcp_channels(shared_id)?;
        Ok(Self {
            inbound_reader: layout.inbound_reader,
            inbound_signal: layout.inbound_signal,
            outbound_writer: Some(layout.outbound_writer),
            outbound_signal: layout.outbound_signal,
            read_buffer: Vec::new(),
            read_offset: 0,
            pending_read_wait: None,
            pending_write_wait: None,
        })
    }

    /// Attempts to read the next frame payload without blocking.
    fn try_read_frame(&mut self) -> std::result::Result<Option<Vec<u8>>, GuestError> {
        match self.inbound_reader.read() {
            Ok((payload, _tag)) => Ok(Some(payload)),
            Err(crate::io::channels::Error::ChannelEmpty) => Ok(None),
            Err(e) => Err(GuestError::Io(crate::io::Error::Guest(e.to_string()))),
        }
    }
}

#[expect(clippy::indexing_slicing, reason = "bounds checked by min operations")]
impl AsyncRead for TcpStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        // Drain any buffered partial-frame bytes first.
        if self.read_offset < self.read_buffer.len() {
            let available = self.read_buffer.len() - self.read_offset;
            let to_copy = available.min(buf.remaining());
            buf.put_slice(&self.read_buffer[self.read_offset..self.read_offset + to_copy]);
            self.read_offset += to_copy;
            if self.read_offset >= self.read_buffer.len() {
                self.read_buffer.clear();
                self.read_offset = 0;
            }
            return Poll::Ready(Ok(()));
        }

        // If a previous signal wait is pending, poll it.
        if let Some(ref mut fut) = self.pending_read_wait {
            let poll = Pin::new(fut).poll(cx);
            match poll {
                Poll::Ready(Ok(HostcallOutput::SignalGeneration(_))) => {
                    self.pending_read_wait = None;
                }
                Poll::Ready(Ok(_)) => {
                    self.pending_read_wait = None;
                    return Poll::Ready(Err(io::Error::other(
                        "unexpected hostcall output during read wait",
                    )));
                }
                Poll::Ready(Err(e)) => {
                    self.pending_read_wait = None;
                    return Poll::Ready(Err(io::Error::other(e.to_string())));
                }
                Poll::Pending => return Poll::Pending,
            }
        }

        // Try to read a frame.
        match self.try_read_frame() {
            Ok(Some(payload)) => {
                let to_copy = payload.len().min(buf.remaining());
                buf.put_slice(&payload[..to_copy]);
                if to_copy < payload.len() {
                    self.read_buffer.extend_from_slice(&payload[to_copy..]);
                }
                Poll::Ready(Ok(()))
            }
            Ok(None) => {
                // Channel empty. Check writer count for EOF.
                match self
                    .inbound_reader
                    .region()
                    .read_writer_count()
                    .map_err(|e| io::Error::other(e.to_string()))
                {
                    Ok(0) => Poll::Ready(Ok(())), // EOF
                    Ok(_) => {
                        // Start a signal wait and return Pending.
                        let observed = self
                            .inbound_signal
                            .generation()
                            .map_err(|e| io::Error::other(e.to_string()))?;
                        self.pending_read_wait =
                            Some(hostcall_async(HostcallRequest::SignalWait {
                                local_id: self.inbound_signal.local_id(),
                                observed_generation: observed,
                                timeout_ms: 30_000,
                            }));
                        // Poll the newly created future once.
                        if let Some(ref mut fut) = self.pending_read_wait {
                            match Pin::new(fut).poll(cx) {
                                Poll::Ready(Ok(HostcallOutput::SignalGeneration(_))) => {
                                    self.pending_read_wait = None;
                                    // Signal already fired; try reading again.
                                    cx.waker().wake_by_ref();
                                    Poll::Pending
                                }
                                Poll::Ready(Ok(_)) => {
                                    Poll::Ready(Err(io::Error::other("unexpected hostcall output")))
                                }
                                Poll::Ready(Err(e)) => {
                                    Poll::Ready(Err(io::Error::other(e.to_string())))
                                }
                                Poll::Pending => Poll::Pending,
                            }
                        } else {
                            Poll::Pending
                        }
                    }
                    Err(e) => Poll::Ready(Err(e)),
                }
            }
            Err(e) => Poll::Ready(Err(io::Error::other(e.to_string()))),
        }
    }
}

impl AsyncWrite for TcpStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        // If a previous signal wait is pending, poll it.
        if let Some(ref mut fut) = self.pending_write_wait {
            let poll = Pin::new(fut).poll(cx);
            match poll {
                Poll::Ready(Ok(HostcallOutput::SignalGeneration(_))) => {
                    self.pending_write_wait = None;
                }
                Poll::Ready(Ok(_)) => {
                    self.pending_write_wait = None;
                    return Poll::Ready(Err(io::Error::other(
                        "unexpected hostcall output during write wait",
                    )));
                }
                Poll::Ready(Err(e)) => {
                    self.pending_write_wait = None;
                    return Poll::Ready(Err(io::Error::other(e.to_string())));
                }
                Poll::Pending => return Poll::Pending,
            }
        }

        let writer = match self.outbound_writer.as_mut() {
            Some(w) => w,
            None => {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::NotConnected,
                    "stream shut down",
                )));
            }
        };

        // Try to write the entire buffer as one frame.
        match writer.write(buf) {
            Ok(()) => Poll::Ready(Ok(buf.len())),
            Err(crate::io::channels::Error::ChannelFull) => {
                // Start a signal wait on the cached outbound signal.
                let observed = self
                    .outbound_signal
                    .generation()
                    .map_err(|e| io::Error::other(e.to_string()))?;

                self.pending_write_wait = Some(hostcall_async(HostcallRequest::SignalWait {
                    local_id: self.outbound_signal.local_id(),
                    observed_generation: observed,
                    timeout_ms: 30_000,
                }));

                if let Some(ref mut fut) = self.pending_write_wait {
                    match Pin::new(fut).poll(cx) {
                        Poll::Ready(Ok(HostcallOutput::SignalGeneration(_))) => {
                            self.pending_write_wait = None;
                            cx.waker().wake_by_ref();
                            Poll::Pending
                        }
                        Poll::Ready(Ok(_)) => {
                            Poll::Ready(Err(io::Error::other("unexpected hostcall output")))
                        }
                        Poll::Ready(Err(e)) => Poll::Ready(Err(io::Error::other(e.to_string()))),
                        Poll::Pending => Poll::Pending,
                    }
                } else {
                    Poll::Pending
                }
            }
            Err(e) => Poll::Ready(Err(io::Error::other(format!("write error: {e}")))),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // Take the outbound writer, which drops StrongWriter and decrements
        // the writer count. Notify the outbound signal so the kernel proxy
        // detects close promptly.
        drop(self.outbound_writer.take());
        drop(self.outbound_signal.notify());
        Poll::Ready(Ok(()))
    }
}

impl Drop for TcpStream {
    fn drop(&mut self) {
        // If poll_shutdown was not called, StrongWriter::drop will decrement
        // the writer count. If it was already called, the writer is None.
        // Notify the outbound signal so the kernel proxy wakes up promptly
        // to detect writer_count == 0.
        drop(self.outbound_signal.notify());
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

fn attach_tcp_channels(shared_id: u64) -> Result<TcpChannelLayout> {
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

    let (in_offset, in_len) = header
        .memory(0)
        .map_err(|e| GuestError::Host(e.to_string()))?;
    let (out_offset, out_len) = header
        .memory(1)
        .map_err(|e| GuestError::Host(e.to_string()))?;

    header
        .detach()
        .map_err(|e| GuestError::Host(e.to_string()))?;

    // Inbound channel (kernel writes, guest reads)
    let in_mapping = SharedMemory::attach_shared(shared_id, in_offset, in_len)
        .map_err(|e| GuestError::Host(e.to_string()))?;
    let in_region = ChannelRegion::from_mapping(
        in_mapping,
        (in_len as u64).saturating_sub(REGION_HEADER_BYTES),
    );

    // Outbound channel (guest writes, kernel reads)
    let out_mapping = SharedMemory::attach_shared(shared_id, out_offset, out_len)
        .map_err(|e| GuestError::Host(e.to_string()))?;
    let out_region = ChannelRegion::from_mapping(
        out_mapping,
        (out_len as u64).saturating_sub(REGION_HEADER_BYTES),
    );

    // Read signal ids from region headers
    let in_signal_id = in_region
        .read_header_u64(SIGNAL_SHARED_ID_OFFSET)
        .map_err(|e| GuestError::Host(e.to_string()))?;
    let out_signal_id = out_region
        .read_header_u64(SIGNAL_SHARED_ID_OFFSET)
        .map_err(|e| GuestError::Host(e.to_string()))?;

    let inbound_signal =
        Signal::attach(in_signal_id).map_err(|e| GuestError::Host(e.to_string()))?;
    let outbound_signal =
        Signal::attach(out_signal_id).map_err(|e| GuestError::Host(e.to_string()))?;

    // Create inbound StrongReader
    let in_tail = in_region
        .read_next_tail()
        .map_err(|e| GuestError::Host(e.to_string()))?;
    let reader_id = in_region
        .allocate_reader_slot(in_tail)
        .map_err(|e| GuestError::Host(e.to_string()))?;
    let inbound_reader = StrongReader::new(in_region, in_tail, reader_id);

    // Create outbound StrongWriter with auto-notify signal
    out_region
        .increment_writer_count()
        .map_err(|e| GuestError::Host(e.to_string()))?;
    let writer_id = out_region
        .allocate_writer_id()
        .map_err(|e| GuestError::Host(e.to_string()))?;
    let outbound_writer = StrongWriter::new(out_region, writer_id, Some(outbound_signal.clone()));

    Ok(TcpChannelLayout {
        inbound_reader,
        inbound_signal,
        outbound_writer,
        outbound_signal,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resource::IncomingConnection;

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

    #[cfg(feature = "axum")]
    #[test]
    fn tcp_listener_impls_axum_listener() {
        // Full trait implementation check — implicitly verifies all associated
        // type bounds (Io = TcpStream with AsyncRead+AsyncWrite+Send+Unpin,
        // Addr = SocketAddr).
        fn assert_listener<L: axum::serve::Listener>() {}
        assert_listener::<TcpListener>();
    }
}
