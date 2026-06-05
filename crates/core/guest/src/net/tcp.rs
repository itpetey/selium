//! TCP stream and listener backed by shared-memory ring buffers.
//!
//! This module is a stub. The full implementation relied on the removed
//! `Signal` and host-mediated `SharedMemory` APIs. It will be re-implemented
//! against the new `alloc_region` / `attach_region` ABI in a follow-up
//! networking change.

use std::{
    io,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
};

use selium_abi::{HostcallOutput, HostcallRequest};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::{
    GuestError, Result,
    hostcall::{hostcall_async},
    resource::{Accept, IncomingConnection, ResourceListener},
};

/// A TCP stream backed by shared-memory ring buffers (stub).
pub struct TcpStream {
    _private: (),
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
    pub async fn connect(address: impl Into<String>) -> Result<Self> {
        let _descriptor = match hostcall_async(HostcallRequest::TcpConnect {
            address: address.into(),
        })
        .await?
        {
            HostcallOutput::SharedRegion(descriptor) => descriptor,
            _ => return Err(GuestError::UnexpectedHostcallOutput),
        };
        Err(GuestError::Host(
            "TCP stream not yet implemented against new shared memory ABI".to_string(),
        ))
    }

    /// Attaches to an existing shared region containing TCP stream ring buffers.
    pub fn attach_shared(_shared_id: u64) -> Result<Self> {
        Err(GuestError::Host(
            "TCP stream not yet implemented against new shared memory ABI".to_string(),
        ))
    }
}

impl AsyncRead for TcpStream {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Ready(Err(io::Error::other("TCP stream stub")))
    }
}

impl AsyncWrite for TcpStream {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Err(io::Error::other("TCP stream stub")))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
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

    fn accept(_connection: IncomingConnection) -> Result<Self::Item> {
        TcpStream::attach_shared(0)
    }
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
