//! Network capability for async network operations.
//!
//! Provides async network operations that yield to the host.

use parking_lot::RwLock;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::kernel::Capability;

#[derive(Debug, Clone)]
pub enum NetworkError {
    ConnectionRefused,
    Timeout,
    IoError(String),
    InvalidAddress,
}

impl std::fmt::Display for NetworkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NetworkError::ConnectionRefused => write!(f, "Connection refused"),
            NetworkError::Timeout => write!(f, "Connection timeout"),
            NetworkError::IoError(msg) => write!(f, "IO error: {}", msg),
            NetworkError::InvalidAddress => write!(f, "Invalid address"),
        }
    }
}

impl std::error::Error for NetworkError {}

pub type NetworkResult<T> = std::result::Result<T, NetworkError>;

pub struct NetworkCapability {
    listener: Arc<RwLock<Option<TcpListener>>>,
    bound_addr: Arc<RwLock<Option<SocketAddr>>>,
}

impl NetworkCapability {
    pub fn new() -> Self {
        Self {
            listener: Arc::new(RwLock::new(None)),
            bound_addr: Arc::new(RwLock::new(None)),
        }
    }

    pub async fn connect(&self, addr: &str) -> NetworkResult<TcpStream> {
        let addr = addr
            .parse::<SocketAddr>()
            .map_err(|_| NetworkError::InvalidAddress)?;

        TcpStream::connect(addr).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::ConnectionRefused {
                NetworkError::ConnectionRefused
            } else {
                NetworkError::IoError(e.to_string())
            }
        })
    }

    pub async fn bind(&self, addr: &str) -> NetworkResult<SocketAddr> {
        let addr = addr
            .parse::<SocketAddr>()
            .map_err(|_| NetworkError::InvalidAddress)?;

        let listener = TcpListener::bind(addr)
            .await
            .map_err(|e| NetworkError::IoError(e.to_string()))?;

        let local_addr = listener
            .local_addr()
            .map_err(|e| NetworkError::IoError(e.to_string()))?;

        *self.listener.write() = Some(listener);
        *self.bound_addr.write() = Some(local_addr);

        Ok(local_addr)
    }

    pub async fn accept(&self) -> NetworkResult<(TcpStream, SocketAddr)> {
        let listener = {
            let mut guard = self.listener.write();
            guard.take()
        }
        .ok_or(NetworkError::IoError("Not bound".to_string()))?;

        let result = listener.accept().await;

        // Put the listener back
        *self.listener.write() = Some(listener);

        result.map_err(|e| NetworkError::IoError(e.to_string()))
    }

    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.bound_addr.read().as_ref().copied()
    }

    pub async fn read(&self, stream: &mut TcpStream, buf: &mut [u8]) -> NetworkResult<usize> {
        stream
            .read(buf)
            .await
            .map_err(|e| NetworkError::IoError(e.to_string()))
    }

    pub async fn write(&self, stream: &mut TcpStream, buf: &[u8]) -> NetworkResult<usize> {
        stream
            .write(buf)
            .await
            .map_err(|e| NetworkError::IoError(e.to_string()))
    }
}

impl Default for NetworkCapability {
    fn default() -> Self {
        Self::new()
    }
}

impl Capability for NetworkCapability {
    fn name(&self) -> &'static str {
        "selium::network"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_network_connect_invalid_address() {
        let network = NetworkCapability::new();

        let result = network.connect("not-an-address").await;
        assert!(matches!(result, Err(NetworkError::InvalidAddress)));
    }

    #[tokio::test]
    async fn test_network_bind_and_accept() {
        let network = NetworkCapability::new();

        let addr = network.bind("127.0.0.1:0").await.unwrap();
        assert!(addr.port() > 0);

        let connect_handle = tokio::spawn({
            let network = NetworkCapability::new();
            async move {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                network.connect(&format!("127.0.0.1:{}", addr.port())).await
            }
        });

        let (stream, _peer_addr) = network.accept().await.unwrap();
        let _connected = connect_handle.await.unwrap().unwrap();

        assert!(stream.peer_addr().is_ok());
    }
}
