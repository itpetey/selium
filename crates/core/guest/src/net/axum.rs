use std::future::Future;

use tokio::io;

use crate::{TcpAccept, TcpListener, TcpStream};

impl axum::serve::Listener for TcpListener {
    type Io = TcpStream;
    type Addr = std::net::SocketAddr;

    fn accept(&mut self) -> impl Future<Output = (Self::Io, Self::Addr)> + Send {
        async {
            loop {
                match self.listener.accept::<TcpAccept>().await {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tcp_listener_impls_axum_listener() {
        // Full trait implementation check — implicitly verifies all associated
        // type bounds (Io = TcpStream with AsyncRead+AsyncWrite+Send+Unpin,
        // Addr = SocketAddr).
        fn assert_listener<L: axum::serve::Listener>() {}
        assert_listener::<TcpListener>();
    }
}
