//! QUIC handshake spike: a real TLS 1.3 handshake through the connector's
//! production [`build_endpoint`] seam.
//!
//! The production [`QuicUdpSocket`] and [`ConnectorRuntime`] are WASM/shm
//! adapters that need the guest hostcalls, so they cannot drive a real
//! handshake in a native test. This test therefore substitutes two native
//! test doubles — a `tokio` UDP socket adapter and a `tokio` runtime — and
//! runs [`build_endpoint`] exactly as the entrypoint does, then completes a
//! handshake against a host-side quinn client and round-trips bytes on a
//! bidirectional stream.
//!
//! The shm adapter/runtime types themselves are compile-verified against
//! quinn's trait bounds in the crate's `tests` module (`cargo check`), and
//! their wire behaviour is exercised end-to-end in the runtime substrate
//! tests once the guest is built for wasm32.

use std::{
    future::Future,
    io::{self, IoSliceMut},
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use parking_lot::Mutex;
use quinn::{
    ClientConfig, ServerConfig,
    udp::{RecvMeta, Transmit},
};
use selium_connector_quic::{
    build_endpoint, handle_connection,
    resolve::RouteResolver,
    runtime::{ConnectorRuntime, ConnectorTimer},
    sni_of,
    udp_adapter::QuicUdpSocket,
};

const CERT_DER: &[u8] = include_bytes!("fixtures/cert.der");
const KEY_DER: &[u8] = include_bytes!("fixtures/key.der");

/// A native test-only `quinn::AsyncUdpSocket` over `tokio::net::UdpSocket`.
struct TokioUdpSocket {
    inner: Arc<tokio::net::UdpSocket>,
    buf: Mutex<Vec<u8>>,
}

struct TokioUdpPoller {
    socket: Arc<TokioUdpSocket>,
    writable: Option<Pin<Box<dyn Future<Output = io::Result<()>> + Send + Sync>>>,
}

/// A native test-only `quinn::Runtime` over the tokio executor.
#[derive(Debug, Default)]
struct TokioRuntime;

struct TokioTimer {
    deadline: std::time::Instant,
    sleep: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl quinn::AsyncUdpSocket for TokioUdpSocket {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn quinn::UdpPoller>> {
        Box::pin(TokioUdpPoller {
            socket: self,
            writable: None,
        })
    }

    fn try_send(&self, transmit: &Transmit) -> io::Result<()> {
        self.inner
            .try_send_to(transmit.contents, transmit.destination)
            .map(|_| ())
    }

    fn poll_recv(
        &self,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [RecvMeta],
    ) -> Poll<io::Result<usize>> {
        let mut guard = self.buf.lock();
        let buf = &mut *guard;
        buf.resize(65536, 0);
        let mut read_buf = tokio::io::ReadBuf::new(buf);
        match self.inner.poll_recv_from(cx, &mut read_buf) {
            Poll::Ready(Ok(addr)) => {
                let n = read_buf.filled().len();
                if let (Some(dst), Some(meta_slot)) = (bufs.first_mut(), meta.first_mut()) {
                    dst[..n].copy_from_slice(&read_buf.filled()[..n]);
                    *meta_slot = RecvMeta {
                        addr,
                        len: n,
                        stride: n,
                        ecn: None,
                        dst_ip: None,
                    };
                    Poll::Ready(Ok(1))
                } else {
                    Poll::Ready(Ok(0))
                }
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }

    fn max_transmit_segments(&self) -> usize {
        1
    }

    fn max_receive_segments(&self) -> usize {
        1
    }

    fn may_fragment(&self) -> bool {
        false
    }
}

impl std::fmt::Debug for TokioUdpSocket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokioUdpSocket").finish_non_exhaustive()
    }
}

impl quinn::UdpPoller for TokioUdpPoller {
    fn poll_writable(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.writable.is_none() {
            let socket = self.socket.clone();
            self.writable = Some(Box::pin(async move { socket.inner.writable().await }));
        }
        let future = self.writable.as_mut().expect("writable future present");
        match Pin::new(future).poll(cx) {
            Poll::Ready(result) => {
                self.writable = None;
                Poll::Ready(result)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl std::fmt::Debug for TokioUdpPoller {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokioUdpPoller").finish_non_exhaustive()
    }
}

impl quinn::Runtime for TokioRuntime {
    fn new_timer(&self, deadline: std::time::Instant) -> Pin<Box<dyn quinn::AsyncTimer>> {
        Box::pin(TokioTimer {
            deadline,
            sleep: None,
        })
    }

    fn spawn(&self, future: Pin<Box<dyn Future<Output = ()> + Send>>) {
        tokio::spawn(future);
    }

    fn wrap_udp_socket(
        &self,
        _: std::net::UdpSocket,
    ) -> io::Result<Arc<dyn quinn::AsyncUdpSocket>> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "use new_with_abstract_socket",
        ))
    }

    fn now(&self) -> std::time::Instant {
        std::time::Instant::now()
    }
}

impl quinn::AsyncTimer for TokioTimer {
    fn reset(self: Pin<&mut Self>, deadline: std::time::Instant) {
        let this = self.get_mut();
        this.deadline = deadline;
        this.sleep = None;
    }

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        if std::time::Instant::now() >= this.deadline {
            this.sleep = None;
            return Poll::Ready(());
        }
        if this.sleep.is_none() {
            this.sleep = Some(Box::pin(tokio::time::sleep_until(
                tokio::time::Instant::from_std(this.deadline),
            )));
        }
        if let Some(sleep) = this.sleep.as_mut() {
            match Future::poll(sleep.as_mut(), cx) {
                Poll::Ready(()) => {
                    this.sleep = None;
                    Poll::Ready(())
                }
                Poll::Pending => Poll::Pending,
            }
        } else {
            Poll::Pending
        }
    }
}

impl std::fmt::Debug for TokioTimer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokioTimer")
            .field("deadline", &self.deadline)
            .finish()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn handshake_completes_and_relays_a_stream() {
    let (server_config, cert) = server_config();

    // Server endpoint over a loopback tokio UDP socket.
    let server_socket = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind server socket");
    let server_addr = server_socket.local_addr().expect("server addr");
    let server_socket = TokioUdpSocket {
        inner: Arc::new(server_socket),
        buf: Mutex::new(vec![0u8; 65536]),
    };
    let endpoint = build_endpoint(
        Arc::new(server_socket),
        Arc::new(TokioRuntime),
        Some(server_config),
    )
    .expect("build server endpoint");

    // Client endpoint with the self-signed cert trusted.
    let client_socket = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind client socket");
    let client_socket = TokioUdpSocket {
        inner: Arc::new(client_socket),
        buf: Mutex::new(vec![0u8; 65536]),
    };
    let mut client_endpoint = build_endpoint(Arc::new(client_socket), Arc::new(TokioRuntime), None)
        .expect("build client endpoint");

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert).expect("add root cert");
    let client_config =
        ClientConfig::with_root_certificates(Arc::new(roots)).expect("client config");
    client_endpoint.set_default_client_config(client_config);

    // Drive both sides concurrently: the client handshake runs in a spawned
    // task while the server accepts.
    let client_task = {
        let ep = client_endpoint.clone();
        tokio::spawn(async move {
            ep.connect(server_addr, "localhost")
                .expect("client connect")
                .await
                .expect("client connection")
        })
    };

    let server_conn = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let incoming = endpoint.accept().await.expect("server incoming");
        incoming
            .await
            .expect("server handshake completes (incoming.await yields a connection)")
    })
    .await
    .expect("handshake completed within timeout");

    let client_conn = client_task.await.expect("client task");

    // Drive the client stream concurrently: open, write "ping", read "pong".
    let client_stream_task = {
        let client_conn = client_conn.clone();
        tokio::spawn(async move {
            let (mut send, mut recv) = client_conn.open_bi().await.expect("client open_bi");
            send.write_all(b"ping").await.expect("client write");
            send.finish().expect("client finish");

            let mut buf = [0u8; 4];
            recv.read_exact(&mut buf).await.expect("client read");
            assert_eq!(&buf, b"pong");
        })
    };

    let (mut send, mut recv) = server_conn
        .accept_bi()
        .await
        .expect("server accepts bidirectional stream");

    let mut buf = [0u8; 4];
    recv.read_exact(&mut buf).await.expect("server read");
    assert_eq!(&buf, b"ping");

    send.write_all(b"pong").await.expect("server write");
    send.finish().expect("server finish");

    client_stream_task.await.expect("client stream task");

    drop(client_conn);
    drop(endpoint);
    drop(client_endpoint);
}

/// The production shm adapter + guest runtime satisfy quinn's trait bounds.
///
/// This is the compile-level verification for the wasm-only types (they cannot
/// drive a real handshake natively); the wire behaviour is exercised by the
/// runtime substrate tests once the guest is built for wasm32.
#[test]
fn production_adapter_types_satisfy_quinn_trait_bounds() {
    fn assert_udp<T: quinn::AsyncUdpSocket>() {}
    fn assert_runtime<T: quinn::Runtime>() {}
    fn assert_timer<T: quinn::AsyncTimer>() {}
    assert_udp::<QuicUdpSocket>();
    assert_runtime::<ConnectorRuntime>();
    assert_timer::<ConnectorTimer>();
}

/// Builds the server config from the embedded self-signed test certificate,
/// returning the certificate so the client can trust it.
fn server_config() -> (
    ServerConfig,
    quinn::rustls::pki_types::CertificateDer<'static>,
) {
    use quinn::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

    let cert = CertificateDer::from(CERT_DER.to_vec());
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(KEY_DER.to_vec()));
    let config = ServerConfig::with_single_cert(vec![cert.clone()], key).expect("server config");
    (config, cert)
}

/// Unknown SNI is refused: the connector closes the connection before ever
/// contacting an app guest (no discovery context = nothing to contact).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_sni_is_refused_without_guest_contact() {
    let (server_config, cert) = server_config();

    let server_socket = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind server socket");
    let server_addr = server_socket.local_addr().expect("server addr");
    let server_socket = TokioUdpSocket {
        inner: Arc::new(server_socket),
        buf: Mutex::new(vec![0u8; 65536]),
    };
    let endpoint = build_endpoint(
        Arc::new(server_socket),
        Arc::new(TokioRuntime),
        Some(server_config),
    )
    .expect("build server endpoint");

    let client_socket = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind client socket");
    let client_socket = TokioUdpSocket {
        inner: Arc::new(client_socket),
        buf: Mutex::new(vec![0u8; 65536]),
    };
    let mut client_endpoint = build_endpoint(Arc::new(client_socket), Arc::new(TokioRuntime), None)
        .expect("build client endpoint");
    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert).expect("add root cert");
    client_endpoint.set_default_client_config(
        ClientConfig::with_root_certificates(Arc::new(roots)).expect("client config"),
    );

    // Connect with a valid server name (so TLS succeeds) and drive both sides.
    let client_task = {
        let ep = client_endpoint.clone();
        tokio::spawn(async move {
            ep.connect(server_addr, "localhost")
                .expect("connect")
                .await
                .expect("client connection")
        })
    };
    let server_conn = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let incoming = endpoint.accept().await.expect("server incoming");
        incoming.await.expect("server handshake")
    })
    .await
    .expect("handshake completes");
    let client_conn = client_task.await.expect("client task");

    // The presented SNI is recovered from the handshake.
    assert_eq!(sni_of(&server_conn).as_deref(), Some("localhost"));

    // An empty resolver = no registered route: the connector must refuse and
    // close the connection without contacting any app guest.
    let resolver: selium_connector_quic::resolve::ResolverHandle =
        Arc::new(tokio::sync::Mutex::new(RouteResolver::empty()));
    handle_connection(server_conn.clone(), resolver).await;

    let closed = tokio::time::timeout(std::time::Duration::from_secs(5), server_conn.closed())
        .await
        .is_ok();
    assert!(closed, "unknown SNI must be refused and closed");

    drop(client_conn);
    drop(endpoint);
    drop(client_endpoint);
}
