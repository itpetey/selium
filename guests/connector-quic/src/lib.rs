//! QUIC edge connector system guest.
//!
//! Terminates external QUIC (TLS 1.3) at the edge with a quinn [`Endpoint`]
//! running over the guest's shared-memory [`UdpSocket`], and relays each
//! accepted bidirectional stream's bytes over per-stream shared-memory
//! channels, so application guests serve QUIC byte transport with **zero
//! `Network` grants** and no quinn dependency of their own.
//!
//! # Architecture
//!
//! - The [`QuicUdpSocket`](udp_adapter::QuicUdpSocket) adapter maps quinn
//!   datagram I/O onto the shm send/recv rings; the
//!   [`ConnectorRuntime`](runtime::ConnectorRuntime) gives quinn its executor
//!   and timers.
//! - TLS server material (certificate + key) is loaded from blob storage via
//!   the connector's `Storage` grant, failing loudly when missing or invalid.
//! - One quinn server endpoint accepts connections; the serving guest for each
//!   connection is resolved from the handshake SNI (`sel-quic://<name>`), and
//!   each accepted bidirectional stream is relayed over its own two-ring
//!   shared-memory channel (see [`pipeline`]).

use std::{net::SocketAddr, sync::Arc};

use quinn::ServerConfig;
use rustls_pemfile as pemfile;
use selium_guest::{
    Context, ResourceSender, UdpSocket, entrypoint, error, info, mark_ready, spawn, warn,
};

// Feature-unification anchor, not a code dependency: pulls in `ring` (with its
// `wasm32_unknown_unknown_js` feature) so `SystemRandom` compiles on
// wasm32-unknown-unknown — the backend actually used is getrandom's `custom`
// (see `.cargo/config.toml`). Guards against cargo-shear removing the dep.
#[cfg(target_arch = "wasm32")]
use ring as _;

use crate::{
    pipeline::relay_stream,
    resolve::{ResolveError, ResolverHandle, RouteResolver},
    runtime::ConnectorRuntime,
    udp_adapter::QuicUdpSocket,
};

pub mod pipeline;
pub mod resolve;
pub mod runtime;
pub mod stream;
pub mod udp_adapter;

/// Manifest name for the certificate chain PEM.
const TLS_CERT_MANIFEST: &str = "cert-pem";
/// Manifest name for the private key PEM.
const TLS_KEY_MANIFEST: &str = "key-pem";
/// Storage blob store name for TLS material.
const TLS_STORE_NAME: &str = "tls-certs";

#[derive(Debug)]
enum TlsError {
    StorageUnavailable,
    MissingCertificate,
    MissingKey,
    InvalidCertificate,
    InvalidKey,
    ConfigError,
}

impl std::fmt::Display for TlsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TlsError::StorageUnavailable => write!(f, "TLS storage unavailable"),
            TlsError::MissingCertificate => write!(f, "TLS certificate not found"),
            TlsError::MissingKey => write!(f, "TLS private key not found"),
            TlsError::InvalidCertificate => write!(f, "invalid TLS certificate"),
            TlsError::InvalidKey => write!(f, "invalid TLS private key"),
            TlsError::ConfigError => write!(f, "TLS configuration error"),
        }
    }
}

/// Custom `getrandom` backend for wasm32, invoked by the `getrandom` crate
/// when built with the `custom` backend (`getrandom_backend = "custom"`).
///
/// # Safety
/// The contract is defined by `getrandom`: `dest` must be valid for writes of
/// `len` bytes, and on success the entire buffer must be initialised.
#[cfg(target_arch = "wasm32")]
#[unsafe(no_mangle)]
unsafe extern "Rust" fn __getrandom_v03_custom(
    dest: *mut u8,
    len: usize,
) -> Result<(), getrandom::Error> {
    use selium_guest::random_bytes;

    if len == 0 {
        return Ok(());
    }

    let bytes = match random_bytes(len as u32) {
        Ok(bytes) => bytes,
        Err(_) => return Err(getrandom::Error::UNEXPECTED),
    };

    // SAFETY: `getrandom` guarantees `dest` is valid for `len` bytes of writes.
    unsafe {
        core::ptr::copy_nonoverlapping(bytes.as_ptr(), dest, len);
    }
    Ok(())
}

/// Registers the wasm32 time source backing `web_time::Instant`, forwarding to
/// the hostcall monotonic and wall clocks.
///
/// Must run before any TLS/quinn operation on wasm32.
#[cfg(target_arch = "wasm32")]
pub fn register_wasm_time_source() {
    web_time::set_custom_time_source(web_time::TimeSource {
        monotonic_ns: || {
            selium_guest::time::Instant::now()
                .expect("TimeMonotonic hostcall")
                .as_nanos()
        },
        wall_clock_ns: || selium_guest::time::now().expect("TimeNow hostcall"),
    });
}

/// Builds a quinn server endpoint from an abstract UDP socket and runtime.
///
/// This is the quinn-on-wasm32 seam: every endpoint in this crate is built via
/// [`quinn::Endpoint::new_with_abstract_socket`], with the connector supplying
/// both halves quinn needs (the shm datagram adapter and the guest runtime).
pub fn build_endpoint(
    socket: Arc<dyn quinn::AsyncUdpSocket>,
    runtime: Arc<dyn quinn::Runtime>,
    server_config: Option<ServerConfig>,
) -> std::io::Result<quinn::Endpoint> {
    quinn::Endpoint::new_with_abstract_socket(
        quinn::EndpointConfig::default(),
        server_config,
        socket,
        runtime,
    )
}

/// Entrypoint for the QUIC connector system guest.
///
/// Receives a discovery `Context` for SNI route resolution. On wasm32 the
/// host provides randomness and time through hostcalls; both backends are
/// registered before any TLS operation. Certificates are loaded from blob
/// storage (loud failure on missing/invalid material), a UDP socket is bound,
/// and the quinn endpoint accepts connections. Each accepted connection is
/// routed by SNI and served by its own relay task.
#[entrypoint]
async fn connector_quic(ctx: Context) {
    #[cfg(target_arch = "wasm32")]
    register_wasm_time_source();

    drop(selium_guest::log::init());
    info!("quic-connector: started");

    let server_config = match load_server_config() {
        Ok(config) => {
            info!("quic-connector: TLS configured");
            config
        }
        Err(e) => {
            error!("quic-connector: TLS setup failed: {e}");
            error!("quic-connector: refusing to serve QUIC without TLS material");
            return;
        }
    };

    let local_addr: SocketAddr = match QUIC_LISTEN_ADDR.parse() {
        Ok(addr) => addr,
        Err(e) => {
            error!("quic-connector: invalid listen address: {e}");
            return;
        }
    };

    let socket = match UdpSocket::bind(QUIC_LISTEN_ADDR).await {
        Ok(socket) => socket,
        Err(e) => {
            error!("quic-connector: UDP bind failed: {e}");
            return;
        }
    };

    let quic_socket = QuicUdpSocket::new(socket, local_addr);
    let endpoint = match build_endpoint(
        Arc::new(quic_socket),
        Arc::new(ConnectorRuntime),
        Some(server_config),
    ) {
        Ok(endpoint) => endpoint,
        Err(e) => {
            error!("quic-connector: endpoint creation failed: {e}");
            return;
        }
    };

    info!("quic-connector: listening on {QUIC_LISTEN_ADDR}");
    mark_ready();

    let resolver: ResolverHandle = Arc::new(tokio::sync::Mutex::new(RouteResolver::new(ctx)));

    loop {
        let incoming = match endpoint.accept().await {
            Some(incoming) => incoming,
            None => return,
        };

        let connection = match incoming.await {
            Ok(connection) => connection,
            Err(e) => {
                warn!("quic-connector: incoming connection failed: {e}");
                continue;
            }
        };

        info!("quic-connector: QUIC handshake complete");
        let resolver = resolver.clone();
        spawn(async move {
            handle_connection(connection, resolver).await;
        });
    }
}

/// Serves one QUIC connection: resolve its serving guest once from SNI, then
/// relay every accepted bidirectional stream to that guest over a per-stream
/// byte channel.
///
/// Exposed for the connector's integration tests: the refusal path (unknown
/// or absent SNI) closes the connection before any guest contact.
pub async fn handle_connection(connection: quinn::Connection, resolver: ResolverHandle) {
    // Route from the handshake SNI. Unknown/absent SNI refuses the connection
    // without ever contacting an app guest.
    let Some(server_name) = sni_of(&connection) else {
        warn!("quic-connector: refusing connection with no SNI");
        connection.close(REFUSE_ERROR_CODE.into(), b"no server name");
        return;
    };

    let target = match resolver.lock().await.resolve(&server_name).await {
        Ok(target) => target,
        Err(ResolveError::NotFound) => {
            warn!("quic-connector: refusing connection: no route for {server_name}");
            connection.close(REFUSE_ERROR_CODE.into(), b"unknown server name");
            return;
        }
    };

    // Deliver every accepted stream over its own byte channel.
    let sender = match ResourceSender::attach(target.resource_id) {
        Ok(sender) => sender,
        Err(e) => {
            warn!("quic-connector: attach to guest queue failed: {e}");
            return;
        }
    };

    loop {
        let (send, recv) = match connection.accept_bi().await {
            Ok(streams) => streams,
            Err(e) => {
                warn!("quic-connector: accept_bi failed: {e}");
                break;
            }
        };

        let channel = match crate::stream::QuicChannel::allocate() {
            Ok(channel) => channel,
            Err(e) => {
                warn!("quic-connector: stream channel allocation failed: {e}");
                continue;
            }
        };

        if let Err(e) = sender.send(channel.shared_id()).await {
            // Stale route: evict so the next connection re-resolves.
            warn!("quic-connector: stream delivery failed: {e}");
            resolver.lock().await.evict(&server_name);
            continue;
        }

        let (guest_reader, guest_writer) = channel.into_halves();
        spawn(relay_stream(recv, send, guest_reader, guest_writer));
    }
}

/// Extracts the rustls server name (SNI) from an established connection.
pub fn sni_of(connection: &quinn::Connection) -> Option<String> {
    let data = connection.handshake_data()?;
    let handshake = data
        .downcast::<quinn::crypto::rustls::HandshakeData>()
        .ok()?;
    handshake.server_name
}

/// QUIC close code used for handshake refusal (crypto error, unspecified).
const REFUSE_ERROR_CODE: u32 = 0x100;

/// Default listener address for the QUIC connector.
///
/// Deferred policy: recorded in the connector's config, not spec behaviour
/// (see design open questions).
const QUIC_LISTEN_ADDR: &str = "0.0.0.0:4433";

/// Loads the QUIC server TLS config from storage via the connector's
/// `Storage` grant. Fails loudly on missing or invalid material.
fn load_server_config() -> Result<ServerConfig, TlsError> {
    use rustls_pki_types::{CertificateDer, PrivateKeyDer};
    use selium_guest::BlobStore;

    let store = BlobStore::open(TLS_STORE_NAME).map_err(|e| {
        error!("quic-connector: failed to open blob store '{TLS_STORE_NAME}': {e}");
        TlsError::StorageUnavailable
    })?;

    let cert_blob_id = store
        .manifest(TLS_CERT_MANIFEST)
        .map_err(|e| {
            error!("quic-connector: cert manifest '{TLS_CERT_MANIFEST}' not found: {e}");
            TlsError::MissingCertificate
        })?
        .ok_or_else(|| {
            error!("quic-connector: cert manifest '{TLS_CERT_MANIFEST}' is empty");
            TlsError::MissingCertificate
        })?;
    let cert_pem = store
        .get(&cert_blob_id)
        .map_err(|e| {
            error!("quic-connector: failed to read cert blob: {e}");
            TlsError::MissingCertificate
        })?
        .ok_or_else(|| {
            error!("quic-connector: cert blob is empty");
            TlsError::MissingCertificate
        })?;

    let key_blob_id = store
        .manifest(TLS_KEY_MANIFEST)
        .map_err(|e| {
            error!("quic-connector: key manifest '{TLS_KEY_MANIFEST}' not found: {e}");
            TlsError::MissingKey
        })?
        .ok_or_else(|| {
            error!("quic-connector: key manifest '{TLS_KEY_MANIFEST}' is empty");
            TlsError::MissingKey
        })?;
    let key_pem = store
        .get(&key_blob_id)
        .map_err(|e| {
            error!("quic-connector: failed to read key blob: {e}");
            TlsError::MissingKey
        })?
        .ok_or_else(|| {
            error!("quic-connector: key blob is empty");
            TlsError::MissingKey
        })?;

    let mut cert_reader = std::io::BufReader::new(cert_pem.as_slice());
    let certs: Vec<CertificateDer<'static>> = pemfile::certs(&mut cert_reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| {
            error!("quic-connector: invalid cert PEM: {e}");
            TlsError::InvalidCertificate
        })?;

    if certs.is_empty() {
        error!("quic-connector: empty certificate chain");
        return Err(TlsError::InvalidCertificate);
    }

    let mut key_reader = std::io::BufReader::new(key_pem.as_slice());
    let key = loop {
        match pemfile::read_one(&mut key_reader).map_err(|e| {
            error!("quic-connector: invalid key PEM: {e}");
            TlsError::InvalidKey
        })? {
            Some(pemfile::Item::Pkcs1Key(k)) => break PrivateKeyDer::Pkcs1(k),
            Some(pemfile::Item::Pkcs8Key(k)) => break PrivateKeyDer::Pkcs8(k),
            Some(pemfile::Item::Sec1Key(k)) => break PrivateKeyDer::Sec1(k),
            None => {
                error!("quic-connector: no private key found in key PEM");
                return Err(TlsError::InvalidKey);
            }
            _ => continue,
        }
    };

    ServerConfig::with_single_cert(certs, key).map_err(|e| {
        error!("quic-connector: failed to build TLS config: {e}");
        TlsError::ConfigError
    })
}
