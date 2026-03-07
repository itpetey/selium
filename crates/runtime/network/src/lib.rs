//! Protocol-driven network runtime for Selium guests.

use std::{
    collections::{BTreeMap, HashMap},
    convert::Infallible,
    fmt,
    net::{SocketAddr, ToSocketAddrs},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, anyhow};
use bytes::Bytes;
use futures_util::stream;
use http_body_util::{BodyExt, Full, StreamBody, combinators::BoxBody};
use hyper::{
    Request, Response,
    body::{Frame, Incoming},
    service::service_fn,
};
use hyper_util::rt::TokioIo;
use quinn::crypto::rustls::QuicServerConfig;
use quinn::{Connection, Endpoint, RecvStream, SendStream};
use rustls::{RootCertStore, pki_types::PrivateKeyDer};
use rustls_pemfile::{certs, private_key};
use selium_abi::{
    InteractionKind, NetworkConnect, NetworkListen, NetworkProtocol, NetworkRpcAwait,
    NetworkRpcBodyRead, NetworkRpcBodyReadResult, NetworkRpcBodyWrite, NetworkRpcInvoke,
    NetworkRpcRequestHead, NetworkRpcRespond, NetworkRpcResponseHead, NetworkStatus,
    NetworkStatusCode, NetworkStreamChunk, NetworkStreamRecv, NetworkStreamRecvResult,
    NetworkStreamSend,
};
use selium_control_plane_protocol::{read_framed, write_framed};
use selium_kernel::{
    guest_error::GuestError,
    spi::network::{
        AcceptedRpc, AcceptedSession, AcceptedStream, AwaitedRpc, ListenerHandle,
        NetworkCapability, NetworkFuture, NetworkProcessPolicy, RespondedRpc, RpcBodyReaderHandle,
        RpcBodyWriterHandle, SessionHandle, StartedRpc, StreamHandle,
    },
};
use thiserror::Error;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Mutex, RwLock, mpsc, oneshot},
    task::JoinHandle,
    time::{Duration, timeout},
};
use tokio_rustls::{TlsAcceptor, TlsConnector};

#[derive(Debug, Clone)]
pub struct NetworkEgressProfile {
    pub name: String,
    pub protocol: NetworkProtocol,
    pub interactions: Vec<InteractionKind>,
    pub allowed_authorities: Vec<String>,
    pub ca_cert_path: PathBuf,
    pub client_cert_path: Option<PathBuf>,
    pub client_key_path: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct NetworkIngressBinding {
    pub name: String,
    pub protocol: NetworkProtocol,
    pub interactions: Vec<InteractionKind>,
    pub listen_addr: String,
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
}

#[derive(Debug, Error)]
pub enum NetworkError {
    #[error("network policy denied access")]
    PermissionDenied,
    #[error("network protocol or interaction unsupported")]
    Unsupported,
    #[error("network resource not found")]
    NotFound,
    #[error("network operation timed out")]
    Timeout,
    #[error("network operation would block")]
    WouldBlock,
    #[error("network resource closed")]
    Closed,
    #[error("invalid network argument")]
    InvalidArgument,
    #[error("{0}")]
    Other(String),
}

trait ProtocolDriver: Send + Sync {
    fn listen(
        &self,
        binding: NetworkIngressBinding,
    ) -> NetworkFuture<RuntimeListener, NetworkError>;

    fn connect(
        &self,
        profile: NetworkEgressProfile,
        authority: String,
    ) -> NetworkFuture<RuntimeSession, NetworkError>;
}

trait ProtocolListener: Send + Sync {
    fn protocol(&self) -> NetworkProtocol;
    fn interactions(&self) -> Vec<InteractionKind>;
    fn accept(
        &self,
        timeout_ms: u32,
    ) -> NetworkFuture<AcceptedSession<RuntimeSession>, NetworkError>;
    fn close(&self) -> NetworkFuture<NetworkStatus, NetworkError>;
}

trait ProtocolSession: Send + Sync {
    fn protocol(&self) -> NetworkProtocol;
    fn interactions(&self) -> Vec<InteractionKind>;
    fn stream_open(&self) -> NetworkFuture<AcceptedStream<RuntimeStream>, NetworkError>;
    fn stream_accept(
        &self,
        timeout_ms: u32,
    ) -> NetworkFuture<AcceptedStream<RuntimeStream>, NetworkError>;
    fn stream_supported(&self) -> bool;
    fn rpc_invoke(
        &self,
        input: NetworkRpcInvoke,
    ) -> NetworkFuture<StartedRpc<RuntimeRpcClientExchange, RuntimeRpcBodyWriter>, NetworkError>;
    fn rpc_accept(
        &self,
        timeout_ms: u32,
    ) -> NetworkFuture<AcceptedRpc<RuntimeRpcExchange, RuntimeRpcBodyReader>, NetworkError>;
    fn rpc_supported(&self) -> bool;
    fn close(&self) -> NetworkFuture<NetworkStatus, NetworkError>;
}

trait ProtocolStream: Send + Sync {
    fn send(&self, input: NetworkStreamSend) -> NetworkFuture<NetworkStatus, NetworkError>;
    fn recv(
        &self,
        input: NetworkStreamRecv,
    ) -> NetworkFuture<NetworkStreamRecvResult, NetworkError>;
    fn close(&self) -> NetworkFuture<NetworkStatus, NetworkError>;
}

trait ProtocolRpcExchange: Send {
    fn respond(
        self: Box<Self>,
        input: NetworkRpcRespond,
    ) -> NetworkFuture<RespondedRpc<RuntimeRpcBodyWriter>, NetworkError>;
    fn close(self: Box<Self>) -> NetworkFuture<NetworkStatus, NetworkError>;
}

trait ProtocolRpcClientExchange: Send {
    fn await_response(
        self: Box<Self>,
        input: NetworkRpcAwait,
    ) -> NetworkFuture<AwaitedRpc<RuntimeRpcBodyReader>, NetworkError>;
    fn close(self: Box<Self>) -> NetworkFuture<NetworkStatus, NetworkError>;
}

trait ProtocolRpcBodyReader: Send + Sync {
    fn read(
        &self,
        input: NetworkRpcBodyRead,
    ) -> NetworkFuture<NetworkRpcBodyReadResult, NetworkError>;
    fn close(&self) -> NetworkFuture<NetworkStatus, NetworkError>;
}

trait ProtocolRpcBodyWriter: Send + Sync {
    fn write(&self, input: NetworkRpcBodyWrite) -> NetworkFuture<NetworkStatus, NetworkError>;
    fn close(&self) -> NetworkFuture<NetworkStatus, NetworkError>;
}

#[derive(Clone)]
pub struct RuntimeListener {
    inner: Arc<dyn ProtocolListener>,
}

#[derive(Clone)]
pub struct RuntimeSession {
    inner: Arc<dyn ProtocolSession>,
}

#[derive(Clone)]
pub struct RuntimeStream {
    inner: Arc<dyn ProtocolStream>,
}

pub struct RuntimeRpcExchange {
    inner: Box<dyn ProtocolRpcExchange>,
}

pub struct RuntimeRpcClientExchange {
    inner: Box<dyn ProtocolRpcClientExchange>,
}

#[derive(Clone)]
pub struct RuntimeRpcBodyReader {
    inner: Arc<dyn ProtocolRpcBodyReader>,
}

#[derive(Clone)]
pub struct RuntimeRpcBodyWriter {
    inner: Arc<dyn ProtocolRpcBodyWriter>,
}

impl RuntimeListener {
    fn new(inner: impl ProtocolListener + 'static) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }

    fn protocol(&self) -> NetworkProtocol {
        self.inner.protocol()
    }

    fn interactions(&self) -> Vec<InteractionKind> {
        self.inner.interactions()
    }
}

impl RuntimeSession {
    fn new(inner: impl ProtocolSession + 'static) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }

    fn protocol(&self) -> NetworkProtocol {
        self.inner.protocol()
    }

    fn interactions(&self) -> Vec<InteractionKind> {
        self.inner.interactions()
    }
}

impl RuntimeStream {
    fn new(inner: impl ProtocolStream + 'static) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

impl RuntimeRpcExchange {
    fn new(inner: impl ProtocolRpcExchange + 'static) -> Self {
        Self {
            inner: Box::new(inner),
        }
    }
}

impl RuntimeRpcClientExchange {
    fn new(inner: impl ProtocolRpcClientExchange + 'static) -> Self {
        Self {
            inner: Box::new(inner),
        }
    }
}

impl RuntimeRpcBodyReader {
    fn new(inner: impl ProtocolRpcBodyReader + 'static) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

impl RuntimeRpcBodyWriter {
    fn new(inner: impl ProtocolRpcBodyWriter + 'static) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

impl fmt::Debug for RuntimeListener {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RuntimeListener")
            .field("protocol", &self.protocol())
            .field("interactions", &self.interactions())
            .finish()
    }
}

impl fmt::Debug for RuntimeSession {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RuntimeSession")
            .field("protocol", &self.protocol())
            .field("interactions", &self.interactions())
            .finish()
    }
}

impl fmt::Debug for RuntimeStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RuntimeStream(..)")
    }
}

impl fmt::Debug for RuntimeRpcExchange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RuntimeRpcExchange(..)")
    }
}

impl fmt::Debug for RuntimeRpcClientExchange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RuntimeRpcClientExchange(..)")
    }
}

impl fmt::Debug for RuntimeRpcBodyReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RuntimeRpcBodyReader(..)")
    }
}

impl fmt::Debug for RuntimeRpcBodyWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RuntimeRpcBodyWriter(..)")
    }
}

#[derive(Clone)]
pub struct NetworkService {
    drivers: Arc<HashMap<NetworkProtocol, Arc<dyn ProtocolDriver>>>,
    egress_profiles: Arc<RwLock<HashMap<String, NetworkEgressProfile>>>,
    ingress_bindings: Arc<RwLock<HashMap<String, NetworkIngressBinding>>>,
}

impl NetworkService {
    pub fn new() -> Arc<Self> {
        let mut drivers: HashMap<NetworkProtocol, Arc<dyn ProtocolDriver>> = HashMap::new();
        drivers.insert(NetworkProtocol::Quic, Arc::new(QuicProtocolDriver));
        drivers.insert(NetworkProtocol::Http, Arc::new(HttpProtocolDriver));
        Arc::new(Self {
            drivers: Arc::new(drivers),
            egress_profiles: Arc::new(RwLock::new(HashMap::new())),
            ingress_bindings: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    pub async fn register_egress_profile(&self, profile: NetworkEgressProfile) {
        self.egress_profiles
            .write()
            .await
            .insert(profile.name.clone(), profile);
    }

    pub async fn register_ingress_binding(&self, binding: NetworkIngressBinding) {
        self.ingress_bindings
            .write()
            .await
            .insert(binding.name.clone(), binding);
    }

    pub async fn validate_process_grants(
        &self,
        egress_profiles: &[String],
        ingress_bindings: &[String],
    ) -> Result<(), NetworkError> {
        let profiles = self.egress_profiles.read().await;
        for name in egress_profiles {
            if !profiles.contains_key(name) {
                return Err(NetworkError::Other(format!(
                    "network egress profile `{name}` is not registered"
                )));
            }
        }
        drop(profiles);

        let bindings = self.ingress_bindings.read().await;
        for name in ingress_bindings {
            if !bindings.contains_key(name) {
                return Err(NetworkError::Other(format!(
                    "network ingress binding `{name}` is not registered"
                )));
            }
        }

        Ok(())
    }

    async fn egress_profile(
        &self,
        policy: &NetworkProcessPolicy,
        name: &str,
        requested_protocol: NetworkProtocol,
        authority: &str,
    ) -> Result<NetworkEgressProfile, NetworkError> {
        if !policy.allows_egress_profile(name) {
            return Err(NetworkError::PermissionDenied);
        }

        let profiles = self.egress_profiles.read().await;
        let profile = profiles.get(name).ok_or(NetworkError::NotFound)?;
        if profile.protocol != requested_protocol {
            return Err(NetworkError::Unsupported);
        }
        if !authority_allowed(authority, &profile.allowed_authorities) {
            return Err(NetworkError::PermissionDenied);
        }
        Ok(profile.clone())
    }

    async fn ingress_binding(
        &self,
        policy: &NetworkProcessPolicy,
        name: &str,
    ) -> Result<NetworkIngressBinding, NetworkError> {
        if !policy.allows_ingress_binding(name) {
            return Err(NetworkError::PermissionDenied);
        }
        let bindings = self.ingress_bindings.read().await;
        bindings.get(name).cloned().ok_or(NetworkError::NotFound)
    }

    fn driver_for(
        &self,
        protocol: NetworkProtocol,
    ) -> Result<Arc<dyn ProtocolDriver>, NetworkError> {
        self.drivers
            .get(&protocol)
            .cloned()
            .ok_or(NetworkError::Unsupported)
    }
}

impl NetworkCapability for NetworkService {
    type Error = NetworkError;
    type Listener = RuntimeListener;
    type Session = RuntimeSession;
    type Stream = RuntimeStream;
    type RpcClientExchange = RuntimeRpcClientExchange;
    type RpcExchange = RuntimeRpcExchange;
    type RpcBodyReader = RuntimeRpcBodyReader;
    type RpcBodyWriter = RuntimeRpcBodyWriter;

    fn listen(
        &self,
        policy: Arc<NetworkProcessPolicy>,
        input: NetworkListen,
    ) -> NetworkFuture<ListenerHandle<Self::Listener>, Self::Error> {
        let service = self.clone();
        Box::pin(async move {
            let binding = service
                .ingress_binding(&policy, &input.binding_name)
                .await?;
            let interactions = binding.interactions.clone();
            let protocol = binding.protocol;
            let driver = service.driver_for(protocol)?;
            let listener = driver.listen(binding).await?;
            Ok(ListenerHandle {
                protocol,
                interactions,
                inner: listener,
            })
        })
    }

    fn connect(
        &self,
        policy: Arc<NetworkProcessPolicy>,
        input: NetworkConnect,
    ) -> NetworkFuture<SessionHandle<Self::Session>, Self::Error> {
        let service = self.clone();
        Box::pin(async move {
            let profile = service
                .egress_profile(
                    &policy,
                    &input.profile_name,
                    input.protocol,
                    &input.authority,
                )
                .await?;
            let interactions = profile.interactions.clone();
            let protocol = profile.protocol;
            let driver = service.driver_for(protocol)?;
            let session = driver.connect(profile, input.authority).await?;
            Ok(SessionHandle {
                protocol,
                interactions,
                inner: session,
            })
        })
    }

    fn accept(
        &self,
        listener: &Self::Listener,
        timeout_ms: u32,
    ) -> NetworkFuture<AcceptedSession<Self::Session>, Self::Error> {
        listener.inner.accept(timeout_ms)
    }

    fn stream_open(
        &self,
        session: &Self::Session,
    ) -> NetworkFuture<AcceptedStream<Self::Stream>, Self::Error> {
        session.inner.stream_open()
    }

    fn stream_accept(
        &self,
        session: &Self::Session,
        timeout_ms: u32,
    ) -> NetworkFuture<AcceptedStream<Self::Stream>, Self::Error> {
        session.inner.stream_accept(timeout_ms)
    }

    fn stream_send(
        &self,
        stream: &Self::Stream,
        input: NetworkStreamSend,
    ) -> NetworkFuture<NetworkStatus, Self::Error> {
        stream.inner.send(input)
    }

    fn stream_recv(
        &self,
        stream: &Self::Stream,
        input: NetworkStreamRecv,
    ) -> NetworkFuture<NetworkStreamRecvResult, Self::Error> {
        stream.inner.recv(input)
    }

    fn rpc_invoke(
        &self,
        session: &Self::Session,
        input: NetworkRpcInvoke,
    ) -> NetworkFuture<StartedRpc<Self::RpcClientExchange, Self::RpcBodyWriter>, Self::Error> {
        session.inner.rpc_invoke(input)
    }

    fn rpc_await(
        &self,
        exchange: Self::RpcClientExchange,
        input: NetworkRpcAwait,
    ) -> NetworkFuture<AwaitedRpc<Self::RpcBodyReader>, Self::Error> {
        exchange.inner.await_response(input)
    }

    fn rpc_accept(
        &self,
        session: &Self::Session,
        timeout_ms: u32,
    ) -> NetworkFuture<AcceptedRpc<Self::RpcExchange, Self::RpcBodyReader>, Self::Error> {
        session.inner.rpc_accept(timeout_ms)
    }

    fn rpc_respond(
        &self,
        exchange: Self::RpcExchange,
        input: NetworkRpcRespond,
    ) -> NetworkFuture<RespondedRpc<Self::RpcBodyWriter>, Self::Error> {
        exchange.inner.respond(input)
    }

    fn rpc_body_read(
        &self,
        body: &Self::RpcBodyReader,
        input: NetworkRpcBodyRead,
    ) -> NetworkFuture<NetworkRpcBodyReadResult, Self::Error> {
        body.inner.read(input)
    }

    fn rpc_body_write(
        &self,
        body: &Self::RpcBodyWriter,
        input: NetworkRpcBodyWrite,
    ) -> NetworkFuture<NetworkStatus, Self::Error> {
        body.inner.write(input)
    }

    fn close_listener(
        &self,
        listener: ListenerHandle<Self::Listener>,
    ) -> NetworkFuture<NetworkStatus, Self::Error> {
        listener.inner.inner.close()
    }

    fn close_session(
        &self,
        session: SessionHandle<Self::Session>,
    ) -> NetworkFuture<NetworkStatus, Self::Error> {
        session.inner.inner.close()
    }

    fn close_stream(
        &self,
        stream: StreamHandle<Self::Stream>,
    ) -> NetworkFuture<NetworkStatus, Self::Error> {
        stream.inner.inner.close()
    }

    fn close_rpc_exchange(
        &self,
        exchange: Self::RpcExchange,
    ) -> NetworkFuture<NetworkStatus, Self::Error> {
        exchange.inner.close()
    }

    fn close_rpc_client_exchange(
        &self,
        exchange: Self::RpcClientExchange,
    ) -> NetworkFuture<NetworkStatus, Self::Error> {
        exchange.inner.close()
    }

    fn close_rpc_body_reader(
        &self,
        body: RpcBodyReaderHandle<Self::RpcBodyReader>,
    ) -> NetworkFuture<NetworkStatus, Self::Error> {
        body.inner.inner.close()
    }

    fn close_rpc_body_writer(
        &self,
        body: RpcBodyWriterHandle<Self::RpcBodyWriter>,
    ) -> NetworkFuture<NetworkStatus, Self::Error> {
        body.inner.inner.close()
    }
}

struct QuicProtocolDriver;

impl ProtocolDriver for QuicProtocolDriver {
    fn listen(
        &self,
        binding: NetworkIngressBinding,
    ) -> NetworkFuture<RuntimeListener, NetworkError> {
        Box::pin(async move {
            let endpoint = build_quic_server_endpoint(&binding).map_err(network_other)?;
            Ok(RuntimeListener::new(QuicListener {
                endpoint,
                interactions: binding.interactions,
            }))
        })
    }

    fn connect(
        &self,
        profile: NetworkEgressProfile,
        authority: String,
    ) -> NetworkFuture<RuntimeSession, NetworkError> {
        Box::pin(async move {
            let session = connect_quic(&profile, &authority).await?;
            Ok(RuntimeSession::new(session))
        })
    }
}

struct HttpProtocolDriver;

impl ProtocolDriver for HttpProtocolDriver {
    fn listen(
        &self,
        binding: NetworkIngressBinding,
    ) -> NetworkFuture<RuntimeListener, NetworkError> {
        Box::pin(async move {
            let listener = TcpListener::bind(&binding.listen_addr)
                .await
                .with_context(|| format!("bind HTTP listener {}", binding.listen_addr))
                .map_err(network_other)?;
            let acceptor = build_tls_acceptor(&binding).map_err(network_other)?;
            let (tx, rx) = mpsc::channel(64);
            let task = tokio::spawn(run_http_accept_loop(listener, acceptor, tx));
            Ok(RuntimeListener::new(HttpListener {
                receiver: Arc::new(Mutex::new(rx)),
                accept_task: Arc::new(Mutex::new(Some(task))),
                interactions: binding.interactions,
            }))
        })
    }

    fn connect(
        &self,
        profile: NetworkEgressProfile,
        authority: String,
    ) -> NetworkFuture<RuntimeSession, NetworkError> {
        Box::pin(async move {
            Ok(RuntimeSession::new(HttpClientSession {
                authority,
                profile,
            }))
        })
    }
}

struct QuicListener {
    endpoint: Endpoint,
    interactions: Vec<InteractionKind>,
}

impl ProtocolListener for QuicListener {
    fn protocol(&self) -> NetworkProtocol {
        NetworkProtocol::Quic
    }

    fn interactions(&self) -> Vec<InteractionKind> {
        self.interactions.clone()
    }

    fn accept(
        &self,
        timeout_ms: u32,
    ) -> NetworkFuture<AcceptedSession<RuntimeSession>, NetworkError> {
        let endpoint = self.endpoint.clone();
        let interactions = self.interactions.clone();
        Box::pin(async move {
            let incoming = wait_for(timeout_ms, endpoint.accept())
                .await?
                .ok_or(NetworkError::Closed)?;
            let connecting = incoming.accept().map_err(network_other)?;
            let connection = wait_result(timeout_ms, connecting).await?;
            Ok(AcceptedSession {
                code: NetworkStatusCode::Ok,
                session: Some(SessionHandle {
                    protocol: NetworkProtocol::Quic,
                    interactions: interactions.clone(),
                    inner: RuntimeSession::new(QuicSession {
                        connection,
                        interactions,
                    }),
                }),
            })
        })
    }

    fn close(&self) -> NetworkFuture<NetworkStatus, NetworkError> {
        let endpoint = self.endpoint.clone();
        Box::pin(async move {
            endpoint.close(0u32.into(), b"close");
            Ok(ok_status())
        })
    }
}

struct HttpListener {
    receiver: Arc<Mutex<mpsc::Receiver<PendingHttpRequest>>>,
    accept_task: Arc<Mutex<Option<JoinHandle<()>>>>,
    interactions: Vec<InteractionKind>,
}

impl ProtocolListener for HttpListener {
    fn protocol(&self) -> NetworkProtocol {
        NetworkProtocol::Http
    }

    fn interactions(&self) -> Vec<InteractionKind> {
        self.interactions.clone()
    }

    fn accept(
        &self,
        timeout_ms: u32,
    ) -> NetworkFuture<AcceptedSession<RuntimeSession>, NetworkError> {
        let receiver = Arc::clone(&self.receiver);
        let interactions = self.interactions.clone();
        Box::pin(async move {
            let request = {
                let mut receiver = receiver.lock().await;
                wait_for(timeout_ms, receiver.recv())
                    .await?
                    .ok_or(NetworkError::Closed)?
            };
            Ok(AcceptedSession {
                code: NetworkStatusCode::Ok,
                session: Some(SessionHandle {
                    protocol: NetworkProtocol::Http,
                    interactions,
                    inner: RuntimeSession::new(HttpServerSession {
                        pending: Arc::new(Mutex::new(Some(request))),
                    }),
                }),
            })
        })
    }

    fn close(&self) -> NetworkFuture<NetworkStatus, NetworkError> {
        let accept_task = Arc::clone(&self.accept_task);
        Box::pin(async move {
            if let Some(task) = accept_task.lock().await.take() {
                task.abort();
            }
            Ok(ok_status())
        })
    }
}

struct QuicSession {
    connection: Connection,
    interactions: Vec<InteractionKind>,
}

impl ProtocolSession for QuicSession {
    fn protocol(&self) -> NetworkProtocol {
        NetworkProtocol::Quic
    }

    fn interactions(&self) -> Vec<InteractionKind> {
        self.interactions.clone()
    }

    fn stream_supported(&self) -> bool {
        self.interactions.contains(&InteractionKind::Stream)
    }

    fn stream_open(&self) -> NetworkFuture<AcceptedStream<RuntimeStream>, NetworkError> {
        if !self.stream_supported() {
            return unsupported_stream();
        }

        let connection = self.connection.clone();
        Box::pin(async move {
            let (send, recv) = connection.open_bi().await.map_err(network_other)?;
            Ok(AcceptedStream {
                code: NetworkStatusCode::Ok,
                stream: Some(StreamHandle {
                    inner: RuntimeStream::new(QuicStream {
                        send: Arc::new(Mutex::new(send)),
                        recv: Arc::new(Mutex::new(recv)),
                    }),
                }),
            })
        })
    }

    fn stream_accept(
        &self,
        timeout_ms: u32,
    ) -> NetworkFuture<AcceptedStream<RuntimeStream>, NetworkError> {
        if !self.stream_supported() {
            return unsupported_stream();
        }

        let connection = self.connection.clone();
        Box::pin(async move {
            let (send, recv) = wait_result(timeout_ms, connection.accept_bi()).await?;
            Ok(AcceptedStream {
                code: NetworkStatusCode::Ok,
                stream: Some(StreamHandle {
                    inner: RuntimeStream::new(QuicStream {
                        send: Arc::new(Mutex::new(send)),
                        recv: Arc::new(Mutex::new(recv)),
                    }),
                }),
            })
        })
    }

    fn rpc_supported(&self) -> bool {
        self.interactions.contains(&InteractionKind::Rpc)
    }

    fn rpc_invoke(
        &self,
        input: NetworkRpcInvoke,
    ) -> NetworkFuture<StartedRpc<RuntimeRpcClientExchange, RuntimeRpcBodyWriter>, NetworkError>
    {
        if !self.rpc_supported() {
            return unsupported_rpc_invoke();
        }

        let connection = self.connection.clone();
        Box::pin(async move {
            let (mut send, recv) = wait_result(input.timeout_ms, connection.open_bi()).await?;
            let bytes = selium_abi::encode_rkyv(&input.request).map_err(network_other)?;
            wait_result(input.timeout_ms, write_framed(&mut send, &bytes)).await?;
            Ok(StartedRpc {
                code: NetworkStatusCode::Ok,
                exchange: Some(RuntimeRpcClientExchange::new(QuicClientExchange { recv })),
                request_body: Some(RpcBodyWriterHandle {
                    inner: RuntimeRpcBodyWriter::new(QuicRpcBodyWriter {
                        send: Arc::new(Mutex::new(send)),
                    }),
                }),
            })
        })
    }

    fn rpc_accept(
        &self,
        timeout_ms: u32,
    ) -> NetworkFuture<AcceptedRpc<RuntimeRpcExchange, RuntimeRpcBodyReader>, NetworkError> {
        if !self.rpc_supported() {
            return unsupported_rpc_accept();
        }

        let connection = self.connection.clone();
        Box::pin(async move {
            let (send, mut recv) = wait_result(timeout_ms, connection.accept_bi()).await?;
            let frame = wait_result(timeout_ms, read_framed(&mut recv)).await?;
            let request =
                selium_abi::decode_rkyv::<NetworkRpcRequestHead>(&frame).map_err(network_other)?;
            Ok(AcceptedRpc {
                code: NetworkStatusCode::Ok,
                exchange: Some(RuntimeRpcExchange::new(QuicRpcExchange { send })),
                request: Some(request),
                request_body: Some(RpcBodyReaderHandle {
                    inner: RuntimeRpcBodyReader::new(QuicRpcBodyReader {
                        recv: Arc::new(Mutex::new(recv)),
                    }),
                }),
            })
        })
    }

    fn close(&self) -> NetworkFuture<NetworkStatus, NetworkError> {
        let connection = self.connection.clone();
        Box::pin(async move {
            connection.close(0u32.into(), b"close");
            Ok(ok_status())
        })
    }
}

struct HttpClientSession {
    authority: String,
    profile: NetworkEgressProfile,
}

impl ProtocolSession for HttpClientSession {
    fn protocol(&self) -> NetworkProtocol {
        NetworkProtocol::Http
    }

    fn interactions(&self) -> Vec<InteractionKind> {
        self.profile.interactions.clone()
    }

    fn stream_supported(&self) -> bool {
        false
    }

    fn stream_open(&self) -> NetworkFuture<AcceptedStream<RuntimeStream>, NetworkError> {
        unsupported_stream()
    }

    fn stream_accept(
        &self,
        _timeout_ms: u32,
    ) -> NetworkFuture<AcceptedStream<RuntimeStream>, NetworkError> {
        unsupported_stream()
    }

    fn rpc_supported(&self) -> bool {
        self.profile.interactions.contains(&InteractionKind::Rpc)
    }

    fn rpc_invoke(
        &self,
        input: NetworkRpcInvoke,
    ) -> NetworkFuture<StartedRpc<RuntimeRpcClientExchange, RuntimeRpcBodyWriter>, NetworkError>
    {
        if !self.rpc_supported() {
            return unsupported_rpc_invoke();
        }

        let authority = self.authority.clone();
        let profile = self.profile.clone();
        Box::pin(async move {
            let (exchange, request_body) =
                start_http_exchange(&authority, &profile, input.request).await?;
            Ok(StartedRpc {
                code: NetworkStatusCode::Ok,
                exchange: Some(exchange),
                request_body: Some(RpcBodyWriterHandle {
                    inner: request_body,
                }),
            })
        })
    }

    fn rpc_accept(
        &self,
        _timeout_ms: u32,
    ) -> NetworkFuture<AcceptedRpc<RuntimeRpcExchange, RuntimeRpcBodyReader>, NetworkError> {
        unsupported_rpc_accept()
    }

    fn close(&self) -> NetworkFuture<NetworkStatus, NetworkError> {
        Box::pin(async move { Ok(ok_status()) })
    }
}

struct HttpServerSession {
    pending: Arc<Mutex<Option<PendingHttpRequest>>>,
}

impl ProtocolSession for HttpServerSession {
    fn protocol(&self) -> NetworkProtocol {
        NetworkProtocol::Http
    }

    fn interactions(&self) -> Vec<InteractionKind> {
        vec![InteractionKind::Rpc]
    }

    fn stream_supported(&self) -> bool {
        false
    }

    fn stream_open(&self) -> NetworkFuture<AcceptedStream<RuntimeStream>, NetworkError> {
        unsupported_stream()
    }

    fn stream_accept(
        &self,
        _timeout_ms: u32,
    ) -> NetworkFuture<AcceptedStream<RuntimeStream>, NetworkError> {
        unsupported_stream()
    }

    fn rpc_supported(&self) -> bool {
        true
    }

    fn rpc_invoke(
        &self,
        _input: NetworkRpcInvoke,
    ) -> NetworkFuture<StartedRpc<RuntimeRpcClientExchange, RuntimeRpcBodyWriter>, NetworkError>
    {
        unsupported_rpc_invoke()
    }

    fn rpc_accept(
        &self,
        _timeout_ms: u32,
    ) -> NetworkFuture<AcceptedRpc<RuntimeRpcExchange, RuntimeRpcBodyReader>, NetworkError> {
        let pending = Arc::clone(&self.pending);
        Box::pin(async move {
            let pending_request = pending.lock().await.take();
            Ok(match pending_request {
                Some(pending_request) => AcceptedRpc {
                    code: NetworkStatusCode::Ok,
                    exchange: Some(RuntimeRpcExchange::new(HttpRpcExchange {
                        responder: pending_request.responder,
                    })),
                    request: Some(pending_request.request),
                    request_body: Some(RpcBodyReaderHandle {
                        inner: pending_request.body,
                    }),
                },
                None => AcceptedRpc {
                    code: NetworkStatusCode::Closed,
                    exchange: None,
                    request: None,
                    request_body: None,
                },
            })
        })
    }

    fn close(&self) -> NetworkFuture<NetworkStatus, NetworkError> {
        Box::pin(async move { Ok(ok_status()) })
    }
}

struct QuicStream {
    send: Arc<Mutex<SendStream>>,
    recv: Arc<Mutex<RecvStream>>,
}

impl ProtocolStream for QuicStream {
    fn send(&self, input: NetworkStreamSend) -> NetworkFuture<NetworkStatus, NetworkError> {
        let send = Arc::clone(&self.send);
        Box::pin(async move {
            let mut send = send.lock().await;
            wait_result(input.timeout_ms, send.write_all(&input.bytes)).await?;
            if input.finish {
                send.finish().map_err(network_other)?;
            }
            Ok(ok_status())
        })
    }

    fn recv(
        &self,
        input: NetworkStreamRecv,
    ) -> NetworkFuture<NetworkStreamRecvResult, NetworkError> {
        let recv = Arc::clone(&self.recv);
        Box::pin(async move {
            let mut recv = recv.lock().await;
            let chunk = wait_result(
                input.timeout_ms,
                recv.read_chunk(input.max_bytes as usize, true),
            )
            .await?;
            Ok(NetworkStreamRecvResult {
                code: NetworkStatusCode::Ok,
                chunk: Some(match chunk {
                    Some(chunk) => NetworkStreamChunk {
                        bytes: chunk.bytes.to_vec(),
                        finish: false,
                    },
                    None => NetworkStreamChunk {
                        bytes: Vec::new(),
                        finish: true,
                    },
                }),
            })
        })
    }

    fn close(&self) -> NetworkFuture<NetworkStatus, NetworkError> {
        let send = Arc::clone(&self.send);
        Box::pin(async move {
            let mut send = send.lock().await;
            send.finish().map_err(network_other)?;
            Ok(ok_status())
        })
    }
}

struct QuicRpcExchange {
    send: SendStream,
}

impl ProtocolRpcExchange for QuicRpcExchange {
    fn respond(
        self: Box<Self>,
        input: NetworkRpcRespond,
    ) -> NetworkFuture<RespondedRpc<RuntimeRpcBodyWriter>, NetworkError> {
        Box::pin(async move {
            let mut send = self.send;
            let bytes = selium_abi::encode_rkyv(&input.response).map_err(network_other)?;
            write_framed(&mut send, &bytes)
                .await
                .map_err(network_other)?;
            Ok(RespondedRpc {
                code: NetworkStatusCode::Ok,
                response_body: Some(RpcBodyWriterHandle {
                    inner: RuntimeRpcBodyWriter::new(QuicRpcBodyWriter {
                        send: Arc::new(Mutex::new(send)),
                    }),
                }),
            })
        })
    }

    fn close(mut self: Box<Self>) -> NetworkFuture<NetworkStatus, NetworkError> {
        Box::pin(async move {
            let _ = self.send.finish();
            Ok(ok_status())
        })
    }
}

struct QuicClientExchange {
    recv: RecvStream,
}

impl ProtocolRpcClientExchange for QuicClientExchange {
    fn await_response(
        self: Box<Self>,
        input: NetworkRpcAwait,
    ) -> NetworkFuture<AwaitedRpc<RuntimeRpcBodyReader>, NetworkError> {
        Box::pin(async move {
            let mut recv = self.recv;
            let frame = wait_result(input.timeout_ms, read_framed(&mut recv)).await?;
            let response =
                selium_abi::decode_rkyv::<NetworkRpcResponseHead>(&frame).map_err(network_other)?;
            Ok(AwaitedRpc {
                code: NetworkStatusCode::Ok,
                response: Some(response),
                response_body: Some(RpcBodyReaderHandle {
                    inner: RuntimeRpcBodyReader::new(QuicRpcBodyReader {
                        recv: Arc::new(Mutex::new(recv)),
                    }),
                }),
            })
        })
    }

    fn close(self: Box<Self>) -> NetworkFuture<NetworkStatus, NetworkError> {
        Box::pin(async move { Ok(ok_status()) })
    }
}

type HttpResponseTask = Arc<Mutex<Option<JoinHandle<Result<Response<Incoming>, NetworkError>>>>>;

struct HttpClientExchange {
    response_task: HttpResponseTask,
}

impl ProtocolRpcClientExchange for HttpClientExchange {
    fn await_response(
        self: Box<Self>,
        input: NetworkRpcAwait,
    ) -> NetworkFuture<AwaitedRpc<RuntimeRpcBodyReader>, NetworkError> {
        Box::pin(async move {
            let response_task = self
                .response_task
                .lock()
                .await
                .take()
                .ok_or(NetworkError::Closed)?;
            let response = wait_for(input.timeout_ms, response_task)
                .await
                .map_err(network_other)?
                .map_err(network_other)??;
            let (parts, body) = response.into_parts();
            Ok(AwaitedRpc {
                code: NetworkStatusCode::Ok,
                response: Some(NetworkRpcResponseHead {
                    status: parts.status.as_u16(),
                    metadata: headers_to_map(&parts.headers),
                }),
                response_body: Some(RpcBodyReaderHandle {
                    inner: RuntimeRpcBodyReader::new(HttpIncomingBodyReader {
                        state: Arc::new(Mutex::new(HttpIncomingBodyState {
                            body,
                            pending: Bytes::new(),
                            finished: false,
                        })),
                    }),
                }),
            })
        })
    }

    fn close(self: Box<Self>) -> NetworkFuture<NetworkStatus, NetworkError> {
        Box::pin(async move {
            if let Some(task) = self.response_task.lock().await.take() {
                task.abort();
            }
            Ok(ok_status())
        })
    }
}

struct QuicRpcBodyReader {
    recv: Arc<Mutex<RecvStream>>,
}

impl ProtocolRpcBodyReader for QuicRpcBodyReader {
    fn read(
        &self,
        input: NetworkRpcBodyRead,
    ) -> NetworkFuture<NetworkRpcBodyReadResult, NetworkError> {
        let recv = Arc::clone(&self.recv);
        Box::pin(async move {
            let mut recv = recv.lock().await;
            let chunk = wait_result(
                input.timeout_ms,
                recv.read_chunk(input.max_bytes as usize, true),
            )
            .await?;
            Ok(NetworkRpcBodyReadResult {
                code: NetworkStatusCode::Ok,
                chunk: Some(match chunk {
                    Some(chunk) => NetworkStreamChunk {
                        bytes: chunk.bytes.to_vec(),
                        finish: false,
                    },
                    None => NetworkStreamChunk {
                        bytes: Vec::new(),
                        finish: true,
                    },
                }),
            })
        })
    }

    fn close(&self) -> NetworkFuture<NetworkStatus, NetworkError> {
        Box::pin(async move { Ok(ok_status()) })
    }
}

struct QuicRpcBodyWriter {
    send: Arc<Mutex<SendStream>>,
}

impl ProtocolRpcBodyWriter for QuicRpcBodyWriter {
    fn write(&self, input: NetworkRpcBodyWrite) -> NetworkFuture<NetworkStatus, NetworkError> {
        let send = Arc::clone(&self.send);
        Box::pin(async move {
            let mut send = send.lock().await;
            wait_result(input.timeout_ms, send.write_all(&input.bytes)).await?;
            if input.finish {
                send.finish().map_err(network_other)?;
            }
            Ok(ok_status())
        })
    }

    fn close(&self) -> NetworkFuture<NetworkStatus, NetworkError> {
        let send = Arc::clone(&self.send);
        Box::pin(async move {
            let mut send = send.lock().await;
            send.finish().map_err(network_other)?;
            Ok(ok_status())
        })
    }
}

struct HttpRpcExchange {
    responder: oneshot::Sender<PendingHttpResponse>,
}

impl ProtocolRpcExchange for HttpRpcExchange {
    fn respond(
        self: Box<Self>,
        input: NetworkRpcRespond,
    ) -> NetworkFuture<RespondedRpc<RuntimeRpcBodyWriter>, NetworkError> {
        Box::pin(async move {
            let (tx, rx) = mpsc::channel(16);
            let _ = self.responder.send(PendingHttpResponse {
                response: input.response,
                body: rx,
            });
            Ok(RespondedRpc {
                code: NetworkStatusCode::Ok,
                response_body: Some(RpcBodyWriterHandle {
                    inner: RuntimeRpcBodyWriter::new(HttpRpcBodyWriter { sender: tx }),
                }),
            })
        })
    }

    fn close(self: Box<Self>) -> NetworkFuture<NetworkStatus, NetworkError> {
        Box::pin(async move {
            let (_tx, rx) = mpsc::channel(1);
            let _ = self.responder.send(PendingHttpResponse {
                response: NetworkRpcResponseHead {
                    status: 500,
                    metadata: BTreeMap::new(),
                },
                body: rx,
            });
            Ok(ok_status())
        })
    }
}

#[derive(Debug)]
struct PendingHttpRequest {
    request: NetworkRpcRequestHead,
    body: RuntimeRpcBodyReader,
    responder: oneshot::Sender<PendingHttpResponse>,
}

#[derive(Debug)]
struct PendingHttpResponse {
    response: NetworkRpcResponseHead,
    body: mpsc::Receiver<Option<Bytes>>,
}

struct HttpRpcBodyWriter {
    sender: mpsc::Sender<Option<Bytes>>,
}

impl ProtocolRpcBodyWriter for HttpRpcBodyWriter {
    fn write(&self, input: NetworkRpcBodyWrite) -> NetworkFuture<NetworkStatus, NetworkError> {
        let sender = self.sender.clone();
        Box::pin(async move {
            if !input.bytes.is_empty() {
                wait_result(
                    input.timeout_ms,
                    sender.send(Some(Bytes::from(input.bytes))),
                )
                .await?;
            }
            if input.finish {
                wait_result(input.timeout_ms, sender.send(None)).await?;
            }
            Ok(ok_status())
        })
    }

    fn close(&self) -> NetworkFuture<NetworkStatus, NetworkError> {
        let sender = self.sender.clone();
        Box::pin(async move {
            let _ = sender.send(None).await;
            Ok(ok_status())
        })
    }
}

struct HttpIncomingBodyReader {
    state: Arc<Mutex<HttpIncomingBodyState>>,
}

struct HttpIncomingBodyState {
    body: Incoming,
    pending: Bytes,
    finished: bool,
}

impl ProtocolRpcBodyReader for HttpIncomingBodyReader {
    fn read(
        &self,
        input: NetworkRpcBodyRead,
    ) -> NetworkFuture<NetworkRpcBodyReadResult, NetworkError> {
        let state = Arc::clone(&self.state);
        Box::pin(async move { read_http_body_chunk(state, input).await })
    }

    fn close(&self) -> NetworkFuture<NetworkStatus, NetworkError> {
        Box::pin(async move { Ok(ok_status()) })
    }
}

async fn run_http_accept_loop(
    listener: TcpListener,
    acceptor: TlsAcceptor,
    tx: mpsc::Sender<PendingHttpRequest>,
) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            break;
        };
        let acceptor = acceptor.clone();
        let tx = tx.clone();
        tokio::spawn(async move {
            if let Err(err) = run_http_connection(stream, acceptor, tx).await {
                tracing::warn!("http network connection error: {err:#}");
            }
        });
    }
}

async fn run_http_connection(
    stream: TcpStream,
    acceptor: TlsAcceptor,
    tx: mpsc::Sender<PendingHttpRequest>,
) -> Result<()> {
    let tls = acceptor.accept(stream).await.context("accept TLS")?;
    let service = service_fn(move |request: Request<Incoming>| {
        let tx = tx.clone();
        async move {
            let response = handle_http_request(request, tx).await;
            Ok::<_, hyper::Error>(response)
        }
    });
    hyper::server::conn::http1::Builder::new()
        .serve_connection(TokioIo::new(tls), service)
        .await
        .context("serve HTTP connection")?;
    Ok(())
}

async fn handle_http_request(
    request: Request<Incoming>,
    tx: mpsc::Sender<PendingHttpRequest>,
) -> Response<BoxBody<Bytes, Infallible>> {
    match handle_http_request_inner(request, tx).await {
        Ok(response) => response,
        Err(err) => Response::builder()
            .status(500)
            .body(Full::new(Bytes::from(err.to_string())).boxed())
            .expect("build 500 response"),
    }
}

async fn handle_http_request_inner(
    request: Request<Incoming>,
    tx: mpsc::Sender<PendingHttpRequest>,
) -> Result<Response<BoxBody<Bytes, Infallible>>> {
    let (parts, body) = request.into_parts();
    let request = NetworkRpcRequestHead {
        method: parts.method.as_str().to_string(),
        path: parts.uri.to_string(),
        metadata: headers_to_map(&parts.headers),
    };
    let (response_tx, response_rx) = oneshot::channel();
    let body = RuntimeRpcBodyReader::new(HttpIncomingBodyReader {
        state: Arc::new(Mutex::new(HttpIncomingBodyState {
            body,
            pending: Bytes::new(),
            finished: false,
        })),
    });
    tx.send(PendingHttpRequest {
        request,
        body,
        responder: response_tx,
    })
    .await
    .context("send pending HTTP request")?;
    let response = response_rx.await.context("await guest response")?;
    let mut builder = Response::builder().status(response.response.status);
    for (key, value) in &response.response.metadata {
        builder = builder.header(key, value);
    }
    let body_stream = http_body_stream(response.body);
    builder
        .body(body_stream.boxed())
        .context("build HTTP response")
}

async fn start_http_exchange(
    authority: &str,
    profile: &NetworkEgressProfile,
    request: NetworkRpcRequestHead,
) -> Result<(RuntimeRpcClientExchange, RuntimeRpcBodyWriter), NetworkError> {
    let addr = parse_authority_addr(authority).map_err(network_other)?;
    let server_name = derive_server_name(authority);
    let stream = TcpStream::connect(addr).await.map_err(network_other)?;
    let connector = build_tls_connector(profile).map_err(network_other)?;
    let tls = connector
        .connect(server_name.try_into().map_err(network_other)?, stream)
        .await
        .map_err(network_other)?;

    let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(tls))
        .await
        .map_err(network_other)?;
    tokio::spawn(async move {
        if let Err(err) = conn.await {
            tracing::warn!("http client connection error: {err:#}");
        }
    });

    let (tx, rx) = mpsc::channel(16);
    let mut builder = Request::builder().method(request.method.as_str());
    let uri = if request.path.starts_with("http://") || request.path.starts_with("https://") {
        request.path
    } else {
        format!("https://{authority}{}", request.path)
    };
    builder = builder.uri(uri);
    for (key, value) in &request.metadata {
        builder = builder.header(key, value);
    }
    let req = builder.body(http_body_stream(rx)).map_err(network_other)?;
    let response_task =
        tokio::spawn(async move { sender.send_request(req).await.map_err(network_other) });
    Ok((
        RuntimeRpcClientExchange::new(HttpClientExchange {
            response_task: Arc::new(Mutex::new(Some(response_task))),
        }),
        RuntimeRpcBodyWriter::new(HttpRpcBodyWriter { sender: tx }),
    ))
}

fn http_body_stream(
    rx: mpsc::Receiver<Option<Bytes>>,
) -> StreamBody<impl futures_util::Stream<Item = Result<Frame<Bytes>, Infallible>>> {
    StreamBody::new(stream::unfold(rx, |mut rx| async move {
        match rx.recv().await {
            Some(Some(bytes)) => Some((Ok(Frame::data(bytes)), rx)),
            Some(None) | None => None,
        }
    }))
}

async fn read_http_body_chunk(
    state: Arc<Mutex<HttpIncomingBodyState>>,
    input: NetworkRpcBodyRead,
) -> Result<NetworkRpcBodyReadResult, NetworkError> {
    if input.max_bytes == 0 {
        return Err(NetworkError::InvalidArgument);
    }

    let mut state = state.lock().await;
    loop {
        if !state.pending.is_empty() {
            let count = state.pending.len().min(input.max_bytes as usize);
            let bytes = state.pending.split_to(count);
            return Ok(NetworkRpcBodyReadResult {
                code: NetworkStatusCode::Ok,
                chunk: Some(NetworkStreamChunk {
                    bytes: bytes.to_vec(),
                    finish: state.finished && state.pending.is_empty(),
                }),
            });
        }

        if state.finished {
            return Ok(NetworkRpcBodyReadResult {
                code: NetworkStatusCode::Ok,
                chunk: Some(NetworkStreamChunk {
                    bytes: Vec::new(),
                    finish: true,
                }),
            });
        }

        match wait_for(input.timeout_ms, state.body.frame()).await? {
            Some(Ok(frame)) => match frame.into_data() {
                Ok(data) if data.is_empty() => continue,
                Ok(data) => state.pending = data,
                Err(_frame) => continue,
            },
            Some(Err(err)) => return Err(network_other(err)),
            None => state.finished = true,
        }
    }
}

async fn connect_quic(
    profile: &NetworkEgressProfile,
    authority: &str,
) -> Result<QuicSession, NetworkError> {
    const QUIC_CONNECT_TIMEOUT: Duration = Duration::from_millis(500);

    let bind = "127.0.0.1:0".parse::<SocketAddr>().map_err(network_other)?;
    let mut endpoint = Endpoint::client(bind).map_err(network_other)?;
    endpoint.set_default_client_config(build_quic_client_config(profile).map_err(network_other)?);
    let addr = parse_authority_addr(authority).map_err(network_other)?;
    let connecting = endpoint
        .connect(addr, &derive_server_name(authority))
        .map_err(network_other)?;
    let connection = timeout(QUIC_CONNECT_TIMEOUT, connecting)
        .await
        .map_err(|_| NetworkError::Timeout)?
        .map_err(network_other)?;
    Ok(QuicSession {
        connection,
        interactions: profile.interactions.clone(),
    })
}

fn build_quic_server_endpoint(binding: &NetworkIngressBinding) -> Result<Endpoint> {
    let cert_chain = load_cert_chain(&binding.cert_path)?;
    let key = load_private_key(&binding.key_path)?;
    let server_tls = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert_chain, key)
        .context("build QUIC server TLS config")?;
    let quic_server = QuicServerConfig::try_from(server_tls).context("build QUIC server config")?;
    let server_config = quinn::ServerConfig::with_crypto(Arc::new(quic_server));
    let addr = parse_authority_addr(&binding.listen_addr)?;
    Endpoint::server(server_config, addr).context("bind QUIC server endpoint")
}

fn build_quic_client_config(profile: &NetworkEgressProfile) -> Result<quinn::ClientConfig> {
    let tls = build_tls_client_config(profile)?;
    let quic_crypto =
        quinn::crypto::rustls::QuicClientConfig::try_from(tls).context("build QUIC crypto")?;
    Ok(quinn::ClientConfig::new(Arc::new(quic_crypto)))
}

fn build_tls_client_config(profile: &NetworkEgressProfile) -> Result<rustls::ClientConfig> {
    let roots = load_root_store(&profile.ca_cert_path)?;
    let builder = rustls::ClientConfig::builder().with_root_certificates(roots);
    match (&profile.client_cert_path, &profile.client_key_path) {
        (Some(cert), Some(key)) => {
            let cert_chain = load_cert_chain(cert)?;
            let key = load_private_key(key)?;
            builder
                .with_client_auth_cert(cert_chain, key)
                .context("build TLS client config")
        }
        _ => Ok(builder.with_no_client_auth()),
    }
}

fn build_tls_connector(profile: &NetworkEgressProfile) -> Result<TlsConnector> {
    let config = build_tls_client_config(profile)?;
    Ok(TlsConnector::from(Arc::new(config)))
}

fn build_tls_acceptor(binding: &NetworkIngressBinding) -> Result<TlsAcceptor> {
    let cert_chain = load_cert_chain(&binding.cert_path)?;
    let key = load_private_key(&binding.key_path)?;
    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert_chain, key)
        .context("build TLS server config")?;
    Ok(TlsAcceptor::from(Arc::new(config)))
}

fn authority_allowed(authority: &str, allowed: &[String]) -> bool {
    let (authority_endpoint, _) = split_authority(authority);
    if allowed.is_empty() {
        return false;
    }

    allowed.iter().any(|pattern| {
        let (pattern_endpoint, _) = split_authority(pattern);
        if pattern == authority || pattern_endpoint == authority_endpoint {
            return true;
        }
        if let Some(suffix) = pattern_endpoint.strip_prefix("*.") {
            return authority_endpoint
                .split(':')
                .next()
                .map(|host| host.ends_with(suffix))
                .unwrap_or(false);
        }
        false
    })
}

fn parse_authority_addr(raw: &str) -> Result<SocketAddr> {
    let (raw, _) = split_authority(raw);
    if let Ok(addr) = raw.parse::<SocketAddr>() {
        return Ok(addr);
    }

    raw.to_socket_addrs()?
        .next()
        .ok_or_else(|| anyhow!("no socket addresses for `{raw}`"))
}

fn derive_server_name(authority: &str) -> String {
    let (authority, explicit_server_name) = split_authority(authority);
    explicit_server_name.unwrap_or_else(|| {
        authority
            .trim_start_matches("https://")
            .trim_start_matches("http://")
            .trim_start_matches("quic://")
            .trim_end_matches('/')
            .split(':')
            .next()
            .unwrap_or("localhost")
            .to_string()
    })
}

fn split_authority(raw: &str) -> (&str, Option<String>) {
    let trimmed = raw.trim();
    match trimmed.rsplit_once('@') {
        Some((authority, server_name)) if !server_name.trim().is_empty() => {
            (authority.trim(), Some(server_name.trim().to_string()))
        }
        _ => (trimmed, None),
    }
}

fn headers_to_map(headers: &hyper::HeaderMap) -> BTreeMap<String, String> {
    headers
        .iter()
        .filter_map(|(key, value)| {
            value
                .to_str()
                .ok()
                .map(|value| (key.as_str().to_string(), value.to_string()))
        })
        .collect()
}

fn load_root_store(path: &Path) -> Result<RootCertStore> {
    let pem = std::fs::read(path).with_context(|| format!("read CA cert {}", path.display()))?;
    let mut reader = std::io::Cursor::new(pem);
    let mut store = RootCertStore::empty();
    for cert in certs(&mut reader) {
        let cert = cert.context("parse CA cert")?;
        store
            .add(cert)
            .map_err(|err| anyhow!("add CA cert to root store: {err}"))?;
    }
    Ok(store)
}

fn load_cert_chain(path: &Path) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>> {
    let pem = std::fs::read(path).with_context(|| format!("read cert {}", path.display()))?;
    let mut reader = std::io::Cursor::new(pem);
    let mut chain = Vec::new();
    for cert in certs(&mut reader) {
        chain.push(cert.context("parse cert")?);
    }
    if chain.is_empty() {
        return Err(anyhow!("no certs found in {}", path.display()));
    }
    Ok(chain)
}

fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>> {
    let pem = std::fs::read(path).with_context(|| format!("read key {}", path.display()))?;
    let mut reader = std::io::Cursor::new(pem);
    private_key(&mut reader)
        .context("parse private key")?
        .ok_or_else(|| anyhow!("no private key in {}", path.display()))
}

fn ok_status() -> NetworkStatus {
    NetworkStatus {
        code: NetworkStatusCode::Ok,
    }
}

fn unsupported_stream() -> NetworkFuture<AcceptedStream<RuntimeStream>, NetworkError> {
    Box::pin(async {
        Ok(AcceptedStream {
            code: NetworkStatusCode::Unsupported,
            stream: None,
        })
    })
}

fn unsupported_rpc_accept()
-> NetworkFuture<AcceptedRpc<RuntimeRpcExchange, RuntimeRpcBodyReader>, NetworkError> {
    Box::pin(async {
        Ok(AcceptedRpc {
            code: NetworkStatusCode::Unsupported,
            exchange: None,
            request: None,
            request_body: None,
        })
    })
}

fn unsupported_rpc_invoke()
-> NetworkFuture<StartedRpc<RuntimeRpcClientExchange, RuntimeRpcBodyWriter>, NetworkError> {
    Box::pin(async {
        Ok(StartedRpc {
            code: NetworkStatusCode::Unsupported,
            exchange: None,
            request_body: None,
        })
    })
}

async fn wait_for<T, F>(timeout_ms: u32, fut: F) -> Result<T, NetworkError>
where
    F: std::future::Future<Output = T>,
{
    if timeout_ms == 0 {
        Ok(timeout(Duration::from_millis(1), fut)
            .await
            .map_err(|_| NetworkError::WouldBlock)?)
    } else {
        Ok(timeout(Duration::from_millis(u64::from(timeout_ms)), fut)
            .await
            .map_err(|_| NetworkError::Timeout)?)
    }
}

async fn wait_result<T, E, F>(timeout_ms: u32, fut: F) -> Result<T, NetworkError>
where
    F: std::future::Future<Output = Result<T, E>>,
    E: fmt::Display,
{
    wait_for(timeout_ms, fut).await?.map_err(network_other)
}

impl From<NetworkError> for GuestError {
    fn from(value: NetworkError) -> Self {
        match value {
            NetworkError::PermissionDenied => GuestError::PermissionDenied,
            NetworkError::NotFound => GuestError::NotFound,
            NetworkError::InvalidArgument => GuestError::InvalidArgument,
            NetworkError::Timeout
            | NetworkError::WouldBlock
            | NetworkError::Closed
            | NetworkError::Unsupported => GuestError::Subsystem(value.to_string()),
            NetworkError::Other(message) => GuestError::Subsystem(message),
        }
    }
}

fn network_other(err: impl fmt::Display) -> NetworkError {
    NetworkError::Other(err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authority_allowed_supports_exact_and_suffix_patterns() {
        assert!(authority_allowed(
            "api.example.com:443",
            &[String::from("*.example.com")]
        ));
        assert!(authority_allowed(
            "127.0.0.1:7000",
            &[String::from("127.0.0.1:7000")]
        ));
        assert!(!authority_allowed(
            "api.other.test:443",
            &[String::from("*.example.com")]
        ));
    }

    #[test]
    fn derive_server_name_strips_scheme_and_port() {
        assert_eq!(derive_server_name("https://localhost:7443"), "localhost");
        assert_eq!(derive_server_name("quic://127.0.0.1:7443"), "127.0.0.1");
    }
}
