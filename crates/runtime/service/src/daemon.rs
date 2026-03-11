use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    future::Future,
    net::{SocketAddr, ToSocketAddrs},
    path::{Path, PathBuf},
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use anyhow::{Context, Result, anyhow, bail};
use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use quinn::{Connection, Endpoint, Incoming};
use rustls::{RootCertStore, pki_types::PrivateKeyDer};
use rustls_pemfile::{certs, private_key};
use selium_abi::{
    Capability, DataValue, InteractionKind, NetworkProtocol, ProcessLogBindings, QueueAck,
    QueueAttach, QueueCommit, QueueCreate, QueueDelivery, QueueOverflow, QueueReserve, QueueRole,
    QueueStatusCode, ShmAlloc, decode_rkyv, encode_rkyv,
};
use selium_control_plane_api::{
    ContractKind, ControlPlaneState, DiscoveryCapabilityScope, DiscoveryOperation,
    DiscoveryRequest, DiscoveryResolution, DiscoveryTarget, NodeSpec, OperationalProcessRecord,
    PublicEndpointRef,
};
use selium_control_plane_protocol::{
    ActivateEndpointBridgeRequest, ActivateEndpointBridgeResponse, BridgeMessage,
    DeactivateEndpointBridgeRequest, DeactivateEndpointBridgeResponse, DeliverBridgeMessageRequest,
    DeliverBridgeMessageResponse, Empty, EndpointBridgeMode, EndpointBridgeSemantics, Envelope,
    EventBridgeMessage, GuestLogEvent, ListRequest, ListResponse, ManagedEndpointBinding,
    ManagedEndpointBindingType, ManagedEndpointRole, Method, QueryApiRequest, QueryApiResponse,
    RuntimeUsageApiRequest, RuntimeUsageApiResponse, ServiceBridgeMessage, ServiceMessagePhase,
    StartRequest, StartResponse, StatusApiResponse, StopRequest, StopResponse,
    StreamBridgeMessage, SubscribeGuestLogsRequest, SubscribeGuestLogsResponse, decode_envelope,
    decode_error, decode_payload, encode_error_response, encode_request, encode_response,
    is_error, is_request, read_framed, write_framed,
};
use selium_io_durability::RetentionPolicy;
use selium_kernel::{
    Kernel,
    registry::{Registry, ResourceHandle, ResourceType},
    services::{
        queue_service::QueueService, session_service::Session,
        shared_memory_service::SharedMemoryDriver,
    },
    spi::{queue::QueueCapability, shared_memory::SharedMemoryCapability},
};
use selium_module_control_plane::{
    runtime::Query,
    ControlPlaneModuleConfig, ENTRYPOINT, EVENT_LOG_NAME, INTERNAL_BINDING_NAME, MODULE_ID,
    PEER_PROFILE_NAME, PeerTarget, SNAPSHOT_BLOB_STORE_NAME,
};
use selium_runtime_network::{NetworkEgressProfile, NetworkIngressBinding, NetworkService};
use selium_runtime_storage::{StorageBlobStoreDefinition, StorageLogDefinition, StorageService};
use tokio::{
    io::AsyncWrite,
    signal,
    sync::{Mutex, Notify},
    task::{JoinHandle, LocalSet, spawn_local},
    time::{Duration, sleep, timeout},
};
use tracing::info;

use crate::{config::DaemonArgs, modules};

const QUIC_CONNECT_TIMEOUT: Duration = Duration::from_millis(500);
const QUIC_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const MANAGED_EVENT_QUEUE_DEPTH: u32 = 64;
const MANAGED_EVENT_MAX_FRAME_BYTES: u32 = 64 * 1024;
const MANAGED_EVENT_RETRY_DELAY: Duration = Duration::from_millis(100);
const GUEST_LOG_STDOUT_ENDPOINT: &str = "stdout";
const GUEST_LOG_STDERR_ENDPOINT: &str = "stderr";

struct DaemonState {
    node_id: String,
    kernel: Kernel,
    registry: Arc<Registry>,
    work_dir: PathBuf,
    processes: Mutex<BTreeMap<String, usize>>,
    source_bindings: Mutex<BTreeMap<(String, ContractKind, String), ManagedEventEndpointQueue>>,
    target_bindings: Mutex<BTreeMap<(String, ContractKind, String), ManagedEventEndpointQueue>>,
    service_response_bindings:
        Mutex<BTreeMap<(String, ContractKind, String), ManagedEventEndpointQueue>>,
    active_bridges: Mutex<BTreeMap<String, ActiveEndpointBridge>>,
    host_subscription_id: AtomicU64,
    control_plane: Arc<LocalControlPlaneClient>,
    control_plane_process_id: usize,
    tls_paths: ManagedEventTlsPaths,
}

#[derive(Clone)]
struct ManagedEventEndpointQueue {
    queue: selium_kernel::services::queue_service::QueueState,
    queue_shared_id: u64,
}

#[derive(Debug)]
struct ActiveEndpointBridge {
    spec: ActiveEndpointBridgeSpec,
    bridge_task: JoinHandle<()>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ActiveEndpointBridgeSpec {
    Event(ActiveEventEndpointBridgeSpec),
    Service(ActiveServiceEndpointBridgeSpec),
    Stream(ActiveStreamEndpointBridgeSpec),
}

impl ActiveEndpointBridgeSpec {
    fn source_instance_id(&self) -> &str {
        match self {
            Self::Event(spec) => &spec.source_instance_id,
            Self::Service(spec) => &spec.source_instance_id,
            Self::Stream(spec) => &spec.source_instance_id,
        }
    }

    fn mode(&self) -> EndpointBridgeMode {
        match self {
            Self::Event(spec) => spec.mode,
            Self::Service(spec) => spec.mode,
            Self::Stream(spec) => spec.mode,
        }
    }

    fn target_node(&self) -> &str {
        match self {
            Self::Event(spec) => &spec.target_node,
            Self::Service(spec) => &spec.target_node,
            Self::Stream(spec) => &spec.target_node,
        }
    }

    fn target_instance_id(&self) -> &str {
        match self {
            Self::Event(spec) => &spec.target_instance_id,
            Self::Service(spec) => &spec.target_instance_id,
            Self::Stream(spec) => &spec.target_instance_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ActiveEventEndpointBridgeSpec {
    source_instance_id: String,
    source_endpoint: PublicEndpointRef,
    target_instance_id: String,
    target_endpoint: PublicEndpointRef,
    mode: EndpointBridgeMode,
    target_node: String,
    target_daemon_addr: String,
    target_daemon_server_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ActiveServiceEndpointBridgeSpec {
    source_instance_id: String,
    source_endpoint: PublicEndpointRef,
    target_instance_id: String,
    target_endpoint: PublicEndpointRef,
    mode: EndpointBridgeMode,
    target_node: String,
    target_daemon_addr: String,
    target_daemon_server_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ActiveStreamEndpointBridgeSpec {
    source_instance_id: String,
    source_endpoint: PublicEndpointRef,
    target_instance_id: String,
    target_endpoint: PublicEndpointRef,
    mode: EndpointBridgeMode,
    target_node: String,
    target_daemon_addr: String,
    target_daemon_server_name: String,
}

#[derive(Debug, Clone)]
struct ManagedEventTlsPaths {
    ca_cert: PathBuf,
    client_cert: PathBuf,
    client_key: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BridgeMessageDelivery {
    delivered: bool,
    message: Option<BridgeMessage>,
}

#[derive(Debug, Clone)]
struct ResolvedGuestLogSubscription {
    target: OperationalProcessRecord,
    source_node: NodeSpec,
    local_node: NodeSpec,
    streams: Vec<PublicEndpointRef>,
}

#[derive(Debug, Clone)]
struct ActiveGuestLogSubscription {
    instance_id: String,
    bridge_ids: Vec<String>,
}

struct ControlPlaneTlsPaths<'a> {
    cert_path: &'a Path,
    key_path: &'a Path,
    ca_path: &'a Path,
    peer_cert_path: &'a Path,
    peer_key_path: &'a Path,
}

struct ControlPlaneAddresses<'a> {
    public_addr: &'a str,
    internal_addr: &'a str,
}

struct LocalControlPlaneClient {
    endpoint: Endpoint,
    addr: SocketAddr,
    server_name: String,
    connection: Mutex<Option<Connection>>,
    request_id: AtomicU64,
}

impl LocalControlPlaneClient {
    fn new(
        addr: SocketAddr,
        server_name: String,
        ca_path: &Path,
        client_cert_path: &Path,
        client_key_path: &Path,
    ) -> Result<Self> {
        Ok(Self {
            endpoint: build_client_endpoint(ca_path, client_cert_path, client_key_path)?,
            addr,
            server_name,
            connection: Mutex::new(None),
            request_id: AtomicU64::new(1),
        })
    }

    async fn request_raw(&self, frame: &[u8]) -> Result<Vec<u8>> {
        let connection = self.connection().await?;
        let (mut send, mut recv) = timeout(QUIC_REQUEST_TIMEOUT, connection.open_bi())
            .await
            .map_err(|_| anyhow!("timed out"))
            .context("open proxy stream")??;
        timeout(QUIC_REQUEST_TIMEOUT, write_framed(&mut send, frame))
            .await
            .map_err(|_| anyhow!("timed out"))
            .context("write proxy request")??;
        let _ = send.finish();
        timeout(QUIC_REQUEST_TIMEOUT, read_framed(&mut recv))
            .await
            .map_err(|_| anyhow!("timed out"))
            .context("read proxy response")?
    }

    async fn wait_until_ready(&self) -> Result<()> {
        for _ in 0..50 {
            let request_id = self.request_id.fetch_add(1, Ordering::Relaxed);
            let frame = encode_request(Method::ControlStatus, request_id, &Empty {})
                .context("encode probe")?;
            match self.request_raw(&frame).await {
                Ok(response) => {
                    let envelope = decode_envelope(&response).context("decode probe response")?;
                    if envelope.method != Method::ControlStatus || envelope.request_id != request_id
                    {
                        bail!("control-plane readiness probe returned mismatched envelope");
                    }
                    if is_error(&envelope) {
                        let err = decode_error(&envelope).context("decode probe error")?;
                        return Err(anyhow!(
                            "control-plane guest reported {}: {}",
                            err.code,
                            err.message
                        ));
                    }
                    let _: StatusApiResponse =
                        decode_payload(&envelope).context("decode probe payload")?;
                    return Ok(());
                }
                Err(_) => {
                    self.reset_connection().await;
                    sleep(Duration::from_millis(100)).await;
                }
            }
        }

        Err(anyhow!("timed out waiting for guest control-plane module"))
    }

    async fn query_value(&self, query: Query, allow_stale: bool) -> Result<DataValue> {
        let request_id = self.request_id.fetch_add(1, Ordering::Relaxed);
        let frame = encode_request(
            Method::ControlQuery,
            request_id,
            &QueryApiRequest { query, allow_stale },
        )
        .context("encode control-plane query")?;
        let response = self.request_raw(&frame).await?;
        let envelope = decode_envelope(&response).context("decode control-plane query response")?;
        if envelope.method != Method::ControlQuery || envelope.request_id != request_id {
            bail!("control-plane query returned mismatched envelope");
        }
        if is_error(&envelope) {
            let err = decode_error(&envelope).context("decode control-plane query error")?;
            bail!("control-plane guest reported {}: {}", err.code, err.message);
        }
        let response: QueryApiResponse =
            decode_payload(&envelope).context("decode control-plane query payload")?;
        if let Some(error) = response.error {
            bail!(
                "control-plane query failed: {} (leader_hint={:?})",
                error,
                response.leader_hint
            );
        }
        response
            .result
            .ok_or_else(|| anyhow!("control-plane query returned no result"))
    }

    async fn reset_connection(&self) {
        let mut guard = self.connection.lock().await;
        if let Some(connection) = guard.take() {
            connection.close(0u32.into(), b"reset");
        }
    }

    async fn connection(&self) -> Result<Connection> {
        {
            let guard = self.connection.lock().await;
            if let Some(connection) = guard.as_ref()
                && connection.close_reason().is_none()
            {
                return Ok(connection.clone());
            }
        }

        let connecting = self
            .endpoint
            .connect(self.addr, &self.server_name)
            .context("connect guest control-plane")?;
        let connection = timeout(QUIC_CONNECT_TIMEOUT, connecting)
            .await
            .map_err(|_| anyhow!("timed out"))
            .context("await guest control-plane connect")??;
        let mut guard = self.connection.lock().await;
        *guard = Some(connection.clone());
        Ok(connection)
    }
}

fn bootstrap_runtime_session() {
    // This would normally be done by the orchestrator, however during bootstrap we
    // have a chicken-and-egg problem, so we construct the session manually.
    let entitlements = vec![
        Capability::SessionLifecycle,
        Capability::ProcessLifecycle,
        Capability::TimeRead,
        Capability::SharedMemory,
        Capability::QueueLifecycle,
        Capability::QueueWriter,
        Capability::QueueReader,
        Capability::NetworkLifecycle,
        Capability::NetworkConnect,
        Capability::NetworkAccept,
        Capability::NetworkStreamRead,
        Capability::NetworkStreamWrite,
        Capability::NetworkRpcClient,
        Capability::NetworkRpcServer,
    ];
    let _session = Session::bootstrap(entitlements, [0; 32]);
}

pub(crate) async fn run(
    kernel: Kernel,
    registry: Arc<Registry>,
    shutdown: Arc<Notify>,
    work_dir: impl AsRef<Path>,
    modules_from_cli: Option<&Vec<String>>,
) -> Result<()> {
    info!("kernel initialised; starting host bridge");
    bootstrap_runtime_session();

    if let Some(mods) = modules_from_cli {
        modules::spawn_from_cli(&kernel, &registry, &work_dir, mods).await?;
    }

    signal::ctrl_c().await?;
    shutdown.notify_waiters();
    Ok(())
}

pub(crate) async fn run_daemon(
    kernel: Kernel,
    registry: Arc<Registry>,
    shutdown: Arc<Notify>,
    work_dir: PathBuf,
    args: DaemonArgs,
) -> Result<()> {
    info!(listen = %args.listen, "starting runtime daemon");
    bootstrap_runtime_session();

    let cert_path = make_abs(&work_dir, &args.quic_cert);
    let key_path = make_abs(&work_dir, &args.quic_key);
    let peer_cert_path = args
        .quic_peer_cert
        .as_ref()
        .map(|path| make_abs(&work_dir, path))
        .unwrap_or_else(|| cert_path.clone());
    let peer_key_path = args
        .quic_peer_key
        .as_ref()
        .map(|path| make_abs(&work_dir, path))
        .unwrap_or_else(|| key_path.clone());
    let ca_path = make_abs(&work_dir, &args.quic_ca);
    let public_addr = args
        .cp_public_addr
        .clone()
        .unwrap_or_else(|| args.listen.clone());
    let public_addr = normalise_authority_endpoint(&public_addr);
    let internal_addr = match &args.cp_internal_addr {
        Some(addr) => normalise_authority_endpoint(addr),
        None => default_internal_addr(&args.listen)?,
    };
    let tls_paths = ControlPlaneTlsPaths {
        cert_path: &cert_path,
        key_path: &key_path,
        ca_path: &ca_path,
        peer_cert_path: &peer_cert_path,
        peer_key_path: &peer_key_path,
    };
    let addresses = ControlPlaneAddresses {
        public_addr: &public_addr,
        internal_addr: &internal_addr,
    };
    let (control_plane_process_id, control_plane) =
        bootstrap_control_plane(&kernel, &registry, &work_dir, &args, &tls_paths, &addresses)
            .await?;

    let state = Rc::new(DaemonState {
        node_id: args.cp_node_id.clone(),
        kernel,
        registry,
        work_dir,
        processes: Mutex::new(BTreeMap::new()),
        source_bindings: Mutex::new(BTreeMap::new()),
        target_bindings: Mutex::new(BTreeMap::new()),
        service_response_bindings: Mutex::new(BTreeMap::new()),
        active_bridges: Mutex::new(BTreeMap::new()),
        host_subscription_id: AtomicU64::new(1),
        control_plane,
        control_plane_process_id,
        tls_paths: ManagedEventTlsPaths {
            ca_cert: ca_path.clone(),
            client_cert: peer_cert_path.clone(),
            client_key: peer_key_path.clone(),
        },
    });

    let endpoint = build_server_endpoint(&args.listen, &cert_path, &key_path, &ca_path)?;
    let local = LocalSet::new();
    let state_for_loop = Rc::clone(&state);

    local
        .run_until(async move {
            loop {
                tokio::select! {
                    incoming = endpoint.accept() => {
                        let Some(incoming) = incoming else {
                            break;
                        };
                        let state = Rc::clone(&state_for_loop);
                        tokio::task::spawn_local(async move {
                            if let Err(err) = handle_incoming(state, incoming).await {
                                tracing::warn!("daemon QUIC connection error: {err:#}");
                            }
                        });
                    }
                    _ = signal::ctrl_c() => {
                        info!("daemon shutting down after Ctrl-C");
                        break;
                    }
                }
            }
        })
        .await;

    let to_stop = {
        let processes = state.processes.lock().await;
        processes.clone()
    };
    for (instance_id, process_id) in to_stop {
        if let Err(err) = modules::stop_process(&state.kernel, &state.registry, process_id).await {
            tracing::warn!(%instance_id, process_id, "failed to stop process on shutdown: {err:#}");
        }
    }
    if let Err(err) = modules::stop_process(
        &state.kernel,
        &state.registry,
        state.control_plane_process_id,
    )
    .await
    {
        tracing::warn!(
            process_id = state.control_plane_process_id,
            "failed to stop control-plane module on shutdown: {err:#}"
        );
    }

    shutdown.notify_waiters();
    Ok(())
}

async fn bootstrap_control_plane(
    kernel: &Kernel,
    registry: &Arc<Registry>,
    work_dir: &Path,
    args: &DaemonArgs,
    tls_paths: &ControlPlaneTlsPaths<'_>,
    addresses: &ControlPlaneAddresses<'_>,
) -> Result<(usize, Arc<LocalControlPlaneClient>)> {
    let peers = parse_peer_targets(&args.cp_peers)?;
    register_control_plane_runtime_resources(kernel, work_dir, args, tls_paths, addresses, &peers)
        .await?;

    let module_config = ControlPlaneModuleConfig {
        node_id: args.cp_node_id.clone(),
        public_daemon_addr: addresses.public_addr.to_string(),
        public_daemon_server_name: args.cp_server_name.clone(),
        capacity_slots: args.cp_capacity_slots,
        allocatable_cpu_millis: args.cp_allocatable_cpu_millis,
        allocatable_memory_mib: args.cp_allocatable_memory_mib,
        heartbeat_interval_ms: args.cp_heartbeat_interval_ms,
        bootstrap_leader: args.cp_bootstrap_leader,
        peers,
    };

    let process_id = spawn_control_plane_module(kernel, registry, work_dir, &module_config).await?;
    let client = Arc::new(LocalControlPlaneClient::new(
        parse_socket_addr(addresses.internal_addr)?,
        args.cp_server_name.clone(),
        tls_paths.ca_path,
        tls_paths.peer_cert_path,
        tls_paths.peer_key_path,
    )?);
    info!(
        internal_addr = %addresses.internal_addr,
        process_id,
        "waiting for control-plane guest readiness"
    );
    client.wait_until_ready().await?;
    info!(
        internal_addr = %addresses.internal_addr,
        process_id,
        "control-plane guest reported ready"
    );
    Ok((process_id, client))
}

async fn register_control_plane_runtime_resources(
    kernel: &Kernel,
    work_dir: &Path,
    args: &DaemonArgs,
    tls_paths: &ControlPlaneTlsPaths<'_>,
    addresses: &ControlPlaneAddresses<'_>,
    peers: &[PeerTarget],
) -> Result<()> {
    let network = kernel
        .get::<NetworkService>()
        .ok_or_else(|| anyhow!("missing NetworkService in kernel"))?;
    let storage = kernel
        .get::<StorageService>()
        .ok_or_else(|| anyhow!("missing StorageService in kernel"))?;

    let mut allowed_authorities = BTreeSet::new();
    allowed_authorities.insert(addresses.public_addr.to_string());
    for peer in peers {
        allowed_authorities.insert(peer.daemon_addr.clone());
    }

    network
        .register_egress_profile(NetworkEgressProfile {
            name: PEER_PROFILE_NAME.to_string(),
            protocol: NetworkProtocol::Quic,
            interactions: vec![InteractionKind::Stream],
            allowed_authorities: allowed_authorities.into_iter().collect(),
            ca_cert_path: tls_paths.ca_path.to_path_buf(),
            client_cert_path: Some(tls_paths.peer_cert_path.to_path_buf()),
            client_key_path: Some(tls_paths.peer_key_path.to_path_buf()),
        })
        .await;
    network
        .register_ingress_binding(NetworkIngressBinding {
            name: INTERNAL_BINDING_NAME.to_string(),
            protocol: NetworkProtocol::Quic,
            interactions: vec![InteractionKind::Stream],
            listen_addr: addresses.internal_addr.to_string(),
            cert_path: tls_paths.cert_path.to_path_buf(),
            key_path: tls_paths.key_path.to_path_buf(),
        })
        .await;

    let state_dir = make_abs(work_dir, &args.cp_state_dir);
    storage
        .register_log(StorageLogDefinition {
            name: EVENT_LOG_NAME.to_string(),
            path: state_dir.join("events.rkyv"),
            retention: RetentionPolicy::default(),
        })
        .await;
    storage
        .register_blob_store(StorageBlobStoreDefinition {
            name: SNAPSHOT_BLOB_STORE_NAME.to_string(),
            path: state_dir.join("snapshots"),
        })
        .await;

    Ok(())
}

async fn spawn_control_plane_module(
    kernel: &Kernel,
    registry: &Arc<Registry>,
    work_dir: &Path,
    config: &ControlPlaneModuleConfig,
) -> Result<usize> {
    let config_bytes = encode_rkyv(config).context("encode control-plane module config")?;
    let module_spec = format!(
        "path={MODULE_ID};entrypoint={ENTRYPOINT};capabilities={capabilities};network-egress-profiles={PEER_PROFILE_NAME};network-ingress-bindings={INTERNAL_BINDING_NAME};storage-logs={EVENT_LOG_NAME};storage-blobs={SNAPSHOT_BLOB_STORE_NAME};params=buffer;args=buffer:hex:{config_hex}",
        capabilities = control_plane_capabilities().join(","),
        config_hex = encode_hex(&config_bytes),
    );

    let spawned = modules::spawn_from_cli(kernel, registry, work_dir, &[module_spec]).await?;
    spawned
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("control-plane spawn returned no process id"))
}

fn control_plane_capabilities() -> Vec<&'static str> {
    vec![
        "time_read",
        "network_lifecycle",
        "network_connect",
        "network_accept",
        "network_stream_read",
        "network_stream_write",
        "storage_lifecycle",
        "storage_log_read",
        "storage_log_write",
        "storage_blob_read",
        "storage_blob_write",
    ]
}

fn parse_peer_targets(peer_specs: &[String]) -> Result<Vec<PeerTarget>> {
    peer_specs
        .iter()
        .map(|raw| {
            let (node_id, endpoint) = raw.split_once('=').ok_or_else(|| {
                anyhow!("invalid --cp-peer `{raw}` expected node_id=host:port[@server_name]")
            })?;
            let (daemon_addr, explicit_server_name) = endpoint
                .split_once('@')
                .map(|(left, right)| {
                    (
                        normalise_authority_endpoint(left),
                        Some(right.trim().to_string()),
                    )
                })
                .unwrap_or((normalise_authority_endpoint(endpoint), None));
            parse_socket_addr(&daemon_addr)
                .with_context(|| format!("parse peer endpoint `{daemon_addr}` for `{node_id}`"))?;

            Ok(PeerTarget {
                node_id: node_id.to_string(),
                daemon_addr: daemon_addr.clone(),
                daemon_server_name: explicit_server_name
                    .unwrap_or_else(|| derive_server_name(&daemon_addr)),
            })
        })
        .collect()
}

fn normalise_authority_endpoint(raw: &str) -> String {
    raw.trim()
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .trim_start_matches("quic://")
        .trim_end_matches('/')
        .to_string()
}

fn default_internal_addr(listen: &str) -> Result<String> {
    let listen_addr = parse_socket_addr(listen)?;
    let port = listen_addr
        .port()
        .checked_add(1)
        .ok_or_else(|| anyhow!("cannot derive internal control-plane port from `{listen}`"))?;
    let host = if listen_addr.is_ipv6() {
        "[::1]"
    } else {
        "127.0.0.1"
    };
    Ok(format!("{host}:{port}"))
}

fn derive_server_name(endpoint: &str) -> String {
    let host = endpoint
        .rsplit_once(':')
        .map(|(host, _)| host)
        .unwrap_or(endpoint)
        .trim_matches('[')
        .trim_matches(']')
        .to_string();

    if host.parse::<std::net::IpAddr>().is_ok() {
        "localhost".to_string()
    } else {
        host
    }
}

fn encode_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

async fn handle_incoming(state: Rc<DaemonState>, incoming: Incoming) -> Result<()> {
    let connection = incoming.await.context("accept QUIC connection")?;

    loop {
        match connection.accept_bi().await {
            Ok((mut send, mut recv)) => {
                let state = Rc::clone(&state);
                spawn_local(async move {
                    if let Err(err) = handle_stream(state, &mut send, &mut recv).await {
                        tracing::warn!("stream request error: {err:#}");
                    }
                });
            }
            Err(quinn::ConnectionError::ApplicationClosed { .. }) => break,
            Err(quinn::ConnectionError::ConnectionClosed(_)) => break,
            Err(err) => return Err(anyhow!(err).context("accept bidirectional stream")),
        }
    }

    Ok(())
}

async fn handle_stream(
    state: Rc<DaemonState>,
    send: &mut quinn::SendStream,
    recv: &mut quinn::RecvStream,
) -> Result<()> {
    let bytes = read_framed(recv).await.context("read request frame")?;
    let envelope = decode_envelope(&bytes).context("decode request envelope")?;

    if !is_request(&envelope) {
        let response = encode_error_response(
            envelope.method,
            envelope.request_id,
            400,
            "invalid frame flags",
            false,
        )?;
        write_framed(send, &response)
            .await
            .context("write invalid request response")?;
        let _ = send.finish();
        return Ok(());
    }

    if envelope.method == Method::SubscribeGuestLogs {
        return handle_guest_log_subscription_stream(state, send, envelope).await;
    }

    let response = handle_stream_request(state, &bytes, envelope).await?;
    write_framed(send, &response)
        .await
        .context("write response frame")?;
    let _ = send.finish();
    Ok(())
}

async fn handle_stream_request(
    state: Rc<DaemonState>,
    bytes: &[u8],
    envelope: Envelope,
) -> Result<Vec<u8>> {
    match envelope.method {
        Method::ControlMutate
        | Method::ControlQuery
        | Method::ControlStatus
        | Method::ControlMetrics
        | Method::ControlReplay
        | Method::RaftRequestVote
        | Method::RaftAppendEntries => match state.control_plane.request_raw(&bytes).await {
            Ok(response) => Ok(response),
            Err(err) => encode_error_response(
                envelope.method,
                envelope.request_id,
                502,
                err.to_string(),
                true,
            ),
        },
        Method::StartInstance => {
            let payload: StartRequest = match decode_payload(&envelope) {
                Ok(payload) => payload,
                Err(err) => return rpc_decode_error(envelope.method, envelope.request_id, err),
            };
            if payload.node_id != state.node_id {
                return encode_error_response(
                    envelope.method,
                    envelope.request_id,
                    404,
                    format!("node `{}` not served by this daemon", payload.node_id),
                    false,
                );
            }
            if payload.instance_id.trim().is_empty() {
                return encode_error_response(
                    envelope.method,
                    envelope.request_id,
                    400,
                    "instance_id is required",
                    false,
                );
            }

            {
                let processes = state.processes.lock().await;
                if let Some(existing) = processes.get(&payload.instance_id) {
                    return encode_response(
                        envelope.method,
                        envelope.request_id,
                        &StartResponse {
                            status: "ok".to_string(),
                            instance_id: payload.instance_id,
                            process_id: *existing,
                            already_running: true,
                        },
                    );
                }
            }

            let managed_endpoint_bindings = match ensure_managed_endpoint_bindings(
                &state,
                &payload.instance_id,
                &payload.managed_endpoint_bindings,
            )
            .await
            {
                Ok(bindings) => bindings,
                Err(err) => return rpc_server_error(envelope.method, envelope.request_id, err),
            };
            let guest_log_bindings =
                match ensure_guest_log_bindings(&state, &payload.instance_id).await {
                    Ok(bindings) => bindings,
                    Err(err) => return rpc_server_error(envelope.method, envelope.request_id, err),
                };
            let module_spec = match append_managed_endpoint_bindings_arg(
                &payload.module_spec,
                managed_endpoint_bindings.as_deref(),
            ) {
                Ok(spec) => spec,
                Err(err) => return rpc_server_error(envelope.method, envelope.request_id, err),
            };
            let module_spec = append_guest_log_bindings(&module_spec, guest_log_bindings);
            let specs = vec![module_spec];
            let spawned = match modules::spawn_from_cli_with_context(
                &state.kernel,
                &state.registry,
                &state.work_dir,
                &specs,
                modules::ProcessLaunchContext {
                    workload_key: Some(&payload.workload_key),
                    instance_id: Some(&payload.instance_id),
                    external_account_ref: payload.external_account_ref.as_deref(),
                },
            )
            .await
            {
                Ok(spawned) => spawned,
                Err(err) => {
                    return encode_error_response(
                        envelope.method,
                        envelope.request_id,
                        400,
                        err.to_string(),
                        false,
                    );
                }
            };

            let process_id = match spawned.first().copied() {
                Some(id) => id,
                None => {
                    return encode_error_response(
                        envelope.method,
                        envelope.request_id,
                        500,
                        "spawn returned no process id",
                        false,
                    );
                }
            };

            state
                .processes
                .lock()
                .await
                .insert(payload.instance_id.clone(), process_id);

            encode_response(
                envelope.method,
                envelope.request_id,
                &StartResponse {
                    status: "ok".to_string(),
                    instance_id: payload.instance_id,
                    process_id,
                    already_running: false,
                },
            )
        }
        Method::StopInstance => {
            let payload: StopRequest = match decode_payload(&envelope) {
                Ok(payload) => payload,
                Err(err) => return rpc_decode_error(envelope.method, envelope.request_id, err),
            };
            if payload.node_id != state.node_id {
                return encode_error_response(
                    envelope.method,
                    envelope.request_id,
                    404,
                    format!("node `{}` not served by this daemon", payload.node_id),
                    false,
                );
            }

            let process_id = state.processes.lock().await.remove(&payload.instance_id);
            let Some(process_id) = process_id else {
                return encode_response(
                    envelope.method,
                    envelope.request_id,
                    &StopResponse {
                        status: "not_found".to_string(),
                        instance_id: payload.instance_id,
                        process_id: None,
                    },
                );
            };

            state.active_bridges.lock().await.retain(|_, route| {
                let remove = route.spec.source_instance_id() == payload.instance_id
                    || route.spec.target_instance_id() == payload.instance_id;
                if remove {
                    route.bridge_task.abort();
                }
                !remove
            });
            state
                .source_bindings
                .lock()
                .await
                .retain(|(instance_id, _, _), _| instance_id != &payload.instance_id);
            state
                .target_bindings
                .lock()
                .await
                .retain(|(instance_id, _, _), _| instance_id != &payload.instance_id);

            if let Err(err) =
                modules::stop_process(&state.kernel, &state.registry, process_id).await
            {
                return rpc_server_error(envelope.method, envelope.request_id, err);
            }

            encode_response(
                envelope.method,
                envelope.request_id,
                &StopResponse {
                    status: "ok".to_string(),
                    instance_id: payload.instance_id,
                    process_id: Some(process_id),
                },
            )
        }
        Method::ListInstances => {
            let payload: ListRequest = match decode_payload(&envelope) {
                Ok(payload) => payload,
                Err(err) => return rpc_decode_error(envelope.method, envelope.request_id, err),
            };
            if payload.node_id != state.node_id {
                return encode_error_response(
                    envelope.method,
                    envelope.request_id,
                    404,
                    format!("node `{}` not served by this daemon", payload.node_id),
                    false,
                );
            }
            let processes = state.processes.lock().await;
            let entries = processes
                .iter()
                .map(|(instance, process)| (instance.clone(), *process))
                .collect::<BTreeMap<_, _>>();
            let active_bridges = state.active_bridges.lock().await.keys().cloned().collect();
            encode_response(
                envelope.method,
                envelope.request_id,
                &ListResponse {
                    instances: entries,
                    active_bridges,
                },
            )
        }
        Method::ActivateEndpointBridge => {
            let payload: ActivateEndpointBridgeRequest = match decode_payload(&envelope) {
                Ok(payload) => payload,
                Err(err) => return rpc_decode_error(envelope.method, envelope.request_id, err),
            };
            if payload.node_id != state.node_id {
                return encode_error_response(
                    envelope.method,
                    envelope.request_id,
                    404,
                    format!("node `{}` not served by this daemon", payload.node_id),
                    false,
                );
            }
            match activate_endpoint_bridge(&state, &payload).await {
                Ok(response) => encode_response(envelope.method, envelope.request_id, &response),
                Err(err) => rpc_server_error(envelope.method, envelope.request_id, err),
            }
        }
        Method::DeactivateEndpointBridge => {
            let payload: DeactivateEndpointBridgeRequest = match decode_payload(&envelope) {
                Ok(payload) => payload,
                Err(err) => return rpc_decode_error(envelope.method, envelope.request_id, err),
            };
            if payload.node_id != state.node_id {
                return encode_error_response(
                    envelope.method,
                    envelope.request_id,
                    404,
                    format!("node `{}` not served by this daemon", payload.node_id),
                    false,
                );
            }
            match deactivate_endpoint_bridge(&state, &payload).await {
                Ok(response) => encode_response(envelope.method, envelope.request_id, &response),
                Err(err) => rpc_server_error(envelope.method, envelope.request_id, err),
            }
        }
        Method::DeliverBridgeMessage => {
            let payload: DeliverBridgeMessageRequest = match decode_payload(&envelope) {
                Ok(payload) => payload,
                Err(err) => return rpc_decode_error(envelope.method, envelope.request_id, err),
            };
            let message = payload.message;
            let delivered = match deliver_bridge_message_local(
                &state,
                &payload.target_instance_id,
                &payload.target_endpoint,
                &message,
            )
            .await
            {
                Ok(delivered) => delivered,
                Err(err) => return rpc_server_error(envelope.method, envelope.request_id, err),
            };
            let status = if delivered.delivered {
                "ok".to_string()
            } else {
                "not_found".to_string()
            };
            encode_response(
                envelope.method,
                envelope.request_id,
                &DeliverBridgeMessageResponse {
                    status,
                    delivered: delivered.delivered,
                    message: delivered.message,
                },
            )
        }
        Method::RuntimeUsageQuery => {
            let payload: RuntimeUsageApiRequest = match decode_payload(&envelope) {
                Ok(payload) => payload,
                Err(err) => return rpc_decode_error(envelope.method, envelope.request_id, err),
            };
            if payload.node_id != state.node_id {
                return encode_error_response(
                    envelope.method,
                    envelope.request_id,
                    404,
                    format!("node `{}` not served by this daemon", payload.node_id),
                    false,
                );
            }
            match query_runtime_usage(&state, &payload).await {
                Ok(response) => encode_response(envelope.method, envelope.request_id, &response),
                Err(err) => rpc_server_error(envelope.method, envelope.request_id, err),
            }
        }
        Method::SubscribeGuestLogs => unreachable!("subscription streams are handled separately"),
    }
}

async fn query_runtime_usage(
    state: &DaemonState,
    payload: &RuntimeUsageApiRequest,
) -> Result<RuntimeUsageApiResponse> {
    let collector = state
        .kernel
        .get::<crate::usage::RuntimeUsageCollector>()
        .cloned()
        .ok_or_else(|| anyhow!("runtime usage collector unavailable"))?;
    let result = collector.replay_usage(&payload.query).await?;
    let high_watermark = result.high_watermark;
    let next_sequence =
        next_runtime_usage_sequence(&payload.query, &result.records, high_watermark);
    if let Some(name) = payload.query.save_checkpoint.as_deref() {
        let checkpoint_sequence = next_sequence.unwrap_or(collector.next_sequence().await);
        collector.checkpoint(name, checkpoint_sequence).await?;
    }
    Ok(RuntimeUsageApiResponse {
        next_sequence,
        records: result.records,
        high_watermark,
    })
}

fn next_runtime_usage_sequence(
    query: &selium_abi::RuntimeUsageQuery,
    records: &[selium_abi::RuntimeUsageRecord],
    high_watermark: Option<u64>,
) -> Option<u64> {
    if query.limit == 0 {
        return None;
    }
    if let Some(record) = records.last() {
        return Some(record.sequence.saturating_add(1));
    }
    let advanced = high_watermark.map(|sequence| sequence.saturating_add(1));
    match query.start {
        selium_abi::RuntimeUsageReplayStart::Sequence(sequence) => {
            Some(advanced.map_or(sequence, |next| next.max(sequence)))
        }
        selium_abi::RuntimeUsageReplayStart::Checkpoint(_) => advanced,
        _ => advanced,
    }
}

async fn handle_guest_log_subscription_stream(
    state: Rc<DaemonState>,
    send: &mut quinn::SendStream,
    envelope: Envelope,
) -> Result<()> {
    let payload: SubscribeGuestLogsRequest = match decode_payload(&envelope) {
        Ok(payload) => payload,
        Err(err) => {
            let response = rpc_decode_error(envelope.method, envelope.request_id, err)?;
            write_framed(send, &response)
                .await
                .context("write subscription decode error")?;
            let _ = send.finish();
            return Ok(());
        }
    };

    let resolved = match resolve_guest_log_subscription(&state, &payload).await {
        Ok(resolved) => resolved,
        Err(err) => {
            let response = encode_error_response(
                envelope.method,
                envelope.request_id,
                400,
                err.to_string(),
                false,
            )?;
            write_framed(send, &response)
                .await
                .context("write subscription resolution error")?;
            let _ = send.finish();
            return Ok(());
        }
    };

    let subscription = match activate_guest_log_subscription(&state, &resolved).await {
        Ok(subscription) => subscription,
        Err(err) => {
            let response = rpc_server_error(envelope.method, envelope.request_id, err)?;
            write_framed(send, &response)
                .await
                .context("write subscription setup error")?;
            let _ = send.finish();
            return Ok(());
        }
    };

    let response = encode_response(
        envelope.method,
        envelope.request_id,
        &SubscribeGuestLogsResponse {
            status: "ok".to_string(),
            target_node: resolved.target.node.clone(),
            target_instance_id: resolved.target.replica_key.clone(),
            streams: resolved.streams.clone(),
        },
    )?;

    let stream_result = async {
        write_framed(send, &response)
            .await
            .context("write subscription response")?;
        stream_guest_log_events(&state, &subscription.instance_id, &resolved.streams, send).await
    }
    .await;
    let cleanup_result = deactivate_guest_log_subscription(&state, &resolved, &subscription).await;
    let _ = send.finish();

    if let Err(err) = cleanup_result {
        return Err(err);
    }
    match stream_result {
        Ok(()) => Ok(()),
        Err(err) if is_closed_stream_error(&err) => Ok(()),
        Err(err) => Err(err),
    }
}

async fn resolve_guest_log_subscription(
    state: &Rc<DaemonState>,
    payload: &SubscribeGuestLogsRequest,
) -> Result<ResolvedGuestLogSubscription> {
    resolve_guest_log_subscription_with(&state.node_id, payload, |query| {
        state.control_plane.query_value(query, true)
    })
    .await
}

async fn resolve_guest_log_subscription_with<F, Fut>(
    local_node_id: &str,
    payload: &SubscribeGuestLogsRequest,
    query_value: F,
) -> Result<ResolvedGuestLogSubscription>
where
    F: Fn(Query) -> Fut,
    Fut: Future<Output = Result<DataValue>>,
{
    let target = decode_query_bytes::<DiscoveryResolution>(
        query_value(Query::ResolveDiscovery {
            request: DiscoveryRequest {
                operation: DiscoveryOperation::Discover,
                target: DiscoveryTarget::RunningProcess(payload.target.clone()),
                scope: DiscoveryCapabilityScope::allow_all(),
            },
        })
        .await?,
        "running process discovery",
    )?;
    let DiscoveryResolution::RunningProcess(target) = target else {
        bail!("control-plane returned non-process discovery for guest log target");
    };

    let control_plane = decode_query_bytes::<ControlPlaneState>(
        query_value(Query::ControlPlaneState).await?,
        "control-plane state",
    )?;
    let source_node = control_plane
        .nodes
        .get(&target.node)
        .cloned()
        .ok_or_else(|| {
            anyhow!(
                "unknown node `{}` for replica `{}`",
                target.node,
                target.replica_key
            )
        })?;
    let local_node = control_plane
        .nodes
        .get(local_node_id)
        .cloned()
        .ok_or_else(|| {
            anyhow!("current daemon node `{local_node_id}` is missing from control-plane state")
        })?;

    let mut seen = BTreeSet::new();
    let mut streams = Vec::new();
    for stream_name in &payload.stream_names {
        let stream_name = stream_name.trim();
        if stream_name.is_empty() {
            bail!("log stream names must not be empty");
        }
        if !seen.insert(stream_name.to_string()) {
            continue;
        }
        let endpoint = PublicEndpointRef {
            workload: target.workload.clone(),
            kind: ContractKind::Event,
            name: stream_name.to_string(),
        };
        let resolution = decode_query_bytes::<DiscoveryResolution>(
            query_value(Query::ResolveDiscovery {
                request: DiscoveryRequest {
                    operation: DiscoveryOperation::Bind,
                    target: DiscoveryTarget::Endpoint(endpoint),
                    scope: DiscoveryCapabilityScope::allow_all(),
                },
            })
            .await?,
            "guest log endpoint discovery",
        )?;
        let DiscoveryResolution::Endpoint(resolved) = resolution else {
            bail!("control-plane returned non-endpoint discovery for guest log stream");
        };
        if resolved.endpoint.kind != ContractKind::Event {
            bail!(
                "guest log stream `{}` resolved to non-event endpoint kind {:?}",
                resolved.endpoint.name,
                resolved.endpoint.kind
            );
        }
        streams.push(resolved.endpoint);
    }
    if streams.is_empty() {
        bail!("at least one guest log stream is required");
    }

    Ok(ResolvedGuestLogSubscription {
        target,
        source_node,
        local_node,
        streams,
    })
}

fn decode_query_bytes<T>(value: DataValue, subject: &str) -> Result<T>
where
    T: rkyv::Archive + Sized,
    for<'a> T::Archived: rkyv::Deserialize<T, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>
        + rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
{
    let bytes = match value {
        DataValue::Bytes(bytes) => bytes,
        other => bail!("invalid {subject} payload (expected bytes), got {other:?}"),
    };
    decode_rkyv(&bytes).with_context(|| format!("decode {subject}"))
}

async fn activate_guest_log_subscription(
    state: &Rc<DaemonState>,
    resolved: &ResolvedGuestLogSubscription,
) -> Result<ActiveGuestLogSubscription> {
    let subscription_id = state.host_subscription_id.fetch_add(1, Ordering::Relaxed);
    let instance_id = format!("host-log-subscription-{subscription_id}");
    let bindings = resolved
        .streams
        .iter()
        .map(|endpoint| ManagedEndpointBinding {
            endpoint_name: endpoint.name.clone(),
            endpoint_kind: ContractKind::Event,
            role: ManagedEndpointRole::Ingress,
            binding_type: ManagedEndpointBindingType::OneWay,
        })
        .collect::<Vec<_>>();
    ensure_managed_endpoint_bindings(state, &instance_id, &bindings).await?;

    let mut bridge_ids = Vec::new();
    for endpoint in &resolved.streams {
        let bridge_id = format!("{instance_id}:{}", endpoint.name);
        let request = ActivateEndpointBridgeRequest {
            node_id: resolved.target.node.clone(),
            bridge_id: bridge_id.clone(),
            source_instance_id: resolved.target.replica_key.clone(),
            source_endpoint: endpoint.clone(),
            target_instance_id: instance_id.clone(),
            target_node: state.node_id.clone(),
            target_daemon_addr: resolved.local_node.daemon_addr.clone(),
            target_daemon_server_name: resolved.local_node.daemon_server_name.clone(),
            target_endpoint: endpoint.clone(),
            semantics: EndpointBridgeSemantics::Event(
                selium_control_plane_protocol::EventBridgeSemantics {
                    delivery: selium_control_plane_protocol::EventDeliveryMode::Frame,
                },
            ),
        };
        let activation = if resolved.target.node == state.node_id {
            activate_endpoint_bridge(state, &request).await
        } else {
            activate_endpoint_bridge_remote(&state.tls_paths, &resolved.source_node, &request).await
        };
        if let Err(err) = activation {
            let partial = ActiveGuestLogSubscription {
                instance_id: instance_id.clone(),
                bridge_ids,
            };
            let _ = deactivate_guest_log_subscription(state, resolved, &partial).await;
            return Err(err);
        }
        bridge_ids.push(bridge_id);
    }

    Ok(ActiveGuestLogSubscription {
        instance_id,
        bridge_ids,
    })
}

async fn deactivate_guest_log_subscription(
    state: &Rc<DaemonState>,
    resolved: &ResolvedGuestLogSubscription,
    subscription: &ActiveGuestLogSubscription,
) -> Result<()> {
    let mut first_error: Option<anyhow::Error> = None;
    for bridge_id in &subscription.bridge_ids {
        let request = DeactivateEndpointBridgeRequest {
            node_id: resolved.target.node.clone(),
            bridge_id: bridge_id.clone(),
        };
        let outcome = if resolved.target.node == state.node_id {
            deactivate_endpoint_bridge(state, &request)
                .await
                .map(|_| ())
        } else {
            deactivate_endpoint_bridge_remote(&state.tls_paths, &resolved.source_node, &request)
                .await
                .map(|_| ())
        };
        if let Err(err) = outcome
            && first_error.is_none()
        {
            first_error = Some(err);
        }
    }
    state
        .target_bindings
        .lock()
        .await
        .retain(|(instance_id, _, _), _| instance_id != &subscription.instance_id);

    if let Some(err) = first_error {
        return Err(err);
    }
    Ok(())
}

async fn stream_guest_log_events<W>(
    state: &DaemonState,
    subscription_instance_id: &str,
    streams: &[PublicEndpointRef],
    writer: &mut W,
) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut readers = Vec::new();
    for endpoint in streams {
        let queue = {
            let bindings = state.target_bindings.lock().await;
            bindings
                .get(&endpoint_key(subscription_instance_id, endpoint))
                .cloned()
                .ok_or_else(|| {
                    anyhow!(
                        "subscription queue missing for `{}` on `{subscription_instance_id}`",
                        endpoint.name
                    )
                })?
        };
        let reader = QueueService
            .attach(
                &queue.queue,
                QueueAttach {
                    shared_id: 0,
                    role: QueueRole::Reader,
                },
            )
            .with_context(|| format!("attach guest log reader for `{}`", endpoint.name))?;
        readers.push((endpoint.clone(), reader));
    }

    loop {
        for (endpoint, reader) in &readers {
            let waited = QueueService
                .wait(reader, MANAGED_EVENT_RETRY_DELAY.as_millis() as u32)
                .await
                .with_context(|| format!("wait for guest log `{}`", endpoint.name))?;
            match waited.code {
                QueueStatusCode::Timeout => continue,
                QueueStatusCode::Ok => {
                    let Some(frame) = waited.frame else {
                        continue;
                    };
                    let payload = read_managed_event_frame(
                        state,
                        frame.shm_shared_id,
                        frame.offset,
                        frame.len,
                    )
                    .await?;
                    let event = GuestLogEvent {
                        endpoint: endpoint.clone(),
                        payload,
                    };
                    let frame_bytes = encode_rkyv(&event).context("encode guest log event")?;
                    write_framed(writer, &frame_bytes)
                        .await
                        .with_context(|| format!("write guest log `{}`", endpoint.name))?;
                    QueueService
                        .ack(
                            reader,
                            QueueAck {
                                endpoint_id: 0,
                                seq: frame.seq,
                            },
                        )
                        .with_context(|| format!("ack guest log `{}`", endpoint.name))?;
                }
                other => {
                    bail!(
                        "guest log queue wait failed for `{}` with {other:?}",
                        endpoint.name
                    )
                }
            }
        }
    }
}

fn is_closed_stream_error(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause.downcast_ref::<std::io::Error>().is_some_and(|io| {
            matches!(
                io.kind(),
                std::io::ErrorKind::BrokenPipe
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::NotConnected
                    | std::io::ErrorKind::UnexpectedEof
            )
        })
    })
}

async fn activate_endpoint_bridge_remote(
    tls_paths: &ManagedEventTlsPaths,
    node: &NodeSpec,
    payload: &ActivateEndpointBridgeRequest,
) -> Result<ActivateEndpointBridgeResponse> {
    request_remote_daemon(
        tls_paths,
        &node.daemon_addr,
        &node.daemon_server_name,
        Method::ActivateEndpointBridge,
        payload,
    )
    .await
}

async fn deactivate_endpoint_bridge_remote(
    tls_paths: &ManagedEventTlsPaths,
    node: &NodeSpec,
    payload: &DeactivateEndpointBridgeRequest,
) -> Result<DeactivateEndpointBridgeResponse> {
    request_remote_daemon(
        tls_paths,
        &node.daemon_addr,
        &node.daemon_server_name,
        Method::DeactivateEndpointBridge,
        payload,
    )
    .await
}

async fn activate_endpoint_bridge(
    state: &Rc<DaemonState>,
    payload: &ActivateEndpointBridgeRequest,
) -> Result<ActivateEndpointBridgeResponse> {
    let source_key = endpoint_key(&payload.source_instance_id, &payload.source_endpoint);
    if !state.source_bindings.lock().await.contains_key(&source_key) {
        bail!(
            "source endpoint `{}` is not registered for instance `{}`",
            payload.source_endpoint.name,
            payload.source_instance_id
        );
    }
    if !state
        .processes
        .lock()
        .await
        .contains_key(&payload.source_instance_id)
    {
        bail!(
            "source instance `{}` is not running",
            payload.source_instance_id
        );
    }

    let mode = if payload.target_node == state.node_id {
        EndpointBridgeMode::Local
    } else {
        EndpointBridgeMode::Remote
    };

    let spec = build_active_endpoint_bridge_spec(payload, mode)?;

    let mut bridges = state.active_bridges.lock().await;
    if let Some(existing) = bridges.get(&payload.bridge_id)
        && existing.spec == spec
    {
        return Ok(ActivateEndpointBridgeResponse {
            status: "ok".to_string(),
            bridge_id: payload.bridge_id.clone(),
            mode: spec.mode(),
            target_node: spec.target_node().to_string(),
            target_instance_id: spec.target_instance_id().to_string(),
            already_active: true,
        });
    }
    if let Some(existing) = bridges.remove(&payload.bridge_id) {
        existing.bridge_task.abort();
    }

    let state_for_task = Rc::clone(state);
    let bridge_id = payload.bridge_id.clone();
    let spec_for_task = spec.clone();
    let bridge_task = spawn_local(async move {
        if let Err(err) = forward_endpoint_bridge(state_for_task, spec_for_task).await {
            info!(bridge_id, error = %err, "managed endpoint bridge stopped");
        }
    });
    bridges.insert(
        payload.bridge_id.clone(),
        ActiveEndpointBridge {
            spec: spec.clone(),
            bridge_task,
        },
    );

    Ok(ActivateEndpointBridgeResponse {
        status: "ok".to_string(),
        bridge_id: payload.bridge_id.clone(),
        mode: spec.mode(),
        target_node: spec.target_node().to_string(),
        target_instance_id: spec.target_instance_id().to_string(),
        already_active: false,
    })
}

async fn deactivate_endpoint_bridge(
    state: &DaemonState,
    payload: &DeactivateEndpointBridgeRequest,
) -> Result<DeactivateEndpointBridgeResponse> {
    let existed = state.active_bridges.lock().await.remove(&payload.bridge_id);
    if let Some(route) = existed.as_ref() {
        route.bridge_task.abort();
    }
    Ok(DeactivateEndpointBridgeResponse {
        status: if existed.is_some() {
            "ok".to_string()
        } else {
            "not_found".to_string()
        },
        bridge_id: payload.bridge_id.clone(),
        existed: existed.is_some(),
    })
}

fn build_active_endpoint_bridge_spec(
    payload: &ActivateEndpointBridgeRequest,
    mode: EndpointBridgeMode,
) -> Result<ActiveEndpointBridgeSpec> {
    match &payload.semantics {
        EndpointBridgeSemantics::Event(_) => {
            ensure_endpoint_kind("source", &payload.source_endpoint, ContractKind::Event)?;
            ensure_endpoint_kind("target", &payload.target_endpoint, ContractKind::Event)?;
            Ok(ActiveEndpointBridgeSpec::Event(
                ActiveEventEndpointBridgeSpec {
                    source_instance_id: payload.source_instance_id.clone(),
                    source_endpoint: payload.source_endpoint.clone(),
                    target_instance_id: payload.target_instance_id.clone(),
                    target_endpoint: payload.target_endpoint.clone(),
                    mode,
                    target_node: payload.target_node.clone(),
                    target_daemon_addr: payload.target_daemon_addr.clone(),
                    target_daemon_server_name: payload.target_daemon_server_name.clone(),
                },
            ))
        }
        EndpointBridgeSemantics::Service(_) => {
            ensure_endpoint_kind("source", &payload.source_endpoint, ContractKind::Service)?;
            ensure_endpoint_kind("target", &payload.target_endpoint, ContractKind::Service)?;
            Ok(ActiveEndpointBridgeSpec::Service(
                ActiveServiceEndpointBridgeSpec {
                    source_instance_id: payload.source_instance_id.clone(),
                    source_endpoint: payload.source_endpoint.clone(),
                    target_instance_id: payload.target_instance_id.clone(),
                    target_endpoint: payload.target_endpoint.clone(),
                    mode,
                    target_node: payload.target_node.clone(),
                    target_daemon_addr: payload.target_daemon_addr.clone(),
                    target_daemon_server_name: payload.target_daemon_server_name.clone(),
                },
            ))
        }
        EndpointBridgeSemantics::Stream(_) => {
            ensure_endpoint_kind("source", &payload.source_endpoint, ContractKind::Stream)?;
            ensure_endpoint_kind("target", &payload.target_endpoint, ContractKind::Stream)?;
            Ok(ActiveEndpointBridgeSpec::Stream(
                ActiveStreamEndpointBridgeSpec {
                    source_instance_id: payload.source_instance_id.clone(),
                    source_endpoint: payload.source_endpoint.clone(),
                    target_instance_id: payload.target_instance_id.clone(),
                    target_endpoint: payload.target_endpoint.clone(),
                    mode,
                    target_node: payload.target_node.clone(),
                    target_daemon_addr: payload.target_daemon_addr.clone(),
                    target_daemon_server_name: payload.target_daemon_server_name.clone(),
                },
            ))
        }
    }
}

fn ensure_endpoint_kind(
    label: &str,
    endpoint: &PublicEndpointRef,
    expected: ContractKind,
) -> Result<()> {
    if endpoint.kind != expected {
        bail!(
            "{label} endpoint `{}` expected `{}` kind semantics, got `{}`",
            endpoint.name,
            expected.as_str(),
            endpoint.kind.as_str()
        );
    }
    Ok(())
}

async fn ensure_managed_endpoint_bindings(
    state: &DaemonState,
    instance_id: &str,
    bindings: &[ManagedEndpointBinding],
) -> Result<Option<Vec<u8>>> {
    if bindings.is_empty() {
        return Ok(None);
    }

    let mut writers = BTreeMap::<String, BTreeMap<String, DataValue>>::new();
    let mut readers = BTreeMap::<String, BTreeMap<String, DataValue>>::new();
    for binding in bindings {
        let primary_queue = ensure_managed_endpoint_queue(state, instance_id, binding).await?;
        let primary_descriptor = queue_descriptor(binding, primary_queue.queue_shared_id);
        match (binding.role.clone(), binding.binding_type.clone()) {
            (ManagedEndpointRole::Egress, ManagedEndpointBindingType::OneWay)
            | (ManagedEndpointRole::Egress, ManagedEndpointBindingType::Session) => {
                insert_managed_endpoint_descriptor(&mut writers, binding, primary_descriptor);
            }
            (ManagedEndpointRole::Ingress, ManagedEndpointBindingType::OneWay)
            | (ManagedEndpointRole::Ingress, ManagedEndpointBindingType::Session) => {
                insert_managed_endpoint_descriptor(&mut readers, binding, primary_descriptor);
            }
            (ManagedEndpointRole::Egress, ManagedEndpointBindingType::RequestResponse) => {
                let response_queue =
                    ensure_service_response_queue(state, instance_id, binding).await?;
                insert_managed_endpoint_descriptor(&mut writers, binding, primary_descriptor);
                insert_managed_endpoint_descriptor(
                    &mut readers,
                    binding,
                    queue_descriptor(binding, response_queue.queue_shared_id),
                );
            }
            (ManagedEndpointRole::Ingress, ManagedEndpointBindingType::RequestResponse) => {
                let response_queue =
                    ensure_service_response_queue(state, instance_id, binding).await?;
                insert_managed_endpoint_descriptor(&mut readers, binding, primary_descriptor);
                insert_managed_endpoint_descriptor(
                    &mut writers,
                    binding,
                    queue_descriptor(binding, response_queue.queue_shared_id),
                );
            }
        }
    }

    Ok(Some(
        encode_rkyv(&DataValue::Map(BTreeMap::from([
            (
                "writers".to_string(),
                DataValue::Map(encode_managed_endpoint_section(writers)),
            ),
            (
                "readers".to_string(),
                DataValue::Map(encode_managed_endpoint_section(readers)),
            ),
        ])))
        .context("encode managed endpoint bindings")?,
    ))
}

fn insert_managed_endpoint_descriptor(
    section: &mut BTreeMap<String, BTreeMap<String, DataValue>>,
    binding: &ManagedEndpointBinding,
    descriptor: DataValue,
) {
    section
        .entry(binding.endpoint_kind.as_str().to_string())
        .or_default()
        .insert(binding.endpoint_name.clone(), descriptor);
}

fn queue_descriptor(binding: &ManagedEndpointBinding, queue_shared_id: u64) -> DataValue {
    DataValue::Map(BTreeMap::from([
        (
            "queue_shared_id".to_string(),
            DataValue::U64(queue_shared_id),
        ),
        (
            "max_frame_bytes".to_string(),
            DataValue::U64(MANAGED_EVENT_MAX_FRAME_BYTES as u64),
        ),
        (
            "endpoint_kind".to_string(),
            DataValue::String(binding.endpoint_kind.as_str().to_string()),
        ),
    ]))
}

fn encode_managed_endpoint_section(
    section: BTreeMap<String, BTreeMap<String, DataValue>>,
) -> BTreeMap<String, DataValue> {
    section
        .into_iter()
        .map(|(kind, endpoints)| (kind, DataValue::Map(endpoints)))
        .collect()
}

async fn ensure_guest_log_bindings(
    state: &DaemonState,
    instance_id: &str,
) -> Result<ProcessLogBindings> {
    let stdout = ensure_managed_endpoint_queue(
        state,
        instance_id,
        &guest_log_binding(GUEST_LOG_STDOUT_ENDPOINT),
    )
    .await?;
    let stderr = ensure_managed_endpoint_queue(
        state,
        instance_id,
        &guest_log_binding(GUEST_LOG_STDERR_ENDPOINT),
    )
    .await?;

    Ok(ProcessLogBindings {
        stdout_queue_shared_id: Some(stdout.queue_shared_id),
        stderr_queue_shared_id: Some(stderr.queue_shared_id),
    })
}

fn guest_log_binding(endpoint_name: &str) -> ManagedEndpointBinding {
    ManagedEndpointBinding {
        endpoint_name: endpoint_name.to_string(),
        endpoint_kind: ContractKind::Event,
        role: ManagedEndpointRole::Egress,
        binding_type: ManagedEndpointBindingType::OneWay,
    }
}

async fn ensure_managed_endpoint_queue(
    state: &DaemonState,
    instance_id: &str,
    binding: &ManagedEndpointBinding,
) -> Result<ManagedEventEndpointQueue> {
    let key = endpoint_binding_key(instance_id, binding.endpoint_kind, &binding.endpoint_name);
    let bindings = match binding.role {
        ManagedEndpointRole::Egress => &state.source_bindings,
        ManagedEndpointRole::Ingress => &state.target_bindings,
    };

    if let Some(existing) = bindings.lock().await.get(&key).cloned() {
        return Ok(existing);
    }

    let queue = QueueService
        .create(QueueCreate {
            capacity_frames: MANAGED_EVENT_QUEUE_DEPTH,
            max_frame_bytes: MANAGED_EVENT_MAX_FRAME_BYTES,
            delivery: QueueDelivery::Lossless,
            overflow: QueueOverflow::Block,
        })
        .context("create managed event queue")?;
    let handle = state
        .registry
        .add(queue.clone(), None, ResourceType::Queue)
        .context("register managed event queue")?;
    let queue_shared_id = state
        .registry
        .share_handle(handle.into_id())
        .context("share managed event queue")?;
    let registered = ManagedEventEndpointQueue {
        queue,
        queue_shared_id,
    };
    bindings.lock().await.insert(key, registered.clone());
    Ok(registered)
}

async fn ensure_service_response_queue(
    state: &DaemonState,
    instance_id: &str,
    binding: &ManagedEndpointBinding,
) -> Result<ManagedEventEndpointQueue> {
    let key = endpoint_binding_key(instance_id, binding.endpoint_kind, &binding.endpoint_name);
    if let Some(existing) = state
        .service_response_bindings
        .lock()
        .await
        .get(&key)
        .cloned()
    {
        return Ok(existing);
    }

    let queue = QueueService
        .create(QueueCreate {
            capacity_frames: MANAGED_EVENT_QUEUE_DEPTH,
            max_frame_bytes: MANAGED_EVENT_MAX_FRAME_BYTES,
            delivery: QueueDelivery::Lossless,
            overflow: QueueOverflow::Block,
        })
        .context("create managed service response queue")?;
    let handle = state
        .registry
        .add(queue.clone(), None, ResourceType::Queue)
        .context("register managed service response queue")?;
    let queue_shared_id = state
        .registry
        .share_handle(handle.into_id())
        .context("share managed service response queue")?;
    let registered = ManagedEventEndpointQueue {
        queue,
        queue_shared_id,
    };
    state
        .service_response_bindings
        .lock()
        .await
        .insert(key, registered.clone());
    Ok(registered)
}

fn append_managed_endpoint_bindings_arg(
    module_spec: &str,
    bindings: Option<&[u8]>,
) -> Result<String> {
    let Some(bytes) = bindings else {
        return Ok(module_spec.to_string());
    };

    let binding_arg = format!("buffer:hex:{}", encode_hex(bytes));
    let mut entries: Vec<String> = module_spec
        .replace(';', "\n")
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned)
        .collect();
    let mut params_found = false;
    let mut args_found = false;

    for entry in &mut entries {
        if let Some(existing) = entry.strip_prefix("params=") {
            *entry = format!("params={}", prepend_module_spec_value("buffer", existing));
            params_found = true;
        } else if let Some(existing) = entry.strip_prefix("param=") {
            *entry = format!("param={}", prepend_module_spec_value("buffer", existing));
            params_found = true;
        } else if let Some(existing) = entry.strip_prefix("args=") {
            *entry = format!("args={}", prepend_module_spec_value(&binding_arg, existing));
            args_found = true;
        }
    }

    if !args_found {
        entries.push(format!("args={binding_arg}"));
    }
    if !params_found && !args_found {
        entries.push("params=buffer".to_string());
    }

    Ok(entries.join(";"))
}

fn append_guest_log_bindings(module_spec: &str, bindings: ProcessLogBindings) -> String {
    if bindings.is_empty() {
        return module_spec.to_string();
    }

    let mut entries: Vec<String> = module_spec
        .replace(';', "\n")
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned)
        .collect();

    if let Some(shared_id) = bindings.stdout_queue_shared_id {
        entries.push(format!(
            "{}={shared_id}",
            modules::MODULE_SPEC_GUEST_LOG_STDOUT_QUEUE_SHARED_ID
        ));
    }
    if let Some(shared_id) = bindings.stderr_queue_shared_id {
        entries.push(format!(
            "{}={shared_id}",
            modules::MODULE_SPEC_GUEST_LOG_STDERR_QUEUE_SHARED_ID
        ));
    }

    entries.join(";")
}

fn prepend_module_spec_value(prefix: &str, existing: &str) -> String {
    let existing = existing.trim();
    if existing.is_empty() {
        prefix.to_string()
    } else {
        format!("{prefix},{existing}")
    }
}

async fn forward_endpoint_bridge(
    state: Rc<DaemonState>,
    spec: ActiveEndpointBridgeSpec,
) -> Result<()> {
    match spec {
        ActiveEndpointBridgeSpec::Event(spec) => forward_event_endpoint_bridge(state, spec).await,
        ActiveEndpointBridgeSpec::Service(spec) => {
            forward_service_endpoint_bridge(state, spec).await
        }
        ActiveEndpointBridgeSpec::Stream(spec) => forward_stream_endpoint_bridge(state, spec).await,
    }
}

async fn forward_stream_endpoint_bridge(
    state: Rc<DaemonState>,
    spec: ActiveStreamEndpointBridgeSpec,
) -> Result<()> {
    let source_queue = {
        let bindings = state.source_bindings.lock().await;
        bindings
            .get(&endpoint_key(
                &spec.source_instance_id,
                &spec.source_endpoint,
            ))
            .cloned()
            .ok_or_else(|| {
                anyhow!(
                    "missing source binding for `{}` on `{}`",
                    spec.source_endpoint.name,
                    spec.source_instance_id
                )
            })?
    };

    let reader = QueueService
        .attach(
            &source_queue.queue,
            QueueAttach {
                shared_id: 0,
                role: QueueRole::Reader,
            },
        )
        .context("attach managed stream route reader")?;

    loop {
        let waited = QueueService
            .wait(&reader, MANAGED_EVENT_RETRY_DELAY.as_millis() as u32)
            .await
            .context("wait for managed stream frame")?;
        match waited.code {
            QueueStatusCode::Timeout => continue,
            QueueStatusCode::Ok => {
                let Some(frame) = waited.frame else {
                    continue;
                };
                let message =
                    read_managed_stream_frame(&state, frame.shm_shared_id, frame.offset, frame.len)
                        .await?;
                let delivered = match spec.mode {
                    EndpointBridgeMode::Local => {
                        deliver_bridge_message_local(
                            &state,
                            &spec.target_instance_id,
                            &spec.target_endpoint,
                            &BridgeMessage::Stream(message.clone()),
                        )
                        .await?
                        .delivered
                    }
                    EndpointBridgeMode::Remote => match deliver_bridge_message_remote(
                        state.tls_paths.clone(),
                        &spec.target_daemon_addr,
                        &spec.target_daemon_server_name,
                        &spec.target_instance_id,
                        &spec.target_endpoint,
                        BridgeMessage::Stream(message.clone()),
                    )
                    .await
                    {
                        Ok(delivery) => delivery.delivered,
                        Err(err) => {
                            tracing::warn!(
                                source = %spec.source_endpoint.key(),
                                target = %spec.target_endpoint.key(),
                                target_node = %spec.target_node,
                                session_id = %message.session_id,
                                lifecycle = ?message.lifecycle,
                                error = %err,
                                "remote managed stream delivery attempt failed; retrying"
                            );
                            sleep(MANAGED_EVENT_RETRY_DELAY).await;
                            continue;
                        }
                    },
                };
                if delivered {
                    if spec.mode == EndpointBridgeMode::Remote {
                        info!(
                            "delivered remote managed stream frame {} -> {}",
                            spec.source_endpoint.key(),
                            spec.target_endpoint.key()
                        );
                    }
                    QueueService
                        .ack(
                            &reader,
                            QueueAck {
                                endpoint_id: 0,
                                seq: frame.seq,
                            },
                        )
                        .context("ack managed stream frame")?;
                } else {
                    sleep(MANAGED_EVENT_RETRY_DELAY).await;
                }
            }
            other => bail!("managed stream queue wait failed with {other:?}"),
        }
    }
}

async fn forward_service_endpoint_bridge(
    state: Rc<DaemonState>,
    spec: ActiveServiceEndpointBridgeSpec,
) -> Result<()> {
    let source_queue = {
        let bindings = state.source_bindings.lock().await;
        bindings
            .get(&endpoint_key(
                &spec.source_instance_id,
                &spec.source_endpoint,
            ))
            .cloned()
            .ok_or_else(|| {
                anyhow!(
                    "missing source binding for `{}` on `{}`",
                    spec.source_endpoint.name,
                    spec.source_instance_id
                )
            })?
    };

    let reader = QueueService
        .attach(
            &source_queue.queue,
            QueueAttach {
                shared_id: 0,
                role: QueueRole::Reader,
            },
        )
        .context("attach managed service route reader")?;

    loop {
        let waited = QueueService
            .wait(&reader, MANAGED_EVENT_RETRY_DELAY.as_millis() as u32)
            .await
            .context("wait for managed service frame")?;
        match waited.code {
            QueueStatusCode::Timeout => continue,
            QueueStatusCode::Ok => {
                let Some(frame) = waited.frame else {
                    continue;
                };
                let message = read_managed_service_frame(
                    &state,
                    frame.shm_shared_id,
                    frame.offset,
                    frame.len,
                )
                .await?;
                if message.phase != ServiceMessagePhase::Request {
                    bail!(
                        "managed service bridge expected request phase for `{}`",
                        spec.source_endpoint.key()
                    );
                }

                let delivered = match spec.mode {
                    EndpointBridgeMode::Local => {
                        deliver_bridge_message_local(
                            &state,
                            &spec.target_instance_id,
                            &spec.target_endpoint,
                            &BridgeMessage::Service(message.clone()),
                        )
                        .await?
                    }
                    EndpointBridgeMode::Remote => match deliver_bridge_message_remote(
                        state.tls_paths.clone(),
                        &spec.target_daemon_addr,
                        &spec.target_daemon_server_name,
                        &spec.target_instance_id,
                        &spec.target_endpoint,
                        BridgeMessage::Service(message.clone()),
                    )
                    .await
                    {
                        Ok(delivery) => delivery,
                        Err(err) => {
                            tracing::warn!(
                                source = %spec.source_endpoint.key(),
                                target = %spec.target_endpoint.key(),
                                target_node = %spec.target_node,
                                exchange_id = %message.exchange_id,
                                error = %err,
                                "remote managed service delivery attempt failed; retrying"
                            );
                            sleep(MANAGED_EVENT_RETRY_DELAY).await;
                            continue;
                        }
                    },
                };

                if !delivered.delivered {
                    sleep(MANAGED_EVENT_RETRY_DELAY).await;
                    continue;
                }

                let response = match delivered.message {
                    Some(BridgeMessage::Service(response)) => response,
                    Some(other) => bail!("service bridge received unexpected response {other:?}"),
                    None => bail!(
                        "service bridge missing correlated response for exchange `{}`",
                        message.exchange_id
                    ),
                };

                let response_delivery = deliver_bridge_message_local(
                    &state,
                    &spec.source_instance_id,
                    &spec.source_endpoint,
                    &BridgeMessage::Service(response),
                )
                .await?;
                if !response_delivery.delivered {
                    bail!(
                        "source response binding missing for `{}` on `{}`",
                        spec.source_endpoint.name,
                        spec.source_instance_id
                    );
                }

                QueueService
                    .ack(
                        &reader,
                        QueueAck {
                            endpoint_id: 0,
                            seq: frame.seq,
                        },
                    )
                    .context("ack managed service frame")?;
            }
            other => bail!("managed service queue wait failed with {other:?}"),
        }
    }
}

async fn forward_event_endpoint_bridge(
    state: Rc<DaemonState>,
    spec: ActiveEventEndpointBridgeSpec,
) -> Result<()> {
    let source_queue = {
        let bindings = state.source_bindings.lock().await;
        bindings
            .get(&endpoint_key(
                &spec.source_instance_id,
                &spec.source_endpoint,
            ))
            .cloned()
            .ok_or_else(|| {
                anyhow!(
                    "missing source binding for `{}` on `{}`",
                    spec.source_endpoint.name,
                    spec.source_instance_id
                )
            })?
    };

    let reader = QueueService
        .attach(
            &source_queue.queue,
            QueueAttach {
                shared_id: 0,
                role: QueueRole::Reader,
            },
        )
        .context("attach managed event route reader")?;

    loop {
        let waited = QueueService
            .wait(&reader, MANAGED_EVENT_RETRY_DELAY.as_millis() as u32)
            .await
            .context("wait for managed event frame")?;
        match waited.code {
            QueueStatusCode::Timeout => continue,
            QueueStatusCode::Ok => {
                let Some(frame) = waited.frame else {
                    continue;
                };
                let payload =
                    read_managed_event_frame(&state, frame.shm_shared_id, frame.offset, frame.len)
                        .await?;
                let message = BridgeMessage::Event(EventBridgeMessage { payload });
                let delivered = match spec.mode {
                    EndpointBridgeMode::Local => {
                        deliver_bridge_message_local(
                            &state,
                            &spec.target_instance_id,
                            &spec.target_endpoint,
                            &message,
                        )
                        .await?
                        .delivered
                    }
                    EndpointBridgeMode::Remote => match deliver_bridge_message_remote(
                        state.tls_paths.clone(),
                        &spec.target_daemon_addr,
                        &spec.target_daemon_server_name,
                        &spec.target_instance_id,
                        &spec.target_endpoint,
                        message.clone(),
                    )
                    .await
                    {
                        Ok(delivery) => delivery.delivered,
                        Err(err) => {
                            tracing::warn!(
                                source = %spec.source_endpoint.key(),
                                target = %spec.target_endpoint.key(),
                                target_node = %spec.target_node,
                                error = %err,
                                "remote managed event delivery attempt failed; retrying"
                            );
                            sleep(MANAGED_EVENT_RETRY_DELAY).await;
                            continue;
                        }
                    },
                };
                if delivered {
                    if spec.mode == EndpointBridgeMode::Remote {
                        info!(
                            "delivered remote managed event frame {} -> {}",
                            spec.source_endpoint.key(),
                            spec.target_endpoint.key()
                        );
                    }
                    QueueService
                        .ack(
                            &reader,
                            QueueAck {
                                endpoint_id: 0,
                                seq: frame.seq,
                            },
                        )
                        .context("ack managed event frame")?;
                } else {
                    sleep(MANAGED_EVENT_RETRY_DELAY).await;
                }
            }
            other => bail!("managed event queue wait failed with {other:?}"),
        }
    }
}

async fn deliver_bridge_message_local(
    state: &DaemonState,
    target_instance_id: &str,
    target_endpoint: &PublicEndpointRef,
    message: &BridgeMessage,
) -> Result<BridgeMessageDelivery> {
    match message {
        BridgeMessage::Event(frame) => Ok(BridgeMessageDelivery {
            delivered: deliver_event_frame(
                state,
                target_instance_id,
                target_endpoint,
                &frame.payload,
            )
            .await?,
            message: None,
        }),
        BridgeMessage::Service(frame) => {
            deliver_service_frame(state, target_instance_id, target_endpoint, frame).await
        }
        BridgeMessage::Stream(frame) => Ok(BridgeMessageDelivery {
            delivered: deliver_stream_frame(state, target_instance_id, target_endpoint, frame)
                .await?,
            message: None,
        }),
    }
}

async fn read_managed_stream_frame(
    state: &DaemonState,
    shm_shared_id: u64,
    offset: u32,
    len: u32,
) -> Result<StreamBridgeMessage> {
    let payload = read_managed_event_frame(state, shm_shared_id, offset, len).await?;
    decode_rkyv(&payload).context("decode managed stream frame")
}

async fn read_managed_service_frame(
    state: &DaemonState,
    shm_shared_id: u64,
    offset: u32,
    len: u32,
) -> Result<ServiceBridgeMessage> {
    let payload = read_managed_event_frame(state, shm_shared_id, offset, len).await?;
    decode_rkyv(&payload).context("decode managed service frame")
}

async fn deliver_service_frame(
    state: &DaemonState,
    target_instance_id: &str,
    target_endpoint: &PublicEndpointRef,
    message: &ServiceBridgeMessage,
) -> Result<BridgeMessageDelivery> {
    match message.phase {
        ServiceMessagePhase::Request => {
            deliver_service_request(state, target_instance_id, target_endpoint, message).await
        }
        ServiceMessagePhase::Response => Ok(BridgeMessageDelivery {
            delivered: deliver_service_response(
                state,
                target_instance_id,
                target_endpoint,
                message,
            )
            .await?,
            message: None,
        }),
    }
}

async fn deliver_service_request(
    state: &DaemonState,
    target_instance_id: &str,
    target_endpoint: &PublicEndpointRef,
    message: &ServiceBridgeMessage,
) -> Result<BridgeMessageDelivery> {
    let request_binding = {
        let bindings = state.target_bindings.lock().await;
        bindings
            .get(&endpoint_key(target_instance_id, target_endpoint))
            .cloned()
    };
    let response_binding = {
        let bindings = state.service_response_bindings.lock().await;
        bindings
            .get(&endpoint_key(target_instance_id, target_endpoint))
            .cloned()
    };
    let (Some(request_binding), Some(response_binding)) = (request_binding, response_binding)
    else {
        return Ok(BridgeMessageDelivery {
            delivered: false,
            message: None,
        });
    };

    enqueue_managed_service_frame(state, &request_binding.queue, message).await?;
    let response =
        await_service_response(state, &response_binding.queue, &message.exchange_id).await?;
    Ok(BridgeMessageDelivery {
        delivered: true,
        message: Some(BridgeMessage::Service(response)),
    })
}

async fn deliver_service_response(
    state: &DaemonState,
    target_instance_id: &str,
    target_endpoint: &PublicEndpointRef,
    message: &ServiceBridgeMessage,
) -> Result<bool> {
    let binding = {
        let bindings = state.service_response_bindings.lock().await;
        bindings
            .get(&endpoint_key(target_instance_id, target_endpoint))
            .cloned()
    };
    let Some(binding) = binding else {
        return Ok(false);
    };
    enqueue_managed_service_frame(state, &binding.queue, message).await?;
    Ok(true)
}

async fn await_service_response(
    state: &DaemonState,
    queue: &selium_kernel::services::queue_service::QueueState,
    exchange_id: &str,
) -> Result<ServiceBridgeMessage> {
    let reader = QueueService
        .attach(
            queue,
            QueueAttach {
                shared_id: 0,
                role: QueueRole::Reader,
            },
        )
        .context("attach managed service response reader")?;
    let waited = QueueService
        .wait(&reader, QUIC_REQUEST_TIMEOUT.as_millis() as u32)
        .await
        .context("wait for managed service response")?;
    match waited.code {
        QueueStatusCode::Ok => {
            let frame = waited
                .frame
                .ok_or_else(|| anyhow!("missing managed service response frame"))?;
            let response =
                read_managed_service_frame(state, frame.shm_shared_id, frame.offset, frame.len)
                    .await?;
            if response.phase != ServiceMessagePhase::Response {
                bail!("managed service response queue yielded non-response phase");
            }
            if response.exchange_id != exchange_id {
                bail!(
                    "managed service response exchange mismatch: expected `{exchange_id}`, got `{}`",
                    response.exchange_id
                );
            }
            QueueService
                .ack(
                    &reader,
                    QueueAck {
                        endpoint_id: 0,
                        seq: frame.seq,
                    },
                )
                .context("ack managed service response")?;
            Ok(response)
        }
        QueueStatusCode::Timeout => {
            bail!("timed out waiting for service response for exchange `{exchange_id}`")
        }
        other => bail!("managed service response wait failed with {other:?}"),
    }
}

async fn read_managed_event_frame(
    state: &DaemonState,
    shm_shared_id: u64,
    offset: u32,
    len: u32,
) -> Result<Vec<u8>> {
    let shm_id = state
        .registry
        .resolve_shared(shm_shared_id)
        .context("resolve managed event shared memory")?;
    let region = state
        .registry
        .with(
            ResourceHandle::new(shm_id),
            |region: &mut selium_abi::ShmRegion| *region,
        )
        .context("load managed event shared-memory region")?;
    let driver = state
        .kernel
        .get::<SharedMemoryDriver>()
        .ok_or_else(|| anyhow!("shared memory driver unavailable"))?;
    Ok(driver.read(region, offset, len)?)
}

async fn deliver_event_frame(
    state: &DaemonState,
    target_instance_id: &str,
    target_endpoint: &PublicEndpointRef,
    payload: &[u8],
) -> Result<bool> {
    let binding = {
        let bindings = state.target_bindings.lock().await;
        bindings
            .get(&endpoint_key(target_instance_id, target_endpoint))
            .cloned()
    };
    let Some(binding) = binding else {
        return Ok(false);
    };
    enqueue_managed_event_frame(state, &binding.queue, payload).await?;
    Ok(true)
}

async fn deliver_stream_frame(
    state: &DaemonState,
    target_instance_id: &str,
    target_endpoint: &PublicEndpointRef,
    message: &StreamBridgeMessage,
) -> Result<bool> {
    let binding = {
        let bindings = state.target_bindings.lock().await;
        bindings
            .get(&endpoint_key(target_instance_id, target_endpoint))
            .cloned()
    };
    let Some(binding) = binding else {
        return Ok(false);
    };
    enqueue_managed_stream_frame(state, &binding.queue, message).await?;
    Ok(true)
}

async fn enqueue_managed_event_frame(
    state: &DaemonState,
    queue: &selium_kernel::services::queue_service::QueueState,
    payload: &[u8],
) -> Result<()> {
    let driver = state
        .kernel
        .get::<SharedMemoryDriver>()
        .ok_or_else(|| anyhow!("shared memory driver unavailable"))?;
    let region = driver
        .alloc(ShmAlloc {
            size: payload.len() as u32,
            align: 8,
        })
        .context("allocate bridge payload shared memory")?;
    driver
        .write(region, 0, payload)
        .context("write bridge payload shared memory")?;
    let shm = state
        .registry
        .add(region, None, ResourceType::SharedMemory)
        .context("register bridge payload shared memory")?;
    let shm_shared_id = state
        .registry
        .share_handle(shm.into_id())
        .context("share bridge payload shared memory")?;
    let writer = QueueService
        .attach(
            queue,
            QueueAttach {
                shared_id: 0,
                role: QueueRole::Writer { writer_id: 0 },
            },
        )
        .context("attach bridge queue writer")?;
    let reserved = QueueService
        .reserve(
            &writer,
            QueueReserve {
                endpoint_id: 0,
                len: payload.len() as u32,
                timeout_ms: 1_000,
            },
        )
        .await
        .context("reserve bridge queue slot")?;
    if reserved.code != QueueStatusCode::Ok {
        bail!("failed to reserve bridge queue slot: {:?}", reserved.code);
    }
    let reservation = reserved
        .reservation
        .ok_or_else(|| anyhow!("missing queue reservation"))?;
    QueueService
        .commit(
            &writer,
            QueueCommit {
                endpoint_id: 0,
                reservation_id: reservation.reservation_id,
                shm_shared_id,
                offset: 0,
                len: payload.len() as u32,
            },
        )
        .context("commit bridge queue slot")?;
    Ok(())
}

async fn enqueue_managed_service_frame(
    state: &DaemonState,
    queue: &selium_kernel::services::queue_service::QueueState,
    message: &ServiceBridgeMessage,
) -> Result<()> {
    let payload = encode_rkyv(message).context("encode managed service frame")?;
    enqueue_managed_event_frame(state, queue, &payload).await
}

async fn enqueue_managed_stream_frame(
    state: &DaemonState,
    queue: &selium_kernel::services::queue_service::QueueState,
    message: &StreamBridgeMessage,
) -> Result<()> {
    let payload = encode_rkyv(message).context("encode managed stream frame")?;
    enqueue_managed_event_frame(state, queue, &payload).await
}

async fn request_remote_daemon<Req, Resp>(
    tls_paths: &ManagedEventTlsPaths,
    target_daemon_addr: &str,
    target_daemon_server_name: &str,
    method: Method,
    payload: &Req,
) -> Result<Resp>
where
    Req: selium_abi::RkyvEncode,
    Resp: rkyv::Archive + Sized,
    for<'a> Resp::Archived: rkyv::Deserialize<Resp, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>
        + rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
{
    let endpoint = build_client_endpoint(
        &tls_paths.ca_cert,
        &tls_paths.client_cert,
        &tls_paths.client_key,
    )?;
    let connection = timeout(
        QUIC_CONNECT_TIMEOUT,
        endpoint.connect(
            target_daemon_addr.parse::<SocketAddr>()?,
            target_daemon_server_name,
        )?,
    )
    .await
    .map_err(|_| anyhow!("timed out"))
    .context("connect remote daemon")??;
    let request_id = 1;
    let frame =
        encode_request(method, request_id, payload).context("encode remote daemon request")?;
    let (mut send, mut recv) = timeout(QUIC_REQUEST_TIMEOUT, connection.open_bi())
        .await
        .map_err(|_| anyhow!("timed out"))
        .context("open remote route stream")??;
    timeout(QUIC_REQUEST_TIMEOUT, write_framed(&mut send, &frame))
        .await
        .map_err(|_| anyhow!("timed out"))
        .context("write remote route request")??;
    let _ = send.finish();
    let response = timeout(QUIC_REQUEST_TIMEOUT, read_framed(&mut recv))
        .await
        .map_err(|_| anyhow!("timed out"))
        .context("read remote route response")??;
    let envelope = decode_envelope(&response).context("decode remote route response")?;
    if envelope.method != method || envelope.request_id != request_id {
        bail!("remote daemon response mismatch");
    }
    if is_error(&envelope) {
        let err = decode_error(&envelope).context("decode remote route error")?;
        bail!("remote daemon returned {}: {}", err.code, err.message);
    }
    decode_payload(&envelope).context("decode remote route payload")
}

async fn deliver_bridge_message_remote(
    tls_paths: ManagedEventTlsPaths,
    target_daemon_addr: &str,
    target_daemon_server_name: &str,
    target_instance_id: &str,
    target_endpoint: &PublicEndpointRef,
    message: BridgeMessage,
) -> Result<BridgeMessageDelivery> {
    let delivered: DeliverBridgeMessageResponse = request_remote_daemon(
        &tls_paths,
        target_daemon_addr,
        target_daemon_server_name,
        Method::DeliverBridgeMessage,
        &DeliverBridgeMessageRequest {
            target_instance_id: target_instance_id.to_string(),
            target_endpoint: target_endpoint.clone(),
            message,
        },
    )
    .await?;
    Ok(BridgeMessageDelivery {
        delivered: delivered.delivered,
        message: delivered.message,
    })
}

fn endpoint_binding_key(
    instance_id: &str,
    endpoint_kind: ContractKind,
    endpoint_name: &str,
) -> (String, ContractKind, String) {
    (
        instance_id.to_string(),
        endpoint_kind,
        endpoint_name.to_string(),
    )
}

fn endpoint_key(instance_id: &str, endpoint: &PublicEndpointRef) -> (String, ContractKind, String) {
    endpoint_binding_key(instance_id, endpoint.kind, &endpoint.name)
}

fn rpc_decode_error(method: Method, request_id: u64, error: anyhow::Error) -> Result<Vec<u8>> {
    encode_error_response(method, request_id, 400, error.to_string(), false)
}

fn rpc_server_error(method: Method, request_id: u64, error: anyhow::Error) -> Result<Vec<u8>> {
    encode_error_response(method, request_id, 500, error.to_string(), false)
}

fn build_server_endpoint(
    listen: &str,
    cert_path: &Path,
    key_path: &Path,
    ca_path: &Path,
) -> Result<Endpoint> {
    let cert_chain = load_cert_chain(cert_path)?;
    let key = load_private_key(key_path)?;
    let roots = load_root_store(ca_path)?;
    let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
        .build()
        .context("build mTLS verifier")?;

    let server_tls = rustls::ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(cert_chain, key)
        .context("build QUIC server TLS config")?;

    let quic_server = QuicServerConfig::try_from(server_tls).context("build QUIC server config")?;
    let server_config = quinn::ServerConfig::with_crypto(Arc::new(quic_server));

    let bind_addr =
        parse_socket_addr(listen).with_context(|| format!("parse listen addr {listen}"))?;
    Endpoint::server(server_config, bind_addr).context("bind QUIC endpoint")
}

fn build_client_endpoint(
    ca_path: &Path,
    client_cert_path: &Path,
    client_key_path: &Path,
) -> Result<Endpoint> {
    let bind = if cfg!(target_family = "unix") {
        "0.0.0.0:0"
    } else {
        "127.0.0.1:0"
    }
    .parse::<SocketAddr>()?;

    let mut endpoint = Endpoint::client(bind).context("create QUIC client endpoint")?;
    let roots = load_root_store(ca_path)?;
    let cert_chain = load_cert_chain(client_cert_path)?;
    let key = load_private_key(client_key_path)?;

    let tls = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(cert_chain, key)
        .context("build QUIC client TLS config")?;
    let quic_crypto = QuicClientConfig::try_from(tls).context("build QUIC client config")?;
    endpoint.set_default_client_config(quinn::ClientConfig::new(Arc::new(quic_crypto)));
    Ok(endpoint)
}

fn parse_socket_addr(raw: &str) -> Result<SocketAddr> {
    if let Ok(addr) = raw.parse::<SocketAddr>() {
        return Ok(addr);
    }

    raw.to_socket_addrs()?
        .next()
        .ok_or_else(|| anyhow!("no socket addresses for `{raw}`"))
}

fn load_root_store(path: &Path) -> Result<RootCertStore> {
    let pem = fs::read(path).with_context(|| format!("read CA cert {}", path.display()))?;
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
    let pem = fs::read(path).with_context(|| format!("read cert {}", path.display()))?;
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
    let pem = fs::read(path).with_context(|| format!("read key {}", path.display()))?;
    let mut reader = std::io::Cursor::new(pem);
    private_key(&mut reader)
        .context("parse private key")?
        .ok_or_else(|| anyhow!("no private key in {}", path.display()))
}

fn make_abs(work_dir: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        work_dir.join(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    use selium_abi::{RuntimeUsageQuery, RuntimeUsageReplayStart, decode_rkyv};
    use selium_control_plane_api::{
        ContractRef, ControlPlaneState, DeploymentSpec, IsolationProfile, NodeSpec,
        OperationalProcessSelector, WorkloadRef, parse_idl,
    };
    use selium_module_control_plane::runtime::ControlPlaneEngine;
    use tokio::io::duplex;

    use crate::usage::{ProcessUsageAttribution, RuntimeUsageCollector};

    #[tokio::test(flavor = "current_thread")]
    async fn activate_endpoint_bridge_forwards_local_event_frames() {
        LocalSet::new()
            .run_until(async {
                let state = sample_state("local-node");
                state
                    .processes
                    .lock()
                    .await
                    .insert("source-1".to_string(), 7);
                ensure_managed_endpoint_bindings(
                    &state,
                    "source-1",
                    &[ManagedEndpointBinding {
                        endpoint_name: "camera.frames".to_string(),
                        endpoint_kind: ContractKind::Event,
                        role: ManagedEndpointRole::Egress,
                        binding_type:
                            selium_control_plane_protocol::ManagedEndpointBindingType::OneWay,
                    }],
                )
                .await
                .expect("register source binding");
                ensure_managed_endpoint_bindings(
                    &state,
                    "target-1",
                    &[ManagedEndpointBinding {
                        endpoint_name: "camera.frames".to_string(),
                        endpoint_kind: ContractKind::Event,
                        role: ManagedEndpointRole::Ingress,
                        binding_type:
                            selium_control_plane_protocol::ManagedEndpointBindingType::OneWay,
                    }],
                )
                .await
                .expect("register target binding");

                let response = activate_endpoint_bridge(
                    &state,
                    &ActivateEndpointBridgeRequest {
                        node_id: "local-node".to_string(),
                        bridge_id: "bridge-1".to_string(),
                        source_instance_id: "source-1".to_string(),
                        source_endpoint: sample_endpoint("ingress", "camera.frames"),
                        target_instance_id: "target-1".to_string(),
                        target_node: "local-node".to_string(),
                        target_daemon_addr: "127.0.0.1:7100".to_string(),
                        target_daemon_server_name: "localhost".to_string(),
                        target_endpoint: sample_endpoint("detector", "camera.frames"),
                        semantics: selium_control_plane_protocol::EndpointBridgeSemantics::Event(
                            selium_control_plane_protocol::EventBridgeSemantics {
                                delivery: selium_control_plane_protocol::EventDeliveryMode::Frame,
                            },
                        ),
                    },
                )
                .await
                .expect("activate route");
                assert_eq!(response.mode, EndpointBridgeMode::Local);

                let source = state
                    .source_bindings
                    .lock()
                    .await
                    .get(&endpoint_key(
                        "source-1",
                        &sample_endpoint("ingress", "camera.frames"),
                    ))
                    .cloned()
                    .expect("source binding present");
                let target = state
                    .target_bindings
                    .lock()
                    .await
                    .get(&endpoint_key(
                        "target-1",
                        &sample_endpoint("detector", "camera.frames"),
                    ))
                    .cloned()
                    .expect("target binding present");

                enqueue_managed_event_frame(&state, &source.queue, b"frame-local")
                    .await
                    .expect("enqueue source frame");

                let reader = QueueService
                    .attach(
                        &target.queue,
                        QueueAttach {
                            shared_id: 0,
                            role: QueueRole::Reader,
                        },
                    )
                    .expect("attach target reader");
                let waited = QueueService
                    .wait(&reader, 2_000)
                    .await
                    .expect("wait target frame");
                let frame = waited.frame.expect("frame available");
                let payload =
                    read_managed_event_frame(&state, frame.shm_shared_id, frame.offset, frame.len)
                        .await
                        .expect("read target frame");
                assert_eq!(payload, b"frame-local");
            })
            .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn deliver_bridge_message_event_writes_target_queue() {
        let state = sample_state("remote-node");
        ensure_managed_endpoint_bindings(
            &state,
            "target-1",
            &[ManagedEndpointBinding {
                endpoint_name: "camera.frames".to_string(),
                endpoint_kind: ContractKind::Event,
                role: ManagedEndpointRole::Ingress,
                binding_type: selium_control_plane_protocol::ManagedEndpointBindingType::OneWay,
            }],
        )
        .await
        .expect("register target binding");

        let delivered = deliver_bridge_message_local(
            &state,
            "target-1",
            &sample_endpoint("detector", "camera.frames"),
            &BridgeMessage::Event(EventBridgeMessage {
                payload: b"frame-remote".to_vec(),
            }),
        )
        .await
        .expect("deliver bridge message");
        assert!(matches!(
            delivered,
            BridgeMessageDelivery {
                delivered: true,
                message: None,
            }
        ));

        let target = state
            .target_bindings
            .lock()
            .await
            .get(&endpoint_key(
                "target-1",
                &sample_endpoint("detector", "camera.frames"),
            ))
            .cloned()
            .expect("target binding present");
        let reader = QueueService
            .attach(
                &target.queue,
                QueueAttach {
                    shared_id: 0,
                    role: QueueRole::Reader,
                },
            )
            .expect("attach target reader");
        let waited = QueueService
            .wait(&reader, 2_000)
            .await
            .expect("wait target frame");
        let frame = waited.frame.expect("frame available");
        let payload =
            read_managed_event_frame(&state, frame.shm_shared_id, frame.offset, frame.len)
                .await
                .expect("read target frame");
        assert_eq!(payload, b"frame-remote");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn activate_endpoint_bridge_forwards_local_guest_log_event_frames() {
        LocalSet::new()
            .run_until(async {
                let state = sample_state("local-node");
                state
                    .processes
                    .lock()
                    .await
                    .insert("source-1".to_string(), 7);
                ensure_guest_log_bindings(&state, "source-1")
                    .await
                    .expect("register guest log bindings");
                ensure_managed_endpoint_bindings(
                    &state,
                    "target-1",
                    &[ManagedEndpointBinding {
                        endpoint_name: GUEST_LOG_STDOUT_ENDPOINT.to_string(),
                        endpoint_kind: ContractKind::Event,
                        role: ManagedEndpointRole::Ingress,
                        binding_type:
                            selium_control_plane_protocol::ManagedEndpointBindingType::OneWay,
                    }],
                )
                .await
                .expect("register target binding");

                let response = activate_endpoint_bridge(
                    &state,
                    &ActivateEndpointBridgeRequest {
                        node_id: "local-node".to_string(),
                        bridge_id: "bridge-guest-log".to_string(),
                        source_instance_id: "source-1".to_string(),
                        source_endpoint: sample_endpoint("ingest", GUEST_LOG_STDOUT_ENDPOINT),
                        target_instance_id: "target-1".to_string(),
                        target_node: "local-node".to_string(),
                        target_daemon_addr: "127.0.0.1:7100".to_string(),
                        target_daemon_server_name: "localhost".to_string(),
                        target_endpoint: sample_endpoint("attach", GUEST_LOG_STDOUT_ENDPOINT),
                        semantics: selium_control_plane_protocol::EndpointBridgeSemantics::Event(
                            selium_control_plane_protocol::EventBridgeSemantics {
                                delivery: selium_control_plane_protocol::EventDeliveryMode::Frame,
                            },
                        ),
                    },
                )
                .await
                .expect("activate guest log route");
                assert_eq!(response.mode, EndpointBridgeMode::Local);

                let source = state
                    .source_bindings
                    .lock()
                    .await
                    .get(&endpoint_key(
                        "source-1",
                        &sample_endpoint("ingest", GUEST_LOG_STDOUT_ENDPOINT),
                    ))
                    .cloned()
                    .expect("guest log source binding present");
                let target = state
                    .target_bindings
                    .lock()
                    .await
                    .get(&endpoint_key(
                        "target-1",
                        &sample_endpoint("attach", GUEST_LOG_STDOUT_ENDPOINT),
                    ))
                    .cloned()
                    .expect("target binding present");

                enqueue_managed_event_frame(&state, &source.queue, b"guest stdout")
                    .await
                    .expect("enqueue guest log frame");

                let reader = QueueService
                    .attach(
                        &target.queue,
                        QueueAttach {
                            shared_id: 0,
                            role: QueueRole::Reader,
                        },
                    )
                    .expect("attach target reader");
                let waited = QueueService
                    .wait(&reader, 2_000)
                    .await
                    .expect("wait target frame");
                let frame = waited.frame.expect("frame available");
                let payload =
                    read_managed_event_frame(&state, frame.shm_shared_id, frame.offset, frame.len)
                        .await
                        .expect("read target frame");
                assert_eq!(payload, b"guest stdout");
            })
            .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn resolve_guest_log_subscription_accepts_exact_stdout_and_stderr() {
        let engine = guest_log_resolution_engine(1);
        let resolved = resolve_guest_log_subscription_with(
            "local-node",
            &SubscribeGuestLogsRequest {
                target: OperationalProcessSelector::ReplicaKey(
                    "tenant=tenant-a;namespace=media;workload=ingest;replica=0".to_string(),
                ),
                stream_names: vec![
                    GUEST_LOG_STDOUT_ENDPOINT.to_string(),
                    GUEST_LOG_STDERR_ENDPOINT.to_string(),
                ],
            },
            |query| async { engine_query(&engine, query) },
        )
        .await
        .expect("resolve guest log subscription");

        assert_eq!(resolved.target.node, "local-node");
        assert_eq!(
            resolved.target.replica_key,
            "tenant=tenant-a;namespace=media;workload=ingest;replica=0"
        );
        assert_eq!(
            resolved
                .streams
                .iter()
                .map(|endpoint| endpoint.name.as_str())
                .collect::<Vec<_>>(),
            vec![GUEST_LOG_STDOUT_ENDPOINT, GUEST_LOG_STDERR_ENDPOINT]
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn resolve_guest_log_subscription_reports_ambiguous_workloads() {
        let engine = guest_log_resolution_engine(2);
        let err = resolve_guest_log_subscription_with(
            "local-node",
            &SubscribeGuestLogsRequest {
                target: OperationalProcessSelector::Workload(WorkloadRef {
                    tenant: "tenant-a".to_string(),
                    namespace: "media".to_string(),
                    name: "ingest".to_string(),
                }),
                stream_names: vec![GUEST_LOG_STDOUT_ENDPOINT.to_string()],
            },
            |query| async { engine_query(&engine, query) },
        )
        .await
        .expect_err("ambiguous workload target");

        assert!(err.to_string().contains("specify a replica key"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn resolve_guest_log_subscription_reports_unknown_streams() {
        let engine = guest_log_resolution_engine(1);
        let err = resolve_guest_log_subscription_with(
            "local-node",
            &SubscribeGuestLogsRequest {
                target: OperationalProcessSelector::ReplicaKey(
                    "tenant=tenant-a;namespace=media;workload=ingest;replica=0".to_string(),
                ),
                stream_names: vec!["stdx".to_string()],
            },
            |query| async { engine_query(&engine, query) },
        )
        .await
        .expect_err("unknown stream target");

        assert!(err.to_string().contains("unknown endpoint"));
        assert!(err.to_string().contains("stdx"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn guest_log_subscription_streams_live_stdout_and_stderr_frames() {
        LocalSet::new()
            .run_until(async {
                let state = sample_state("local-node");
                state
                    .processes
                    .lock()
                    .await
                    .insert("source-1".to_string(), 7);
                ensure_guest_log_bindings(&state, "source-1")
                    .await
                    .expect("register guest log bindings");

                let resolved = ResolvedGuestLogSubscription {
                    target: OperationalProcessRecord {
                        workload: WorkloadRef {
                            tenant: "tenant-a".to_string(),
                            namespace: "media".to_string(),
                            name: "ingest".to_string(),
                        },
                        replica_key: "source-1".to_string(),
                        node: "local-node".to_string(),
                    },
                    source_node: sample_node("local-node", "127.0.0.1:7100", "localhost"),
                    local_node: sample_node("local-node", "127.0.0.1:7100", "localhost"),
                    streams: vec![
                        sample_endpoint("ingest", GUEST_LOG_STDOUT_ENDPOINT),
                        sample_endpoint("ingest", GUEST_LOG_STDERR_ENDPOINT),
                    ],
                };
                let subscription = activate_guest_log_subscription(&state, &resolved)
                    .await
                    .expect("activate guest log subscription");

                let stdout_queue = state
                    .source_bindings
                    .lock()
                    .await
                    .get(&endpoint_key(
                        "source-1",
                        &sample_endpoint("ingest", GUEST_LOG_STDOUT_ENDPOINT),
                    ))
                    .cloned()
                    .expect("stdout queue present");
                let stderr_queue = state
                    .source_bindings
                    .lock()
                    .await
                    .get(&endpoint_key(
                        "source-1",
                        &sample_endpoint("ingest", GUEST_LOG_STDERR_ENDPOINT),
                    ))
                    .cloned()
                    .expect("stderr queue present");

                let (mut client, mut server) = duplex(4096);
                let state_for_task = Rc::clone(&state);
                let instance_id = subscription.instance_id.clone();
                let streams = resolved.streams.clone();
                let stream_task = spawn_local(async move {
                    stream_guest_log_events(&state_for_task, &instance_id, &streams, &mut server)
                        .await
                });

                enqueue_managed_event_frame(&state, &stdout_queue.queue, b"hello stdout")
                    .await
                    .expect("enqueue stdout frame");
                enqueue_managed_event_frame(&state, &stderr_queue.queue, b"hello stderr")
                    .await
                    .expect("enqueue stderr frame");

                let first: GuestLogEvent =
                    decode_rkyv(&read_framed(&mut client).await.expect("read first frame"))
                        .expect("decode first guest log event");
                let second: GuestLogEvent =
                    decode_rkyv(&read_framed(&mut client).await.expect("read second frame"))
                        .expect("decode second guest log event");
                assert_eq!(first.endpoint.name, GUEST_LOG_STDOUT_ENDPOINT);
                assert_eq!(first.payload, b"hello stdout");
                assert_eq!(second.endpoint.name, GUEST_LOG_STDERR_ENDPOINT);
                assert_eq!(second.payload, b"hello stderr");

                drop(client);
                enqueue_managed_event_frame(&state, &stdout_queue.queue, b"goodbye")
                    .await
                    .expect("enqueue disconnect frame");
                let stream_result = stream_task.await.expect("stream task join");
                assert!(stream_result.is_err());

                deactivate_guest_log_subscription(&state, &resolved, &subscription)
                    .await
                    .expect("deactivate guest log subscription");
            })
            .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn activate_endpoint_bridge_forwards_local_service_request_response() {
        LocalSet::new()
            .run_until(async {
                let state = sample_state("local-node");
                state
                    .processes
                    .lock()
                    .await
                    .insert("source-1".to_string(), 7);
                ensure_managed_endpoint_bindings(
                    &state,
                    "source-1",
                    &[ManagedEndpointBinding {
                        endpoint_name: "shared".to_string(),
                        endpoint_kind: ContractKind::Service,
                        role: ManagedEndpointRole::Egress,
                        binding_type: ManagedEndpointBindingType::RequestResponse,
                    }],
                )
                .await
                .expect("register source binding");
                ensure_managed_endpoint_bindings(
                    &state,
                    "target-1",
                    &[ManagedEndpointBinding {
                        endpoint_name: "shared".to_string(),
                        endpoint_kind: ContractKind::Service,
                        role: ManagedEndpointRole::Ingress,
                        binding_type: ManagedEndpointBindingType::RequestResponse,
                    }],
                )
                .await
                .expect("register target binding");

                let response = activate_endpoint_bridge(
                    &state,
                    &ActivateEndpointBridgeRequest {
                        node_id: "local-node".to_string(),
                        bridge_id: "bridge-service".to_string(),
                        source_instance_id: "source-1".to_string(),
                        source_endpoint: sample_endpoint_kind(ContractKind::Service, "ingest", "shared"),
                        target_instance_id: "target-1".to_string(),
                        target_node: "local-node".to_string(),
                        target_daemon_addr: "127.0.0.1:7100".to_string(),
                        target_daemon_server_name: "localhost".to_string(),
                        target_endpoint: sample_endpoint_kind(ContractKind::Service, "detector", "shared"),
                        semantics: EndpointBridgeSemantics::Service(
                            selium_control_plane_protocol::ServiceBridgeSemantics {
                                correlation:
                                    selium_control_plane_protocol::ServiceCorrelationMode::RequestId,
                            },
                        ),
                    },
                )
                .await
                .expect("activate service route");
                assert_eq!(response.mode, EndpointBridgeMode::Local);

                let source_request = state
                    .source_bindings
                    .lock()
                    .await
                    .get(&endpoint_key(
                        "source-1",
                        &sample_endpoint_kind(ContractKind::Service, "ingest", "shared"),
                    ))
                    .cloned()
                    .expect("source request binding present");
                let source_response = state
                    .service_response_bindings
                    .lock()
                    .await
                    .get(&endpoint_key(
                        "source-1",
                        &sample_endpoint_kind(ContractKind::Service, "ingest", "shared"),
                    ))
                    .cloned()
                    .expect("source response binding present");
                let target_request = state
                    .target_bindings
                    .lock()
                    .await
                    .get(&endpoint_key(
                        "target-1",
                        &sample_endpoint_kind(ContractKind::Service, "detector", "shared"),
                    ))
                    .cloned()
                    .expect("target request binding present");
                let target_response = state
                    .service_response_bindings
                    .lock()
                    .await
                    .get(&endpoint_key(
                        "target-1",
                        &sample_endpoint_kind(ContractKind::Service, "detector", "shared"),
                    ))
                    .cloned()
                    .expect("target response binding present");

                let service_state = Rc::clone(&state);
                spawn_local(async move {
                    let reader = QueueService
                        .attach(
                            &target_request.queue,
                            QueueAttach {
                                shared_id: 0,
                                role: QueueRole::Reader,
                            },
                        )
                        .expect("attach target request reader");
                    let waited = QueueService
                        .wait(&reader, 2_000)
                        .await
                        .expect("wait target request");
                    let frame = waited.frame.expect("target request frame");
                    let request = read_managed_service_frame(
                        &service_state,
                        frame.shm_shared_id,
                        frame.offset,
                        frame.len,
                    )
                    .await
                    .expect("decode service request");
                    assert_eq!(request.exchange_id, "req-42");
                    assert_eq!(request.phase, ServiceMessagePhase::Request);
                    assert_eq!(request.payload, b"detect".to_vec());
                    QueueService
                        .ack(
                            &reader,
                            QueueAck {
                                endpoint_id: 0,
                                seq: frame.seq,
                            },
                        )
                        .expect("ack target request");

                    enqueue_managed_service_frame(
                        &service_state,
                        &target_response.queue,
                        &ServiceBridgeMessage {
                            exchange_id: request.exchange_id,
                            phase: ServiceMessagePhase::Response,
                            sequence: request.sequence + 1,
                            complete: true,
                            payload: b"ok".to_vec(),
                        },
                    )
                    .await
                    .expect("enqueue target response");
                });

                enqueue_managed_service_frame(
                    &state,
                    &source_request.queue,
                    &ServiceBridgeMessage {
                        exchange_id: "req-42".to_string(),
                        phase: ServiceMessagePhase::Request,
                        sequence: 0,
                        complete: true,
                        payload: b"detect".to_vec(),
                    },
                )
                .await
                .expect("enqueue source request");

                let reader = QueueService
                    .attach(
                        &source_response.queue,
                        QueueAttach {
                            shared_id: 0,
                            role: QueueRole::Reader,
                        },
                    )
                    .expect("attach source response reader");
                let waited = QueueService
                    .wait(&reader, 2_000)
                    .await
                    .expect("wait source response");
                let frame = waited.frame.expect("source response frame");
                let response = read_managed_service_frame(
                    &state,
                    frame.shm_shared_id,
                    frame.offset,
                    frame.len,
                )
                .await
                .expect("decode source response");
                assert_eq!(response.exchange_id, "req-42");
                assert_eq!(response.phase, ServiceMessagePhase::Response);
                assert_eq!(response.payload, b"ok".to_vec());
            })
            .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn activate_endpoint_bridge_forwards_local_stream_lifecycle_frames() {
        LocalSet::new()
            .run_until(async {
                let state = sample_state("local-node");
                state
                    .processes
                    .lock()
                    .await
                    .insert("source-1".to_string(), 7);
                ensure_managed_endpoint_bindings(
                    &state,
                    "source-1",
                    &[ManagedEndpointBinding {
                        endpoint_name: "shared".to_string(),
                        endpoint_kind: ContractKind::Stream,
                        role: ManagedEndpointRole::Egress,
                        binding_type: ManagedEndpointBindingType::Session,
                    }],
                )
                .await
                .expect("register source binding");
                ensure_managed_endpoint_bindings(
                    &state,
                    "target-1",
                    &[ManagedEndpointBinding {
                        endpoint_name: "shared".to_string(),
                        endpoint_kind: ContractKind::Stream,
                        role: ManagedEndpointRole::Ingress,
                        binding_type: ManagedEndpointBindingType::Session,
                    }],
                )
                .await
                .expect("register target binding");

                let response = activate_endpoint_bridge(
                    &state,
                    &ActivateEndpointBridgeRequest {
                        node_id: "local-node".to_string(),
                        bridge_id: "bridge-stream".to_string(),
                        source_instance_id: "source-1".to_string(),
                        source_endpoint: sample_endpoint_kind(ContractKind::Stream, "ingest", "shared"),
                        target_instance_id: "target-1".to_string(),
                        target_node: "local-node".to_string(),
                        target_daemon_addr: "127.0.0.1:7100".to_string(),
                        target_daemon_server_name: "localhost".to_string(),
                        target_endpoint: sample_endpoint_kind(ContractKind::Stream, "detector", "shared"),
                        semantics: EndpointBridgeSemantics::Stream(
                            selium_control_plane_protocol::StreamBridgeSemantics {
                                lifecycle: selium_control_plane_protocol::StreamLifecycleMode::SessionFrames,
                            },
                        ),
                    },
                )
                .await
                .expect("activate stream route");
                assert_eq!(response.mode, EndpointBridgeMode::Local);

                let source = state
                    .source_bindings
                    .lock()
                    .await
                    .get(&endpoint_key(
                        "source-1",
                        &sample_endpoint_kind(ContractKind::Stream, "ingest", "shared"),
                    ))
                    .cloned()
                    .expect("source binding present");
                let target = state
                    .target_bindings
                    .lock()
                    .await
                    .get(&endpoint_key(
                        "target-1",
                        &sample_endpoint_kind(ContractKind::Stream, "detector", "shared"),
                    ))
                    .cloned()
                    .expect("target binding present");

                for message in [
                    StreamBridgeMessage {
                        session_id: "session-7".to_string(),
                        lifecycle: selium_control_plane_protocol::StreamLifecycle::Open,
                        sequence: 0,
                        payload: b"hello".to_vec(),
                    },
                    StreamBridgeMessage {
                        session_id: "session-7".to_string(),
                        lifecycle: selium_control_plane_protocol::StreamLifecycle::Data,
                        sequence: 1,
                        payload: b"chunk".to_vec(),
                    },
                    StreamBridgeMessage {
                        session_id: "session-7".to_string(),
                        lifecycle: selium_control_plane_protocol::StreamLifecycle::Close,
                        sequence: 2,
                        payload: Vec::new(),
                    },
                ] {
                    enqueue_managed_stream_frame(&state, &source.queue, &message)
                        .await
                        .expect("enqueue source stream frame");
                }

                let reader = QueueService
                    .attach(
                        &target.queue,
                        QueueAttach {
                            shared_id: 0,
                            role: QueueRole::Reader,
                        },
                    )
                    .expect("attach target reader");

                for (expected_lifecycle, expected_sequence, expected_payload) in [
                    (selium_control_plane_protocol::StreamLifecycle::Open, 0, b"hello".as_slice()),
                    (selium_control_plane_protocol::StreamLifecycle::Data, 1, b"chunk".as_slice()),
                    (selium_control_plane_protocol::StreamLifecycle::Close, 2, b"".as_slice()),
                ] {
                    let waited = QueueService
                        .wait(&reader, 2_000)
                        .await
                        .expect("wait target stream frame");
                    let frame = waited.frame.expect("stream frame available");
                    let message = read_managed_stream_frame(
                        &state,
                        frame.shm_shared_id,
                        frame.offset,
                        frame.len,
                    )
                    .await
                    .expect("read target stream frame");
                    assert_eq!(message.session_id, "session-7");
                    assert_eq!(message.lifecycle, expected_lifecycle);
                    assert_eq!(message.sequence, expected_sequence);
                    assert_eq!(message.payload, expected_payload);
                    QueueService
                        .ack(
                            &reader,
                            QueueAck {
                                endpoint_id: 0,
                                seq: frame.seq,
                            },
                        )
                        .expect("ack target stream frame");
                }
            })
            .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn deliver_bridge_message_stream_writes_target_queue() {
        let state = sample_state("remote-node");
        ensure_managed_endpoint_bindings(
            &state,
            "target-1",
            &[ManagedEndpointBinding {
                endpoint_name: "shared".to_string(),
                endpoint_kind: ContractKind::Stream,
                role: ManagedEndpointRole::Ingress,
                binding_type: ManagedEndpointBindingType::Session,
            }],
        )
        .await
        .expect("register target binding");

        let delivered = deliver_bridge_message_local(
            &state,
            "target-1",
            &sample_endpoint_kind(ContractKind::Stream, "detector", "shared"),
            &BridgeMessage::Stream(StreamBridgeMessage {
                session_id: "session-9".to_string(),
                lifecycle: selium_control_plane_protocol::StreamLifecycle::Abort,
                sequence: 3,
                payload: b"cancel".to_vec(),
            }),
        )
        .await
        .expect("deliver bridge stream message");
        assert!(matches!(
            delivered,
            BridgeMessageDelivery {
                delivered: true,
                message: None,
            }
        ));

        let target = state
            .target_bindings
            .lock()
            .await
            .get(&endpoint_key(
                "target-1",
                &sample_endpoint_kind(ContractKind::Stream, "detector", "shared"),
            ))
            .cloned()
            .expect("target binding present");
        let reader = QueueService
            .attach(
                &target.queue,
                QueueAttach {
                    shared_id: 0,
                    role: QueueRole::Reader,
                },
            )
            .expect("attach target reader");
        let waited = QueueService
            .wait(&reader, 2_000)
            .await
            .expect("wait target stream frame");
        let frame = waited.frame.expect("stream frame available");
        let message =
            read_managed_stream_frame(&state, frame.shm_shared_id, frame.offset, frame.len)
                .await
                .expect("read target stream frame");
        assert_eq!(message.session_id, "session-9");
        assert_eq!(
            message.lifecycle,
            selium_control_plane_protocol::StreamLifecycle::Abort
        );
        assert_eq!(message.sequence, 3);
        assert_eq!(message.payload, b"cancel");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn managed_endpoint_binding_payload_partitions_same_name_bindings_by_kind() {
        let state = sample_state("remote-node");
        let bindings = ensure_managed_endpoint_bindings(
            &state,
            "target-1",
            &[
                ManagedEndpointBinding {
                    endpoint_name: "shared".to_string(),
                    endpoint_kind: ContractKind::Event,
                    role: ManagedEndpointRole::Ingress,
                    binding_type: selium_control_plane_protocol::ManagedEndpointBindingType::OneWay,
                },
                ManagedEndpointBinding {
                    endpoint_name: "shared".to_string(),
                    endpoint_kind: ContractKind::Service,
                    role: ManagedEndpointRole::Ingress,
                    binding_type:
                        selium_control_plane_protocol::ManagedEndpointBindingType::RequestResponse,
                },
                ManagedEndpointBinding {
                    endpoint_name: "shared".to_string(),
                    endpoint_kind: ContractKind::Stream,
                    role: ManagedEndpointRole::Ingress,
                    binding_type: ManagedEndpointBindingType::Session,
                },
            ],
        )
        .await
        .expect("register bindings")
        .expect("bindings payload");

        let decoded = decode_rkyv::<DataValue>(&bindings).expect("decode bindings payload");
        let event = decoded
            .get("readers")
            .and_then(|section| section.get("event"))
            .and_then(|section| section.get("shared"))
            .expect("event binding present");
        let service_reader = decoded
            .get("readers")
            .and_then(|section| section.get("service"))
            .and_then(|section| section.get("shared"))
            .expect("service reader binding present");
        let service_writer = decoded
            .get("writers")
            .and_then(|section| section.get("service"))
            .and_then(|section| section.get("shared"))
            .expect("service writer binding present");
        let stream = decoded
            .get("readers")
            .and_then(|section| section.get("stream"))
            .and_then(|section| section.get("shared"))
            .expect("stream binding present");

        assert_ne!(
            event
                .get("queue_shared_id")
                .and_then(DataValue::as_u64)
                .expect("event queue id"),
            service_reader
                .get("queue_shared_id")
                .and_then(DataValue::as_u64)
                .expect("service reader queue id")
        );
        assert_ne!(
            service_reader
                .get("queue_shared_id")
                .and_then(DataValue::as_u64)
                .expect("service reader queue id"),
            service_writer
                .get("queue_shared_id")
                .and_then(DataValue::as_u64)
                .expect("service writer queue id")
        );
        assert_ne!(
            event
                .get("queue_shared_id")
                .and_then(DataValue::as_u64)
                .expect("event queue id"),
            stream
                .get("queue_shared_id")
                .and_then(DataValue::as_u64)
                .expect("stream queue id")
        );
        assert_ne!(
            service_reader
                .get("queue_shared_id")
                .and_then(DataValue::as_u64)
                .expect("service reader queue id"),
            stream
                .get("queue_shared_id")
                .and_then(DataValue::as_u64)
                .expect("stream queue id")
        );
        assert_eq!(
            event
                .get("endpoint_kind")
                .and_then(DataValue::as_str)
                .expect("event kind"),
            "event"
        );
        assert_eq!(
            service_reader
                .get("endpoint_kind")
                .and_then(DataValue::as_str)
                .expect("service kind"),
            "service"
        );
        assert_eq!(
            service_writer
                .get("endpoint_kind")
                .and_then(DataValue::as_str)
                .expect("service writer kind"),
            "service"
        );
        assert_eq!(
            stream
                .get("endpoint_kind")
                .and_then(DataValue::as_str)
                .expect("stream kind"),
            "stream"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn read_managed_event_frame_respects_committed_length() {
        let state = sample_state("remote-node");
        let queue = QueueService
            .create(QueueCreate {
                capacity_frames: 8,
                max_frame_bytes: 512,
                delivery: QueueDelivery::Lossless,
                overflow: QueueOverflow::Block,
            })
            .expect("create queue");
        let writer = QueueService
            .attach(
                &queue,
                QueueAttach {
                    shared_id: 0,
                    role: QueueRole::Writer { writer_id: 1 },
                },
            )
            .expect("attach writer");
        let reader = QueueService
            .attach(
                &queue,
                QueueAttach {
                    shared_id: 0,
                    role: QueueRole::Reader,
                },
            )
            .expect("attach reader");
        let driver = state
            .kernel
            .get::<SharedMemoryDriver>()
            .expect("shared memory driver");
        let region = driver
            .alloc(ShmAlloc {
                size: 512,
                align: 8,
            })
            .expect("allocate shared memory");
        driver
            .write(region, 0, b"frame-remote")
            .expect("write payload");
        driver
            .write(region, 12, &[0; 4])
            .expect("write trailing padding");
        let shm = state
            .registry
            .add(region, None, ResourceType::SharedMemory)
            .expect("register shared memory");
        let shm_shared_id = state
            .registry
            .share_handle(shm.into_id())
            .expect("share shared memory");
        let reserved = QueueService
            .reserve(
                &writer,
                QueueReserve {
                    endpoint_id: 0,
                    len: 12,
                    timeout_ms: 1_000,
                },
            )
            .await
            .expect("reserve queue slot");
        let reservation = reserved.reservation.expect("queue reservation");
        QueueService
            .commit(
                &writer,
                QueueCommit {
                    endpoint_id: 0,
                    reservation_id: reservation.reservation_id,
                    shm_shared_id,
                    offset: 0,
                    len: 12,
                },
            )
            .expect("commit queue slot");

        let waited = QueueService
            .wait(&reader, 2_000)
            .await
            .expect("wait for frame");
        let frame = waited.frame.expect("frame available");
        let payload =
            read_managed_event_frame(&state, frame.shm_shared_id, frame.offset, frame.len)
                .await
                .expect("read committed payload");
        assert_eq!(payload, b"frame-remote");
    }

    #[test]
    fn append_managed_endpoint_bindings_arg_uses_typed_buffer_argument() {
        let spec = append_managed_endpoint_bindings_arg("path=demo.wasm", Some(&[0x41, 0x42]))
            .expect("append bindings arg");
        assert_eq!(spec, "path=demo.wasm;args=buffer:hex:4142;params=buffer");
    }

    #[test]
    fn append_managed_endpoint_bindings_arg_prepends_existing_params_and_args() {
        let spec = append_managed_endpoint_bindings_arg(
            "path=demo.wasm;params=utf8,i32;args=billing,3",
            Some(&[0x41, 0x42]),
        )
        .expect("append bindings arg");
        assert_eq!(
            spec,
            "path=demo.wasm;params=buffer,utf8,i32;args=buffer:hex:4142,billing,3"
        );
    }

    #[test]
    fn append_managed_endpoint_bindings_arg_keeps_typed_arg_inference_when_params_are_omitted() {
        let spec = append_managed_endpoint_bindings_arg(
            "path=demo.wasm;args=utf8:search,i32:5",
            Some(&[0x41, 0x42]),
        )
        .expect("append bindings arg");
        assert_eq!(
            spec,
            "path=demo.wasm;args=buffer:hex:4142,utf8:search,i32:5"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn runtime_usage_query_returns_bounded_records_and_resume_cursor() {
        let state = sample_state("node-a");
        let collector = state
            .kernel
            .get::<RuntimeUsageCollector>()
            .expect("runtime usage collector")
            .clone();
        let first = collector
            .register_process(
                "tenant-a/media/ingest",
                "process-a",
                ProcessUsageAttribution {
                    instance_id: Some("tenant-a/media/ingest/0".to_string()),
                    external_account_ref: Some("acct-a".to_string()),
                    module_id: "module-a".to_string(),
                },
            )
            .await
            .expect("register first process");
        sleep(Duration::from_millis(2)).await;
        first.finish().await.expect("finish first process");

        sleep(Duration::from_millis(2)).await;

        let second = collector
            .register_process(
                "tenant-a/media/ingest",
                "process-b",
                ProcessUsageAttribution {
                    instance_id: Some("tenant-a/media/ingest/1".to_string()),
                    external_account_ref: Some("acct-a".to_string()),
                    module_id: "module-a".to_string(),
                },
            )
            .await
            .expect("register second process");
        sleep(Duration::from_millis(2)).await;
        second.finish().await.expect("finish second process");

        let response = query_runtime_usage(
            &state,
            &RuntimeUsageApiRequest {
                node_id: "node-a".to_string(),
                query: RuntimeUsageQuery {
                    start: RuntimeUsageReplayStart::Earliest,
                    save_checkpoint: None,
                    limit: 1,
                    external_account_ref: Some("acct-a".to_string()),
                    workload: Some("tenant-a/media/ingest".to_string()),
                    module: Some("module-a".to_string()),
                    window_start_ms: None,
                    window_end_ms: None,
                },
            },
        )
        .await
        .expect("query runtime usage");

        assert_eq!(response.records.len(), 1);
        assert_eq!(response.records[0].sample.process_id, "process-a");
        assert_eq!(response.records[0].sample.attribution.module_id, "module-a");
        let high_watermark = response.high_watermark.expect("high watermark");
        assert!(high_watermark >= response.records[0].sequence);
        assert_eq!(
            response.next_sequence,
            Some(response.records[0].sequence.saturating_add(1))
        );

        let resumed = query_runtime_usage(
            &state,
            &RuntimeUsageApiRequest {
                node_id: "node-a".to_string(),
                query: RuntimeUsageQuery {
                    start: RuntimeUsageReplayStart::Sequence(
                        response.next_sequence.expect("resume sequence"),
                    ),
                    save_checkpoint: None,
                    limit: 10,
                    external_account_ref: Some("acct-a".to_string()),
                    workload: Some("tenant-a/media/ingest".to_string()),
                    module: Some("module-a".to_string()),
                    window_start_ms: None,
                    window_end_ms: None,
                },
            },
        )
        .await
        .expect("resume runtime usage query");

        assert_eq!(resumed.records.len(), 1);
        assert_eq!(resumed.records[0].sample.process_id, "process-b");
        assert_eq!(resumed.high_watermark, Some(high_watermark));
        assert_eq!(
            resumed.next_sequence,
            Some(resumed.records[0].sequence.saturating_add(1))
        );
    }

    #[tokio::test]
    async fn runtime_usage_query_zero_limit_returns_watermark_without_advancing_cursor() {
        let state = sample_state("node-a");
        let collector = state
            .kernel
            .get::<crate::usage::RuntimeUsageCollector>()
            .cloned()
            .expect("collector available");
        let handle = collector
            .register_process(
                "tenant-a/media/ingest",
                "process-a",
                ProcessUsageAttribution {
                    instance_id: Some("tenant-a/media/ingest/0".to_string()),
                    external_account_ref: Some("acct-a".to_string()),
                    module_id: "module-a".to_string(),
                },
            )
            .await
            .expect("register process");
        sleep(Duration::from_millis(2)).await;
        handle.finish().await.expect("finish process");

        let response = query_runtime_usage(
            &state,
            &RuntimeUsageApiRequest {
                node_id: "node-a".to_string(),
                query: RuntimeUsageQuery {
                    start: RuntimeUsageReplayStart::Sequence(1),
                    save_checkpoint: None,
                    limit: 0,
                    external_account_ref: Some("acct-a".to_string()),
                    workload: Some("tenant-a/media/ingest".to_string()),
                    module: Some("module-a".to_string()),
                    window_start_ms: None,
                    window_end_ms: None,
                },
            },
        )
        .await
        .expect("query runtime usage");

        assert!(response.records.is_empty());
        assert!(response.high_watermark.is_some());
        assert_eq!(response.next_sequence, None);
    }

    #[tokio::test]
    async fn runtime_usage_query_can_save_and_resume_named_checkpoint() {
        let state = sample_state("node-a");
        let collector = state
            .kernel
            .get::<crate::usage::RuntimeUsageCollector>()
            .cloned()
            .expect("collector available");
        let first = collector
            .register_process(
                "tenant-a/media/ingest",
                "process-a",
                ProcessUsageAttribution {
                    instance_id: Some("tenant-a/media/ingest/0".to_string()),
                    external_account_ref: Some("acct-a".to_string()),
                    module_id: "module-a".to_string(),
                },
            )
            .await
            .expect("register first process");
        sleep(Duration::from_millis(2)).await;
        first.finish().await.expect("finish first process");

        let second = collector
            .register_process(
                "tenant-a/media/ingest",
                "process-b",
                ProcessUsageAttribution {
                    instance_id: Some("tenant-a/media/ingest/1".to_string()),
                    external_account_ref: Some("acct-a".to_string()),
                    module_id: "module-a".to_string(),
                },
            )
            .await
            .expect("register second process");
        sleep(Duration::from_millis(2)).await;
        second.finish().await.expect("finish second process");

        let response = query_runtime_usage(
            &state,
            &RuntimeUsageApiRequest {
                node_id: "node-a".to_string(),
                query: RuntimeUsageQuery {
                    start: RuntimeUsageReplayStart::Earliest,
                    save_checkpoint: Some("usage-export".to_string()),
                    limit: 1,
                    external_account_ref: Some("acct-a".to_string()),
                    workload: Some("tenant-a/media/ingest".to_string()),
                    module: Some("module-a".to_string()),
                    window_start_ms: None,
                    window_end_ms: None,
                },
            },
        )
        .await
        .expect("query runtime usage");

        assert_eq!(response.records.len(), 1);
        assert_eq!(response.records[0].sample.process_id, "process-a");
        assert_eq!(
            collector.checkpoint_sequence("usage-export").await,
            response.next_sequence
        );

        let resumed = query_runtime_usage(
            &state,
            &RuntimeUsageApiRequest {
                node_id: "node-a".to_string(),
                query: RuntimeUsageQuery {
                    start: RuntimeUsageReplayStart::Checkpoint("usage-export".to_string()),
                    save_checkpoint: Some("usage-export".to_string()),
                    limit: 10,
                    external_account_ref: Some("acct-a".to_string()),
                    workload: Some("tenant-a/media/ingest".to_string()),
                    module: Some("module-a".to_string()),
                    window_start_ms: None,
                    window_end_ms: None,
                },
            },
        )
        .await
        .expect("resume named checkpoint");

        assert_eq!(resumed.records.len(), 1);
        assert_eq!(resumed.records[0].sample.process_id, "process-b");
        assert_eq!(
            collector.checkpoint_sequence("usage-export").await,
            resumed.next_sequence
        );
    }

    #[tokio::test]
    async fn runtime_usage_query_missing_checkpoint_fails_deterministically() {
        let state = sample_state("node-a");
        let err = query_runtime_usage(
            &state,
            &RuntimeUsageApiRequest {
                node_id: "node-a".to_string(),
                query: RuntimeUsageQuery {
                    start: RuntimeUsageReplayStart::Checkpoint("missing".to_string()),
                    save_checkpoint: None,
                    limit: 10,
                    external_account_ref: None,
                    workload: None,
                    module: None,
                    window_start_ms: None,
                    window_end_ms: None,
                },
            },
        )
        .await
        .expect_err("missing checkpoint should fail");

        assert!(err.chain().any(|cause| {
            cause
                .to_string()
                .contains("checkpoint `missing` does not exist")
        }));
    }

    #[test]
    fn append_guest_log_bindings_adds_hidden_queue_ids() {
        let spec = append_guest_log_bindings(
            "path=demo.wasm;capabilities=time_read",
            ProcessLogBindings {
                stdout_queue_shared_id: Some(11),
                stderr_queue_shared_id: Some(22),
            },
        );

        assert_eq!(
            spec,
            "path=demo.wasm;capabilities=time_read;guest_log_stdout_queue_shared_id=11;guest_log_stderr_queue_shared_id=22"
        );
    }

    fn sample_node(name: &str, daemon_addr: &str, daemon_server_name: &str) -> NodeSpec {
        NodeSpec {
            name: name.to_string(),
            capacity_slots: 8,
            allocatable_cpu_millis: None,
            allocatable_memory_mib: None,
            supported_isolation: vec![IsolationProfile::Standard],
            daemon_addr: daemon_addr.to_string(),
            daemon_server_name: daemon_server_name.to_string(),
            last_heartbeat_ms: 0,
        }
    }

    fn guest_log_resolution_engine(replicas: usize) -> ControlPlaneEngine {
        let mut state = ControlPlaneState::new_local_default();
        let package = parse_idl(
            r#"
            package media.camera.v1;
            schema Frame { payload: bytes; }
            event camera.frames(Frame) { replay: enabled; }
            "#,
        )
        .expect("parse package");
        state
            .registry
            .register_package(package)
            .expect("register package");
        state
            .upsert_deployment(DeploymentSpec {
                workload: WorkloadRef {
                    tenant: "tenant-a".to_string(),
                    namespace: "media".to_string(),
                    name: "ingest".to_string(),
                },
                module: "ingest.wasm".to_string(),
                replicas: replicas as u32,
                contracts: vec![ContractRef {
                    namespace: "media.camera".to_string(),
                    kind: ContractKind::Event,
                    name: "camera.frames".to_string(),
                    version: "v1".to_string(),
                }],
                isolation: IsolationProfile::Standard,
                cpu_millis: 0,
                memory_mib: 0,
                ephemeral_storage_mib: 0,
                bandwidth_profile: selium_control_plane_api::BandwidthProfile::Standard,
                volume_mounts: Vec::new(),
                external_account_ref: None,
            })
            .expect("register deployment");
        ControlPlaneEngine::new(state)
    }

    fn engine_query(engine: &ControlPlaneEngine, query: Query) -> Result<DataValue> {
        Ok(engine
            .query(query)
            .map_err(|err| anyhow!(err.to_string()))?
            .result)
    }

    fn sample_state(node_id: &str) -> Rc<DaemonState> {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let work_dir = std::env::temp_dir().join(format!("selium-daemon-test-{unique}"));
        fs::create_dir_all(work_dir.join("modules")).expect("create temp modules dir");
        let (kernel, _) = crate::kernel::build(&work_dir).expect("build kernel");
        let registry = Registry::new();
        let endpoint = Endpoint::client("127.0.0.1:0".parse().expect("client bind"))
            .expect("create client endpoint");
        Rc::new(DaemonState {
            node_id: node_id.to_string(),
            kernel,
            registry,
            work_dir,
            processes: Mutex::new(BTreeMap::new()),
            source_bindings: Mutex::new(BTreeMap::new()),
            target_bindings: Mutex::new(BTreeMap::new()),
            service_response_bindings: Mutex::new(BTreeMap::new()),
            active_bridges: Mutex::new(BTreeMap::new()),
            host_subscription_id: AtomicU64::new(1),
            control_plane: Arc::new(LocalControlPlaneClient {
                endpoint,
                addr: "127.0.0.1:1".parse().expect("dummy addr"),
                server_name: "dummy.local".to_string(),
                connection: Mutex::new(None),
                request_id: AtomicU64::new(1),
            }),
            control_plane_process_id: 0,
            tls_paths: ManagedEventTlsPaths {
                ca_cert: PathBuf::new(),
                client_cert: PathBuf::new(),
                client_key: PathBuf::new(),
            },
        })
    }

    fn sample_endpoint(workload: &str, endpoint: &str) -> PublicEndpointRef {
        sample_endpoint_kind(ContractKind::Event, workload, endpoint)
    }

    fn sample_endpoint_kind(
        kind: ContractKind,
        workload: &str,
        endpoint: &str,
    ) -> PublicEndpointRef {
        PublicEndpointRef {
            workload: WorkloadRef {
                tenant: "tenant-a".to_string(),
                namespace: "media".to_string(),
                name: workload.to_string(),
            },
            kind,
            name: endpoint.to_string(),
        }
    }
}
