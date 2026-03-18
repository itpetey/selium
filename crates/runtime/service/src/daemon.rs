mod guest_logs;

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    future::Future,
    io::BufReader,
    net::SocketAddr,
    path::{Path, PathBuf},
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use anyhow::{Context, Result, anyhow, bail};
use quinn::{Connection, Endpoint, Incoming};
use selium_abi::{
    Capability, DataValue, InteractionKind, NetworkProtocol, ProcessLogBindings, QueueAck,
    QueueAttach, QueueCommit, QueueCreate, QueueDelivery, QueueOverflow, QueueReserve, QueueRole,
    QueueStatusCode, ShmAlloc, decode_rkyv, encode_rkyv,
};
use selium_control_plane_agent::{
    ControlPlaneModuleConfig, ENTRYPOINT, EVENT_LOG_NAME, INTERNAL_BINDING_NAME, MODULE_ID,
    PEER_PROFILE_NAME, PeerTarget, SNAPSHOT_BLOB_STORE_NAME,
};
use selium_control_plane_api::{
    ContractKind, ControlPlaneState, DiscoveryCapabilityScope, DiscoveryOperation,
    DiscoveryRequest, DiscoveryResolution, DiscoveryTarget, NodeSpec, OperationalProcessRecord,
    PublicEndpointRef,
};
use selium_control_plane_core::{Mutation, Query};
use selium_control_plane_protocol::{
    ActivateEndpointBridgeRequest, ActivateEndpointBridgeResponse, BridgeMessage,
    DeactivateEndpointBridgeRequest, DeactivateEndpointBridgeResponse, DeliverBridgeMessageRequest,
    DeliverBridgeMessageResponse, Empty, EndpointBridgeMode, EndpointBridgeSemantics, Envelope,
    EventBridgeMessage, GuestLogEvent, ListRequest, ListResponse, ManagedEndpointBinding,
    ManagedEndpointBindingType, ManagedEndpointRole, Method, MutateApiRequest, QueryApiRequest,
    QueryApiResponse, ReplayApiRequest, RuntimeUsageApiRequest, RuntimeUsageApiResponse,
    ServiceBridgeMessage, ServiceMessagePhase, StartRequest, StartResponse, StatusApiResponse,
    StopRequest, StopResponse, StreamBridgeMessage, SubscribeGuestLogsRequest,
    SubscribeGuestLogsResponse, decode_envelope, decode_error, decode_payload,
    encode_error_response, encode_request, encode_response, is_error, is_request,
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
use selium_runtime_network::{NetworkEgressProfile, NetworkIngressBinding, NetworkService};
use selium_runtime_storage::{StorageBlobStoreDefinition, StorageLogDefinition, StorageService};
use selium_runtime_support::{
    ClientTlsPaths, ServerTlsPaths, build_quic_client_endpoint, build_quic_server_endpoint,
    parse_socket_addr, read_framed, write_framed,
};
use tokio::{
    io::AsyncWrite,
    signal,
    sync::{Mutex, Notify},
    task::{JoinHandle, LocalSet, spawn_local},
    time::{Duration, sleep, timeout},
};
use tracing::{info, warn};

#[cfg(test)]
use self::guest_logs::{
    activate_guest_log_subscription, classify_guest_log_resolution_error,
    deactivate_guest_log_subscription, resolve_guest_log_subscription_with,
    stream_guest_log_events,
};
use self::guest_logs::{handle_guest_log_subscription_stream, query_runtime_usage};
use crate::{
    auth::{
        AccessPolicy, AuthenticatedRequestContext, peer_fingerprint, principal_from_cert,
        principal_from_connection,
    },
    config::DaemonArgs,
    modules,
    usage::RuntimeUsageCollector,
};

const QUIC_CONNECT_TIMEOUT: Duration = Duration::from_millis(500);
const QUIC_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const CONTROL_PLANE_PROXY_TIMEOUT: Duration = Duration::from_secs(30);
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
    instance_workloads: Mutex<BTreeMap<String, String>>,
    source_bindings: Mutex<BTreeMap<(String, ContractKind, String), ManagedEventEndpointQueue>>,
    target_bindings: Mutex<BTreeMap<(String, ContractKind, String), ManagedEventEndpointQueue>>,
    service_response_bindings:
        Mutex<BTreeMap<(String, ContractKind, String), ManagedEventEndpointQueue>>,
    active_bridges: Mutex<BTreeMap<String, ActiveEndpointBridge>>,
    host_subscription_id: AtomicU64,
    control_plane: Arc<LocalControlPlaneClient>,
    control_plane_process_id: usize,
    tls_paths: ManagedEventTlsPaths,
    access_policy: AccessPolicy,
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

    fn source_endpoint(&self) -> &PublicEndpointRef {
        match self {
            Self::Event(spec) => &spec.source_endpoint,
            Self::Service(spec) => &spec.source_endpoint,
            Self::Stream(spec) => &spec.source_endpoint,
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

    fn target_endpoint(&self) -> &PublicEndpointRef {
        match self {
            Self::Event(spec) => &spec.target_endpoint,
            Self::Service(spec) => &spec.target_endpoint,
            Self::Stream(spec) => &spec.target_endpoint,
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
    client_cert: Option<PathBuf>,
    client_key: Option<PathBuf>,
}

impl ManagedEventTlsPaths {
    fn client_identity(&self) -> Option<(&Path, &Path)> {
        self.client_cert.as_deref().zip(self.client_key.as_deref())
    }
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
    internal_client_cert_path: Option<&'a Path>,
    internal_client_key_path: Option<&'a Path>,
    peer_cert_path: Option<&'a Path>,
    peer_key_path: Option<&'a Path>,
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
        client_cert_path: Option<&Path>,
        client_key_path: Option<&Path>,
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
        for attempt in 0..=1 {
            match self.try_request_raw(frame).await {
                Ok(response) => return Ok(response),
                Err(err) if attempt == 0 => {
                    self.reset_connection().await;
                    tracing::warn!(
                        "retrying guest control-plane proxy request after error: {err:#}"
                    );
                }
                Err(err) => return Err(err),
            }
        }

        unreachable!("proxy request retry loop should return or error")
    }

    async fn try_request_raw(&self, frame: &[u8]) -> Result<Vec<u8>> {
        let connection = self.connection().await?;
        let (mut send, mut recv) = timeout(CONTROL_PLANE_PROXY_TIMEOUT, connection.open_bi())
            .await
            .map_err(|_| anyhow!("timed out"))
            .context("open proxy stream")??;
        timeout(CONTROL_PLANE_PROXY_TIMEOUT, write_framed(&mut send, frame))
            .await
            .map_err(|_| anyhow!("timed out"))
            .context("write proxy request")??;
        let _ = send.finish();
        timeout(CONTROL_PLANE_PROXY_TIMEOUT, read_framed(&mut recv))
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
    let peer_client_identity = resolve_peer_client_tls_paths(
        &work_dir,
        args.quic_peer_cert.as_deref(),
        args.quic_peer_key.as_deref(),
    )?;
    if !args.cp_peers.is_empty() && peer_client_identity.is_none() {
        bail!(
            "outbound peer TLS requires --quic-peer-cert/--quic-peer-key or certs/peer.crt and certs/peer.key"
        );
    }
    let (internal_client_cert_path, internal_client_key_path) = resolve_internal_client_tls_paths(
        &work_dir,
        args.quic_peer_cert.as_deref(),
        args.quic_peer_key.as_deref(),
    )?;
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
        internal_client_cert_path: internal_client_cert_path.as_deref(),
        internal_client_key_path: internal_client_key_path.as_deref(),
        peer_cert_path: peer_client_identity
            .as_ref()
            .map(|(cert, _)| cert.as_path()),
        peer_key_path: peer_client_identity.as_ref().map(|(_, key)| key.as_path()),
    };
    let addresses = ControlPlaneAddresses {
        public_addr: &public_addr,
        internal_addr: &internal_addr,
    };
    let (control_plane_process_id, control_plane) =
        bootstrap_control_plane(&kernel, &registry, &work_dir, &args, &tls_paths, &addresses)
            .await?;
    let access_policy = build_access_policy(&work_dir, &args)?;

    let state = Rc::new(DaemonState {
        node_id: args.cp_node_id.clone(),
        kernel,
        registry,
        work_dir,
        processes: Mutex::new(BTreeMap::new()),
        instance_workloads: Mutex::new(BTreeMap::new()),
        source_bindings: Mutex::new(BTreeMap::new()),
        target_bindings: Mutex::new(BTreeMap::new()),
        service_response_bindings: Mutex::new(BTreeMap::new()),
        active_bridges: Mutex::new(BTreeMap::new()),
        host_subscription_id: AtomicU64::new(1),
        control_plane,
        control_plane_process_id,
        tls_paths: ManagedEventTlsPaths {
            ca_cert: ca_path.clone(),
            client_cert: peer_client_identity.as_ref().map(|(cert, _)| cert.clone()),
            client_key: peer_client_identity.as_ref().map(|(_, key)| key.clone()),
        },
        access_policy,
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
        reserve_cpu_utilisation_ppm: args.cp_reserve_cpu_utilisation_ppm,
        reserve_memory_utilisation_ppm: args.cp_reserve_memory_utilisation_ppm,
        reserve_slots_utilisation_ppm: args.cp_reserve_slots_utilisation_ppm,
        heartbeat_interval_ms: args.cp_heartbeat_interval_ms,
        bootstrap_leader: args.cp_bootstrap_leader,
        peers,
    };

    let process_id = spawn_control_plane_module(kernel, registry, work_dir, &module_config).await?;
    let client = Arc::new(LocalControlPlaneClient::new(
        parse_socket_addr(addresses.internal_addr)?,
        args.cp_server_name.clone(),
        tls_paths.ca_path,
        tls_paths.internal_client_cert_path,
        tls_paths.internal_client_key_path,
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
            client_cert_path: tls_paths.peer_cert_path.map(Path::to_path_buf),
            client_key_path: tls_paths.peer_key_path.map(Path::to_path_buf),
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
    let principal = principal_from_connection(&connection).context("resolve peer principal")?;
    let request_context = state
        .access_policy
        .resolve(principal, peer_fingerprint(&connection));
    info!(
        principal = %request_context.principal(),
        fingerprint = %request_context.peer_fingerprint,
        "accepted daemon connection"
    );

    loop {
        match connection.accept_bi().await {
            Ok((mut send, mut recv)) => {
                let state = Rc::clone(&state);
                let request_context = request_context.clone();
                spawn_local(async move {
                    if let Err(err) =
                        handle_stream(state, request_context, &mut send, &mut recv).await
                    {
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
    request_context: AuthenticatedRequestContext,
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
        if !request_context.allows(envelope.method) {
            return write_unauthorised_response(send, &request_context, envelope).await;
        }
        return handle_guest_log_subscription_stream(state, request_context, send, envelope).await;
    }

    let response = handle_stream_request(state, request_context, &bytes, envelope).await?;
    write_framed(send, &response)
        .await
        .context("write response frame")?;
    let _ = send.finish();
    Ok(())
}

async fn handle_stream_request(
    state: Rc<DaemonState>,
    request_context: AuthenticatedRequestContext,
    bytes: &[u8],
    envelope: Envelope,
) -> Result<Vec<u8>> {
    if !request_context.allows(envelope.method) {
        warn!(
            principal = %request_context.principal(),
            fingerprint = %request_context.peer_fingerprint,
            method = ?envelope.method,
            "daemon request denied"
        );
        return encode_error_response(
            envelope.method,
            envelope.request_id,
            403,
            "unauthorised",
            false,
        );
    }

    match envelope.method {
        Method::ControlMutate
        | Method::ControlQuery
        | Method::ControlStatus
        | Method::ControlMetrics
        | Method::ControlReplay
        | Method::RaftRequestVote
        | Method::RaftAppendEntries => {
            match forward_control_plane_request(&state, &request_context, bytes, &envelope).await {
                Ok(response) => Ok(response),
                Err(err) => {
                    encode_forward_control_plane_error(envelope.method, envelope.request_id, err)
                }
            }
        }
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
            if let Err(err) = ensure_workload_authorised(
                &request_context,
                Method::StartInstance,
                &payload.workload_key,
                "instance start",
            ) {
                return encode_error_response(
                    envelope.method,
                    envelope.request_id,
                    403,
                    err.to_string(),
                    false,
                );
            }

            let existing = {
                state
                    .processes
                    .lock()
                    .await
                    .get(&payload.instance_id)
                    .copied()
            };
            if let Some(existing) = existing {
                let Some(existing_workload) = state
                    .instance_workloads
                    .lock()
                    .await
                    .get(&payload.instance_id)
                    .cloned()
                else {
                    return encode_error_response(
                        envelope.method,
                        envelope.request_id,
                        500,
                        format!(
                            "instance `{}` missing workload attribution",
                            payload.instance_id
                        ),
                        false,
                    );
                };
                if let Err(err) = ensure_workload_authorised(
                    &request_context,
                    Method::StartInstance,
                    &existing_workload,
                    "instance start",
                ) {
                    return encode_error_response(
                        envelope.method,
                        envelope.request_id,
                        403,
                        err.to_string(),
                        false,
                    );
                }
                return encode_response(
                    envelope.method,
                    envelope.request_id,
                    &StartResponse {
                        status: "ok".to_string(),
                        instance_id: payload.instance_id,
                        process_id: existing,
                        already_running: true,
                    },
                );
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
                    principal: Some(request_context.principal()),
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
            state
                .instance_workloads
                .lock()
                .await
                .insert(payload.instance_id.clone(), payload.workload_key.clone());

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

            let process_id = {
                state
                    .processes
                    .lock()
                    .await
                    .get(&payload.instance_id)
                    .copied()
            };
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
            let Some(workload_key) = state
                .instance_workloads
                .lock()
                .await
                .get(&payload.instance_id)
                .cloned()
            else {
                return encode_error_response(
                    envelope.method,
                    envelope.request_id,
                    500,
                    format!(
                        "instance `{}` missing workload attribution",
                        payload.instance_id
                    ),
                    false,
                );
            };
            if let Err(err) = ensure_workload_authorised(
                &request_context,
                Method::StopInstance,
                &workload_key,
                "instance stop",
            ) {
                return encode_error_response(
                    envelope.method,
                    envelope.request_id,
                    403,
                    err.to_string(),
                    false,
                );
            }
            state.processes.lock().await.remove(&payload.instance_id);
            state
                .instance_workloads
                .lock()
                .await
                .remove(&payload.instance_id);

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
            let workloads = state.instance_workloads.lock().await;
            let entries = processes
                .iter()
                .filter(|(instance, _)| {
                    instance_visible_for_list_instances(&request_context, &workloads, instance)
                })
                .map(|(instance, process)| (instance.clone(), *process))
                .collect::<BTreeMap<_, _>>();
            let mut observed_workloads = BTreeMap::new();
            for instance in entries.keys() {
                let Some(workload) = workloads.get(instance) else {
                    continue;
                };
                *observed_workloads.entry(workload.clone()).or_insert(0) += 1;
            }
            let visible_instances = entries.keys().cloned().collect::<BTreeSet<_>>();
            let local_instances = processes.keys().cloned().collect::<BTreeSet<_>>();
            let active_bridges = state
                .active_bridges
                .lock()
                .await
                .iter()
                .filter(|(_, route)| {
                    bridge_visible_for_list_instances(
                        &request_context,
                        &visible_instances,
                        &local_instances,
                        &route.spec,
                    )
                })
                .map(|(bridge_id, _)| bridge_id.clone())
                .collect();
            let (observed_memory_bytes, observed_workload_memory_bytes) =
                match state.kernel.get::<RuntimeUsageCollector>() {
                    Some(collector) => {
                        let visible_process_ids = entries
                            .values()
                            .map(|process| process.to_string())
                            .collect::<Vec<_>>();
                        let observed_memory_bytes = Some(
                            collector
                                .observed_load_for_processes(visible_process_ids.iter())
                                .await
                                .memory_bytes,
                        );
                        let process_memory = collector
                            .observed_memory_bytes_by_processes(visible_process_ids.iter())
                            .await;
                        let mut observed_workload_memory_bytes = BTreeMap::new();
                        for (instance, process_id) in &entries {
                            let Some(workload) = workloads.get(instance) else {
                                continue;
                            };
                            let Some(memory_bytes) = process_memory.get(&process_id.to_string())
                            else {
                                continue;
                            };
                            *observed_workload_memory_bytes
                                .entry(workload.clone())
                                .or_insert(0) += *memory_bytes;
                        }
                        (observed_memory_bytes, observed_workload_memory_bytes)
                    }
                    None => (None, BTreeMap::new()),
                };
            encode_response(
                envelope.method,
                envelope.request_id,
                &ListResponse {
                    instances: entries,
                    active_bridges,
                    observed_memory_bytes,
                    observed_workloads,
                    observed_workload_memory_bytes,
                },
            )
        }
        Method::ActivateEndpointBridge => {
            let payload: ActivateEndpointBridgeRequest = match decode_payload(&envelope) {
                Ok(payload) => payload,
                Err(err) => return rpc_decode_error(envelope.method, envelope.request_id, err),
            };
            if let Err(err) = ensure_instance_endpoint_authorised(
                &state,
                &request_context,
                Method::ActivateEndpointBridge,
                &payload.source_instance_id,
                &payload.source_endpoint,
                "bridge source endpoint",
            )
            .await
            {
                return encode_error_response(
                    envelope.method,
                    envelope.request_id,
                    403,
                    err.to_string(),
                    false,
                );
            }
            if let Err(err) = ensure_instance_endpoint_authorised(
                &state,
                &request_context,
                Method::ActivateEndpointBridge,
                &payload.target_instance_id,
                &payload.target_endpoint,
                "bridge target endpoint",
            )
            .await
            {
                return encode_error_response(
                    envelope.method,
                    envelope.request_id,
                    403,
                    err.to_string(),
                    false,
                );
            }
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
            let bridge_spec = state
                .active_bridges
                .lock()
                .await
                .get(&payload.bridge_id)
                .map(|bridge| bridge.spec.clone());
            if let Some(spec) = bridge_spec {
                if let Err(err) = ensure_endpoint_authorised(
                    &request_context,
                    Method::DeactivateEndpointBridge,
                    spec.source_endpoint(),
                    "bridge source endpoint",
                ) {
                    return encode_error_response(
                        envelope.method,
                        envelope.request_id,
                        403,
                        err.to_string(),
                        false,
                    );
                }
                if let Err(err) = ensure_endpoint_authorised(
                    &request_context,
                    Method::DeactivateEndpointBridge,
                    spec.target_endpoint(),
                    "bridge target endpoint",
                ) {
                    return encode_error_response(
                        envelope.method,
                        envelope.request_id,
                        403,
                        err.to_string(),
                        false,
                    );
                }
            }
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
            if let Err(err) = ensure_instance_endpoint_authorised(
                &state,
                &request_context,
                Method::DeliverBridgeMessage,
                &payload.target_instance_id,
                &payload.target_endpoint,
                "bridge target endpoint",
            )
            .await
            {
                return encode_error_response(
                    envelope.method,
                    envelope.request_id,
                    403,
                    err.to_string(),
                    false,
                );
            }
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
            if let Err(err) = ensure_workload_filter_authorised(
                &request_context,
                Method::RuntimeUsageQuery,
                payload.query.workload.as_deref(),
                "runtime usage query",
            ) {
                return encode_error_response(
                    envelope.method,
                    envelope.request_id,
                    403,
                    err.to_string(),
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

fn build_access_policy(work_dir: &Path, args: &DaemonArgs) -> Result<AccessPolicy> {
    let mut specs = args.access_grants.clone();
    if specs.is_empty() {
        specs.push(default_access_grant_spec(
            &resolve_default_client_principal(work_dir)?,
        ));
    }
    specs.push(default_peer_access_grant_spec());
    AccessPolicy::from_specs(&specs)
}

fn default_access_grant_spec(principal: &selium_abi::PrincipalRef) -> String {
    format!(
        "principal={};methods=control_read,control_read_global,control_write,node_manage,bridge_manage;workloads=*;endpoints=*;allow-operational-processes=true",
        principal
    )
}

fn default_peer_access_grant_spec() -> String {
    format!(
        "principal={}:*;methods=peer;workloads=*;endpoints=*",
        selium_abi::PrincipalKind::RuntimePeer
    )
}

async fn forward_control_plane_request(
    state: &DaemonState,
    request_context: &AuthenticatedRequestContext,
    bytes: &[u8],
    envelope: &Envelope,
) -> std::result::Result<Vec<u8>, ForwardControlPlaneError> {
    match envelope.method {
        Method::ControlMutate => {
            let mut request: MutateApiRequest =
                decode_payload(envelope).map_err(ForwardControlPlaneError::bad_request)?;
            request.mutation =
                rewrite_mutation_for_authorisation(request.mutation, request_context)
                    .map_err(ForwardControlPlaneError::forbidden)?;
            let frame = encode_request(envelope.method, envelope.request_id, &request)
                .context("encode control mutate request")
                .map_err(ForwardControlPlaneError::internal)?;
            state
                .control_plane
                .request_raw(&frame)
                .await
                .map_err(ForwardControlPlaneError::upstream)
        }
        Method::ControlQuery => {
            let mut request: QueryApiRequest =
                decode_payload(envelope).map_err(ForwardControlPlaneError::bad_request)?;
            request.query =
                rewrite_query_for_authorisation(request.query, request_context, envelope.method)
                    .map_err(ForwardControlPlaneError::forbidden)?;
            let frame = encode_request(envelope.method, envelope.request_id, &request)
                .context("encode control query request")
                .map_err(ForwardControlPlaneError::internal)?;
            state
                .control_plane
                .request_raw(&frame)
                .await
                .map_err(ForwardControlPlaneError::upstream)
        }
        Method::ControlReplay => {
            let request: ReplayApiRequest =
                decode_payload(envelope).map_err(ForwardControlPlaneError::bad_request)?;
            ensure_workload_filter_authorised(
                request_context,
                envelope.method,
                request.workload.as_deref(),
                "control replay",
            )
            .map_err(ForwardControlPlaneError::forbidden)?;
            state
                .control_plane
                .request_raw(bytes)
                .await
                .map_err(ForwardControlPlaneError::upstream)
        }
        _ => state
            .control_plane
            .request_raw(bytes)
            .await
            .map_err(ForwardControlPlaneError::upstream),
    }
}

#[derive(Debug)]
enum ForwardControlPlaneError {
    Local {
        status: u16,
        message: String,
        retryable: bool,
    },
    Upstream(anyhow::Error),
}

impl ForwardControlPlaneError {
    fn bad_request(error: anyhow::Error) -> Self {
        Self::Local {
            status: 400,
            message: error.to_string(),
            retryable: false,
        }
    }

    fn forbidden(error: anyhow::Error) -> Self {
        Self::Local {
            status: 403,
            message: error.to_string(),
            retryable: false,
        }
    }

    fn internal(error: anyhow::Error) -> Self {
        Self::Local {
            status: 500,
            message: error.to_string(),
            retryable: false,
        }
    }

    fn upstream(error: anyhow::Error) -> Self {
        Self::Upstream(error)
    }
}

fn encode_forward_control_plane_error(
    method: Method,
    request_id: u64,
    error: ForwardControlPlaneError,
) -> Result<Vec<u8>> {
    match error {
        ForwardControlPlaneError::Local {
            status,
            message,
            retryable,
        } => encode_error_response(method, request_id, status, message, retryable),
        ForwardControlPlaneError::Upstream(error) => {
            encode_error_response(method, request_id, 502, error.to_string(), true)
        }
    }
}

fn rewrite_mutation_for_authorisation(
    mutation: Mutation,
    request_context: &AuthenticatedRequestContext,
) -> Result<Mutation> {
    match mutation {
        Mutation::UpsertDeployment { spec } => {
            let workload = spec.workload.key();
            ensure_workload_authorised(
                request_context,
                Method::ControlMutate,
                &workload,
                "deployment mutation",
            )?;
            Ok(Mutation::UpsertDeployment { spec })
        }
        Mutation::SetScale { workload, replicas } => {
            let workload_key = workload.key();
            ensure_workload_authorised(
                request_context,
                Method::ControlMutate,
                &workload_key,
                "scale mutation",
            )?;
            Ok(Mutation::SetScale { workload, replicas })
        }
        other if request_context.allows_all_workloads_for(Method::ControlMutate) => Ok(other),
        _ => bail!("mutation requires full workload access"),
    }
}

fn rewrite_query_for_authorisation(
    query: Query,
    request_context: &AuthenticatedRequestContext,
    method: Method,
) -> Result<Query> {
    match query {
        Query::DiscoveryState { .. } => Ok(Query::DiscoveryState {
            scope: request_context.discovery_scope_for(method),
        }),
        Query::ResolveDiscovery { mut request } => {
            request.scope = request_context.discovery_scope_for(method);
            Ok(Query::ResolveDiscovery { request })
        }
        Query::AttributedInfrastructureInventory { filter } => {
            ensure_workload_filter_authorised(
                request_context,
                method,
                filter.workload.as_deref(),
                "infrastructure inventory query",
            )?;
            Ok(Query::AttributedInfrastructureInventory { filter })
        }
        other
            if request_context.allows_all_workloads_for(method)
                && request_context.allows_all_endpoints_for(method) =>
        {
            Ok(other)
        }
        _ => bail!("query requires full workload and endpoint access"),
    }
}

fn ensure_workload_filter_authorised(
    request_context: &AuthenticatedRequestContext,
    method: Method,
    workload: Option<&str>,
    subject: &str,
) -> Result<()> {
    match workload {
        Some(workload) => ensure_workload_authorised(request_context, method, workload, subject),
        None if request_context.allows_all_workloads_for(method) => Ok(()),
        None => bail!("{subject} requires an explicit authorised workload filter"),
    }
}

fn ensure_workload_authorised(
    request_context: &AuthenticatedRequestContext,
    method: Method,
    workload: &str,
    subject: &str,
) -> Result<()> {
    if request_context.allows_workload_for(method, workload) {
        Ok(())
    } else {
        bail!("unauthorised workload `{workload}` for {subject}")
    }
}

fn ensure_endpoint_authorised(
    request_context: &AuthenticatedRequestContext,
    method: Method,
    endpoint: &PublicEndpointRef,
    subject: &str,
) -> Result<()> {
    if request_context.allows_endpoint_for(method, endpoint) {
        Ok(())
    } else {
        bail!("unauthorised endpoint `{}` for {subject}", endpoint.key())
    }
}

fn instance_visible_for_list_instances(
    request_context: &AuthenticatedRequestContext,
    workloads: &BTreeMap<String, String>,
    instance_id: &str,
) -> bool {
    workloads.get(instance_id).is_some_and(|workload| {
        request_context.allows_workload_for(Method::ListInstances, workload)
    }) || (request_context.allows_all_workloads_for(Method::ListInstances)
        && !workloads.contains_key(instance_id))
}

fn bridge_visible_for_list_instances(
    request_context: &AuthenticatedRequestContext,
    visible_instances: &BTreeSet<String>,
    local_instances: &BTreeSet<String>,
    spec: &ActiveEndpointBridgeSpec,
) -> bool {
    visible_instances.contains(spec.source_instance_id())
        && request_context.allows_workload_for(
            Method::ListInstances,
            &spec.source_endpoint().workload.key(),
        )
        && request_context.allows_workload_for(
            Method::ListInstances,
            &spec.target_endpoint().workload.key(),
        )
        && (!local_instances.contains(spec.target_instance_id())
            || visible_instances.contains(spec.target_instance_id()))
}

async fn ensure_instance_endpoint_authorised(
    state: &DaemonState,
    request_context: &AuthenticatedRequestContext,
    method: Method,
    instance_id: &str,
    endpoint: &PublicEndpointRef,
    subject: &str,
) -> Result<()> {
    ensure_endpoint_authorised(request_context, method, endpoint, subject)?;

    let workload = state
        .instance_workloads
        .lock()
        .await
        .get(instance_id)
        .cloned();
    if let Some(workload) = workload {
        ensure_workload_authorised(request_context, method, &workload, subject)?;
        if endpoint.workload.key() != workload {
            bail!(
                "endpoint `{}` does not match instance `{instance_id}` workload `{workload}`",
                endpoint.key()
            );
        }
    }

    Ok(())
}

async fn write_unauthorised_response(
    send: &mut quinn::SendStream,
    request_context: &AuthenticatedRequestContext,
    envelope: Envelope,
) -> Result<()> {
    warn!(
        principal = %request_context.principal(),
        fingerprint = %request_context.peer_fingerprint,
        method = ?envelope.method,
        "daemon request denied"
    );
    let response = encode_error_response(
        envelope.method,
        envelope.request_id,
        403,
        "unauthorised",
        false,
    )?;
    write_framed(send, &response)
        .await
        .context("write unauthorised response")?;
    let _ = send.finish();
    Ok(())
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
    let Some((client_cert, client_key)) = tls_paths.client_identity() else {
        bail!(
            "remote daemon access requires a peer certificate with a Selium principal URI; configure --quic-peer-cert/--quic-peer-key or provision certs/peer.crt and certs/peer.key"
        );
    };
    let endpoint = build_client_endpoint(&tls_paths.ca_cert, Some(client_cert), Some(client_key))?;
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
    let bind_addr =
        parse_socket_addr(listen).with_context(|| format!("parse listen addr {listen}"))?;
    build_quic_server_endpoint(
        bind_addr,
        ServerTlsPaths {
            cert: cert_path,
            key: key_path,
        },
        Some(ca_path),
    )
}

fn build_client_endpoint(
    ca_path: &Path,
    client_cert_path: Option<&Path>,
    client_key_path: Option<&Path>,
) -> Result<Endpoint> {
    let bind = if cfg!(target_family = "unix") {
        "0.0.0.0:0"
    } else {
        "127.0.0.1:0"
    }
    .parse::<SocketAddr>()?;
    build_quic_client_endpoint(
        bind,
        ClientTlsPaths {
            ca_cert: ca_path,
            client_cert: client_cert_path,
            client_key: client_key_path,
        },
    )
}

fn make_abs(work_dir: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        work_dir.join(path)
    }
}

fn resolve_internal_client_tls_paths(
    work_dir: &Path,
    peer_cert_override: Option<&Path>,
    peer_key_override: Option<&Path>,
) -> Result<(Option<PathBuf>, Option<PathBuf>)> {
    let client_identity =
        resolve_peer_client_tls_paths(work_dir, peer_cert_override, peer_key_override)?;
    Ok(match client_identity {
        Some((cert, key)) => (Some(cert), Some(key)),
        None => (None, None),
    })
}

fn resolve_peer_client_tls_paths(
    work_dir: &Path,
    peer_cert_override: Option<&Path>,
    peer_key_override: Option<&Path>,
) -> Result<Option<(PathBuf, PathBuf)>> {
    match (peer_cert_override, peer_key_override) {
        (Some(cert), Some(key)) => Ok(Some((make_abs(work_dir, cert), make_abs(work_dir, key)))),
        (Some(_), None) | (None, Some(_)) => {
            bail!("--quic-peer-cert and --quic-peer-key must be provided together")
        }
        (None, None) => {
            let default_peer_cert_path = default_peer_client_cert_path(work_dir);
            let default_peer_key_path = default_peer_client_key_path(work_dir);
            if default_peer_cert_path.exists() && default_peer_key_path.exists() {
                Ok(Some((default_peer_cert_path, default_peer_key_path)))
            } else {
                Ok(None)
            }
        }
    }
}

fn default_peer_client_cert_path(work_dir: &Path) -> PathBuf {
    work_dir.join("certs/peer.crt")
}

fn default_peer_client_key_path(work_dir: &Path) -> PathBuf {
    work_dir.join("certs/peer.key")
}

fn default_client_cert_path(work_dir: &Path) -> PathBuf {
    work_dir.join("certs/client.crt")
}

fn resolve_default_client_principal(work_dir: &Path) -> Result<selium_abi::PrincipalRef> {
    resolve_cert_principal(
        &default_client_cert_path(work_dir),
        selium_abi::PrincipalRef::new(selium_abi::PrincipalKind::Machine, "client.localhost"),
    )
}

fn resolve_cert_principal(
    cert_path: &Path,
    fallback: selium_abi::PrincipalRef,
) -> Result<selium_abi::PrincipalRef> {
    if !cert_path.exists() {
        return Ok(fallback);
    }
    let cert_der = load_first_pem_certificate(cert_path)?;
    principal_from_cert(cert_der.as_ref()).with_context(|| {
        format!(
            "resolve Selium principal from certificate {}; generated/client certificates must embed a Selium principal URI SAN, or the daemon must be configured with explicit `--access-grant` entries",
            cert_path.display()
        )
    })
}

fn load_first_pem_certificate(
    cert_path: &Path,
) -> Result<rustls::pki_types::CertificateDer<'static>> {
    let file = fs::File::open(cert_path)
        .with_context(|| format!("open certificate {}", cert_path.display()))?;
    let mut reader = BufReader::new(file);
    let mut certs = rustls_pemfile::certs(&mut reader);
    certs
        .next()
        .transpose()
        .with_context(|| format!("read certificate {}", cert_path.display()))?
        .ok_or_else(|| {
            anyhow!(
                "certificate {} did not contain a PEM block",
                cert_path.display()
            )
        })
}

#[cfg(test)]
mod tests;
