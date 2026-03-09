use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
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
    Capability, DataValue, InteractionKind, NetworkProtocol, QueueAck, QueueAttach, QueueCommit,
    QueueCreate, QueueDelivery, QueueOverflow, QueueReserve, QueueRole, QueueStatusCode, ShmAlloc,
    decode_rkyv, encode_rkyv,
};
use selium_control_plane_api::{ControlPlaneState, EventEndpointRef};
use selium_control_plane_protocol::{
    ActivateEventRouteRequest, ActivateEventRouteResponse, DeactivateEventRouteRequest,
    DeactivateEventRouteResponse, DeliverEventFrameRequest, DeliverEventFrameResponse, Empty,
    EventRouteMode, ListRequest, ListResponse, ManagedEventBinding, ManagedEventBindingRole,
    Method, QueryApiRequest, QueryApiResponse, StartRequest, StartResponse, StatusApiResponse,
    StopRequest, StopResponse, decode_envelope, decode_error, decode_payload,
    encode_error_response, encode_request, encode_response, is_error, is_request, read_framed,
    write_framed,
};
use selium_control_plane_runtime::Query;
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
    ControlPlaneModuleConfig, ENTRYPOINT, EVENT_LOG_NAME, INTERNAL_BINDING_NAME, MODULE_ID,
    PEER_PROFILE_NAME, PeerTarget, SNAPSHOT_BLOB_STORE_NAME,
};
use selium_runtime_network::{NetworkEgressProfile, NetworkIngressBinding, NetworkService};
use selium_runtime_storage::{StorageBlobStoreDefinition, StorageLogDefinition, StorageService};
use tokio::{
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

struct DaemonState {
    node_id: String,
    kernel: Kernel,
    registry: Arc<Registry>,
    work_dir: PathBuf,
    processes: Mutex<BTreeMap<String, usize>>,
    source_bindings: Mutex<BTreeMap<(String, String), ManagedEventEndpointQueue>>,
    target_bindings: Mutex<BTreeMap<(String, String), ManagedEventEndpointQueue>>,
    active_routes: Mutex<BTreeMap<String, ActiveEventRoute>>,
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
struct ActiveEventRoute {
    spec: ActiveEventRouteSpec,
    bridge_task: JoinHandle<()>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ActiveEventRouteSpec {
    source_instance_id: String,
    source_endpoint: EventEndpointRef,
    target_instance_id: String,
    target_endpoint: EventEndpointRef,
    mode: EventRouteMode,
    target_node: String,
}

#[derive(Debug, Clone)]
struct ManagedEventTlsPaths {
    ca_cert: PathBuf,
    client_cert: PathBuf,
    client_key: PathBuf,
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

    async fn query(&self, query: Query, allow_stale: bool) -> Result<QueryApiResponse> {
        let request_id = self.request_id.fetch_add(1, Ordering::Relaxed);
        let frame = encode_request(
            Method::ControlQuery,
            request_id,
            &QueryApiRequest { query, allow_stale },
        )
        .context("encode control query")?;
        let response = self.request_raw(&frame).await?;
        let envelope = decode_envelope(&response).context("decode query response")?;
        if envelope.method != Method::ControlQuery || envelope.request_id != request_id {
            bail!("control-plane query returned mismatched envelope");
        }
        if is_error(&envelope) {
            let err = decode_error(&envelope).context("decode query error")?;
            bail!(
                "control-plane query failed with {}: {}",
                err.code,
                err.message
            );
        }
        decode_payload(&envelope).context("decode query payload")
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
        active_routes: Mutex::new(BTreeMap::new()),
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
                let response = match handle_stream_request(Rc::clone(&state), &mut recv).await {
                    Ok(bytes) => bytes,
                    Err(err) => {
                        tracing::warn!("stream request error: {err:#}");
                        continue;
                    }
                };

                if let Err(err) = write_framed(&mut send, &response).await {
                    tracing::warn!("stream response write error: {err:#}");
                }
                let _ = send.finish();
            }
            Err(quinn::ConnectionError::ApplicationClosed { .. }) => break,
            Err(quinn::ConnectionError::ConnectionClosed(_)) => break,
            Err(err) => return Err(anyhow!(err).context("accept bidirectional stream")),
        }
    }

    Ok(())
}

async fn handle_stream_request(
    state: Rc<DaemonState>,
    recv: &mut quinn::RecvStream,
) -> Result<Vec<u8>> {
    let bytes = read_framed(recv).await.context("read request frame")?;
    let envelope = decode_envelope(&bytes).context("decode request envelope")?;

    if !is_request(&envelope) {
        return encode_error_response(
            envelope.method,
            envelope.request_id,
            400,
            "invalid frame flags",
            false,
        );
    }

    match envelope.method {
        Method::ControlMutate
        | Method::ControlQuery
        | Method::ControlStatus
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

            let managed_event_bindings = match ensure_managed_event_bindings(
                &state,
                &payload.instance_id,
                &payload.managed_event_bindings,
            )
            .await
            {
                Ok(bindings) => bindings,
                Err(err) => return rpc_server_error(envelope.method, envelope.request_id, err),
            };
            let module_spec = match append_managed_event_bindings_arg(
                &payload.module_spec,
                managed_event_bindings.as_deref(),
            ) {
                Ok(spec) => spec,
                Err(err) => return rpc_server_error(envelope.method, envelope.request_id, err),
            };
            let specs = vec![module_spec];
            let spawned = match modules::spawn_from_cli(
                &state.kernel,
                &state.registry,
                &state.work_dir,
                &specs,
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

            state.active_routes.lock().await.retain(|_, route| {
                let remove = route.spec.source_instance_id == payload.instance_id
                    || route.spec.target_instance_id == payload.instance_id;
                if remove {
                    route.bridge_task.abort();
                }
                !remove
            });
            state
                .source_bindings
                .lock()
                .await
                .retain(|(instance_id, _), _| instance_id != &payload.instance_id);
            state
                .target_bindings
                .lock()
                .await
                .retain(|(instance_id, _), _| instance_id != &payload.instance_id);

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
            let active_routes = state.active_routes.lock().await.keys().cloned().collect();
            encode_response(
                envelope.method,
                envelope.request_id,
                &ListResponse {
                    instances: entries,
                    active_routes,
                },
            )
        }
        Method::ActivateEventRoute => {
            let payload: ActivateEventRouteRequest = match decode_payload(&envelope) {
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
            match activate_event_route(&state, &payload).await {
                Ok(response) => encode_response(envelope.method, envelope.request_id, &response),
                Err(err) => rpc_server_error(envelope.method, envelope.request_id, err),
            }
        }
        Method::DeactivateEventRoute => {
            let payload: DeactivateEventRouteRequest = match decode_payload(&envelope) {
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
            match deactivate_event_route(&state, &payload).await {
                Ok(response) => encode_response(envelope.method, envelope.request_id, &response),
                Err(err) => rpc_server_error(envelope.method, envelope.request_id, err),
            }
        }
        Method::DeliverEventFrame => {
            let payload: DeliverEventFrameRequest = match decode_payload(&envelope) {
                Ok(payload) => payload,
                Err(err) => return rpc_decode_error(envelope.method, envelope.request_id, err),
            };
            match deliver_event_frame(
                &state,
                &payload.target_instance_id,
                &payload.target_endpoint_name,
                &payload.payload,
            )
            .await
            {
                Ok(delivered) => encode_response(
                    envelope.method,
                    envelope.request_id,
                    &DeliverEventFrameResponse {
                        status: if delivered {
                            "ok".to_string()
                        } else {
                            "not_found".to_string()
                        },
                        delivered,
                    },
                ),
                Err(err) => rpc_server_error(envelope.method, envelope.request_id, err),
            }
        }
    }
}

async fn activate_event_route(
    state: &Rc<DaemonState>,
    payload: &ActivateEventRouteRequest,
) -> Result<ActivateEventRouteResponse> {
    let source_key = endpoint_key(&payload.source_instance_id, &payload.source_endpoint.name);
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

    let spec = ActiveEventRouteSpec {
        source_instance_id: payload.source_instance_id.clone(),
        source_endpoint: payload.source_endpoint.clone(),
        target_instance_id: payload.target_instance_id.clone(),
        target_endpoint: payload.target_endpoint.clone(),
        mode: if payload.target_node == state.node_id {
            EventRouteMode::Local
        } else {
            EventRouteMode::Remote
        },
        target_node: payload.target_node.clone(),
    };

    let mut routes = state.active_routes.lock().await;
    if let Some(existing) = routes.get(&payload.route_id) {
        if existing.spec == spec {
            return Ok(ActivateEventRouteResponse {
                status: "ok".to_string(),
                route_id: payload.route_id.clone(),
                mode: spec.mode,
                target_node: spec.target_node.clone(),
                target_instance_id: spec.target_instance_id.clone(),
                already_active: true,
            });
        }
    }
    if let Some(existing) = routes.remove(&payload.route_id) {
        existing.bridge_task.abort();
    }

    let state_for_task = Rc::clone(state);
    let route_id = payload.route_id.clone();
    let spec_for_task = spec.clone();
    let bridge_task = spawn_local(async move {
        if let Err(err) = forward_event_route(state_for_task, spec_for_task).await {
            info!(route_id, error = %err, "managed event route bridge stopped");
        }
    });
    routes.insert(
        payload.route_id.clone(),
        ActiveEventRoute {
            spec: spec.clone(),
            bridge_task,
        },
    );

    Ok(ActivateEventRouteResponse {
        status: "ok".to_string(),
        route_id: payload.route_id.clone(),
        mode: spec.mode,
        target_node: spec.target_node,
        target_instance_id: spec.target_instance_id,
        already_active: false,
    })
}

async fn deactivate_event_route(
    state: &DaemonState,
    payload: &DeactivateEventRouteRequest,
) -> Result<DeactivateEventRouteResponse> {
    let existed = state.active_routes.lock().await.remove(&payload.route_id);
    if let Some(route) = existed.as_ref() {
        route.bridge_task.abort();
    }
    Ok(DeactivateEventRouteResponse {
        status: if existed.is_some() {
            "ok".to_string()
        } else {
            "not_found".to_string()
        },
        route_id: payload.route_id.clone(),
        existed: existed.is_some(),
    })
}

async fn ensure_managed_event_bindings(
    state: &DaemonState,
    instance_id: &str,
    bindings: &[ManagedEventBinding],
) -> Result<Option<Vec<u8>>> {
    if bindings.is_empty() {
        return Ok(None);
    }

    let mut writers = BTreeMap::new();
    let mut readers = BTreeMap::new();
    for binding in bindings {
        let queue = ensure_managed_endpoint_queue(state, instance_id, binding).await?;
        let descriptor = DataValue::Map(BTreeMap::from([
            (
                "queue_shared_id".to_string(),
                DataValue::U64(queue.queue_shared_id),
            ),
            (
                "max_frame_bytes".to_string(),
                DataValue::U64(MANAGED_EVENT_MAX_FRAME_BYTES as u64),
            ),
        ]));
        match binding.role {
            ManagedEventBindingRole::Source => {
                writers.insert(binding.endpoint_name.clone(), descriptor);
            }
            ManagedEventBindingRole::Target => {
                readers.insert(binding.endpoint_name.clone(), descriptor);
            }
        }
    }

    Ok(Some(
        encode_rkyv(&DataValue::Map(BTreeMap::from([
            ("writers".to_string(), DataValue::Map(writers)),
            ("readers".to_string(), DataValue::Map(readers)),
        ])))
        .context("encode managed event bindings")?,
    ))
}

async fn ensure_managed_endpoint_queue(
    state: &DaemonState,
    instance_id: &str,
    binding: &ManagedEventBinding,
) -> Result<ManagedEventEndpointQueue> {
    let key = endpoint_key(instance_id, &binding.endpoint_name);
    let bindings = match binding.role {
        ManagedEventBindingRole::Source => &state.source_bindings,
        ManagedEventBindingRole::Target => &state.target_bindings,
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

fn append_managed_event_bindings_arg(module_spec: &str, bindings: Option<&[u8]>) -> Result<String> {
    match bindings {
        Some(bytes) => Ok(format!(
            "{module_spec};params=buffer;args=buffer:hex:{}",
            encode_hex(bytes)
        )),
        None => Ok(module_spec.to_string()),
    }
}

async fn forward_event_route(state: Rc<DaemonState>, spec: ActiveEventRouteSpec) -> Result<()> {
    let source_queue = {
        let bindings = state.source_bindings.lock().await;
        bindings
            .get(&endpoint_key(
                &spec.source_instance_id,
                &spec.source_endpoint.name,
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
                let payload = read_managed_event_frame(&state, frame.shm_shared_id).await?;
                let delivered = match spec.mode {
                    EventRouteMode::Local => {
                        deliver_event_frame(
                            &state,
                            &spec.target_instance_id,
                            &spec.target_endpoint.name,
                            &payload,
                        )
                        .await?
                    }
                    EventRouteMode::Remote => {
                        deliver_event_frame_remote(
                            &state,
                            &spec.target_node,
                            &spec.target_instance_id,
                            &spec.target_endpoint.name,
                            &payload,
                        )
                        .await?
                    }
                };
                if delivered {
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

async fn read_managed_event_frame(state: &DaemonState, shm_shared_id: u64) -> Result<Vec<u8>> {
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
    Ok(driver.read(region, 0, region.len)?)
}

async fn deliver_event_frame(
    state: &DaemonState,
    target_instance_id: &str,
    target_endpoint_name: &str,
    payload: &[u8],
) -> Result<bool> {
    let binding = {
        let bindings = state.target_bindings.lock().await;
        bindings
            .get(&endpoint_key(target_instance_id, target_endpoint_name))
            .cloned()
    };
    let Some(binding) = binding else {
        return Ok(false);
    };
    enqueue_managed_event_frame(state, &binding.queue, payload).await?;
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

async fn deliver_event_frame_remote(
    state: &DaemonState,
    target_node: &str,
    target_instance_id: &str,
    target_endpoint_name: &str,
    payload: &[u8],
) -> Result<bool> {
    let control_plane = query_control_plane_state(&state.control_plane).await?;
    let node = control_plane
        .nodes
        .get(target_node)
        .ok_or_else(|| anyhow!("missing daemon details for node `{target_node}`"))?;
    let endpoint = build_client_endpoint(
        &state.tls_paths.ca_cert,
        &state.tls_paths.client_cert,
        &state.tls_paths.client_key,
    )?;
    let connection = timeout(
        QUIC_CONNECT_TIMEOUT,
        endpoint.connect(
            node.daemon_addr.parse::<SocketAddr>()?,
            &node.daemon_server_name,
        )?,
    )
    .await
    .map_err(|_| anyhow!("timed out"))
    .context("connect remote daemon")??;
    let request_id = 1;
    let frame = encode_request(
        Method::DeliverEventFrame,
        request_id,
        &DeliverEventFrameRequest {
            target_instance_id: target_instance_id.to_string(),
            target_endpoint_name: target_endpoint_name.to_string(),
            payload: payload.to_vec(),
        },
    )
    .context("encode remote frame delivery")?;
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
    if is_error(&envelope) {
        let err = decode_error(&envelope).context("decode remote route error")?;
        bail!("remote daemon returned {}: {}", err.code, err.message);
    }
    let delivered: DeliverEventFrameResponse =
        decode_payload(&envelope).context("decode remote route delivery")?;
    Ok(delivered.delivered)
}

fn endpoint_key(instance_id: &str, endpoint_name: &str) -> (String, String) {
    (instance_id.to_string(), endpoint_name.to_string())
}

async fn query_control_plane_state(
    control_plane: &LocalControlPlaneClient,
) -> Result<ControlPlaneState> {
    let response = control_plane.query(Query::ControlPlaneState, false).await?;
    let Some(DataValue::Bytes(bytes)) = response.result else {
        bail!("control-plane state query did not return bytes")
    };
    decode_rkyv(&bytes).context("decode control-plane state")
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

    use selium_control_plane_api::WorkloadRef;

    #[tokio::test(flavor = "current_thread")]
    async fn activate_event_route_forwards_local_frames() {
        LocalSet::new()
            .run_until(async {
                let state = sample_state("local-node");
                state
                    .processes
                    .lock()
                    .await
                    .insert("source-1".to_string(), 7);
                ensure_managed_event_bindings(
                    &state,
                    "source-1",
                    &[ManagedEventBinding {
                        endpoint_name: "camera.frames".to_string(),
                        role: ManagedEventBindingRole::Source,
                    }],
                )
                .await
                .expect("register source binding");
                ensure_managed_event_bindings(
                    &state,
                    "target-1",
                    &[ManagedEventBinding {
                        endpoint_name: "camera.frames".to_string(),
                        role: ManagedEventBindingRole::Target,
                    }],
                )
                .await
                .expect("register target binding");

                let response = activate_event_route(
                    &state,
                    &ActivateEventRouteRequest {
                        node_id: "local-node".to_string(),
                        route_id: "route-1".to_string(),
                        source_instance_id: "source-1".to_string(),
                        source_endpoint: sample_endpoint("ingress", "camera.frames"),
                        target_instance_id: "target-1".to_string(),
                        target_node: "local-node".to_string(),
                        target_endpoint: sample_endpoint("detector", "camera.frames"),
                    },
                )
                .await
                .expect("activate route");
                assert_eq!(response.mode, EventRouteMode::Local);

                let source = state
                    .source_bindings
                    .lock()
                    .await
                    .get(&endpoint_key("source-1", "camera.frames"))
                    .cloned()
                    .expect("source binding present");
                let target = state
                    .target_bindings
                    .lock()
                    .await
                    .get(&endpoint_key("target-1", "camera.frames"))
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
                let payload = read_managed_event_frame(&state, frame.shm_shared_id)
                    .await
                    .expect("read target frame");
                assert_eq!(payload, b"frame-local");
            })
            .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn deliver_event_frame_rpc_writes_target_queue() {
        let state = sample_state("remote-node");
        ensure_managed_event_bindings(
            &state,
            "target-1",
            &[ManagedEventBinding {
                endpoint_name: "camera.frames".to_string(),
                role: ManagedEventBindingRole::Target,
            }],
        )
        .await
        .expect("register target binding");

        assert!(
            deliver_event_frame(&state, "target-1", "camera.frames", b"frame-remote")
                .await
                .expect("deliver frame")
        );

        let target = state
            .target_bindings
            .lock()
            .await
            .get(&endpoint_key("target-1", "camera.frames"))
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
        let payload = read_managed_event_frame(&state, frame.shm_shared_id)
            .await
            .expect("read target frame");
        assert_eq!(payload, b"frame-remote");
    }

    #[test]
    fn append_managed_event_bindings_arg_uses_typed_buffer_argument() {
        let spec = append_managed_event_bindings_arg("path=demo.wasm", Some(&[0x41, 0x42]))
            .expect("append bindings arg");
        assert_eq!(spec, "path=demo.wasm;params=buffer;args=buffer:hex:4142");
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
            active_routes: Mutex::new(BTreeMap::new()),
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

    fn sample_endpoint(workload: &str, endpoint: &str) -> EventEndpointRef {
        EventEndpointRef {
            workload: WorkloadRef {
                tenant: "tenant-a".to_string(),
                namespace: "media".to_string(),
                name: workload.to_string(),
            },
            name: endpoint.to_string(),
        }
    }
}
