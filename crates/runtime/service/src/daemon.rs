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
use selium_abi::{Capability, InteractionKind, NetworkProtocol, encode_rkyv};
use selium_control_plane_protocol::{
    Empty, ListRequest, ListResponse, Method, StartRequest, StartResponse, StatusApiResponse,
    StopRequest, StopResponse, decode_envelope, decode_error, decode_payload,
    encode_error_response, encode_request, encode_response, is_error, is_request, read_framed,
    write_framed,
};
use selium_io_durability::RetentionPolicy;
use selium_kernel::{Kernel, registry::Registry, services::session_service::Session};
use selium_module_control_plane::{
    ControlPlaneModuleConfig, ENTRYPOINT, EVENT_LOG_NAME, INTERNAL_BINDING_NAME, MODULE_ID,
    PEER_PROFILE_NAME, PeerTarget, SNAPSHOT_BLOB_STORE_NAME,
};
use selium_runtime_network::{NetworkEgressProfile, NetworkIngressBinding, NetworkService};
use selium_runtime_storage::{StorageBlobStoreDefinition, StorageLogDefinition, StorageService};
use tokio::{
    signal,
    sync::{Mutex, Notify},
    task::LocalSet,
    time::{Duration, sleep, timeout},
};
use tracing::info;

use crate::{config::DaemonArgs, modules};

const QUIC_CONNECT_TIMEOUT: Duration = Duration::from_millis(500);
const QUIC_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

struct DaemonState {
    node_id: String,
    kernel: Kernel,
    registry: Arc<Registry>,
    work_dir: PathBuf,
    processes: Mutex<BTreeMap<String, usize>>,
    control_plane: Arc<LocalControlPlaneClient>,
    control_plane_process_id: usize,
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
        control_plane,
        control_plane_process_id,
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

            let specs = vec![payload.module_spec.clone()];
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
            encode_response(
                envelope.method,
                envelope.request_id,
                &ListResponse { instances: entries },
            )
        }
    }
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
