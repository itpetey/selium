use std::{
    collections::BTreeMap,
    env,
    ffi::OsString,
    fs,
    net::{SocketAddr, ToSocketAddrs},
    path::{Path, PathBuf},
    rc::Rc,
    sync::Arc,
};

use anyhow::{Context, Result, anyhow};
use clap::{
    Args, CommandFactory, FromArgMatches, Parser, Subcommand, ValueEnum, parser::ValueSource,
};
use quinn::crypto::rustls::QuicServerConfig;
use quinn::{Endpoint, Incoming};
use rustls::{RootCertStore, pki_types::PrivateKeyDer};
use rustls_pemfile::{certs, private_key};
use selium_abi::Capability;
use selium_control_plane_protocol::{
    AppendEntriesApiRequest, ListResponse, Method, MutateApiRequest, QueryApiRequest,
    ReplayApiRequest, ReplayApiResponse, RequestVoteApiRequest, StartRequest, StartResponse,
    StopRequest, StopResponse, decode_envelope, decode_payload, encode_error_response,
    encode_response, is_request, read_framed, write_framed,
};
use selium_kernel::{Kernel, registry::Registry, services::session_service::Session};
use selium_module_control_plane::{
    api::{IsolationProfile, NodeSpec},
    runtime::Mutation,
};
use serde::Deserialize;
use tokio::{
    signal,
    sync::{Mutex, Notify},
    time::{Duration, sleep},
};
use tracing::info;
use tracing_subscriber::{EnvFilter, fmt::time::SystemTime};

mod certs;
mod control_plane;
mod kernel;
mod modules;
mod providers;
mod wasmtime;

#[derive(Copy, Clone, Debug, Deserialize, ValueEnum, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum LogFormat {
    /// Human-friendly text logs suitable for local development.
    Text,
    /// JSON logs for ingestion into systems such as Loki or OTLP collectors.
    Json,
}

#[derive(Parser, Debug)]
#[command(version, about = "Selium host runtime")]
struct ServerOptions {
    /// Optional TOML config file that provides defaults for omitted flags.
    #[arg(short = 'c', long, global = true, value_name = "FILE")]
    config: Option<PathBuf>,
    /// Log output format (text or JSON) for tracing events.
    #[arg(long, env = "SELIUM_LOG_FORMAT", default_value = "text")]
    log_format: LogFormat,
    #[command(subcommand)]
    command: Option<ServerCommand>,
    /// Base directory where certificates and WASM modules are stored.
    #[arg(short, long, env = "SELIUM_WORK_DIR", default_value_os = ".")]
    work_dir: PathBuf,
    /// Module specification to start (repeatable). Format:
    /// `path=...;capabilities=...;adapter=wasmtime;profile=standard;args=...`
    #[arg(long, value_name = "SPEC")]
    module: Option<Vec<String>>,
}

#[derive(Subcommand, Debug)]
enum ServerCommand {
    /// Generate a local CA plus server and client certificate pairs.
    GenerateCerts(GenerateCertsArgs),
    /// Run long-lived runtime daemon with lifecycle API.
    Daemon(DaemonArgs),
}

#[derive(Args, Debug)]
struct GenerateCertsArgs {
    /// Directory to write certificate and key files to.
    #[arg(long, default_value = "certs")]
    output_dir: PathBuf,
    /// Common Name to embed in the generated CA.
    #[arg(long, default_value = "Selium Local CA")]
    ca_common_name: String,
    /// DNS name to embed in the server certificate.
    #[arg(long, default_value = "localhost")]
    server_name: String,
    /// DNS name to embed in the client certificate.
    #[arg(long, default_value = "client.localhost")]
    client_name: String,
}

#[derive(Args, Debug)]
struct DaemonArgs {
    /// QUIC listener address for daemon lifecycle and control-plane API.
    #[arg(long, default_value = "127.0.0.1:7100")]
    listen: String,
    /// Logical node identifier for control-plane consensus.
    #[arg(long, default_value = "local-node")]
    cp_node_id: String,
    /// Peer node endpoint in node_id=host:port[@server_name] format (repeatable).
    #[arg(long = "cp-peer")]
    cp_peers: Vec<String>,
    /// Bootstrap this node as leader when no persisted term exists.
    #[arg(long)]
    cp_bootstrap_leader: bool,
    /// Directory used for control-plane raft state, snapshots, and durable events.
    #[arg(long, default_value = ".selium/control-plane")]
    cp_state_dir: PathBuf,
    /// Path to server cert PEM used for daemon QUIC endpoint.
    #[arg(long, default_value = "certs/server.crt")]
    quic_cert: PathBuf,
    /// Path to server key PEM used for daemon QUIC endpoint.
    #[arg(long, default_value = "certs/server.key")]
    quic_key: PathBuf,
    /// Path to client cert PEM used for outbound peer RPCs (defaults to --quic-cert).
    #[arg(long)]
    quic_peer_cert: Option<PathBuf>,
    /// Path to client key PEM used for outbound peer RPCs (defaults to --quic-key).
    #[arg(long)]
    quic_peer_key: Option<PathBuf>,
    /// Path to CA cert PEM used to validate mTLS clients and peers.
    #[arg(long, default_value = "certs/ca.crt")]
    quic_ca: PathBuf,
    /// Public daemon address advertised into control-plane node registry.
    #[arg(long)]
    cp_public_addr: Option<String>,
    /// TLS server name advertised for this node's daemon endpoint.
    #[arg(long, default_value = "localhost")]
    cp_server_name: String,
    /// Capacity slots advertised for this node.
    #[arg(long, default_value_t = 64)]
    cp_capacity_slots: u32,
    /// Heartbeat interval for node liveness updates (milliseconds).
    #[arg(long, default_value_t = 1000)]
    cp_heartbeat_interval_ms: u64,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "kebab-case", default)]
struct RuntimeConfig {
    log_format: Option<LogFormat>,
    work_dir: Option<PathBuf>,
    module: Option<Vec<String>>,
    generate_certs: Option<GenerateCertsConfig>,
    daemon: Option<DaemonConfig>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "kebab-case", default)]
struct GenerateCertsConfig {
    output_dir: Option<PathBuf>,
    ca_common_name: Option<String>,
    server_name: Option<String>,
    client_name: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "kebab-case", default)]
struct DaemonConfig {
    listen: Option<String>,
    cp_node_id: Option<String>,
    cp_peers: Option<Vec<String>>,
    cp_bootstrap_leader: Option<bool>,
    cp_state_dir: Option<PathBuf>,
    quic_cert: Option<PathBuf>,
    quic_key: Option<PathBuf>,
    quic_peer_cert: Option<PathBuf>,
    quic_peer_key: Option<PathBuf>,
    quic_ca: Option<PathBuf>,
    cp_public_addr: Option<String>,
    cp_server_name: Option<String>,
    cp_capacity_slots: Option<u32>,
    cp_heartbeat_interval_ms: Option<u64>,
}

struct DaemonState {
    kernel: Kernel,
    registry: Arc<Registry>,
    work_dir: PathBuf,
    processes: Mutex<BTreeMap<String, usize>>,
    control_plane: Arc<control_plane::ControlPlaneService>,
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
    ];
    let _session = Session::bootstrap(entitlements, [0; 32]);
}

async fn run(
    kernel: Kernel,
    registry: Arc<Registry>,
    shutdown: Arc<Notify>,
    work_dir: impl AsRef<Path>,
    modules: Option<&Vec<String>>,
) -> Result<()> {
    info!("kernel initialised; starting host bridge");
    bootstrap_runtime_session();

    if let Some(mods) = modules {
        modules::spawn_from_cli(&kernel, &registry, &work_dir, mods).await?;
    }

    signal::ctrl_c().await?;
    shutdown.notify_waiters();
    Ok(())
}

async fn run_daemon(
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

    let cp_state_dir = make_abs(&work_dir, &args.cp_state_dir);
    let cp_config = control_plane::ControlPlaneConfig::from_parts(
        args.cp_node_id.clone(),
        &args.cp_peers,
        args.cp_bootstrap_leader,
        cp_state_dir,
        control_plane::QuicPeerAuthConfig {
            ca_cert_path: ca_path.clone(),
            cert_path: peer_cert_path,
            key_path: peer_key_path,
        },
    )?;
    let control_plane = Arc::new(control_plane::ControlPlaneService::new(cp_config)?);
    control_plane.recover().await?;
    tokio::spawn(Arc::clone(&control_plane).run());

    let public_addr = args
        .cp_public_addr
        .clone()
        .unwrap_or_else(|| args.listen.clone());
    tokio::spawn(run_node_heartbeat(
        Arc::clone(&control_plane),
        args.cp_node_id.clone(),
        public_addr,
        args.cp_server_name.clone(),
        args.cp_capacity_slots,
        args.cp_heartbeat_interval_ms,
    ));

    let state = Rc::new(DaemonState {
        kernel,
        registry,
        work_dir,
        processes: Mutex::new(BTreeMap::new()),
        control_plane,
    });

    let endpoint = build_server_endpoint(&args.listen, &cert_path, &key_path, &ca_path)?;

    loop {
        tokio::select! {
            incoming = endpoint.accept() => {
                let Some(incoming) = incoming else {
                    break;
                };
                if let Err(err) = handle_incoming(Rc::clone(&state), incoming).await {
                    tracing::warn!("daemon QUIC connection error: {err:#}");
                }
            }
            _ = signal::ctrl_c() => {
                info!("daemon shutting down after Ctrl-C");
                break;
            }
        }
    }

    let to_stop = {
        let processes = state.processes.lock().await;
        processes.clone()
    };
    for (instance_id, process_id) in to_stop {
        if let Err(err) = modules::stop_process(&state.kernel, &state.registry, process_id).await {
            tracing::warn!(%instance_id, process_id, "failed to stop process on shutdown: {err:#}");
        }
    }

    shutdown.notify_waiters();
    Ok(())
}

async fn run_node_heartbeat(
    control_plane: Arc<control_plane::ControlPlaneService>,
    node_id: String,
    daemon_addr: String,
    daemon_server_name: String,
    capacity_slots: u32,
    interval_ms: u64,
) {
    loop {
        let now_ms = unix_ms();
        let request = MutateApiRequest {
            idempotency_key: format!("node-heartbeat:{node_id}:{now_ms}"),
            mutation: Mutation::UpsertNode {
                spec: NodeSpec {
                    name: node_id.clone(),
                    capacity_slots,
                    supported_isolation: vec![
                        IsolationProfile::Standard,
                        IsolationProfile::Hardened,
                        IsolationProfile::Microvm,
                    ],
                    daemon_addr: daemon_addr.clone(),
                    daemon_server_name: daemon_server_name.clone(),
                    last_heartbeat_ms: now_ms,
                },
            },
        };

        if let Err(err) = control_plane.mutate(request).await {
            tracing::warn!(node_id, "failed to publish node heartbeat: {err:#}");
        }

        sleep(Duration::from_millis(interval_ms.max(250))).await;
    }
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
        Method::ControlMutate => {
            let payload: MutateApiRequest = match decode_payload(&envelope) {
                Ok(payload) => payload,
                Err(err) => return rpc_decode_error(envelope.method, envelope.request_id, err),
            };
            match state.control_plane.mutate(payload).await {
                Ok(response) => encode_response(envelope.method, envelope.request_id, &response),
                Err(err) => rpc_server_error(envelope.method, envelope.request_id, err),
            }
        }
        Method::ControlQuery => {
            let payload: QueryApiRequest = match decode_payload(&envelope) {
                Ok(payload) => payload,
                Err(err) => return rpc_decode_error(envelope.method, envelope.request_id, err),
            };
            match state.control_plane.query(payload).await {
                Ok(response) => encode_response(envelope.method, envelope.request_id, &response),
                Err(err) => rpc_server_error(envelope.method, envelope.request_id, err),
            }
        }
        Method::ControlStatus => match state.control_plane.status().await {
            Ok(status) => encode_response(envelope.method, envelope.request_id, &status),
            Err(err) => rpc_server_error(envelope.method, envelope.request_id, err),
        },
        Method::ControlReplay => {
            let payload: ReplayApiRequest = match decode_payload(&envelope) {
                Ok(payload) => payload,
                Err(err) => return rpc_decode_error(envelope.method, envelope.request_id, err),
            };
            match state.control_plane.replay_events(payload.limit).await {
                Ok(events) => encode_response(
                    envelope.method,
                    envelope.request_id,
                    &ReplayApiResponse { events },
                ),
                Err(err) => rpc_server_error(envelope.method, envelope.request_id, err),
            }
        }
        Method::RaftRequestVote => {
            let payload: RequestVoteApiRequest = match decode_payload(&envelope) {
                Ok(payload) => payload,
                Err(err) => return rpc_decode_error(envelope.method, envelope.request_id, err),
            };
            match state.control_plane.handle_request_vote(payload).await {
                Ok(response) => encode_response(envelope.method, envelope.request_id, &response),
                Err(err) => rpc_server_error(envelope.method, envelope.request_id, err),
            }
        }
        Method::RaftAppendEntries => {
            let payload: AppendEntriesApiRequest = match decode_payload(&envelope) {
                Ok(payload) => payload,
                Err(err) => return rpc_decode_error(envelope.method, envelope.request_id, err),
            };
            match state.control_plane.handle_append_entries(payload).await {
                Ok(response) => encode_response(envelope.method, envelope.request_id, &response),
                Err(err) => rpc_server_error(envelope.method, envelope.request_id, err),
            }
        }
        Method::StartInstance => {
            let payload: StartRequest = match decode_payload(&envelope) {
                Ok(payload) => payload,
                Err(err) => return rpc_decode_error(envelope.method, envelope.request_id, err),
            };
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

fn unix_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn initialise_tracing(format: LogFormat) -> Result<()> {
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(env::var("RUST_LOG").unwrap_or_else(|_| "info".into())))?;

    match format {
        LogFormat::Text => {
            tracing_subscriber::fmt()
                .with_env_filter(filter.clone())
                .with_target(false)
                .with_timer(SystemTime)
                .init();
        }
        LogFormat::Json => {
            tracing_subscriber::fmt()
                .json()
                .with_env_filter(filter)
                .with_target(false)
                .with_current_span(true)
                .with_span_list(true)
                .init();
        }
    }

    Ok(())
}

fn load_server_options() -> Result<ServerOptions> {
    load_server_options_from(std::env::args_os())
}

fn load_server_options_from<I, T>(args: I) -> Result<ServerOptions>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let matches = ServerOptions::command().get_matches_from(args);
    let mut options = ServerOptions::from_arg_matches(&matches)
        .map_err(|err| anyhow!("parse runtime arguments: {err}"))?;

    if let Some(path) = options.config.clone() {
        let config: RuntimeConfig = load_toml_config(&path)?;
        merge_runtime_config(&mut options, &matches, config);
    }

    Ok(options)
}

fn merge_runtime_config(
    options: &mut ServerOptions,
    matches: &clap::ArgMatches,
    config: RuntimeConfig,
) {
    let RuntimeConfig {
        log_format,
        work_dir,
        module,
        generate_certs,
        daemon,
    } = config;

    if should_apply_config(matches.value_source("log_format")) {
        if let Some(log_format) = log_format {
            options.log_format = log_format;
        }
    }

    if should_apply_config(matches.value_source("work_dir")) {
        if let Some(work_dir) = work_dir {
            options.work_dir = work_dir;
        }
    }

    if should_apply_config(matches.value_source("module")) {
        if let Some(module) = module {
            options.module = Some(module);
        }
    }

    match (&mut options.command, generate_certs, daemon) {
        (Some(ServerCommand::GenerateCerts(args)), Some(cfg), _) => {
            merge_generate_certs_config(args, matches.subcommand_matches("generate-certs"), cfg);
        }
        (Some(ServerCommand::Daemon(args)), _, Some(cfg)) => {
            merge_daemon_config(args, matches.subcommand_matches("daemon"), cfg);
        }
        _ => {}
    }
}

fn merge_generate_certs_config(
    args: &mut GenerateCertsArgs,
    matches: Option<&clap::ArgMatches>,
    config: GenerateCertsConfig,
) {
    let value_source = |name| matches.and_then(|m| m.value_source(name));

    if should_apply_config(value_source("output_dir")) {
        if let Some(output_dir) = config.output_dir {
            args.output_dir = output_dir;
        }
    }
    if should_apply_config(value_source("ca_common_name")) {
        if let Some(ca_common_name) = config.ca_common_name {
            args.ca_common_name = ca_common_name;
        }
    }
    if should_apply_config(value_source("server_name")) {
        if let Some(server_name) = config.server_name {
            args.server_name = server_name;
        }
    }
    if should_apply_config(value_source("client_name")) {
        if let Some(client_name) = config.client_name {
            args.client_name = client_name;
        }
    }
}

fn merge_daemon_config(
    args: &mut DaemonArgs,
    matches: Option<&clap::ArgMatches>,
    config: DaemonConfig,
) {
    let value_source = |name| matches.and_then(|m| m.value_source(name));

    if should_apply_config(value_source("listen")) {
        if let Some(listen) = config.listen {
            args.listen = listen;
        }
    }
    if should_apply_config(value_source("cp_node_id")) {
        if let Some(cp_node_id) = config.cp_node_id {
            args.cp_node_id = cp_node_id;
        }
    }
    if should_apply_config(value_source("cp_peers")) {
        if let Some(cp_peers) = config.cp_peers {
            args.cp_peers = cp_peers;
        }
    }
    if should_apply_config(value_source("cp_bootstrap_leader")) {
        if let Some(cp_bootstrap_leader) = config.cp_bootstrap_leader {
            args.cp_bootstrap_leader = cp_bootstrap_leader;
        }
    }
    if should_apply_config(value_source("cp_state_dir")) {
        if let Some(cp_state_dir) = config.cp_state_dir {
            args.cp_state_dir = cp_state_dir;
        }
    }
    if should_apply_config(value_source("quic_cert")) {
        if let Some(quic_cert) = config.quic_cert {
            args.quic_cert = quic_cert;
        }
    }
    if should_apply_config(value_source("quic_key")) {
        if let Some(quic_key) = config.quic_key {
            args.quic_key = quic_key;
        }
    }
    if should_apply_config(value_source("quic_peer_cert")) {
        if let Some(quic_peer_cert) = config.quic_peer_cert {
            args.quic_peer_cert = Some(quic_peer_cert);
        }
    }
    if should_apply_config(value_source("quic_peer_key")) {
        if let Some(quic_peer_key) = config.quic_peer_key {
            args.quic_peer_key = Some(quic_peer_key);
        }
    }
    if should_apply_config(value_source("quic_ca")) {
        if let Some(quic_ca) = config.quic_ca {
            args.quic_ca = quic_ca;
        }
    }
    if should_apply_config(value_source("cp_public_addr")) {
        if let Some(cp_public_addr) = config.cp_public_addr {
            args.cp_public_addr = Some(cp_public_addr);
        }
    }
    if should_apply_config(value_source("cp_server_name")) {
        if let Some(cp_server_name) = config.cp_server_name {
            args.cp_server_name = cp_server_name;
        }
    }
    if should_apply_config(value_source("cp_capacity_slots")) {
        if let Some(cp_capacity_slots) = config.cp_capacity_slots {
            args.cp_capacity_slots = cp_capacity_slots;
        }
    }
    if should_apply_config(value_source("cp_heartbeat_interval_ms")) {
        if let Some(cp_heartbeat_interval_ms) = config.cp_heartbeat_interval_ms {
            args.cp_heartbeat_interval_ms = cp_heartbeat_interval_ms;
        }
    }
}

fn should_apply_config(source: Option<ValueSource>) -> bool {
    !matches!(
        source,
        Some(ValueSource::CommandLine | ValueSource::EnvVariable)
    )
}

fn load_toml_config<T>(path: &Path) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    let raw =
        fs::read_to_string(path).with_context(|| format!("read config file {}", path.display()))?;
    toml::from_str(&raw).with_context(|| format!("parse TOML config {}", path.display()))
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = load_server_options()?;
    initialise_tracing(args.log_format)?;

    if let Some(ServerCommand::GenerateCerts(cert_args)) = &args.command {
        certs::generate_certificates(
            &cert_args.output_dir,
            &cert_args.ca_common_name,
            &cert_args.server_name,
            &cert_args.client_name,
        )?;
        return Ok(());
    }

    let (kernel, shutdown) = kernel::build(&args.work_dir).context("build runtime kernel")?;
    let registry = Registry::new();

    if let Some(ServerCommand::Daemon(daemon_args)) = args.command {
        return run_daemon(kernel, registry, shutdown, args.work_dir, daemon_args).await;
    }

    run(
        kernel,
        registry,
        shutdown,
        &args.work_dir,
        args.module.as_ref(),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn write_test_config(contents: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "selium-runtime-config-{}.toml",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        fs::write(&path, contents).expect("write config");
        path
    }

    #[test]
    fn parses_default_options() {
        let opts = load_server_options_from(["selium-runtime"]).expect("parse opts");
        assert_eq!(opts.log_format, LogFormat::Text);
        assert!(opts.command.is_none());
        assert_eq!(opts.work_dir, PathBuf::from("."));
    }

    #[test]
    fn parses_generate_certs_command() {
        let opts = load_server_options_from([
            "selium-runtime",
            "generate-certs",
            "--output-dir",
            "certs-out",
            "--server-name",
            "example.local",
        ])
        .expect("parse opts");
        let Some(ServerCommand::GenerateCerts(args)) = opts.command else {
            panic!("expected generate-certs command");
        };
        assert_eq!(args.output_dir, PathBuf::from("certs-out"));
        assert_eq!(args.server_name, "example.local");
    }

    #[test]
    fn parses_daemon_command() {
        let opts =
            load_server_options_from(["selium-runtime", "daemon", "--listen", "127.0.0.1:7999"])
                .expect("parse opts");
        let Some(ServerCommand::Daemon(args)) = opts.command else {
            panic!("expected daemon command");
        };
        assert_eq!(args.listen, "127.0.0.1:7999");
    }

    #[test]
    fn applies_runtime_config_when_flag_missing() {
        let config = write_test_config(
            r#"
log-format = "json"
work-dir = "runtime-data"

[daemon]
listen = "127.0.0.1:7999"
cp-node-id = "cfg-node"
"#,
        );

        let opts = load_server_options_from([
            "selium-runtime",
            "--config",
            config.to_str().expect("config path"),
            "daemon",
        ])
        .expect("parse opts");

        assert_eq!(opts.log_format, LogFormat::Json);
        assert_eq!(opts.work_dir, PathBuf::from("runtime-data"));
        let Some(ServerCommand::Daemon(args)) = opts.command else {
            panic!("expected daemon command");
        };
        assert_eq!(args.listen, "127.0.0.1:7999");
        assert_eq!(args.cp_node_id, "cfg-node");
    }

    #[test]
    fn command_line_runtime_args_override_config() {
        let config = write_test_config(
            r#"
[daemon]
listen = "127.0.0.1:7999"
"#,
        );

        let opts = load_server_options_from([
            "selium-runtime",
            "--config",
            config.to_str().expect("config path"),
            "daemon",
            "--listen",
            "127.0.0.1:8001",
        ])
        .expect("parse opts");

        let Some(ServerCommand::Daemon(args)) = opts.command else {
            panic!("expected daemon command");
        };
        assert_eq!(args.listen, "127.0.0.1:8001");
    }
}
