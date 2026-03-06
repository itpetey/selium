use std::{
    collections::BTreeMap,
    fs,
    net::{SocketAddr, ToSocketAddrs},
    path::{Path, PathBuf},
    rc::Rc,
    sync::Arc,
};

use anyhow::{Context, Result, anyhow};
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
use tokio::{
    signal,
    sync::{Mutex, Notify},
    time::{Duration, sleep},
};
use tracing::info;

use crate::{config::DaemonArgs, control_plane, modules};

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
