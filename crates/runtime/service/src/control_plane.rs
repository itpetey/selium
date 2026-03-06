use std::{
    collections::BTreeMap,
    fs,
    net::{IpAddr, SocketAddr, ToSocketAddrs},
    path::PathBuf,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow};
use quinn::crypto::rustls::QuicClientConfig;
use quinn::{Connection, Endpoint};
use rkyv::{
    Archive, Deserialize, Serialize,
    api::high::{HighDeserializer, HighValidator},
};
use rustls::{RootCertStore, pki_types::PrivateKeyDer};
use rustls_pemfile::{certs, private_key};
use selium_abi::{DataValue, RkyvEncode, decode_rkyv, encode_rkyv};
use selium_control_plane_protocol::{
    AppendEntriesApiRequest, Method, MutateApiRequest, MutateApiResponse, QueryApiRequest,
    QueryApiResponse, RequestVoteApiRequest, StatusApiResponse, decode_envelope, decode_error,
    decode_payload, encode_request, is_error, read_framed, write_framed,
};
use selium_io_consensus::{
    AppendEntries, AppendEntriesResponse, ConsensusConfig, ConsensusError, RaftNode, RequestVote,
    RequestVoteResponse, TickAction,
};
use selium_io_durability::{DurableLogStore, ReplayStart, RetentionPolicy};
use selium_module_control_plane::runtime::{
    ControlPlaneEngine, EngineSnapshot, MutationEnvelope, MutationResponse, RuntimeError,
};
use tokio::{
    sync::Mutex,
    time::{Duration, sleep},
};

#[derive(Debug, Clone)]
pub struct QuicPeerAuthConfig {
    pub ca_cert_path: PathBuf,
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct PeerTarget {
    pub address: SocketAddr,
    pub server_name: String,
}

#[derive(Debug, Clone)]
pub struct ControlPlaneConfig {
    pub node_id: String,
    pub peers: BTreeMap<String, PeerTarget>,
    pub bootstrap_leader: bool,
    pub state_dir: PathBuf,
    pub quic_auth: QuicPeerAuthConfig,
}

impl ControlPlaneConfig {
    pub fn from_parts(
        node_id: String,
        peer_specs: &[String],
        bootstrap_leader: bool,
        state_dir: PathBuf,
        quic_auth: QuicPeerAuthConfig,
    ) -> Result<Self> {
        let mut peers = BTreeMap::new();
        for raw in peer_specs {
            let (id, endpoint) = raw.split_once('=').ok_or_else(|| {
                anyhow!("invalid --cp-peer `{raw}` expected node_id=host:port[@server_name]")
            })?;
            let (endpoint, explicit_server_name) = endpoint
                .split_once('@')
                .map(|(left, right)| (left, Some(right)))
                .unwrap_or((endpoint, None));

            let address = parse_socket_addr(endpoint)
                .with_context(|| format!("parse peer endpoint `{endpoint}` for `{id}`"))?;
            let server_name = explicit_server_name
                .map(ToString::to_string)
                .unwrap_or_else(|| derive_server_name(endpoint));

            peers.insert(
                id.to_string(),
                PeerTarget {
                    address,
                    server_name,
                },
            );
        }

        Ok(Self {
            node_id,
            peers,
            bootstrap_leader,
            state_dir,
            quic_auth,
        })
    }
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
#[rkyv(bytecheck())]
struct DurableEvent {
    idempotency_key: String,
    index: u64,
    term: u64,
    result: DataValue,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
#[rkyv(bytecheck())]
struct PersistedRaft {
    raft: RaftNode,
}

struct ControlPlaneInner {
    raft: RaftNode,
    engine: ControlPlaneEngine,
    dedupe: BTreeMap<String, MutationResponse>,
    durable: DurableLogStore,
}

pub struct ControlPlaneService {
    config: ControlPlaneConfig,
    raft_path: PathBuf,
    snapshot_path: PathBuf,
    peer_endpoint: Endpoint,
    peer_connections: Mutex<BTreeMap<String, Connection>>,
    request_seq: AtomicU64,
    inner: Mutex<ControlPlaneInner>,
}

impl ControlPlaneService {
    pub fn new(config: ControlPlaneConfig) -> Result<Self> {
        fs::create_dir_all(&config.state_dir).with_context(|| {
            format!(
                "create control-plane state dir {}",
                config.state_dir.display()
            )
        })?;

        let raft_path = config.state_dir.join("raft-state.rkyv");
        let snapshot_path = config.state_dir.join("engine-snapshot.rkyv");
        let durable_path = config.state_dir.join("control-plane-events.rkyv");

        let peers = config.peers.keys().cloned().collect::<Vec<_>>();
        let now_ms = unix_ms();
        let mut raft = if raft_path.exists() {
            let bytes = fs::read(&raft_path)
                .with_context(|| format!("read raft state {}", raft_path.display()))?;
            decode_rkyv::<PersistedRaft>(&bytes)
                .with_context(|| format!("decode raft state {}", raft_path.display()))?
                .raft
        } else {
            RaftNode::new(
                ConsensusConfig::default_for(config.node_id.clone(), peers),
                now_ms,
            )
        };

        if (config.bootstrap_leader || config.peers.is_empty())
            && !raft.is_leader()
            && raft.current_term() == 0
        {
            raft.bootstrap_as_leader();
        }

        let engine = if snapshot_path.exists() {
            let bytes = fs::read(&snapshot_path)
                .with_context(|| format!("read snapshot {}", snapshot_path.display()))?;
            let snapshot = decode_rkyv::<EngineSnapshot>(&bytes)
                .with_context(|| format!("decode snapshot {}", snapshot_path.display()))?;
            ControlPlaneEngine::from_snapshot(snapshot)
        } else {
            ControlPlaneEngine::default()
        };

        let durable = DurableLogStore::file_backed(durable_path, RetentionPolicy::default())
            .context("initialise durable control-plane event log")?;

        let peer_endpoint = build_client_endpoint(&config.quic_auth)?;

        let service = Self {
            config,
            raft_path,
            snapshot_path,
            peer_endpoint,
            peer_connections: Mutex::new(BTreeMap::new()),
            request_seq: AtomicU64::new(1),
            inner: Mutex::new(ControlPlaneInner {
                raft,
                engine,
                dedupe: BTreeMap::new(),
                durable,
            }),
        };

        Ok(service)
    }

    pub async fn recover(&self) -> Result<()> {
        let mut inner = self.inner.lock().await;
        let entries = inner
            .raft
            .committed_entries_from(inner.engine.last_applied());
        for entry in entries {
            let response = inner
                .engine
                .apply_committed_entry(&entry)
                .map_err(anyhow_from_runtime)?;
            let envelope =
                ControlPlaneEngine::decode_mutation(&entry.payload).map_err(anyhow_from_runtime)?;
            inner
                .dedupe
                .insert(envelope.idempotency_key.clone(), response.clone());
        }
        self.persist_locked(&inner).await?;
        Ok(())
    }

    pub async fn run(self: Arc<Self>) {
        loop {
            if let Err(err) = self.tick_once().await {
                tracing::warn!("control-plane tick error: {err:#}");
            }
            sleep(Duration::from_millis(150)).await;
        }
    }

    pub async fn status(&self) -> Result<StatusApiResponse> {
        let inner = self.inner.lock().await;
        let status = inner.raft.status();
        Ok(StatusApiResponse {
            node_id: status.node_id,
            role: format!("{:?}", status.role),
            current_term: status.current_term,
            leader_id: status.leader_id,
            commit_index: status.commit_index,
            last_applied: status.last_applied,
            peers: self.config.peers.keys().cloned().collect(),
            table_count: inner.engine.snapshot().tables.tables().len(),
            durable_events: inner.durable.latest_sequence(),
        })
    }

    pub async fn mutate(&self, request: MutateApiRequest) -> Result<MutateApiResponse> {
        let forward_request = request.clone();
        {
            let inner = self.inner.lock().await;
            if let Some(existing) = inner.dedupe.get(&request.idempotency_key) {
                return Ok(MutateApiResponse {
                    committed: true,
                    index: Some(existing.index),
                    leader_hint: inner.raft.leader_hint(),
                    result: Some(existing.result.clone()),
                    error: None,
                });
            }
        }

        let envelope = MutationEnvelope {
            idempotency_key: request.idempotency_key.clone(),
            mutation: request.mutation,
        };

        let payload =
            ControlPlaneEngine::encode_mutation(&envelope).map_err(anyhow_from_runtime)?;
        let (entry_index, append_requests, leader_hint) = {
            let mut inner = self.inner.lock().await;
            let entry = match inner.raft.propose(payload) {
                Ok(entry) => entry,
                Err(ConsensusError::NotLeader) => {
                    let leader_hint = inner.raft.leader_hint();
                    drop(inner);
                    if let Some(forwarded) = self
                        .forward_mutation_to_leader(&forward_request, leader_hint.as_deref())
                        .await
                    {
                        return Ok(forwarded);
                    }
                    return Ok(MutateApiResponse {
                        committed: false,
                        index: None,
                        leader_hint,
                        result: None,
                        error: Some("not leader".to_string()),
                    });
                }
            };
            let requests = inner.raft.build_append_entries_requests();
            if requests.is_empty() {
                inner.raft.force_commit_to(entry.index);
            }
            (entry.index, requests, inner.raft.leader_hint())
        };

        for (peer_id, append) in append_requests {
            let response = self.send_append_entries(&peer_id, append).await;
            if let Some(response) = response {
                let mut inner = self.inner.lock().await;
                inner
                    .raft
                    .handle_append_entries_response(&peer_id, response);
            }
        }

        let mut inner = self.inner.lock().await;
        let applied = self.apply_committed_locked(&mut inner).await?;
        self.persist_locked(&inner).await?;

        if let Some(result) = applied.get(&entry_index) {
            return Ok(MutateApiResponse {
                committed: true,
                index: Some(result.index),
                leader_hint,
                result: Some(result.result.clone()),
                error: None,
            });
        }

        Ok(MutateApiResponse {
            committed: false,
            index: Some(entry_index),
            leader_hint,
            result: None,
            error: Some("quorum not reached".to_string()),
        })
    }

    pub async fn query(&self, request: QueryApiRequest) -> Result<QueryApiResponse> {
        let inner = self.inner.lock().await;
        if !request.allow_stale && !inner.raft.is_leader() {
            let leader_hint = inner.raft.leader_hint();
            drop(inner);
            if let Some(forwarded) = self
                .forward_query_to_leader(&request, leader_hint.as_deref())
                .await
            {
                return Ok(forwarded);
            }
            return Ok(QueryApiResponse {
                leader_hint,
                result: None,
                error: Some("not leader".to_string()),
            });
        }

        let response = inner
            .engine
            .query(request.query)
            .map_err(anyhow_from_runtime)?;

        Ok(QueryApiResponse {
            leader_hint: inner.raft.leader_hint(),
            result: Some(response.result),
            error: None,
        })
    }

    async fn forward_mutation_to_leader(
        &self,
        request: &MutateApiRequest,
        leader_hint: Option<&str>,
    ) -> Option<MutateApiResponse> {
        let leader = leader_hint?;
        if leader == self.config.node_id {
            return None;
        }
        self.send_peer_rpc::<MutateApiRequest, MutateApiResponse>(
            leader,
            Method::ControlMutate,
            request,
        )
        .await
    }

    async fn forward_query_to_leader(
        &self,
        request: &QueryApiRequest,
        leader_hint: Option<&str>,
    ) -> Option<QueryApiResponse> {
        let leader = leader_hint?;
        if leader == self.config.node_id {
            return None;
        }
        self.send_peer_rpc::<QueryApiRequest, QueryApiResponse>(
            leader,
            Method::ControlQuery,
            request,
        )
        .await
    }

    pub async fn handle_request_vote(
        &self,
        request: RequestVoteApiRequest,
    ) -> Result<RequestVoteResponse> {
        let mut inner = self.inner.lock().await;
        let response = inner.raft.handle_request_vote(request.request, unix_ms());
        self.persist_locked(&inner).await?;
        Ok(response)
    }

    pub async fn handle_append_entries(
        &self,
        request: AppendEntriesApiRequest,
    ) -> Result<AppendEntriesResponse> {
        let mut inner = self.inner.lock().await;
        let response = inner.raft.handle_append_entries(request.request, unix_ms());
        let _ = self.apply_committed_locked(&mut inner).await?;
        self.persist_locked(&inner).await?;
        Ok(response)
    }

    async fn tick_once(&self) -> Result<()> {
        let action = {
            let mut inner = self.inner.lock().await;
            inner.raft.tick(unix_ms())
        };

        let Some(action) = action else {
            return Ok(());
        };

        match action {
            TickAction::RequestVotes(requests) => {
                for (peer_id, request_vote) in requests {
                    if let Some(response) = self.send_request_vote(&peer_id, request_vote).await {
                        let follow_up = {
                            let mut inner = self.inner.lock().await;
                            inner.raft.handle_request_vote_response(&peer_id, response)
                        };
                        if let Some(TickAction::AppendEntries(heartbeats)) = follow_up {
                            for (peer, heartbeat) in heartbeats {
                                let response = self.send_append_entries(&peer, heartbeat).await;
                                if let Some(response) = response {
                                    let mut inner = self.inner.lock().await;
                                    inner.raft.handle_append_entries_response(&peer, response);
                                }
                            }
                        }
                    }
                }
            }
            TickAction::AppendEntries(requests) => {
                for (peer_id, append) in requests {
                    if let Some(response) = self.send_append_entries(&peer_id, append).await {
                        let mut inner = self.inner.lock().await;
                        inner
                            .raft
                            .handle_append_entries_response(&peer_id, response);
                    }
                }
            }
        }

        let mut inner = self.inner.lock().await;
        let _ = self.apply_committed_locked(&mut inner).await?;
        self.persist_locked(&inner).await?;
        Ok(())
    }

    async fn apply_committed_locked(
        &self,
        inner: &mut ControlPlaneInner,
    ) -> Result<BTreeMap<u64, MutationResponse>> {
        let entries = inner.raft.take_committed_entries();
        let mut applied = BTreeMap::new();

        for entry in entries {
            let envelope =
                ControlPlaneEngine::decode_mutation(&entry.payload).map_err(anyhow_from_runtime)?;
            let response = inner
                .engine
                .apply_committed_entry(&entry)
                .map_err(anyhow_from_runtime)?;

            inner
                .dedupe
                .insert(envelope.idempotency_key.clone(), response.clone());
            applied.insert(entry.index, response.clone());

            let event = DurableEvent {
                idempotency_key: envelope.idempotency_key,
                index: response.index,
                term: entry.term,
                result: response.result,
            };
            inner
                .durable
                .append(
                    unix_ms(),
                    BTreeMap::from([("source".to_string(), "control-plane".to_string())]),
                    encode_rkyv(&event).map_err(|err| anyhow!("serialize durable event: {err}"))?,
                )
                .map_err(|err| anyhow!("append durable event: {err}"))?;
        }

        Ok(applied)
    }

    async fn persist_locked(&self, inner: &ControlPlaneInner) -> Result<()> {
        let persisted = PersistedRaft {
            raft: inner.raft.clone(),
        };
        fs::write(
            &self.raft_path,
            encode_rkyv(&persisted).context("encode raft state")?,
        )
        .with_context(|| format!("write raft state {}", self.raft_path.display()))?;

        fs::write(
            &self.snapshot_path,
            encode_rkyv(&inner.engine.snapshot()).context("encode engine snapshot")?,
        )
        .with_context(|| format!("write engine snapshot {}", self.snapshot_path.display()))?;

        Ok(())
    }

    async fn send_request_vote(
        &self,
        peer_id: &str,
        request_vote: RequestVote,
    ) -> Option<RequestVoteResponse> {
        self.send_peer_rpc(
            peer_id,
            Method::RaftRequestVote,
            &RequestVoteApiRequest {
                request: request_vote,
            },
        )
        .await
    }

    async fn send_append_entries(
        &self,
        peer_id: &str,
        append: AppendEntries,
    ) -> Option<AppendEntriesResponse> {
        self.send_peer_rpc(
            peer_id,
            Method::RaftAppendEntries,
            &AppendEntriesApiRequest { request: append },
        )
        .await
    }

    async fn send_peer_rpc<Req, Resp>(
        &self,
        peer_id: &str,
        method: Method,
        request: &Req,
    ) -> Option<Resp>
    where
        Req: RkyvEncode,
        Resp: Archive + Sized,
        for<'a> Resp::Archived: rkyv::Deserialize<Resp, HighDeserializer<rkyv::rancor::Error>>
            + rkyv::bytecheck::CheckBytes<HighValidator<'a, rkyv::rancor::Error>>,
    {
        let connection = self.connection_for_peer(peer_id).await?;
        let (mut send, mut recv) = connection.open_bi().await.ok()?;
        let request_id = self.request_seq.fetch_add(1, Ordering::Relaxed);
        let request_bytes = encode_request(method, request_id, request).ok()?;
        write_framed(&mut send, &request_bytes).await.ok()?;
        let _ = send.finish();

        let response_bytes = read_framed(&mut recv).await.ok()?;
        let envelope = decode_envelope(&response_bytes).ok()?;
        if envelope.method != method || envelope.request_id != request_id {
            return None;
        }

        if is_error(&envelope) {
            if let Ok(error) = decode_error(&envelope) {
                tracing::warn!(
                    peer_id,
                    method = method.as_u16(),
                    "peer rpc error {}",
                    error.message
                );
            }
            return None;
        }

        decode_payload::<Resp>(&envelope).ok()
    }

    async fn connection_for_peer(&self, peer_id: &str) -> Option<Connection> {
        {
            let connections = self.peer_connections.lock().await;
            if let Some(existing) = connections.get(peer_id)
                && existing.close_reason().is_none()
            {
                return Some(existing.clone());
            }
        }

        let target = self.config.peers.get(peer_id)?;
        let connecting = self
            .peer_endpoint
            .connect(target.address, &target.server_name)
            .ok()?;
        let connection = connecting.await.ok()?;

        let mut connections = self.peer_connections.lock().await;
        connections.insert(peer_id.to_string(), connection.clone());
        Some(connection)
    }

    pub async fn replay_events(&self, limit: usize) -> Result<Vec<DataValue>> {
        let inner = self.inner.lock().await;
        let batch = inner
            .durable
            .replay(ReplayStart::Latest, limit)
            .map_err(|err| anyhow!("replay durable events: {err}"))?;
        let events = batch
            .records
            .into_iter()
            .filter_map(|record| decode_rkyv::<DurableEvent>(&record.payload).ok())
            .map(|event| {
                DataValue::Map(BTreeMap::from([
                    (
                        "idempotency_key".to_string(),
                        DataValue::from(event.idempotency_key),
                    ),
                    ("index".to_string(), DataValue::from(event.index)),
                    ("term".to_string(), DataValue::from(event.term)),
                    ("result".to_string(), event.result),
                ]))
            })
            .collect::<Vec<_>>();
        Ok(events)
    }
}

fn build_client_endpoint(config: &QuicPeerAuthConfig) -> Result<Endpoint> {
    let bind = if cfg!(target_family = "unix") {
        SocketAddr::from_str("0.0.0.0:0").expect("valid addr")
    } else {
        SocketAddr::from_str("127.0.0.1:0").expect("valid addr")
    };
    let mut endpoint = Endpoint::client(bind).context("create QUIC client endpoint")?;

    let roots = load_root_store(&config.ca_cert_path)?;
    let cert_chain = load_cert_chain(&config.cert_path)?;
    let key = load_private_key(&config.key_path)?;

    let tls = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(cert_chain, key)
        .context("build QUIC client TLS config")?;

    let quic_crypto = QuicClientConfig::try_from(tls).context("build QUIC crypto config")?;
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

fn derive_server_name(endpoint: &str) -> String {
    let host = endpoint
        .rsplit_once(':')
        .map(|(host, _)| host)
        .unwrap_or(endpoint)
        .trim_matches('[')
        .trim_matches(']')
        .to_string();

    if host.parse::<IpAddr>().is_ok() {
        "localhost".to_string()
    } else {
        host
    }
}

fn load_root_store(path: &PathBuf) -> Result<RootCertStore> {
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

fn load_cert_chain(path: &PathBuf) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>> {
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

fn load_private_key(path: &PathBuf) -> Result<PrivateKeyDer<'static>> {
    let pem = fs::read(path).with_context(|| format!("read key {}", path.display()))?;
    let mut reader = std::io::Cursor::new(pem);
    private_key(&mut reader)
        .context("parse private key")?
        .ok_or_else(|| anyhow!("no private key in {}", path.display()))
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn anyhow_from_runtime(error: RuntimeError) -> anyhow::Error {
    anyhow!(error)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("selium-cp-{name}-{}", unix_ms()))
    }

    #[test]
    fn derives_server_name_for_hostname() {
        assert_eq!(derive_server_name("example.com:7100"), "example.com");
    }

    #[test]
    fn derives_server_name_for_ip_defaults_localhost() {
        assert_eq!(derive_server_name("127.0.0.1:7100"), "localhost");
    }

    #[tokio::test]
    async fn single_node_mutate_and_query_round_trip() {
        let cert_dir = test_dir("certs");
        crate::certs::generate_certificates(&cert_dir, "Test CA", "localhost", "localhost")
            .expect("certs");

        let service = ControlPlaneService::new(ControlPlaneConfig {
            node_id: "n1".to_string(),
            peers: BTreeMap::new(),
            bootstrap_leader: true,
            state_dir: test_dir("single"),
            quic_auth: QuicPeerAuthConfig {
                ca_cert_path: cert_dir.join("ca.crt"),
                cert_path: cert_dir.join("server.crt"),
                key_path: cert_dir.join("server.key"),
            },
        })
        .expect("service");
        service.recover().await.expect("recover");

        let response = service
            .mutate(MutateApiRequest {
                idempotency_key: "k1".to_string(),
                mutation: selium_module_control_plane::runtime::Mutation::Table {
                    command: selium_io_tables::TableCommand::Put {
                        table: "apps".to_string(),
                        key: "echo".to_string(),
                        value: DataValue::Map(BTreeMap::from([(
                            "replicas".to_string(),
                            DataValue::from(1u64),
                        )])),
                    },
                },
            })
            .await
            .expect("mutate");
        assert!(response.committed);

        let query = service
            .query(QueryApiRequest {
                query: selium_module_control_plane::runtime::Query::TableGet {
                    table: "apps".to_string(),
                    key: "echo".to_string(),
                },
                allow_stale: false,
            })
            .await
            .expect("query");
        let result = query.result.expect("value");
        assert_eq!(result.get("version").and_then(DataValue::as_u64), Some(1));
    }
}
