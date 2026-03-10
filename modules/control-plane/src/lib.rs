//! System control-plane module bootstrap helpers and guest control-plane loop.

use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    rc::Rc,
    time::Duration,
};

use anyhow::{Context, Result, anyhow};
use rkyv::{Archive, Deserialize, Serialize};
use selium_abi::{DataValue, StorageReplayStart, decode_rkyv, encode_rkyv};
use selium_control_plane_api::{
    ContractKind, ControlPlaneState, IsolationProfile, NodeSpec, PublicEndpointRef,
};
use selium_control_plane_protocol::{
    ActivateEndpointBridgeRequest, ActivateEndpointBridgeResponse, AppendEntriesApiRequest,
    DeactivateEndpointBridgeRequest, DeactivateEndpointBridgeResponse, EndpointBridgeSemantics,
    EventBridgeSemantics, EventDeliveryMode, ListResponse, ManagedEndpointBinding,
    ManagedEndpointBindingType, ManagedEndpointRole, Method, MutateApiRequest, MutateApiResponse,
    QueryApiRequest, QueryApiResponse, ReplayApiResponse, RequestVoteApiRequest,
    ServiceBridgeSemantics, ServiceCorrelationMode, StartRequest, StatusApiResponse, StopRequest,
    StreamBridgeSemantics, StreamLifecycleMode, decode_envelope, decode_payload,
    encode_error_response, encode_response, is_error, is_request,
};
use selium_control_plane_runtime::{
    ControlPlaneEngine, EngineSnapshot, Mutation, MutationEnvelope, MutationResponse, RuntimeError,
};
use selium_control_plane_scheduler::{
    SchedulePlan, ScheduledEndpointBridgeIntent, ScheduledInstance, build_endpoint_bridge_intents,
    build_plan,
};
use selium_guest::{network, spawn, storage, time};
use selium_io_consensus::{
    AppendEntries, AppendEntriesResponse, ConsensusConfig, ConsensusError, LogEntry, RaftNode,
    RequestVote, RequestVoteResponse, TickAction,
};

mod module_spec;

pub mod api {
    pub use selium_control_plane_api::*;
}

pub mod runtime {
    pub use selium_control_plane_runtime::*;
}

pub mod scheduler {
    pub use selium_control_plane_scheduler::*;
}

pub use module_spec::{build_module_spec, default_capabilities, deployment_module_spec};

/// Well-known module identifier used by the host when wiring system workloads.
pub const MODULE_ID: &str = "system/control-plane.wasm";
/// Default entrypoint function expected by the runtime.
pub const ENTRYPOINT: &str = "start";
/// Runtime-managed binding granted to the system control-plane service.
pub const INTERNAL_BINDING_NAME: &str = "system-control-plane-internal";
/// Runtime-managed egress profile used for daemon and peer RPCs.
pub const PEER_PROFILE_NAME: &str = "system-control-plane-peers";
/// Runtime-managed durable log used for committed event replay.
pub const EVENT_LOG_NAME: &str = "system-control-plane-events";
/// Runtime-managed blob store used for raft and engine snapshots.
pub const SNAPSHOT_BLOB_STORE_NAME: &str = "system-control-plane-snapshots";
const RAFT_MANIFEST: &str = "raft";
const ENGINE_MANIFEST: &str = "engine";
const TICK_INTERVAL_MS: u64 = 150;
const RECONCILE_INTERVAL_MS: u64 = 1_000;
const STREAM_TIMEOUT_MS: u32 = 5_000;

/// Minimal bootstrap output for host-side diagnostics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapReport {
    pub module_id: &'static str,
    pub entrypoint: &'static str,
    pub managed_capabilities: &'static [&'static str],
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct AgentState {
    pub running_instances: BTreeMap<String, String>,
    pub active_bridges: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum ReconcileAction {
    Start {
        instance_id: String,
        deployment: String,
        managed_endpoint_bindings: Vec<ManagedEndpointBinding>,
    },
    Stop {
        instance_id: String,
    },
    EnsureEndpointBridge {
        bridge_id: String,
        source_instance_id: String,
        source_endpoint: PublicEndpointRef,
        target_instance_id: String,
        target_node: String,
        target_endpoint: PublicEndpointRef,
    },
    RemoveEndpointBridge {
        bridge_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct PeerTarget {
    pub node_id: String,
    pub daemon_addr: String,
    pub daemon_server_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ControlPlaneModuleConfig {
    pub node_id: String,
    pub public_daemon_addr: String,
    pub public_daemon_server_name: String,
    pub capacity_slots: u32,
    pub heartbeat_interval_ms: u64,
    pub bootstrap_leader: bool,
    pub peers: Vec<PeerTarget>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
struct DurableCommittedEntry {
    idempotency_key: String,
    index: u64,
    term: u64,
    payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
struct PersistedRaft {
    raft: RaftNode,
}

struct ControlPlaneRuntimeState {
    config: ControlPlaneModuleConfig,
    event_log: storage::Log,
    snapshots: storage::BlobStore,
    raft: RaftNode,
    engine: ControlPlaneEngine,
    dedupe: BTreeMap<String, MutationResponse>,
    next_request_id: u64,
}

type SharedState = Rc<RefCell<ControlPlaneRuntimeState>>;

pub fn reconcile(
    node_name: &str,
    state: &ControlPlaneState,
    desired: &SchedulePlan,
    current: &AgentState,
) -> Vec<ReconcileAction> {
    let desired_on_node = desired
        .instances
        .iter()
        .filter(|instance| instance.node == node_name)
        .collect::<Vec<&ScheduledInstance>>();

    let desired_ids = desired_on_node
        .iter()
        .map(|instance| instance.instance_id.clone())
        .collect::<BTreeSet<_>>();

    let mut actions = Vec::new();
    let all_bridges = build_endpoint_bridge_intents(state, desired);

    for instance in desired_on_node {
        if !current
            .running_instances
            .contains_key(&instance.instance_id)
        {
            actions.push(ReconcileAction::Start {
                instance_id: instance.instance_id.clone(),
                deployment: instance.deployment.clone(),
                managed_endpoint_bindings: managed_endpoint_bindings_for_instance(
                    &instance.instance_id,
                    &all_bridges,
                ),
            });
        }
    }

    for instance_id in current.running_instances.keys() {
        if !desired_ids.contains(instance_id) {
            actions.push(ReconcileAction::Stop {
                instance_id: instance_id.clone(),
            });
        }
    }

    let desired_bridges = all_bridges
        .into_iter()
        .filter(|bridge| bridge.source_node == node_name)
        .collect::<Vec<_>>();
    let desired_bridge_ids = desired_bridges
        .iter()
        .map(|bridge| bridge.bridge_id.clone())
        .collect::<BTreeSet<_>>();

    for bridge in desired_bridges {
        if !current.active_bridges.contains(&bridge.bridge_id) {
            actions.push(ReconcileAction::EnsureEndpointBridge {
                bridge_id: bridge.bridge_id,
                source_instance_id: bridge.source_instance_id,
                source_endpoint: bridge.source_endpoint,
                target_instance_id: bridge.target_instance_id,
                target_node: bridge.target_node,
                target_endpoint: bridge.target_endpoint,
            });
        }
    }

    for bridge_id in &current.active_bridges {
        if !desired_bridge_ids.contains(bridge_id) {
            actions.push(ReconcileAction::RemoveEndpointBridge {
                bridge_id: bridge_id.clone(),
            });
        }
    }

    actions.sort_by(|lhs, rhs| {
        action_order(lhs)
            .cmp(&action_order(rhs))
            .then_with(|| format!("{lhs:?}").cmp(&format!("{rhs:?}")))
    });
    actions
}

fn action_order(action: &ReconcileAction) -> u8 {
    match action {
        ReconcileAction::RemoveEndpointBridge { .. } => 0,
        ReconcileAction::Stop { .. } => 1,
        ReconcileAction::Start { .. } => 2,
        ReconcileAction::EnsureEndpointBridge { .. } => 3,
    }
}

pub fn apply(current: &mut AgentState, actions: &[ReconcileAction]) {
    for action in actions {
        match action {
            ReconcileAction::Start {
                instance_id,
                deployment,
                managed_endpoint_bindings: _,
            } => {
                current
                    .running_instances
                    .insert(instance_id.clone(), deployment.clone());
            }
            ReconcileAction::Stop { instance_id } => {
                current.running_instances.remove(instance_id);
            }
            ReconcileAction::EnsureEndpointBridge { bridge_id, .. } => {
                current.active_bridges.insert(bridge_id.clone());
            }
            ReconcileAction::RemoveEndpointBridge { bridge_id } => {
                current.active_bridges.remove(bridge_id);
            }
        }
    }
}

fn managed_endpoint_bindings_for_instance(
    instance_id: &str,
    bridges: &[ScheduledEndpointBridgeIntent],
) -> Vec<ManagedEndpointBinding> {
    let mut bindings = BTreeMap::<(u8, ContractKind, String), ManagedEndpointBinding>::new();
    for bridge in bridges {
        if bridge.source_instance_id == instance_id {
            bindings
                .entry((
                    0,
                    bridge.source_endpoint.kind,
                    bridge.source_endpoint.name.clone(),
                ))
                .or_insert_with(|| ManagedEndpointBinding {
                    endpoint_name: bridge.source_endpoint.name.clone(),
                    endpoint_kind: bridge.source_endpoint.kind,
                    role: ManagedEndpointRole::Egress,
                    binding_type: binding_type_for_kind(bridge.source_endpoint.kind),
                });
        }
        if bridge.target_instance_id == instance_id {
            bindings
                .entry((
                    1,
                    bridge.target_endpoint.kind,
                    bridge.target_endpoint.name.clone(),
                ))
                .or_insert_with(|| ManagedEndpointBinding {
                    endpoint_name: bridge.target_endpoint.name.clone(),
                    endpoint_kind: bridge.target_endpoint.kind,
                    role: ManagedEndpointRole::Ingress,
                    binding_type: binding_type_for_kind(bridge.target_endpoint.kind),
                });
        }
    }
    bindings.into_values().collect()
}

fn binding_type_for_kind(kind: ContractKind) -> ManagedEndpointBindingType {
    match kind {
        ContractKind::Event => ManagedEndpointBindingType::OneWay,
        ContractKind::Service => ManagedEndpointBindingType::RequestResponse,
        ContractKind::Stream => ManagedEndpointBindingType::Session,
    }
}

fn endpoint_bridge_semantics(kind: ContractKind) -> EndpointBridgeSemantics {
    match kind {
        ContractKind::Event => EndpointBridgeSemantics::Event(EventBridgeSemantics {
            delivery: EventDeliveryMode::Frame,
        }),
        ContractKind::Service => EndpointBridgeSemantics::Service(ServiceBridgeSemantics {
            correlation: ServiceCorrelationMode::RequestId,
        }),
        ContractKind::Stream => EndpointBridgeSemantics::Stream(StreamBridgeSemantics {
            lifecycle: StreamLifecycleMode::SessionFrames,
        }),
    }
}

#[selium_guest::entrypoint]
pub async fn start(config: ControlPlaneModuleConfig) -> Result<()> {
    let state = Rc::new(RefCell::new(recover_state(config).await?));
    let listener = network::listen(INTERNAL_BINDING_NAME)
        .await
        .context("listen for internal control-plane proxy")?;

    spawn(run_server(listener, Rc::clone(&state)));
    spawn(run_tick_loop(Rc::clone(&state)));
    spawn(run_heartbeat_loop(Rc::clone(&state)));
    spawn(run_reconcile_loop(state));

    loop {
        time::sleep(Duration::from_secs(60))
            .await
            .context("sleep while control-plane service is idle")?;
    }
}

async fn recover_state(config: ControlPlaneModuleConfig) -> Result<ControlPlaneRuntimeState> {
    let event_log = storage::open_log(EVENT_LOG_NAME)
        .await
        .context("open control-plane event log")?;
    let snapshots = storage::open_blob_store(SNAPSHOT_BLOB_STORE_NAME)
        .await
        .context("open control-plane snapshot store")?;

    let raft = match snapshots.manifest(RAFT_MANIFEST).await? {
        Some(blob_id) => {
            let bytes = snapshots
                .get(blob_id)
                .await?
                .ok_or_else(|| anyhow!("missing persisted raft blob"))?;
            decode_rkyv::<PersistedRaft>(&bytes)
                .context("decode persisted raft")?
                .raft
        }
        None => {
            let peers = config
                .peers
                .iter()
                .map(|peer| peer.node_id.clone())
                .collect();
            let now = time::now().await.context("read host time")?.unix_ms;
            let mut raft = RaftNode::new(
                ConsensusConfig::default_for(config.node_id.clone(), peers),
                now,
            );
            if config.bootstrap_leader || config.peers.is_empty() {
                raft.bootstrap_as_leader();
            }
            raft
        }
    };

    let engine = match snapshots.manifest(ENGINE_MANIFEST).await? {
        Some(blob_id) => {
            let bytes = snapshots
                .get(blob_id)
                .await?
                .ok_or_else(|| anyhow!("missing persisted engine snapshot"))?;
            let snapshot =
                decode_rkyv::<EngineSnapshot>(&bytes).context("decode engine snapshot")?;
            ControlPlaneEngine::from_snapshot(snapshot)
        }
        None => ControlPlaneEngine::default(),
    };

    let mut state = ControlPlaneRuntimeState {
        config,
        event_log,
        snapshots,
        raft,
        engine,
        dedupe: BTreeMap::new(),
        next_request_id: 1,
    };
    replay_committed_events(&mut state).await?;
    persist_state(&state).await?;
    Ok(state)
}

async fn replay_committed_events(state: &mut ControlPlaneRuntimeState) -> Result<()> {
    let mut next_sequence = state.engine.last_applied().saturating_add(1);
    loop {
        let (records, _) = state
            .event_log
            .replay(StorageReplayStart::Sequence(next_sequence), 128)
            .await
            .context("replay durable control-plane events")?;
        if records.is_empty() {
            break;
        }

        for record in records {
            let committed = decode_rkyv::<DurableCommittedEntry>(&record.payload)
                .context("decode durable committed entry")?;
            let envelope = ControlPlaneEngine::decode_mutation(&committed.payload)
                .map_err(anyhow_from_runtime)?;
            let response = state
                .engine
                .apply_committed_entry(&LogEntry {
                    index: committed.index,
                    term: committed.term,
                    payload: committed.payload.clone(),
                })
                .map_err(anyhow_from_runtime)?;
            state
                .dedupe
                .insert(envelope.idempotency_key.clone(), response.clone());
            next_sequence = committed.index.saturating_add(1);
        }
    }

    Ok(())
}

async fn run_server(listener: network::Listener, state: SharedState) {
    loop {
        match listener.accept(STREAM_TIMEOUT_MS).await {
            Ok(Some(session)) => {
                spawn(run_session(session, Rc::clone(&state)));
            }
            Ok(None) => {
                let _ = time::sleep(Duration::from_millis(50)).await;
            }
            Err(err) => {
                tracing::warn!("control-plane accept error: {err}");
                let _ = time::sleep(Duration::from_millis(250)).await;
            }
        }
    }
}

async fn run_session(session: network::Session, state: SharedState) {
    loop {
        match network::stream::accept(&session, STREAM_TIMEOUT_MS).await {
            Ok(Some(stream)) => {
                spawn(handle_stream(stream, Rc::clone(&state)));
            }
            Ok(None) => {
                let _ = time::sleep(Duration::from_millis(50)).await;
            }
            Err(_) => break,
        }
    }
}

async fn handle_stream(stream: network::StreamChannel, state: SharedState) {
    let response = match read_framed_stream(&stream).await {
        Ok(frame) => handle_request_frame(frame, state).await,
        Err(err) => encode_error_response(Method::ControlQuery, 0, 500, err.to_string(), false),
    };

    if let Ok(frame) = response {
        let _ = write_framed_stream(&stream, &frame).await;
    }
    let _ = stream.close().await;
}

async fn handle_request_frame(frame: Vec<u8>, state: SharedState) -> Result<Vec<u8>> {
    let envelope = decode_envelope(&frame).context("decode request envelope")?;
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
            let payload: MutateApiRequest = decode_payload(&envelope).context("decode mutate")?;
            let response = handle_mutate(Rc::clone(&state), payload).await?;
            encode_response(envelope.method, envelope.request_id, &response)
        }
        Method::ControlQuery => {
            let payload: QueryApiRequest = decode_payload(&envelope).context("decode query")?;
            let response = handle_query(Rc::clone(&state), payload).await?;
            encode_response(envelope.method, envelope.request_id, &response)
        }
        Method::ControlStatus => {
            let response = handle_status(Rc::clone(&state)).await?;
            encode_response(envelope.method, envelope.request_id, &response)
        }
        Method::ControlReplay => {
            let payload: selium_control_plane_protocol::ReplayApiRequest =
                decode_payload(&envelope).context("decode replay")?;
            let response = handle_replay(Rc::clone(&state), payload.limit).await?;
            encode_response(envelope.method, envelope.request_id, &response)
        }
        Method::RaftRequestVote => {
            let payload: RequestVoteApiRequest =
                decode_payload(&envelope).context("decode request vote")?;
            let response = handle_request_vote(Rc::clone(&state), payload).await?;
            encode_response(envelope.method, envelope.request_id, &response)
        }
        Method::RaftAppendEntries => {
            let payload: AppendEntriesApiRequest =
                decode_payload(&envelope).context("decode append entries")?;
            let response = handle_append_entries(Rc::clone(&state), payload).await?;
            encode_response(envelope.method, envelope.request_id, &response)
        }
        _ => encode_error_response(
            envelope.method,
            envelope.request_id,
            404,
            "unsupported control-plane method",
            false,
        ),
    }
}

async fn handle_mutate(state: SharedState, request: MutateApiRequest) -> Result<MutateApiResponse> {
    {
        let borrowed = state.borrow();
        if let Some(existing) = borrowed.dedupe.get(&request.idempotency_key) {
            return Ok(MutateApiResponse {
                committed: true,
                index: Some(existing.index),
                leader_hint: borrowed.raft.leader_hint(),
                result: Some(existing.result.clone()),
                error: None,
            });
        }
    }

    let envelope = MutationEnvelope {
        idempotency_key: request.idempotency_key.clone(),
        mutation: request.mutation.clone(),
    };
    let payload = ControlPlaneEngine::encode_mutation(&envelope).map_err(anyhow_from_runtime)?;

    let proposal = {
        let mut borrowed = state.borrow_mut();
        match borrowed.raft.propose(payload) {
            Ok(entry) => {
                let requests = borrowed.raft.build_append_entries_requests();
                if requests.is_empty() {
                    borrowed.raft.force_commit_to(entry.index);
                }
                Ok((entry.index, requests, borrowed.raft.leader_hint()))
            }
            Err(ConsensusError::NotLeader) => Err(borrowed.raft.leader_hint()),
        }
    };

    let (entry_index, append_requests, leader_hint) = match proposal {
        Ok(proposal) => proposal,
        Err(leader_hint) => {
            if let Some(leader_id) = leader_hint.as_deref()
                && let Some(forwarded) =
                    forward_mutation_to_leader(Rc::clone(&state), leader_id, request.clone())
                        .await?
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

    for (peer_id, append) in append_requests {
        if let Some(response) = send_append_entries(Rc::clone(&state), &peer_id, append).await? {
            state
                .borrow_mut()
                .raft
                .handle_append_entries_response(&peer_id, response);
        }
    }

    let applied = apply_committed(Rc::clone(&state)).await?;
    persist_state_shared(Rc::clone(&state)).await?;

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

async fn forward_mutation_to_leader(
    state: SharedState,
    leader_id: &str,
    request: MutateApiRequest,
) -> Result<Option<MutateApiResponse>> {
    let authority = {
        let borrowed = state.borrow();
        authority_for_node(&borrowed, leader_id)?
    };
    let Some(authority) = authority else {
        return Ok(None);
    };
    send_daemon_rpc(&state, &authority, Method::ControlMutate, &request).await
}

async fn handle_query(state: SharedState, request: QueryApiRequest) -> Result<QueryApiResponse> {
    let (is_leader, leader_hint) = {
        let borrowed = state.borrow();
        (borrowed.raft.is_leader(), borrowed.raft.leader_hint())
    };
    if !request.allow_stale && !is_leader {
        let authority = if let Some(leader_id) = leader_hint.as_deref() {
            let borrowed = state.borrow();
            authority_for_node(&borrowed, leader_id)?
        } else {
            None
        };
        if let Some(authority) = authority
            && let Some(forwarded) =
                send_daemon_rpc(&state, &authority, Method::ControlQuery, &request).await?
        {
            return Ok(forwarded);
        }

        return Ok(QueryApiResponse {
            leader_hint,
            result: None,
            error: Some("not leader".to_string()),
        });
    }

    let borrowed = state.borrow();
    let response = borrowed
        .engine
        .query(request.query)
        .map_err(anyhow_from_runtime)?;
    Ok(QueryApiResponse {
        leader_hint: borrowed.raft.leader_hint(),
        result: Some(response.result),
        error: None,
    })
}

async fn handle_status(state: SharedState) -> Result<StatusApiResponse> {
    let (status, peers, table_count, event_log) = {
        let borrowed = state.borrow();
        (
            borrowed.raft.status(),
            borrowed
                .config
                .peers
                .iter()
                .map(|peer| peer.node_id.clone())
                .collect::<Vec<_>>(),
            borrowed.engine.snapshot().tables.tables().len(),
            borrowed.event_log,
        )
    };
    Ok(StatusApiResponse {
        node_id: status.node_id,
        role: format!("{:?}", status.role),
        current_term: status.current_term,
        leader_id: status.leader_id,
        commit_index: status.commit_index,
        last_applied: status.last_applied,
        peers,
        table_count,
        durable_events: event_log.bounds().await?.latest_sequence,
    })
}

async fn handle_replay(state: SharedState, limit: usize) -> Result<ReplayApiResponse> {
    let event_log = state.borrow().event_log;
    let (records, _) = event_log
        .replay(StorageReplayStart::Latest, limit as u32)
        .await?;
    let events = records
        .into_iter()
        .map(|record| {
            decode_rkyv::<DurableCommittedEntry>(&record.payload)
                .map(|entry| {
                    DataValue::Map(BTreeMap::from([
                        ("idempotency_key".to_string(), entry.idempotency_key.into()),
                        ("index".to_string(), entry.index.into()),
                        ("term".to_string(), entry.term.into()),
                    ]))
                })
                .unwrap_or_else(|_| DataValue::Bytes(record.payload))
        })
        .collect();
    Ok(ReplayApiResponse { events })
}

async fn handle_request_vote(
    state: SharedState,
    request: RequestVoteApiRequest,
) -> Result<RequestVoteResponse> {
    let now = time::now().await?.unix_ms;
    let response = state
        .borrow_mut()
        .raft
        .handle_request_vote(request.request, now);
    persist_state_shared(Rc::clone(&state)).await?;
    Ok(response)
}

async fn handle_append_entries(
    state: SharedState,
    request: AppendEntriesApiRequest,
) -> Result<AppendEntriesResponse> {
    let now = time::now().await?.unix_ms;
    let response = state
        .borrow_mut()
        .raft
        .handle_append_entries(request.request, now);
    let _ = apply_committed(Rc::clone(&state)).await?;
    persist_state_shared(Rc::clone(&state)).await?;
    Ok(response)
}

async fn run_tick_loop(state: SharedState) {
    loop {
        if let Err(err) = tick_once(Rc::clone(&state)).await {
            tracing::warn!("control-plane tick error: {err:#}");
        }
        let _ = time::sleep(Duration::from_millis(TICK_INTERVAL_MS)).await;
    }
}

async fn tick_once(state: SharedState) -> Result<()> {
    let now = time::now().await?.unix_ms;
    let action = state.borrow_mut().raft.tick(now);
    let Some(action) = action else {
        return Ok(());
    };

    match action {
        TickAction::RequestVotes(requests) => {
            for (peer_id, request_vote) in requests {
                if let Some(response) =
                    send_request_vote(Rc::clone(&state), &peer_id, request_vote).await?
                {
                    let follow_up = state
                        .borrow_mut()
                        .raft
                        .handle_request_vote_response(&peer_id, response);
                    if let Some(TickAction::AppendEntries(heartbeats)) = follow_up {
                        for (peer, heartbeat) in heartbeats {
                            if let Some(response) =
                                send_append_entries(Rc::clone(&state), &peer, heartbeat).await?
                            {
                                state
                                    .borrow_mut()
                                    .raft
                                    .handle_append_entries_response(&peer, response);
                            }
                        }
                    }
                }
            }
        }
        TickAction::AppendEntries(requests) => {
            for (peer_id, append) in requests {
                if let Some(response) =
                    send_append_entries(Rc::clone(&state), &peer_id, append).await?
                {
                    state
                        .borrow_mut()
                        .raft
                        .handle_append_entries_response(&peer_id, response);
                }
            }
        }
    }

    let _ = apply_committed(Rc::clone(&state)).await?;
    persist_state_shared(Rc::clone(&state)).await?;
    Ok(())
}

async fn run_heartbeat_loop(state: SharedState) {
    loop {
        if let Err(err) = publish_heartbeat(Rc::clone(&state)).await {
            tracing::warn!("control-plane heartbeat error: {err:#}");
        }
        let interval = state.borrow().config.heartbeat_interval_ms.max(250);
        let _ = time::sleep(Duration::from_millis(interval)).await;
    }
}

async fn publish_heartbeat(state: SharedState) -> Result<()> {
    let now = time::now().await?.unix_ms;
    let request = MutateApiRequest {
        idempotency_key: format!("node-heartbeat:{}:{now}", state.borrow().config.node_id),
        mutation: Mutation::UpsertNode {
            spec: NodeSpec {
                name: state.borrow().config.node_id.clone(),
                capacity_slots: state.borrow().config.capacity_slots,
                supported_isolation: vec![
                    IsolationProfile::Standard,
                    IsolationProfile::Hardened,
                    IsolationProfile::Microvm,
                ],
                daemon_addr: state.borrow().config.public_daemon_addr.clone(),
                daemon_server_name: state.borrow().config.public_daemon_server_name.clone(),
                last_heartbeat_ms: now,
            },
        },
    };
    let _ = handle_mutate(state, request).await?;
    Ok(())
}

async fn run_reconcile_loop(state: SharedState) {
    loop {
        if let Err(err) = reconcile_once(Rc::clone(&state)).await {
            tracing::warn!("control-plane reconcile error: {err:#}");
        }
        let _ = time::sleep(Duration::from_millis(RECONCILE_INTERVAL_MS)).await;
    }
}

async fn reconcile_once(state: SharedState) -> Result<()> {
    if !state.borrow().raft.is_leader() {
        return Ok(());
    }

    let snapshot = state.borrow().engine.snapshot().control_plane;
    let desired = build_plan(&snapshot).map_err(|err| anyhow!("build schedule plan: {err}"))?;

    for (node, spec) in &snapshot.nodes {
        let authority = daemon_authority(&spec.daemon_addr, &spec.daemon_server_name);
        let list: ListResponse = send_daemon_rpc(
            &state,
            &authority,
            Method::ListInstances,
            &selium_control_plane_protocol::Empty {},
        )
        .await?
        .unwrap_or(ListResponse {
            instances: BTreeMap::new(),
            active_bridges: Vec::new(),
        });
        let current = AgentState {
            running_instances: list
                .instances
                .into_iter()
                .map(|(instance_id, process_id)| (instance_id, process_id.to_string()))
                .collect(),
            active_bridges: list.active_bridges.into_iter().collect(),
        };
        let actions = reconcile(node, &snapshot, &desired, &current);
        execute_daemon_actions(&state, &snapshot, &actions, node).await?;
    }

    Ok(())
}

async fn execute_daemon_actions(
    state: &SharedState,
    cp_state: &ControlPlaneState,
    actions: &[ReconcileAction],
    node: &str,
) -> Result<()> {
    let node_spec = cp_state
        .nodes
        .get(node)
        .ok_or_else(|| anyhow!("unknown node `{node}`"))?;
    let authority = daemon_authority(&node_spec.daemon_addr, &node_spec.daemon_server_name);

    for action in actions {
        match action {
            ReconcileAction::Start {
                instance_id,
                deployment,
                managed_endpoint_bindings,
            } => {
                let spec = cp_state
                    .deployments
                    .get(deployment)
                    .ok_or_else(|| anyhow!("missing deployment `{deployment}`"))?;
                let module_spec = deployment_module_spec(spec);
                let _: selium_control_plane_protocol::StartResponse = send_daemon_rpc(
                    state,
                    &authority,
                    Method::StartInstance,
                    &StartRequest {
                        node_id: node.to_string(),
                        instance_id: instance_id.clone(),
                        module_spec,
                        managed_endpoint_bindings: managed_endpoint_bindings.clone(),
                    },
                )
                .await?
                .ok_or_else(|| anyhow!("missing start response"))?;
            }
            ReconcileAction::Stop { instance_id } => {
                let _: selium_control_plane_protocol::StopResponse = send_daemon_rpc(
                    state,
                    &authority,
                    Method::StopInstance,
                    &StopRequest {
                        node_id: node.to_string(),
                        instance_id: instance_id.clone(),
                    },
                )
                .await?
                .ok_or_else(|| anyhow!("missing stop response"))?;
            }
            ReconcileAction::EnsureEndpointBridge {
                bridge_id,
                source_instance_id,
                source_endpoint,
                target_instance_id,
                target_node,
                target_endpoint,
            } => {
                let target_spec = cp_state
                    .nodes
                    .get(target_node)
                    .ok_or_else(|| anyhow!("unknown target node `{target_node}`"))?
                    .clone();
                let _: ActivateEndpointBridgeResponse = send_daemon_rpc(
                    state,
                    &authority,
                    Method::ActivateEndpointBridge,
                    &ActivateEndpointBridgeRequest {
                        node_id: node.to_string(),
                        bridge_id: bridge_id.clone(),
                        source_instance_id: source_instance_id.clone(),
                        source_endpoint: source_endpoint.clone(),
                        target_instance_id: target_instance_id.clone(),
                        target_node: target_node.clone(),
                        target_daemon_addr: target_spec.daemon_addr,
                        target_daemon_server_name: target_spec.daemon_server_name,
                        target_endpoint: target_endpoint.clone(),
                        semantics: endpoint_bridge_semantics(source_endpoint.kind),
                    },
                )
                .await?
                .ok_or_else(|| anyhow!("missing activate route response"))?;
            }
            ReconcileAction::RemoveEndpointBridge { bridge_id } => {
                let _: DeactivateEndpointBridgeResponse = send_daemon_rpc(
                    state,
                    &authority,
                    Method::DeactivateEndpointBridge,
                    &DeactivateEndpointBridgeRequest {
                        node_id: node.to_string(),
                        bridge_id: bridge_id.clone(),
                    },
                )
                .await?
                .ok_or_else(|| anyhow!("missing deactivate route response"))?;
            }
        }
    }

    Ok(())
}

async fn apply_committed(state: SharedState) -> Result<BTreeMap<u64, MutationResponse>> {
    let entries = state.borrow_mut().raft.take_committed_entries();
    let mut applied = BTreeMap::new();

    for entry in entries {
        let envelope =
            ControlPlaneEngine::decode_mutation(&entry.payload).map_err(anyhow_from_runtime)?;
        let response = state
            .borrow_mut()
            .engine
            .apply_committed_entry(&entry)
            .map_err(anyhow_from_runtime)?;

        state
            .borrow_mut()
            .dedupe
            .insert(envelope.idempotency_key.clone(), response.clone());
        let event_log = state.borrow().event_log;
        event_log
            .append(
                time::now().await?.unix_ms,
                BTreeMap::from([("source".to_string(), "control-plane".to_string())]),
                encode_rkyv(&DurableCommittedEntry {
                    idempotency_key: envelope.idempotency_key,
                    index: entry.index,
                    term: entry.term,
                    payload: entry.payload.clone(),
                })
                .context("encode durable committed entry")?,
            )
            .await
            .context("append durable event")?;
        applied.insert(entry.index, response);
    }

    Ok(applied)
}

async fn persist_state(state: &ControlPlaneRuntimeState) -> Result<()> {
    let raft_blob = state
        .snapshots
        .put(encode_rkyv(&PersistedRaft {
            raft: state.raft.clone(),
        })?)
        .await
        .context("write raft snapshot blob")?;
    state
        .snapshots
        .set_manifest(RAFT_MANIFEST, raft_blob)
        .await
        .context("publish raft snapshot manifest")?;

    let engine_blob = state
        .snapshots
        .put(encode_rkyv(&state.engine.snapshot())?)
        .await
        .context("write engine snapshot blob")?;
    state
        .snapshots
        .set_manifest(ENGINE_MANIFEST, engine_blob)
        .await
        .context("publish engine snapshot manifest")?;

    Ok(())
}

async fn persist_state_shared(state: SharedState) -> Result<()> {
    let (snapshots, raft, engine_snapshot) = {
        let borrowed = state.borrow();
        (
            borrowed.snapshots,
            borrowed.raft.clone(),
            borrowed.engine.snapshot(),
        )
    };

    let raft_blob = snapshots
        .put(encode_rkyv(&PersistedRaft { raft })?)
        .await
        .context("write raft snapshot blob")?;
    snapshots
        .set_manifest(RAFT_MANIFEST, raft_blob)
        .await
        .context("publish raft snapshot manifest")?;

    let engine_blob = snapshots
        .put(encode_rkyv(&engine_snapshot)?)
        .await
        .context("write engine snapshot blob")?;
    snapshots
        .set_manifest(ENGINE_MANIFEST, engine_blob)
        .await
        .context("publish engine snapshot manifest")?;

    Ok(())
}

async fn send_request_vote(
    state: SharedState,
    peer_id: &str,
    request_vote: RequestVote,
) -> Result<Option<RequestVoteResponse>> {
    let authority = authority_for_node(&state.borrow(), peer_id)?;
    match authority {
        Some(authority) => {
            send_daemon_rpc(
                &state,
                &authority,
                Method::RaftRequestVote,
                &RequestVoteApiRequest {
                    request: request_vote,
                },
            )
            .await
        }
        None => Ok(None),
    }
}

async fn send_append_entries(
    state: SharedState,
    peer_id: &str,
    append: AppendEntries,
) -> Result<Option<AppendEntriesResponse>> {
    let authority = authority_for_node(&state.borrow(), peer_id)?;
    match authority {
        Some(authority) => {
            send_daemon_rpc(
                &state,
                &authority,
                Method::RaftAppendEntries,
                &AppendEntriesApiRequest { request: append },
            )
            .await
        }
        None => Ok(None),
    }
}

async fn send_daemon_rpc<Req, Resp>(
    state: &SharedState,
    authority: &str,
    method: Method,
    request: &Req,
) -> Result<Option<Resp>>
where
    Req: selium_abi::RkyvEncode,
    Resp: Archive + Sized,
    for<'a> Resp::Archived: rkyv::Deserialize<Resp, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>
        + rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
{
    let request_id = {
        let mut borrowed = state.borrow_mut();
        let request_id = borrowed.next_request_id;
        borrowed.next_request_id = borrowed.next_request_id.saturating_add(1);
        request_id
    };
    let frame = selium_control_plane_protocol::encode_request(method, request_id, request)
        .context("encode daemon request")?;
    let response = send_raw_frame(authority, frame).await?;
    let envelope = decode_envelope(&response).context("decode daemon response")?;
    if envelope.method != method || envelope.request_id != request_id {
        return Err(anyhow!("daemon response mismatch"));
    }
    if is_error(&envelope) {
        let error = selium_control_plane_protocol::decode_error(&envelope)
            .context("decode daemon error")?;
        return Err(anyhow!("daemon error {}: {}", error.code, error.message));
    }
    Ok(Some(
        decode_payload::<Resp>(&envelope).context("decode daemon payload")?,
    ))
}

fn authority_for_node(state: &ControlPlaneRuntimeState, node_id: &str) -> Result<Option<String>> {
    if node_id == state.config.node_id {
        return Ok(Some(daemon_authority(
            &state.config.public_daemon_addr,
            &state.config.public_daemon_server_name,
        )));
    }
    if let Some(node) = state.engine.snapshot().control_plane.nodes.get(node_id) {
        return Ok(Some(daemon_authority(
            &node.daemon_addr,
            &node.daemon_server_name,
        )));
    }
    Ok(state
        .config
        .peers
        .iter()
        .find(|peer| peer.node_id == node_id)
        .map(|peer| daemon_authority(&peer.daemon_addr, &peer.daemon_server_name)))
}

fn daemon_authority(daemon_addr: &str, daemon_server_name: &str) -> String {
    format!("{daemon_addr}@{daemon_server_name}")
}

async fn send_raw_frame(authority: &str, frame: Vec<u8>) -> Result<Vec<u8>> {
    let session = network::quic::connect(PEER_PROFILE_NAME, authority)
        .await
        .with_context(|| format!("connect to daemon {authority}"))?;
    let stream = network::stream::open(&session)
        .await
        .context("open daemon stream")?
        .ok_or_else(|| anyhow!("daemon stream open would block"))?;
    write_framed_stream(&stream, &frame).await?;
    let response = read_framed_stream(&stream).await?;
    let _ = stream.close().await;
    let _ = session.close().await;
    Ok(response)
}

async fn write_framed_stream(stream: &network::StreamChannel, payload: &[u8]) -> Result<()> {
    let len: u32 = payload
        .len()
        .try_into()
        .map_err(|_| anyhow!("frame too large"))?;
    stream
        .send(len.to_be_bytes().to_vec(), false, STREAM_TIMEOUT_MS)
        .await
        .context("write frame length")?;
    stream
        .send(payload.to_vec(), true, STREAM_TIMEOUT_MS)
        .await
        .context("write frame payload")?;
    Ok(())
}

async fn read_framed_stream(stream: &network::StreamChannel) -> Result<Vec<u8>> {
    let mut buffer = Vec::new();
    loop {
        let chunk = stream
            .recv(4096, STREAM_TIMEOUT_MS)
            .await
            .context("receive frame chunk")?
            .ok_or_else(|| anyhow!("stream would block"))?;
        buffer.extend_from_slice(&chunk.bytes);
        if chunk.finish {
            break;
        }
    }
    if buffer.len() < 4 {
        return Err(anyhow!("frame too short"));
    }
    let len = u32::from_be_bytes(buffer[..4].try_into().expect("len header")) as usize;
    if buffer.len() != 4 + len {
        return Err(anyhow!(
            "invalid framed payload length {} != {}",
            buffer.len().saturating_sub(4),
            len
        ));
    }
    Ok(buffer[4..].to_vec())
}

fn anyhow_from_runtime(err: RuntimeError) -> anyhow::Error {
    anyhow!(err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use selium_control_plane_api::{
        ContractRef, DeploymentSpec, EventEndpointRef, PipelineEdge, PipelineEndpoint,
        PipelineSpec, WorkloadRef, parse_idl,
    };

    const SAMPLE_IDL: &str = r#"
package media.pipeline.v1;

schema Frame {
  camera_id: string;
}

event camera.frames(Frame) {
  partitions: 1;
  delivery: at_least_once;
}
"#;

    #[test]
    fn reconcile_generates_start_and_stop_actions() {
        let mut state = ControlPlaneState::new_local_default();
        state
            .upsert_deployment(DeploymentSpec {
                workload: selium_control_plane_api::WorkloadRef {
                    tenant: "tenant-a".to_string(),
                    namespace: "default".to_string(),
                    name: "echo".to_string(),
                },
                module: "echo.wasm".to_string(),
                replicas: 2,
                contracts: Vec::new(),
                isolation: IsolationProfile::Standard,
            })
            .expect("deployment");

        let desired = build_plan(&state).expect("schedule");
        let mut current = AgentState {
            running_instances: BTreeMap::from([("old-0".to_string(), "old".to_string())]),
            active_bridges: BTreeSet::new(),
        };

        let actions = reconcile("local-node", &state, &desired, &current);
        assert_eq!(actions.len(), 3);

        apply(&mut current, &actions);
        assert_eq!(current.running_instances.len(), 2);
        assert!(current.active_bridges.is_empty());
        assert!(
            current
                .running_instances
                .contains_key("tenant=tenant-a;namespace=default;workload=echo;replica=0")
        );
        assert!(
            current
                .running_instances
                .contains_key("tenant=tenant-a;namespace=default;workload=echo;replica=1")
        );
    }

    #[test]
    fn reconcile_generates_endpoint_bridge_actions() {
        let mut state = ControlPlaneState::new_local_default();
        state
            .registry
            .register_package(parse_idl(SAMPLE_IDL).expect("parse idl"))
            .expect("register package");

        let ingest = WorkloadRef {
            tenant: "tenant-a".to_string(),
            namespace: "media".to_string(),
            name: "ingest".to_string(),
        };
        let detector = WorkloadRef {
            tenant: "tenant-a".to_string(),
            namespace: "media".to_string(),
            name: "detector".to_string(),
        };
        let contract = ContractRef {
            namespace: "media.pipeline".to_string(),
            kind: selium_control_plane_api::ContractKind::Event,
            name: "camera.frames".to_string(),
            version: "v1".to_string(),
        };
        for workload in [ingest.clone(), detector.clone()] {
            state
                .upsert_deployment(DeploymentSpec {
                    workload,
                    module: "module.wasm".to_string(),
                    replicas: 1,
                    contracts: vec![contract.clone()],
                    isolation: IsolationProfile::Standard,
                })
                .expect("deployment");
        }
        state.upsert_pipeline(PipelineSpec {
            name: "camera".to_string(),
            tenant: "tenant-a".to_string(),
            namespace: "media".to_string(),
            edges: vec![PipelineEdge {
                from: PipelineEndpoint {
                    endpoint: EventEndpointRef {
                        workload: ingest,
                        name: "camera.frames".to_string(),
                    },
                    contract: contract.clone(),
                },
                to: PipelineEndpoint {
                    endpoint: EventEndpointRef {
                        workload: detector,
                        name: "camera.frames".to_string(),
                    },
                    contract,
                },
            }],
        });

        let desired = build_plan(&state).expect("schedule");
        let current = AgentState::default();
        let actions = reconcile("local-node", &state, &desired, &current);

        assert!(
            actions
                .iter()
                .any(|action| matches!(action, ReconcileAction::EnsureEndpointBridge { .. }))
        );
    }

    #[test]
    fn reconcile_orders_starts_before_endpoint_bridges() {
        let mut state = ControlPlaneState::new_local_default();
        state
            .registry
            .register_package(parse_idl(SAMPLE_IDL).expect("parse idl"))
            .expect("register package");

        let ingest = WorkloadRef {
            tenant: "tenant-a".to_string(),
            namespace: "media".to_string(),
            name: "ingest".to_string(),
        };
        let detector = WorkloadRef {
            tenant: "tenant-a".to_string(),
            namespace: "media".to_string(),
            name: "detector".to_string(),
        };
        let contract = ContractRef {
            namespace: "media.pipeline".to_string(),
            kind: selium_control_plane_api::ContractKind::Event,
            name: "camera.frames".to_string(),
            version: "v1".to_string(),
        };
        for workload in [ingest.clone(), detector.clone()] {
            state
                .upsert_deployment(DeploymentSpec {
                    workload,
                    module: "module.wasm".to_string(),
                    replicas: 1,
                    contracts: vec![contract.clone()],
                    isolation: IsolationProfile::Standard,
                })
                .expect("deployment");
        }
        state.upsert_pipeline(PipelineSpec {
            name: "camera".to_string(),
            tenant: "tenant-a".to_string(),
            namespace: "media".to_string(),
            edges: vec![PipelineEdge {
                from: PipelineEndpoint {
                    endpoint: EventEndpointRef {
                        workload: ingest,
                        name: "camera.frames".to_string(),
                    },
                    contract: contract.clone(),
                },
                to: PipelineEndpoint {
                    endpoint: EventEndpointRef {
                        workload: detector,
                        name: "camera.frames".to_string(),
                    },
                    contract,
                },
            }],
        });

        let desired = build_plan(&state).expect("schedule");
        let actions = reconcile("local-node", &state, &desired, &AgentState::default());
        let start_idx = actions
            .iter()
            .position(|action| matches!(action, ReconcileAction::Start { .. }))
            .expect("start action");
        let ensure_idx = actions
            .iter()
            .position(|action| matches!(action, ReconcileAction::EnsureEndpointBridge { .. }))
            .expect("ensure action");

        assert!(
            start_idx < ensure_idx,
            "expected start before ensure actions, got {actions:?}"
        );
    }

    #[test]
    fn managed_endpoint_bindings_keep_same_name_cross_kind_entries_distinct() {
        let bindings = managed_endpoint_bindings_for_instance(
            "instance-a",
            &[
                ScheduledEndpointBridgeIntent {
                    bridge_id: "bridge-event".to_string(),
                    source_instance_id: "instance-a".to_string(),
                    source_node: "local-node".to_string(),
                    source_endpoint: PublicEndpointRef {
                        workload: WorkloadRef {
                            tenant: "tenant-a".to_string(),
                            namespace: "media".to_string(),
                            name: "ingest".to_string(),
                        },
                        kind: ContractKind::Event,
                        name: "shared".to_string(),
                    },
                    target_instance_id: "instance-b".to_string(),
                    target_node: "local-node".to_string(),
                    target_endpoint: PublicEndpointRef {
                        workload: WorkloadRef {
                            tenant: "tenant-a".to_string(),
                            namespace: "media".to_string(),
                            name: "detector".to_string(),
                        },
                        kind: ContractKind::Event,
                        name: "shared".to_string(),
                    },
                    contract: ContractRef {
                        namespace: "media.pipeline".to_string(),
                        kind: ContractKind::Event,
                        name: "shared".to_string(),
                        version: "v1".to_string(),
                    },
                },
                ScheduledEndpointBridgeIntent {
                    bridge_id: "bridge-service".to_string(),
                    source_instance_id: "instance-a".to_string(),
                    source_node: "local-node".to_string(),
                    source_endpoint: PublicEndpointRef {
                        workload: WorkloadRef {
                            tenant: "tenant-a".to_string(),
                            namespace: "media".to_string(),
                            name: "ingest".to_string(),
                        },
                        kind: ContractKind::Service,
                        name: "shared".to_string(),
                    },
                    target_instance_id: "instance-c".to_string(),
                    target_node: "remote-node".to_string(),
                    target_endpoint: PublicEndpointRef {
                        workload: WorkloadRef {
                            tenant: "tenant-a".to_string(),
                            namespace: "media".to_string(),
                            name: "uploader".to_string(),
                        },
                        kind: ContractKind::Service,
                        name: "shared".to_string(),
                    },
                    contract: ContractRef {
                        namespace: "media.pipeline".to_string(),
                        kind: ContractKind::Service,
                        name: "shared".to_string(),
                        version: "v1".to_string(),
                    },
                },
                ScheduledEndpointBridgeIntent {
                    bridge_id: "bridge-stream".to_string(),
                    source_instance_id: "instance-a".to_string(),
                    source_node: "local-node".to_string(),
                    source_endpoint: PublicEndpointRef {
                        workload: WorkloadRef {
                            tenant: "tenant-a".to_string(),
                            namespace: "media".to_string(),
                            name: "ingest".to_string(),
                        },
                        kind: ContractKind::Stream,
                        name: "shared".to_string(),
                    },
                    target_instance_id: "instance-d".to_string(),
                    target_node: "local-node".to_string(),
                    target_endpoint: PublicEndpointRef {
                        workload: WorkloadRef {
                            tenant: "tenant-a".to_string(),
                            namespace: "media".to_string(),
                            name: "streamer".to_string(),
                        },
                        kind: ContractKind::Stream,
                        name: "shared".to_string(),
                    },
                    contract: ContractRef {
                        namespace: "media.pipeline".to_string(),
                        kind: ContractKind::Stream,
                        name: "shared".to_string(),
                        version: "v1".to_string(),
                    },
                },
            ],
        );

        assert_eq!(bindings.len(), 3);
        assert!(bindings.iter().any(|binding| {
            binding.endpoint_kind == ContractKind::Event
                && binding.binding_type == ManagedEndpointBindingType::OneWay
        }));
        assert!(bindings.iter().any(|binding| {
            binding.endpoint_kind == ContractKind::Service
                && binding.binding_type == ManagedEndpointBindingType::RequestResponse
        }));
        assert!(bindings.iter().any(|binding| {
            binding.endpoint_kind == ContractKind::Stream
                && binding.binding_type == ManagedEndpointBindingType::Session
        }));
    }
}
