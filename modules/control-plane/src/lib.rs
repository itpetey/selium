//! System control-plane module bootstrap helpers and guest control-plane loop.

use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    rc::Rc,
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use rkyv::{Archive, Deserialize, Serialize};
use selium_abi::{
    DataValue, StorageLogBoundsResult, StorageLogRecord, StorageReplayStart, decode_rkyv,
    encode_rkyv,
};
use selium_control_plane_api::{
    ContractKind, ControlPlaneState, DeploymentSpec, IsolationProfile, NodeSpec, PublicEndpointRef,
};
use selium_control_plane_protocol::{
    ActivateEndpointBridgeRequest, ActivateEndpointBridgeResponse, AppendEntriesApiRequest,
    DeactivateEndpointBridgeRequest, DeactivateEndpointBridgeResponse, EndpointBridgeSemantics,
    EventBridgeSemantics, EventDeliveryMode, ListResponse, ManagedEndpointBinding,
    ManagedEndpointBindingType, ManagedEndpointRole, Method, MetricsApiResponse, MutateApiRequest,
    MutateApiResponse, QueryApiRequest, QueryApiResponse, ReplayApiResponse, RequestVoteApiRequest,
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
    pub allocatable_cpu_millis: Option<u32>,
    pub allocatable_memory_mib: Option<u32>,
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
        Method::ControlMetrics => {
            let response = handle_metrics(Rc::clone(&state)).await?;
            encode_response(envelope.method, envelope.request_id, &response)
        }
        Method::ControlReplay => {
            let payload: selium_control_plane_protocol::ReplayApiRequest =
                decode_payload(&envelope).context("decode replay")?;
            let response = handle_replay(Rc::clone(&state), payload).await?;
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

async fn handle_metrics(state: SharedState) -> Result<MetricsApiResponse> {
    let (status, peer_count, snapshot, event_log) = {
        let borrowed = state.borrow();
        (
            borrowed.raft.status(),
            borrowed.config.peers.len(),
            borrowed.engine.snapshot(),
            borrowed.event_log,
        )
    };
    Ok(MetricsApiResponse {
        node_id: status.node_id,
        deployment_count: snapshot.control_plane.deployments.len(),
        pipeline_count: snapshot.control_plane.pipelines.len(),
        node_count: snapshot.control_plane.nodes.len(),
        peer_count,
        table_count: snapshot.tables.tables().len(),
        commit_index: status.commit_index,
        last_applied: status.last_applied,
        durable_events: event_log.bounds().await?.latest_sequence,
    })
}

async fn handle_replay(
    state: SharedState,
    request: selium_control_plane_protocol::ReplayApiRequest,
) -> Result<ReplayApiResponse> {
    let event_log = state.borrow().event_log;
    let bounds = event_log.bounds().await?;
    let checkpoint_sequence = replay_requested_checkpoint_sequence(&event_log, &request).await?;
    let start_sequence = replay_start_sequence(&bounds, &request, checkpoint_sequence);
    let Some(start) = start_sequence else {
        if let Some(name) = request.save_checkpoint.as_deref() {
            event_log
                .checkpoint(
                    name,
                    replay_checkpoint_sequence_to_save(
                        &request,
                        checkpoint_sequence,
                        None,
                        bounds.next_sequence,
                    ),
                )
                .await?;
        }
        return Ok(ReplayApiResponse {
            events: Vec::new(),
            start_sequence: None,
            next_sequence: None,
            high_watermark: bounds.latest_sequence,
        });
    };

    let limit = replay_fetch_limit(&bounds, start, request.limit);
    if limit == 0 {
        let next_sequence = replay_next_sequence(&request, &[], bounds.latest_sequence);
        if let Some(name) = request.save_checkpoint.as_deref() {
            event_log
                .checkpoint(
                    name,
                    replay_checkpoint_sequence_to_save(
                        &request,
                        checkpoint_sequence,
                        next_sequence,
                        bounds.next_sequence,
                    ),
                )
                .await?;
        }
        return Ok(ReplayApiResponse {
            events: Vec::new(),
            start_sequence: Some(start),
            next_sequence,
            high_watermark: bounds.latest_sequence,
        });
    }

    let replay_start = request
        .checkpoint
        .as_ref()
        .map(|name| StorageReplayStart::Checkpoint(name.clone()))
        .unwrap_or(StorageReplayStart::Sequence(start));
    let (records, high_watermark) = event_log.replay(replay_start, limit).await?;
    let selected = select_replay_records(records, &request);
    let next_sequence = replay_next_sequence(&request, &selected, high_watermark);
    if let Some(name) = request.save_checkpoint.as_deref() {
        event_log
            .checkpoint(
                name,
                replay_checkpoint_sequence_to_save(
                    &request,
                    checkpoint_sequence,
                    next_sequence,
                    bounds.next_sequence,
                ),
            )
            .await?;
    }
    let events = selected.into_iter().map(replay_record_value).collect();
    Ok(ReplayApiResponse {
        events,
        start_sequence: Some(start),
        next_sequence,
        high_watermark,
    })
}

fn replay_start_sequence(
    bounds: &StorageLogBoundsResult,
    request: &selium_control_plane_protocol::ReplayApiRequest,
    checkpoint_sequence: Option<u64>,
) -> Option<u64> {
    if request.limit == 0 {
        return None;
    }
    if let Some(sequence) = checkpoint_sequence {
        return Some(sequence);
    }
    let first = bounds.first_sequence?;
    let latest = bounds.latest_sequence?;
    if let Some(sequence) = request.since_sequence {
        return Some(sequence.max(first));
    }
    if replay_has_filters(request) {
        return Some(first);
    }
    let window = request.limit.saturating_sub(1) as u64;
    Some(latest.saturating_sub(window).max(first))
}

fn replay_fetch_limit(
    bounds: &StorageLogBoundsResult,
    start_sequence: u64,
    requested: usize,
) -> u32 {
    if requested == 0 {
        return 0;
    }
    let Some(latest) = bounds.latest_sequence else {
        return 0;
    };
    if start_sequence > latest {
        return 0;
    }
    latest
        .saturating_sub(start_sequence)
        .saturating_add(1)
        .min(u32::MAX as u64) as u32
}

fn replay_has_filters(request: &selium_control_plane_protocol::ReplayApiRequest) -> bool {
    request.external_account_ref.is_some()
        || request.workload.is_some()
        || request.module.is_some()
        || request.pipeline.is_some()
        || request.node.is_some()
}

fn replay_pages_forward(request: &selium_control_plane_protocol::ReplayApiRequest) -> bool {
    request.since_sequence.is_some() || request.checkpoint.is_some() || replay_has_filters(request)
}

fn select_replay_records(
    records: Vec<StorageLogRecord>,
    request: &selium_control_plane_protocol::ReplayApiRequest,
) -> Vec<StorageLogRecord> {
    let mut matching = records
        .into_iter()
        .filter(|record| replay_record_matches(record, request))
        .collect::<Vec<_>>();
    if replay_pages_forward(request) {
        matching.truncate(request.limit);
        return matching;
    }
    let keep = matching.len().saturating_sub(request.limit);
    matching.drain(0..keep);
    matching
}

fn replay_next_sequence(
    request: &selium_control_plane_protocol::ReplayApiRequest,
    records: &[StorageLogRecord],
    high_watermark: Option<u64>,
) -> Option<u64> {
    if request.limit == 0 {
        return None;
    }
    if let Some(record) = records.last() {
        return Some(record.sequence.saturating_add(1));
    }
    let advanced = high_watermark.map(|sequence| sequence.saturating_add(1));
    if replay_pages_forward(request) {
        return request
            .since_sequence
            .map(|sequence| advanced.map_or(sequence, |next| next.max(sequence)))
            .or(advanced);
    }
    advanced
}

fn replay_requested_checkpoint_name(
    request: &selium_control_plane_protocol::ReplayApiRequest,
) -> Result<Option<&str>> {
    let Some(name) = request.checkpoint.as_deref() else {
        return Ok(None);
    };
    if request.since_sequence.is_some() {
        bail!("replay request cannot set both since_sequence and checkpoint");
    }
    if name.trim().is_empty() {
        bail!("invalid checkpoint name");
    }
    Ok(Some(name))
}

fn replay_checkpoint_sequence_or_err(name: &str, checkpoint_sequence: Option<u64>) -> Result<u64> {
    checkpoint_sequence.ok_or_else(|| anyhow!("checkpoint `{name}` does not exist"))
}

async fn replay_requested_checkpoint_sequence(
    event_log: &storage::Log,
    request: &selium_control_plane_protocol::ReplayApiRequest,
) -> Result<Option<u64>> {
    let Some(name) = replay_requested_checkpoint_name(request)? else {
        return Ok(None);
    };
    let checkpoint_sequence = event_log.checkpoint_sequence(name).await?;
    Ok(Some(replay_checkpoint_sequence_or_err(
        name,
        checkpoint_sequence,
    )?))
}

fn replay_checkpoint_sequence_to_save(
    request: &selium_control_plane_protocol::ReplayApiRequest,
    checkpoint_sequence: Option<u64>,
    next_sequence: Option<u64>,
    current_next_sequence: u64,
) -> u64 {
    next_sequence
        .or(checkpoint_sequence)
        .or(request.since_sequence)
        .unwrap_or(current_next_sequence)
}

fn replay_record_matches(
    record: &StorageLogRecord,
    request: &selium_control_plane_protocol::ReplayApiRequest,
) -> bool {
    match_header(
        &record.headers,
        "external_account_ref",
        request.external_account_ref.as_deref(),
    ) && match_header(&record.headers, "workload", request.workload.as_deref())
        && match_header(&record.headers, "module", request.module.as_deref())
        && match_header(&record.headers, "pipeline", request.pipeline.as_deref())
        && match_header(&record.headers, "node", request.node.as_deref())
}

fn match_header(headers: &BTreeMap<String, String>, key: &str, expected: Option<&str>) -> bool {
    expected.is_none_or(|value| headers.get(key).map(String::as_str) == Some(value))
}

fn replay_record_value(record: StorageLogRecord) -> DataValue {
    let mut event = BTreeMap::from([
        ("sequence".to_string(), DataValue::from(record.sequence)),
        (
            "timestamp_ms".to_string(),
            DataValue::from(record.timestamp_ms),
        ),
        (
            "headers".to_string(),
            DataValue::Map(
                record
                    .headers
                    .iter()
                    .map(|(key, value)| (key.clone(), DataValue::from(value.clone())))
                    .collect(),
            ),
        ),
    ]);

    match decode_rkyv::<DurableCommittedEntry>(&record.payload) {
        Ok(entry) => {
            event.insert(
                "idempotency_key".to_string(),
                DataValue::from(entry.idempotency_key.clone()),
            );
            event.insert("index".to_string(), DataValue::from(entry.index));
            event.insert("term".to_string(), DataValue::from(entry.term));
            if let Ok(envelope) = ControlPlaneEngine::decode_mutation(&entry.payload) {
                let mut audit = audit_event_fields(&entry, &envelope.mutation);
                for key in ["external_account_ref", "module"] {
                    if !audit.contains_key(key) {
                        if let Some(value) = record.headers.get(key) {
                            audit.insert(key.to_string(), DataValue::from(value.clone()));
                        }
                    }
                }
                event.insert("audit".to_string(), DataValue::Map(audit));
            }
        }
        Err(_) => {
            event.insert("payload".to_string(), DataValue::Bytes(record.payload));
        }
    }

    DataValue::Map(event)
}

fn audit_event_fields(
    entry: &DurableCommittedEntry,
    mutation: &Mutation,
) -> BTreeMap<String, DataValue> {
    let mut audit = BTreeMap::from([
        (
            "actor_kind".to_string(),
            DataValue::from(mutation_actor_kind(mutation)),
        ),
        (
            "mutation_kind".to_string(),
            DataValue::from(mutation_kind(mutation)),
        ),
        (
            "idempotency_key".to_string(),
            DataValue::from(entry.idempotency_key.clone()),
        ),
        ("index".to_string(), DataValue::from(entry.index)),
        ("term".to_string(), DataValue::from(entry.term)),
    ]);
    audit.extend(mutation_target_fields(mutation));
    audit
}

fn audit_headers(
    state: Option<&ControlPlaneState>,
    mutation: &Mutation,
) -> BTreeMap<String, String> {
    let mut headers = BTreeMap::from([
        ("source".to_string(), "control-plane".to_string()),
        ("event_kind".to_string(), "audit".to_string()),
        (
            "actor_kind".to_string(),
            mutation_actor_kind(mutation).to_string(),
        ),
        (
            "mutation_kind".to_string(),
            mutation_kind(mutation).to_string(),
        ),
    ]);
    match mutation {
        Mutation::UpsertDeployment { spec } => {
            headers.insert("workload".to_string(), spec.workload.to_string());
            headers.insert("module".to_string(), spec.module.clone());
            if let Some(reference) = &spec.external_account_ref {
                headers.insert("external_account_ref".to_string(), reference.key.clone());
            }
        }
        Mutation::SetScale { workload, .. } => {
            headers.insert("workload".to_string(), workload.to_string());
            if let Some(deployment) = state.and_then(|state| state.deployments.get(&workload.key()))
            {
                headers.insert("module".to_string(), deployment.module.clone());
                if let Some(reference) = &deployment.external_account_ref {
                    headers.insert("external_account_ref".to_string(), reference.key.clone());
                }
            }
        }
        Mutation::UpsertPipeline { spec } => {
            headers.insert(
                "pipeline".to_string(),
                format!("{}/{}/{}", spec.tenant, spec.namespace, spec.name),
            );
            if let Some(reference) = &spec.external_account_ref {
                headers.insert("external_account_ref".to_string(), reference.key.clone());
            }
        }
        Mutation::UpsertNode { spec } => {
            headers.insert("node".to_string(), spec.name.clone());
        }
        Mutation::PublishIdl { .. } | Mutation::Table { .. } => {}
    }
    headers
}

fn mutation_actor_kind(mutation: &Mutation) -> &'static str {
    match mutation {
        Mutation::UpsertNode { .. } => "system",
        Mutation::PublishIdl { .. }
        | Mutation::UpsertDeployment { .. }
        | Mutation::UpsertPipeline { .. }
        | Mutation::SetScale { .. }
        | Mutation::Table { .. } => "operator",
    }
}

fn mutation_kind(mutation: &Mutation) -> &'static str {
    match mutation {
        Mutation::PublishIdl { .. } => "publish_idl",
        Mutation::UpsertDeployment { .. } => "upsert_deployment",
        Mutation::UpsertPipeline { .. } => "upsert_pipeline",
        Mutation::UpsertNode { .. } => "upsert_node",
        Mutation::SetScale { .. } => "set_scale",
        Mutation::Table { .. } => "table",
    }
}

fn mutation_target_fields(mutation: &Mutation) -> BTreeMap<String, DataValue> {
    match mutation {
        Mutation::PublishIdl { .. } | Mutation::Table { .. } => BTreeMap::new(),
        Mutation::UpsertDeployment { spec } => BTreeMap::from([
            (
                "workload".to_string(),
                DataValue::from(spec.workload.to_string()),
            ),
            ("replicas".to_string(), DataValue::from(spec.replicas)),
            ("module".to_string(), DataValue::from(spec.module.clone())),
            (
                "external_account_ref".to_string(),
                serialize_external_account_ref(&spec.external_account_ref),
            ),
            (
                "resources".to_string(),
                serialize_deployment_resources(spec),
            ),
        ]),
        Mutation::UpsertPipeline { spec } => BTreeMap::from([
            (
                "pipeline".to_string(),
                DataValue::from(format!("{}/{}/{}", spec.tenant, spec.namespace, spec.name)),
            ),
            (
                "external_account_ref".to_string(),
                serialize_external_account_ref(&spec.external_account_ref),
            ),
            ("edge_count".to_string(), DataValue::from(spec.edges.len())),
        ]),
        Mutation::UpsertNode { spec } => BTreeMap::from([
            ("node".to_string(), DataValue::from(spec.name.clone())),
            (
                "capacity_slots".to_string(),
                DataValue::from(spec.capacity_slots),
            ),
            (
                "allocatable_cpu_millis".to_string(),
                serialize_optional_u32(spec.allocatable_cpu_millis),
            ),
            (
                "allocatable_memory_mib".to_string(),
                serialize_optional_u32(spec.allocatable_memory_mib),
            ),
        ]),
        Mutation::SetScale { workload, replicas } => BTreeMap::from([
            (
                "workload".to_string(),
                DataValue::from(workload.to_string()),
            ),
            ("replicas".to_string(), DataValue::from(*replicas)),
        ]),
    }
}

fn serialize_deployment_resources(spec: &DeploymentSpec) -> DataValue {
    DataValue::Map(BTreeMap::from([
        ("cpu_millis".to_string(), DataValue::from(spec.cpu_millis)),
        ("memory_mib".to_string(), DataValue::from(spec.memory_mib)),
        (
            "ephemeral_storage_mib".to_string(),
            DataValue::from(spec.ephemeral_storage_mib),
        ),
        (
            "bandwidth_profile".to_string(),
            DataValue::from(spec.bandwidth_profile.as_str()),
        ),
        (
            "volume_mounts".to_string(),
            DataValue::List(
                spec.volume_mounts
                    .iter()
                    .map(|mount| {
                        DataValue::Map(BTreeMap::from([
                            ("name".to_string(), DataValue::from(mount.name.clone())),
                            (
                                "mount_path".to_string(),
                                DataValue::from(mount.mount_path.clone()),
                            ),
                            ("read_only".to_string(), DataValue::from(mount.read_only)),
                        ]))
                    })
                    .collect(),
            ),
        ),
    ]))
}

fn serialize_optional_u32(value: Option<u32>) -> DataValue {
    value.map_or(DataValue::Null, DataValue::from)
}

fn serialize_external_account_ref(
    external_account_ref: &Option<selium_control_plane_api::ExternalAccountRef>,
) -> DataValue {
    external_account_ref
        .as_ref()
        .map_or(DataValue::Null, |reference| {
            DataValue::from(reference.key.clone())
        })
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
                allocatable_cpu_millis: state.borrow().config.allocatable_cpu_millis,
                allocatable_memory_mib: state.borrow().config.allocatable_memory_mib,
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
                        workload_key: deployment.clone(),
                        instance_id: instance_id.clone(),
                        module_spec,
                        external_account_ref: spec
                            .external_account_ref
                            .as_ref()
                            .map(|reference| reference.key.clone()),
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
        let snapshot = state.borrow().engine.snapshot();
        let headers = audit_headers(Some(&snapshot.control_plane), &envelope.mutation);
        event_log
            .append(
                time::now().await?.unix_ms,
                headers,
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
        BandwidthProfile, ContractRef, DeploymentSpec, EventEndpointRef, ExternalAccountRef,
        PipelineEdge, PipelineEndpoint, PipelineSpec, VolumeMount, WorkloadRef, parse_idl,
    };
    use selium_control_plane_protocol::ReplayApiRequest;

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
                cpu_millis: 0,
                memory_mib: 0,
                ephemeral_storage_mib: 0,
                bandwidth_profile: BandwidthProfile::Standard,
                volume_mounts: Vec::new(),
                external_account_ref: None,
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
                    cpu_millis: 0,
                    memory_mib: 0,
                    ephemeral_storage_mib: 0,
                    bandwidth_profile: BandwidthProfile::Standard,
                    volume_mounts: Vec::new(),
                    external_account_ref: None,
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
            external_account_ref: None,
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
                    cpu_millis: 0,
                    memory_mib: 0,
                    ephemeral_storage_mib: 0,
                    bandwidth_profile: BandwidthProfile::Standard,
                    volume_mounts: Vec::new(),
                    external_account_ref: None,
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
            external_account_ref: None,
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

    #[test]
    fn replay_record_value_includes_structured_operator_audit_fields() {
        let workload = WorkloadRef {
            tenant: "tenant-a".to_string(),
            namespace: "media".to_string(),
            name: "router".to_string(),
        };
        let workload_key = workload.to_string();
        let mut state = ControlPlaneState::default();
        state
            .upsert_deployment(DeploymentSpec {
                workload: workload.clone(),
                module: "router.wasm".to_string(),
                replicas: 1,
                contracts: Vec::new(),
                isolation: IsolationProfile::Standard,
                cpu_millis: 0,
                memory_mib: 0,
                ephemeral_storage_mib: 0,
                bandwidth_profile: BandwidthProfile::Standard,
                volume_mounts: Vec::new(),
                external_account_ref: Some(ExternalAccountRef {
                    key: "acct-123".to_string(),
                }),
            })
            .expect("seed deployment");
        let envelope = MutationEnvelope {
            idempotency_key: "audit-1".to_string(),
            mutation: Mutation::SetScale {
                workload: workload.clone(),
                replicas: 3,
            },
        };
        let committed = DurableCommittedEntry {
            idempotency_key: envelope.idempotency_key.clone(),
            index: 7,
            term: 2,
            payload: ControlPlaneEngine::encode_mutation(&envelope).expect("encode mutation"),
        };
        let record = StorageLogRecord {
            sequence: 11,
            timestamp_ms: 42,
            headers: audit_headers(Some(&state), &envelope.mutation),
            payload: encode_rkyv(&committed).expect("encode committed entry"),
        };

        let value = replay_record_value(record);
        assert_eq!(value.get("sequence").and_then(DataValue::as_u64), Some(11));
        assert_eq!(value.get("index").and_then(DataValue::as_u64), Some(7));
        let audit = value.get("audit").expect("audit section");
        assert_eq!(
            audit.get("actor_kind").and_then(DataValue::as_str),
            Some("operator")
        );
        assert_eq!(
            audit.get("mutation_kind").and_then(DataValue::as_str),
            Some("set_scale")
        );
        assert_eq!(
            audit.get("workload").and_then(DataValue::as_str),
            Some(workload_key.as_str())
        );
        assert_eq!(
            audit.get("module").and_then(DataValue::as_str),
            Some("router.wasm")
        );
        assert_eq!(audit.get("replicas").and_then(DataValue::as_u64), Some(3));
        assert_eq!(
            audit
                .get("external_account_ref")
                .and_then(DataValue::as_str),
            Some("acct-123")
        );
    }

    #[test]
    fn replay_record_value_includes_declared_resource_fields() {
        let envelope = MutationEnvelope {
            idempotency_key: "audit-2".to_string(),
            mutation: Mutation::UpsertDeployment {
                spec: DeploymentSpec {
                    workload: WorkloadRef {
                        tenant: "tenant-a".to_string(),
                        namespace: "media".to_string(),
                        name: "router".to_string(),
                    },
                    module: "router.wasm".to_string(),
                    replicas: 1,
                    contracts: Vec::new(),
                    isolation: IsolationProfile::Standard,
                    cpu_millis: 600,
                    memory_mib: 512,
                    ephemeral_storage_mib: 128,
                    bandwidth_profile: BandwidthProfile::High,
                    volume_mounts: vec![VolumeMount {
                        name: "scratch".to_string(),
                        mount_path: "/scratch".to_string(),
                        read_only: false,
                    }],
                    external_account_ref: Some(ExternalAccountRef {
                        key: "acct-789".to_string(),
                    }),
                },
            },
        };
        let committed = DurableCommittedEntry {
            idempotency_key: envelope.idempotency_key.clone(),
            index: 8,
            term: 2,
            payload: ControlPlaneEngine::encode_mutation(&envelope).expect("encode mutation"),
        };
        let record = StorageLogRecord {
            sequence: 12,
            timestamp_ms: 43,
            headers: audit_headers(None, &envelope.mutation),
            payload: encode_rkyv(&committed).expect("encode committed entry"),
        };

        let event = replay_record_value(record);
        let audit = event.get("audit").expect("audit section");

        assert_eq!(
            audit
                .get("resources")
                .and_then(|resources| resources.get("cpu_millis"))
                .and_then(DataValue::as_u64),
            Some(600)
        );
        assert_eq!(
            audit
                .get("resources")
                .and_then(|resources| resources.get("bandwidth_profile"))
                .and_then(DataValue::as_str),
            Some("high")
        );
        assert_eq!(
            audit
                .get("external_account_ref")
                .and_then(DataValue::as_str),
            Some("acct-789")
        );
        assert_eq!(
            event
                .get("headers")
                .and_then(|headers| headers.get("external_account_ref"))
                .and_then(DataValue::as_str),
            Some("acct-789")
        );
    }

    #[test]
    fn replay_record_value_includes_pipeline_external_account_reference() {
        let contract = ContractRef {
            namespace: "media.pipeline".to_string(),
            kind: selium_control_plane_api::ContractKind::Event,
            name: "camera.frames".to_string(),
            version: "v1".to_string(),
        };
        let workload = WorkloadRef {
            tenant: "tenant-a".to_string(),
            namespace: "media".to_string(),
            name: "router".to_string(),
        };
        let envelope = MutationEnvelope {
            idempotency_key: "audit-3".to_string(),
            mutation: Mutation::UpsertPipeline {
                spec: PipelineSpec {
                    name: "camera".to_string(),
                    tenant: "tenant-a".to_string(),
                    namespace: "media".to_string(),
                    edges: vec![PipelineEdge {
                        from: PipelineEndpoint {
                            endpoint: EventEndpointRef {
                                workload: workload.clone(),
                                name: contract.name.clone(),
                            },
                            contract: contract.clone(),
                        },
                        to: PipelineEndpoint {
                            endpoint: EventEndpointRef {
                                workload,
                                name: contract.name.clone(),
                            },
                            contract,
                        },
                    }],
                    external_account_ref: Some(ExternalAccountRef {
                        key: "acct-321".to_string(),
                    }),
                },
            },
        };
        let committed = DurableCommittedEntry {
            idempotency_key: envelope.idempotency_key.clone(),
            index: 9,
            term: 2,
            payload: ControlPlaneEngine::encode_mutation(&envelope).expect("encode mutation"),
        };
        let record = StorageLogRecord {
            sequence: 13,
            timestamp_ms: 44,
            headers: audit_headers(None, &envelope.mutation),
            payload: encode_rkyv(&committed).expect("encode committed entry"),
        };

        let event = replay_record_value(record);
        let audit = event.get("audit").expect("audit section");

        assert_eq!(
            audit.get("pipeline").and_then(DataValue::as_str),
            Some("tenant-a/media/camera")
        );
        assert_eq!(
            audit
                .get("external_account_ref")
                .and_then(DataValue::as_str),
            Some("acct-321")
        );
        assert_eq!(
            event
                .get("headers")
                .and_then(|headers| headers.get("external_account_ref"))
                .and_then(DataValue::as_str),
            Some("acct-321")
        );
    }

    #[test]
    fn audit_headers_classify_node_upserts_as_system() {
        let headers = audit_headers(
            None,
            &Mutation::UpsertNode {
                spec: NodeSpec {
                    name: "node-a".to_string(),
                    capacity_slots: 64,
                    allocatable_cpu_millis: Some(2_000),
                    allocatable_memory_mib: Some(4_096),
                    supported_isolation: vec![IsolationProfile::Standard],
                    daemon_addr: "127.0.0.1:7100".to_string(),
                    daemon_server_name: "localhost".to_string(),
                    last_heartbeat_ms: 0,
                },
            },
        );

        assert_eq!(headers.get("actor_kind"), Some(&"system".to_string()));
        assert_eq!(
            headers.get("mutation_kind"),
            Some(&"upsert_node".to_string())
        );
        assert_eq!(headers.get("node"), Some(&"node-a".to_string()));
    }

    #[test]
    fn replay_selection_respects_since_sequence_and_header_filters() {
        let request = ReplayApiRequest {
            limit: 2,
            since_sequence: Some(12),
            checkpoint: None,
            save_checkpoint: None,
            external_account_ref: Some("acct-123".to_string()),
            workload: None,
            module: None,
            pipeline: None,
            node: None,
        };
        let bounds = StorageLogBoundsResult {
            code: selium_abi::StorageStatusCode::Ok,
            first_sequence: Some(10),
            latest_sequence: Some(14),
            next_sequence: 15,
        };
        assert_eq!(replay_start_sequence(&bounds, &request, None), Some(12));

        let selected = select_replay_records(
            vec![
                StorageLogRecord {
                    sequence: 12,
                    timestamp_ms: 1,
                    headers: BTreeMap::from([(
                        "external_account_ref".to_string(),
                        "acct-123".to_string(),
                    )]),
                    payload: Vec::new(),
                },
                StorageLogRecord {
                    sequence: 13,
                    timestamp_ms: 2,
                    headers: BTreeMap::from([(
                        "external_account_ref".to_string(),
                        "acct-999".to_string(),
                    )]),
                    payload: Vec::new(),
                },
                StorageLogRecord {
                    sequence: 14,
                    timestamp_ms: 3,
                    headers: BTreeMap::from([(
                        "external_account_ref".to_string(),
                        "acct-123".to_string(),
                    )]),
                    payload: Vec::new(),
                },
            ],
            &request,
        );

        assert_eq!(
            selected
                .iter()
                .map(|record| record.sequence)
                .collect::<Vec<_>>(),
            vec![12, 14]
        );
        assert_eq!(
            replay_next_sequence(&request, &selected, Some(14)),
            Some(15)
        );
    }

    #[test]
    fn inventory_snapshot_marker_hands_off_to_replay_since_sequence() {
        let account = ExternalAccountRef {
            key: "acct-123".to_string(),
        };
        let workload = WorkloadRef {
            tenant: "tenant-a".to_string(),
            namespace: "media".to_string(),
            name: "ingest".to_string(),
        };
        let mut engine = ControlPlaneEngine::default();
        engine
            .apply_mutation(
                1,
                Mutation::UpsertDeployment {
                    spec: DeploymentSpec {
                        workload: workload.clone(),
                        module: "ingest.wasm".to_string(),
                        replicas: 1,
                        contracts: Vec::new(),
                        isolation: IsolationProfile::Standard,
                        cpu_millis: 250,
                        memory_mib: 128,
                        ephemeral_storage_mib: 0,
                        bandwidth_profile: BandwidthProfile::Standard,
                        volume_mounts: Vec::new(),
                        external_account_ref: Some(account.clone()),
                    },
                },
            )
            .expect("deployment");
        engine
            .apply_mutation(
                2,
                Mutation::SetScale {
                    workload: workload.clone(),
                    replicas: 2,
                },
            )
            .expect("scale to snapshot state");

        let inventory = engine
            .query(runtime::Query::AttributedInfrastructureInventory {
                filter: runtime::AttributedInfrastructureFilter {
                    external_account_ref: Some(account.key.clone()),
                    ..Default::default()
                },
            })
            .expect("inventory query");
        let last_applied = inventory
            .result
            .get("snapshot_marker")
            .and_then(|marker| marker.get("last_applied"))
            .and_then(DataValue::as_u64)
            .expect("snapshot marker last_applied");

        let request = ReplayApiRequest {
            limit: 10,
            since_sequence: Some(last_applied.saturating_add(1)),
            checkpoint: None,
            save_checkpoint: None,
            external_account_ref: Some(account.key.clone()),
            workload: Some(workload.to_string()),
            module: None,
            pipeline: None,
            node: None,
        };
        let bounds = StorageLogBoundsResult {
            code: selium_abi::StorageStatusCode::Ok,
            first_sequence: Some(1),
            latest_sequence: Some(3),
            next_sequence: 4,
        };

        assert_eq!(last_applied, 2);
        let start_sequence =
            replay_start_sequence(&bounds, &request, None).expect("start sequence");
        assert_eq!(start_sequence, 3);
        assert_eq!(
            replay_fetch_limit(&bounds, start_sequence, request.limit),
            1
        );

        let snapshot_state = engine.snapshot().control_plane;
        let delta_record = StorageLogRecord {
            sequence: 3,
            timestamp_ms: 3,
            headers: audit_headers(
                Some(&snapshot_state),
                &Mutation::SetScale {
                    workload: workload.clone(),
                    replicas: 3,
                },
            ),
            payload: encode_rkyv(&DurableCommittedEntry {
                idempotency_key: "scale-3".to_string(),
                index: 3,
                term: 1,
                payload: ControlPlaneEngine::encode_mutation(&MutationEnvelope {
                    idempotency_key: "scale-3".to_string(),
                    mutation: Mutation::SetScale {
                        workload: workload.clone(),
                        replicas: 3,
                    },
                })
                .expect("encode delta mutation"),
            })
            .expect("encode delta record"),
        };

        let selected = select_replay_records(vec![delta_record], &request);
        assert_eq!(
            selected
                .iter()
                .map(|record| record.sequence)
                .collect::<Vec<_>>(),
            vec![3]
        );
        assert_eq!(replay_next_sequence(&request, &selected, Some(3)), Some(4));

        let event = replay_record_value(selected.into_iter().next().expect("delta record"));
        assert_eq!(event.get("sequence").and_then(DataValue::as_u64), Some(3));
        assert_eq!(event.get("index").and_then(DataValue::as_u64), Some(3));
        assert_eq!(
            event
                .get("audit")
                .and_then(|audit| audit.get("replicas"))
                .and_then(DataValue::as_u64),
            Some(3)
        );
    }

    #[test]
    fn replay_selection_with_filters_and_no_cursor_pages_forward() {
        let request = ReplayApiRequest {
            limit: 2,
            since_sequence: None,
            checkpoint: None,
            save_checkpoint: None,
            external_account_ref: Some("acct-123".to_string()),
            workload: None,
            module: None,
            pipeline: None,
            node: None,
        };

        let selected = select_replay_records(
            vec![
                StorageLogRecord {
                    sequence: 11,
                    timestamp_ms: 1,
                    headers: BTreeMap::from([(
                        "external_account_ref".to_string(),
                        "acct-123".to_string(),
                    )]),
                    payload: Vec::new(),
                },
                StorageLogRecord {
                    sequence: 12,
                    timestamp_ms: 2,
                    headers: BTreeMap::from([(
                        "external_account_ref".to_string(),
                        "acct-999".to_string(),
                    )]),
                    payload: Vec::new(),
                },
                StorageLogRecord {
                    sequence: 13,
                    timestamp_ms: 3,
                    headers: BTreeMap::from([(
                        "external_account_ref".to_string(),
                        "acct-123".to_string(),
                    )]),
                    payload: Vec::new(),
                },
                StorageLogRecord {
                    sequence: 14,
                    timestamp_ms: 4,
                    headers: BTreeMap::from([(
                        "external_account_ref".to_string(),
                        "acct-123".to_string(),
                    )]),
                    payload: Vec::new(),
                },
            ],
            &request,
        );

        assert_eq!(
            selected
                .iter()
                .map(|record| record.sequence)
                .collect::<Vec<_>>(),
            vec![11, 13]
        );
        assert_eq!(
            replay_next_sequence(&request, &selected, Some(14)),
            Some(14)
        );
    }

    #[test]
    fn replay_next_sequence_preserves_cursor_ahead_of_high_watermark() {
        let request = ReplayApiRequest {
            limit: 5,
            since_sequence: Some(25),
            checkpoint: None,
            save_checkpoint: None,
            external_account_ref: Some("acct-123".to_string()),
            workload: None,
            module: None,
            pipeline: None,
            node: None,
        };

        assert_eq!(replay_next_sequence(&request, &[], Some(14)), Some(25));
    }

    #[test]
    fn replay_checkpoint_pages_forward_and_advances_cursor() {
        let request = ReplayApiRequest {
            limit: 2,
            since_sequence: None,
            checkpoint: Some("replay-cursor".to_string()),
            save_checkpoint: Some("replay-cursor-next".to_string()),
            external_account_ref: None,
            workload: None,
            module: None,
            pipeline: None,
            node: None,
        };
        let bounds = StorageLogBoundsResult {
            code: selium_abi::StorageStatusCode::Ok,
            first_sequence: Some(10),
            latest_sequence: Some(14),
            next_sequence: 15,
        };

        assert_eq!(replay_start_sequence(&bounds, &request, Some(13)), Some(13));

        let selected = select_replay_records(
            vec![
                StorageLogRecord {
                    sequence: 13,
                    timestamp_ms: 3,
                    headers: BTreeMap::new(),
                    payload: Vec::new(),
                },
                StorageLogRecord {
                    sequence: 14,
                    timestamp_ms: 4,
                    headers: BTreeMap::new(),
                    payload: Vec::new(),
                },
            ],
            &request,
        );

        assert_eq!(
            selected
                .iter()
                .map(|record| record.sequence)
                .collect::<Vec<_>>(),
            vec![13, 14]
        );
        let next_sequence = replay_next_sequence(&request, &selected, Some(14));
        assert_eq!(next_sequence, Some(15));
        assert_eq!(
            replay_checkpoint_sequence_to_save(
                &request,
                Some(13),
                next_sequence,
                bounds.next_sequence
            ),
            15
        );
    }

    #[test]
    fn replay_checkpoint_errors_are_deterministic() {
        let invalid = ReplayApiRequest {
            limit: 10,
            since_sequence: Some(7),
            checkpoint: Some("named".to_string()),
            save_checkpoint: None,
            external_account_ref: None,
            workload: None,
            module: None,
            pipeline: None,
            node: None,
        };
        assert_eq!(
            replay_requested_checkpoint_name(&invalid)
                .expect_err("mixed cursor should fail")
                .to_string(),
            "replay request cannot set both since_sequence and checkpoint"
        );

        assert_eq!(
            replay_checkpoint_sequence_or_err("missing", None)
                .expect_err("unknown checkpoint should fail")
                .to_string(),
            "checkpoint `missing` does not exist"
        );

        let request = ReplayApiRequest {
            limit: 10,
            since_sequence: None,
            checkpoint: Some("missing".to_string()),
            save_checkpoint: None,
            external_account_ref: None,
            workload: None,
            module: None,
            pipeline: None,
            node: None,
        };
        assert_eq!(
            replay_checkpoint_sequence_to_save(&request, Some(12), None, 15),
            12
        );
    }
}
