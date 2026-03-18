//! System control-plane module bootstrap helpers and guest control-plane loop.

use std::{cell::RefCell, collections::BTreeMap, rc::Rc, time::Duration};

use anyhow::{Context, Result, anyhow, bail};
use rkyv::{Archive, Deserialize, Serialize};
use selium_abi::{
    DataValue, StorageLogBoundsResult, StorageLogRecord, StorageReplayStart, decode_frame_len,
    decode_rkyv, encode_frame, encode_rkyv,
};
use selium_control_plane_agent::{
    AgentState, ControlPlaneModuleConfig, EVENT_LOG_NAME, INTERNAL_BINDING_NAME, PEER_PROFILE_NAME,
    ReconcileAction, SNAPSHOT_BLOB_STORE_NAME, deployment_module_spec, endpoint_bridge_semantics,
    reconcile,
};
use selium_control_plane_api::{
    ControlPlaneState, DiscoveryRequest, DiscoveryTarget, IsolationProfile, NodeSpec,
};
use selium_control_plane_core::{
    Mutation, MutationEnvelope, MutationResponse, Query, serialize_deployment_resources,
    serialize_external_account_ref, serialize_optional_u32,
};
use selium_control_plane_protocol::{
    ActivateEndpointBridgeRequest, ActivateEndpointBridgeResponse, AppendEntriesApiRequest,
    DeactivateEndpointBridgeRequest, DeactivateEndpointBridgeResponse, ListRequest, ListResponse,
    Method, MetricsApiResponse, MutateApiRequest, MutateApiResponse, QueryApiRequest,
    QueryApiResponse, ReplayApiResponse, RequestVoteApiRequest, StartRequest, StatusApiResponse,
    StopRequest, decode_envelope, decode_list_response, decode_payload, encode_error_response,
    encode_response, is_error, is_request,
};
use selium_control_plane_runtime::{ControlPlaneEngine, RuntimeError};
use selium_control_plane_scheduler::build_plan;
use selium_guest::{network, spawn, storage, time};
use selium_io_consensus::{
    AppendEntries, AppendEntriesResponse, ConsensusConfig, ConsensusError, LogEntry, RaftNode,
    RequestVote, RequestVoteResponse, TickAction,
};

#[cfg(test)]
use selium_control_plane_agent::{apply, managed_endpoint_bindings_for_instance};
#[cfg(test)]
use selium_control_plane_api::{ContractKind, PublicEndpointRef};
#[cfg(test)]
use std::collections::BTreeSet;

const RAFT_MANIFEST: &str = "raft";
const ENGINE_MANIFEST: &str = "engine";
const TICK_INTERVAL_MS: u64 = 150;
const RECONCILE_INTERVAL_MS: u64 = 1_000;
const STREAM_TIMEOUT_MS: u32 = 5_000;

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
                ControlPlaneEngine::decode_snapshot(&bytes).context("decode engine snapshot")?;
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
            let envelope = ControlPlaneEngine::decode_replayed_mutation(&committed.payload)
                .map_err(anyhow_from_runtime)?;
            let response = state
                .engine
                .apply_replayed_entry(&LogEntry {
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
    let query = request.query.clone();
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

    let (response, leader_hint) = if query_uses_observed_load_for_scheduling(&query) {
        let now_ms = time::now().await?.unix_ms;
        let borrowed = state.borrow();
        let mut snapshot = borrowed.engine.snapshot();
        clear_stale_observed_node_load(
            &mut snapshot.control_plane,
            now_ms,
            max_observed_load_age_ms(&borrowed.config),
        );
        let response = ControlPlaneEngine::from_snapshot(snapshot)
            .query(query)
            .map_err(anyhow_from_runtime)?;
        (response, borrowed.raft.leader_hint())
    } else {
        let borrowed = state.borrow();
        let response = borrowed.engine.query(query).map_err(anyhow_from_runtime)?;
        (response, borrowed.raft.leader_hint())
    };

    Ok(QueryApiResponse {
        leader_hint,
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
                    if !audit.contains_key(key)
                        && let Some(value) = record.headers.get(key)
                    {
                        audit.insert(key.to_string(), DataValue::from(value.clone()));
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
            (
                "reserve_cpu_utilisation_ppm".to_string(),
                DataValue::from(spec.reserve_cpu_utilisation_ppm),
            ),
            (
                "reserve_memory_utilisation_ppm".to_string(),
                DataValue::from(spec.reserve_memory_utilisation_ppm),
            ),
            (
                "reserve_slots_utilisation_ppm".to_string(),
                DataValue::from(spec.reserve_slots_utilisation_ppm),
            ),
            (
                "observed_running_instances".to_string(),
                serialize_optional_u32(spec.observed_running_instances),
            ),
            (
                "observed_active_bridges".to_string(),
                serialize_optional_u32(spec.observed_active_bridges),
            ),
            (
                "observed_memory_mib".to_string(),
                serialize_optional_u32(spec.observed_memory_mib),
            ),
            (
                "observed_workloads".to_string(),
                DataValue::Map(
                    spec.observed_workloads
                        .iter()
                        .map(|(workload, count)| (workload.clone(), DataValue::from(*count)))
                        .collect(),
                ),
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
    let observed = local_observed_node_load(&state).await;
    let request = MutateApiRequest {
        idempotency_key: format!("node-heartbeat:{}:{now}", state.borrow().config.node_id),
        mutation: Mutation::UpsertNode {
            spec: NodeSpec {
                name: state.borrow().config.node_id.clone(),
                capacity_slots: state.borrow().config.capacity_slots,
                allocatable_cpu_millis: state.borrow().config.allocatable_cpu_millis,
                allocatable_memory_mib: state.borrow().config.allocatable_memory_mib,
                reserve_cpu_utilisation_ppm: state.borrow().config.reserve_cpu_utilisation_ppm,
                reserve_memory_utilisation_ppm: state
                    .borrow()
                    .config
                    .reserve_memory_utilisation_ppm,
                reserve_slots_utilisation_ppm: state.borrow().config.reserve_slots_utilisation_ppm,
                observed_running_instances: Some(observed.running_instances),
                observed_active_bridges: Some(observed.active_bridges),
                observed_memory_mib: Some(observed.memory_mib),
                observed_workloads: observed.observed_workloads.clone(),
                observed_workload_memory_mib: observed.observed_workload_memory_mib.clone(),
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

#[derive(Debug, Clone, Default)]
struct ObservedNodeLoad {
    running_instances: u32,
    active_bridges: u32,
    memory_mib: u32,
    observed_workloads: BTreeMap<String, u32>,
    observed_workload_memory_mib: BTreeMap<String, u32>,
}

async fn local_observed_node_load(state: &SharedState) -> ObservedNodeLoad {
    let previous = current_observed_node_load(state);
    let (authority, node_id) = {
        let borrowed = state.borrow();
        (
            daemon_authority(
                &borrowed.config.public_daemon_addr,
                &borrowed.config.public_daemon_server_name,
            ),
            borrowed.config.node_id.clone(),
        )
    };

    match send_daemon_list_request(state, &authority, ListRequest { node_id }).await {
        Ok(Some(list)) => merge_observed_node_load(previous, list),
        Ok(None) | Err(_) => previous,
    }
}

fn current_observed_node_load(state: &SharedState) -> ObservedNodeLoad {
    let node_id = state.borrow().config.node_id.clone();
    state
        .borrow()
        .engine
        .snapshot()
        .control_plane
        .nodes
        .get(&node_id)
        .map(|node| ObservedNodeLoad {
            running_instances: node.observed_running_instances.unwrap_or(0),
            active_bridges: node.observed_active_bridges.unwrap_or(0),
            memory_mib: node.observed_memory_mib.unwrap_or(0),
            observed_workloads: node.observed_workloads.clone(),
            observed_workload_memory_mib: node.observed_workload_memory_mib.clone(),
        })
        .unwrap_or_default()
}

fn merge_observed_node_load(previous: ObservedNodeLoad, list: ListResponse) -> ObservedNodeLoad {
    let has_instances = !list.instances.is_empty();
    ObservedNodeLoad {
        running_instances: list.instances.len().min(u32::MAX as usize) as u32,
        active_bridges: list.active_bridges.len().min(u32::MAX as usize) as u32,
        memory_mib: match list.observed_memory_bytes {
            Some(bytes) => bytes_to_mib(bytes),
            None if has_instances => previous.memory_mib,
            None => 0,
        },
        observed_workloads: if list.observed_workloads.is_empty() && has_instances {
            previous.observed_workloads
        } else {
            list.observed_workloads
        },
        observed_workload_memory_mib: if list.observed_workload_memory_bytes.is_empty()
            && has_instances
        {
            previous.observed_workload_memory_mib
        } else {
            workload_bytes_to_mib(&list.observed_workload_memory_bytes)
        },
    }
}

fn bytes_to_mib(bytes: u64) -> u32 {
    const MIB: u64 = 1024 * 1024;
    if bytes == 0 {
        return 0;
    }
    bytes
        .saturating_add(MIB.saturating_sub(1))
        .saturating_div(MIB)
        .min(u64::from(u32::MAX)) as u32
}

fn workload_bytes_to_mib(workload_bytes: &BTreeMap<String, u64>) -> BTreeMap<String, u32> {
    let total_bytes = workload_bytes
        .values()
        .copied()
        .fold(0u64, u64::saturating_add);
    let total_mib = bytes_to_mib(total_bytes);
    if workload_bytes.is_empty() {
        return BTreeMap::new();
    }
    if total_bytes == 0 || total_mib == 0 {
        return workload_bytes
            .keys()
            .cloned()
            .map(|workload| (workload, 0))
            .collect();
    }

    let total_bytes = u128::from(total_bytes);
    let total_mib = u128::from(total_mib);
    let mut assigned = BTreeMap::new();
    let mut remainders = Vec::new();
    let mut assigned_total = 0u32;

    for (workload, bytes) in workload_bytes {
        let weighted = total_mib.saturating_mul(u128::from(*bytes));
        let mib = (weighted / total_bytes) as u32;
        let remainder = weighted % total_bytes;
        assigned_total = assigned_total.saturating_add(mib);
        assigned.insert(workload.clone(), mib);
        remainders.push((workload.clone(), remainder));
    }

    remainders.sort_by(|lhs, rhs| rhs.1.cmp(&lhs.1).then_with(|| lhs.0.cmp(&rhs.0)));
    for (workload, _) in remainders
        .into_iter()
        .take(total_mib.saturating_sub(u128::from(assigned_total)) as usize)
    {
        if let Some(value) = assigned.get_mut(&workload) {
            *value = value.saturating_add(1);
        }
    }

    assigned
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

    let max_observed_load_age_ms = {
        let borrowed = state.borrow();
        max_observed_load_age_ms(&borrowed.config)
    };
    let now_ms = time::now().await?.unix_ms;
    let mut snapshot = state.borrow().engine.snapshot().control_plane;
    clear_stale_observed_node_load(&mut snapshot, now_ms, max_observed_load_age_ms);
    let desired = build_plan(&snapshot).map_err(|err| anyhow!("build schedule plan: {err}"))?;

    for (node, spec) in &snapshot.nodes {
        let authority = daemon_authority(&spec.daemon_addr, &spec.daemon_server_name);
        let list: ListResponse = send_daemon_list_request(
            &state,
            &authority,
            ListRequest {
                node_id: node.clone(),
            },
        )
        .await?
        .unwrap_or(ListResponse {
            instances: BTreeMap::new(),
            active_bridges: Vec::new(),
            observed_memory_bytes: None,
            observed_workloads: BTreeMap::new(),
            observed_workload_memory_bytes: BTreeMap::new(),
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

fn query_uses_observed_load_for_scheduling(query: &Query) -> bool {
    matches!(
        query,
        Query::ControlPlaneSummary
            | Query::AttributedInfrastructureInventory { .. }
            | Query::ResolveDiscovery {
                request: DiscoveryRequest {
                    target: DiscoveryTarget::RunningProcess(_),
                    ..
                },
            }
    )
}

fn max_observed_load_age_ms(config: &ControlPlaneModuleConfig) -> u64 {
    config
        .heartbeat_interval_ms
        .saturating_mul(3)
        .max(RECONCILE_INTERVAL_MS)
}

fn clear_stale_observed_node_load(
    snapshot: &mut ControlPlaneState,
    now_ms: u64,
    max_observed_load_age_ms: u64,
) {
    for node in snapshot.nodes.values_mut() {
        let heartbeat_age_ms = now_ms.saturating_sub(node.last_heartbeat_ms);
        if node.last_heartbeat_ms > 0 && heartbeat_age_ms <= max_observed_load_age_ms {
            continue;
        }
        clear_observed_node_load(node);
    }
}

fn clear_observed_node_load(node: &mut NodeSpec) {
    node.observed_running_instances = None;
    node.observed_active_bridges = None;
    node.observed_memory_mib = None;
    node.observed_workloads.clear();
    node.observed_workload_memory_mib.clear();
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
            ReconcileAction::EnsureEndpointBridge(action) => {
                let target_spec = cp_state
                    .nodes
                    .get(&action.target_node)
                    .ok_or_else(|| anyhow!("unknown target node `{}`", action.target_node))?
                    .clone();
                let _: ActivateEndpointBridgeResponse = send_daemon_rpc(
                    state,
                    &authority,
                    Method::ActivateEndpointBridge,
                    &ActivateEndpointBridgeRequest {
                        node_id: node.to_string(),
                        bridge_id: action.bridge_id.clone(),
                        source_instance_id: action.source_instance_id.clone(),
                        source_endpoint: action.source_endpoint.clone(),
                        target_instance_id: action.target_instance_id.clone(),
                        target_node: action.target_node.clone(),
                        target_daemon_addr: target_spec.daemon_addr,
                        target_daemon_server_name: target_spec.daemon_server_name,
                        target_endpoint: action.target_endpoint.clone(),
                        semantics: endpoint_bridge_semantics(action.source_endpoint.kind),
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

async fn send_daemon_list_request(
    state: &SharedState,
    authority: &str,
    request: ListRequest,
) -> Result<Option<ListResponse>> {
    send_daemon_list_request_with_payload(state, authority, &request).await
}

async fn send_daemon_list_request_with_payload<Req>(
    state: &SharedState,
    authority: &str,
    request: &Req,
) -> Result<Option<ListResponse>>
where
    Req: selium_abi::RkyvEncode,
{
    let request_id = {
        let mut borrowed = state.borrow_mut();
        let request_id = borrowed.next_request_id;
        borrowed.next_request_id = borrowed.next_request_id.saturating_add(1);
        request_id
    };
    let frame =
        selium_control_plane_protocol::encode_request(Method::ListInstances, request_id, request)
            .context("encode daemon request")?;
    let response = send_raw_frame(authority, frame).await?;
    let envelope = decode_envelope(&response).context("decode daemon response")?;
    if envelope.method != Method::ListInstances || envelope.request_id != request_id {
        return Err(anyhow!("daemon response mismatch"));
    }
    if is_error(&envelope) {
        let error = selium_control_plane_protocol::decode_error(&envelope)
            .context("decode daemon error")?;
        return Err(anyhow!("daemon error {}: {}", error.code, error.message));
    }
    Ok(Some(
        decode_list_response(&envelope).context("decode daemon payload")?,
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
    let frame = encode_frame(payload).map_err(|err| anyhow!(err.to_string()))?;
    stream
        .send(frame, true, STREAM_TIMEOUT_MS)
        .await
        .context("write framed payload")?;
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
    let len = decode_frame_len(&buffer[..4]).map_err(|err| anyhow!(err.to_string()))?;
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
mod tests;
