//! Shared control-plane daemon protocol and binary envelope utilities.

use std::collections::BTreeMap;

use anyhow::{anyhow, Context, Result};
use rkyv::{
    api::high::{HighDeserializer, HighValidator},
    Archive, Deserialize, Serialize,
};
use selium_abi::{
    decode_rkyv, encode_rkyv, DataValue, RkyvEncode, RuntimeUsageQuery, RuntimeUsageRecord,
};
use selium_control_plane_api::{
    BandwidthProfile, ContractKind, DiscoveryCapabilityScope, IsolationProfile,
    OperationalProcessSelector, PublicEndpointRef,
};
use selium_control_plane_core::{Mutation, Query};
use selium_io_consensus::{AppendEntries, RequestVote};

pub const PROTOCOL_VERSION: u16 = 2;
pub const FLAG_REQUEST: u16 = 0x01;
pub const FLAG_RESPONSE: u16 = 0x02;
pub const FLAG_ERROR: u16 = 0x04;
const HEADER_LEN: usize = 18;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum Method {
    ControlMutate = 1,
    ControlQuery = 2,
    ControlStatus = 3,
    ControlReplay = 4,
    RaftRequestVote = 5,
    RaftAppendEntries = 6,
    ControlMetrics = 7,
    StartInstance = 100,
    StopInstance = 101,
    ListInstances = 102,
    ActivateEndpointBridge = 103,
    DeactivateEndpointBridge = 104,
    DeliverBridgeMessage = 105,
    RuntimeUsageQuery = 106,
    SubscribeGuestLogs = 107,
}

impl Method {
    pub fn from_u16(value: u16) -> Option<Self> {
        match value {
            1 => Some(Self::ControlMutate),
            2 => Some(Self::ControlQuery),
            3 => Some(Self::ControlStatus),
            4 => Some(Self::ControlReplay),
            5 => Some(Self::RaftRequestVote),
            6 => Some(Self::RaftAppendEntries),
            7 => Some(Self::ControlMetrics),
            100 => Some(Self::StartInstance),
            101 => Some(Self::StopInstance),
            102 => Some(Self::ListInstances),
            103 => Some(Self::ActivateEndpointBridge),
            104 => Some(Self::DeactivateEndpointBridge),
            105 => Some(Self::DeliverBridgeMessage),
            106 => Some(Self::RuntimeUsageQuery),
            107 => Some(Self::SubscribeGuestLogs),
            _ => None,
        }
    }

    pub fn as_u16(self) -> u16 {
        self as u16
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Envelope {
    pub version: u16,
    pub method: Method,
    pub request_id: u64,
    pub flags: u16,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct Empty {}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ErrorBody {
    pub code: u16,
    pub message: String,
    pub retryable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct MutateApiRequest {
    /// Client-supplied idempotency key for the semantic mutation.
    pub idempotency_key: String,
    /// Guest-owned semantic mutation to apply.
    pub mutation: Mutation,
    /// Host-authenticated discovery and binding scope forwarded to the guest.
    pub authorisation: Option<DiscoveryCapabilityScope>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct QueryApiRequest {
    /// Guest-owned semantic query to execute.
    pub query: Query,
    /// Whether followers may answer without forwarding to the leader.
    pub allow_stale: bool,
    /// Host-authenticated discovery and binding scope forwarded to the guest.
    pub authorisation: Option<DiscoveryCapabilityScope>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct MutateApiResponse {
    pub committed: bool,
    pub index: Option<u64>,
    pub leader_hint: Option<String>,
    pub result: Option<DataValue>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct QueryApiResponse {
    pub leader_hint: Option<String>,
    pub result: Option<DataValue>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct StatusApiResponse {
    pub node_id: String,
    pub role: String,
    pub current_term: u64,
    pub leader_id: Option<String>,
    pub commit_index: u64,
    pub last_applied: u64,
    pub peers: Vec<String>,
    pub table_count: usize,
    pub durable_events: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct MetricsApiResponse {
    pub node_id: String,
    pub deployment_count: usize,
    pub pipeline_count: usize,
    pub node_count: usize,
    pub peer_count: usize,
    pub table_count: usize,
    pub commit_index: u64,
    pub last_applied: u64,
    pub durable_events: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ReplayApiRequest {
    /// Return at most this many matching events.
    pub limit: usize,
    /// Inclusive sequence cursor to resume replay from when present.
    pub since_sequence: Option<u64>,
    /// Optional Selium-managed checkpoint name to resolve into the replay start cursor.
    pub checkpoint: Option<String>,
    /// Optional Selium-managed checkpoint name to create or advance to the response cursor.
    pub save_checkpoint: Option<String>,
    /// Restrict results to this external account reference when present.
    pub external_account_ref: Option<String>,
    /// Restrict results to this workload key when present.
    pub workload: Option<String>,
    /// Restrict results to this module identifier when present.
    pub module: Option<String>,
    /// Restrict results to this pipeline key when present.
    pub pipeline: Option<String>,
    /// Restrict results to this node name when present.
    pub node: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ReplayApiResponse {
    /// Filtered replay events in response order.
    pub events: Vec<DataValue>,
    /// Durable sequence the replay scan started from.
    pub start_sequence: Option<u64>,
    /// Sequence cursor external consumers can resume from on the next call.
    pub next_sequence: Option<u64>,
    /// Latest durable sequence known to the replay log at query time.
    pub high_watermark: Option<u64>,
}

/// Runtime-daemon request for attributed runtime usage export.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct RuntimeUsageApiRequest {
    /// Node expected to serve this daemon request.
    pub node_id: String,
    /// Replay anchor and Wave 2 filter set to apply.
    pub query: RuntimeUsageQuery,
}

/// Attributed runtime usage records returned by the daemon.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct RuntimeUsageApiResponse {
    /// Bounded immutable usage records that matched the query.
    pub records: Vec<RuntimeUsageRecord>,
    /// Sequence cursor external consumers can resume from on the next call.
    pub next_sequence: Option<u64>,
    /// Latest durable sequence known to the runtime usage log at query time.
    pub high_watermark: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct RequestVoteApiRequest {
    pub request: RequestVote,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct AppendEntriesApiRequest {
    pub request: AppendEntries,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum ManagedEndpointRole {
    Ingress,
    Egress,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum ManagedEndpointBindingType {
    OneWay,
    RequestResponse,
    Session,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ManagedEndpointBinding {
    pub endpoint_name: String,
    pub endpoint_kind: ContractKind,
    pub role: ManagedEndpointRole,
    pub binding_type: ManagedEndpointBindingType,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
/// Guest-owned reason for a workload lifecycle transition.
pub enum WorkloadLifecycleReason {
    Reconcile,
    OperatorRequest,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
/// Desired workload lifecycle transition requested by the control plane.
pub enum DesiredWorkloadTransition {
    Start,
    Stop,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
/// Guest-authored runtime posture the host should realize for a workload instance.
pub struct WorkloadResourcePosture {
    /// Isolation profile to enforce for the instance.
    pub isolation: IsolationProfile,
    /// CPU budget in millis requested for the instance.
    pub cpu_millis: u32,
    /// Memory budget in MiB requested for the instance.
    pub memory_mib: u32,
    /// Ephemeral storage budget in MiB requested for the instance.
    pub ephemeral_storage_mib: u32,
    /// Bandwidth profile requested for the instance.
    pub bandwidth_profile: BandwidthProfile,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
/// Guest-owned orchestration intent forwarded to the host daemon.
pub struct WorkloadOrchestrationIntent {
    /// Workload identity that owns the transition.
    pub workload_key: String,
    /// Instance identity to start or stop.
    pub instance_id: String,
    /// Desired lifecycle transition.
    pub transition: DesiredWorkloadTransition,
    /// Why the transition should happen.
    pub reason: WorkloadLifecycleReason,
    /// Requested runtime posture for start-like transitions.
    pub resource_posture: Option<WorkloadResourcePosture>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct StartRequest {
    pub node_id: String,
    /// Guest-owned orchestration intent for the new instance.
    pub intent: WorkloadOrchestrationIntent,
    pub module_spec: String,
    pub external_account_ref: Option<String>,
    pub managed_endpoint_bindings: Vec<ManagedEndpointBinding>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct StartResponse {
    pub status: String,
    pub instance_id: String,
    pub process_id: usize,
    pub already_running: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct StopRequest {
    pub node_id: String,
    /// Guest-owned orchestration intent for the running instance.
    pub intent: WorkloadOrchestrationIntent,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ListRequest {
    pub node_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct StopResponse {
    pub status: String,
    pub instance_id: String,
    pub process_id: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ListResponse {
    pub instances: BTreeMap<String, usize>,
    pub active_bridges: Vec<String>,
    pub observed_memory_bytes: Option<u64>,
    pub observed_workloads: BTreeMap<String, u32>,
    pub observed_workload_memory_bytes: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct SubscribeGuestLogsRequest {
    pub target: OperationalProcessSelector,
    pub stream_names: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct SubscribeGuestLogsResponse {
    pub status: String,
    pub target_node: String,
    pub target_instance_id: String,
    pub streams: Vec<PublicEndpointRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct GuestLogEvent {
    pub endpoint: PublicEndpointRef,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum EndpointBridgeMode {
    Local,
    Remote,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum EventDeliveryMode {
    Frame,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct EventBridgeSemantics {
    pub delivery: EventDeliveryMode,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum ServiceCorrelationMode {
    RequestId,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ServiceBridgeSemantics {
    pub correlation: ServiceCorrelationMode,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum StreamLifecycleMode {
    SessionFrames,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct StreamBridgeSemantics {
    pub lifecycle: StreamLifecycleMode,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum EndpointBridgeSemantics {
    Event(EventBridgeSemantics),
    Service(ServiceBridgeSemantics),
    Stream(StreamBridgeSemantics),
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
/// Guest-owned classification for a realized endpoint bridge.
pub enum EndpointBridgeClassification {
    WorkloadDataPlane,
    OperationalLogs,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
/// Health ownership policy for a bridge intent.
pub enum EndpointBridgeHealthPolicy {
    GuestControlled,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
/// Guest-owned lifecycle policy for a bridge intent.
pub struct EndpointBridgeLifecyclePolicy {
    /// Whether the host should keep the bridge attached while it remains healthy.
    pub keep_attached_when_healthy: bool,
    /// Which side owns health policy decisions.
    pub health: EndpointBridgeHealthPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
/// Guest-authored bridge intent forwarded to the host daemon.
pub struct EndpointBridgeIntent {
    /// Stable bridge identifier.
    pub bridge_id: String,
    /// Source instance identity.
    pub source_instance_id: String,
    /// Source public endpoint.
    pub source_endpoint: PublicEndpointRef,
    /// Target instance identity.
    pub target_instance_id: String,
    /// Target node identity.
    pub target_node: String,
    /// Target daemon address.
    pub target_daemon_addr: String,
    /// Target daemon TLS server name.
    pub target_daemon_server_name: String,
    /// Target public endpoint.
    pub target_endpoint: PublicEndpointRef,
    /// Guest-owned bridge classification.
    pub classification: EndpointBridgeClassification,
    /// Guest-owned lifecycle and health policy.
    pub lifecycle: EndpointBridgeLifecyclePolicy,
    /// Bridge message semantics the host should realize.
    pub semantics: EndpointBridgeSemantics,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ActivateEndpointBridgeRequest {
    pub node_id: String,
    /// Guest-authored bridge intent to realize on this node.
    pub intent: EndpointBridgeIntent,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ActivateEndpointBridgeResponse {
    pub status: String,
    pub bridge_id: String,
    pub mode: EndpointBridgeMode,
    pub target_node: String,
    pub target_instance_id: String,
    pub already_active: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct DeactivateEndpointBridgeRequest {
    pub node_id: String,
    pub bridge_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct DeactivateEndpointBridgeResponse {
    pub status: String,
    pub bridge_id: String,
    pub existed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct EventBridgeMessage {
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum ServiceMessagePhase {
    Request,
    Response,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ServiceBridgeMessage {
    pub exchange_id: String,
    pub phase: ServiceMessagePhase,
    pub sequence: u64,
    pub complete: bool,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum StreamLifecycle {
    Open,
    Data,
    Close,
    Abort,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct StreamBridgeMessage {
    pub session_id: String,
    pub lifecycle: StreamLifecycle,
    pub sequence: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum BridgeMessage {
    Event(EventBridgeMessage),
    Service(ServiceBridgeMessage),
    Stream(StreamBridgeMessage),
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct DeliverBridgeMessageRequest {
    pub target_instance_id: String,
    pub target_endpoint: PublicEndpointRef,
    pub message: BridgeMessage,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct DeliverBridgeMessageResponse {
    pub status: String,
    pub delivered: bool,
    pub message: Option<BridgeMessage>,
}

pub fn encode_request<T: RkyvEncode>(
    method: Method,
    request_id: u64,
    payload: &T,
) -> Result<Vec<u8>> {
    let payload = encode_rkyv(payload).context("encode request payload")?;
    encode_envelope(&Envelope {
        version: PROTOCOL_VERSION,
        method,
        request_id,
        flags: FLAG_REQUEST,
        payload,
    })
}

pub fn encode_response<T: RkyvEncode>(
    method: Method,
    request_id: u64,
    payload: &T,
) -> Result<Vec<u8>> {
    let payload = encode_rkyv(payload).context("encode response payload")?;
    encode_envelope(&Envelope {
        version: PROTOCOL_VERSION,
        method,
        request_id,
        flags: FLAG_RESPONSE,
        payload,
    })
}

pub fn encode_error_response(
    method: Method,
    request_id: u64,
    code: u16,
    message: impl Into<String>,
    retryable: bool,
) -> Result<Vec<u8>> {
    let body = ErrorBody {
        code,
        message: message.into(),
        retryable,
    };
    let payload = encode_rkyv(&body).context("encode error payload")?;
    encode_envelope(&Envelope {
        version: PROTOCOL_VERSION,
        method,
        request_id,
        flags: FLAG_RESPONSE | FLAG_ERROR,
        payload,
    })
}

pub fn decode_envelope(bytes: &[u8]) -> Result<Envelope> {
    if bytes.len() < HEADER_LEN {
        return Err(anyhow!("envelope too short: {}", bytes.len()));
    }

    let version = u16::from_be_bytes([bytes[0], bytes[1]]);
    if version != PROTOCOL_VERSION {
        return Err(anyhow!("unsupported protocol version {version}"));
    }

    let method_raw = u16::from_be_bytes([bytes[2], bytes[3]]);
    let method =
        Method::from_u16(method_raw).ok_or_else(|| anyhow!("unknown method {method_raw}"))?;
    let request_id = u64::from_be_bytes([
        bytes[4], bytes[5], bytes[6], bytes[7], bytes[8], bytes[9], bytes[10], bytes[11],
    ]);
    let flags = u16::from_be_bytes([bytes[12], bytes[13]]);
    let payload_len = u32::from_be_bytes([bytes[14], bytes[15], bytes[16], bytes[17]]) as usize;

    if bytes.len() != HEADER_LEN + payload_len {
        return Err(anyhow!(
            "invalid payload length header={}, actual={}",
            payload_len,
            bytes.len().saturating_sub(HEADER_LEN)
        ));
    }

    Ok(Envelope {
        version,
        method,
        request_id,
        flags,
        payload: bytes[HEADER_LEN..].to_vec(),
    })
}

pub fn decode_payload<T>(envelope: &Envelope) -> Result<T>
where
    T: Archive + Sized,
    for<'a> T::Archived: rkyv::Deserialize<T, HighDeserializer<rkyv::rancor::Error>>
        + rkyv::bytecheck::CheckBytes<HighValidator<'a, rkyv::rancor::Error>>,
{
    decode_rkyv(&envelope.payload).context("decode payload")
}

pub fn decode_list_response(envelope: &Envelope) -> Result<ListResponse> {
    decode_rkyv(&envelope.payload).context("decode list response payload")
}

pub fn decode_error(envelope: &Envelope) -> Result<ErrorBody> {
    decode_rkyv(&envelope.payload).context("decode error payload")
}

pub fn is_request(envelope: &Envelope) -> bool {
    envelope.flags & FLAG_REQUEST == FLAG_REQUEST
}

pub fn is_error(envelope: &Envelope) -> bool {
    envelope.flags & FLAG_ERROR == FLAG_ERROR
}

fn encode_envelope(envelope: &Envelope) -> Result<Vec<u8>> {
    let payload_len: u32 = envelope
        .payload
        .len()
        .try_into()
        .map_err(|_| anyhow!("payload too large"))?;

    let mut out = Vec::with_capacity(HEADER_LEN + envelope.payload.len());
    out.extend_from_slice(&envelope.version.to_be_bytes());
    out.extend_from_slice(&envelope.method.as_u16().to_be_bytes());
    out.extend_from_slice(&envelope.request_id.to_be_bytes());
    out.extend_from_slice(&envelope.flags.to_be_bytes());
    out.extend_from_slice(&payload_len.to_be_bytes());
    out.extend_from_slice(&envelope.payload);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_endpoint(kind: ContractKind, name: &str) -> PublicEndpointRef {
        PublicEndpointRef {
            workload: selium_control_plane_api::WorkloadRef {
                tenant: "tenant-a".to_string(),
                namespace: "media".to_string(),
                name: "camera".to_string(),
            },
            kind,
            name: name.to_string(),
        }
    }

    fn sample_bridge_intent(kind: ContractKind, name: &str) -> EndpointBridgeIntent {
        EndpointBridgeIntent {
            bridge_id: format!("bridge-{name}"),
            source_instance_id: "source-1".to_string(),
            source_endpoint: sample_endpoint(kind, name),
            target_instance_id: "target-1".to_string(),
            target_node: "node-b".to_string(),
            target_daemon_addr: "127.0.0.1:7100".to_string(),
            target_daemon_server_name: "node-b.local".to_string(),
            target_endpoint: sample_endpoint(kind, name),
            classification: EndpointBridgeClassification::WorkloadDataPlane,
            lifecycle: EndpointBridgeLifecyclePolicy {
                keep_attached_when_healthy: true,
                health: EndpointBridgeHealthPolicy::GuestControlled,
            },
            semantics: match kind {
                ContractKind::Event => EndpointBridgeSemantics::Event(EventBridgeSemantics {
                    delivery: EventDeliveryMode::Frame,
                }),
                ContractKind::Service => EndpointBridgeSemantics::Service(ServiceBridgeSemantics {
                    correlation: ServiceCorrelationMode::RequestId,
                }),
                ContractKind::Stream => EndpointBridgeSemantics::Stream(StreamBridgeSemantics {
                    lifecycle: StreamLifecycleMode::SessionFrames,
                }),
            },
        }
    }

    fn sample_workload_intent(
        instance_id: &str,
        transition: DesiredWorkloadTransition,
    ) -> WorkloadOrchestrationIntent {
        WorkloadOrchestrationIntent {
            workload_key: "tenant-a/media/ingest".to_string(),
            instance_id: instance_id.to_string(),
            transition,
            reason: WorkloadLifecycleReason::Reconcile,
            resource_posture: Some(WorkloadResourcePosture {
                isolation: IsolationProfile::Standard,
                cpu_millis: 250,
                memory_mib: 128,
                ephemeral_storage_mib: 64,
                bandwidth_profile: BandwidthProfile::Standard,
            }),
        }
    }

    #[test]
    fn managed_endpoint_bindings_preserve_kind_role_and_type() {
        let binding = ManagedEndpointBinding {
            endpoint_name: "camera.frames".to_string(),
            endpoint_kind: ContractKind::Event,
            role: ManagedEndpointRole::Egress,
            binding_type: ManagedEndpointBindingType::OneWay,
        };
        let bytes = encode_request(
            Method::StartInstance,
            7,
            &StartRequest {
                node_id: "node-a".to_string(),
                intent: sample_workload_intent("inst-1", DesiredWorkloadTransition::Start),
                module_spec: "path=demo.wasm".to_string(),
                external_account_ref: Some("acct-42".to_string()),
                managed_endpoint_bindings: vec![binding.clone()],
            },
        )
        .expect("encode");
        let envelope = decode_envelope(&bytes).expect("decode envelope");
        let decoded: StartRequest = decode_payload(&envelope).expect("decode payload");
        assert_eq!(decoded.intent.workload_key, "tenant-a/media/ingest");
        assert_eq!(decoded.external_account_ref.as_deref(), Some("acct-42"));
        assert_eq!(decoded.managed_endpoint_bindings, vec![binding]);
    }

    #[test]
    fn endpoint_bridge_activation_round_trips_event_semantics() {
        let request = ActivateEndpointBridgeRequest {
            node_id: "node-a".to_string(),
            intent: sample_bridge_intent(ContractKind::Event, "camera.frames"),
        };
        let bytes = encode_request(Method::ActivateEndpointBridge, 11, &request).expect("encode");
        let envelope = decode_envelope(&bytes).expect("decode envelope");
        let decoded: ActivateEndpointBridgeRequest =
            decode_payload(&envelope).expect("decode payload");
        assert_eq!(decoded, request);
    }

    #[test]
    fn endpoint_bridge_activation_round_trips_stream_session_semantics() {
        let request = ActivateEndpointBridgeRequest {
            node_id: "node-a".to_string(),
            intent: sample_bridge_intent(ContractKind::Stream, "camera.raw"),
        };
        let bytes = encode_request(Method::ActivateEndpointBridge, 12, &request).expect("encode");
        let envelope = decode_envelope(&bytes).expect("decode envelope");
        let decoded: ActivateEndpointBridgeRequest =
            decode_payload(&envelope).expect("decode payload");
        assert_eq!(decoded, request);
    }

    #[test]
    fn deliver_bridge_message_round_trips_service_request_response_semantics() {
        let request = DeliverBridgeMessageRequest {
            target_instance_id: "target-1".to_string(),
            target_endpoint: sample_endpoint(ContractKind::Service, "camera.detect"),
            message: BridgeMessage::Service(ServiceBridgeMessage {
                exchange_id: "req-42".to_string(),
                phase: ServiceMessagePhase::Response,
                sequence: 3,
                complete: true,
                payload: b"ok".to_vec(),
            }),
        };
        let bytes = encode_request(Method::DeliverBridgeMessage, 19, &request).expect("encode");
        let envelope = decode_envelope(&bytes).expect("decode envelope");
        let decoded: DeliverBridgeMessageRequest =
            decode_payload(&envelope).expect("decode payload");
        assert_eq!(decoded, request);
    }

    #[test]
    fn deliver_bridge_message_response_round_trips_service_reply() {
        let response = DeliverBridgeMessageResponse {
            status: "ok".to_string(),
            delivered: true,
            message: Some(BridgeMessage::Service(ServiceBridgeMessage {
                exchange_id: "req-42".to_string(),
                phase: ServiceMessagePhase::Response,
                sequence: 4,
                complete: true,
                payload: b"done".to_vec(),
            })),
        };
        let bytes = encode_response(Method::DeliverBridgeMessage, 29, &response).expect("encode");
        let envelope = decode_envelope(&bytes).expect("decode envelope");
        let decoded: DeliverBridgeMessageResponse =
            decode_payload(&envelope).expect("decode payload");
        assert_eq!(decoded, response);
    }

    #[test]
    fn deliver_bridge_message_round_trips_stream_session_lifecycle_semantics() {
        let request = DeliverBridgeMessageRequest {
            target_instance_id: "target-1".to_string(),
            target_endpoint: sample_endpoint(ContractKind::Stream, "camera.raw"),
            message: BridgeMessage::Stream(StreamBridgeMessage {
                session_id: "session-7".to_string(),
                lifecycle: StreamLifecycle::Close,
                sequence: 9,
                payload: Vec::new(),
            }),
        };
        let bytes = encode_request(Method::DeliverBridgeMessage, 23, &request).expect("encode");
        let envelope = decode_envelope(&bytes).expect("decode envelope");
        let decoded: DeliverBridgeMessageRequest =
            decode_payload(&envelope).expect("decode payload");
        assert_eq!(decoded, request);
    }

    #[test]
    fn control_metrics_response_round_trips() {
        let response = MetricsApiResponse {
            node_id: "node-a".to_string(),
            deployment_count: 2,
            pipeline_count: 1,
            node_count: 3,
            peer_count: 2,
            table_count: 4,
            commit_index: 9,
            last_applied: 9,
            durable_events: Some(12),
        };
        let bytes = encode_response(Method::ControlMetrics, 31, &response).expect("encode");
        let envelope = decode_envelope(&bytes).expect("decode envelope");
        let decoded: MetricsApiResponse = decode_payload(&envelope).expect("decode payload");
        assert_eq!(decoded, response);
    }

    #[test]
    fn control_replay_request_and_response_round_trip_filters_and_bounds() {
        let request = ReplayApiRequest {
            limit: 25,
            since_sequence: None,
            checkpoint: Some("replay-cursor".to_string()),
            save_checkpoint: Some("replay-cursor-next".to_string()),
            external_account_ref: Some("acct-123".to_string()),
            workload: Some("tenant-a/media/ingest".to_string()),
            module: Some("ingest.wasm".to_string()),
            pipeline: Some("tenant-a/media/camera".to_string()),
            node: Some("node-a".to_string()),
        };
        let bytes = encode_request(Method::ControlReplay, 37, &request).expect("encode");
        let envelope = decode_envelope(&bytes).expect("decode envelope");
        let decoded: ReplayApiRequest = decode_payload(&envelope).expect("decode payload");
        assert_eq!(decoded, request);

        let response = ReplayApiResponse {
            events: vec![DataValue::Map(BTreeMap::from([(
                "sequence".to_string(),
                DataValue::from(41_u64),
            )]))],
            start_sequence: Some(41),
            next_sequence: Some(42),
            high_watermark: Some(56),
        };
        let bytes = encode_response(Method::ControlReplay, 38, &response).expect("encode");
        let envelope = decode_envelope(&bytes).expect("decode envelope");
        let decoded: ReplayApiResponse = decode_payload(&envelope).expect("decode payload");
        assert_eq!(decoded, response);
    }

    #[test]
    fn runtime_usage_query_request_and_response_round_trip_filters_and_cursor() {
        let request = RuntimeUsageApiRequest {
            node_id: "node-a".to_string(),
            query: RuntimeUsageQuery {
                start: selium_abi::RuntimeUsageReplayStart::Checkpoint("usage-cursor".to_string()),
                save_checkpoint: Some("usage-cursor-next".to_string()),
                limit: 25,
                external_account_ref: Some("acct-123".to_string()),
                workload: Some("tenant-a/media/ingest".to_string()),
                module: Some("ingest.wasm".to_string()),
                window_start_ms: Some(1_000),
                window_end_ms: Some(2_000),
            },
        };
        let bytes = encode_request(Method::RuntimeUsageQuery, 39, &request).expect("encode");
        let envelope = decode_envelope(&bytes).expect("decode envelope");
        let decoded: RuntimeUsageApiRequest = decode_payload(&envelope).expect("decode payload");
        assert_eq!(decoded, request);

        let response = RuntimeUsageApiResponse {
            records: vec![RuntimeUsageRecord {
                sequence: 41,
                timestamp_ms: 1_500,
                headers: BTreeMap::from([("module_id".to_string(), "ingest.wasm".to_string())]),
                sample: selium_abi::RuntimeUsageSample {
                    workload_key: "tenant-a/media/ingest".to_string(),
                    process_id: "process-7".to_string(),
                    attribution: selium_abi::RuntimeUsageAttribution {
                        external_account_ref: Some("acct-123".to_string()),
                        module_id: "ingest.wasm".to_string(),
                    },
                    window_start_ms: 1_000,
                    window_end_ms: 2_000,
                    trigger: selium_abi::RuntimeUsageSampleTrigger::Interval,
                    cpu_time_millis: 1_000,
                    memory_high_watermark_bytes: 4_096,
                    memory_byte_millis: 4_096_000,
                    ingress_bytes: 7,
                    egress_bytes: 11,
                    storage_read_bytes: 13,
                    storage_write_bytes: 17,
                },
            }],
            next_sequence: Some(42),
            high_watermark: Some(56),
        };
        let bytes = encode_response(Method::RuntimeUsageQuery, 40, &response).expect("encode");
        let envelope = decode_envelope(&bytes).expect("decode envelope");
        let decoded: RuntimeUsageApiResponse = decode_payload(&envelope).expect("decode payload");
        assert_eq!(decoded, response);
    }

    #[test]
    fn subscribe_guest_logs_round_trips_request() {
        let request = SubscribeGuestLogsRequest {
            target: OperationalProcessSelector::Workload(selium_control_plane_api::WorkloadRef {
                tenant: "tenant-a".to_string(),
                namespace: "media".to_string(),
                name: "camera".to_string(),
            }),
            stream_names: vec!["stdout".to_string(), "stderr".to_string()],
        };
        let bytes = encode_request(Method::SubscribeGuestLogs, 31, &request).expect("encode");
        let envelope = decode_envelope(&bytes).expect("decode envelope");
        let decoded: SubscribeGuestLogsRequest = decode_payload(&envelope).expect("decode payload");
        assert_eq!(decoded, request);
    }

    #[test]
    fn subscribe_guest_logs_round_trips_response_and_frames() {
        let response = SubscribeGuestLogsResponse {
            status: "ok".to_string(),
            target_node: "node-a".to_string(),
            target_instance_id: "tenant=tenant-a;namespace=media;workload=camera;replica=0"
                .to_string(),
            streams: vec![sample_endpoint(ContractKind::Event, "stdout")],
        };
        let bytes = encode_response(Method::SubscribeGuestLogs, 32, &response).expect("encode");
        let envelope = decode_envelope(&bytes).expect("decode envelope");
        let decoded: SubscribeGuestLogsResponse =
            decode_payload(&envelope).expect("decode payload");
        assert_eq!(decoded, response);

        let event = GuestLogEvent {
            endpoint: sample_endpoint(ContractKind::Event, "stderr"),
            payload: b"guest log".to_vec(),
        };
        let bytes = encode_rkyv(&event).expect("encode event");
        let decoded: GuestLogEvent = decode_rkyv(&bytes).expect("decode event");
        assert_eq!(decoded, event);
    }

    #[test]
    fn list_response_decodes_current_payload() {
        let response = ListResponse {
            instances: BTreeMap::from([("tenant-a/media/ingest/0".to_string(), 42usize)]),
            active_bridges: vec!["bridge-1".to_string()],
            observed_memory_bytes: Some(5_242_880),
            observed_workloads: BTreeMap::from([("tenant-a/media/ingest".to_string(), 1u32)]),
            observed_workload_memory_bytes: BTreeMap::from([(
                "tenant-a/media/ingest".to_string(),
                5_242_880u64,
            )]),
        };
        let bytes = encode_response(Method::ListInstances, 41, &response).expect("encode");
        let envelope = decode_envelope(&bytes).expect("decode envelope");

        let decoded = decode_list_response(&envelope).expect("decode list response");

        assert_eq!(decoded, response);
    }
}
