//! Shared control-plane daemon protocol and binary envelope utilities.

use std::collections::BTreeMap;

use anyhow::{Context, Result, anyhow};
use rkyv::{
    Archive, Deserialize, Serialize,
    api::high::{HighDeserializer, HighValidator},
};
use selium_abi::{DataValue, RkyvEncode, decode_rkyv, encode_rkyv};
use selium_control_plane_api::EventEndpointRef;
use selium_control_plane_runtime::{Mutation, Query};
use selium_io_consensus::{AppendEntries, RequestVote};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const PROTOCOL_VERSION: u16 = 1;
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
    StartInstance = 100,
    StopInstance = 101,
    ListInstances = 102,
    ActivateEventRoute = 103,
    DeactivateEventRoute = 104,
    DeliverEventFrame = 105,
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
            100 => Some(Self::StartInstance),
            101 => Some(Self::StopInstance),
            102 => Some(Self::ListInstances),
            103 => Some(Self::ActivateEventRoute),
            104 => Some(Self::DeactivateEventRoute),
            105 => Some(Self::DeliverEventFrame),
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
    pub idempotency_key: String,
    pub mutation: Mutation,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct QueryApiRequest {
    pub query: Query,
    pub allow_stale: bool,
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
pub struct ReplayApiRequest {
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ReplayApiResponse {
    pub events: Vec<DataValue>,
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
pub enum ManagedEventBindingRole {
    Source,
    Target,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ManagedEventBinding {
    pub endpoint_name: String,
    pub role: ManagedEventBindingRole,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct StartRequest {
    pub node_id: String,
    pub instance_id: String,
    pub module_spec: String,
    pub managed_event_bindings: Vec<ManagedEventBinding>,
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
    pub instance_id: String,
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
    pub active_routes: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum EventRouteMode {
    Local,
    Remote,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ActivateEventRouteRequest {
    pub node_id: String,
    pub route_id: String,
    pub source_instance_id: String,
    pub source_endpoint: EventEndpointRef,
    pub target_instance_id: String,
    pub target_node: String,
    pub target_endpoint: EventEndpointRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ActivateEventRouteResponse {
    pub status: String,
    pub route_id: String,
    pub mode: EventRouteMode,
    pub target_node: String,
    pub target_instance_id: String,
    pub already_active: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct DeactivateEventRouteRequest {
    pub node_id: String,
    pub route_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct DeactivateEventRouteResponse {
    pub status: String,
    pub route_id: String,
    pub existed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct DeliverEventFrameRequest {
    pub target_instance_id: String,
    pub target_endpoint_name: String,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct DeliverEventFrameResponse {
    pub status: String,
    pub delivered: bool,
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

pub async fn write_framed<W>(writer: &mut W, payload: &[u8]) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let len: u32 = payload
        .len()
        .try_into()
        .map_err(|_| anyhow!("frame too large"))?;
    writer.write_all(&len.to_be_bytes()).await?;
    writer.write_all(payload).await?;
    writer.flush().await?;
    Ok(())
}

pub async fn read_framed<R>(reader: &mut R) -> Result<Vec<u8>>
where
    R: AsyncRead + Unpin,
{
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut payload = vec![0u8; len];
    reader.read_exact(&mut payload).await?;
    Ok(payload)
}
