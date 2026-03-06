//! Protocol-neutral network payload types for guest I/O.

use std::collections::BTreeMap;

use rkyv::{Archive, Deserialize, Serialize};

use crate::GuestUint;

/// Supported network protocols.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum NetworkProtocol {
    /// QUIC transport.
    Quic = 0,
    /// HTTP over TLS.
    Http = 1,
}

/// Interaction mode supported by a protocol endpoint.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum InteractionKind {
    /// Bidirectional byte streaming.
    Stream = 0,
    /// Request/response RPC.
    Rpc = 1,
}

/// Status code returned by network operations.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum NetworkStatusCode {
    /// Operation completed successfully.
    Ok = 0,
    /// Operation would block and was not allowed to wait.
    WouldBlock = 1,
    /// Operation timed out.
    Timeout = 2,
    /// Resource is closed.
    Closed = 3,
    /// Requested resource was not found.
    NotFound = 4,
    /// Caller lacks permission.
    PermissionDenied = 5,
    /// Requested protocol/interaction is unsupported.
    Unsupported = 6,
    /// Input was invalid.
    InvalidArgument = 7,
    /// Internal failure.
    Internal = 255,
}

/// Open a runtime-managed ingress binding.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct NetworkListen {
    /// Logical ingress binding name.
    pub binding_name: String,
}

/// Connect to an outbound remote authority using the selected protocol and profile.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct NetworkConnect {
    /// Transport protocol to use.
    pub protocol: NetworkProtocol,
    /// Logical egress profile name.
    pub profile_name: String,
    /// Remote authority, such as `localhost:7443`.
    pub authority: String,
}

/// Accept an inbound session from a listener.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct NetworkAccept {
    /// Local listener resource identifier.
    pub listener_id: GuestUint,
    /// Wait timeout in milliseconds (`0` for non-blocking).
    pub timeout_ms: u32,
}

/// Open an outbound stream on a protocol session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct NetworkStreamOpen {
    /// Local protocol session resource identifier.
    pub session_id: GuestUint,
}

/// Accept an inbound stream on a protocol session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct NetworkStreamAccept {
    /// Local protocol session resource identifier.
    pub session_id: GuestUint,
    /// Wait timeout in milliseconds (`0` for non-blocking).
    pub timeout_ms: u32,
}

/// Send a byte chunk on an open stream.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct NetworkStreamSend {
    /// Local stream resource identifier.
    pub stream_id: GuestUint,
    /// Payload bytes to write.
    pub bytes: Vec<u8>,
    /// Whether this write should finish the stream.
    pub finish: bool,
    /// Wait timeout in milliseconds (`0` for non-blocking).
    pub timeout_ms: u32,
}

/// Read a byte chunk from an open stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct NetworkStreamRecv {
    /// Local stream resource identifier.
    pub stream_id: GuestUint,
    /// Maximum bytes to read into one chunk.
    pub max_bytes: u32,
    /// Wait timeout in milliseconds (`0` for non-blocking).
    pub timeout_ms: u32,
}

/// Invoke one buffered RPC against a protocol session.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct NetworkRpcInvoke {
    /// Local protocol session resource identifier.
    pub session_id: GuestUint,
    /// Request head metadata.
    pub request: NetworkRpcRequestHead,
    /// Start timeout in milliseconds (`0` for non-blocking where supported).
    pub timeout_ms: u32,
}

/// Await the response head for an outbound RPC exchange.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct NetworkRpcAwait {
    /// Local outbound RPC exchange resource identifier.
    pub exchange_id: GuestUint,
    /// Wait timeout in milliseconds (`0` for non-blocking).
    pub timeout_ms: u32,
}

/// Accept one inbound buffered RPC on a protocol session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct NetworkRpcAccept {
    /// Local protocol session resource identifier.
    pub session_id: GuestUint,
    /// Wait timeout in milliseconds (`0` for non-blocking).
    pub timeout_ms: u32,
}

/// Respond to an inbound buffered RPC.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct NetworkRpcRespond {
    /// Local RPC exchange resource identifier.
    pub exchange_id: GuestUint,
    /// Response head metadata.
    pub response: NetworkRpcResponseHead,
    /// Wait timeout in milliseconds (`0` for non-blocking).
    pub timeout_ms: u32,
}

/// Write a byte chunk to an RPC body writer.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct NetworkRpcBodyWrite {
    /// Local body writer resource identifier.
    pub body_id: GuestUint,
    /// Payload bytes to write.
    pub bytes: Vec<u8>,
    /// Whether this write finishes the body.
    pub finish: bool,
    /// Wait timeout in milliseconds (`0` for non-blocking).
    pub timeout_ms: u32,
}

/// Read a byte chunk from an RPC body reader.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct NetworkRpcBodyRead {
    /// Local body reader resource identifier.
    pub body_id: GuestUint,
    /// Maximum bytes to read into one chunk.
    pub max_bytes: u32,
    /// Wait timeout in milliseconds (`0` for non-blocking).
    pub timeout_ms: u32,
}

/// Close any local network resource.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct NetworkClose {
    /// Local resource identifier.
    pub resource_id: GuestUint,
}

/// Protocol-neutral RPC request head.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct NetworkRpcRequestHead {
    /// Protocol-level method or verb.
    pub method: String,
    /// Protocol-level target path or operation name.
    pub path: String,
    /// Protocol metadata or headers.
    pub metadata: BTreeMap<String, String>,
}

/// Protocol-neutral RPC response head.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct NetworkRpcResponseHead {
    /// Response status code.
    pub status: u16,
    /// Protocol metadata or headers.
    pub metadata: BTreeMap<String, String>,
}

/// Buffered RPC request.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct NetworkRpcRequest {
    /// Request head metadata.
    pub head: NetworkRpcRequestHead,
    /// Buffered request body.
    pub body: Vec<u8>,
}

/// Buffered RPC response.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct NetworkRpcResponse {
    /// Response head metadata.
    pub head: NetworkRpcResponseHead,
    /// Buffered response body.
    pub body: Vec<u8>,
}

/// Local listener descriptor returned by `listen`.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct NetworkListenerDescriptor {
    /// Local listener resource identifier.
    pub resource_id: GuestUint,
    /// Protocol served by this listener.
    pub protocol: NetworkProtocol,
    /// Interaction modes accepted by this listener.
    pub interactions: Vec<InteractionKind>,
}

/// Local protocol session descriptor returned by `connect` or `accept`.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct NetworkSessionDescriptor {
    /// Local session resource identifier.
    pub resource_id: GuestUint,
    /// Protocol associated with the session.
    pub protocol: NetworkProtocol,
    /// Interaction modes supported by the session.
    pub interactions: Vec<InteractionKind>,
}

/// Local stream descriptor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct NetworkStreamDescriptor {
    /// Local stream resource identifier.
    pub resource_id: GuestUint,
}

/// Local RPC exchange descriptor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct NetworkRpcExchangeDescriptor {
    /// Local exchange resource identifier.
    pub resource_id: GuestUint,
}

/// Local outbound RPC client exchange descriptor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct NetworkRpcClientExchangeDescriptor {
    /// Local exchange resource identifier.
    pub resource_id: GuestUint,
}

/// Local RPC body reader descriptor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct NetworkRpcBodyReaderDescriptor {
    /// Local body reader resource identifier.
    pub resource_id: GuestUint,
}

/// Local RPC body writer descriptor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct NetworkRpcBodyWriterDescriptor {
    /// Local body writer resource identifier.
    pub resource_id: GuestUint,
}

/// Common network status result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct NetworkStatus {
    /// Operation outcome.
    pub code: NetworkStatusCode,
}

/// Result of accepting a protocol session.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct NetworkAcceptResult {
    /// Operation outcome.
    pub code: NetworkStatusCode,
    /// Accepted session descriptor when available.
    pub session: Option<NetworkSessionDescriptor>,
}

/// Result of opening or accepting a stream.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct NetworkStreamResult {
    /// Operation outcome.
    pub code: NetworkStatusCode,
    /// Stream descriptor when successful.
    pub stream: Option<NetworkStreamDescriptor>,
}

/// One received stream chunk.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct NetworkStreamChunk {
    /// Chunk bytes.
    pub bytes: Vec<u8>,
    /// Whether this chunk observed stream finish.
    pub finish: bool,
}

/// Result of reading a stream chunk.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct NetworkStreamRecvResult {
    /// Operation outcome.
    pub code: NetworkStatusCode,
    /// Received bytes when successful.
    pub chunk: Option<NetworkStreamChunk>,
}

/// Result of starting an outbound RPC exchange.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct NetworkRpcInvokeResult {
    /// Operation outcome.
    pub code: NetworkStatusCode,
    /// Client exchange descriptor when successful.
    pub exchange: Option<NetworkRpcClientExchangeDescriptor>,
    /// Request body writer descriptor when successful.
    pub request_body: Option<NetworkRpcBodyWriterDescriptor>,
}

/// Result of awaiting an outbound RPC response head.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct NetworkRpcResponseResult {
    /// Operation outcome.
    pub code: NetworkStatusCode,
    /// Response head when successful.
    pub response: Option<NetworkRpcResponseHead>,
    /// Response body reader when successful.
    pub response_body: Option<NetworkRpcBodyReaderDescriptor>,
}

/// Result of accepting an inbound RPC request.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct NetworkRpcAcceptResult {
    /// Operation outcome.
    pub code: NetworkStatusCode,
    /// Exchange descriptor when successful.
    pub exchange: Option<NetworkRpcExchangeDescriptor>,
    /// Accepted request head.
    pub request: Option<NetworkRpcRequestHead>,
    /// Accepted request body reader.
    pub request_body: Option<NetworkRpcBodyReaderDescriptor>,
}

/// Result of writing an inbound RPC response head.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct NetworkRpcRespondResult {
    /// Operation outcome.
    pub code: NetworkStatusCode,
    /// Response body writer descriptor when successful.
    pub response_body: Option<NetworkRpcBodyWriterDescriptor>,
}

/// Result of reading an RPC body chunk.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct NetworkRpcBodyReadResult {
    /// Operation outcome.
    pub code: NetworkStatusCode,
    /// Received bytes when successful.
    pub chunk: Option<NetworkStreamChunk>,
}

impl NetworkRpcRequest {
    /// Split a buffered request into head and body.
    pub fn into_head_and_body(self) -> (NetworkRpcRequestHead, Vec<u8>) {
        (self.head, self.body)
    }
}

impl NetworkRpcResponse {
    /// Build a buffered response from a head and collected body bytes.
    pub fn from_head_and_body(head: NetworkRpcResponseHead, body: Vec<u8>) -> Self {
        Self { head, body }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn network_protocol_values_are_stable() {
        assert_eq!(NetworkProtocol::Quic as u8, 0);
        assert_eq!(NetworkProtocol::Http as u8, 1);
    }

    #[test]
    fn rpc_request_round_trips_basic_fields() {
        let request = NetworkRpcRequest {
            head: NetworkRpcRequestHead {
                method: "GET".to_string(),
                path: "/health".to_string(),
                metadata: BTreeMap::from([(String::from("accept"), String::from("text/plain"))]),
            },
            body: Vec::new(),
        };
        assert_eq!(request.head.method, "GET");
        assert_eq!(request.head.path, "/health");
        assert_eq!(
            request.head.metadata.get("accept").map(String::as_str),
            Some("text/plain")
        );
    }
}
