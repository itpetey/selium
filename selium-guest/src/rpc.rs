//! RPC framework types.
//!
//! Provides unified RPC between guests built on queue handles.

/// RPC envelope for requests.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RpcEnvelope {
    /// Call ID for matching responses to requests.
    pub call_id: u64,
    /// Method name to invoke.
    pub method: String,
    /// Serialized method parameters.
    pub params: Vec<u8>,
}

/// RPC response.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RpcResponse {
    /// Call ID matching the request.
    pub call_id: u64,
    /// Serialized result or error.
    pub result: Result<Vec<u8>, String>,
}

/// A pending RPC call waiting for a response.
pub struct PendingCall {
    pub call_id: u64,
    pub response: tokio::sync::oneshot::Receiver<RpcResponse>,
}
