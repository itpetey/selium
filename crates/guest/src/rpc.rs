//! RPC framework for inter-guest communication.
//!
//! Built on queue handles with attribution-based routing.
//! - RpcEnvelope: Request envelope with call_id, method, params
//! - RpcResponse: Response envelope with call_id and result
//! - RpcClient: Async client for making RPC calls
//! - RpcServer: Server for handling RPC requests

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::r#async::future::FutureSharedState;
use crate::error::{GuestError, GuestResult};

static NEXT_CALL_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

pub fn next_call_id() -> u64 {
    NEXT_CALL_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RpcEnvelope {
    pub call_id: u64,
    pub method: String,
    pub params: Vec<u8>,
}

impl RpcEnvelope {
    pub fn new(method: impl Into<String>, params: Vec<u8>) -> Self {
        Self {
            call_id: next_call_id(),
            method: method.into(),
            params,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RpcResponse {
    pub call_id: u64,
    pub result: std::result::Result<Vec<u8>, String>,
}

impl RpcResponse {
    pub fn ok(call_id: u64, data: Vec<u8>) -> Self {
        Self {
            call_id,
            result: Ok(data),
        }
    }

    pub fn error(call_id: u64, err: impl Into<String>) -> Self {
        Self {
            call_id,
            result: Err(err.into()),
        }
    }
}

#[derive(Debug)]
pub struct RpcCall {
    pub call_id: u64,
    pub method: String,
    pub params: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct Attribution {
    pub source_guest_id: u64,
    pub trace_id: Option<u64>,
}

impl Attribution {
    pub fn new(source_guest_id: u64) -> Self {
        Self {
            source_guest_id,
            trace_id: None,
        }
    }

    pub fn with_trace(mut self, trace_id: u64) -> Self {
        self.trace_id = Some(trace_id);
        self
    }
}

type RpcHandler = Box<dyn Fn(RpcCall, Attribution) -> GuestResult<Vec<u8>> + Send + Sync>;

pub struct RpcServer {
    handlers: Arc<parking_lot::RwLock<HashMap<String, RpcHandler>>>,
}

impl RpcServer {
    pub fn new() -> Self {
        Self {
            handlers: Arc::new(parking_lot::RwLock::new(HashMap::new())),
        }
    }

    pub fn register<H>(&self, method: &str, handler: H)
    where
        H: Fn(RpcCall, Attribution) -> GuestResult<Vec<u8>> + Send + Sync + 'static,
    {
        let handler = Box::new(handler);

        let mut handlers = self.handlers.write();
        handlers.insert(method.to_string(), handler);
    }

    pub fn dispatch(
        &self,
        envelope: RpcEnvelope,
        attribution: Attribution,
    ) -> GuestResult<RpcResponse> {
        let handlers = self.handlers.read();

        let handler = handlers
            .get(&envelope.method)
            .ok_or_else(|| GuestError::Error(format!("Unknown method: {}", envelope.method)))?;

        let call = RpcCall {
            call_id: envelope.call_id,
            method: envelope.method,
            params: envelope.params,
        };

        let result = handler(call, attribution);

        match result {
            Ok(data) => Ok(RpcResponse::ok(envelope.call_id, data)),
            Err(e) => Ok(RpcResponse::error(envelope.call_id, e.to_string())),
        }
    }
}

impl Default for RpcServer {
    fn default() -> Self {
        Self::new()
    }
}

type PendingCallState = Arc<FutureSharedState<Vec<u8>, String>>;

pub struct RpcClient {
    outbound_queue: Arc<dyn Fn(Vec<u8>) + Send + Sync>,
    #[allow(clippy::type_complexity)]
    pending_calls: Arc<parking_lot::RwLock<HashMap<u64, PendingCallState>>>,
    call_id_counter: Arc<std::sync::atomic::AtomicU64>,
}

impl RpcClient {
    pub fn new<F>(outbound: F) -> Self
    where
        F: Fn(Vec<u8>) + Send + Sync + 'static,
    {
        Self {
            outbound_queue: Arc::new(outbound),
            pending_calls: Arc::new(parking_lot::RwLock::new(HashMap::new())),
            call_id_counter: Arc::new(std::sync::atomic::AtomicU64::new(1)),
        }
    }

    pub fn next_call_id(&self) -> u64 {
        self.call_id_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    pub fn call<P: serde::Serialize>(&self, method: &str, params: &P) -> RpcCallFuture {
        let call_id = self.next_call_id();
        let params_bytes = serde_json::to_vec(params).unwrap_or_default();

        let envelope = RpcEnvelope {
            call_id,
            method: method.to_string(),
            params: params_bytes,
        };

        let state = Arc::new(FutureSharedState::new());
        let state_clone = state.clone();

        {
            let mut pending = self.pending_calls.write();
            pending.insert(call_id, state_clone);
        }

        let envelope_bytes = serde_json::to_vec(&envelope).unwrap_or_default();
        (self.outbound_queue)(envelope_bytes);

        RpcCallFuture {
            call_id,
            state,
            pending_calls: self.pending_calls.clone(),
        }
    }

    pub fn handle_response(&self, response: RpcResponse) {
        let mut pending = self.pending_calls.write();
        if let Some(state) = pending.remove(&response.call_id) {
            match response.result {
                Ok(data) => state.complete(Ok(data)),
                Err(e) => state.complete(Err(e)),
            }
        }
    }
}

pub struct RpcCallFuture {
    call_id: u64,
    state: PendingCallState,
    #[allow(clippy::type_complexity)]
    pending_calls: Arc<parking_lot::RwLock<HashMap<u64, PendingCallState>>>,
}

impl Future for RpcCallFuture {
    type Output = std::result::Result<Vec<u8>, String>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(result) = self.state.take_result() {
            return Poll::Ready(result);
        }

        self.state.register_waker(cx.waker().clone());

        Poll::Pending
    }
}

impl Drop for RpcCallFuture {
    fn drop(&mut self) {
        let mut pending = self.pending_calls.write();
        pending.remove(&self.call_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rpc_envelope_new() {
        let envelope = RpcEnvelope::new("test_method", vec![1, 2, 3]);
        assert_eq!(envelope.method, "test_method");
        assert_eq!(envelope.params, vec![1, 2, 3]);
        assert!(envelope.call_id > 0);
    }

    #[test]
    fn test_rpc_response_ok() {
        let response = RpcResponse::ok(42, vec![1, 2, 3]);
        assert_eq!(response.call_id, 42);
        assert!(response.result.is_ok());
        assert_eq!(response.result.unwrap(), vec![1, 2, 3]);
    }

    #[test]
    fn test_rpc_response_error() {
        let response = RpcResponse::error(42, "test error");
        assert_eq!(response.call_id, 42);
        assert!(response.result.is_err());
        assert_eq!(response.result.unwrap_err(), "test error");
    }

    #[test]
    fn test_attribution_new() {
        let attr = Attribution::new(123);
        assert_eq!(attr.source_guest_id, 123);
        assert!(attr.trace_id.is_none());
    }

    #[test]
    fn test_attribution_with_trace() {
        let attr = Attribution::new(123).with_trace(456);
        assert_eq!(attr.source_guest_id, 123);
        assert_eq!(attr.trace_id, Some(456));
    }

    #[test]
    fn test_rpc_server_dispatch_unknown_method() {
        let server = RpcServer::new();
        let envelope = RpcEnvelope::new("unknown", vec![]);
        let attribution = Attribution::new(1);

        let result = server.dispatch(envelope, attribution);
        assert!(result.is_err());
    }

    #[test]
    fn test_next_call_id_increments() {
        let id1 = next_call_id();
        let id2 = next_call_id();
        assert!(id2 > id1);
    }
}
