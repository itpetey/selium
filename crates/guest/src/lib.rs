//! Selium Guest - Shared library for WASM guest modules.
//!
//! Provides:
//! - Cooperative multitasking (spawn, yield_now)
//! - FutureSharedState for bridging host async to guest
//! - Shutdown signaling
//! - RPC framework for inter-guest communication
//! - Error types (GuestError, GuestResult)

pub mod r#async;
pub mod error;
pub mod mailbox;
pub mod rpc;

pub use r#async::{FutureSharedState, JoinHandle, block_on, shutdown, spawn, yield_now};
pub use error::{GuestError, GuestResult};
pub use rpc::{
    Attribution, RpcCall, RpcCallFuture, RpcClient, RpcEnvelope, RpcResponse, RpcServer,
    next_call_id,
};
