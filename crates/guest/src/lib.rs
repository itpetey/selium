//! Selium Guest - Guest-side utilities for WASM modules.
//!
//! This crate re-exports from selium-guest-runtime for backward compatibility.
//! New code should use selium-guest-runtime directly.

pub mod mailbox;

pub use selium_guest_runtime::Attribution;
pub use selium_guest_runtime::FutureSharedState;
pub use selium_guest_runtime::GuestError;
pub use selium_guest_runtime::GuestResult;
pub use selium_guest_runtime::JoinHandle;
pub use selium_guest_runtime::RpcCall;
pub use selium_guest_runtime::RpcCallFuture;
pub use selium_guest_runtime::RpcClient;
pub use selium_guest_runtime::RpcEnvelope;
pub use selium_guest_runtime::RpcResponse;
pub use selium_guest_runtime::RpcServer;
pub use selium_guest_runtime::async_;
pub use selium_guest_runtime::block_on;
pub use selium_guest_runtime::error;
pub use selium_guest_runtime::next_call_id;
pub use selium_guest_runtime::rpc;
pub use selium_guest_runtime::shutdown;
pub use selium_guest_runtime::spawn;
pub use selium_guest_runtime::yield_now;
