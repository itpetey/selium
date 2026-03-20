//! Selium Guest - Guest-side utilities for WASM modules.
//!
//! Provides:
//! - Cooperative multitasking (spawn, yield_now)
//! - Mailbox ring buffer for host → guest wake
//! - FutureSharedState for bridging host async to guest
//! - Shutdown signaling
//! - RPC framework
//! - Guest modules: init, consensus, scheduler, discovery, supervisor, routing

pub mod async_;
pub mod consensus;
pub mod discovery;
pub mod error;
pub mod init;
pub mod mailbox;
pub mod routing;
pub mod rpc;
pub mod scheduler;
pub mod supervisor;

pub use async_::{JoinHandle, block_on, shutdown, spawn, yield_now};
pub use error::{GuestError, GuestResult};
