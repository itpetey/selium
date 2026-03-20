//! Selium Guest - Guest-side utilities for WASM modules.
//!
//! Provides:
//! - Cooperative multitasking (spawn, yield_now)
//! - Mailbox ring buffer for host → guest wake
//! - FutureSharedState for bridging host async to guest
//! - Shutdown signaling
//! - RPC framework

pub mod async_;
pub mod error;
pub mod spawn;
pub mod yield_;
pub mod shutdown;
pub mod wait;
pub mod mailbox;
pub mod rpc;

pub use async_::{block_on, spawn, yield_now, shutdown};
pub use error::{GuestError, GuestResult};
pub use spawn::{spawn, JoinHandle};
pub use yield_::yield_now;
pub use shutdown::shutdown;
pub use wait::wait;
