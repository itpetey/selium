//! Selium Host - A minimal kernel providing raw materials for WASM guests.
//!
//! The host provides:
//! - WASM execution (via wasmtime)
//! - Capability registry and enforcement
//! - Process lifecycle (spawn, stop, JoinHandle)
//! - Time primitives (monotonic and wall clock)
//! - Memory management for guests
//! - Hostcall dispatch with versioning
//! - Usage metering

pub mod error;
pub mod kernel;
pub mod guest;
pub mod hostcalls;
pub mod time;
pub mod metering;
pub mod process;

pub use error::{Error, Result, GuestExitStatus};
pub use kernel::{Kernel, Capability};
pub use guest::{Guest, GuestId};
pub use hostcalls::{HostcallDispatcher, HostcallVersion, DeprecatedHostcall, HOST_VERSION};
pub use time::TimeSource;
pub use metering::UsageMeter;
pub use process::{ProcessHandle, ProcessId};
