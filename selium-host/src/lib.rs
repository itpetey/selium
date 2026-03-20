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
pub mod guest;
pub mod hostcalls;
pub mod kernel;
pub mod metering;
pub mod process;
pub mod time;

pub use error::{Error, GuestExitStatus, Result};
pub use guest::{Guest, GuestId};
pub use hostcalls::{DeprecatedHostcall, HostcallDispatcher, HostcallVersion, HOST_VERSION};
pub use kernel::{Capability, Kernel};
pub use metering::UsageMeter;
pub use process::{ProcessHandle, ProcessId};
pub use time::TimeSource;
