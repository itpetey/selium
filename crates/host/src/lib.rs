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
//! - Async I/O (storage, network, queue)
//! - Capability delegation and isolation

pub mod async_host;
pub mod async_host_functions;
pub mod capabilities;
pub mod error;
pub mod guest;
pub mod handles;
pub mod hostcalls;
pub mod kernel;
pub mod metering;
pub mod network;
pub mod process;
pub mod queue;
pub mod storage;
pub mod time;

pub use async_host::{AsyncHostExtension, TaskId, next_task_id};
pub use capabilities::{CapabilityGrant, CapabilityRegistry, GuestNamespace};
pub use error::{Error, GuestExitStatus, Result};
pub use guest::{Guest, GuestId};
pub use handles::{AnyHandle, HandleId, NetworkHandle, StorageHandle, next_handle_id};
pub use hostcalls::{DeprecatedHostcall, HOST_VERSION, HostcallDispatcher, HostcallVersion};
pub use kernel::{Capability, Kernel};
pub use metering::UsageMeter;
pub use network::{NetworkCapability, NetworkError, NetworkResult};
pub use process::{ProcessHandle, ProcessId};
pub use queue::{QueueCapability, QueueError, QueueHandle, QueueResult};
pub use storage::{StorageCapability, StorageError, StorageResult};
pub use time::TimeSource;
