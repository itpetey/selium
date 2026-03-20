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

pub mod error;
pub mod guest;
pub mod hostcalls;
pub mod kernel;
pub mod metering;
pub mod process;
pub mod time;
pub mod async_host;
pub mod async_host_functions;
pub mod storage;
pub mod network;
pub mod queue;
pub mod handles;
pub mod capabilities;

pub use error::{Error, GuestExitStatus, Result};
pub use guest::{Guest, GuestId};
pub use hostcalls::{DeprecatedHostcall, HostcallDispatcher, HostcallVersion, HOST_VERSION};
pub use kernel::{Capability, Kernel};
pub use metering::UsageMeter;
pub use process::{ProcessHandle, ProcessId};
pub use time::TimeSource;
pub use async_host::{AsyncHostExtension, next_task_id, TaskId};
pub use storage::{StorageCapability, StorageError, StorageResult};
pub use network::{NetworkCapability, NetworkError, NetworkResult};
pub use queue::{QueueCapability, QueueError, QueueResult, QueueHandle};
pub use handles::{StorageHandle, NetworkHandle, AnyHandle, HandleId, next_handle_id};
pub use capabilities::{
    CapabilityRegistry, GuestNamespace, CapabilityGrant,
};
