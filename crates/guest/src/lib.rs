//! Selium guest SDK.

mod async_runtime;
mod codec;
mod error;
mod hostcall;
mod memory;
mod network;
mod platform;
mod process;
mod signal;
mod storage;

pub use async_runtime::{
    JoinHandle, poll_reactor, poll_safely, run_entrypoint_safely, spawn, yield_now,
};
pub use codec::{decode_typed, encode_typed};
pub use error::{GuestError, Result};
pub use memory::{SharedMemory, SharedRegion};
pub use network::{NetworkListener, NetworkSession, NetworkStream, RequestExchange};
pub use platform::{mark_ready, process_id};
pub use process::{ActivityLog, GuestLog, Metering, Process};
pub use selium_abi::{
    Capability, CapabilityGrant, EntrypointMetadata, InterfaceMetadata, LocalityScope,
    ResourceClass, ResourceIdentity, ResourceSelector, ScopeContext,
};
pub use selium_guest_macros::{entrypoint, pattern_interface};
pub use signal::Signal;
pub use storage::{BlobStore, DurableLog};
pub use tracing::{debug, error, info, trace, warn};
