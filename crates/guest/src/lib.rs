//! Selium guest SDK.

mod async_runtime;
mod codec;
mod error;
mod hostcall;
mod platform;
mod signal;

pub use async_runtime::{
    JoinHandle, poll_reactor, poll_safely, run_entrypoint_safely, spawn, yield_now,
};
pub use codec::{decode_typed, encode_typed};
pub use error::{GuestError, Result};
pub use platform::{mark_ready, process_id};
pub use selium_abi::{
    Capability, CapabilityGrant, EntrypointMetadata, InterfaceMetadata, LocalityScope,
    ResourceClass, ResourceIdentity, ResourceSelector, ScopeContext,
};
pub use selium_guest_macros::{entrypoint, pattern_interface};
pub use signal::Signal;
pub use tracing::{debug, error, info, trace, warn};
