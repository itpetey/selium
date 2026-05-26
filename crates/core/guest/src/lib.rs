//! Selium guest SDK.

pub use selium_abi::{
    Capability, CapabilityGrant, DiscoveryRequest, DiscoveryResponse, EntrypointMetadata,
    InterfaceMetadata, LocalityScope, ResourceClass, ResourceIdentity, ResourceSelector,
    ResourceTarget, ScopeContext,
};
pub use selium_guest_macros::{entrypoint, pattern_interface};
pub use tracing::{debug, error, info, trace, warn};

pub use crate::{
    async_runtime::{
        JoinHandle, poll_reactor, poll_safely, run_entrypoint_safely, spawn, yield_now,
    },
    codec::{decode_typed, encode_typed},
    error::{GuestError, Result},
    memory::{SHARED_REGION_MAGIC, SharedMemory, SharedRegion, SharedRegionBuilder},
    network::{NetworkListener, NetworkSession, NetworkStream, RequestExchange},
    platform::{mark_ready, process_id},
    process::{ActivityLog, GuestLog, Metering, Process},
    resource::{Accept, IncomingConnection, ResourceListener, ResourceSender},
    signal::Signal,
    storage::{BlobStore, DurableLog},
};

mod async_runtime;
mod codec;
mod error;
mod hostcall;
mod memory;
mod network;
mod platform;
mod process;
mod resource;
mod signal;
mod storage;
