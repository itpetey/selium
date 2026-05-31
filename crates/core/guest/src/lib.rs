//! Selium guest SDK.

pub use crate::{
    async_runtime::{
        JoinHandle, poll_reactor, poll_safely, run_entrypoint_safely, spawn, yield_now,
    },
    codec::{decode_typed, encode_typed},
    context::Context,
    error::{GuestError, Result},
    memory::{SHARED_REGION_MAGIC, SharedMemory, SharedRegion, SharedRegionBuilder},
    net::tcp::{TcpAccept, TcpListener, TcpStream},
    net::udp::UdpSocket,
    platform::{mark_ready, process_id},
    process::{ActivityLog, GuestLog, Metering, Process},
    resource::{Accept, IncomingConnection, ResourceListener, ResourceSender},
    signal::Signal,
    storage::{BlobStore, DurableLog},
};
pub use selium_abi::{
    Capability, CapabilityGrant, DiscoveryRequest, DiscoveryResponse, EntrypointMetadata,
    InterfaceMetadata, LocalityScope, ResourceClass, ResourceIdentity, ResourceSelector,
    ResourceTarget, ScopeContext,
};
pub use selium_guest_macros::{entrypoint, pattern_interface};
pub use tracing::{debug, error, info, trace, warn};

mod async_runtime;
mod codec;
mod context;
mod error;
mod hostcall;
#[cfg(feature = "io")]
pub mod io;
mod memory;
mod net;
mod platform;
mod process;
mod resource;
mod signal;
mod storage;
