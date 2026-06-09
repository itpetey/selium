//! Selium guest SDK.

#[cfg(feature = "quinn")]
pub use crate::net::quinn::SeliumQuinnRuntime;
pub use crate::{
    async_runtime::{
        JoinHandle, poll_reactor, poll_safely, run_entrypoint_safely, spawn, yield_now,
    },
    codec::{decode_typed, encode_typed},
    context::Context,
    encoding::{FieldEncoder, FlatMsg, HasSchema, SchemaDescriptor},
    error::{GuestError, Result},
    memory::{SHARED_REGION_MAGIC, SharedRegion, attach_region, free_region},
    net::tcp::{TcpAccept, TcpListener, TcpStream},
    net::udp::UdpSocket,
    platform::{mark_ready, process_id},
    process::{ActivityLog, GuestLog, Metering, Process},
    resource::{Accept, IncomingConnection, ResourceListener, ResourceSender},
    storage::{BlobStore, DurableLog},
    time::{Instant, Timer, now},
};
pub use selium_abi::{
    Capability, CapabilityGrant, DiscoveryRequest, DiscoveryResponse, EntrypointMetadata,
    InterfaceMetadata, LocalityScope, RegionProt, ResourceClass, ResourceIdentity,
    ResourceSelector, ResourceTarget, ScopeContext,
};
pub use selium_guest_macros::{entrypoint, pattern_interface, schema};
pub use tracing::{debug, error, info, trace, warn};

mod async_runtime;
mod codec;
mod context;
pub mod encoding;
mod error;
#[allow(warnings)]
#[rustfmt::skip]
pub mod fbs;
mod hostcall;
#[cfg(feature = "io")]
pub mod io;
mod memory;
mod net;
mod platform;
mod process;
mod resource;
mod storage;
pub mod time;
