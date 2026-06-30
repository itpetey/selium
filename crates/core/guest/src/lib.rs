//! Selium guest SDK.

#[cfg(feature = "quinn")]
pub use crate::net::quinn::{QuinnUdpSocket, SeliumQuinnRuntime};
pub use crate::{
    async_runtime::{
        JoinHandle, poll_reactor, poll_safely, run_entrypoint_safely, spawn, yield_now,
    },
    context::Context,
    error::{GuestError, Result},
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

// Re-export transport-agnostic memory primitives.
pub use selium_memory::{RegionMapping, PAGE_SIZE, SHARED_REGION_MAGIC};

// Re-export encoding types.
pub use selium_encoding::{
    FieldEncoder, FlatMsg, HasSchema, SchemaDescriptor,
    codec::{decode_typed, encode_typed},
    log::{LogField, LogLevel, LogRecord, LogSpan},
};

mod async_runtime;
mod context;
mod error;
mod hostcall_region_provider;
mod hostcall;
pub mod log;
mod net;
mod platform;
mod process;
mod resource;
mod storage;
pub mod time;

use crate::hostcall_region_provider::HostcallRegionProvider;

/// Installs the hostcall-backed region provider and registers the mailbox
/// reactor so the guest can allocate and share memory regions.
///
/// This should be called once per guest process, typically from an
/// entrypoint before any I/O patterns are used. It is safe to call multiple
/// times; subsequent calls are no-ops.
pub fn init() -> Result<()> {
    if selium_memory::region_provider().is_err() {
        selium_memory::set_region_provider(Box::new(HostcallRegionProvider::new()))
            .map_err(|error| GuestError::Host(error.to_string()))?;
    }
    crate::platform::register_mailbox();
    Ok(())
}
