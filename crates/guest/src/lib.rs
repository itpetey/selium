//! Selium guest SDK.

use crate::hostcall_region_provider::HostcallRegionProvider;

pub use crate::{
    async_runtime::{
        JoinHandle, poll_reactor, poll_safely, run_entrypoint_safely, run_entrypoint_with_result,
        spawn, yield_now,
    },
    context::{Context, Serve},
    error::{GuestError, Result},
    hostcall::{
        process_capability, process_tenant, random_bytes, record_registration,
        record_resolved_queue_for, resolve_protocol_handler, self_info,
    },
    net::{Datagram, TcpListener, TcpStream, UdpSocket},
    platform::{mark_ready, process_id},
    process::{ActivityLog, Metering, Process},
    resource::{Accept, IncomingConnection, ResourceListener, ResourceSender},
    storage::{BlobStore, DurableLog},
    time::{Instant, Timer, now},
};
pub use selium_abi::{
    Capability, CapabilityGrant, EntrypointMetadata, LocalityScope, RegionProt, ResourceClass,
    ResourceIdentity, ResourceSelector, ScopeContext,
};
// Re-export service message types and encoding types.
pub use selium_guest_macros::{entrypoint, pattern_interface, schema};
// Re-export transport-agnostic memory primitives.
pub use selium_memory::{RING_HEADER_SIZE, RegionMapping, SHARED_REGION_MAGIC, WASM_PAGE_SIZE};
pub use selium_service::{
    DiscoveryRequest, DiscoveryResponse, FieldEncoder, FlatMsg, HasSchema, InterfaceMetadata,
    ResourceTarget, SchemaDescriptor,
    codec::{decode_typed, encode_typed},
    log::{LogField, LogLevel, LogRecord, LogSpan},
};
pub use tracing::{debug, error, info, trace, warn};

pub mod args;
mod async_runtime;
mod context;
mod error;
mod hostcall;
mod hostcall_region_provider;
pub mod log;
pub mod net;
mod platform;
mod process;
mod resource;
mod storage;
pub mod time;

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
