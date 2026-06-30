//! Selium shared-memory ring channels and transport.
//!
//! This crate builds on [`selium_memory`] and [`selium_wire`] to provide
//! process-local and cross-process shared-memory channels, plus a
//! [`MessageTransport`] implementation over those channels.

use selium_memory::Region;
use selium_wire::error::Error;

pub use channels::{Channel, ChannelBackpressure};
pub use cursor::{Cursor, mask_for_capacity};
pub use region::{ChannelRegion, DATA_OFFSET, MIN_REGION_BYTES};
pub use ring_buf::{RingBuf, round_capacity};
pub use rpc::{RpcClient, RpcConnection, RpcError, RpcRequest, accept, connect};
pub use selium_memory::PAGE_SIZE;
pub use transport::{ShmRendezvous, ShmTransport};

pub mod channels;
pub mod cursor;
pub mod region;
pub mod ring_buf;
pub mod rpc;
pub mod transport;

/// Allocates a shared memory region via the global provider.
pub(crate) fn allocate_region(
    pages: u32,
    prot: selium_abi::RegionProt,
    purpose: selium_abi::ResourceKind,
) -> Result<Region, Error> {
    selium_memory::region_provider()?
        .allocate(pages, prot, purpose)
        .map_err(Error::from)
}

/// Attaches to an existing shared memory region via the global provider.
pub(crate) fn attach_region(
    region_id: u64,
    reader_slot: Option<u32>,
    prot: selium_abi::RegionProt,
) -> Result<Region, Error> {
    selium_memory::region_provider()?
        .attach(region_id, reader_slot, prot)
        .map_err(Error::from)
}

/// Ensures a heap provider is installed when running under `cfg(test)`.
#[cfg(test)]
pub(crate) fn ensure_heap_provider() {
    if selium_memory::region_provider().is_err() {
        install_heap_provider();
    }
}

/// Frees a shared memory region via the global provider.
#[cfg(test)]
pub(crate) fn free_region(region_id: u64) -> Result<(), Error> {
    selium_memory::region_provider()?
        .free(region_id)
        .map_err(Error::from)
}

/// Convenience helper to install the heap provider for tests.
#[cfg(test)]
pub(crate) fn install_heap_provider() {
    drop(selium_memory::set_region_provider(Box::new(
        selium_memory::HeapRegionProvider::new(),
    )));
}
