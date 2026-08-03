//! Discovery probe test fixture guest.
//!
//! Minimal application guest used by the discovery integration test.
//! Takes the discovery handle as its sole entrypoint argument, builds
//! `Context::from_raw` (exercising the discovery rendezvous), allocates a
//! shared-memory region, logs its progress, and marks ready.
//!
//! Cross-guest shared-memory RPC wake is not yet implemented, so the probe
//! does not perform Tier-2 register/lookup through discovery. Those paths
//! are exercised by the existing `shm_transport` RPC tests.

use selium_guest::{Context, entrypoint};
use selium_shm::{Channel, ChannelBackpressure};

/// Channel capacity for the probe region.
const PROBE_CHANNEL_CAPACITY: u64 = 4096;

/// Error wrapper for the probe entrypoint.
#[derive(Debug)]
struct ProbeError(String);

impl std::fmt::Display for ProbeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for ProbeError {}

impl From<selium_guest::GuestError> for ProbeError {
    fn from(e: selium_guest::GuestError) -> Self {
        Self(e.to_string())
    }
}

#[entrypoint]
async fn discovery_probe(discovery_handle: u64) -> Result<(), ProbeError> {
    drop(selium_guest::log::init());
    selium_guest::info!(guest = "discovery-probe", "booting");

    // Build a discovery context — this exercises the discovery rendezvous
    // (HostQueueAttach + HostQueueSend) and proves the runtime-injected
    // discovery handle is valid.
    let _ctx = Context::from_raw(discovery_handle).await?;

    // Allocate a shared-memory channel — the runtime publishes Tier-1
    // registration events on the discovery feed for this region.
    let channel = Channel::create(PROBE_CHANNEL_CAPACITY, ChannelBackpressure::Park)
        .map_err(|e| ProbeError(e.to_string()))?;
    selium_guest::info!(region_id = channel.region_id(), "probe: region allocated");

    selium_guest::info!("guest ready");
    selium_guest::mark_ready();

    Ok(())
}
