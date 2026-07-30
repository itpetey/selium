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

#[entrypoint]
async fn discovery_probe(discovery_handle: u64) {
    drop(selium_guest::log::init());
    selium_guest::info!(guest = "discovery-probe", "booting");

    // Build a discovery context — this exercises the discovery rendezvous
    // (HostQueueAttach + HostQueueSend) and proves the runtime-injected
    // discovery handle is valid.
    let _ctx = match Context::from_raw(discovery_handle).await {
        Ok(ctx) => ctx,
        Err(error) => {
            selium_guest::error!("failed to create discovery context: {error}");
            return;
        }
    };

    // Allocate a shared-memory channel — the runtime publishes Tier-1
    // registration events on the discovery feed for this region.
    match Channel::create(PROBE_CHANNEL_CAPACITY, ChannelBackpressure::Park) {
        Ok(channel) => {
            selium_guest::info!(region_id = channel.region_id(), "probe: region allocated");
        }
        Err(error) => {
            selium_guest::error!("probe: channel create failed: {error}");
            return;
        }
    }

    selium_guest::info!("guest ready");
    selium_guest::mark_ready();
}
