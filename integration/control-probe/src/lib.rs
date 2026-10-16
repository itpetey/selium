//! Resource-catalogue probe test fixture guest.
//!
//! Minimal application guest used by the control-plane integration test. It
//! receives its bootstrap discovery `Context`, resolves a catalogue name or —
//! on first boot — allocates a named region and registers it, logs the
//! outcome, and marks ready. The second boot of the same state root observes
//! the name replayed from the catalogue's durable log.
//!
//! It also allocates an anonymous shared region every boot, exercising the
//! spec's "catalogue unreachable" scenario: the host allocation primitive
//! succeeds independently of the catalogue.
//!
use anyhow::Result;
use selium_abi::ResourceKind;
use selium_guest::{Context, entrypoint, info};
use selium_shm::{Channel, ChannelBackpressure};

/// The name the probe registers in the resource catalogue.
pub const PROBE_REGION_NAME: &str = "selium.control-probe.region";

#[entrypoint]
async fn control_probe(mut ctx: Context) -> Result<()> {
    drop(selium_guest::log::init());
    info!(guest = "control-probe", "booting");

    let catalogue_result = ctx.resolve_region(PROBE_REGION_NAME).await;

    // Anonymous allocation is a host primitive: it must succeed regardless of
    // whether the catalogue is reachable.
    let channel = Channel::create(4096, ChannelBackpressure::Park)?;
    info!("anonymous allocation succeeded: {}", channel.region_id());

    match catalogue_result {
        Err(error) => {
            info!("catalogue unreachable: {error}");
        }
        Ok(Some(record)) => {
            info!(
                "catalogue replayed: {} -> {} (owner {})",
                record.uri, record.resource_id, record.owner
            );
        }
        Ok(None) => {
            match ctx
                .create_region(PROBE_REGION_NAME, 1, ResourceKind::RpcRing)
                .await
            {
                Ok(allocation) => info!(
                    "catalogue registered: {} -> {}",
                    PROBE_REGION_NAME, allocation.region_id
                ),
                Err(error) => info!("catalogue registration failed: {error}"),
            }
        }
    }

    // Health is published state: publish a health record and observe it.
    match ctx.publish_health("probe").await {
        Ok(()) => {
            let observed = ctx.observe_health("probe").await.unwrap_or(false);
            info!("health published and observed: {observed}");
        }
        Err(error) => info!("health publish failed: {error}"),
    }

    info!("guest ready");

    Ok(())
}
