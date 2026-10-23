//! Discovery probe test fixture guest.
//!
//! Minimal application guest used by the discovery integration test.
//! Receives its bootstrap discovery `Context` (built by the entrypoint
//! macro, exercising the discovery rendezvous), allocates a shared-memory
//! region, logs its progress, and marks ready.
//!
//! The same module also carries the two roles the route-watch convergence
//! test needs, selected by the trailing integer entrypoint argument:
//!
//! - [`ROLE_PROVIDER`] registers a route in the discovery registry, announces
//!   the resource it minted, and stays resident.
//! - [`ROLE_CONSUMER`] watches that route through [`selium_guest::Context::watch`]
//!   and logs every event it observes. It starts *before* its provider, so the
//!   appearance it eventually sees proves wake-driven convergence rather than
//!   a host poll.

use anyhow::{Context as _, bail};
use selium_guest::{Context, ResourceClass, ResourceTarget, RouteEvent, entrypoint, info};
use selium_shm::{Channel, ChannelBackpressure};

/// Channel capacity for the probe region.
const PROBE_CHANNEL_CAPACITY: u64 = 4096;
/// The historical role: allocate a region, log, and return.
const ROLE_REGION: u64 = 0;
/// Register a route and stay resident as its provider.
const ROLE_PROVIDER: u64 = 1;
/// Watch a route and log every change the watch emits.
const ROLE_CONSUMER: u64 = 2;
/// How long the provider stays resident between checks. Long enough never to
/// elapse during a test; the entrypoint is a service, not a one-shot job.
const PROVIDER_PARK: std::time::Duration = std::time::Duration::from_secs(3_600);

#[entrypoint]
async fn discovery_probe(mut ctx: Context, route: (u64, u64), mode: u64) -> anyhow::Result<()> {
    drop(selium_guest::log::init());
    match mode {
        ROLE_REGION => {
            info!(guest = "discovery-probe", "booting");

            // Allocate a shared-memory channel. The host records its authority
            // synchronously; the owner (not the runtime) publishes any
            // registration for it.
            let channel = Channel::create(PROBE_CHANNEL_CAPACITY, ChannelBackpressure::Park)?;
            info!(region_id = channel.region_id(), "probe: region allocated");

            info!("guest ready");
            Ok(())
        }
        ROLE_PROVIDER => provider(&mut ctx, route_uri(route)).await,
        ROLE_CONSUMER => consumer(&mut ctx, route_uri(route)).await,
        other => bail!("discovery-probe: unknown mode {other}"),
    }
}

/// Decodes the route URI argument the runtime wrote into guest memory.
fn route_uri(route: (u64, u64)) -> &'static str {
    // SAFETY: `route` is this entrypoint's pointer argument; the runtime
    // populated it with the descriptor's argument bytes before the call.
    unsafe { selium_guest::args::str(route.0, route.1) }.unwrap_or_default()
}

/// Registers `uri` in the registry and stays resident as its provider.
async fn provider(ctx: &mut Context, uri: &str) -> anyhow::Result<()> {
    let channel = Channel::create(PROBE_CHANNEL_CAPACITY, ChannelBackpressure::Park)
        .context("provider: create channel")?;
    let resource_id = channel.region_id();
    let target = ResourceTarget {
        uri: uri.to_string(),
        host_id: "discovery-probe".to_string(),
        resource_id,
        interface: None,
        tenant: None,
        class: ResourceClass::SharedRegion,
        labels: Vec::new(),
    };

    // Registering immediately after minting the region must be admitted on the
    // first attempt: the registry asks the host whether this process owns the
    // target, rather than waiting for its own Tier-1 view to catch up. This
    // fixture deliberately does *not* retry, so a regression in that admission
    // path shows up here as a failed provider rather than as a slow one.
    ctx.register(uri, target)
        .await
        .map_err(|error| anyhow::anyhow!("provider: register {uri}: {error}"))?;
    info!("provider: registered {uri} {resource_id}");

    // A provider is a service: stay resident so the route is not revoked
    // until the process is actually stopped.
    loop {
        let now = selium_guest::Instant::now().context("provider: clock")?;
        let deadline = now
            .checked_add(PROVIDER_PARK)
            .unwrap_or(selium_guest::Instant::MAX);
        selium_guest::Timer::new(deadline).await;
    }
}

/// Watches `uri` and logs every event, forever.
async fn consumer(ctx: &mut Context, uri: &str) -> anyhow::Result<()> {
    let mut watch = ctx
        .watch(uri)
        .await
        .with_context(|| format!("consumer: watch {uri}"))?;
    info!("consumer: watching {uri}");

    loop {
        match watch.next().await.context("consumer: watch")? {
            RouteEvent::Appeared(target) => {
                info!("consumer: appeared {uri} {}", target.resource_id);
            }
            RouteEvent::Replaced(target) => {
                info!("consumer: replaced {uri} {}", target.resource_id);
            }
            RouteEvent::Revoked => {
                info!("consumer: revoked {uri}");
            }
        }
    }
}
