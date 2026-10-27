//! Discovery probe test fixture guest.
//!
//! Minimal application guest used by the discovery and failure-injection
//! integration tests. Receives its bootstrap discovery `Context` (built by the
//! entrypoint macro, exercising the discovery rendezvous), allocates a
//! shared-memory region, logs its progress, and marks ready.
//!
//! Roles are selected by the trailing integer entrypoint argument:
//!
//! - [`ROLE_PROVIDER`] registers a route in the discovery registry through the
//!   SDK (which also renews its lease) and stays resident.
//! - [`ROLE_CONSUMER`] watches a route through [`selium_guest::Context::watch`]
//!   and logs every event it observes.
//! - [`ROLE_RESOLVER`] resolves a URI once and logs `found` or `notfound`.
//! - [`ROLE_REGISTER_NO_RENEW`] registers through the raw discovery client and
//!   deliberately never renews, so the lease sweep must expire it.
//! - [`ROLE_TRAP_AFTER_REGISTER`] registers and then crashes, so the runtime's
//!   failure path (not the graceful stop path) has to revoke the route.
//! - [`ROLE_FORGE`] tries to register a reserved root route without holding
//!   its claim; discovery must refuse it.
//! - [`ROLE_CLAIM_ROUTE`] registers a reserved handler route under a
//!   host-provisioned claim and stays resident, so a second claimant can be
//!   refused with a conflict.

use anyhow::{Context as _, bail};
use selium_guest::{
    Context, ResourceClass, ResourceTarget, RouteEvent, entrypoint, info, process_id,
};
use selium_service::{DiscoveryRequest, DiscoveryResponse};
use selium_shm::{Channel, ChannelBackpressure};

/// Channel capacity for the probe region.
const PROBE_CHANNEL_CAPACITY: u64 = 4096;
/// How long a resident role stays parked between checks. Long enough never to
/// elapse during a test; these entrypoints are services, not one-shot jobs.
const PROVIDER_PARK: std::time::Duration = std::time::Duration::from_secs(3_600);
/// Allocate a region, log, and return.
const ROLE_REGION: u64 = 0;
/// Register a route (SDK path, with lease renewal) and stay resident.
const ROLE_PROVIDER: u64 = 1;
/// Watch a route and log every change.
const ROLE_CONSUMER: u64 = 2;
/// Resolve a URI once and log the answer.
const ROLE_RESOLVER: u64 = 3;
/// Register through the raw client and never renew the lease.
const ROLE_REGISTER_NO_RENEW: u64 = 4;
/// Register, then crash (a trap, not a graceful exit).
const ROLE_TRAP_AFTER_REGISTER: u64 = 5;
/// Try to register a reserved root route without its claim.
const ROLE_FORGE: u64 = 6;
/// Register a reserved handler route under a host-provisioned claim.
const ROLE_CLAIM_ROUTE: u64 = 7;
/// Resolve a URI until it is gone, logging each answer.
const ROLE_RESOLVER_UNTIL_GONE: u64 = 8;

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
        ROLE_RESOLVER => resolver(&mut ctx, route_uri(route)).await,
        ROLE_REGISTER_NO_RENEW => register_no_renew(&mut ctx, route_uri(route)).await,
        ROLE_TRAP_AFTER_REGISTER => trap_after_register(&mut ctx, route_uri(route)).await,
        ROLE_FORGE => forge(&mut ctx, route_uri(route)).await,
        ROLE_CLAIM_ROUTE => claim_route(&mut ctx, route_uri(route)).await,
        ROLE_RESOLVER_UNTIL_GONE => resolver_until_gone(&mut ctx, route_uri(route)).await,
        other => bail!("discovery-probe: unknown mode {other}"),
    }
}

/// Registers `uri` in the registry (with the SDK's lease renewal) and stays
/// resident as its provider.
async fn provider(ctx: &mut Context, uri: &str) -> anyhow::Result<()> {
    let channel = Channel::create(PROBE_CHANNEL_CAPACITY, ChannelBackpressure::Park)
        .context("provider: create channel")?;
    let resource_id = channel.region_id();
    let target = region_target(uri, resource_id);

    // Registering immediately after minting the region must be admitted on the
    // first attempt: the registry asks the host whether this process owns the
    // target, rather than waiting for its own Tier-1 view to catch up. This
    // fixture deliberately does *not* retry, so a regression in that admission
    // path shows up here as a failed provider rather than as a slow one.
    ctx.register(uri, target)
        .await
        .map_err(|error| anyhow::anyhow!("provider: register {uri}: {error}"))?;
    info!("provider: registered {uri} {resource_id}");
    park().await
}

/// Resolves `uri` once and logs the observable outcome.
async fn resolver(ctx: &mut Context, uri: &str) -> anyhow::Result<()> {
    let response = ctx
        .discovery()
        .request(DiscoveryRequest::Resolve(uri.to_string()))
        .await
        .context("resolver: resolve")?;
    match response {
        DiscoveryResponse::Found(target) => {
            info!("resolver: found {uri} {}", target.resource_id);
        }
        DiscoveryResponse::NotFound => info!("resolver: notfound {uri}"),
        other => info!("resolver: other {uri} {other:?}"),
    }
    Ok(())
}

/// Resolves `uri` periodically until it reports `NotFound`.
///
/// A single resolve races the discovery sweep; a lapsed-lease test must keep
/// asking until the sweep has ticked past the deadline.
async fn resolver_until_gone(ctx: &mut Context, uri: &str) -> anyhow::Result<()> {
    loop {
        let response = ctx
            .discovery()
            .request(DiscoveryRequest::Resolve(uri.to_string()))
            .await
            .context("resolver: resolve")?;
        match response {
            DiscoveryResponse::NotFound => {
                info!("resolver: notfound {uri}");
                return Ok(());
            }
            DiscoveryResponse::Found(target) => {
                info!("resolver: found {uri} {}", target.resource_id);
            }
            other => info!("resolver: other {uri} {other:?}"),
        }
        let now = selium_guest::Instant::now().context("resolver: clock")?;
        let deadline = now
            .checked_add(std::time::Duration::from_millis(250))
            .unwrap_or(selium_guest::Instant::MAX);
        selium_guest::Timer::new(deadline).await;
    }
}

/// Registers `uri` through the raw client and never renews its lease.
///
/// The lease is set at registration time; with no `Renew` request, the
/// discovery sweep must expire the registration once its TTL has lapsed.
async fn register_no_renew(ctx: &mut Context, uri: &str) -> anyhow::Result<()> {
    let channel = Channel::create(PROBE_CHANNEL_CAPACITY, ChannelBackpressure::Park)
        .context("lease: create channel")?;
    let resource_id = channel.region_id();
    match request_register(ctx, uri, region_target(uri, resource_id), false).await? {
        DiscoveryResponse::Registered => {
            info!("lease: registered {uri} {resource_id} without renewal");
        }
        other => bail!("lease: registration was not admitted: {other:?}"),
    }
    park().await
}

/// Registers `uri` and then crashes, so revocation must run on the failure
/// path rather than through a graceful stop.
#[expect(
    clippy::panic,
    reason = "the deliberate crash is the point of this fixture role"
)]
async fn trap_after_register(ctx: &mut Context, uri: &str) -> anyhow::Result<()> {
    let channel = Channel::create(PROBE_CHANNEL_CAPACITY, ChannelBackpressure::Park)
        .context("crash: create channel")?;
    let resource_id = channel.region_id();
    match request_register(ctx, uri, region_target(uri, resource_id), false).await? {
        DiscoveryResponse::Registered => info!("crash: registered {uri} {resource_id}"),
        other => bail!("crash: registration was not admitted: {other:?}"),
    }
    // Park once so the guest is admitted by bootstrap (which runs the
    // entrypoint to its first park), then trap on the next wake. A trap during
    // the initial poll would be reported as a bootstrap failure, exercising the
    // wrong path: this fixture exists to drive the runtime's *crash* cleanup.
    let now = selium_guest::Instant::now().context("crash: clock")?;
    let deadline = now
        .checked_add(std::time::Duration::from_secs(5))
        .unwrap_or(selium_guest::Instant::MAX);
    selium_guest::Timer::new(deadline).await;
    panic!("failure-injection: deliberate guest crash after registering {uri}");
}

/// Tries to register a reserved root route without holding its claim.
async fn forge(ctx: &mut Context, uri: &str) -> anyhow::Result<()> {
    let channel = Channel::create(PROBE_CHANNEL_CAPACITY, ChannelBackpressure::Park)
        .context("forge: create channel")?;
    let target = region_target(uri, channel.region_id());
    match request_register(ctx, uri, target, true).await? {
        DiscoveryResponse::Registered => info!("forge: registered {uri} (unexpected)"),
        other => info!("forge: refused {uri} {other:?}"),
    }
    Ok(())
}

/// Registers a reserved handler route as a root service under the caller's
/// host-provisioned handler claim, then stays resident.
async fn claim_route(ctx: &mut Context, uri: &str) -> anyhow::Result<()> {
    let target = ResourceTarget {
        uri: uri.to_string(),
        host_id: "discovery-probe".to_string(),
        resource_id: process_id(),
        interface: None,
        tenant: None,
        class: ResourceClass::Process,
        labels: Vec::new(),
    };
    match request_register(ctx, uri, target, true).await? {
        DiscoveryResponse::Registered => info!("claim: registered {uri}"),
        other => info!("claim: refused {uri} {other:?}"),
    }
    park().await
}

/// Builds a region-backed target for `uri`.
fn region_target(uri: &str, resource_id: u64) -> ResourceTarget {
    ResourceTarget {
        uri: uri.to_string(),
        host_id: "discovery-probe".to_string(),
        resource_id,
        interface: None,
        tenant: None,
        class: ResourceClass::SharedRegion,
        labels: Vec::new(),
    }
}

/// Sends a raw registration through the discovery client, bypassing the SDK's
/// lease-renewal bookkeeping.
async fn request_register(
    ctx: &mut Context,
    uri: &str,
    target: ResourceTarget,
    root_service: bool,
) -> anyhow::Result<DiscoveryResponse> {
    ctx.discovery()
        .request(DiscoveryRequest::Register {
            uri: uri.to_string(),
            target,
            owner: None,
            root_service,
        })
        .await
        .context("register: discovery request")
}

/// Stays resident without doing work; the entrypoint is a service.
async fn park() -> anyhow::Result<()> {
    loop {
        let now = selium_guest::Instant::now().context("park: clock")?;
        let deadline = now
            .checked_add(PROVIDER_PARK)
            .unwrap_or(selium_guest::Instant::MAX);
        selium_guest::Timer::new(deadline).await;
    }
}

/// Decodes the route URI argument the runtime wrote into guest memory.
fn route_uri(route: (u64, u64)) -> &'static str {
    // SAFETY: `route` is this entrypoint's pointer argument; the runtime
    // populated it with the descriptor's argument bytes before the call.
    unsafe { selium_guest::args::str(route.0, route.1) }.unwrap_or_default()
}
