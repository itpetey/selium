//! Per-tenant bridge server system guest.
//!
//! The QUIC connector terminates QUIC and delivers each accepted stream to the
//! tenant's bridge server as a per-stream handoff whose [`IncomingConnection`]
//! metadata carries the authenticated client identity (`tenant` +
//! fingerprint). The bridge server:
//!
//! - creates its own listener and registers its serving route
//!   (`sel://<tenant>/bridge`) with discovery via `Context::serve`, deriving
//!   both the internal path and the wire names (`bridge.<tenant>`,
//!   `bridge.<owned-domain>`) from that one declaration;
//! - resolves the handoff's identity to a grant set via an interim identity
//!   source (a [`IdentityGrantMap`] here; the identity guest's RPC surface is
//!   deferred as a design open question);
//! - spawns one `bridge-channel <shared_id, grants>` per stream, conferring the
//!   client's grants plus an `ExplicitResource` for the handed-off region via
//!   the `DelegateGrants` capability;
//! - refuses unknown identities by attaching then closing the delivered region
//!   so the connector observes EOF and FINs the client stream;
//! - enforces a per-identity spawn bound to blunt stream-mint amplification.
//!
//! The bridge server terminates no QUIC and relays no stream bytes; it is a
//! control-plane guest only.

use std::collections::HashMap;

use selium_abi::{
    Capability, CapabilityGrant, ResourceClass, ResourceIdentity, ResourceSelector, ResourceTarget,
    client_identity::ClientIdentity,
};
use selium_guest::{
    Context, Process, ResourceListener, Serve, entrypoint, error, info, mark_ready,
    net::ByteStream, warn,
};

const BRIDGE_CHANNEL_ENTRYPOINT: &str = "bridge_channel";
/// The `bridge-channel` module id and entrypoint this server spawns.
const BRIDGE_CHANNEL_MODULE: &str = "bridge-channel-module";
/// Per-identity spawn bound (open question: exact concurrency/rate policy).
const DEFAULT_SPAWN_BOUND_PER_IDENTITY: usize = 256;

/// Interrim identity source: maps a client key fingerprint to its grants. The
/// deployed identity guest's RPC surface replaces this stub.
///
/// The stub recognises a single documented client (the bridge test client)
/// and grants it the tenant's default data-plane capability set.
#[derive(Default)]
pub struct IdentityGrantMap {
    by_fingerprint: HashMap<[u8; 32], Vec<CapabilityGrant>>,
}

/// Tracks per-identity spawn totals to bound stream-mint amplification.
#[derive(Default)]
pub struct SpawnBudget {
    per_identity: HashMap<[u8; 32], usize>,
}

impl IdentityGrantMap {
    /// Inserts the grant set for a client fingerprint.
    pub fn insert(&mut self, fingerprint: [u8; 32], grants: Vec<CapabilityGrant>) {
        self.by_fingerprint.insert(fingerprint, grants);
    }

    /// Returns the grants for a fingerprint, if the identity is known.
    pub fn grants_for(&self, fingerprint: &[u8; 32]) -> Option<&[CapabilityGrant]> {
        self.by_fingerprint.get(fingerprint).map(Vec::as_slice)
    }

    /// Builds the interim stub identity source.
    pub fn stub() -> Self {
        let mut map = Self::default();
        // SHA-256 of the SPKI of `guests/connector-quic/tests/fixtures/client_cert.pem`.
        let stub_fingerprint: [u8; 32] = [
            0x8b, 0x09, 0x39, 0x2b, 0x5d, 0xa0, 0x86, 0x8e, 0xd8, 0x35, 0xc8, 0x26, 0x93, 0x07,
            0x77, 0x28, 0xb4, 0x60, 0x74, 0x2c, 0x17, 0x42, 0x9d, 0x66, 0xb4, 0xf5, 0x31, 0x04,
            0xf2, 0x9c, 0x04, 0x67,
        ];
        map.insert(stub_fingerprint, tenant_acme_client_grants());
        map
    }
}

impl SpawnBudget {
    /// Attempts to acquire a spawn slot for `fingerprint`, bounded by `limit`.
    /// The budget is a lifetime per-identity cap: the bridge server has no
    /// child-exit signal, so true concurrency tracking is deferred to the
    /// supervisor (see design open questions). Slots acquired for spawns
    /// that fail are returned via [`Self::release`], so a failed spawn does
    /// not permanently consume a client's budget.
    pub fn try_acquire(&mut self, fingerprint: &[u8; 32], limit: usize) -> bool {
        let count = self.per_identity.entry(*fingerprint).or_insert(0);
        if *count >= limit {
            return false;
        }
        *count += 1;
        true
    }

    /// Returns a spawn slot previously acquired by
    /// [`Self::try_acquire`] (e.g. the spawn failed and never consumed it).
    pub fn release(&mut self, fingerprint: &[u8; 32]) {
        if let Some(count) = self.per_identity.get_mut(fingerprint) {
            *count = count.saturating_sub(1);
        }
    }
}

/// Encodes a `u64` entrypoint argument in the `WasmValue::I64` wire form.
fn arg_u64(value: u64) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(9);
    bytes.push(1);
    bytes.extend_from_slice(&value.to_le_bytes());
    bytes
}

/// Attaches the delivered stream region then closes it, so the connector
/// observes the close as EOF and FINs the client's stream.
fn attach_then_close(shared_id: u64) {
    match ByteStream::attach_blocking(shared_id) {
        Ok(stream) => drop(stream),
        Err(e) => warn!(shared_id, "bridge-server: attach-then-close failed: {e}"),
    }
}

/// Bridge server entrypoint.
///
/// The server receives its bootstrap discovery `Context` (built by the
/// entrypoint macro) and hands the underlying discovery handle to spawned
/// bridge-channels via [`Context::raw_handle`]. The server creates its own
/// listener and self-registers its serving route via [`Context::serve`]; the
/// runtime no longer provisions the route or injects a listener argument.
#[entrypoint]
async fn bridge_server(mut ctx: Context) {
    drop(selium_guest::log::init());
    info!("bridge-server: started");

    // The server's own tenant scope: handoffs carrying identities resolved
    // for any other tenant are refused (the connector derives the identity's
    // tenant from the verifying trust anchor, but a client verified by
    // another tenant's anchor must not reach this tenant's bridge).
    let own_tenant = match selium_guest::self_info() {
        Ok((_, Some(tenant))) => tenant,
        Ok((_, None)) => {
            error!("bridge-server: no tenant scope provisioned; refusing to serve");
            return;
        }
        Err(e) => {
            error!("bridge-server: self info failed: {e}");
            return;
        }
    };

    // The server creates its own listener: self-registration replaces the
    // runtime's well-known-URI queue minting.
    let mut listener = match ResourceListener::create() {
        Ok(listener) => listener,
        Err(e) => {
            error!("bridge-server: create listener failed: {e}");
            return;
        }
    };

    // Pin the QUIC connector: handoff metadata is sender-controlled, so an
    // unpinned listener would let any guest that resolves and attaches the
    // bridge route forge an authenticated identity and mint grants. Handoffs
    // from any process other than the registered `sel-quic` handler are
    // refused by the listener.
    let connector = match selium_guest::resolve_protocol_handler("sel-quic") {
        Ok(Some(connector)) => connector,
        Ok(None) => {
            error!(
                "bridge-server: no sel-quic protocol handler registered; refusing to serve unpinned handoffs"
            );
            return;
        }
        Err(e) => {
            error!("bridge-server: connector resolve failed: {e}");
            return;
        }
    };
    listener.expect_sender(connector);

    // Register the serving route (`sel://<tenant>/bridge`) from one declaration.
    let target = ResourceTarget {
        uri: String::new(), // pinned by `serve` to the derived internal path
        host_id: String::new(),
        resource_id: listener.descriptor().shared_id,
        interface: None,
        tenant: Some(own_tenant.clone()),
        class: ResourceClass::HostQueue,
        labels: Vec::new(),
    };
    if let Err(e) = ctx
        .serve(Serve {
            path: vec!["bridge".to_string()],
            target,
            default: false,
        })
        .await
    {
        error!("bridge-server: serve failed: {e}");
        return;
    }

    let identity_source = IdentityGrantMap::stub();
    let mut budget = SpawnBudget::default();
    mark_ready();

    loop {
        let incoming = match listener.recv().await {
            Ok(incoming) => incoming,
            Err(e) => {
                error!("bridge-server: handoff receive failed: {e}");
                continue;
            }
        };

        // Refuse a handoff with no usable client identity: attach-then-close
        // so the connector observes EOF and FINs the client stream.
        let Some(identity) = ClientIdentity::decode(&incoming.metadata) else {
            warn!("bridge-server: refusing handoff with unparseable identity");
            attach_then_close(incoming.shared_id);
            continue;
        };

        // Refuse identities resolved for a foreign tenant: the identity
        // source is scoped to this server's tenant only.
        if identity.tenant != own_tenant {
            warn!(
                own = %own_tenant,
                identity_tenant = %identity.tenant,
                "bridge-server: refusing cross-tenant identity"
            );
            attach_then_close(incoming.shared_id);
            continue;
        }

        let Some(grants) = identity_source
            .grants_for(&identity.fingerprint)
            .map(<[CapabilityGrant]>::to_vec)
        else {
            warn!(
                tenant = %identity.tenant,
                "bridge-server: refusing unknown client identity"
            );
            attach_then_close(incoming.shared_id);
            continue;
        };

        if !budget.try_acquire(&identity.fingerprint, DEFAULT_SPAWN_BOUND_PER_IDENTITY) {
            warn!(
                tenant = %identity.tenant,
                "bridge-server: refusing handoff over spawn bound"
            );
            attach_then_close(incoming.shared_id);
            continue;
        }

        // Confer the client's grants plus a tenant-scoped ExplicitResource
        // grant so the child may attach the handed-off stream region. The
        // grant carries the tenant selector because delegation only admits
        // child grants that are tenant-scoped within the DelegateGrants
        // fence (unscoped grants fall through to the subset check, which
        // the server cannot satisfy for a region it merely handed off).
        let mut child_grants = grants;
        child_grants.push(CapabilityGrant::new(
            Capability::SharedMemory,
            vec![
                ResourceSelector::Tenant(own_tenant.clone()),
                ResourceSelector::ExplicitResource(ResourceIdentity::Shared(incoming.shared_id)),
            ],
        ));

        match Process::start(
            BRIDGE_CHANNEL_MODULE,
            BRIDGE_CHANNEL_ENTRYPOINT,
            vec![
                arg_u64(ctx.raw_handle()),
                arg_u64(incoming.shared_id),
            ],
            child_grants,
        ) {
            Ok(_child) => info!(
                tenant = %identity.tenant,
                shared_id = incoming.shared_id,
                "bridge-server: spawned bridge-channel"
            ),
            Err(e) => {
                error!("bridge-server: bridge-channel spawn failed: {e}");
                // The slot was never consumed by a live child; return it so
                // failed spawns do not permanently eat the client's budget.
                budget.release(&identity.fingerprint);
                attach_then_close(incoming.shared_id);
            }
        }
    }
}

/// The stub client's data-plane grants: tenant-scoped shared memory, host
/// queues, and network streams. A real identity source provisions these from
/// policy rather than a fixed table.
fn tenant_acme_client_grants() -> Vec<CapabilityGrant> {
    vec![
        CapabilityGrant::new(
            Capability::SharedMemory,
            vec![
                ResourceSelector::Tenant("acme".to_string()),
                ResourceSelector::ResourceClass(ResourceClass::SharedRegion),
            ],
        ),
        CapabilityGrant::new(
            Capability::HostQueue,
            vec![
                ResourceSelector::Tenant("acme".to_string()),
                ResourceSelector::ResourceClass(ResourceClass::HostQueue),
            ],
        ),
        CapabilityGrant::new(
            Capability::Network,
            vec![
                ResourceSelector::Tenant("acme".to_string()),
                ResourceSelector::ResourceClass(ResourceClass::TcpStream),
            ],
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn grants() -> Vec<CapabilityGrant> {
        tenant_acme_client_grants()
    }

    #[test]
    fn identity_map_returns_known_grants() {
        let mut map = IdentityGrantMap::default();
        let fp = [7u8; 32];
        map.insert(fp, grants());
        let resolved = map.grants_for(&fp).expect("known identity");
        assert_eq!(resolved.len(), 3);
    }

    #[test]
    fn identity_map_misses_unknown_fingerprint() {
        let map = IdentityGrantMap::stub();
        assert!(map.grants_for(&[0u8; 32]).is_none());
    }

    #[test]
    fn spawn_budget_refuses_over_limit() {
        let mut budget = SpawnBudget::default();
        let fp = [9u8; 32];
        assert!(budget.try_acquire(&fp, 2));
        assert!(budget.try_acquire(&fp, 2));
        assert!(!budget.try_acquire(&fp, 2), "third spawn must be refused");
    }

    #[test]
    fn spawn_budget_release_frees_failed_spawns() {
        let mut budget = SpawnBudget::default();
        let fp = [11u8; 32];
        assert!(budget.try_acquire(&fp, 1));
        assert!(!budget.try_acquire(&fp, 1), "bound consumed");
        budget.release(&fp);
        assert!(
            budget.try_acquire(&fp, 1),
            "a failed spawn must not permanently consume the budget"
        );
        // Releasing below zero is a no-op, not an underflow.
        budget.release(&fp);
        budget.release(&fp);
    }

    /// 4.5 (test uplift): refusing an unknown identity attaches the
    /// delivered region and closes it, so the connector-side peer observes
    /// EOF rather than parking on a region nobody attaches.
    #[tokio::test]
    async fn attach_then_close_surfaces_eof_to_the_connector_peer() {
        drop(selium_memory::set_region_provider(Box::new(
            selium_memory::HeapRegionProvider::new(),
        )));
        let (ring_to_guest, ring_from_guest, shared_id, _region) =
            selium_shm::byte_channel::create(4096, 4096).expect("create byte channel");

        // Connector-side peer half of the relayed stream.
        let region = selium_memory::region_provider()
            .expect("provider")
            .attach(shared_id, None, selium_abi::RegionProt::ReadWrite)
            .expect("attach");
        let mut peer = selium_guest::net::ByteStream::from_ring_channels(
            &ring_from_guest,
            &ring_to_guest,
            region,
            true,
        )
        .expect("connector peer");

        // The refusal: attach then immediately close.
        attach_then_close(shared_id);

        // The peer observes EOF (a read of zero bytes) promptly, not a hang.
        let mut buf = [0u8; 8];
        let read = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            use tokio::io::AsyncReadExt;
            peer.read(&mut buf).await
        })
        .await
        .expect("peer must observe the close promptly")
        .expect("peer read must succeed");
        assert_eq!(read, 0, "peer observes EOF after attach-then-close");
    }
}
