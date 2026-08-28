//! Discovery system guest.

use std::{cell::RefCell, collections::BTreeMap, collections::HashMap, rc::Rc};

use selium_abi::{DiscoveryRequest, DiscoveryResponse, ResourceTarget, decode_rkyv, uri};
use selium_guest::{InterfaceMetadata, entrypoint, pattern_interface};
use selium_shm::{Channel, transport::ShmTransport};
use selium_wire::{framed::FramedRead, pubsub::Subscriber};

pub const DISCOVERY_EXCHANGE: &str = "selium.discovery.resolve";
pub const INTERFACE_METADATA_TABLE: &str = "selium.discovery.interfaces";
pub const REGISTRATION_LOG: &str = "selium.discovery.registrations";
pub const URI_LIVE_TABLE: &str = "selium.discovery.uri-table";

#[pattern_interface]
pub trait DiscoveryControl {
    fn register(target: ResourceTarget);
    fn remove(uri: String);
    fn resolve_exact(uri: String);
    fn resolve_prefix(prefix: String);
}

#[derive(Debug, Clone, Default)]
pub struct DiscoveryStore {
    registrations: BTreeMap<String, ResourceTarget>,
    /// Ownership table: maps `(process_id, resource_id)` pairs, populated by
    /// Tier-1 (runtime) registrations. Used to validate Tier-2 (guest) custom
    /// URI registrations.
    ownership: HashMap<(u64, u64), ()>,
    /// Protocol handlers by scheme (`sel-http`, `sel-dns`, …), populated by
    /// Tier-1 `RegisterHandler` events. A protocol-aware route registration
    /// is rejected when its scheme has no live handler.
    handlers: HashMap<String, ResourceTarget>,
}

impl DiscoveryStore {
    pub fn register(&mut self, target: ResourceTarget) -> Option<ResourceTarget> {
        self.registrations.insert(target.uri.clone(), target)
    }

    /// Tier-1 registration: store the mapping AND populate the ownership table.
    /// Called for runtime-published `sel://_sys/proc/<id>/` URIs.
    pub fn register_tier1(
        &mut self,
        process_id: u64,
        target: ResourceTarget,
    ) -> Option<ResourceTarget> {
        self.ownership.insert((process_id, target.resource_id), ());
        self.registrations.insert(target.uri.clone(), target)
    }

    /// Tier-2 registration: validate that `client_process_id` owns
    /// `target.resource_id` before storing the mapping.
    #[expect(
        clippy::result_unit_err,
        reason = "tier2 registration uses unit error for boolean failure"
    )]
    pub fn register_tier2(
        &mut self,
        client_process_id: u64,
        target: ResourceTarget,
    ) -> Result<Option<ResourceTarget>, ()> {
        if !self
            .ownership
            .contains_key(&(client_process_id, target.resource_id))
        {
            return Err(());
        }
        Ok(self.registrations.insert(target.uri.clone(), target))
    }

    /// Registers a protocol handler for `protocol` scheme. Tier-1 only.
    pub fn register_handler(
        &mut self,
        protocol: String,
        target: ResourceTarget,
    ) -> Option<ResourceTarget> {
        self.handlers.insert(protocol, target)
    }

    /// Removes a protocol handler registration. Tier-1 only.
    pub fn revoke_handler(&mut self, protocol: &str) -> Option<ResourceTarget> {
        self.handlers.remove(protocol)
    }

    /// Returns whether a handler is registered for `scheme`.
    pub fn has_handler(&self, scheme: &str) -> bool {
        self.handlers.contains_key(scheme)
    }

    /// Applies a guest (Tier-2) registration with the full validation chain:
    /// reserved namespace, protocol handler presence, then ownership.
    pub fn apply_register(&mut self, caller: u64, target: ResourceTarget) -> DiscoveryResponse {
        if uri::is_reserved(&target.uri) {
            return DiscoveryResponse::Forbidden;
        }
        if let Some(scheme) = uri::protocol_scheme(&target.uri)
            && !self.handlers.contains_key(scheme)
        {
            return DiscoveryResponse::NoHandler;
        }
        match self.register_tier2(caller, target) {
            Ok(_) => DiscoveryResponse::Registered,
            Err(()) => DiscoveryResponse::Forbidden,
        }
    }

    pub fn remove(&mut self, uri: &str) -> Option<ResourceTarget> {
        let removed = self.registrations.remove(uri);
        // Clean up ownership entries for resources no longer referenced by any URI.
        if let Some(ref target) = removed {
            let resource_id = target.resource_id;
            let still_referenced = self
                .registrations
                .values()
                .any(|t| t.resource_id == resource_id);
            if !still_referenced {
                self.ownership.retain(|(_, rid), _| *rid != resource_id);
            }
        }
        removed
    }

    /// Removes all registrations and ownership entries for a process.
    pub fn revoke_process(&mut self, process_id: u64) {
        let prefix = format!("{}{process_id}/", uri::PROC_URI_PREFIX);
        self.registrations
            .retain(|uri, _| !uri.starts_with(&prefix));
        self.ownership.retain(|(pid, _), _| *pid != process_id);
    }

    pub fn resolve_exact(&self, uri: &str) -> Option<ResourceTarget> {
        self.registrations.get(uri).cloned()
    }

    /// Resolves a URI with optional tenant scoping. For process-scoped URIs
    /// (`sel://_sys/proc/<id>/...`), only returns `Found` if the caller's tenant
    /// matches the target's tenant. If either tenant is None, the check is skipped
    /// (backward compatible with non-tenant-aware registrations).
    pub fn resolve_exact_scoped(
        &self,
        uri: &str,
        caller_tenant: Option<&str>,
    ) -> Option<ResourceTarget> {
        let target = self.resolve_exact(uri)?;

        // Only enforce tenant scoping for process-scoped URIs
        if uri.starts_with(uri::PROC_URI_PREFIX) {
            // If caller provides a tenant and target has a tenant, they must match
            if let (Some(caller), Some(target_tenant)) = (caller_tenant, &target.tenant)
                && caller != target_tenant
            {
                return None; // Tenant mismatch - deny access
            }
        }

        Some(target)
    }

    /// Returns whether a process owns a given resource.
    pub fn owns_resource(&self, process_id: u64, resource_id: u64) -> bool {
        self.ownership.contains_key(&(process_id, resource_id))
    }

    pub fn resolve_prefix(&self, prefix: &str) -> Vec<ResourceTarget> {
        self.registrations
            .iter()
            .filter(|(uri, _target)| uri::prefix_matches(prefix, uri))
            .map(|(_uri, target)| target.clone())
            .collect()
    }

    pub fn ingest_interface_metadata(&mut self, uri: &str, metadata: InterfaceMetadata) -> bool {
        let Some(target) = self.registrations.get_mut(uri) else {
            return false;
        };
        target.interface = Some(metadata);
        true
    }

    /// Applies a volatile Tier-1 event from the runtime feed.
    ///
    /// Tier-1 authority comes from the *transport* (the runtime feed), never
    /// from the URI string: everything arriving here is treated as
    /// runtime-authoritative — process registrations populate the ownership
    /// table, reserved `_sys` URIs are stored without an ownership entry, and
    /// protocol handlers are recorded for scheme validation.
    fn apply_tier1_event(&mut self, request: DiscoveryRequest) {
        match request {
            DiscoveryRequest::Register { uri: _, target } => {
                if let Some(process_id) = uri::extract_process_id(&target.uri) {
                    self.register_tier1(process_id, target);
                } else if uri::is_reserved(&target.uri) {
                    // Reserved system URIs (e.g. the DNS connector's channel)
                    // are runtime-authoritative: provisioned at spawn time,
                    // revoked at teardown. No ownership entry — guests cannot
                    // re-register or take over a system URI via Tier-2.
                    self.register(target);
                } else {
                    // Runtime should only publish reserved Tier-1 registrations.
                    selium_guest::warn!(
                        uri = target.uri,
                        "ignoring non-reserved Tier-1 registration"
                    );
                }
            }
            DiscoveryRequest::Revoke { uri } => {
                self.remove(&uri);
            }
            DiscoveryRequest::RegisterHandler { protocol, target } => {
                self.register_handler(protocol, target);
            }
            DiscoveryRequest::RevokeHandler { protocol } => {
                self.revoke_handler(&protocol);
            }
            _ => {}
        }
    }
}

pub fn interface_metadata() -> InterfaceMetadata {
    discoverycontrol_pattern_metadata()
}

fn attach_feed_subscriber(
    feed_region_id: u64,
) -> selium_guest::Result<Subscriber<Vec<u8>, ShmTransport>> {
    let channel = Channel::attach(feed_region_id)
        .map_err(|error| selium_guest::GuestError::Host(error.to_string()))?;
    let transport = ShmTransport::new(&channel, &channel)
        .map_err(|error| selium_guest::GuestError::Host(error.to_string()))?;
    let framed = FramedRead::new(transport);
    // Disable overwrite detection: the discovery feed is volatile and the guest
    // reads whatever is currently available, accepting that events may be lost.
    Ok(Subscriber::new(framed, None))
}

#[entrypoint]
async fn discovery_main(feed_region_id: u64, listener_shared_id: u64) {
    drop(selium_guest::log::init());
    selium_guest::info!(guest = "selium-discovery", "system guest booting");

    let feed_subscriber = match attach_feed_subscriber(feed_region_id) {
        Ok(s) => s,
        Err(error) => {
            selium_guest::error!("failed to attach discovery feed subscriber: {error}");
            return;
        }
    };

    let listener = match selium_guest::ResourceListener::attach(listener_shared_id) {
        Ok(l) => l,
        Err(error) => {
            selium_guest::error!("failed to attach discovery listener: {error}");
            return;
        }
    };

    selium_guest::info!(
        feed_region_id,
        shared_id = listener.descriptor().shared_id,
        "discovery feed and listener attached"
    );
    selium_guest::mark_ready();

    let store = Rc::new(RefCell::new(DiscoveryStore::default()));

    // Spawn the feed processing loop.
    selium_guest::spawn(feed_loop(store.clone(), feed_subscriber));

    // Accept incoming RPC connections forever.
    loop {
        let incoming = match listener.recv().await {
            Ok(connection) => connection,
            Err(error) => {
                selium_guest::warn!("discovery accept failed: {error}");
                continue;
            }
        };

        let connection =
            match selium_shm::rpc::accept::<DiscoveryRequest, DiscoveryResponse>(incoming.into()) {
                Ok(c) => c,
                Err(error) => {
                    selium_guest::warn!("discovery rpc accept failed: {error}");
                    continue;
                }
            };

        let store = store.clone();
        selium_guest::spawn(handler(store, connection));
    }
}

async fn feed_loop(
    store: Rc<RefCell<DiscoveryStore>>,
    mut subscriber: Subscriber<Vec<u8>, ShmTransport>,
) {
    loop {
        match subscriber.read_with_tag() {
            Ok((bytes, _tag)) => match decode_rkyv::<DiscoveryRequest>(&bytes) {
                Ok(request) => store.borrow_mut().apply_tier1_event(request),
                Err(error) => {
                    selium_guest::warn!("discovery feed decode failed: {error}");
                }
            },
            Err(selium_wire::error::Error::BufferEmpty) => {
                selium_guest::yield_now().await;
            }
            Err(error) => {
                selium_guest::warn!("discovery feed read failed: {error}");
                break;
            }
        }
    }
}

async fn handler(
    store: Rc<RefCell<DiscoveryStore>>,
    mut conn: selium_shm::rpc::RpcConnection<DiscoveryRequest, DiscoveryResponse>,
) {
    let client_process_id = conn.client_process_id();
    loop {
        match conn.recv().await {
            Ok(request) => {
                let response = {
                    let mut store = store.borrow_mut();
                    match request.payload() {
                        Ok(DiscoveryRequest::Resolve(uri)) => {
                            // TODO: Pass caller's tenant from RPC connection metadata
                            // when tenant tracking is added to the runtime.
                            match store.resolve_exact_scoped(&uri, None) {
                                Some(target) => {
                                    // Record the resolved queue id with the runtime so
                                    // the resolving client gains an authorisation basis
                                    // for cross-process `HostQueueAttach`. The runtime
                                    // accepts this only from the discovery guest.
                                    if let Err(error) = selium_guest::record_resolved_queue_for(
                                        client_process_id,
                                        target.resource_id,
                                    ) {
                                        selium_guest::warn!(
                                            "resolve authorisation record failed: {error}"
                                        );
                                    }
                                    DiscoveryResponse::Found(target)
                                }
                                None => DiscoveryResponse::NotFound,
                            }
                        }
                        Ok(DiscoveryRequest::Register { uri: _, target }) => {
                            // Guest registrations are always Tier-2: reserved
                            // namespace, handler presence, then ownership are
                            // validated before the mapping is stored. Tier-1
                            // registrations arrive only over the runtime feed.
                            store.apply_register(client_process_id, target)
                        }
                        Ok(DiscoveryRequest::Revoke { uri }) => {
                            // Guests may not revoke reserved system URIs.
                            if uri::is_reserved(&uri) {
                                DiscoveryResponse::Forbidden
                            } else {
                                store.remove(&uri);
                                DiscoveryResponse::Revoked
                            }
                        }
                        // Handler lifecycle is runtime-authoritative (Tier-1):
                        // guests cannot register themselves as protocol handlers.
                        Ok(DiscoveryRequest::RegisterHandler { .. })
                        | Ok(DiscoveryRequest::RevokeHandler { .. }) => {
                            DiscoveryResponse::Forbidden
                        }
                        Err(error) => {
                            selium_guest::warn!("discovery payload decode failed: {error}");
                            continue;
                        }
                    }
                };
                if let Err(error) = request.reply(response).await {
                    selium_guest::warn!("discovery reply failed: {error}");
                    break;
                }
            }
            Err(selium_shm::rpc::RpcError::ConnectionClosed) => break,
            Err(error) => {
                selium_guest::warn!("discovery recv failed: {error}");
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target(uri: &str, resource_id: u64) -> ResourceTarget {
        ResourceTarget {
            uri: uri.to_string(),
            host_id: "host-a".to_string(),
            resource_id,
            interface: None,
            tenant: None,
        }
    }

    #[test]
    fn registers_resolves_and_removes_uri() {
        let mut store = DiscoveryStore::default();

        assert_eq!(store.register(target("sel://tenant/app/api", 7)), None);
        assert_eq!(
            store.resolve_exact("sel://tenant/app/api"),
            Some(target("sel://tenant/app/api", 7))
        );
        assert_eq!(
            store.remove("sel://tenant/app/api"),
            Some(target("sel://tenant/app/api", 7))
        );
        assert_eq!(store.resolve_exact("sel://tenant/app/api"), None);
    }

    #[test]
    fn resolves_uri_prefixes() {
        let mut store = DiscoveryStore::default();
        store.register(target("sel://tenant/app/api", 7));
        store.register(target("sel://tenant/app/worker", 8));
        store.register(target("sel://tenant/other/api", 9));

        let results = store.resolve_prefix("sel://tenant/app/");

        assert_eq!(results.len(), 2);
    }

    #[test]
    fn stores_interface_metadata() {
        let mut store = DiscoveryStore::default();
        store.register(target("sel://tenant/app/api", 7));

        let updated = store.ingest_interface_metadata(
            "sel://tenant/app/api",
            InterfaceMetadata::new("Api", vec!["deploy".to_string()]),
        );

        assert!(updated);
        assert!(
            store
                .resolve_exact("sel://tenant/app/api")
                .is_some_and(|target| target.interface.is_some())
        );
    }

    #[test]
    fn discovery_store_routes_resolve_request_exact() {
        let mut store = DiscoveryStore::default();
        store.register(target("sel://tenant/app/api", 7));

        let request = DiscoveryRequest::Resolve("sel://tenant/app/api".to_string());
        let response = match request {
            DiscoveryRequest::Resolve(uri) => {
                if let Some(t) = store.resolve_exact(&uri) {
                    DiscoveryResponse::Found(t)
                } else {
                    DiscoveryResponse::NotFound
                }
            }
            DiscoveryRequest::Register { .. } => DiscoveryResponse::Registered,
            DiscoveryRequest::RegisterHandler { .. } => DiscoveryResponse::Forbidden,
            DiscoveryRequest::RevokeHandler { .. } => DiscoveryResponse::Forbidden,
            DiscoveryRequest::Revoke { .. } => DiscoveryResponse::Revoked,
        };

        assert!(matches!(response, DiscoveryResponse::Found(_)));
    }

    #[test]
    fn discovery_store_routes_resolve_request_not_found() {
        let store = DiscoveryStore::default();

        let request = DiscoveryRequest::Resolve("sel://tenant/app/api".to_string());
        let response = match request {
            DiscoveryRequest::Resolve(uri) => {
                if let Some(t) = store.resolve_exact(&uri) {
                    DiscoveryResponse::Found(t)
                } else {
                    DiscoveryResponse::NotFound
                }
            }
            DiscoveryRequest::Register { .. } => DiscoveryResponse::Registered,
            DiscoveryRequest::RegisterHandler { .. } => DiscoveryResponse::Forbidden,
            DiscoveryRequest::RevokeHandler { .. } => DiscoveryResponse::Forbidden,
            DiscoveryRequest::Revoke { .. } => DiscoveryResponse::Revoked,
        };

        assert!(matches!(response, DiscoveryResponse::NotFound));
    }

    #[test]
    fn discovery_store_routes_resolve_request_prefix() {
        let mut store = DiscoveryStore::default();
        store.register(target("sel://tenant/app/api", 7));
        store.register(target("sel://tenant/app/worker", 8));
        store.register(target("sel://tenant/other/api", 9));

        let prefix = "sel://tenant/app/";
        let results = store.resolve_prefix(prefix);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn tier1_register_populates_ownership() {
        let mut store = DiscoveryStore::default();
        let t = target("sel://_sys/proc/42/regions/7", 7);
        store.register_tier1(42, t);

        assert!(store.owns_resource(42, 7));
        assert!(!store.owns_resource(99, 7));
        assert!(
            store
                .resolve_exact("sel://_sys/proc/42/regions/7")
                .is_some()
        );
    }

    #[test]
    fn tier2_register_succeeds_for_owned_resource() {
        let mut store = DiscoveryStore::default();
        store.register_tier1(42, target("sel://_sys/proc/42/regions/7", 7));

        let custom = target("sel://my-app/logs", 7);
        let result = store.register_tier2(42, custom);
        assert!(result.is_ok());
        assert!(store.resolve_exact("sel://my-app/logs").is_some());
    }

    #[test]
    fn tier2_register_rejected_for_unowned_resource() {
        let mut store = DiscoveryStore::default();
        store.register_tier1(42, target("sel://_sys/proc/42/regions/7", 7));

        // Process 99 does not own resource 7
        let custom = target("sel://evil/logs", 7);
        let result = store.register_tier2(99, custom);
        result.unwrap_err();
        assert!(store.resolve_exact("sel://evil/logs").is_none());
    }

    #[test]
    fn revoke_removes_registration_and_ownership() {
        let mut store = DiscoveryStore::default();
        store.register_tier1(42, target("sel://_sys/proc/42/regions/7", 7));
        assert!(store.owns_resource(42, 7));

        store.remove("sel://_sys/proc/42/regions/7");
        assert!(
            store
                .resolve_exact("sel://_sys/proc/42/regions/7")
                .is_none()
        );
        assert!(!store.owns_resource(42, 7));
    }

    #[test]
    fn revoke_process_removes_all_process_entries() {
        let mut store = DiscoveryStore::default();
        store.register_tier1(42, target("sel://_sys/proc/42/regions/7", 7));
        store.register_tier1(42, target("sel://_sys/proc/42/logs", 8));
        store.register_tier1(99, target("sel://_sys/proc/99/regions/1", 1));

        store.revoke_process(42);

        assert!(
            store
                .resolve_exact("sel://_sys/proc/42/regions/7")
                .is_none()
        );
        assert!(store.resolve_exact("sel://_sys/proc/42/logs").is_none());
        assert!(!store.owns_resource(42, 7));
        assert!(!store.owns_resource(42, 8));
        // Process 99 unaffected.
        assert!(
            store
                .resolve_exact("sel://_sys/proc/99/regions/1")
                .is_some()
        );
        assert!(store.owns_resource(99, 1));
    }

    #[test]
    fn extract_process_id_from_uri_works() {
        assert_eq!(
            uri::extract_process_id("sel://_sys/proc/42/regions/7"),
            Some(42)
        );
        assert_eq!(uri::extract_process_id("sel://_sys/proc/99/logs"), Some(99));
        assert_eq!(uri::extract_process_id("sel://my-app/logs"), None);
        assert_eq!(uri::extract_process_id("not-a-uri"), None);
    }

    #[test]
    fn apply_tier1_register_event() {
        let mut store = DiscoveryStore::default();
        let t = target("sel://_sys/proc/42/regions/7", 7);
        store.apply_tier1_event(DiscoveryRequest::Register {
            uri: t.uri.clone(),
            target: t,
        });

        assert!(store.owns_resource(42, 7));
        assert!(
            store
                .resolve_exact("sel://_sys/proc/42/regions/7")
                .is_some()
        );
    }

    #[test]
    fn apply_tier1_revoke_event() {
        let mut store = DiscoveryStore::default();
        store.register_tier1(42, target("sel://_sys/proc/42/regions/7", 7));
        assert!(store.owns_resource(42, 7));

        store.apply_tier1_event(DiscoveryRequest::Revoke {
            uri: "sel://_sys/proc/42/regions/7".to_string(),
        });

        assert!(
            store
                .resolve_exact("sel://_sys/proc/42/regions/7")
                .is_none()
        );
        assert!(!store.owns_resource(42, 7));
    }

    #[test]
    fn well_known_sys_uri_registers_and_resolves() {
        // The DNS connector's well-known channel is runtime-authoritative:
        // a Tier-1 Register must store it so guests can resolve it.
        let mut store = DiscoveryStore::default();
        store.apply_tier1_event(DiscoveryRequest::Register {
            uri: "sel://_sys/dns/resolve".to_string(),
            target: target("sel://_sys/dns/resolve", 12),
        });

        let resolved = store.resolve_exact("sel://_sys/dns/resolve");
        assert_eq!(resolved.expect("sys uri resolves").resource_id, 12);

        // No ownership entry: a guest cannot take over a system URI via
        // Tier-2, and teardown revokes by URI.
        assert!(!store.owns_resource(0, 12));
        store.apply_tier1_event(DiscoveryRequest::Revoke {
            uri: "sel://_sys/dns/resolve".to_string(),
        });
        assert!(store.resolve_exact("sel://_sys/dns/resolve").is_none());
    }

    #[test]
    fn non_system_non_process_tier1_registration_is_ignored() {
        let mut store = DiscoveryStore::default();
        store.apply_tier1_event(DiscoveryRequest::Register {
            uri: "sel://my-app/logs".to_string(),
            target: target("sel://my-app/logs", 7),
        });
        assert!(store.resolve_exact("sel://my-app/logs").is_none());
    }

    #[test]
    fn tier2_register_rejected_for_reserved_namespace() {
        let mut store = DiscoveryStore::default();
        store.register_tier1(42, target("sel://_sys/proc/42/queues/7", 7));

        let response = store.apply_register(42, target("sel://_sys/handlers/sel-http", 7));
        assert!(matches!(response, DiscoveryResponse::Forbidden));
        assert!(
            store
                .resolve_exact("sel://_sys/handlers/sel-http")
                .is_none()
        );
    }

    #[test]
    fn protocol_route_requires_registered_handler() {
        let mut store = DiscoveryStore::default();
        store.register_tier1(42, target("sel://_sys/proc/42/queues/7", 7));

        let response = store.apply_register(42, target("sel-http://example.com/api", 7));
        assert!(matches!(response, DiscoveryResponse::NoHandler));
        assert!(store.resolve_exact("sel-http://example.com/api").is_none());

        store.apply_tier1_event(DiscoveryRequest::RegisterHandler {
            protocol: "sel-http".to_string(),
            target: target("sel://_sys/handlers/sel-http", 100),
        });
        assert!(store.has_handler("sel-http"));

        let response = store.apply_register(42, target("sel-http://example.com/api", 7));
        assert!(matches!(response, DiscoveryResponse::Registered));
        assert!(store.resolve_exact("sel-http://example.com/api").is_some());
    }

    #[test]
    fn generic_route_does_not_require_a_handler() {
        let mut store = DiscoveryStore::default();
        store.register_tier1(42, target("sel://_sys/proc/42/queues/7", 7));

        let response = store.apply_register(42, target("sel://my-app/logs", 7));
        assert!(matches!(response, DiscoveryResponse::Registered));
    }

    #[test]
    fn handler_lifecycle_over_tier1_feed() {
        let mut store = DiscoveryStore::default();
        store.apply_tier1_event(DiscoveryRequest::RegisterHandler {
            protocol: "sel-http".to_string(),
            target: target("sel://_sys/handlers/sel-http", 100),
        });
        assert!(store.has_handler("sel-http"));

        store.apply_tier1_event(DiscoveryRequest::RevokeHandler {
            protocol: "sel-http".to_string(),
        });
        assert!(!store.has_handler("sel-http"));
    }

    #[test]
    fn protocol_prefix_resolution_is_component_aware() {
        let mut store = DiscoveryStore::default();
        store.register(target("sel-http://example.com/foo", 7));
        store.register(target("sel-http://example.com/foobar", 8));

        let results = store.resolve_prefix("sel-http://example.com/foo");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].resource_id, 7);

        let results = store.resolve_prefix("sel-http://example.com/foo/");
        assert_eq!(results.len(), 1);
    }
}
