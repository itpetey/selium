//! Discovery system guest.
//!
//! The store is a fed index over one deterministic URI taxonomy:
//! `sel://<tenant>/<type>/<id>` (typed), `sel://<tenant>/<name>` (leaf alias),
//! and opaque external names (`https://…`, bare hostnames). Everything is
//! tenant-scoped; the empty tenant is the reserved root namespace.

use std::{
    cell::RefCell, collections::BTreeMap, collections::BTreeSet, collections::HashMap, rc::Rc,
};

use selium_abi::{
    DiscoveryRequest, DiscoveryResponse, ProcessId, ResourceTarget, decode_rkyv, uri,
};
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
    /// Exact-key registrations: typed URIs, root well-known URIs, and opaque
    /// external names.
    registrations: BTreeMap<String, ResourceTarget>,
    /// Ownership table: maps `(process_id, resource_id)` pairs to the
    /// resource's class, populated by Tier-1 (runtime) registrations. Used
    /// to validate Tier-2 (guest) registrations and revocations without
    /// parsing the URI — the recorded class pins which *kind* of resource
    /// the owner may alias or name externally.
    ownership: HashMap<(u64, u64), selium_abi::ResourceClass>,
    /// Leaf aliases: alias URI → canonical typed URI.
    aliases: HashMap<String, String>,
    /// Reverse alias index: canonical typed URI → alias URIs, for reverse
    /// revocation when a target is revoked.
    alias_backrefs: HashMap<String, Vec<String>>,
    /// Label index: `(key, value)` → canonical typed URIs.
    label_index: HashMap<(String, String), BTreeSet<String>>,
}

/// Returns whether a caller's tenant admits a target's tenant. The check is
/// skipped when either side is absent (backward-compatible with root/system
/// registrations and untracked tenants).
fn tenant_admits(caller: Option<&str>, target: Option<&str>) -> bool {
    match (caller, target) {
        (Some(caller), Some(target)) => caller == target,
        _ => true,
    }
}

impl DiscoveryStore {
    /// Stores a Tier-1 registration under its exact key, populates the
    /// ownership table from `owner`, and maintains the label index for typed
    /// targets.
    fn store(&mut self, target: ResourceTarget, owner: Option<ProcessId>) {
        if let Some(process_id) = owner {
            self.ownership
                .insert((process_id, target.resource_id), target.class.clone());
        }
        if uri::parse_typed(&target.uri).is_some() {
            for (key, value) in &target.labels {
                self.label_index
                    .entry((key.clone(), value.clone()))
                    .or_default()
                    .insert(target.uri.clone());
            }
        }
        self.registrations.insert(target.uri.clone(), target);
    }

    /// Removes a registration by exact key, revoking any aliases that resolve
    /// to it and cleaning its label index entries.
    fn revoke_key(&mut self, key: &str) {
        let removed = self.registrations.remove(key);
        // Reverse alias revocation: revoking a target revokes its aliases.
        if let Some(aliases) = self.alias_backrefs.remove(key) {
            for alias in aliases {
                self.aliases.remove(&alias);
            }
        }
        // Drop label index entries referencing the removed target.
        let mut empty_labels = Vec::new();
        for ((label_key, label_value), uris) in self.label_index.iter_mut() {
            uris.remove(key);
            if uris.is_empty() {
                empty_labels.push((label_key.clone(), label_value.clone()));
            }
        }
        for label in empty_labels {
            self.label_index.remove(&label);
        }
        // Clean up ownership entries for resources no longer referenced.
        if let Some(target) = removed {
            let still_referenced = self
                .registrations
                .values()
                .any(|t| t.resource_id == target.resource_id);
            if !still_referenced {
                self.ownership
                    .retain(|(_, rid), _| *rid != target.resource_id);
            }
        }
    }

    /// Removes an alias registration (guest/Tier-2 alias, or a Tier-1
    /// revoke-by-alias-key). Returns the alias's canonical key, if any.
    fn revoke_alias(&mut self, alias: &str) {
        if let Some(canonical) = self.aliases.remove(alias)
            && let Some(backrefs) = self.alias_backrefs.get_mut(&canonical)
        {
            backrefs.retain(|a| a != alias);
            if backrefs.is_empty() {
                self.alias_backrefs.remove(&canonical);
            }
        }
    }

    /// Applies a volatile Tier-1 event from the runtime feed.
    ///
    /// Tier-1 authority comes from the *transport* (the runtime feed), never
    /// from the URI string: everything arriving here is stored verbatim.
    fn apply_tier1_event(&mut self, request: DiscoveryRequest) {
        match request {
            DiscoveryRequest::Register { target, owner, .. } => {
                self.store(target, owner);
            }
            DiscoveryRequest::Revoke { uri } => {
                self.revoke_key(&uri);
            }
            // Query variants never arrive over the feed.
            DiscoveryRequest::Resolve(_)
            | DiscoveryRequest::ResolvePrefix(_)
            | DiscoveryRequest::ResolveLabels { .. } => {}
        }
    }

    /// Resolves an exact URI (typed, root well-known, leaf alias, or external
    /// name) with optional tenant scoping.
    pub fn resolve_exact(&self, uri: &str, caller_tenant: Option<&str>) -> Option<ResourceTarget> {
        if let Some(target) = self.registrations.get(uri) {
            return tenant_admits(caller_tenant, target.tenant.as_deref()).then(|| target.clone());
        }
        if let Some(canonical) = self.aliases.get(uri)
            && let Some(target) = self.registrations.get(canonical)
        {
            return tenant_admits(caller_tenant, target.tenant.as_deref()).then(|| target.clone());
        }
        None
    }

    /// Resolves a prefix/`*` enumeration query (`sel://<tenant>/<type>/*` or
    /// `sel://<tenant>/*`) with tenant scoping.
    pub fn resolve_prefix(&self, prefix: &str, caller_tenant: Option<&str>) -> Vec<ResourceTarget> {
        let Some((tenant, path)) = uri::parse_sel(prefix) else {
            return Vec::new();
        };
        let Some(base) = uri::wildcard_prefix(path) else {
            return Vec::new();
        };
        let mut results = self
            .registrations
            .iter()
            .filter_map(|(key, target)| {
                let (target_tenant, target_path) = uri::parse_sel(key)?;
                if target_tenant != tenant {
                    return None;
                }
                if !tenant_admits(caller_tenant, target.tenant.as_deref()) {
                    return None;
                }
                if base.is_empty() {
                    // Enumerate every typed target in the tenant.
                    uri::parse_typed(key)?;
                } else {
                    let first_segment = target_path.split('/').next().unwrap_or("");
                    if first_segment != base {
                        return None;
                    }
                }
                Some(target.clone())
            })
            .collect::<Vec<_>>();
        // Deterministic order from the BTreeMap iteration.
        results.sort_by(|a, b| a.uri.cmp(&b.uri));
        results
    }

    /// Answers a label query: every typed target in the caller's tenant whose
    /// labels match `(key, value)`.
    pub fn resolve_labels(
        &self,
        key: &str,
        value: &str,
        caller_tenant: Option<&str>,
    ) -> Vec<ResourceTarget> {
        let mut results = Vec::new();
        if let Some(uris) = self.label_index.get(&(key.to_string(), value.to_string())) {
            for uri in uris {
                if let Some(target) = self.registrations.get(uri)
                    && tenant_admits(caller_tenant, target.tenant.as_deref())
                {
                    results.push(target.clone());
                }
            }
        }
        results
    }

    /// Applies a guest (Tier-2) registration with the full validation chain:
    /// root namespace rejection, leaf-alias/typed rules, then ownership
    /// (including the claimed resource class) and target existence for
    /// aliases.
    pub fn apply_register(
        &mut self,
        caller: ProcessId,
        caller_tenant: Option<&str>,
        target: ResourceTarget,
    ) -> DiscoveryResponse {
        // A guest may never register inside the root namespace.
        if uri::is_root_uri(&target.uri) {
            return DiscoveryResponse::Forbidden;
        }

        if let Some((tenant, _name)) = uri::parse_alias(&target.uri) {
            // Leaf alias: must live under the caller's own tenant and point
            // at an owned resource of the claimed class whose typed
            // registration currently exists.
            if !tenant_admits(caller_tenant, Some(tenant)) {
                return DiscoveryResponse::Forbidden;
            }
            if self.ownership.get(&(caller, target.resource_id)) != Some(&target.class) {
                return DiscoveryResponse::Forbidden;
            }
            let canonical = uri::resource_uri(tenant, target.class, target.resource_id);
            if !self.registrations.contains_key(&canonical) {
                // The claimed target is not registered (e.g. it was already
                // revoked); a dangling alias would resolve to nothing.
                return DiscoveryResponse::NotFound;
            }
            let alias = target.uri.clone();
            self.aliases.insert(alias.clone(), canonical.clone());
            self.alias_backrefs
                .entry(canonical)
                .or_default()
                .push(alias);
            DiscoveryResponse::Registered
        } else if uri::parse_sel(&target.uri).is_some() {
            // Typed URIs are minted by the runtime; guests may not register
            // them directly.
            DiscoveryResponse::Forbidden
        } else {
            // Opaque external name: validated by ownership (the caller must
            // own a resource of the claimed class), stored and matched
            // exactly.
            if self.ownership.get(&(caller, target.resource_id)) != Some(&target.class) {
                return DiscoveryResponse::Forbidden;
            }
            self.registrations.insert(target.uri.clone(), target);
            DiscoveryResponse::Registered
        }
    }

    /// Applies a guest (Tier-2) revocation. Guests may revoke only custom
    /// registrations (leaf aliases and opaque external names) they own,
    /// within their own tenant; typed URIs are runtime-minted and revoked
    /// over the Tier-1 feed; unknown keys report `NotFound`.
    pub fn apply_revoke(
        &mut self,
        caller: ProcessId,
        caller_tenant: Option<&str>,
        uri: &str,
    ) -> DiscoveryResponse {
        // Guests may not revoke the root namespace.
        if uri::is_root_uri(uri) {
            return DiscoveryResponse::Forbidden;
        }
        if self.aliases.contains_key(uri) {
            // Leaf alias: tenant admission on the alias's own tenant, plus
            // ownership of the aliased resource.
            let Some((tenant, _name)) = uri::parse_alias(uri) else {
                return DiscoveryResponse::NotFound;
            };
            if !tenant_admits(caller_tenant, Some(tenant)) {
                return DiscoveryResponse::Forbidden;
            }
            let canonical = self.aliases.get(uri).cloned();
            let Some((_, class, id)) = canonical.as_deref().and_then(uri::parse_typed) else {
                return DiscoveryResponse::NotFound;
            };
            if self.ownership.get(&(caller, id)) != Some(&class) {
                return DiscoveryResponse::Forbidden;
            }
            self.revoke_alias(uri);
            DiscoveryResponse::Revoked
        } else if uri::parse_sel(uri).is_some() {
            // Typed URIs are runtime-minted; only the Tier-1 feed revokes
            // them.
            DiscoveryResponse::Forbidden
        } else {
            // Opaque external name: the caller must own the target resource.
            let Some(target) = self.registrations.get(uri).cloned() else {
                return DiscoveryResponse::NotFound;
            };
            if self.ownership.get(&(caller, target.resource_id)) != Some(&target.class) {
                return DiscoveryResponse::Forbidden;
            }
            self.revoke_key(uri);
            DiscoveryResponse::Revoked
        }
    }

    pub fn ingest_interface_metadata(&mut self, uri: &str, metadata: InterfaceMetadata) -> bool {
        let Some(target) = self.registrations.get_mut(uri) else {
            return false;
        };
        target.interface = Some(metadata);
        true
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

/// Response used when the caller's tenant scope could not be verified
/// (fail-closed): reads disclose nothing, writes are refused. A tenant
/// *absence* (verified `None`, i.e. a root/system principal) is legitimate
/// and scoped normally — only a failed lookup takes this path.
fn denied_response(request: &DiscoveryRequest) -> DiscoveryResponse {
    match request {
        DiscoveryRequest::Resolve(_) => DiscoveryResponse::NotFound,
        DiscoveryRequest::ResolvePrefix(_) | DiscoveryRequest::ResolveLabels { .. } => {
            DiscoveryResponse::Resolved(Vec::new())
        }
        DiscoveryRequest::Register { .. } | DiscoveryRequest::Revoke { .. } => {
            DiscoveryResponse::Forbidden
        }
    }
}

async fn handler(
    store: Rc<RefCell<DiscoveryStore>>,
    mut conn: selium_shm::rpc::RpcConnection<DiscoveryRequest, DiscoveryResponse>,
) {
    let client_process_id = conn.client_process_id();
    // The caller's tenant is read from the runtime's persisted process
    // authority, scoping every operation to the calling process's own
    // tenant. Fail-closed: if the lookup itself fails, requests are denied
    // rather than silently treated as unscoped.
    let caller_scope = selium_guest::process_tenant(client_process_id);
    let scope_verified = caller_scope.is_ok();
    let caller_tenant = caller_scope.unwrap_or_default();
    if !scope_verified {
        selium_guest::warn!(
            "failed to resolve caller tenant for process {client_process_id}; denying requests"
        );
    }
    loop {
        match conn.recv().await {
            Ok(request) => {
                let response = {
                    let mut store = store.borrow_mut();
                    match request.payload() {
                        Ok(payload) if !scope_verified => denied_response(&payload),
                        Ok(payload) => match payload {
                            DiscoveryRequest::Resolve(uri) => {
                                match store.resolve_exact(&uri, caller_tenant.as_deref()) {
                                    Some(target) => {
                                        // Record the resolved queue id with the runtime so
                                        // the resolving client gains an authorisation basis
                                        // for cross-process `HostQueueAttach`.
                                        if let Err(error) =
                                            selium_guest::record_resolved_queue_for(
                                                client_process_id,
                                                target.resource_id,
                                            )
                                        {
                                            selium_guest::warn!(
                                                "resolve authorisation record failed: {error}"
                                            );
                                        }
                                        DiscoveryResponse::Found(target)
                                    }
                                    None => DiscoveryResponse::NotFound,
                                }
                            }
                            DiscoveryRequest::ResolvePrefix(prefix) => {
                                let targets =
                                    store.resolve_prefix(&prefix, caller_tenant.as_deref());
                                DiscoveryResponse::Resolved(targets)
                            }
                            DiscoveryRequest::ResolveLabels { key, value } => {
                                let targets =
                                    store.resolve_labels(&key, &value, caller_tenant.as_deref());
                                DiscoveryResponse::Resolved(targets)
                            }
                            DiscoveryRequest::Register { target, .. } => store.apply_register(
                                client_process_id,
                                caller_tenant.as_deref(),
                                target,
                            ),
                            DiscoveryRequest::Revoke { uri } => store.apply_revoke(
                                client_process_id,
                                caller_tenant.as_deref(),
                                &uri,
                            ),
                        },
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

    fn target(
        uri: &str,
        resource_id: u64,
        tenant: Option<&str>,
        class: selium_abi::ResourceClass,
    ) -> ResourceTarget {
        ResourceTarget {
            uri: uri.to_string(),
            host_id: "host-a".to_string(),
            resource_id,
            interface: None,
            tenant: tenant.map(str::to_string),
            class,
            labels: Vec::new(),
        }
    }

    fn region(tenant: &str, id: u64) -> ResourceTarget {
        target(
            &uri::resource_uri(tenant, selium_abi::ResourceClass::SharedRegion, id),
            id,
            (!tenant.is_empty()).then_some(tenant),
            selium_abi::ResourceClass::SharedRegion,
        )
    }

    fn queue(tenant: &str, id: u64) -> ResourceTarget {
        target(
            &uri::resource_uri(tenant, selium_abi::ResourceClass::HostQueue, id),
            id,
            (!tenant.is_empty()).then_some(tenant),
            selium_abi::ResourceClass::HostQueue,
        )
    }

    fn process(tenant: &str, id: u64, labels: Vec<(&str, &str)>) -> ResourceTarget {
        let mut t = target(
            &uri::resource_uri(tenant, selium_abi::ResourceClass::Process, id),
            id,
            (!tenant.is_empty()).then_some(tenant),
            selium_abi::ResourceClass::Process,
        );
        t.labels = labels
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        t
    }

    #[test]
    fn typed_uri_resolves_within_tenant() {
        let mut store = DiscoveryStore::default();
        store.store(region("acme", 7), Some(42));

        assert_eq!(
            store.resolve_exact("sel://acme/region/7", Some("acme")),
            Some(region("acme", 7))
        );
        assert_eq!(
            store.resolve_exact("sel://acme/region/7", Some("beta")),
            None
        );
        assert_eq!(
            store.resolve_exact("sel://acme/region/8", Some("acme")),
            None
        );
    }

    #[test]
    fn root_uri_resolves_for_system_callers() {
        let mut store = DiscoveryStore::default();
        let dnchecked = target(
            "sel:///dns/resolve",
            12,
            None,
            selium_abi::ResourceClass::HostQueue,
        );
        store.store(dnchecked, None);

        assert!(store.resolve_exact("sel:///dns/resolve", None).is_some());
        assert!(
            store
                .resolve_exact("sel:///dns/resolve", Some("acme"))
                .is_some()
        );
    }

    #[test]
    fn alias_resolves_to_the_typed_target() {
        let mut store = DiscoveryStore::default();
        store.store(process("acme", 123, vec![]), Some(123));
        store.apply_register(
            123,
            Some("acme"),
            target(
                "sel://acme/proxy",
                123,
                Some("acme"),
                selium_abi::ResourceClass::Process,
            ),
        );

        assert_eq!(
            store.resolve_exact("sel://acme/proxy", Some("acme")),
            Some(process("acme", 123, vec![]))
        );
    }

    #[test]
    fn revoking_a_target_revokes_its_aliases() {
        let mut store = DiscoveryStore::default();
        store.store(region("acme", 456), Some(42));
        store.apply_register(
            42,
            Some("acme"),
            target(
                "sel://acme/proxy",
                456,
                Some("acme"),
                selium_abi::ResourceClass::SharedRegion,
            ),
        );

        store.revoke_key("sel://acme/region/456");
        assert!(
            store
                .resolve_exact("sel://acme/region/456", Some("acme"))
                .is_none()
        );
        assert!(
            store
                .resolve_exact("sel://acme/proxy", Some("acme"))
                .is_none()
        );
    }

    #[test]
    fn guest_cannot_register_in_root_namespace() {
        let mut store = DiscoveryStore::default();
        store.store(region("acme", 7), Some(42));

        let response = store.apply_register(
            42,
            Some("acme"),
            target(
                "sel:///discovery",
                7,
                None,
                selium_abi::ResourceClass::SharedRegion,
            ),
        );
        assert!(matches!(response, DiscoveryResponse::Forbidden));
        assert!(
            store
                .resolve_exact("sel:///discovery", Some("acme"))
                .is_none()
        );
    }

    #[test]
    fn guest_cannot_mint_typed_uris() {
        let mut store = DiscoveryStore::default();
        store.store(region("acme", 7), Some(42));

        let response = store.apply_register(
            42,
            Some("acme"),
            target(
                "sel://acme/region/999",
                7,
                Some("acme"),
                selium_abi::ResourceClass::SharedRegion,
            ),
        );
        assert!(matches!(response, DiscoveryResponse::Forbidden));
    }

    #[test]
    fn guest_registration_requires_ownership() {
        let mut store = DiscoveryStore::default();
        store.store(region("acme", 7), Some(42));

        // Process 99 does not own resource 7.
        let response = store.apply_register(
            99,
            Some("acme"),
            target(
                "sel://acme/proxy",
                7,
                Some("acme"),
                selium_abi::ResourceClass::SharedRegion,
            ),
        );
        assert!(matches!(response, DiscoveryResponse::Forbidden));
        assert!(
            store
                .resolve_exact("sel://acme/proxy", Some("acme"))
                .is_none()
        );
    }

    #[test]
    fn guest_alias_must_live_under_own_tenant() {
        let mut store = DiscoveryStore::default();
        store.store(region("acme", 7), Some(42));

        let response = store.apply_register(
            42,
            Some("beta"),
            target(
                "sel://acme/proxy",
                7,
                Some("acme"),
                selium_abi::ResourceClass::SharedRegion,
            ),
        );
        assert!(matches!(response, DiscoveryResponse::Forbidden));
    }

    #[test]
    fn alias_cannot_shadow_a_class_noun() {
        let mut store = DiscoveryStore::default();
        store.store(region("acme", 7), Some(42));

        // `region` is a reserved class noun; parse_alias rejects it, so it is
        // treated as a typed URI attempt and refused.
        let response = store.apply_register(
            42,
            Some("acme"),
            target(
                "sel://acme/region",
                7,
                Some("acme"),
                selium_abi::ResourceClass::SharedRegion,
            ),
        );
        assert!(matches!(response, DiscoveryResponse::Forbidden));
    }

    #[test]
    fn label_query_returns_matching_targets_only() {
        let mut store = DiscoveryStore::default();
        store.store(process("acme", 123, vec![("app", "web")]), Some(123));
        store.store(process("acme", 124, vec![("app", "web")]), Some(124));
        store.store(process("acme", 125, vec![("app", "worker")]), Some(125));

        let matches = store.resolve_labels("app", "web", Some("acme"));
        assert_eq!(matches.len(), 2);
        assert!(
            matches
                .iter()
                .all(|t| t.labels.contains(&("app".to_string(), "web".to_string())))
        );
    }

    #[test]
    fn label_query_is_tenant_scoped() {
        let mut store = DiscoveryStore::default();
        store.store(process("acme", 123, vec![("app", "web")]), Some(123));
        store.store(process("beta", 124, vec![("app", "web")]), Some(124));

        assert_eq!(store.resolve_labels("app", "web", Some("acme")).len(), 1);
        assert_eq!(store.resolve_labels("app", "web", Some("beta")).len(), 1);
    }

    #[test]
    fn prefix_enumeration_lists_a_tenants_resources() {
        let mut store = DiscoveryStore::default();
        store.store(region("acme", 1), Some(42));
        store.store(region("acme", 2), Some(42));
        store.store(region("beta", 3), Some(43));

        let results = store.resolve_prefix("sel://acme/region/*", Some("acme"));
        assert_eq!(results.len(), 2);

        // Cross-tenant enumeration is denied.
        assert!(
            store
                .resolve_prefix("sel://acme/region/*", Some("beta"))
                .is_empty()
        );
    }

    #[test]
    fn prefix_enumeration_with_bare_star() {
        let mut store = DiscoveryStore::default();
        store.store(region("acme", 1), Some(42));
        store.store(process("acme", 123, vec![]), Some(123));

        let results = store.resolve_prefix("sel://acme/*", Some("acme"));
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn external_name_is_stored_and_matched_exactly() {
        let mut store = DiscoveryStore::default();
        store.store(queue("acme", 7), Some(42));

        let external = target(
            "https://acme.com/path",
            7,
            None,
            selium_abi::ResourceClass::HostQueue,
        );
        let response = store.apply_register(42, Some("acme"), external.clone());
        assert!(matches!(response, DiscoveryResponse::Registered));

        assert_eq!(
            store.resolve_exact("https://acme.com/path", Some("acme")),
            Some(external)
        );
        // A non-equivalently-spelled key does not match (opaque exact match).
        assert!(
            store
                .resolve_exact("https://acme.com/path/", Some("acme"))
                .is_none()
        );
        assert!(
            store
                .resolve_exact("http://acme.com/path", Some("acme"))
                .is_none()
        );
    }

    #[test]
    fn external_name_registration_requires_ownership() {
        let mut store = DiscoveryStore::default();
        store.store(queue("acme", 7), Some(42));

        // Process 99 does not own resource 7.
        let response = store.apply_register(
            99,
            Some("acme"),
            target(
                "https://acme.com/path",
                7,
                None,
                selium_abi::ResourceClass::HostQueue,
            ),
        );
        assert!(matches!(response, DiscoveryResponse::Forbidden));
    }

    #[test]
    fn external_name_class_must_match_the_owned_resource() {
        let mut store = DiscoveryStore::default();
        store.store(region("acme", 7), Some(42));

        // Process 42 owns resource 7 as a SharedRegion; claiming it is a
        // HostQueue must not pass, even for the owner.
        let response = store.apply_register(
            42,
            Some("acme"),
            target(
                "https://acme.com/path",
                7,
                None,
                selium_abi::ResourceClass::HostQueue,
            ),
        );
        assert!(matches!(response, DiscoveryResponse::Forbidden));
    }

    #[test]
    fn tier1_register_populates_ownership() {
        let mut store = DiscoveryStore::default();
        store.store(region("acme", 7), Some(42));
        assert_eq!(
            store.ownership.get(&(42, 7)),
            Some(&selium_abi::ResourceClass::SharedRegion)
        );
        assert!(!store.ownership.contains_key(&(99, 7)));
    }

    #[test]
    fn guest_revokes_their_own_custom_uri() {
        let mut store = DiscoveryStore::default();
        store.store(queue("acme", 7), Some(42));
        let external = target(
            "https://acme.com/path",
            7,
            None,
            selium_abi::ResourceClass::HostQueue,
        );
        store.apply_register(42, Some("acme"), external);

        let response = store.apply_revoke(42, Some("acme"), "https://acme.com/path");
        assert!(matches!(response, DiscoveryResponse::Revoked));
        assert!(
            store
                .resolve_exact("https://acme.com/path", Some("acme"))
                .is_none()
        );
    }

    #[test]
    fn guest_cannot_revoke_root_namespace() {
        let mut store = DiscoveryStore::default();
        let response = store.apply_revoke(42, Some("acme"), "sel:///dns/resolve");
        assert!(matches!(response, DiscoveryResponse::Forbidden));
    }

    #[test]
    fn guest_cannot_revoke_typed_uris() {
        let mut store = DiscoveryStore::default();
        store.store(region("acme", 7), Some(42));

        // Typed URIs are runtime-minted; only the Tier-1 feed revokes them.
        let response = store.apply_revoke(42, Some("acme"), "sel://acme/region/7");
        assert!(matches!(response, DiscoveryResponse::Forbidden));
        assert!(
            store
                .resolve_exact("sel://acme/region/7", Some("acme"))
                .is_some()
        );
    }

    #[test]
    fn guest_cannot_revoke_another_tenants_alias() {
        let mut store = DiscoveryStore::default();
        store.store(region("acme", 7), Some(42));
        let registered = store.apply_register(
            42,
            Some("acme"),
            target(
                "sel://acme/proxy",
                7,
                Some("acme"),
                selium_abi::ResourceClass::SharedRegion,
            ),
        );
        assert!(matches!(registered, DiscoveryResponse::Registered));

        // A caller whose verified tenant is not the alias's tenant is denied.
        let response = store.apply_revoke(42, Some("beta"), "sel://acme/proxy");
        assert!(matches!(response, DiscoveryResponse::Forbidden));
        assert!(
            store
                .resolve_exact("sel://acme/proxy", Some("acme"))
                .is_some()
        );
    }

    #[test]
    fn guest_cannot_revoke_an_alias_it_does_not_own() {
        let mut store = DiscoveryStore::default();
        store.store(region("acme", 7), Some(42));
        store.apply_register(
            42,
            Some("acme"),
            target(
                "sel://acme/proxy",
                7,
                Some("acme"),
                selium_abi::ResourceClass::SharedRegion,
            ),
        );

        // Process 99 does not own resource 7, so it may not revoke the alias.
        let response = store.apply_revoke(99, Some("acme"), "sel://acme/proxy");
        assert!(matches!(response, DiscoveryResponse::Forbidden));
        assert!(
            store
                .resolve_exact("sel://acme/proxy", Some("acme"))
                .is_some()
        );
    }

    #[test]
    fn guest_cannot_revoke_an_external_name_it_does_not_own() {
        let mut store = DiscoveryStore::default();
        store.store(queue("acme", 7), Some(42));
        store.apply_register(
            42,
            Some("acme"),
            target(
                "https://acme.com/path",
                7,
                None,
                selium_abi::ResourceClass::HostQueue,
            ),
        );

        let response = store.apply_revoke(99, Some("acme"), "https://acme.com/path");
        assert!(matches!(response, DiscoveryResponse::Forbidden));
        assert!(
            store
                .resolve_exact("https://acme.com/path", Some("acme"))
                .is_some()
        );
    }

    #[test]
    fn revoking_an_unknown_uri_returns_not_found() {
        let mut store = DiscoveryStore::default();
        store.store(region("acme", 7), Some(42));

        let response = store.apply_revoke(42, Some("acme"), "https://unknown.example/path");
        assert!(matches!(response, DiscoveryResponse::NotFound));
    }

    #[test]
    fn guest_alias_class_must_match_the_owned_resource() {
        let mut store = DiscoveryStore::default();
        store.store(region("acme", 7), Some(42));

        // Process 42 owns resource 7 as a SharedRegion; an alias claiming it
        // is a Process node must not pass, even for the owner.
        let response = store.apply_register(
            42,
            Some("acme"),
            target(
                "sel://acme/proxy",
                7,
                Some("acme"),
                selium_abi::ResourceClass::Process,
            ),
        );
        assert!(matches!(response, DiscoveryResponse::Forbidden));
    }

    #[test]
    fn guest_alias_must_point_at_a_registered_target() {
        let mut store = DiscoveryStore::default();
        // A delegated allocation: the runtime minted region 7 for tenant
        // "beta" on behalf of process 42, which owns it.
        store.store(region("beta", 7), Some(42));

        // An alias under the caller's own tenant claims the canonical
        // `sel://acme/region/7`, which does not exist — the registered
        // target lives under tenant "beta".
        let response = store.apply_register(
            42,
            Some("acme"),
            target(
                "sel://acme/proxy",
                7,
                Some("acme"),
                selium_abi::ResourceClass::SharedRegion,
            ),
        );
        assert!(matches!(response, DiscoveryResponse::NotFound));
    }

    #[test]
    fn denied_response_fails_closed_per_variant() {
        use selium_abi::ResourceTarget;

        let resolve = DiscoveryRequest::Resolve("sel://acme/region/7".to_string());
        assert!(matches!(
            denied_response(&resolve),
            DiscoveryResponse::NotFound
        ));

        let prefix = DiscoveryRequest::ResolvePrefix("sel://acme/region/*".to_string());
        assert!(matches!(
            denied_response(&prefix),
            DiscoveryResponse::Resolved(ref targets) if targets.is_empty()
        ));

        let labels = DiscoveryRequest::ResolveLabels {
            key: "app".to_string(),
            value: "web".to_string(),
        };
        assert!(matches!(
            denied_response(&labels),
            DiscoveryResponse::Resolved(ref targets) if targets.is_empty()
        ));

        let register = DiscoveryRequest::Register {
            uri: "sel://acme/proxy".to_string(),
            target: ResourceTarget {
                uri: "sel://acme/proxy".to_string(),
                host_id: String::new(),
                resource_id: 7,
                interface: None,
                tenant: Some("acme".to_string()),
                class: selium_abi::ResourceClass::SharedRegion,
                labels: Vec::new(),
            },
            owner: None,
        };
        assert!(matches!(
            denied_response(&register),
            DiscoveryResponse::Forbidden
        ));

        let revoke = DiscoveryRequest::Revoke {
            uri: "https://acme.com/path".to_string(),
        };
        assert!(matches!(
            denied_response(&revoke),
            DiscoveryResponse::Forbidden
        ));
    }

    #[test]
    fn end_to_end_discovery_lifecycle() {
        // Task 5.2 store-level golden path: spawn-node → allocate-region →
        // alias → label-query → teardown-revoke.
        let mut store = DiscoveryStore::default();

        // Spawn node: process 123 of tenant "acme" (registered by the runtime).
        store.store(process("acme", 123, vec![("app", "web")]), Some(123));
        assert!(
            store
                .resolve_exact("sel://acme/proc/123", Some("acme"))
                .is_some()
        );

        // Allocate region: runtime mints `sel://acme/region/7`.
        store.store(region("acme", 7), Some(123));
        assert!(
            store
                .resolve_exact("sel://acme/region/7", Some("acme"))
                .is_some()
        );

        // Alias: the owning process registers a leaf alias for the region.
        let alias = target(
            "sel://acme/cache",
            7,
            Some("acme"),
            selium_abi::ResourceClass::SharedRegion,
        );
        assert!(matches!(
            store.apply_register(123, Some("acme"), alias),
            DiscoveryResponse::Registered
        ));
        assert_eq!(
            store.resolve_exact("sel://acme/cache", Some("acme")),
            Some(region("acme", 7))
        );

        // Label query: the process node carries `app=web`.
        assert_eq!(store.resolve_labels("app", "web", Some("acme")).len(), 1);

        // Teardown: revoking the region also revokes its alias.
        store.revoke_key("sel://acme/region/7");
        assert!(
            store
                .resolve_exact("sel://acme/region/7", Some("acme"))
                .is_none()
        );
        assert!(
            store
                .resolve_exact("sel://acme/cache", Some("acme"))
                .is_none()
        );

        // Revoking the process node makes it unresolvable.
        store.revoke_key("sel://acme/proc/123");
        assert!(
            store
                .resolve_exact("sel://acme/proc/123", Some("acme"))
                .is_none()
        );
    }
}
