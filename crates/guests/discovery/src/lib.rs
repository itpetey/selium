//! Discovery system guest.

use std::{cell::RefCell, collections::BTreeMap, collections::HashMap, rc::Rc};

use selium_guest::{
    DiscoveryRequest, DiscoveryResponse, InterfaceMetadata, ResourceTarget, entrypoint,
    io::rpc::{RpcAccept, RpcConnection, RpcError},
    pattern_interface,
};

pub const DISCOVERY_EXCHANGE: &str = "selium.discovery.resolve";
pub const INTERFACE_METADATA_TABLE: &str = "selium.discovery.interfaces";
/// Prefix for process-scoped URIs registered by the runtime (Tier 1).
const PROCESS_URI_PREFIX: &str = "sel://process/";
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
}

impl DiscoveryStore {
    pub fn register(&mut self, target: ResourceTarget) -> Option<ResourceTarget> {
        self.registrations.insert(target.uri.clone(), target)
    }

    /// Tier-1 registration: store the mapping AND populate the ownership table.
    /// Called when the URI starts with `sel://process/<id>/`.
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
        let prefix = format!("sel://process/{process_id}/");
        self.registrations
            .retain(|uri, _| !uri.starts_with(&prefix));
        self.ownership.retain(|(pid, _), _| *pid != process_id);
    }

    pub fn resolve_exact(&self, uri: &str) -> Option<ResourceTarget> {
        self.registrations.get(uri).cloned()
    }

    /// Resolves a URI with optional tenant scoping. For process-scoped URIs
    /// (`sel://process/<id>/...`), only returns `Found` if the caller's tenant
    /// matches the target's tenant. If either tenant is None, the check is skipped
    /// (backward compatible with non-tenant-aware registrations).
    pub fn resolve_exact_scoped(
        &self,
        uri: &str,
        caller_tenant: Option<&str>,
    ) -> Option<ResourceTarget> {
        let target = self.resolve_exact(uri)?;

        // Only enforce tenant scoping for process-scoped URIs
        if uri.starts_with(PROCESS_URI_PREFIX) {
            // If caller provides a tenant and target has a tenant, they must match
            if let (Some(caller), Some(target_tenant)) = (caller_tenant, &target.tenant) {
                if caller != target_tenant {
                    return None; // Tenant mismatch - deny access
                }
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
            .range(prefix.to_string()..)
            .take_while(|(uri, _target)| uri.starts_with(prefix))
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
}

pub fn interface_metadata() -> InterfaceMetadata {
    discoverycontrol_pattern_metadata()
}

#[entrypoint]
async fn discovery_main(listener_shared_id: u64) {
    let _ = selium_guest::log::init();
    selium_guest::info!(guest = "selium-discovery", "system guest booting");

    let listener = match selium_guest::ResourceListener::attach(listener_shared_id) {
        Ok(l) => l,
        Err(error) => {
            selium_guest::error!("failed to attach discovery listener: {error}");
            return;
        }
    };

    selium_guest::info!(
        shared_id = listener.descriptor().shared_id,
        "discovery listener attached"
    );
    selium_guest::mark_ready();

    let store = Rc::new(RefCell::new(DiscoveryStore::default()));

    loop {
        let connection = match listener
            .accept::<RpcAccept<DiscoveryRequest, DiscoveryResponse>>()
            .await
        {
            Ok(c) => c,
            Err(error) => {
                selium_guest::warn!("discovery accept failed: {error}");
                continue;
            }
        };

        let store = store.clone();
        selium_guest::spawn(handler(store, connection));
    }
}

/// Extracts the process id from a `sel://process/<id>/...` URI, if present.
fn extract_process_id_from_uri(uri: &str) -> Option<u64> {
    let rest = uri.strip_prefix(PROCESS_URI_PREFIX)?;
    let id_str = rest.split('/').next()?;
    id_str.parse().ok()
}

async fn handler(
    store: Rc<RefCell<DiscoveryStore>>,
    mut conn: RpcConnection<DiscoveryRequest, DiscoveryResponse>,
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
                            if let Some(target) = store.resolve_exact_scoped(&uri, None) {
                                DiscoveryResponse::Found(target)
                            } else {
                                DiscoveryResponse::NotFound
                            }
                        }
                        Ok(DiscoveryRequest::Register { uri: _, target }) => {
                            // Tier-1: URI starts with sel://process/<id>/ — authoritative
                            // registration from the runtime. Populate ownership table.
                            // Tier-2: custom URI from a guest — validate ownership.
                            if let Some(process_id) = extract_process_id_from_uri(&target.uri) {
                                // Tier-1: runtime-authoritative
                                store.register_tier1(process_id, target.clone());
                                DiscoveryResponse::Registered
                            } else {
                                // Tier-2: guest-requested, validated
                                match store.register_tier2(client_process_id, target.clone()) {
                                    Ok(_) => DiscoveryResponse::Registered,
                                    Err(()) => DiscoveryResponse::Forbidden,
                                }
                            }
                        }
                        Ok(DiscoveryRequest::Revoke { uri }) => {
                            store.remove(&uri);
                            DiscoveryResponse::Revoked
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
            Err(RpcError::ConnectionClosed) => break,
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
        let t = target("sel://process/42/regions/7", 7);
        store.register_tier1(42, t);

        assert!(store.owns_resource(42, 7));
        assert!(!store.owns_resource(99, 7));
        assert!(store.resolve_exact("sel://process/42/regions/7").is_some());
    }

    #[test]
    fn tier2_register_succeeds_for_owned_resource() {
        let mut store = DiscoveryStore::default();
        store.register_tier1(42, target("sel://process/42/regions/7", 7));

        let custom = target("sel://my-app/logs", 7);
        let result = store.register_tier2(42, custom);
        assert!(result.is_ok());
        assert!(store.resolve_exact("sel://my-app/logs").is_some());
    }

    #[test]
    fn tier2_register_rejected_for_unowned_resource() {
        let mut store = DiscoveryStore::default();
        store.register_tier1(42, target("sel://process/42/regions/7", 7));

        // Process 99 does not own resource 7
        let custom = target("sel://evil/logs", 7);
        let result = store.register_tier2(99, custom);
        assert!(result.is_err());
        assert!(store.resolve_exact("sel://evil/logs").is_none());
    }

    #[test]
    fn revoke_removes_registration_and_ownership() {
        let mut store = DiscoveryStore::default();
        store.register_tier1(42, target("sel://process/42/regions/7", 7));
        assert!(store.owns_resource(42, 7));

        store.remove("sel://process/42/regions/7");
        assert!(store.resolve_exact("sel://process/42/regions/7").is_none());
        assert!(!store.owns_resource(42, 7));
    }

    #[test]
    fn revoke_process_removes_all_process_entries() {
        let mut store = DiscoveryStore::default();
        store.register_tier1(42, target("sel://process/42/regions/7", 7));
        store.register_tier1(42, target("sel://process/42/logs", 8));
        store.register_tier1(99, target("sel://process/99/regions/1", 1));

        store.revoke_process(42);

        assert!(store.resolve_exact("sel://process/42/regions/7").is_none());
        assert!(store.resolve_exact("sel://process/42/logs").is_none());
        assert!(!store.owns_resource(42, 7));
        assert!(!store.owns_resource(42, 8));
        // Process 99 unaffected.
        assert!(store.resolve_exact("sel://process/99/regions/1").is_some());
        assert!(store.owns_resource(99, 1));
    }

    #[test]
    fn extract_process_id_from_uri_works() {
        assert_eq!(
            extract_process_id_from_uri("sel://process/42/regions/7"),
            Some(42)
        );
        assert_eq!(
            extract_process_id_from_uri("sel://process/99/logs"),
            Some(99)
        );
        assert_eq!(extract_process_id_from_uri("sel://my-app/logs"), None);
        assert_eq!(extract_process_id_from_uri("not-a-uri"), None);
    }
}
