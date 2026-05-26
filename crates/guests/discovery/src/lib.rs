//! Discovery system guest.

use std::{cell::RefCell, collections::BTreeMap, rc::Rc};

use selium_guest::io::rpc::{RpcAccept, RpcConnection};
use selium_guest::{
    DiscoveryRequest, DiscoveryResponse, InterfaceMetadata, ResourceTarget, entrypoint,
    pattern_interface,
};

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
}

impl DiscoveryStore {
    pub fn register(&mut self, target: ResourceTarget) -> Option<ResourceTarget> {
        self.registrations.insert(target.uri.clone(), target)
    }

    pub fn remove(&mut self, uri: &str) -> Option<ResourceTarget> {
        self.registrations.remove(uri)
    }

    pub fn resolve_exact(&self, uri: &str) -> Option<ResourceTarget> {
        self.registrations.get(uri).cloned()
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

async fn handler(
    store: Rc<RefCell<DiscoveryStore>>,
    mut conn: RpcConnection<DiscoveryRequest, DiscoveryResponse>,
) {
    loop {
        match conn.recv().await {
            Ok(request) => {
                let response = {
                    let store = store.borrow();
                    match request.payload() {
                        DiscoveryRequest::Resolve(uri) => {
                            if let Some(target) = store.resolve_exact(uri) {
                                DiscoveryResponse::Found(target)
                            } else {
                                DiscoveryResponse::NotFound
                            }
                        }
                    }
                };
                if let Err(error) = request.reply(response).await {
                    selium_guest::warn!("discovery reply failed: {error}");
                    break;
                }
            }
            Err(selium_guest::io::rpc::error::RpcError::ConnectionClosed) => break,
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
}
