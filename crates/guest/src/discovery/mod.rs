//! Discovery guest - Service registry and discovery.
//!
//! Provides:
//! - Service registry
//! - Registration handling
//! - Resolve queries

use crate::error::GuestResult;
use crate::rpc::{Attribution, RpcCall, RpcServer};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct ServiceEndpoint {
    pub name: String,
    pub address: String,
    pub port: u16,
    pub metadata: HashMap<String, String>,
}

impl ServiceEndpoint {
    pub fn new(name: impl Into<String>, address: impl Into<String>, port: u16) -> Self {
        Self {
            name: name.into(),
            address: address.into(),
            port,
            metadata: HashMap::new(),
        }
    }
}

pub struct DiscoveryGuest {
    registry: HashMap<String, Vec<ServiceEndpoint>>,
    #[allow(dead_code)]
    server: RpcServer,
}

#[allow(dead_code)]
impl DiscoveryGuest {
    pub fn new() -> Self {
        let server = RpcServer::new();
        Self {
            registry: HashMap::new(),
            server,
        }
    }

    pub fn register(&mut self, endpoint: ServiceEndpoint) {
        let name = endpoint.name.clone();
        self.registry.entry(name).or_default().push(endpoint);
    }

    pub fn deregister(&mut self, name: &str, address: &str) -> bool {
        if let Some(endpoints) = self.registry.get_mut(name) {
            let len_before = endpoints.len();
            endpoints.retain(|e| e.address != address);
            return endpoints.len() < len_before;
        }
        false
    }

    pub fn resolve(&self, name: &str) -> Option<&[ServiceEndpoint]> {
        self.registry.get(name).map(Vec::as_slice)
    }

    #[allow(unused_variables)]
    fn handle_register(call: RpcCall, _attr: Attribution) -> GuestResult<Vec<u8>> {
        Ok(vec![])
    }

    #[allow(unused_variables)]
    fn handle_deregister(call: RpcCall, _attr: Attribution) -> GuestResult<Vec<u8>> {
        Ok(vec![])
    }

    #[allow(unused_variables)]
    fn handle_resolve(call: RpcCall, _attr: Attribution) -> GuestResult<Vec<u8>> {
        Ok(vec![])
    }
}

impl Default for DiscoveryGuest {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_discovery_guest_new() {
        let guest = DiscoveryGuest::new();
        assert!(guest.registry.is_empty());
    }

    #[test]
    fn test_register_service() {
        let mut guest = DiscoveryGuest::new();
        let endpoint = ServiceEndpoint::new("api", "localhost", 8080);
        guest.register(endpoint);
        assert!(guest.registry.contains_key("api"));
    }

    #[test]
    fn test_deregister_service() {
        let mut guest = DiscoveryGuest::new();
        let endpoint = ServiceEndpoint::new("api", "localhost", 8080);
        guest.register(endpoint);
        assert!(guest.deregister("api", "localhost"));
        assert!(
            guest.resolve("api").is_none()
                || guest.resolve("api").map(|v| v.is_empty()).unwrap_or(false)
        );
    }

    #[test]
    fn test_resolve_service() {
        let mut guest = DiscoveryGuest::new();
        let endpoint = ServiceEndpoint::new("api", "localhost", 8080);
        guest.register(endpoint);
        let resolved = guest.resolve("api");
        assert!(resolved.is_some());
        assert_eq!(resolved.unwrap().len(), 1);
    }
}
