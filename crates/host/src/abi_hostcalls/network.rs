use selium_abi::{Capability, GuestContext};
use std::collections::HashMap;
use wasmtime::Linker;

#[derive(Debug, Clone)]
pub struct NetworkConnection {
    pub addr: String,
}

pub struct NetworkCapability {
    connections: HashMap<u64, NetworkConnection>,
}

impl NetworkCapability {
    pub fn new() -> Self {
        Self {
            connections: HashMap::new(),
        }
    }

    pub fn connect(&mut self, addr: &str) -> Result<u64, selium_abi::GuestError> {
        static NEXT_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let id = NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.connections.insert(
            id,
            NetworkConnection {
                addr: addr.to_string(),
            },
        );
        Ok(id)
    }

    pub fn close(&mut self, id: u64) -> Result<(), selium_abi::GuestError> {
        self.connections
            .remove(&id)
            .ok_or(selium_abi::GuestError::NotFound)?;
        Ok(())
    }

    pub fn is_connected(&self, id: u64) -> bool {
        self.connections.contains_key(&id)
    }

    pub fn peer_addr(&self, id: u64) -> Result<String, selium_abi::GuestError> {
        self.connections
            .get(&id)
            .map(|c| c.addr.clone())
            .ok_or(selium_abi::GuestError::NotFound)
    }
}

impl Default for NetworkCapability {
    fn default() -> Self {
        Self::new()
    }
}

use std::sync::OnceLock;
static NETWORK: OnceLock<NetworkCapability> = OnceLock::new();

pub fn global_network() -> &'static NetworkCapability {
    NETWORK.get_or_init(NetworkCapability::new)
}

pub fn network_connect(_ctx: &GuestContext, addr: &str) -> Result<u64, selium_abi::GuestError> {
    if !_ctx.has_capability(Capability::NetworkConnect) {
        return Err(selium_abi::GuestError::PermissionDenied);
    }
    let network = global_network() as *const NetworkCapability as *mut NetworkCapability;
    unsafe { (*network).connect(addr) }
}

pub fn network_close(_ctx: &GuestContext, id: u64) -> Result<(), selium_abi::GuestError> {
    if !_ctx.has_capability(Capability::NetworkLifecycle) {
        return Err(selium_abi::GuestError::PermissionDenied);
    }
    let network = global_network() as *const NetworkCapability as *mut NetworkCapability;
    unsafe { (*network).close(id) }
}

pub fn network_peer_addr(_ctx: &GuestContext, id: u64) -> Result<String, selium_abi::GuestError> {
    if !_ctx.has_capability(Capability::NetworkStreamRead) {
        return Err(selium_abi::GuestError::PermissionDenied);
    }
    let network = global_network() as *const NetworkCapability as *mut NetworkCapability;
    unsafe { (*network).peer_addr(id) }
}

pub fn add_to_linker(_linker: &mut Linker<GuestContext>) -> anyhow::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_ctx(caps: impl IntoIterator<Item = Capability>) -> GuestContext {
        GuestContext::with_capabilities(1, caps)
    }

    #[test]
    fn test_network_connect() {
        let ctx = test_ctx([Capability::NetworkConnect]);
        let id = network_connect(&ctx, "127.0.0.1:8080").unwrap();
        assert!(id > 0);
    }

    #[test]
    fn test_network_close() {
        let ctx = test_ctx([
            Capability::NetworkConnect,
            Capability::NetworkLifecycle,
            Capability::NetworkStreamRead,
        ]);
        let id = network_connect(&ctx, "127.0.0.1:8080").unwrap();
        network_close(&ctx, id).unwrap();
        assert!(matches!(
            network_peer_addr(&ctx, id),
            Err(selium_abi::GuestError::NotFound)
        ));
    }

    #[test]
    fn test_network_peer_addr() {
        let ctx = test_ctx([Capability::NetworkConnect, Capability::NetworkStreamRead]);
        let id = network_connect(&ctx, "127.0.0.1:8080").unwrap();
        assert_eq!(network_peer_addr(&ctx, id).unwrap(), "127.0.0.1:8080");
    }
}
