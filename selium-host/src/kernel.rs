//! Capability registry for Selium Host.
//!
//! The kernel maintains a registry of capabilities that can be granted to guests.
//! Each capability is keyed by a unique identifier (TypeId).

use parking_lot::RwLock;
use std::any::{Any, TypeId};
use std::sync::Arc;

/// A capability that can be registered with the kernel and granted to guests.
pub trait Capability: Any + Send + Sync + 'static {
    /// Returns the name of this capability for debugging/logging.
    fn name(&self) -> &'static str;
}

/// The capability registry - a type-indexed map of capabilities.
pub struct Kernel {
    capabilities: RwLock<std::collections::HashMap<TypeId, Arc<dyn Capability>>>,
}

impl Kernel {
    /// Create a new kernel with no capabilities.
    pub fn new() -> Self {
        Self {
            capabilities: RwLock::new(std::collections::HashMap::new()),
        }
    }

    /// Register a capability with the kernel.
    ///
    /// Only one capability of each type can be registered.
    /// Panics if a capability of this type is already registered.
    pub fn register<C: Capability>(&self, capability: C) {
        let type_id = TypeId::of::<C>();
        let mut caps = self.capabilities.write();
        if caps.contains_key(&type_id) {
            panic!(
                "Capability of type {:?} ({}) is already registered",
                type_id,
                capability.name()
            );
        }
        caps.insert(type_id, Arc::new(capability));
    }

    /// Check if a capability of the given type is registered.
    pub fn has_capability<C: Capability>(&self) -> bool {
        let caps = self.capabilities.read();
        caps.contains_key(&TypeId::of::<C>())
    }

    /// Get a capability by type.
    ///
    /// Returns `None` if the capability is not registered.
    pub fn get<C: Capability>(&self) -> Option<Arc<C>> {
        let caps = self.capabilities.read();
        caps.get(&TypeId::of::<C>()).map(|arc| {
            let any_arc: Arc<dyn Any + Send + Sync> = arc.clone();
            any_arc.downcast::<C>().expect("type should match")
        })
    }

    /// Get a capability by type, panicking if not found.
    ///
    /// # Panics
    ///
    /// Panics if the capability is not registered.
    pub fn expect<C: Capability>(&self, name: &'static str) -> Arc<C> {
        self.get().unwrap_or_else(|| {
            panic!(
                "Capability {} (type {:?}) is not registered",
                name,
                TypeId::of::<C>()
            )
        })
    }

    /// Get all registered capability type IDs.
    pub fn capability_types(&self) -> Vec<TypeId> {
        let caps = self.capabilities.read();
        caps.keys().cloned().collect()
    }
}

impl Default for Kernel {
    fn default() -> Self {
        Self::new()
    }
}
