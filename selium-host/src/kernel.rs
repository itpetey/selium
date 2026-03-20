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

#[cfg(test)]
mod tests {
    use super::*;

    struct TestCapability {
        name: &'static str,
    }

    impl Capability for TestCapability {
        fn name(&self) -> &'static str {
            self.name
        }
    }

    #[test]
    fn test_new_kernel_has_no_capabilities() {
        let kernel = Kernel::new();
        assert!(kernel.capability_types().is_empty());
    }

    #[test]
    fn test_register_and_get_capability() {
        let kernel = Kernel::new();
        let cap = TestCapability { name: "test" };

        kernel.register(cap);

        let retrieved = kernel.get::<TestCapability>();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().name(), "test");
    }

    #[test]
    fn test_has_capability() {
        let kernel = Kernel::new();
        assert!(!kernel.has_capability::<TestCapability>());

        kernel.register(TestCapability { name: "test" });

        assert!(kernel.has_capability::<TestCapability>());
    }

    #[test]
    fn test_get_nonexistent_capability() {
        let kernel = Kernel::new();
        assert!(kernel.get::<TestCapability>().is_none());
    }

    #[test]
    #[should_panic(expected = "is already registered")]
    fn test_register_duplicate_panics() {
        let kernel = Kernel::new();
        kernel.register(TestCapability { name: "test" });
        kernel.register(TestCapability { name: "test" }); // Should panic
    }

    #[test]
    fn test_expect_returns_capability() {
        let kernel = Kernel::new();
        kernel.register(TestCapability { name: "expected" });

        let cap = kernel.expect::<TestCapability>("expected");
        assert_eq!(cap.name(), "expected");
    }

    #[test]
    #[should_panic(expected = "is not registered")]
    fn test_expect_panics_on_missing() {
        let kernel = Kernel::new();
        kernel.expect::<TestCapability>("missing");
    }

    #[test]
    fn test_capability_types_list() {
        struct OtherCapability {
            name: &'static str,
        }
        impl Capability for OtherCapability {
            fn name(&self) -> &'static str {
                self.name
            }
        }

        let kernel = Kernel::new();
        kernel.register(TestCapability { name: "test" });
        kernel.register(OtherCapability { name: "other" });

        let types = kernel.capability_types();
        assert_eq!(types.len(), 2);
    }

    #[test]
    fn test_default_kernel_is_empty() {
        let kernel = Kernel::default();
        assert!(kernel.capability_types().is_empty());
    }
}
