//! Guest environment handle for read-only lookups.

use core::future::Future;

use crate::{DependencyId, FromHandle, driver::DriverError, singleton};
use selium_abi::GuestResourceId;

/// Descriptor that identifies a singleton dependency.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DependencyDescriptor {
    /// Human-readable dependency name.
    pub name: &'static str,
    /// Stable identifier for the dependency.
    pub id: DependencyId,
}

impl DependencyDescriptor {
    /// Create a new descriptor from a name and precomputed identifier.
    pub const fn new(name: &'static str, id: DependencyId) -> Self {
        Self { name, id }
    }
}

/// Dependency that can be resolved from the guest environment.
pub trait Dependency: Sized {
    /// Handle type required to build the dependency.
    type Handle: FromHandle<Handles = GuestResourceId>;
    /// Error type used by the implementor.
    type Error: std::error::Error;

    /// Static descriptor used to locate the dependency.
    const DESCRIPTOR: DependencyDescriptor;

    /// Build the dependency from the raw handle.
    fn from_handle(handle: Self::Handle) -> impl Future<Output = Result<Self, Self::Error>>;
}

/// Read-only view of the guest environment.
#[derive(Clone, Copy, Debug, Default)]
pub struct Context {
    _private: (),
}

impl Context {
    /// Return the current guest environment handle.
    pub fn current() -> Self {
        Self { _private: () }
    }

    /// Look up a singleton dependency by type.
    pub async fn singleton<T>(&self) -> Result<T, T::Error>
    where
        T: Dependency,
        T::Error: From<DriverError>,
    {
        let raw = singleton::lookup(T::DESCRIPTOR.id).await?;
        let handle = unsafe { T::Handle::from_handle(raw) };
        T::from_handle(handle).await
    }

    /// Look up a singleton dependency and trap on failure.
    pub async fn require<T>(&self) -> T
    where
        T: Dependency,
        T::Error: From<DriverError>,
    {
        match self.singleton::<T>().await {
            Ok(value) => value,
            Err(err) => panic!("dependency {} lookup failed: {err}", T::DESCRIPTOR.name),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Copy)]
    struct DummyHandle(GuestResourceId);

    impl crate::FromHandle for DummyHandle {
        type Handles = GuestResourceId;

        unsafe fn from_handle(handles: Self::Handles) -> Self {
            Self(handles)
        }
    }

    struct DummyDependency;

    impl Dependency for DummyDependency {
        type Handle = DummyHandle;
        type Error = std::io::Error;

        const DESCRIPTOR: DependencyDescriptor =
            DependencyDescriptor::new("dummy", selium_abi::DependencyId([1; 16]));

        fn from_handle(handle: Self::Handle) -> impl Future<Output = Result<Self, Self::Error>> {
            let _ = handle.0;
            async { Ok(Self) }
        }
    }

    #[test]
    fn descriptor_constructor_sets_fields() {
        let descriptor = DependencyDescriptor::new("clock", selium_abi::DependencyId([2; 16]));
        assert_eq!(descriptor.name, "clock");
        assert_eq!(descriptor.id, selium_abi::DependencyId([2; 16]));
    }

    #[test]
    fn singleton_lookup_surfaces_driver_errors() {
        let ctx = Context::current();
        match crate::block_on(ctx.singleton::<DummyDependency>()) {
            Ok(_) => panic!("expected driver error"),
            Err(err) => assert_eq!(err.raw_os_error(), Some(-2)),
        }
    }

    #[test]
    fn require_panics_when_dependency_lookup_fails() {
        let ctx = Context::current();
        let panic = std::panic::catch_unwind(|| {
            crate::block_on(ctx.require::<DummyDependency>());
        });
        assert!(panic.is_err());
    }
}
