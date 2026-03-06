//! Session lifecycle SPI contracts.

use std::sync::Arc;

use selium_abi::Capability;

use crate::{guest_error::GuestError, registry::ResourceId, services::session_service::Session};

/// Capability responsible for session lifecycles.
pub trait SessionLifecycleCapability {
    /// Runtime-specific error type.
    type Error: Into<GuestError>;

    /// Create a new session with no entitlements.
    fn create(&self, parent: &Session, pubkey: [u8; 32]) -> Result<Session, Self::Error>;

    /// Add an entitlement to the given session.
    fn add_entitlement(
        &self,
        target: &mut Session,
        entitlement: Capability,
    ) -> Result<(), Self::Error>;

    /// Remove an entitlement from the given session.
    fn rm_entitlement(
        &self,
        target: &mut Session,
        entitlement: Capability,
    ) -> Result<(), Self::Error>;

    /// Add a resource to an entitlement.
    fn add_resource(
        &self,
        target: &mut Session,
        entitlement: Capability,
        resource: ResourceId,
    ) -> Result<bool, Self::Error>;

    /// Remove a resource from an entitlement.
    fn rm_resource(
        &self,
        target: &mut Session,
        entitlement: Capability,
        resource: ResourceId,
    ) -> Result<bool, Self::Error>;

    /// Delete a session.
    fn remove(&self, target: &Session) -> Result<(), Self::Error>;
}

impl<T> SessionLifecycleCapability for Arc<T>
where
    T: SessionLifecycleCapability,
{
    type Error = T::Error;

    fn create(&self, parent: &Session, pubkey: [u8; 32]) -> Result<Session, Self::Error> {
        self.as_ref().create(parent, pubkey)
    }

    fn add_entitlement(
        &self,
        target: &mut Session,
        entitlement: Capability,
    ) -> Result<(), Self::Error> {
        self.as_ref().add_entitlement(target, entitlement)
    }

    fn rm_entitlement(
        &self,
        target: &mut Session,
        entitlement: Capability,
    ) -> Result<(), Self::Error> {
        self.as_ref().rm_entitlement(target, entitlement)
    }

    fn add_resource(
        &self,
        target: &mut Session,
        entitlement: Capability,
        resource: ResourceId,
    ) -> Result<bool, Self::Error> {
        self.as_ref().add_resource(target, entitlement, resource)
    }

    fn rm_resource(
        &self,
        target: &mut Session,
        entitlement: Capability,
        resource: ResourceId,
    ) -> Result<bool, Self::Error> {
        self.as_ref().rm_resource(target, entitlement, resource)
    }

    fn remove(&self, target: &Session) -> Result<(), Self::Error> {
        self.as_ref().remove(target)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct Driver {
        calls: Mutex<Vec<&'static str>>,
    }

    impl SessionLifecycleCapability for Driver {
        type Error = GuestError;

        fn create(&self, parent: &Session, pubkey: [u8; 32]) -> Result<Session, Self::Error> {
            self.calls.lock().expect("calls lock").push("create");
            parent
                .create(std::collections::HashMap::new(), pubkey)
                .map_err(Into::into)
        }

        fn add_entitlement(
            &self,
            _target: &mut Session,
            _entitlement: Capability,
        ) -> Result<(), Self::Error> {
            self.calls
                .lock()
                .expect("calls lock")
                .push("add_entitlement");
            Ok(())
        }

        fn rm_entitlement(
            &self,
            _target: &mut Session,
            _entitlement: Capability,
        ) -> Result<(), Self::Error> {
            self.calls
                .lock()
                .expect("calls lock")
                .push("rm_entitlement");
            Ok(())
        }

        fn add_resource(
            &self,
            _target: &mut Session,
            _entitlement: Capability,
            _resource: ResourceId,
        ) -> Result<bool, Self::Error> {
            self.calls.lock().expect("calls lock").push("add_resource");
            Ok(true)
        }

        fn rm_resource(
            &self,
            _target: &mut Session,
            _entitlement: Capability,
            _resource: ResourceId,
        ) -> Result<bool, Self::Error> {
            self.calls.lock().expect("calls lock").push("rm_resource");
            Ok(true)
        }

        fn remove(&self, _target: &Session) -> Result<(), Self::Error> {
            self.calls.lock().expect("calls lock").push("remove");
            Ok(())
        }
    }

    #[test]
    fn arc_wrapper_forwards_all_methods() {
        let parent = Session::bootstrap(vec![Capability::SessionLifecycle], [0; 32]);
        let mut target = Session::bootstrap(Vec::new(), [1; 32]);
        let driver = Arc::new(Driver {
            calls: Mutex::new(Vec::new()),
        });

        let _child = driver.create(&parent, [2; 32]).expect("create");
        driver
            .add_entitlement(&mut target, Capability::TimeRead)
            .expect("add entitlement");
        driver
            .rm_entitlement(&mut target, Capability::TimeRead)
            .expect("rm entitlement");
        assert!(
            driver
                .add_resource(&mut target, Capability::TimeRead, 1)
                .expect("add resource")
        );
        assert!(
            driver
                .rm_resource(&mut target, Capability::TimeRead, 1)
                .expect("rm resource")
        );
        driver.remove(&target).expect("remove");

        let calls = driver.calls.lock().expect("calls lock");
        assert_eq!(
            calls.as_slice(),
            [
                "create",
                "add_entitlement",
                "rm_entitlement",
                "add_resource",
                "rm_resource",
                "remove"
            ]
        );
    }
}
