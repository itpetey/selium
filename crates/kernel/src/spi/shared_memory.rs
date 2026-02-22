//! Shared memory SPI contracts.

use std::{future::Future, sync::Arc};

use selium_abi::{
    GuestResourceId, GuestUint, ShmAlloc, ShmNotify, ShmReady, ShmRegion, ShmWaitCondition,
};

use crate::guest_error::GuestError;

/// Runtime-specific mapping bridge used by shared-memory capabilities.
pub trait SharedMemoryBindingContext {
    /// Attach the shared heap identified by `shared_id` and return the guest-visible base offset.
    fn attach_mapping(&mut self, shared_id: GuestResourceId) -> Result<GuestUint, GuestError>;

    /// Detach a previously attached shared heap mapping.
    fn detach_mapping(&mut self, shared_id: GuestResourceId) -> Result<(), GuestError>;
}

/// Capability responsible for shared-memory region allocation and ring synchronisation.
pub trait SharedMemoryCapability {
    /// Runtime-specific error type.
    type Error: Into<GuestError>;

    /// Allocate a region in the shared memory arena.
    fn alloc(&self, request: ShmAlloc) -> Result<ShmRegion, Self::Error>;

    /// Attach a guest mapping for the shared-memory region and return the mapping base offset.
    fn attach_mapping(
        &self,
        binding: &mut dyn SharedMemoryBindingContext,
        shared_id: GuestResourceId,
        region: ShmRegion,
    ) -> Result<GuestUint, Self::Error>;

    /// Detach a guest mapping for the shared-memory region.
    fn detach_mapping(
        &self,
        binding: &mut dyn SharedMemoryBindingContext,
        shared_id: GuestResourceId,
        region: ShmRegion,
    ) -> Result<(), Self::Error>;

    /// Wait until the requested ring condition is met.
    fn wait(
        &self,
        shared_id: GuestResourceId,
        region: ShmRegion,
        condition: ShmWaitCondition,
    ) -> impl Future<Output = Result<ShmReady, Self::Error>> + Send;

    /// Notify waiters that ring state changed.
    fn notify(
        &self,
        shared_id: GuestResourceId,
        region: ShmRegion,
        notify: ShmNotify,
    ) -> Result<ShmReady, Self::Error>;
}

impl<T> SharedMemoryCapability for Arc<T>
where
    T: SharedMemoryCapability,
{
    type Error = T::Error;

    fn alloc(&self, request: ShmAlloc) -> Result<ShmRegion, Self::Error> {
        self.as_ref().alloc(request)
    }

    fn attach_mapping(
        &self,
        binding: &mut dyn SharedMemoryBindingContext,
        shared_id: GuestResourceId,
        region: ShmRegion,
    ) -> Result<GuestUint, Self::Error> {
        self.as_ref().attach_mapping(binding, shared_id, region)
    }

    fn detach_mapping(
        &self,
        binding: &mut dyn SharedMemoryBindingContext,
        shared_id: GuestResourceId,
        region: ShmRegion,
    ) -> Result<(), Self::Error> {
        self.as_ref().detach_mapping(binding, shared_id, region)
    }

    fn wait(
        &self,
        shared_id: GuestResourceId,
        region: ShmRegion,
        condition: ShmWaitCondition,
    ) -> impl Future<Output = Result<ShmReady, Self::Error>> + Send {
        self.as_ref().wait(shared_id, region, condition)
    }

    fn notify(
        &self,
        shared_id: GuestResourceId,
        region: ShmRegion,
        notify: ShmNotify,
    ) -> Result<ShmReady, Self::Error> {
        self.as_ref().notify(shared_id, region, notify)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct Binding {
        attached: Vec<GuestResourceId>,
        detached: Vec<GuestResourceId>,
    }

    impl SharedMemoryBindingContext for Binding {
        fn attach_mapping(&mut self, shared_id: GuestResourceId) -> Result<GuestUint, GuestError> {
            self.attached.push(shared_id);
            Ok(64)
        }

        fn detach_mapping(&mut self, shared_id: GuestResourceId) -> Result<(), GuestError> {
            self.detached.push(shared_id);
            Ok(())
        }
    }

    #[derive(Default)]
    struct Driver {
        calls: Mutex<Vec<&'static str>>,
    }

    impl SharedMemoryCapability for Driver {
        type Error = GuestError;

        fn alloc(&self, request: ShmAlloc) -> Result<ShmRegion, Self::Error> {
            self.calls.lock().expect("calls lock").push("alloc");
            Ok(ShmRegion {
                offset: request.align,
                len: request.size,
            })
        }

        fn attach_mapping(
            &self,
            binding: &mut dyn SharedMemoryBindingContext,
            shared_id: GuestResourceId,
            _region: ShmRegion,
        ) -> Result<GuestUint, Self::Error> {
            self.calls.lock().expect("calls lock").push("attach");
            binding.attach_mapping(shared_id)
        }

        fn detach_mapping(
            &self,
            binding: &mut dyn SharedMemoryBindingContext,
            shared_id: GuestResourceId,
            _region: ShmRegion,
        ) -> Result<(), Self::Error> {
            self.calls.lock().expect("calls lock").push("detach");
            binding.detach_mapping(shared_id)
        }

        fn wait(
            &self,
            _shared_id: GuestResourceId,
            _region: ShmRegion,
            _condition: ShmWaitCondition,
        ) -> impl Future<Output = Result<ShmReady, Self::Error>> + Send {
            self.calls.lock().expect("calls lock").push("wait");
            async {
                Ok(ShmReady {
                    head: 1,
                    tail: 2,
                    sequence: 3,
                    readable: 1,
                    writable: 7,
                })
            }
        }

        fn notify(
            &self,
            _shared_id: GuestResourceId,
            _region: ShmRegion,
            _notify: ShmNotify,
        ) -> Result<ShmReady, Self::Error> {
            self.calls.lock().expect("calls lock").push("notify");
            Ok(ShmReady {
                head: 4,
                tail: 5,
                sequence: 6,
                readable: 1,
                writable: 7,
            })
        }
    }

    #[tokio::test]
    async fn arc_wrapper_forwards_all_calls() {
        let driver = Arc::new(Driver::default());
        let mut binding = Binding::default();

        let region = driver.alloc(ShmAlloc { size: 8, align: 4 }).expect("alloc");
        assert_eq!(region.offset, 4);

        let mapped = driver
            .attach_mapping(&mut binding, 9, region)
            .expect("attach mapping");
        assert_eq!(mapped, 64);

        let ready = driver
            .wait(9, region, ShmWaitCondition::DataAvailable)
            .await
            .expect("wait");
        assert_eq!(ready.sequence, 3);

        let notified = driver
            .notify(
                9,
                region,
                ShmNotify {
                    resource_id: 1,
                    sequence: 4,
                },
            )
            .expect("notify");
        assert_eq!(notified.sequence, 6);

        driver
            .detach_mapping(&mut binding, 9, region)
            .expect("detach mapping");

        let calls = driver.calls.lock().expect("calls lock");
        assert_eq!(
            calls.as_slice(),
            ["alloc", "attach", "wait", "notify", "detach"]
        );
    }
}
