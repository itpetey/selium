//! Process lifecycle SPI contracts.

use std::{future::Future, sync::Arc};

use selium_abi::{Capability, EntrypointInvocation};

use crate::{
    guest_error::GuestError,
    registry::{Registry, ResourceId},
};

/// Capability responsible for starting and stopping guest instances.
pub trait ProcessLifecycleCapability {
    /// Runtime-specific process state type.
    type Process: Send;
    /// Runtime-specific error type.
    type Error: Into<GuestError>;

    /// Start a new process, identified by `module_id` and `name`.
    fn start(
        &self,
        registry: &Arc<Registry>,
        process_id: ResourceId,
        module_id: &str,
        name: &str,
        capabilities: Vec<Capability>,
        entrypoint: EntrypointInvocation,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Stop a running process.
    fn stop(
        &self,
        instance: &mut Self::Process,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

impl<T> ProcessLifecycleCapability for Arc<T>
where
    T: ProcessLifecycleCapability,
{
    type Process = T::Process;
    type Error = T::Error;

    fn start(
        &self,
        registry: &Arc<Registry>,
        process_id: ResourceId,
        module_id: &str,
        name: &str,
        capabilities: Vec<Capability>,
        entrypoint: EntrypointInvocation,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.as_ref().start(
            registry,
            process_id,
            module_id,
            name,
            capabilities,
            entrypoint,
        )
    }

    fn stop(
        &self,
        instance: &mut Self::Process,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.as_ref().stop(instance)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use selium_abi::AbiSignature;

    struct Driver {
        started: Mutex<bool>,
        stopped: Mutex<bool>,
    }

    impl ProcessLifecycleCapability for Driver {
        type Process = u32;
        type Error = GuestError;

        fn start(
            &self,
            _registry: &Arc<Registry>,
            _process_id: ResourceId,
            _module_id: &str,
            _name: &str,
            _capabilities: Vec<Capability>,
            _entrypoint: EntrypointInvocation,
        ) -> impl Future<Output = Result<(), Self::Error>> + Send {
            *self.started.lock().expect("started lock") = true;
            async { Ok(()) }
        }

        fn stop(
            &self,
            _instance: &mut Self::Process,
        ) -> impl Future<Output = Result<(), Self::Error>> + Send {
            *self.stopped.lock().expect("stopped lock") = true;
            async { Ok(()) }
        }
    }

    #[tokio::test]
    async fn arc_wrapper_forwards_calls() {
        let registry = Registry::new();
        let process_id = registry
            .add((), None, crate::registry::ResourceType::Process)
            .expect("process")
            .into_id();
        let driver = Arc::new(Driver {
            started: Mutex::new(false),
            stopped: Mutex::new(false),
        });
        let invocation =
            EntrypointInvocation::new(AbiSignature::new(Vec::new(), Vec::new()), Vec::new())
                .expect("invocation");

        driver
            .start(
                &registry,
                process_id,
                "m",
                "n",
                vec![Capability::TimeRead],
                invocation,
            )
            .await
            .expect("start");
        let mut process_state = 0;
        driver.stop(&mut process_state).await.expect("stop");

        assert!(*driver.started.lock().expect("started lock"));
        assert!(*driver.stopped.lock().expect("stopped lock"));
    }
}
