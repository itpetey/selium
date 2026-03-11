//! Process lifecycle SPI contracts.

use std::{future::Future, sync::Arc};

use selium_abi::{Capability, EntrypointInvocation, ProcessLogBindings};

use crate::{
    guest_error::GuestError,
    registry::{Registry, ResourceId},
};

#[derive(Debug)]
pub struct ProcessStartRequest<'a> {
    pub process_id: ResourceId,
    pub module_id: &'a str,
    pub name: &'a str,
    /// Stable Selium workload key when the process belongs to a control-plane-managed workload.
    pub workload_key: Option<&'a str>,
    /// Stable Selium instance identifier for checkpointing and durable metadata.
    pub instance_id: Option<&'a str>,
    /// Opaque external account reference supplied by Selium control-plane resources.
    pub external_account_ref: Option<&'a str>,
    pub capabilities: Vec<Capability>,
    pub guest_log_bindings: ProcessLogBindings,
    pub network_egress_profiles: Vec<String>,
    pub network_ingress_bindings: Vec<String>,
    pub storage_logs: Vec<String>,
    pub storage_blobs: Vec<String>,
    pub entrypoint: EntrypointInvocation,
}

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
        request: ProcessStartRequest<'_>,
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
        request: ProcessStartRequest<'_>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.as_ref().start(registry, request)
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
            _request: ProcessStartRequest<'_>,
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
                ProcessStartRequest {
                    process_id,
                    module_id: "m",
                    name: "n",
                    workload_key: None,
                    instance_id: None,
                    external_account_ref: None,
                    capabilities: vec![Capability::TimeRead],
                    guest_log_bindings: ProcessLogBindings::default(),
                    network_egress_profiles: Vec::new(),
                    network_ingress_bindings: Vec::new(),
                    storage_logs: Vec::new(),
                    storage_blobs: Vec::new(),
                    entrypoint: invocation,
                },
            )
            .await
            .expect("start");
        let mut process_state = 0;
        driver.stop(&mut process_state).await.expect("stop");

        assert!(*driver.started.lock().expect("started lock"));
        assert!(*driver.stopped.lock().expect("stopped lock"));
    }
}
