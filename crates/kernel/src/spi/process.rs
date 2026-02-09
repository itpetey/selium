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
