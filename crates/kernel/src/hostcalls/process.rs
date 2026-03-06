use std::{convert::TryFrom, future::Future, sync::Arc};

use selium_abi::{
    AbiParam, AbiScalarType, AbiScalarValue, AbiValue, EntrypointArg, EntrypointInvocation,
    GuestResourceId, ProcessStart,
};
use tracing::debug;

use super::{Contract, HostcallContext, Operation};
use crate::{
    KernelError,
    guest_error::{GuestError, GuestResult},
    registry::{InstanceRegistry, ResourceHandle, ResourceId, ResourceType},
    spi::process::ProcessLifecycleCapability,
};

/// Helpers for working with entrypoint invocations inside the kernel.
pub trait EntrypointInvocationExt {
    /// Materialise entrypoint arguments into ABI values suitable for execution.
    fn materialise_values(
        &self,
        registry: &mut InstanceRegistry,
    ) -> Result<Vec<AbiValue>, KernelError>;

    /// Resolve guest handle-style resource arguments into absolute shared handles.
    fn resolve_resources(self, registry: &InstanceRegistry) -> GuestResult<EntrypointInvocation>;
}

impl EntrypointInvocationExt for EntrypointInvocation {
    fn materialise_values(
        &self,
        registry: &mut InstanceRegistry,
    ) -> Result<Vec<AbiValue>, KernelError> {
        let mut values = Vec::with_capacity(self.args.len());

        for (index, (param, arg)) in self
            .signature
            .params()
            .iter()
            .zip(self.args.iter())
            .enumerate()
        {
            match (param, arg) {
                (AbiParam::Scalar(_), EntrypointArg::Scalar(value)) => {
                    values.push(AbiValue::Scalar(*value));
                }
                (AbiParam::Scalar(AbiScalarType::I32), EntrypointArg::Resource(resource_id)) => {
                    let resource =
                        ResourceId::try_from(*resource_id).map_err(KernelError::IntConvert)?;
                    let slot = registry.insert_id(resource).map_err(KernelError::from)?;
                    let slot = i32::try_from(slot).map_err(KernelError::IntConvert)?;
                    values.push(AbiValue::Scalar(AbiScalarValue::I32(slot)));
                }
                (AbiParam::Scalar(AbiScalarType::U64), EntrypointArg::Resource(resource_id)) => {
                    if registry.registry().resolve_shared(*resource_id).is_none() {
                        return Err(KernelError::Driver(format!(
                            "argument {index} references unknown shared resource"
                        )));
                    }
                    values.push(AbiValue::Scalar(AbiScalarValue::U64(*resource_id)));
                }
                (AbiParam::Buffer, EntrypointArg::Buffer(bytes)) => {
                    values.push(AbiValue::Buffer(bytes.clone()));
                }
                _ => {
                    return Err(KernelError::Driver(format!(
                        "argument {index} incompatible with signature"
                    )));
                }
            }
        }

        Ok(values)
    }

    fn resolve_resources(self, registry: &InstanceRegistry) -> GuestResult<EntrypointInvocation> {
        resolve_entrypoint_resources(self, registry)
    }
}

fn resolve_entrypoint_resources(
    entrypoint: EntrypointInvocation,
    registry: &InstanceRegistry,
) -> GuestResult<EntrypointInvocation> {
    let signature = entrypoint.signature;
    let mut resolved = Vec::with_capacity(entrypoint.args.len());

    for (index, (param, arg)) in signature
        .params()
        .iter()
        .zip(entrypoint.args.into_iter())
        .enumerate()
    {
        let arg = match (param, arg) {
            (AbiParam::Scalar(AbiScalarType::I32), EntrypointArg::Resource(handle)) => {
                let slot = usize::try_from(handle)
                    .map_err(|err| GuestError::from(KernelError::IntConvert(err)))?;
                let rid = registry.entry(slot).ok_or(GuestError::NotFound)?;
                let rid = GuestResourceId::try_from(rid).map_err(|_| {
                    GuestError::from(KernelError::Driver("invalid handle".to_string()))
                })?;
                EntrypointArg::Resource(rid)
            }
            (AbiParam::Scalar(AbiScalarType::U64), EntrypointArg::Resource(handle)) => {
                if registry.registry().resolve_shared(handle).is_none() {
                    return Err(GuestError::from(KernelError::Driver(format!(
                        "argument {index} references unknown shared resource"
                    ))));
                }
                EntrypointArg::Resource(handle)
            }
            (_, arg) => arg,
        };
        resolved.push(arg);
    }

    Ok(EntrypointInvocation {
        signature,
        args: resolved,
    })
}

type ProcessLifecycleOps<C> = (
    Arc<Operation<ProcessStartDriver<C>>>,
    Arc<Operation<ProcessStopDriver<C>>>,
);

/// Hostcall driver that starts new processes.
pub struct ProcessStartDriver<Impl>(Impl);

/// Hostcall driver that stops running processes.
pub struct ProcessStopDriver<Impl>(Impl);

impl<Impl> Contract for ProcessStartDriver<Impl>
where
    Impl: ProcessLifecycleCapability + Clone + Send + 'static,
{
    type Input = ProcessStart;
    type Output = GuestResourceId;

    fn to_future<C>(
        &self,
        context: &mut C,
        input: Self::Input,
    ) -> impl Future<Output = GuestResult<Self::Output>> + Send + 'static
    where
        C: HostcallContext,
    {
        let inner = self.0.clone();
        let registry = context.registry().registry_arc();
        let ProcessStart {
            module_id,
            name,
            capabilities,
            entrypoint,
        } = input;

        let preparation =
            (|| -> GuestResult<(String, String, Vec<selium_abi::Capability>, EntrypointInvocation)> {
                entrypoint
                    .validate()
                    .map_err(|err| GuestError::from(KernelError::Driver(err.to_string())))?;
                let entrypoint = entrypoint.resolve_resources(context.registry())?;
                Ok((module_id, name, capabilities, entrypoint))
            })();

        async move {
            let (module_id, name, capabilities, entrypoint) = preparation?;
            debug!(%module_id, %name, capabilities = ?capabilities, "process_start requested");
            let process_id = registry
                .reserve(None, ResourceType::Process)
                .map_err(GuestError::from)?;

            match inner
                .start(
                    &registry,
                    process_id,
                    &module_id,
                    &name,
                    capabilities,
                    entrypoint,
                )
                .await
            {
                Ok(()) => {}
                Err(err) => {
                    registry.discard(process_id);
                    return Err(err.into());
                }
            }

            let handle =
                GuestResourceId::try_from(process_id).map_err(|_| GuestError::InvalidArgument)?;
            Ok(handle)
        }
    }
}

impl<Impl> Contract for ProcessStopDriver<Impl>
where
    Impl: ProcessLifecycleCapability + Clone + Send + 'static,
{
    type Input = GuestResourceId;
    type Output = ();

    fn to_future<C>(
        &self,
        context: &mut C,
        input: Self::Input,
    ) -> impl Future<Output = GuestResult<Self::Output>> + Send + 'static
    where
        C: HostcallContext,
    {
        let inner = self.0.clone();
        let registry = context.registry().registry_arc();

        async move {
            let handle = ResourceId::try_from(input).map_err(|_| GuestError::InvalidArgument)?;
            if let Some(meta) = registry.metadata(handle)
                && meta.kind != ResourceType::Process
            {
                return Err(GuestError::InvalidArgument);
            }

            let mut process = registry
                .remove(ResourceHandle::<Impl::Process>::new(handle))
                .ok_or(GuestError::NotFound)?;
            inner.stop(&mut process).await.map_err(Into::into)?;
            Ok(())
        }
    }
}

/// Build hostcall operations for process lifecycle management.
pub fn lifecycle_ops<C>(cap: C) -> ProcessLifecycleOps<C>
where
    C: ProcessLifecycleCapability + Clone + Send + 'static,
{
    (
        Operation::from_hostcall(
            ProcessStartDriver(cap.clone()),
            selium_abi::hostcall_contract!(PROCESS_START),
        ),
        Operation::from_hostcall(
            ProcessStopDriver(cap),
            selium_abi::hostcall_contract!(PROCESS_STOP),
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    use selium_abi::{AbiSignature, Capability};

    use crate::registry::{Registry, ResourceType};

    struct TestContext {
        registry: InstanceRegistry,
    }

    impl HostcallContext for TestContext {
        fn registry(&self) -> &InstanceRegistry {
            &self.registry
        }

        fn registry_mut(&mut self) -> &mut InstanceRegistry {
            &mut self.registry
        }

        fn mailbox_base(&mut self) -> Option<usize> {
            None
        }
    }

    fn context() -> TestContext {
        let registry = Registry::new();
        let instance = registry.instance().expect("instance");
        TestContext { registry: instance }
    }

    #[derive(Clone)]
    struct StartCapability {
        fail: bool,
        seen_process_id: Arc<Mutex<Option<ResourceId>>>,
    }

    impl ProcessLifecycleCapability for StartCapability {
        type Process = ();
        type Error = GuestError;

        fn start(
            &self,
            _registry: &Arc<crate::registry::Registry>,
            process_id: ResourceId,
            _module_id: &str,
            _name: &str,
            _capabilities: Vec<Capability>,
            _entrypoint: EntrypointInvocation,
        ) -> impl Future<Output = Result<(), Self::Error>> + Send {
            let fail = self.fail;
            *self.seen_process_id.lock().expect("process id lock") = Some(process_id);
            async move {
                if fail {
                    Err(GuestError::Subsystem("start failed".to_string()))
                } else {
                    Ok(())
                }
            }
        }

        async fn stop(&self, _instance: &mut Self::Process) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    #[derive(Clone)]
    struct StopCapability {
        stopped: Arc<Mutex<bool>>,
    }

    impl ProcessLifecycleCapability for StopCapability {
        type Process = String;
        type Error = GuestError;

        async fn start(
            &self,
            _registry: &Arc<crate::registry::Registry>,
            _process_id: ResourceId,
            _module_id: &str,
            _name: &str,
            _capabilities: Vec<Capability>,
            _entrypoint: EntrypointInvocation,
        ) -> Result<(), Self::Error> {
            Ok(())
        }

        async fn stop(&self, instance: &mut Self::Process) -> Result<(), Self::Error> {
            *self.stopped.lock().expect("stop lock") = true;
            instance.push_str(":stopped");
            Ok(())
        }
    }

    #[test]
    fn resolve_resources_maps_instance_slot_to_resource_id() {
        let mut ctx = context();
        let slot = ctx
            .registry_mut()
            .insert(5u32, None, ResourceType::Other)
            .expect("insert");
        let expected = ctx.registry().entry(slot).expect("resource id");

        let invocation = EntrypointInvocation::new(
            AbiSignature::new(vec![AbiParam::Scalar(AbiScalarType::I32)], Vec::new()),
            vec![EntrypointArg::Resource(slot as GuestResourceId)],
        )
        .expect("invocation");

        let resolved = invocation
            .resolve_resources(ctx.registry())
            .expect("resolve resources");
        assert_eq!(
            resolved.args,
            vec![EntrypointArg::Resource(expected as GuestResourceId)]
        );
    }

    #[tokio::test]
    async fn process_start_driver_discards_reservation_on_failure() {
        let process_id_seen = Arc::new(Mutex::new(None));
        let capability = StartCapability {
            fail: true,
            seen_process_id: Arc::clone(&process_id_seen),
        };
        let driver = ProcessStartDriver(capability);
        let mut ctx = context();
        let input = ProcessStart {
            module_id: "m".to_string(),
            name: "n".to_string(),
            capabilities: vec![Capability::TimeRead],
            entrypoint: EntrypointInvocation::new(
                AbiSignature::new(Vec::new(), Vec::new()),
                Vec::new(),
            )
            .expect("entrypoint"),
        };

        let err = driver
            .to_future(&mut ctx, input)
            .await
            .expect_err("start should fail");
        assert!(matches!(err, GuestError::Subsystem(_)));
        let process_id = (*process_id_seen.lock().expect("process lock")).expect("process id");
        assert!(ctx.registry().registry().metadata(process_id).is_none());
    }

    #[tokio::test]
    async fn process_stop_driver_rejects_non_process_handle() {
        let driver = ProcessStopDriver(StopCapability {
            stopped: Arc::new(Mutex::new(false)),
        });
        let mut ctx = context();
        let other = ctx
            .registry_mut()
            .registry()
            .add(1u32, None, ResourceType::Other)
            .expect("add other");

        let err = driver
            .to_future(&mut ctx, other.into_id() as GuestResourceId)
            .await
            .expect_err("non-process should fail");
        assert!(matches!(err, GuestError::InvalidArgument));
    }

    #[tokio::test]
    async fn process_stop_driver_calls_capability_for_process() {
        let stopped = Arc::new(Mutex::new(false));
        let driver = ProcessStopDriver(StopCapability {
            stopped: Arc::clone(&stopped),
        });
        let mut ctx = context();
        let process = ctx
            .registry_mut()
            .registry()
            .add("proc".to_string(), None, ResourceType::Process)
            .expect("add process");

        driver
            .to_future(&mut ctx, process.into_id() as GuestResourceId)
            .await
            .expect("stop succeeds");
        assert!(*stopped.lock().expect("stopped lock"));
    }
}
