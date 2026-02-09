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
