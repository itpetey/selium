use std::{convert::TryFrom, future::Future, sync::Arc};

use selium_abi::{
    AbiParam, AbiScalarType, AbiScalarValue, AbiValue, Capability, EntrypointArg,
    EntrypointInvocation, GuestResourceId, ProcessLogBindings, ProcessStart,
};
use tracing::debug;

use super::{Contract, HostcallContext, Operation};
use crate::{
    KernelError,
    guest_error::{GuestError, GuestResult},
    registry::{InstanceRegistry, ResourceHandle, ResourceId, ResourceType},
    spi::process::{ProcessLifecycleCapability, ProcessStartRequest},
};

struct PreparedProcessStart {
    module_id: String,
    name: String,
    capabilities: Vec<selium_abi::Capability>,
    guest_log_bindings: ProcessLogBindings,
    network_egress_profiles: Vec<String>,
    network_ingress_bindings: Vec<String>,
    storage_logs: Vec<String>,
    storage_blobs: Vec<String>,
    entrypoint: EntrypointInvocation,
}

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
        let registrar = context.registry().registrar();
        let root_session = context
            .registry()
            .extension::<crate::services::session_service::RootSession>();
        let ProcessStart {
            module_id,
            name,
            capabilities,
            network_egress_profiles,
            network_ingress_bindings,
            storage_logs,
            storage_blobs,
            entrypoint,
        } = input;
        let guest_log_bindings = context
            .registry()
            .extension::<ProcessLogBindings>()
            .as_deref()
            .copied()
            .unwrap_or_default();

        let preparation = (|| -> GuestResult<(
            PreparedProcessStart,
            Arc<crate::services::session_service::RootSession>,
            std::collections::HashMap<Capability, crate::services::session_service::ResourceScope>,
        )> {
            super::ensure_capability_authorised(context.registry(), Capability::ProcessLifecycle)?;
            let root_session = root_session.ok_or(GuestError::PermissionDenied)?;
            entrypoint
                .validate()
                .map_err(|err| GuestError::from(KernelError::Driver(err.to_string())))?;
            let entrypoint = entrypoint.resolve_resources(context.registry())?;
            let entitlements = root_session
                .0
                .scoped_entitlements_for(&capabilities)
                .map_err(|_| GuestError::PermissionDenied)?;
            Ok((
                PreparedProcessStart {
                    module_id,
                    name,
                    capabilities,
                    guest_log_bindings,
                    network_egress_profiles,
                    network_ingress_bindings,
                    storage_logs,
                    storage_blobs,
                    entrypoint,
                },
                root_session,
                entitlements,
            ))
        })();

        async move {
            let (
                PreparedProcessStart {
                    module_id,
                    name,
                    capabilities,
                    guest_log_bindings,
                    network_egress_profiles,
                    network_ingress_bindings,
                    storage_logs,
                    storage_blobs,
                    entrypoint,
                },
                root_session,
                entitlements,
            ) = preparation?;
            debug!(
                %module_id,
                %name,
                principal = %root_session.0.principal(),
                capabilities = ?capabilities,
                "process_start requested"
            );
            let process_id = registry
                .reserve(None, ResourceType::Process)
                .map_err(GuestError::from)?;

            match inner
                .start(
                    &registry,
                    ProcessStartRequest {
                        process_id,
                        module_id: &module_id,
                        name: &name,
                        workload_key: None,
                        instance_id: None,
                        external_account_ref: None,
                        principal: Some(root_session.0.principal()),
                        entitlements,
                        capabilities,
                        guest_log_bindings,
                        network_egress_profiles,
                        network_ingress_bindings,
                        storage_logs,
                        storage_blobs,
                        entrypoint,
                    },
                )
                .await
            {
                Ok(()) => {}
                Err(err) => {
                    registry.discard(process_id);
                    return Err(err.into());
                }
            }
            registrar
                .grant_root_session_resources(&[Capability::ProcessLifecycle], process_id)
                .map_err(GuestError::from)?;

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
        let authorisation = (|| -> GuestResult<()> {
            let handle = ResourceId::try_from(input).map_err(|_| GuestError::InvalidArgument)?;
            super::ensure_resource_authorised(
                context.registry(),
                Capability::ProcessLifecycle,
                handle,
            )
        })();

        async move {
            authorisation?;
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
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    };

    use selium_abi::{AbiSignature, Capability};

    use crate::{
        registry::{Registry, ResourceType},
        services::session_service::{ResourceScope, RootSession, Session, SessionAuthnMethod},
    };

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
        context_with_capabilities(Capability::ALL.to_vec())
    }

    fn context_with_capabilities(capabilities: Vec<Capability>) -> TestContext {
        context_with_root_session(Session::bootstrap(capabilities, [0; 32]))
    }

    fn context_with_root_session(root_session: Session) -> TestContext {
        let registry = Registry::new();
        let mut instance = registry.instance().expect("instance");
        instance
            .insert_extension(RootSession(root_session))
            .expect("root session");
        TestContext { registry: instance }
    }

    #[derive(Clone)]
    struct StartCapability {
        fail: bool,
        seen_process_id: Arc<Mutex<Option<ResourceId>>>,
        seen_entitlements: Arc<Mutex<Option<HashMap<Capability, ResourceScope>>>>,
    }

    impl ProcessLifecycleCapability for StartCapability {
        type Process = ();
        type Error = GuestError;

        fn start(
            &self,
            _registry: &Arc<crate::registry::Registry>,
            request: ProcessStartRequest<'_>,
        ) -> impl Future<Output = Result<(), Self::Error>> + Send {
            let fail = self.fail;
            *self.seen_process_id.lock().expect("process id lock") = Some(request.process_id);
            *self.seen_entitlements.lock().expect("entitlements lock") =
                Some(request.entitlements.clone());
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
            _request: ProcessStartRequest<'_>,
        ) -> Result<(), Self::Error> {
            Ok(())
        }

        async fn stop(&self, instance: &mut Self::Process) -> Result<(), Self::Error> {
            *self.stopped.lock().expect("stop lock") = true;
            instance.push_str(":stopped");
            Ok(())
        }
    }

    #[derive(Clone)]
    struct ScopedLifecycleCapability {
        stopped: Arc<Mutex<bool>>,
    }

    impl ProcessLifecycleCapability for ScopedLifecycleCapability {
        type Process = String;
        type Error = GuestError;

        fn start(
            &self,
            registry: &Arc<crate::registry::Registry>,
            request: ProcessStartRequest<'_>,
        ) -> impl Future<Output = Result<(), Self::Error>> + Send {
            registry
                .initialise(request.process_id, "child".to_string())
                .expect("initialise child process");
            async { Ok(()) }
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
            seen_entitlements: Arc::new(Mutex::new(None)),
        };
        let driver = ProcessStartDriver(capability);
        let mut ctx = context();
        let input = ProcessStart {
            module_id: "m".to_string(),
            name: "n".to_string(),
            capabilities: vec![Capability::TimeRead],
            network_egress_profiles: Vec::new(),
            network_ingress_bindings: Vec::new(),
            storage_logs: Vec::new(),
            storage_blobs: Vec::new(),
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
    async fn process_start_driver_rejects_child_capabilities_outside_root_session() {
        let process_id_seen = Arc::new(Mutex::new(None));
        let driver = ProcessStartDriver(StartCapability {
            fail: false,
            seen_process_id: Arc::clone(&process_id_seen),
            seen_entitlements: Arc::new(Mutex::new(None)),
        });
        let mut ctx = context_with_capabilities(vec![Capability::ProcessLifecycle]);
        let input = ProcessStart {
            module_id: "m".to_string(),
            name: "n".to_string(),
            capabilities: vec![Capability::StorageBlobRead],
            network_egress_profiles: Vec::new(),
            network_ingress_bindings: Vec::new(),
            storage_logs: Vec::new(),
            storage_blobs: Vec::new(),
            entrypoint: EntrypointInvocation::new(
                AbiSignature::new(Vec::new(), Vec::new()),
                Vec::new(),
            )
            .expect("entrypoint"),
        };

        let err = driver
            .to_future(&mut ctx, input)
            .await
            .expect_err("child capability should be denied");
        assert!(matches!(err, GuestError::PermissionDenied));
        assert!(process_id_seen.lock().expect("process lock").is_none());
    }

    #[tokio::test]
    async fn process_start_driver_preserves_scoped_child_entitlements() {
        let process_id_seen = Arc::new(Mutex::new(None));
        let seen_entitlements = Arc::new(Mutex::new(None));
        let driver = ProcessStartDriver(StartCapability {
            fail: false,
            seen_process_id: Arc::clone(&process_id_seen),
            seen_entitlements: Arc::clone(&seen_entitlements),
        });
        let mut ctx = context_with_root_session(Session::bootstrap_with_scoped_principal(
            HashMap::from([
                (Capability::ProcessLifecycle, ResourceScope::Any),
                (
                    Capability::StorageBlobRead,
                    ResourceScope::Some(std::collections::HashSet::from([7usize])),
                ),
            ]),
            [0; 32],
            selium_abi::PrincipalRef::new(selium_abi::PrincipalKind::Internal, "test"),
            SessionAuthnMethod::Delegated,
        ));
        let input = ProcessStart {
            module_id: "m".to_string(),
            name: "n".to_string(),
            capabilities: vec![Capability::StorageBlobRead],
            network_egress_profiles: Vec::new(),
            network_ingress_bindings: Vec::new(),
            storage_logs: Vec::new(),
            storage_blobs: Vec::new(),
            entrypoint: EntrypointInvocation::new(
                AbiSignature::new(Vec::new(), Vec::new()),
                Vec::new(),
            )
            .expect("entrypoint"),
        };

        let handle = driver
            .to_future(&mut ctx, input)
            .await
            .expect("start should succeed");
        assert!(handle > 0);
        let entitlements = seen_entitlements
            .lock()
            .expect("entitlements lock")
            .clone()
            .expect("captured entitlements");
        assert!(matches!(
            entitlements.get(&Capability::StorageBlobRead),
            Some(ResourceScope::Some(ids)) if ids.contains(&7usize) && ids.len() == 1
        ));
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

    #[tokio::test]
    async fn process_start_driver_grants_scoped_process_lifecycle_access() {
        let stopped = Arc::new(Mutex::new(false));
        let start_driver = ProcessStartDriver(ScopedLifecycleCapability {
            stopped: Arc::clone(&stopped),
        });
        let stop_driver = ProcessStopDriver(ScopedLifecycleCapability {
            stopped: Arc::clone(&stopped),
        });
        let mut ctx = context_with_root_session(Session::bootstrap_with_scoped_principal(
            HashMap::from([(
                Capability::ProcessLifecycle,
                ResourceScope::Some(std::collections::HashSet::new()),
            )]),
            [0; 32],
            selium_abi::PrincipalRef::new(selium_abi::PrincipalKind::Internal, "test"),
            SessionAuthnMethod::Delegated,
        ));
        let input = ProcessStart {
            module_id: "m".to_string(),
            name: "n".to_string(),
            capabilities: Vec::new(),
            network_egress_profiles: Vec::new(),
            network_ingress_bindings: Vec::new(),
            storage_logs: Vec::new(),
            storage_blobs: Vec::new(),
            entrypoint: EntrypointInvocation::new(
                AbiSignature::new(Vec::new(), Vec::new()),
                Vec::new(),
            )
            .expect("entrypoint"),
        };

        let handle = start_driver
            .to_future(&mut ctx, input)
            .await
            .expect("start succeeds");
        stop_driver
            .to_future(&mut ctx, handle)
            .await
            .expect("scoped process manager should stop child");
        assert!(*stopped.lock().expect("stop lock"));
    }
}
