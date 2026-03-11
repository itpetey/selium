//! Wasmtime integration for the Selium host runtime.

use std::{
    collections::{HashMap, HashSet},
    fmt::{Display, Formatter},
    future::Future,
    sync::{Arc, RwLock},
    time::Duration,
};

use selium_abi::{
    self, AbiParam, AbiScalarType, AbiScalarValue, AbiSignature, AbiValue, CallPlan, CallPlanError,
    Capability, EntrypointInvocation, ProcessLogBindings, hostcalls,
};
use selium_kernel::{
    KernelError,
    r#async::futures::FutureSharedState,
    guest_error::GuestError,
    hostcalls::process::EntrypointInvocationExt,
    registry::{InstanceRegistry, ProcessIdentity, Registry, ResourceId},
    spi::{
        module_repository::{ModuleRepositoryError, ModuleRepositoryReadCapability},
        process::{ProcessLifecycleCapability, ProcessStartRequest},
    },
};
use tokio::task::JoinHandle;
use tracing::{debug, warn};
use wasmtime::{Caller, Config, Engine, Func, Linker, Memory, Module, Store, Val, ValType};

use super::{
    guest_async::GuestAsync,
    guest_data::{GuestInt, GuestUint, write_poll_result},
    guest_logs::{GuestLogState, link_guest_logs},
    hostcall_linker::LinkableOperation,
    mailbox,
};
use crate::usage::{ProcessUsageAttribution, ProcessUsageHandle, RuntimeUsageCollector};

const PREALLOC_PAGES: u64 = 256;

/// Wasmtime execution engine used by the runtime process lifecycle adaptor.
pub struct WasmtimeRuntime {
    pub(crate) engine: Engine,
    available_caps: RwLock<HashMap<Capability, Vec<Arc<dyn LinkableOperation>>>>,
    guest_async: Arc<GuestAsync>,
    usage: Arc<RuntimeUsageCollector>,
    shared_memory: Arc<selium_kernel::services::shared_memory_service::SharedMemoryDriver>,
}

pub struct WasmtimeRunRequest<'a> {
    pub registry: &'a Arc<Registry>,
    pub process_id: ResourceId,
    pub module_id: &'a str,
    pub module: Module,
    pub name: &'a str,
    pub workload_key: Option<&'a str>,
    pub instance_id: Option<&'a str>,
    pub external_account_ref: Option<&'a str>,
    pub capabilities: &'a [Capability],
    pub network_egress_profiles: &'a [String],
    pub network_ingress_bindings: &'a [String],
    pub storage_logs: &'a [String],
    pub storage_blobs: &'a [String],
    pub guest_log_bindings: ProcessLogBindings,
    pub entrypoint: EntrypointInvocation,
}

pub struct WasmtimeProcess {
    handle: JoinHandle<Result<Vec<AbiValue>, wasmtime::Error>>,
    mailbox: &'static dyn selium_kernel::spi::wake_mailbox::WakeMailbox,
    usage: Arc<ProcessUsageHandle>,
}

impl WasmtimeProcess {
    fn new(
        handle: JoinHandle<Result<Vec<AbiValue>, wasmtime::Error>>,
        mailbox: &'static dyn selium_kernel::spi::wake_mailbox::WakeMailbox,
        usage: Arc<ProcessUsageHandle>,
    ) -> Self {
        Self {
            handle,
            mailbox,
            usage,
        }
    }
}

#[derive(Debug)]
/// Runtime error surfaced by the Wasmtime process adaptor.
pub enum Error {
    CapabilityUnavailable(Capability),
    Kernel(KernelError),
    ModuleRepository(ModuleRepositoryError),
    Wasmtime(wasmtime::Error),
    CapabilityRegistryPoisoned,
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CapabilityUnavailable(capability) => {
                write!(
                    f,
                    "requested capability ({capability}) is not part of this kernel"
                )
            }
            Self::Kernel(err) => write!(f, "kernel error: {err}"),
            Self::ModuleRepository(err) => write!(f, "module repository error: {err}"),
            Self::Wasmtime(err) => write!(f, "wasmtime error: {err}"),
            Self::CapabilityRegistryPoisoned => {
                write!(f, "capability registry lock is poisoned")
            }
        }
    }
}

impl std::error::Error for Error {}

impl From<KernelError> for Error {
    fn from(value: KernelError) -> Self {
        Self::Kernel(value)
    }
}

impl From<ModuleRepositoryError> for Error {
    fn from(value: ModuleRepositoryError) -> Self {
        Self::ModuleRepository(value)
    }
}

impl From<wasmtime::Error> for Error {
    fn from(value: wasmtime::Error) -> Self {
        Self::Wasmtime(value)
    }
}

impl From<CallPlanError> for Error {
    fn from(value: CallPlanError) -> Self {
        Self::Kernel(KernelError::Driver(value.to_string()))
    }
}

impl WasmtimeRuntime {
    pub fn new(
        available_caps: HashMap<Capability, Vec<Arc<dyn LinkableOperation>>>,
        guest_async: Arc<GuestAsync>,
        usage: Arc<RuntimeUsageCollector>,
        shared_memory: Arc<selium_kernel::services::shared_memory_service::SharedMemoryDriver>,
    ) -> Result<Self, Error> {
        let mut config = Config::new();
        config.memory_may_move(false);

        Ok(Self {
            engine: Engine::new(&config)?,
            available_caps: RwLock::new(available_caps),
            guest_async,
            usage,
            shared_memory,
        })
    }

    pub fn extend_capability(
        &self,
        capability: Capability,
        operations: impl IntoIterator<Item = Arc<dyn LinkableOperation>>,
    ) -> Result<(), Error> {
        let mut map = self
            .available_caps
            .write()
            .map_err(|_| Error::CapabilityRegistryPoisoned)?;
        let entry = map.entry(capability).or_default();
        entry.extend(operations);
        Ok(())
    }

    pub async fn run(&self, request: WasmtimeRunRequest<'_>) -> Result<(), Error> {
        let WasmtimeRunRequest {
            registry,
            process_id,
            module_id,
            module,
            name,
            workload_key,
            instance_id,
            external_account_ref,
            capabilities,
            network_egress_profiles,
            network_ingress_bindings,
            storage_logs,
            storage_blobs,
            guest_log_bindings,
            entrypoint,
        } = request;
        let mut linker = Linker::new(&self.engine);
        let operations_to_link = {
            let map = self
                .available_caps
                .read()
                .map_err(|_| Error::CapabilityRegistryPoisoned)?;
            let mut ops = Vec::new();
            let requested: HashSet<Capability> = capabilities.iter().copied().collect();
            for capability in &requested {
                let operations = map
                    .get(capability)
                    .ok_or(Error::CapabilityUnavailable(*capability))?;

                if operations.is_empty() {
                    return Err(Error::CapabilityUnavailable(*capability));
                }

                ops.extend(operations.iter().cloned());
            }
            ops.extend(stub_operations_for_missing(&requested));
            ops
        };

        for op in operations_to_link {
            op.link(&mut linker)?;
        }

        self.guest_async.link(&mut linker)?;
        let guest_log_state = Arc::new(GuestLogState::new(guest_log_bindings));
        link_guest_logs(
            &mut linker,
            Arc::clone(&self.shared_memory),
            Arc::clone(&guest_log_state),
        )?;

        let instance_registry = registry.instance().map_err(KernelError::from)?;
        let mut store = Store::new(&self.engine, instance_registry);
        let process_id_string = process_id.to_string();
        let workload_key_string = workload_key.unwrap_or(&process_id_string).to_string();
        let usage = self
            .usage
            .register_process(
                workload_key_string,
                process_id_string,
                ProcessUsageAttribution {
                    instance_id: instance_id.map(ToString::to_string),
                    external_account_ref: external_account_ref.map(ToString::to_string),
                    module_id: module_id.to_string(),
                },
            )
            .await
            .map_err(|err| Error::Kernel(KernelError::Driver(err.to_string())))?;
        store
            .data_mut()
            .set_process_id(process_id)
            .map_err(KernelError::from)?;
        let identity = ProcessIdentity::new(process_id);
        store
            .data_mut()
            .insert_extension(identity)
            .map_err(KernelError::from)?;
        store
            .data_mut()
            .insert_extension(guest_log_bindings)
            .map_err(KernelError::from)?;
        store
            .data_mut()
            .insert_extension(
                selium_kernel::spi::network::NetworkProcessPolicy::new(
                    network_egress_profiles.iter().cloned(),
                    network_ingress_bindings.iter().cloned(),
                )
                .with_usage_recorder(usage.clone()),
            )
            .map_err(KernelError::from)?;
        store
            .data_mut()
            .insert_extension(
                selium_kernel::spi::storage::StorageProcessPolicy::new(
                    storage_logs.iter().cloned(),
                    storage_blobs.iter().cloned(),
                )
                .with_usage_recorder(usage.clone()),
            )
            .map_err(KernelError::from)?;
        // Limit linear memory growth to keep the mailbox pointers stable across the
        // instance lifetime. We preallocate and then lock the limit to the current
        // size so guest-initiated growth fails fast instead of moving the base
        // address out from under host-side wakers.
        let instance = linker.instantiate_async(&mut store, &module).await?;

        // Initialise waker mailbox
        let memory = instance.get_memory(&mut store, "memory").ok_or_else(|| {
            Error::Kernel(KernelError::Driver("guest memory missing".to_string()))
        })?;
        let memory_bytes = preallocate_memory(&memory, &mut store);
        usage.set_memory_high_watermark_bytes(memory_bytes as u64);
        store
            .data_mut()
            .set_memory_limit(memory_bytes)
            .map_err(KernelError::from)?;
        let mb = unsafe { mailbox::create_guest_mailbox(&memory, &mut store) };
        store
            .data_mut()
            .load_mailbox(mb)
            .map_err(KernelError::from)?;

        let signature = entrypoint.signature().clone();
        let call_values = {
            let registry = store.data_mut();
            entrypoint.materialise_values(registry)?
        };
        let plan = CallPlan::new(&signature, &call_values)?;
        materialise_plan(&memory, &mut store, &plan)?;

        let func = instance.get_func(&mut store, name).ok_or_else(|| {
            Error::Wasmtime(wasmtime::Error::msg(format!(
                "entrypoint `{name}` not found"
            )))
        })?;
        let func_ty = func.ty(&store);
        let param_types: Vec<ValType> = func_ty.params().collect();
        let result_types: Vec<ValType> = func_ty.results().collect();
        let expected_params = flatten_signature_types(signature.params());
        let expected_results = flatten_signature_types(signature.results());

        let params_match = param_types.len() == expected_params.len()
            && param_types
                .iter()
                .zip(expected_params.iter())
                .all(|(actual, expected)| valtype_eq(actual, expected));

        if !params_match {
            return Err(Error::Kernel(KernelError::Driver(format!(
                "entrypoint `{name}` expects params {:?}, got {:?}",
                expected_params, param_types
            ))));
        }

        let results_match = result_types.len() == expected_results.len()
            && result_types
                .iter()
                .zip(expected_results.iter())
                .all(|(actual, expected)| valtype_eq(actual, expected));

        if !results_match {
            return Err(Error::Kernel(KernelError::Driver(format!(
                "entrypoint expects results {:?}, got {:?}",
                expected_results, result_types
            ))));
        }

        let params = prepare_params(&param_types, plan.params())
            .map_err(|err| Error::Kernel(KernelError::Driver(err)))?;
        let result_template = prepare_results(&result_types)
            .map_err(|err| Error::Kernel(KernelError::Driver(err)))?;
        let signature_clone = signature.clone();
        let (start_tx, start_rx) = tokio::sync::oneshot::channel();
        let entrypoint_name = name.to_string();
        let usage_for_task = usage.clone();
        let runtime_registry = Arc::clone(registry);
        let shared_memory = Arc::clone(&self.shared_memory);
        let handle = tokio::spawn(async move {
            // Wait for registration before invoking entrypoint. This prevents races between
            // guests registering resources and the process_id being set on the registry.
            if start_rx.await.is_err() {
                return Err(wasmtime::Error::msg("process start cancelled"));
            }
            debug!(process_id, entrypoint = %entrypoint_name, "invoking guest entrypoint");
            let result = invoke_entrypoint(
                func,
                store,
                memory,
                params,
                result_template,
                signature_clone,
            )
            .await;
            match &result {
                Ok(values) => debug!(
                    process_id,
                    entrypoint = %entrypoint_name,
                    result_count = values.len(),
                    "guest entrypoint completed"
                ),
                Err(err) => warn!(
                    process_id,
                    entrypoint = %entrypoint_name,
                    "guest entrypoint failed: {err:#}"
                ),
            }
            if let Err(err) = guest_log_state
                .close_all(&runtime_registry, shared_memory.as_ref())
                .await
            {
                warn!(process_id, "failed to close guest log streams: {err}");
            }
            if let Err(err) = usage_for_task.finish().await {
                warn!(process_id, "runtime usage final emission failed: {err:#}");
            }
            result
        });

        registry
            .initialise(process_id, WasmtimeProcess::new(handle, mb, usage))
            .map_err(|err| Error::Kernel(KernelError::from(err)))?;

        // Trigger entrypoint exec
        start_tx.send(()).map_err(|_| {
            Error::Kernel(KernelError::Driver("process start cancelled".to_string()))
        })?;

        Ok(())
    }
}

#[derive(Clone)]
/// Process lifecycle adaptor backed by [`WasmtimeRuntime`].
pub struct WasmtimeProcessDriver {
    runtime: Arc<WasmtimeRuntime>,
    repository: Arc<dyn ModuleRepositoryReadCapability + Send + Sync>,
}

impl WasmtimeProcessDriver {
    /// Create a new Wasmtime-backed process lifecycle adaptor.
    pub fn new(
        runtime: Arc<WasmtimeRuntime>,
        repository: Arc<dyn ModuleRepositoryReadCapability + Send + Sync>,
    ) -> Arc<Self> {
        Arc::new(Self {
            runtime,
            repository,
        })
    }
}

impl ProcessLifecycleCapability for WasmtimeProcessDriver {
    type Process = WasmtimeProcess;
    type Error = Error;

    fn start(
        &self,
        registry: &Arc<Registry>,
        request: ProcessStartRequest<'_>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        let inner = self.clone();

        async move {
            let ProcessStartRequest {
                process_id,
                module_id,
                name,
                workload_key,
                instance_id,
                external_account_ref,
                capabilities,
                network_egress_profiles,
                network_ingress_bindings,
                storage_logs,
                storage_blobs,
                guest_log_bindings,
                entrypoint,
            } = request;
            let bytes = inner.repository.read(module_id)?;
            let module = Module::from_binary(&inner.runtime.engine, &bytes)?;
            inner
                .runtime
                .run(WasmtimeRunRequest {
                    registry,
                    process_id,
                    module_id,
                    module,
                    name,
                    workload_key,
                    instance_id,
                    external_account_ref,
                    capabilities: &capabilities,
                    network_egress_profiles: &network_egress_profiles,
                    network_ingress_bindings: &network_ingress_bindings,
                    storage_logs: &storage_logs,
                    storage_blobs: &storage_blobs,
                    guest_log_bindings,
                    entrypoint,
                })
                .await
        }
    }

    async fn stop(&self, instance: &mut Self::Process) -> Result<(), Self::Error> {
        stop_process(instance).await?;
        Ok(())
    }
}

async fn stop_process(process: &mut WasmtimeProcess) -> Result<(), Error> {
    process.mailbox.close();

    if tokio::time::timeout(Duration::from_secs(1), &mut process.handle)
        .await
        .is_err()
    {
        process.handle.abort();
        let _ = (&mut process.handle).await;
    }

    if let Err(err) = process.usage.finish().await {
        warn!("runtime usage termination emission failed: {err:#}");
    }

    Ok(())
}

impl From<Error> for GuestError {
    fn from(value: Error) -> Self {
        Self::Subsystem(value.to_string())
    }
}

fn materialise_plan(
    memory: &Memory,
    store: &mut Store<InstanceRegistry>,
    plan: &CallPlan,
) -> Result<(), Error> {
    for write in plan.memory_writes() {
        if write.bytes.is_empty() {
            continue;
        }

        let start = usize::try_from(write.offset)
            .map_err(|err| Error::Kernel(KernelError::IntConvert(err)))?;
        let end = start
            .checked_add(write.bytes.len())
            .ok_or_else(|| Error::Kernel(KernelError::MemoryCapacity))?;
        let data = memory
            .data_mut(&mut *store)
            .get_mut(start..end)
            .ok_or(Error::Kernel(KernelError::MemoryCapacity))?;
        data.copy_from_slice(&write.bytes);
    }

    Ok(())
}

fn preallocate_memory(memory: &Memory, store: &mut Store<InstanceRegistry>) -> usize {
    let mut current = memory.size(&mut *store);
    if current < PREALLOC_PAGES {
        let delta = PREALLOC_PAGES - current;
        if let Err(err) = memory.grow(&mut *store, delta) {
            warn!("failed to preallocate guest memory to {PREALLOC_PAGES} pages: {err:?}");
        }
        current = memory.size(&mut *store);
    }
    let bytes = memory.data_size(&*store);
    debug!(pages = current, bytes, "prepared guest linear memory");
    bytes
}

fn prepare_params(param_types: &[ValType], scalars: &[AbiScalarValue]) -> Result<Vec<Val>, String> {
    if param_types.len() != scalars.len() {
        return Err(format!(
            "entrypoint expects {} params, got {}",
            param_types.len(),
            scalars.len()
        ));
    }

    scalars
        .iter()
        .zip(param_types.iter())
        .map(|(scalar, ty)| scalar_to_val(scalar, ty))
        .collect()
}

fn prepare_results(result_types: &[ValType]) -> Result<Vec<Val>, String> {
    Ok(result_types
        .iter()
        .map(|ty| default_val(ty.clone()))
        .collect())
}

fn stub_operations_for_missing(requested: &HashSet<Capability>) -> Vec<Arc<dyn LinkableOperation>> {
    let hostcalls_by_capability = hostcalls::by_capability();

    selium_abi::Capability::ALL
        .iter()
        .copied()
        .filter(|capability| !requested.contains(capability))
        .flat_map(|capability| {
            hostcalls_by_capability
                .get(&capability)
                .into_iter()
                .flatten()
                .map(move |meta| {
                    StubOperation::new(meta.name, capability) as Arc<dyn LinkableOperation>
                })
        })
        .collect()
}

struct StubOperation {
    module: &'static str,
    capability: Capability,
}

impl StubOperation {
    fn new(module: &'static str, capability: Capability) -> Arc<Self> {
        Arc::new(Self { module, capability })
    }

    fn create_stub_future(
        mut caller: Caller<'_, InstanceRegistry>,
        module: &'static str,
        capability: Capability,
    ) -> Result<GuestUint, KernelError> {
        debug!(%module, ?capability, "invoking stub capability binding");

        let state = FutureSharedState::new();
        state.resolve(Err(GuestError::PermissionDenied));
        let handle = caller.data_mut().insert_future(state)?;

        GuestUint::try_from(handle).map_err(KernelError::IntConvert)
    }

    fn poll_stub_future(
        mut caller: Caller<'_, InstanceRegistry>,
        state_id: GuestUint,
        _task_id: GuestUint,
        result_ptr: GuestInt,
        result_capacity: GuestUint,
        module: &'static str,
        capability: Capability,
    ) -> Result<GuestUint, KernelError> {
        debug!(%module, ?capability, "polling stub capability binding");

        let state_id = usize::try_from(state_id).map_err(KernelError::IntConvert)?;
        let Some(state) = caller.data().future_state(state_id) else {
            return Ok(write_poll_result(
                &mut caller,
                result_ptr,
                result_capacity,
                &Err(GuestError::NotFound),
            )?
            .encoded);
        };

        let write = state.with_result(|result| match result {
            Some(result) => write_poll_result(&mut caller, result_ptr, result_capacity, result),
            None => Ok(super::guest_data::WritePollResult {
                encoded: selium_abi::DRIVER_RESULT_PENDING,
                complete: false,
            }),
        })?;
        if write.complete {
            let _ = caller.data_mut().remove_future(state_id);
        }
        Ok(write.encoded)
    }

    fn drop_stub_future(
        mut caller: Caller<'_, InstanceRegistry>,
        state_id: GuestUint,
        result_ptr: GuestInt,
        result_capacity: GuestUint,
        module: &'static str,
        capability: Capability,
    ) -> Result<GuestUint, KernelError> {
        debug!(%module, ?capability, "dropping stub capability binding");

        let state_id = usize::try_from(state_id).map_err(KernelError::IntConvert)?;
        let result = if let Some(state) = caller.data_mut().remove_future(state_id) {
            state.abandon();
            Ok(Vec::new())
        } else {
            Err(GuestError::NotFound)
        };

        write_poll_result(&mut caller, result_ptr, result_capacity, &result)
            .map(|write| write.encoded)
    }
}

impl LinkableOperation for StubOperation {
    fn link(&self, linker: &mut Linker<InstanceRegistry>) -> Result<(), KernelError> {
        let module = self.module;
        let capability = self.capability;
        linker
            .func_wrap(
                module,
                "create",
                move |caller: Caller<'_, InstanceRegistry>,
                      _args_ptr: GuestInt,
                      _args_len: GuestUint| {
                    StubOperation::create_stub_future(caller, module, capability)
                        .map_err(Into::into)
                },
            )
            .map_err(|err| KernelError::Engine(err.to_string()))?;

        let module = self.module;
        let capability = self.capability;
        linker
            .func_wrap(
                module,
                "poll",
                move |caller: Caller<'_, InstanceRegistry>,
                      state_id: GuestUint,
                      task_id: GuestUint,
                      result_ptr: GuestInt,
                      result_capacity: GuestUint| {
                    StubOperation::poll_stub_future(
                        caller,
                        state_id,
                        task_id,
                        result_ptr,
                        result_capacity,
                        module,
                        capability,
                    )
                    .map_err(Into::into)
                },
            )
            .map_err(|err| KernelError::Engine(err.to_string()))?;

        let module = self.module;
        let capability = self.capability;
        linker
            .func_wrap(
                module,
                "drop",
                move |caller: Caller<'_, InstanceRegistry>,
                      state_id: GuestUint,
                      result_ptr: GuestInt,
                      result_capacity: GuestUint| {
                    StubOperation::drop_stub_future(
                        caller,
                        state_id,
                        result_ptr,
                        result_capacity,
                        module,
                        capability,
                    )
                    .map_err(Into::into)
                },
            )
            .map_err(|err| KernelError::Engine(err.to_string()))?;

        Ok(())
    }
}

async fn invoke_entrypoint(
    func: Func,
    mut store: Store<InstanceRegistry>,
    memory: Memory,
    params: Vec<Val>,
    mut results: Vec<Val>,
    signature: AbiSignature,
) -> Result<Vec<AbiValue>, wasmtime::Error> {
    func.call_async(&mut store, &params, &mut results).await?;
    decode_results(&memory, &store, &results, &signature)
}

fn decode_results(
    memory: &Memory,
    store: &Store<InstanceRegistry>,
    raw: &[Val],
    signature: &AbiSignature,
) -> Result<Vec<AbiValue>, wasmtime::Error> {
    let mut iter = raw.iter();
    let mut values = Vec::new();

    for param in signature.results() {
        match param {
            AbiParam::Scalar(kind) => {
                let scalar = decode_scalar(&mut iter, *kind)?;
                values.push(AbiValue::Scalar(scalar));
            }
            AbiParam::Buffer => {
                let ptr_val = iter
                    .next()
                    .ok_or_else(|| wasmtime::Error::msg("missing buffer pointer"))?;
                let len_val = iter
                    .next()
                    .ok_or_else(|| wasmtime::Error::msg("missing buffer length"))?;
                let ptr = match ptr_val {
                    Val::I32(v) if *v >= 0 => *v as usize,
                    _ => return Err(wasmtime::Error::msg("buffer pointer must be i32")),
                };
                let len = match len_val {
                    Val::I32(v) if *v >= 0 => *v as usize,
                    _ => return Err(wasmtime::Error::msg("buffer length must be i32")),
                };

                if len == 0 {
                    values.push(AbiValue::Buffer(Vec::new()));
                    continue;
                }

                let data = memory
                    .data(store)
                    .get(ptr..ptr + len)
                    .ok_or_else(|| wasmtime::Error::msg("buffer result out of bounds"))?;
                values.push(AbiValue::Buffer(data.to_vec()));
            }
        }
    }

    if iter.next().is_some() {
        return Err(wasmtime::Error::msg("extra values returned by entrypoint"));
    }

    Ok(values)
}

fn scalar_to_val(value: &AbiScalarValue, ty: &ValType) -> Result<Val, String> {
    match (value, ty) {
        (AbiScalarValue::I32(v), ValType::I32) => Ok(Val::I32(*v)),
        (AbiScalarValue::U32(v), ValType::I32) => {
            let bits = i32::from_ne_bytes(v.to_ne_bytes());
            Ok(Val::I32(bits))
        }
        (AbiScalarValue::I16(v), ValType::I32) => Ok(Val::I32(i32::from(*v))),
        (AbiScalarValue::U16(v), ValType::I32) => Ok(Val::I32(i32::from(*v))),
        (AbiScalarValue::I8(v), ValType::I32) => Ok(Val::I32(i32::from(*v))),
        (AbiScalarValue::U8(v), ValType::I32) => Ok(Val::I32(i32::from(*v))),
        (AbiScalarValue::I64(v), ValType::I64) => Ok(Val::I64(*v)),
        (AbiScalarValue::F32(v), ValType::F32) => Ok(Val::F32(v.to_bits())),
        (AbiScalarValue::F64(v), ValType::F64) => Ok(Val::F64(v.to_bits())),
        _ => Err(format!(
            "type mismatch: value {:?} cannot be passed as {:?}",
            value, ty
        )),
    }
}

fn decode_scalar(
    iter: &mut std::slice::Iter<Val>,
    expected: AbiScalarType,
) -> Result<AbiScalarValue, wasmtime::Error> {
    match expected {
        AbiScalarType::I8 => {
            let raw = take_i32(iter, "missing i8 result")?;
            i8::try_from(raw)
                .map(AbiScalarValue::I8)
                .map_err(|_| wasmtime::Error::msg("i8 result out of range"))
        }
        AbiScalarType::U8 => {
            let raw = take_u32(iter, "missing u8 result")?;
            u8::try_from(raw)
                .map(AbiScalarValue::U8)
                .map_err(|_| wasmtime::Error::msg("u8 result out of range"))
        }
        AbiScalarType::I16 => {
            let raw = take_i32(iter, "missing i16 result")?;
            i16::try_from(raw)
                .map(AbiScalarValue::I16)
                .map_err(|_| wasmtime::Error::msg("i16 result out of range"))
        }
        AbiScalarType::U16 => {
            let raw = take_u32(iter, "missing u16 result")?;
            u16::try_from(raw)
                .map(AbiScalarValue::U16)
                .map_err(|_| wasmtime::Error::msg("u16 result out of range"))
        }
        AbiScalarType::I32 => {
            let raw = take_i32(iter, "missing i32 result")?;
            Ok(AbiScalarValue::I32(raw))
        }
        AbiScalarType::U32 => {
            let raw = take_u32(iter, "missing u32 result")?;
            Ok(AbiScalarValue::U32(raw))
        }
        AbiScalarType::I64 => {
            let lo = take_u32(iter, "missing low i64 result")?;
            let hi = take_u32(iter, "missing high i64 result")?;
            let combined = (u64::from(hi) << 32) | u64::from(lo);
            Ok(AbiScalarValue::I64(i64::from_le_bytes(
                combined.to_le_bytes(),
            )))
        }
        AbiScalarType::U64 => {
            let lo = take_u32(iter, "missing low u64 result")?;
            let hi = take_u32(iter, "missing high u64 result")?;
            let combined = (u64::from(hi) << 32) | u64::from(lo);
            Ok(AbiScalarValue::U64(combined))
        }
        AbiScalarType::F32 => {
            let val = iter
                .next()
                .ok_or_else(|| wasmtime::Error::msg("missing f32 result"))?;
            match val {
                Val::F32(bits) => Ok(AbiScalarValue::F32(f32::from_bits(*bits))),
                _ => Err(wasmtime::Error::msg("f32 result must be f32")),
            }
        }
        AbiScalarType::F64 => {
            let val = iter
                .next()
                .ok_or_else(|| wasmtime::Error::msg("missing f64 result"))?;
            match val {
                Val::F64(bits) => Ok(AbiScalarValue::F64(f64::from_bits(*bits))),
                _ => Err(wasmtime::Error::msg("f64 result must be f64")),
            }
        }
    }
}

fn default_val(ty: ValType) -> Val {
    match ty {
        ValType::I32 => Val::I32(0),
        ValType::I64 => Val::I64(0),
        ValType::F32 => Val::F32(0u32),
        ValType::F64 => Val::F64(0u64),
        other => panic!("unsupported Wasm value type in entrypoint: {other:?}"),
    }
}

fn flatten_signature_types(spec: &[AbiParam]) -> Vec<ValType> {
    let mut types = Vec::new();
    for param in spec {
        match param {
            AbiParam::Scalar(kind) => push_scalar_types(*kind, &mut types),
            AbiParam::Buffer => {
                types.push(ValType::I32);
                types.push(ValType::I32);
            }
        }
    }
    types
}

fn push_scalar_types(kind: AbiScalarType, types: &mut Vec<ValType>) {
    match kind {
        AbiScalarType::F32 => types.push(ValType::F32),
        AbiScalarType::F64 => types.push(ValType::F64),
        AbiScalarType::I64 | AbiScalarType::U64 => {
            types.push(ValType::I32);
            types.push(ValType::I32);
        }
        AbiScalarType::I8
        | AbiScalarType::U8
        | AbiScalarType::I16
        | AbiScalarType::U16
        | AbiScalarType::I32
        | AbiScalarType::U32 => types.push(ValType::I32),
    }
}

fn valtype_eq(a: &ValType, b: &ValType) -> bool {
    matches!(
        (a, b),
        (ValType::I32, ValType::I32)
            | (ValType::I64, ValType::I64)
            | (ValType::F32, ValType::F32)
            | (ValType::F64, ValType::F64)
    )
}

fn take_i32(iter: &mut std::slice::Iter<Val>, msg: &str) -> Result<i32, wasmtime::Error> {
    let Some(val) = iter.next() else {
        return Err(wasmtime::Error::msg(msg.to_owned()));
    };

    match val {
        Val::I32(v) => Ok(*v),
        _ => Err(wasmtime::Error::msg(msg.to_owned())),
    }
}

fn take_u32(iter: &mut std::slice::Iter<Val>, msg: &str) -> Result<u32, wasmtime::Error> {
    let raw = take_i32(iter, msg)?;
    Ok(u32::from_ne_bytes(raw.to_ne_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use selium_abi::{
        ProcessLogBindings, QueueAck, QueueAttach, QueueCreate, QueueDelivery, QueueOverflow,
        QueueRole, QueueStatusCode, ShmRegion,
    };
    use selium_kernel::spi::wake_mailbox::WakeMailbox;
    use selium_kernel::{
        registry::{ResourceHandle, ResourceType},
        services::{
            queue_service::{QueueService, QueueState},
            shared_memory_service::SharedMemoryDriver,
        },
        spi::{queue::QueueCapability, shared_memory::SharedMemoryCapability},
    };
    use std::{
        sync::atomic::{AtomicBool, Ordering},
        task::Waker,
    };
    use tokio::sync::Notify;

    struct TestMailbox {
        closed: AtomicBool,
        notify: tokio::sync::Notify,
    }

    impl TestMailbox {
        fn new() -> Self {
            Self {
                closed: AtomicBool::new(false),
                notify: tokio::sync::Notify::new(),
            }
        }
    }

    impl selium_kernel::spi::wake_mailbox::WakeMailbox for TestMailbox {
        fn refresh_base(&self, _base: usize) {}

        fn close(&self) {
            self.closed.store(true, Ordering::Release);
            self.notify.notify_waiters();
        }

        fn waker(&'static self, _task_id: usize) -> Waker {
            Waker::noop().clone()
        }

        fn is_closed(&self) -> bool {
            self.closed.load(Ordering::Acquire)
        }

        fn is_signalled(&self) -> bool {
            false
        }

        fn wait_for_signal<'a>(
            &'a self,
        ) -> std::pin::Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
            Box::pin(async move {
                self.notify.notified().await;
            })
        }
    }

    #[test]
    fn prepare_params_detects_count_mismatches() {
        let err = prepare_params(&[ValType::I32], &[]).expect_err("count mismatch");
        assert!(err.contains("expects 1 params"));
    }

    #[test]
    fn scalar_to_val_converts_integer_and_float_types() {
        let i32_val = scalar_to_val(&AbiScalarValue::I32(7), &ValType::I32).expect("i32");
        assert!(matches!(i32_val, Val::I32(7)));
        let f64_val = scalar_to_val(&AbiScalarValue::F64(1.5), &ValType::F64).expect("f64");
        assert!(matches!(f64_val, Val::F64(bits) if bits == 1.5f64.to_bits()));
    }

    #[test]
    fn decode_scalar_handles_u64_split_values() {
        let values = [Val::I32(0x89abcdefu32 as i32), Val::I32(0x01234567)];
        let mut iter = values.iter();
        let decoded = decode_scalar(&mut iter, AbiScalarType::U64).expect("decode u64");
        assert_eq!(decoded, AbiScalarValue::U64(0x0123456789abcdef));
    }

    #[test]
    fn flatten_signature_types_expands_buffers_and_wide_scalars() {
        let types = flatten_signature_types(&[
            AbiParam::Scalar(AbiScalarType::I32),
            AbiParam::Buffer,
            AbiParam::Scalar(AbiScalarType::U64),
        ]);
        let expected = [
            ValType::I32,
            ValType::I32,
            ValType::I32,
            ValType::I32,
            ValType::I32,
        ];
        assert_eq!(types.len(), expected.len());
        for (actual, expected) in types.iter().zip(expected.iter()) {
            assert!(valtype_eq(actual, expected));
        }
    }

    #[test]
    fn valtype_eq_matches_only_same_core_types() {
        assert!(valtype_eq(&ValType::I32, &ValType::I32));
        assert!(!valtype_eq(&ValType::I32, &ValType::I64));
    }

    #[test]
    fn missing_capabilities_generate_stub_operations() {
        let requested = HashSet::from([Capability::TimeRead]);
        let stubs = stub_operations_for_missing(&requested);
        assert!(!stubs.is_empty());
    }

    #[tokio::test]
    async fn stop_process_closes_mailbox_and_waits_for_guest_exit() {
        let usage = crate::usage::RuntimeUsageCollector::in_memory(Duration::from_secs(60));
        let usage = usage
            .register_process(
                "test-workload",
                "test-process",
                ProcessUsageAttribution {
                    instance_id: Some("test-instance".to_string()),
                    external_account_ref: None,
                    module_id: "test.module".to_string(),
                },
            )
            .await
            .expect("register usage handle");
        let mailbox: &'static TestMailbox = Box::leak(Box::new(TestMailbox::new()));
        let wait_mailbox = mailbox;
        let handle = tokio::spawn(async move {
            wait_mailbox.wait_for_signal().await;
            assert!(wait_mailbox.is_closed());
            Ok(Vec::new())
        });
        let mut process = WasmtimeProcess::new(handle, mailbox, usage);

        stop_process(&mut process).await.expect("stop process");

        assert!(mailbox.is_closed());
        assert!(process.handle.is_finished());
    }

    #[tokio::test]
    async fn guest_logs_are_forwarded_into_stdout_and_stderr_event_queues() {
        let shutdown = Arc::new(Notify::new());
        let usage = RuntimeUsageCollector::in_memory(Duration::from_millis(10));
        let shared_memory = Arc::new(SharedMemoryDriver::new());
        let runtime = WasmtimeRuntime::new(
            HashMap::new(),
            Arc::new(GuestAsync::new(Arc::clone(&shutdown))),
            Arc::clone(&usage),
            Arc::clone(&shared_memory),
        )
        .expect("runtime");
        let registry = Arc::new(Registry::new());
        let (stdout_queue, stdout_shared_id) = make_test_queue(&registry);
        let (stderr_queue, stderr_shared_id) = make_test_queue(&registry);
        let process_id = registry
            .reserve(None, ResourceType::Process)
            .expect("process");
        let module = Module::new(&runtime.engine, guest_log_test_module_bytes()).expect("module");

        runtime
            .run(WasmtimeRunRequest {
                registry: &registry,
                process_id,
                module_id: "guest-log-test",
                module,
                name: "start",
                workload_key: None,
                instance_id: None,
                external_account_ref: None,
                capabilities: &[],
                network_egress_profiles: &[],
                network_ingress_bindings: &[],
                storage_logs: &[],
                storage_blobs: &[],
                guest_log_bindings: ProcessLogBindings {
                    stdout_queue_shared_id: Some(stdout_shared_id),
                    stderr_queue_shared_id: Some(stderr_shared_id),
                },
                entrypoint: EntrypointInvocation::new(
                    AbiSignature::new(Vec::new(), Vec::new()),
                    Vec::new(),
                )
                .expect("entrypoint"),
            })
            .await
            .expect("run");

        let process = registry
            .remove(ResourceHandle::<WasmtimeProcess>::new(process_id))
            .expect("process handle");
        process.handle.await.expect("join").expect("guest result");

        let stdout_frames =
            drain_queue_payloads(&registry, &shared_memory, &stdout_queue, stdout_shared_id).await;
        let stderr_frames =
            drain_queue_payloads(&registry, &shared_memory, &stderr_queue, stderr_shared_id).await;

        assert_eq!(stdout_frames, vec![b"guest stdout".to_vec()]);
        assert_eq!(stderr_frames, vec![b"guest stderr".to_vec()]);
    }

    fn make_test_queue(registry: &Arc<Registry>) -> (QueueState, u64) {
        let queue = QueueService
            .create(QueueCreate {
                capacity_frames: 8,
                max_frame_bytes: 64 * 1024,
                delivery: QueueDelivery::Lossless,
                overflow: QueueOverflow::Block,
            })
            .expect("queue");
        let handle = registry
            .add(queue.clone(), None, ResourceType::Queue)
            .expect("queue handle");
        let shared_id = registry
            .share_handle(handle.into_id())
            .expect("share queue");
        (queue, shared_id)
    }

    fn guest_log_test_module_bytes() -> Vec<u8> {
        vec![
            0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, 0x01, 0x0a, 0x02, 0x60, 0x03, 0x7f,
            0x7f, 0x7f, 0x00, 0x60, 0x00, 0x00, 0x02, 0x15, 0x01, 0x0b, 0x73, 0x65, 0x6c, 0x69,
            0x75, 0x6d, 0x3a, 0x3a, 0x6c, 0x6f, 0x67, 0x05, 0x77, 0x72, 0x69, 0x74, 0x65, 0x00,
            0x00, 0x03, 0x02, 0x01, 0x01, 0x05, 0x03, 0x01, 0x00, 0x01, 0x07, 0x12, 0x02, 0x06,
            0x6d, 0x65, 0x6d, 0x6f, 0x72, 0x79, 0x02, 0x00, 0x05, 0x73, 0x74, 0x61, 0x72, 0x74,
            0x00, 0x01, 0x0a, 0x14, 0x01, 0x12, 0x00, 0x41, 0x01, 0x41, 0x00, 0x41, 0x0c, 0x10,
            0x00, 0x41, 0x02, 0x41, 0x20, 0x41, 0x0c, 0x10, 0x00, 0x0b, 0x0b, 0x23, 0x02, 0x00,
            0x41, 0x00, 0x0b, 0x0c, 0x67, 0x75, 0x65, 0x73, 0x74, 0x20, 0x73, 0x74, 0x64, 0x6f,
            0x75, 0x74, 0x00, 0x41, 0x20, 0x0b, 0x0c, 0x67, 0x75, 0x65, 0x73, 0x74, 0x20, 0x73,
            0x74, 0x64, 0x65, 0x72, 0x72,
        ]
    }

    async fn drain_queue_payloads(
        registry: &Arc<Registry>,
        shared_memory: &SharedMemoryDriver,
        queue: &QueueState,
        shared_id: u64,
    ) -> Vec<Vec<u8>> {
        let reader = QueueService
            .attach(
                queue,
                QueueAttach {
                    shared_id,
                    role: QueueRole::Reader,
                },
            )
            .expect("attach reader");
        let mut frames = Vec::new();

        loop {
            let waited = QueueService.wait(&reader, 0).await.expect("wait");
            if waited.code != QueueStatusCode::Ok {
                break;
            }
            let frame = waited.frame.expect("frame");
            frames.push(read_queue_payload(
                registry,
                shared_memory,
                frame.shm_shared_id,
                frame.offset,
                frame.len,
            ));
            QueueService
                .ack(
                    &reader,
                    QueueAck {
                        endpoint_id: 0,
                        seq: frame.seq,
                    },
                )
                .expect("ack");
        }

        frames
    }

    fn read_queue_payload(
        registry: &Arc<Registry>,
        shared_memory: &SharedMemoryDriver,
        shm_shared_id: u64,
        offset: u32,
        len: u32,
    ) -> Vec<u8> {
        let shm_id = registry.resolve_shared(shm_shared_id).expect("shared shm");
        let region = registry
            .with(
                ResourceHandle::<ShmRegion>::new(shm_id),
                |region: &mut ShmRegion| *region,
            )
            .expect("region");
        let bytes = shared_memory
            .read(region, offset, len)
            .expect("read region");
        bytes
    }
}
