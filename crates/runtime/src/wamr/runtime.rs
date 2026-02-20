//! WAMR process lifecycle integration for the Selium host runtime.

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    ffi::{CStr, CString, c_char, c_void},
    fmt::{Display, Formatter},
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use selium_abi::{
    AbiParam, AbiScalarType, AbiScalarValue, AbiSignature, AbiValue, CallPlan, CallPlanError,
    Capability, DRIVER_ERROR_MESSAGE_CODE, DRIVER_RESULT_PENDING, EntrypointInvocation, GuestUint,
    driver_encode_error, driver_encode_ready, encode_driver_error_message,
};
use selium_kernel::{
    KernelError,
    r#async::futures::FutureSharedState,
    guest_error::{GuestError, GuestResult},
    hostcalls::{HostcallContext, process::EntrypointInvocationExt},
    registry::{InstanceRegistry, Registry, ResourceId},
    spi::{
        module_repository::{ModuleRepositoryError, ModuleRepositoryReadCapability},
        process::ProcessLifecycleCapability,
    },
};
use tokio::task::JoinHandle;
use wamr_rust_sdk::sys::{
    NativeSymbol, RuntimeInitArgs, mem_alloc_type_t_Alloc_With_System_Allocator, wasm_exec_env_t,
    wasm_module_inst_t, wasm_runtime_addr_app_to_native, wasm_runtime_call_wasm,
    wasm_runtime_create_exec_env, wasm_runtime_deinstantiate, wasm_runtime_destroy,
    wasm_runtime_destroy_exec_env, wasm_runtime_destroy_thread_env, wasm_runtime_full_init,
    wasm_runtime_get_exception, wasm_runtime_get_exec_env_singleton,
    wasm_runtime_get_function_attachment, wasm_runtime_get_module_inst, wasm_runtime_get_user_data,
    wasm_runtime_init_thread_env, wasm_runtime_instantiate, wasm_runtime_load,
    wasm_runtime_lookup_function, wasm_runtime_register_natives, wasm_runtime_set_exception,
    wasm_runtime_set_user_data, wasm_runtime_unload, wasm_runtime_validate_app_addr,
};

use crate::{
    wamr::hostcall_linker::WamrHostcallOperation, wasmtime::mailbox::create_guest_mailbox_from_base,
};

const STACK_SIZE: u32 = 64 * 1024;
const APP_HEAP_SIZE: u32 = 0;
const ERROR_BUFFER_SIZE: usize = 256;
const YIELD_PARK_TIMEOUT: Duration = Duration::from_millis(1);

static WAMR_INITIALISED: AtomicBool = AtomicBool::new(false);

struct ThreadEnvGuard;

impl ThreadEnvGuard {
    fn create() -> Result<Self, Error> {
        let initialised = unsafe { wasm_runtime_init_thread_env() };
        if !initialised {
            return Err(Error::Wamr(
                "initialise thread environment failed".to_string(),
            ));
        }
        Ok(Self)
    }
}

impl Drop for ThreadEnvGuard {
    fn drop(&mut self) {
        unsafe {
            wasm_runtime_destroy_thread_env();
        }
    }
}

struct OwnedExecEnv {
    exec_env: wasm_exec_env_t,
    destroy_on_drop: bool,
}

struct NativeModuleRegistration {
    module_name: CString,
    symbol_names: Vec<CString>,
    symbol_signatures: Vec<CString>,
    symbols: Box<[NativeSymbol]>,
    attachments: Vec<NativeHostcallAttachment>,
}

struct NativeHostcallAttachment {
    table: Arc<WamrHostcallTable>,
    module: &'static str,
}

struct HostcallInvocationContext {
    registry: *mut InstanceRegistry,
    capabilities: *const HashSet<Capability>,
}

struct WamrHostcallContext<'a> {
    registry: &'a mut InstanceRegistry,
    mailbox_base: Option<usize>,
}

struct WamrHostcallTable {
    available_capabilities: RwLock<HashSet<Capability>>,
    module_capabilities: HashMap<&'static str, Capability>,
    operations: RwLock<HashMap<&'static str, Arc<dyn WamrHostcallOperation>>>,
    stubs: HashMap<&'static str, Arc<dyn WamrHostcallOperation>>,
}

struct StubOperation {
    module: &'static str,
}

impl OwnedExecEnv {
    fn get(module_inst: wasm_module_inst_t) -> Result<Self, Error> {
        let singleton = unsafe { wasm_runtime_get_exec_env_singleton(module_inst) };
        if !singleton.is_null() {
            return Ok(Self {
                exec_env: singleton,
                destroy_on_drop: false,
            });
        }

        let owned = unsafe { wasm_runtime_create_exec_env(module_inst, STACK_SIZE) };
        if owned.is_null() {
            return Err(Error::Wamr(
                "create execution environment failed".to_string(),
            ));
        }

        Ok(Self {
            exec_env: owned,
            destroy_on_drop: true,
        })
    }
}

impl Drop for OwnedExecEnv {
    fn drop(&mut self) {
        if self.destroy_on_drop {
            unsafe {
                wasm_runtime_destroy_exec_env(self.exec_env);
            }
        }
    }
}

impl NativeModuleRegistration {
    fn leak(self) {
        // WAMR stores symbol pointers internally. Keep this registration alive for the
        // lifetime of the process by intentionally leaking the backing allocations.
        std::mem::forget(self.module_name);
        std::mem::forget(self.symbol_names);
        std::mem::forget(self.symbol_signatures);
        std::mem::forget(self.symbols);
        std::mem::forget(self.attachments);
    }
}

impl HostcallInvocationContext {
    fn new(registry: &mut InstanceRegistry, capabilities: &HashSet<Capability>) -> Self {
        Self {
            registry: registry as *mut InstanceRegistry,
            capabilities: capabilities as *const HashSet<Capability>,
        }
    }

    fn registry(&mut self) -> Result<&mut InstanceRegistry, Error> {
        if self.registry.is_null() {
            return Err(Error::Wamr(
                "hostcall registry context is missing".to_string(),
            ));
        }

        Ok(unsafe { &mut *self.registry })
    }

    fn capabilities(&self) -> Result<&HashSet<Capability>, Error> {
        if self.capabilities.is_null() {
            return Err(Error::Wamr(
                "hostcall capability context is missing".to_string(),
            ));
        }

        Ok(unsafe { &*self.capabilities })
    }
}

impl HostcallContext for WamrHostcallContext<'_> {
    fn registry(&self) -> &InstanceRegistry {
        self.registry
    }

    fn registry_mut(&mut self) -> &mut InstanceRegistry {
        self.registry
    }

    fn mailbox_base(&mut self) -> Option<usize> {
        self.mailbox_base
    }
}

impl WamrHostcallTable {
    fn new(
        available_capabilities: HashMap<Capability, Vec<Arc<dyn WamrHostcallOperation>>>,
    ) -> Self {
        let mut operations: HashMap<&'static str, Arc<dyn WamrHostcallOperation>> = HashMap::new();
        let mut available = HashSet::new();
        for (capability, entries) in available_capabilities {
            if entries.is_empty() {
                continue;
            }
            available.insert(capability);
            for entry in entries {
                operations.insert(entry.module(), entry);
            }
        }

        let mut module_capabilities = HashMap::new();
        let mut stubs: HashMap<&'static str, Arc<dyn WamrHostcallOperation>> = HashMap::new();
        let by_capability = selium_abi::hostcalls::by_capability();
        for (capability, hostcalls) in by_capability {
            for hostcall in hostcalls {
                module_capabilities.insert(hostcall.name, capability);
                stubs.insert(
                    hostcall.name,
                    StubOperation::build(hostcall.name, capability)
                        as Arc<dyn WamrHostcallOperation>,
                );
            }
        }

        Self {
            available_capabilities: RwLock::new(available),
            module_capabilities,
            operations: RwLock::new(operations),
            stubs,
        }
    }

    fn ensure_capabilities_available(&self, requested: &HashSet<Capability>) -> Result<(), Error> {
        let available = self
            .available_capabilities
            .read()
            .map_err(|_| Error::Wamr("capability registry lock is poisoned".to_string()))?;
        for capability in requested {
            if !available.contains(capability) {
                return Err(Error::CapabilityUnavailable(*capability));
            }
        }
        Ok(())
    }

    fn module_capability(&self, module: &'static str) -> Result<Capability, Error> {
        self.module_capabilities
            .get(module)
            .copied()
            .ok_or_else(|| Error::Wamr(format!("unknown hostcall module `{module}`")))
    }

    fn resolve(
        &self,
        module: &'static str,
        requested_capabilities: &HashSet<Capability>,
    ) -> Result<Arc<dyn WamrHostcallOperation>, Error> {
        let capability = self.module_capability(module)?;
        if requested_capabilities.contains(&capability) {
            let operations = self
                .operations
                .read()
                .map_err(|_| Error::Wamr("hostcall registry lock is poisoned".to_string()))?;
            return operations
                .get(module)
                .cloned()
                .ok_or(Error::CapabilityUnavailable(capability));
        }

        self.stubs
            .get(module)
            .cloned()
            .ok_or_else(|| Error::Wamr(format!("missing stub operation for `{module}`")))
    }

    fn extend_capability(
        &self,
        capability: Capability,
        operations: impl IntoIterator<Item = Arc<dyn WamrHostcallOperation>>,
    ) -> Result<(), Error> {
        let mut entries = self
            .operations
            .write()
            .map_err(|_| Error::Wamr("hostcall registry lock is poisoned".to_string()))?;
        for operation in operations {
            entries.insert(operation.module(), operation);
        }

        self.available_capabilities
            .write()
            .map_err(|_| Error::Wamr("capability registry lock is poisoned".to_string()))?
            .insert(capability);
        Ok(())
    }
}

impl StubOperation {
    fn build(module: &'static str, _capability: Capability) -> Arc<Self> {
        Arc::new(Self { module })
    }

    fn create_stub_future(context: &mut dyn HostcallContext) -> Result<GuestUint, KernelError> {
        let state = FutureSharedState::new();
        state.resolve(Err(GuestError::PermissionDenied));
        let handle = context.registry_mut().insert_future(state)?;
        GuestUint::try_from(handle).map_err(KernelError::IntConvert)
    }
}

impl WamrHostcallOperation for StubOperation {
    fn module(&self) -> &'static str {
        self.module
    }

    fn create(
        &self,
        context: &mut dyn HostcallContext,
        _args: &[u8],
    ) -> Result<GuestUint, KernelError> {
        Self::create_stub_future(context)
    }

    fn poll(
        &self,
        context: &mut dyn HostcallContext,
        state_id: GuestUint,
        _task_id: GuestUint,
    ) -> Result<GuestResult<Vec<u8>>, KernelError> {
        let state_id = usize::try_from(state_id).map_err(KernelError::IntConvert)?;
        let result = match context.registry_mut().remove_future(state_id) {
            Some(state) => state
                .take_result()
                .unwrap_or(Err(GuestError::PermissionDenied)),
            None => Err(GuestError::NotFound),
        };
        Ok(result)
    }

    fn drop_state(
        &self,
        context: &mut dyn HostcallContext,
        state_id: GuestUint,
    ) -> Result<GuestResult<Vec<u8>>, KernelError> {
        let state_id = usize::try_from(state_id).map_err(KernelError::IntConvert)?;
        let result = if let Some(state) = context.registry_mut().remove_future(state_id) {
            state.abandon();
            Ok(Vec::new())
        } else {
            Err(GuestError::NotFound)
        };
        Ok(result)
    }
}

/// Runtime error surfaced by the WAMR process adapter.
#[derive(Debug)]
pub enum Error {
    CapabilityUnavailable(Capability),
    Kernel(KernelError),
    ModuleRepository(ModuleRepositoryError),
    Wamr(String),
    RuntimeAlreadyInitialised,
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
            Self::Wamr(err) => write!(f, "wamr error: {err}"),
            Self::RuntimeAlreadyInitialised => {
                write!(f, "wamr runtime is already initialised")
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

impl From<CallPlanError> for Error {
    fn from(value: CallPlanError) -> Self {
        Self::Wamr(value.to_string())
    }
}

impl From<Error> for GuestError {
    fn from(value: Error) -> Self {
        Self::Subsystem(value.to_string())
    }
}

/// WAMR execution engine used by the runtime process lifecycle adapter.
pub struct WamrRuntime {
    hostcalls: Arc<WamrHostcallTable>,
}

impl WamrRuntime {
    /// Create a new WAMR runtime and register Selium hostcall bindings.
    pub fn new(
        available_capabilities: HashMap<Capability, Vec<Arc<dyn WamrHostcallOperation>>>,
    ) -> Result<Self, Error> {
        if WAMR_INITIALISED
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(Error::RuntimeAlreadyInitialised);
        }

        let mut init_args = RuntimeInitArgs {
            mem_alloc_type: mem_alloc_type_t_Alloc_With_System_Allocator,
            ..RuntimeInitArgs::default()
        };
        let ok = unsafe { wasm_runtime_full_init(&mut init_args) };
        if !ok {
            WAMR_INITIALISED.store(false, Ordering::Release);
            return Err(Error::Wamr("initialise runtime failed".to_string()));
        }

        let hostcalls = Arc::new(WamrHostcallTable::new(available_capabilities));
        register_hostcall_modules(Arc::clone(&hostcalls))?;

        Ok(Self { hostcalls })
    }

    /// Register additional hostcall operations for an already initialised capability.
    pub fn extend_capability(
        &self,
        capability: Capability,
        operations: impl IntoIterator<Item = Arc<dyn WamrHostcallOperation>>,
    ) -> Result<(), Error> {
        self.hostcalls.extend_capability(capability, operations)
    }

    fn execute(
        &self,
        registry: &Arc<Registry>,
        process_id: ResourceId,
        module_bytes: Vec<u8>,
        entrypoint_name: &str,
        capabilities: Vec<Capability>,
        entrypoint: EntrypointInvocation,
    ) -> Result<Vec<AbiValue>, Error> {
        let _thread_env = ThreadEnvGuard::create()?;
        let mut error_buf = [0 as c_char; ERROR_BUFFER_SIZE];
        let requested_capabilities: HashSet<Capability> = capabilities.into_iter().collect();
        self.hostcalls
            .ensure_capabilities_available(&requested_capabilities)?;

        let module_size = u32::try_from(module_bytes.len())
            .map_err(|_| Error::Wamr("module size exceeds u32".to_string()))?;
        let module = unsafe {
            wasm_runtime_load(
                module_bytes.as_ptr().cast_mut(),
                module_size,
                error_buf.as_mut_ptr(),
                u32::try_from(error_buf.len())
                    .map_err(|_| Error::Wamr("error buffer size overflow".to_string()))?,
            )
        };
        if module.is_null() {
            return Err(Error::Wamr(error_buffer_to_string(&error_buf)));
        }

        let mut module_inst: wasm_module_inst_t = std::ptr::null_mut();
        let result = (|| -> Result<Vec<AbiValue>, Error> {
            module_inst = unsafe {
                wasm_runtime_instantiate(
                    module,
                    STACK_SIZE,
                    APP_HEAP_SIZE,
                    error_buf.as_mut_ptr(),
                    u32::try_from(error_buf.len())
                        .map_err(|_| Error::Wamr("error buffer size overflow".to_string()))?,
                )
            };
            if module_inst.is_null() {
                return Err(Error::Wamr(error_buffer_to_string(&error_buf)));
            }

            let function_name = CString::new(entrypoint_name)
                .map_err(|_| Error::Wamr("entrypoint name contains NUL byte".to_string()))?;
            let function =
                unsafe { wasm_runtime_lookup_function(module_inst, function_name.as_ptr()) };
            if function.is_null() {
                return Err(Error::Wamr(format!(
                    "entrypoint `{entrypoint_name}` not found"
                )));
            }

            let mut instance_registry = registry.instance().map_err(KernelError::from)?;
            instance_registry
                .set_process_id(process_id)
                .map_err(KernelError::from)?;
            let memory_base = guest_memory_base(module_inst).ok_or_else(|| {
                Error::Kernel(KernelError::Driver("guest memory missing".to_string()))
            })?;
            let mailbox = unsafe { create_guest_mailbox_from_base(memory_base) };
            instance_registry
                .load_mailbox(mailbox)
                .map_err(KernelError::from)?;

            let signature = entrypoint.signature().clone();
            let call_values = entrypoint.materialise_values(&mut instance_registry)?;
            let plan = CallPlan::new(&signature, &call_values)?;
            materialise_plan(module_inst, &plan)?;

            let words = encode_param_words(plan.params());
            let mut argv = vec![0u32; required_argv_capacity(&signature, words.len())];
            argv[..words.len()].copy_from_slice(&words);

            let expected_params = u32::try_from(plan.params().len())
                .map_err(|_| Error::Wamr("parameter count exceeds u32".to_string()))?;
            let wasm_params =
                unsafe { wamr_rust_sdk::sys::wasm_func_get_param_count(function, module_inst) };
            if wasm_params != expected_params {
                return Err(Error::Wamr(format!(
                    "entrypoint expects {wasm_params} params, prepared {expected_params}"
                )));
            }

            let exec_env = OwnedExecEnv::get(module_inst)?;
            let mut invocation =
                HostcallInvocationContext::new(&mut instance_registry, &requested_capabilities);
            unsafe {
                wasm_runtime_set_user_data(
                    exec_env.exec_env,
                    (&mut invocation as *mut HostcallInvocationContext).cast::<c_void>(),
                );
            }
            let call_ok = unsafe {
                wasm_runtime_call_wasm(exec_env.exec_env, function, wasm_params, argv.as_mut_ptr())
            };
            unsafe {
                wasm_runtime_set_user_data(exec_env.exec_env, std::ptr::null_mut());
            }
            if !call_ok {
                return Err(Error::Wamr(last_exception(module_inst)));
            }

            decode_results(module_inst, &argv, &signature)
        })();

        if !module_inst.is_null() {
            unsafe {
                wasm_runtime_deinstantiate(module_inst);
            }
        }
        unsafe {
            wasm_runtime_unload(module);
        }

        result
    }
}

impl Drop for WamrRuntime {
    fn drop(&mut self) {
        if WAMR_INITIALISED.swap(false, Ordering::AcqRel) {
            unsafe {
                wasm_runtime_destroy();
            }
        }
    }
}

#[derive(Clone)]
/// Process lifecycle adapter backed by [`WamrRuntime`].
pub struct WamrProcessDriver {
    runtime: Arc<WamrRuntime>,
    repository: Arc<dyn ModuleRepositoryReadCapability + Send + Sync>,
}

impl WamrProcessDriver {
    /// Create a new WAMR-backed process lifecycle adapter.
    pub fn new(
        runtime: Arc<WamrRuntime>,
        repository: Arc<dyn ModuleRepositoryReadCapability + Send + Sync>,
    ) -> Arc<Self> {
        Arc::new(Self {
            runtime,
            repository,
        })
    }
}

impl ProcessLifecycleCapability for WamrProcessDriver {
    type Process = JoinHandle<Result<Vec<AbiValue>, String>>;
    type Error = Error;

    fn start(
        &self,
        registry: &Arc<Registry>,
        process_id: ResourceId,
        module_id: &str,
        name: &str,
        capabilities: Vec<Capability>,
        entrypoint: EntrypointInvocation,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        let inner = self.clone();
        let module_id = module_id.to_string();
        let entrypoint_name = name.to_string();
        let registry = Arc::clone(registry);

        async move {
            let bytes = inner.repository.read(&module_id)?;
            let runtime = Arc::clone(&inner.runtime);
            let registry_for_task = Arc::clone(&registry);
            let (start_tx, start_rx) = tokio::sync::oneshot::channel();
            let task = tokio::spawn(async move {
                start_rx
                    .await
                    .map_err(|_| "process start cancelled".to_string())?;
                tokio::task::spawn_blocking(move || {
                    runtime.execute(
                        &registry_for_task,
                        process_id,
                        bytes,
                        &entrypoint_name,
                        capabilities,
                        entrypoint,
                    )
                })
                .await
                .map_err(|err| format!("WAMR process worker failed: {err}"))?
                .map_err(|err| err.to_string())
            });

            registry
                .initialise(process_id, task)
                .map_err(KernelError::from)
                .map_err(Error::Kernel)?;
            start_tx.send(()).map_err(|_| {
                Error::Kernel(KernelError::Driver("process start cancelled".to_string()))
            })?;
            Ok(())
        }
    }

    async fn stop(&self, instance: &mut Self::Process) -> Result<(), Self::Error> {
        instance.abort();
        Ok(())
    }
}

extern "C" fn hostcall_create(
    exec_env: wasm_exec_env_t,
    args_ptr: *const u8,
    args_len: u32,
) -> u32 {
    match dispatch_create(exec_env, args_ptr, args_len) {
        Ok(word) => word,
        Err(err) => {
            set_exec_env_exception(exec_env, &err.to_string());
            0
        }
    }
}

extern "C" fn hostcall_poll(
    exec_env: wasm_exec_env_t,
    state_id: u32,
    task_id: u32,
    result_ptr: *mut u8,
    result_capacity: u32,
) -> u32 {
    match dispatch_poll(exec_env, state_id, task_id, result_ptr, result_capacity) {
        Ok(word) => word,
        Err(err) => {
            set_exec_env_exception(exec_env, &err.to_string());
            0
        }
    }
}

extern "C" fn hostcall_drop(
    exec_env: wasm_exec_env_t,
    state_id: u32,
    result_ptr: *mut u8,
    result_capacity: u32,
) -> u32 {
    match dispatch_drop(exec_env, state_id, result_ptr, result_capacity) {
        Ok(word) => word,
        Err(err) => {
            set_exec_env_exception(exec_env, &err.to_string());
            0
        }
    }
}

extern "C" fn guest_async_yield_now(exec_env: wasm_exec_env_t) {
    if let Ok(invocation) = hostcall_invocation(exec_env)
        && let Ok(registry) = invocation.registry()
        && let Some(mailbox) = registry.mailbox()
    {
        if mailbox.is_closed() || mailbox.is_signalled() {
            return;
        }

        while !mailbox.is_closed() && !mailbox.is_signalled() {
            std::thread::park_timeout(YIELD_PARK_TIMEOUT);
        }
        return;
    }

    std::thread::yield_now();
}

fn register_hostcall_modules(table: Arc<WamrHostcallTable>) -> Result<(), Error> {
    let mut modules: BTreeSet<&'static str> = BTreeSet::new();
    for hostcall in selium_abi::hostcalls::ALL {
        modules.insert(hostcall.name);
    }

    for module in modules {
        let registration = make_hostcall_registration(module, Arc::clone(&table))?;
        let ok = unsafe {
            wasm_runtime_register_natives(
                registration.module_name.as_ptr(),
                registration.symbols.as_ptr().cast_mut(),
                u32::try_from(registration.symbols.len())
                    .map_err(|_| Error::Wamr("symbol count exceeds u32".to_string()))?,
            )
        };
        if !ok {
            return Err(Error::Wamr(format!(
                "register native hostcall stubs for `{module}` failed"
            )));
        }
        registration.leak();
    }

    let async_registration = make_async_registration()?;
    let ok = unsafe {
        wasm_runtime_register_natives(
            async_registration.module_name.as_ptr(),
            async_registration.symbols.as_ptr().cast_mut(),
            u32::try_from(async_registration.symbols.len())
                .map_err(|_| Error::Wamr("symbol count exceeds u32".to_string()))?,
        )
    };
    if !ok {
        return Err(Error::Wamr(
            "register native guest async stubs failed".to_string(),
        ));
    }
    async_registration.leak();

    Ok(())
}

fn make_async_registration() -> Result<NativeModuleRegistration, Error> {
    let module_name = CString::new("selium::async")
        .map_err(|_| Error::Wamr("module name contains NUL byte".to_string()))?;
    let symbol_names = vec![
        CString::new("yield_now")
            .map_err(|_| Error::Wamr("symbol name contains NUL byte".to_string()))?,
    ];
    let symbol_signatures = vec![
        CString::new("()").map_err(|_| Error::Wamr("signature contains NUL byte".to_string()))?,
    ];
    let symbols = vec![NativeSymbol {
        symbol: symbol_names[0].as_ptr(),
        func_ptr: guest_async_yield_now as *mut c_void,
        signature: symbol_signatures[0].as_ptr(),
        attachment: std::ptr::null_mut(),
    }];

    Ok(NativeModuleRegistration {
        module_name,
        symbol_names,
        symbol_signatures,
        symbols: symbols.into_boxed_slice(),
        attachments: Vec::new(),
    })
}

fn dispatch_create(
    exec_env: wasm_exec_env_t,
    args_ptr: *const u8,
    args_len: u32,
) -> Result<u32, Error> {
    let module_inst = hostcall_module_inst(exec_env)?;
    let attachment = hostcall_attachment(exec_env)?;
    let invocation = hostcall_invocation(exec_env)?;
    let requested_capabilities = invocation.capabilities()?.clone();
    let mailbox_base = guest_memory_base(module_inst);
    let mut context = WamrHostcallContext {
        registry: invocation.registry()?,
        mailbox_base,
    };
    let args = as_guest_slice(args_ptr, args_len)?;
    let operation = attachment
        .table
        .resolve(attachment.module, &requested_capabilities)?;
    operation.create(&mut context, args).map_err(Error::Kernel)
}

fn dispatch_poll(
    exec_env: wasm_exec_env_t,
    state_id: u32,
    task_id: u32,
    result_ptr: *mut u8,
    result_capacity: u32,
) -> Result<u32, Error> {
    let module_inst = hostcall_module_inst(exec_env)?;
    let attachment = hostcall_attachment(exec_env)?;
    let invocation = hostcall_invocation(exec_env)?;
    let requested_capabilities = invocation.capabilities()?.clone();
    let mailbox_base = guest_memory_base(module_inst);
    let mut context = WamrHostcallContext {
        registry: invocation.registry()?,
        mailbox_base,
    };
    let operation = attachment
        .table
        .resolve(attachment.module, &requested_capabilities)?;
    let result = operation
        .poll(&mut context, state_id, task_id)
        .map_err(Error::Kernel)?;
    write_poll_result(result_ptr, result_capacity, result).map_err(Error::Kernel)
}

fn dispatch_drop(
    exec_env: wasm_exec_env_t,
    state_id: u32,
    result_ptr: *mut u8,
    result_capacity: u32,
) -> Result<u32, Error> {
    let module_inst = hostcall_module_inst(exec_env)?;
    let attachment = hostcall_attachment(exec_env)?;
    let invocation = hostcall_invocation(exec_env)?;
    let requested_capabilities = invocation.capabilities()?.clone();
    let mailbox_base = guest_memory_base(module_inst);
    let mut context = WamrHostcallContext {
        registry: invocation.registry()?,
        mailbox_base,
    };
    let operation = attachment
        .table
        .resolve(attachment.module, &requested_capabilities)?;
    let result = operation
        .drop_state(&mut context, state_id)
        .map_err(Error::Kernel)?;
    write_poll_result(result_ptr, result_capacity, result).map_err(Error::Kernel)
}

fn set_exec_env_exception(exec_env: wasm_exec_env_t, message: &str) {
    let module_inst = unsafe { wasm_runtime_get_module_inst(exec_env) };
    if module_inst.is_null() {
        return;
    }

    let filtered: Vec<u8> = message.bytes().filter(|byte| *byte != 0).collect();
    let c_message = CString::new(filtered)
        .or_else(|_| CString::new("kernel error"))
        .ok();
    if let Some(message) = c_message {
        unsafe {
            wasm_runtime_set_exception(module_inst, message.as_ptr());
        }
    }
}

fn as_guest_slice<'a>(ptr: *const u8, len: u32) -> Result<&'a [u8], Error> {
    if len == 0 {
        return Ok(&[]);
    }
    if ptr.is_null() {
        return Err(Error::Kernel(KernelError::MemoryMissing));
    }
    let len = usize::try_from(len).map_err(KernelError::IntConvert)?;
    Ok(unsafe { std::slice::from_raw_parts(ptr, len) })
}

fn write_poll_result(
    result_ptr: *mut u8,
    result_capacity: u32,
    result: GuestResult<Vec<u8>>,
) -> Result<GuestUint, KernelError> {
    match result {
        Ok(bytes) => write_encoded_bytes(result_ptr, result_capacity, &bytes),
        Err(error) => encode_guest_error(result_ptr, result_capacity, error),
    }
}

fn write_encoded_bytes(
    result_ptr: *mut u8,
    result_capacity: u32,
    bytes: &[u8],
) -> Result<GuestUint, KernelError> {
    let capacity = usize::try_from(result_capacity).map_err(KernelError::IntConvert)?;
    if capacity < bytes.len() {
        return Err(KernelError::MemoryCapacity);
    }
    if bytes.is_empty() {
        return driver_encode_ready(0).ok_or(KernelError::MemoryCapacity);
    }
    if result_ptr.is_null() {
        return Err(KernelError::MemoryMissing);
    }

    unsafe {
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), result_ptr, bytes.len());
    }

    let len = GuestUint::try_from(bytes.len()).map_err(|_| KernelError::MemoryCapacity)?;
    driver_encode_ready(len).ok_or(KernelError::MemoryCapacity)
}

fn encode_guest_error(
    result_ptr: *mut u8,
    result_capacity: u32,
    error: GuestError,
) -> Result<GuestUint, KernelError> {
    if matches!(error, GuestError::WouldBlock) {
        return Ok(DRIVER_RESULT_PENDING);
    }

    let bytes = encode_driver_error_message(&error.to_string())
        .map_err(|err| KernelError::Driver(err.to_string()))?;
    write_encoded_bytes(result_ptr, result_capacity, &bytes)?;
    Ok(driver_encode_error(DRIVER_ERROR_MESSAGE_CODE))
}

fn hostcall_module_inst(exec_env: wasm_exec_env_t) -> Result<wasm_module_inst_t, Error> {
    let module_inst = unsafe { wasm_runtime_get_module_inst(exec_env) };
    if module_inst.is_null() {
        return Err(Error::Wamr("module instance is missing".to_string()));
    }
    Ok(module_inst)
}

fn hostcall_attachment(
    exec_env: wasm_exec_env_t,
) -> Result<&'static NativeHostcallAttachment, Error> {
    let attachment_ptr = unsafe { wasm_runtime_get_function_attachment(exec_env) };
    if attachment_ptr.is_null() {
        return Err(Error::Wamr(
            "native function attachment is missing".to_string(),
        ));
    }
    let attachment = unsafe { &*(attachment_ptr as *const NativeHostcallAttachment) };
    Ok(attachment)
}

fn hostcall_invocation(
    exec_env: wasm_exec_env_t,
) -> Result<&'static mut HostcallInvocationContext, Error> {
    let ptr = unsafe { wasm_runtime_get_user_data(exec_env) as *mut HostcallInvocationContext };
    if ptr.is_null() {
        return Err(Error::Wamr(
            "hostcall invocation context is missing".to_string(),
        ));
    }
    Ok(unsafe { &mut *ptr })
}

fn guest_memory_base(module_inst: wasm_module_inst_t) -> Option<usize> {
    let base = unsafe { wasm_runtime_addr_app_to_native(module_inst, 0) };
    if base.is_null() {
        None
    } else {
        Some(base as usize)
    }
}

fn make_hostcall_registration(
    module_name: &'static str,
    table: Arc<WamrHostcallTable>,
) -> Result<NativeModuleRegistration, Error> {
    let module = module_name;
    let module_name = CString::new(module)
        .map_err(|_| Error::Wamr("module name contains NUL byte".to_string()))?;
    let symbol_names = vec![
        CString::new("create")
            .map_err(|_| Error::Wamr("symbol name contains NUL byte".to_string()))?,
        CString::new("poll")
            .map_err(|_| Error::Wamr("symbol name contains NUL byte".to_string()))?,
        CString::new("drop")
            .map_err(|_| Error::Wamr("symbol name contains NUL byte".to_string()))?,
    ];
    let symbol_signatures = vec![
        CString::new("(*~)i")
            .map_err(|_| Error::Wamr("signature contains NUL byte".to_string()))?,
        CString::new("(ii*~)i")
            .map_err(|_| Error::Wamr("signature contains NUL byte".to_string()))?,
        CString::new("(i*~)i")
            .map_err(|_| Error::Wamr("signature contains NUL byte".to_string()))?,
    ];

    let attachments = vec![NativeHostcallAttachment { table, module }];
    let attachment_ptr = attachments
        .as_ptr()
        .cast_mut()
        .cast::<NativeHostcallAttachment>();

    let symbols = vec![
        NativeSymbol {
            symbol: symbol_names[0].as_ptr(),
            func_ptr: hostcall_create as *mut c_void,
            signature: symbol_signatures[0].as_ptr(),
            attachment: attachment_ptr.cast(),
        },
        NativeSymbol {
            symbol: symbol_names[1].as_ptr(),
            func_ptr: hostcall_poll as *mut c_void,
            signature: symbol_signatures[1].as_ptr(),
            attachment: attachment_ptr.cast(),
        },
        NativeSymbol {
            symbol: symbol_names[2].as_ptr(),
            func_ptr: hostcall_drop as *mut c_void,
            signature: symbol_signatures[2].as_ptr(),
            attachment: attachment_ptr.cast(),
        },
    ];

    Ok(NativeModuleRegistration {
        module_name,
        symbol_names,
        symbol_signatures,
        symbols: symbols.into_boxed_slice(),
        attachments,
    })
}

fn materialise_plan(module_inst: wasm_module_inst_t, plan: &CallPlan) -> Result<(), Error> {
    for write in plan.memory_writes() {
        if write.bytes.is_empty() {
            continue;
        }
        write_guest_bytes(module_inst, write.offset.into(), &write.bytes)?;
    }
    Ok(())
}

fn write_guest_bytes(
    module_inst: wasm_module_inst_t,
    offset: u64,
    bytes: &[u8],
) -> Result<(), Error> {
    let size =
        u64::try_from(bytes.len()).map_err(|_| Error::Kernel(KernelError::MemoryCapacity))?;
    let valid = unsafe { wasm_runtime_validate_app_addr(module_inst, offset, size) };
    if !valid {
        return Err(Error::Kernel(KernelError::MemoryCapacity));
    }
    let native = unsafe { wasm_runtime_addr_app_to_native(module_inst, offset) };
    if native.is_null() {
        return Err(Error::Kernel(KernelError::MemoryMissing));
    }
    unsafe {
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), native.cast(), bytes.len());
    }
    Ok(())
}

fn read_guest_bytes(
    module_inst: wasm_module_inst_t,
    offset: u64,
    len: u64,
) -> Result<Vec<u8>, Error> {
    let valid = unsafe { wasm_runtime_validate_app_addr(module_inst, offset, len) };
    if !valid {
        return Err(Error::Kernel(KernelError::MemoryCapacity));
    }
    let native = unsafe { wasm_runtime_addr_app_to_native(module_inst, offset) };
    if native.is_null() {
        return Err(Error::Kernel(KernelError::MemoryMissing));
    }
    let len = usize::try_from(len).map_err(KernelError::IntConvert)?;
    let mut out = vec![0u8; len];
    unsafe {
        std::ptr::copy_nonoverlapping(native.cast::<u8>(), out.as_mut_ptr(), len);
    }
    Ok(out)
}

fn required_argv_capacity(signature: &AbiSignature, param_words: usize) -> usize {
    let mut result_words = 0usize;
    for result in signature.results() {
        match result {
            AbiParam::Scalar(AbiScalarType::I64)
            | AbiParam::Scalar(AbiScalarType::U64)
            | AbiParam::Scalar(AbiScalarType::F64) => result_words += 2,
            AbiParam::Buffer => result_words += 2,
            AbiParam::Scalar(_) => result_words += 1,
        }
    }

    param_words.max(result_words).max(1)
}

fn encode_param_words(values: &[AbiScalarValue]) -> Vec<u32> {
    let mut words = Vec::new();
    for value in values {
        match value {
            AbiScalarValue::I8(v) => words.push(u32::from_ne_bytes(i32::from(*v).to_ne_bytes())),
            AbiScalarValue::U8(v) => words.push(u32::from(*v)),
            AbiScalarValue::I16(v) => words.push(u32::from_ne_bytes(i32::from(*v).to_ne_bytes())),
            AbiScalarValue::U16(v) => words.push(u32::from(*v)),
            AbiScalarValue::I32(v) => words.push(u32::from_ne_bytes(v.to_ne_bytes())),
            AbiScalarValue::U32(v) => words.push(*v),
            AbiScalarValue::I64(v) => {
                split_u64_words(&mut words, u64::from_ne_bytes(v.to_ne_bytes()))
            }
            AbiScalarValue::U64(v) => split_u64_words(&mut words, *v),
            AbiScalarValue::F32(v) => words.push(v.to_bits()),
            AbiScalarValue::F64(v) => split_u64_words(&mut words, v.to_bits()),
        }
    }
    words
}

fn split_u64_words(words: &mut Vec<u32>, value: u64) {
    words.push((value & 0xffff_ffff) as u32);
    words.push((value >> 32) as u32);
}

fn decode_results(
    module_inst: wasm_module_inst_t,
    raw_words: &[u32],
    signature: &AbiSignature,
) -> Result<Vec<AbiValue>, Error> {
    let mut values = Vec::with_capacity(signature.results().len());
    let mut cursor = 0usize;

    for result in signature.results() {
        match result {
            AbiParam::Scalar(kind) => {
                let value = decode_scalar(raw_words, &mut cursor, *kind)?;
                values.push(AbiValue::Scalar(value));
            }
            AbiParam::Buffer => {
                let ptr = take_i32(raw_words, &mut cursor, "missing buffer pointer")?;
                let len = take_i32(raw_words, &mut cursor, "missing buffer length")?;
                if ptr < 0 || len < 0 {
                    return Err(Error::Wamr(
                        "buffer pointer and length must be non-negative".to_string(),
                    ));
                }
                let data = read_guest_bytes(module_inst, ptr as u64, len as u64)?;
                values.push(AbiValue::Buffer(data));
            }
        }
    }

    Ok(values)
}

fn decode_scalar(
    raw_words: &[u32],
    cursor: &mut usize,
    expected: AbiScalarType,
) -> Result<AbiScalarValue, Error> {
    match expected {
        AbiScalarType::I8 => {
            let raw = take_i32(raw_words, cursor, "missing i8 result")?;
            let value =
                i8::try_from(raw).map_err(|_| Error::Wamr("i8 result out of range".to_string()))?;
            Ok(AbiScalarValue::I8(value))
        }
        AbiScalarType::U8 => {
            let raw = take_u32(raw_words, cursor, "missing u8 result")?;
            let value =
                u8::try_from(raw).map_err(|_| Error::Wamr("u8 result out of range".to_string()))?;
            Ok(AbiScalarValue::U8(value))
        }
        AbiScalarType::I16 => {
            let raw = take_i32(raw_words, cursor, "missing i16 result")?;
            let value = i16::try_from(raw)
                .map_err(|_| Error::Wamr("i16 result out of range".to_string()))?;
            Ok(AbiScalarValue::I16(value))
        }
        AbiScalarType::U16 => {
            let raw = take_u32(raw_words, cursor, "missing u16 result")?;
            let value = u16::try_from(raw)
                .map_err(|_| Error::Wamr("u16 result out of range".to_string()))?;
            Ok(AbiScalarValue::U16(value))
        }
        AbiScalarType::I32 => {
            let raw = take_i32(raw_words, cursor, "missing i32 result")?;
            Ok(AbiScalarValue::I32(raw))
        }
        AbiScalarType::U32 => {
            let raw = take_u32(raw_words, cursor, "missing u32 result")?;
            Ok(AbiScalarValue::U32(raw))
        }
        AbiScalarType::I64 => {
            let lo = take_u32(raw_words, cursor, "missing low i64 result")?;
            let hi = take_u32(raw_words, cursor, "missing high i64 result")?;
            let combined = (u64::from(hi) << 32) | u64::from(lo);
            Ok(AbiScalarValue::I64(i64::from_le_bytes(
                combined.to_le_bytes(),
            )))
        }
        AbiScalarType::U64 => {
            let lo = take_u32(raw_words, cursor, "missing low u64 result")?;
            let hi = take_u32(raw_words, cursor, "missing high u64 result")?;
            let combined = (u64::from(hi) << 32) | u64::from(lo);
            Ok(AbiScalarValue::U64(combined))
        }
        AbiScalarType::F32 => {
            let bits = take_u32(raw_words, cursor, "missing f32 result")?;
            Ok(AbiScalarValue::F32(f32::from_bits(bits)))
        }
        AbiScalarType::F64 => {
            let lo = take_u32(raw_words, cursor, "missing low f64 result")?;
            let hi = take_u32(raw_words, cursor, "missing high f64 result")?;
            let combined = (u64::from(hi) << 32) | u64::from(lo);
            Ok(AbiScalarValue::F64(f64::from_bits(combined)))
        }
    }
}

fn take_i32(raw_words: &[u32], cursor: &mut usize, message: &str) -> Result<i32, Error> {
    let word = raw_words
        .get(*cursor)
        .ok_or_else(|| Error::Wamr(message.to_string()))?;
    *cursor += 1;
    Ok(i32::from_ne_bytes(word.to_ne_bytes()))
}

fn take_u32(raw_words: &[u32], cursor: &mut usize, message: &str) -> Result<u32, Error> {
    let word = raw_words
        .get(*cursor)
        .ok_or_else(|| Error::Wamr(message.to_string()))?;
    *cursor += 1;
    Ok(*word)
}

fn last_exception(module_inst: wasm_module_inst_t) -> String {
    let ptr = unsafe { wasm_runtime_get_exception(module_inst) };
    c_ptr_to_string(ptr).unwrap_or_else(|| "unknown WAMR exception".to_string())
}

fn error_buffer_to_string(error_buf: &[c_char]) -> String {
    let ptr = error_buf.as_ptr();
    c_ptr_to_string(ptr).unwrap_or_else(|| "unknown WAMR error".to_string())
}

fn c_ptr_to_string(ptr: *const c_char) -> Option<String> {
    if ptr.is_null() {
        return None;
    }
    Some(unsafe { CStr::from_ptr(ptr) }.to_string_lossy().to_string())
}
