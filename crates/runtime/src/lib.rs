//! Selium runtime built on top of Wasmtiny and the Selium kernel.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use parking_lot::Mutex;
use selium_abi::{
    AbiError, AbiErrorCode, ActivityEvent, Capability, CapabilityGrant, CompletionState,
    EntrypointMetadata, HostcallOutput, HostcallRequest, LocalityScope, OperationId, ProcessId,
    ResourceClass, ResourceIdentity, ScopeContext, SignalDescriptor, pack_hostcall_status,
};
use selium_kernel::Kernel;
use thiserror::Error;
use tracing::info;
use wasmtiny::{
    FunctionType, NumType, ValType, WasmApplication, WasmError, WasmValue,
    runtime::{HostFunc, Store},
};

type LocalHandleOwners = HashMap<(ResourceClass, u64), BTreeSet<ProcessId>>;
type SharedResourceOwners = HashMap<(ResourceClass, u64), BTreeSet<ProcessId>>;
pub type Result<T> = std::result::Result<T, Error>;

const DEFAULT_READINESS_POLL_MS: u64 = 10;
const DEFAULT_READINESS_TIMEOUT_MS: u64 = 1_000;

#[derive(Debug, Error)]
pub enum Error {
    #[error("system guest descriptor not found: {0}")]
    DescriptorNotFound(String),
    #[error("unknown process authority: {0}")]
    UnknownProcessAuthority(ProcessId),
    #[error("unknown dependency: {0}")]
    UnknownDependency(String),
    #[error("dependency cycle or unresolved dependency detected")]
    DependencyCycle,
    #[error("invalid grant for capability {0:?}")]
    InvalidGrant(Capability),
    #[error("duplicate system guest descriptor: {0}")]
    DuplicateDescriptor(String),
    #[error("module id already registered with different bytes: {0}")]
    ModuleConflict(String),
    #[error("module not registered: {0}")]
    UnknownModule(String),
    #[error("invalid entrypoint argument encoding")]
    InvalidEntrypointArgument,
    #[error("readiness condition not satisfied for guest `{0}`")]
    ReadinessUnsatisfied(String),
    #[error("kernel error: {0}")]
    Kernel(#[from] selium_kernel::Error),
    #[error("wasmtiny runtime error: {0}")]
    Wasm(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadinessCondition {
    Immediate,
    ActivityLogContains(String),
}

#[derive(Debug, Clone)]
pub struct SystemGuestDescriptor {
    pub name: String,
    pub module_id: String,
    pub module_bytes: Vec<u8>,
    pub entrypoint: String,
    pub arguments: Vec<Vec<u8>>,
    pub grants: Vec<CapabilityGrant>,
    pub dependencies: Vec<String>,
    pub readiness: ReadinessCondition,
}

#[derive(Debug, Clone, Default)]
pub struct RuntimeConfig {
    pub system_guests: Vec<SystemGuestDescriptor>,
}

#[derive(Debug, Clone)]
pub struct BootstrappedGuest {
    pub name: String,
    pub process_id: ProcessId,
}

#[derive(Debug, Clone, Default)]
pub struct BootstrapReport {
    pub guests: Vec<BootstrappedGuest>,
}

#[derive(Debug, Clone)]
pub struct ProcessAuthority {
    pub grants: Vec<CapabilityGrant>,
}

#[derive(Clone)]
pub struct Runtime {
    kernel: Kernel,
    process_authorities: Arc<Mutex<HashMap<ProcessId, ProcessAuthority>>>,
    loaded_guests: Arc<Mutex<HashMap<selium_abi::ProcessId, LoadedGuest>>>,
    local_handle_owners: Arc<Mutex<LocalHandleOwners>>,
    shared_resource_owners: Arc<Mutex<SharedResourceOwners>>,
    module_registry: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    next_operation_id: Arc<Mutex<OperationId>>,
    operations: Arc<Mutex<HashMap<OperationId, HostOperation>>>,
}

struct LoadedGuest {
    app: WasmApplication,
    module_index: u32,
    entrypoint_results: Vec<WasmValue>,
}

#[derive(Debug, Clone)]
enum HostOperationState {
    Ready(HostcallOutput),
    Failed(AbiError),
    SignalWait {
        local_id: u64,
        observed_generation: u64,
        deadline: Instant,
    },
}

#[derive(Debug, Clone)]
struct HostOperation {
    process_id: ProcessId,
    state: HostOperationState,
}

struct MarkReadyHostFunc {
    runtime: Runtime,
    process_id: selium_abi::ProcessId,
}

struct ProcessIdHostFunc {
    process_id: selium_abi::ProcessId,
}

struct HostcallCreateHostFunc {
    runtime: Runtime,
    process_id: ProcessId,
}

struct HostcallPollHostFunc {
    runtime: Runtime,
    process_id: ProcessId,
}

struct HostcallDropHostFunc {
    runtime: Runtime,
    process_id: ProcessId,
}

impl SystemGuestDescriptor {
    pub fn from_entrypoint_metadata(
        name: impl Into<String>,
        module_id: impl Into<String>,
        module_bytes: Vec<u8>,
        metadata: EntrypointMetadata,
        grants: Vec<CapabilityGrant>,
    ) -> Self {
        Self {
            name: name.into(),
            module_id: module_id.into(),
            module_bytes,
            entrypoint: metadata.name,
            arguments: Vec::new(),
            grants,
            dependencies: Vec::new(),
            readiness: ReadinessCondition::Immediate,
        }
    }
}

impl Runtime {
    pub fn new(kernel: Kernel) -> Self {
        Self {
            kernel,
            process_authorities: Arc::new(Mutex::new(HashMap::new())),
            loaded_guests: Arc::new(Mutex::new(HashMap::new())),
            local_handle_owners: Arc::new(Mutex::new(HashMap::new())),
            shared_resource_owners: Arc::new(Mutex::new(HashMap::new())),
            module_registry: Arc::new(Mutex::new(HashMap::new())),
            next_operation_id: Arc::new(Mutex::new(1)),
            operations: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn kernel(&self) -> Kernel {
        self.kernel.clone()
    }

    pub fn bootstrap_system_guests(&self, config: RuntimeConfig) -> Result<BootstrapReport> {
        let mut pending = BTreeMap::new();
        for descriptor in config.system_guests {
            let name = descriptor.name.clone();
            if pending.insert(name.clone(), descriptor).is_some() {
                return Err(Error::DuplicateDescriptor(name));
            }
        }

        let mut ready = BTreeSet::new();
        let mut report = BootstrapReport::default();

        while !pending.is_empty() {
            let ready_name = pending.iter().find_map(|(name, descriptor)| {
                descriptor
                    .dependencies
                    .iter()
                    .all(|dependency| ready.contains(dependency))
                    .then_some(name.clone())
            });
            let Some(name) = ready_name else {
                if let Some(missing_dependency) = pending
                    .values()
                    .flat_map(|descriptor| descriptor.dependencies.iter())
                    .find(|dependency| {
                        !ready.contains(*dependency) && !pending.contains_key(*dependency)
                    })
                {
                    self.rollback_bootstrapped(&report);
                    return Err(Error::UnknownDependency(missing_dependency.clone()));
                }
                self.rollback_bootstrapped(&report);
                return Err(Error::DependencyCycle);
            };

            let descriptor = pending
                .remove(&name)
                .ok_or_else(|| Error::DescriptorNotFound(name.clone()))?;
            let bootstrapped = match self.spawn_system_guest(descriptor.clone()) {
                Ok(bootstrapped) => bootstrapped,
                Err(error) => {
                    self.rollback_bootstrapped(&report);
                    return Err(error);
                }
            };
            if !self.wait_for_readiness(bootstrapped.process_id, &descriptor.readiness) {
                let _ = self.stop_process(bootstrapped.process_id);
                self.rollback_bootstrapped(&report);
                return Err(Error::ReadinessUnsatisfied(descriptor.name));
            }
            ready.insert(name);
            report.guests.push(bootstrapped);
        }

        Ok(report)
    }

    pub fn spawn_system_guest(
        &self,
        descriptor: SystemGuestDescriptor,
    ) -> Result<BootstrappedGuest> {
        self.validate_grants(&descriptor.grants)?;
        let process = self.kernel.start_process(
            descriptor.module_id.clone(),
            descriptor.entrypoint.clone(),
            descriptor.grants.clone(),
        );
        self.persist_process_authority(process.local_id, descriptor.grants.clone());

        let loaded_guest = match self.load_guest_module(&descriptor.module_bytes, process.local_id)
        {
            Ok(loaded_guest) => loaded_guest,
            Err(error) => {
                self.cleanup_failed_process(process.local_id)?;
                return Err(error);
            }
        };
        let loaded_guest = match self.execute_entrypoint(loaded_guest, &descriptor) {
            Ok(loaded_guest) => loaded_guest,
            Err(error) => {
                self.kernel.record_activity(ActivityEvent {
                    kind: selium_abi::ActivityKind::ProcessExited,
                    process_id: Some(process.local_id),
                    message: format!("guest {} trapped: {error}", descriptor.name),
                });
                self.cleanup_failed_process(process.local_id)?;
                return Err(error);
            }
        };

        self.loaded_guests
            .lock()
            .insert(process.local_id, loaded_guest);
        self.claim_local_handle(process.local_id, ResourceClass::Process, process.local_id);
        self.register_module_bytes(
            descriptor.module_id.clone(),
            descriptor.module_bytes.clone(),
        )?;
        self.kernel.record_activity(ActivityEvent {
            kind: selium_abi::ActivityKind::GuestBootstrapped,
            process_id: Some(process.local_id),
            message: format!("guest {} bootstrapped", descriptor.name),
        });
        info!(
            guest = descriptor.name.as_str(),
            process_id = process.local_id,
            "bootstrapped system guest"
        );

        Ok(BootstrappedGuest {
            name: descriptor.name,
            process_id: process.local_id,
        })
    }

    pub fn stop_process(&self, process_id: selium_abi::ProcessId) -> Result<()> {
        self.kernel.stop_process(process_id)?;
        self.loaded_guests.lock().remove(&process_id);
        if self
            .process_authorities
            .lock()
            .remove(&process_id)
            .is_some()
        {
            self.operations
                .lock()
                .retain(|_, operation| operation.process_id != process_id);
            self.cleanup_process_resources(process_id)?;
        }
        self.local_handle_owners
            .lock()
            .remove(&(ResourceClass::Process, process_id));
        self.kernel.reap_process(process_id)?;
        Ok(())
    }

    pub fn restore_process_authority(&self, process_id: ProcessId) -> Option<ProcessAuthority> {
        self.process_authorities.lock().get(&process_id).cloned()
    }

    pub fn authorises(
        &self,
        process_id: ProcessId,
        capability: Capability,
        context: &ScopeContext,
    ) -> bool {
        self.process_authorities
            .lock()
            .get(&process_id)
            .map(|record| {
                record
                    .grants
                    .iter()
                    .any(|grant| grant.capability == capability && grant.allows(context))
            })
            .unwrap_or(false)
    }

    pub fn project_metering(
        &self,
        process_id: selium_abi::ProcessId,
        observation: selium_abi::MeteringObservation,
    ) {
        self.kernel.observe_metering(process_id, observation);
    }

    pub fn activity_log(&self) -> Vec<ActivityEvent> {
        self.kernel.read_activity_from(0)
    }

    pub fn loaded_entrypoint(&self, process_id: selium_abi::ProcessId) -> Option<u32> {
        self.loaded_guests
            .lock()
            .get(&process_id)
            .map(|guest| guest.module_index)
    }

    pub fn entrypoint_results(&self, process_id: selium_abi::ProcessId) -> Option<Vec<WasmValue>> {
        self.loaded_guests
            .lock()
            .get(&process_id)
            .map(|guest| guest.entrypoint_results.clone())
    }

    pub fn loaded_guest_count(&self) -> usize {
        self.loaded_guests.lock().len()
    }

    pub fn register_module_bytes(&self, module_id: String, module_bytes: Vec<u8>) -> Result<()> {
        let mut registry = self.module_registry.lock();
        match registry.get(&module_id) {
            Some(existing) if existing == &module_bytes => Ok(()),
            Some(_) => Err(Error::ModuleConflict(module_id)),
            None => {
                registry.insert(module_id, module_bytes);
                Ok(())
            }
        }
    }

    pub fn begin_hostcall(
        &self,
        process_id: ProcessId,
        request: HostcallRequest,
    ) -> (u32, OperationId) {
        let state = match self.dispatch_hostcall(process_id, request) {
            Ok(state) => state,
            Err(error) => HostOperationState::Failed(error),
        };
        let status = match state {
            HostOperationState::Ready(_) => selium_abi::HOSTCALL_STATUS_READY,
            HostOperationState::Failed(_) => selium_abi::HOSTCALL_STATUS_FAILED,
            HostOperationState::SignalWait { .. } => selium_abi::HOSTCALL_STATUS_PENDING,
        };
        let mut operations = self.operations.lock();
        let operation_id = self.next_operation_id(&operations);
        operations.insert(operation_id, HostOperation { process_id, state });
        (status, operation_id)
    }

    pub fn poll_hostcall(
        &self,
        process_id: ProcessId,
        operation_id: OperationId,
    ) -> CompletionState {
        let mut operations = self.operations.lock();
        let Some(operation) = operations.get_mut(&operation_id) else {
            return CompletionState::Failed(AbiError::new(
                AbiErrorCode::InvalidHandle,
                format!("unknown operation {operation_id}"),
            ));
        };
        if operation.process_id != process_id {
            return CompletionState::Failed(AbiError::new(
                AbiErrorCode::PermissionDenied,
                "operation belongs to another process",
            ));
        }

        match &operation.state {
            HostOperationState::Ready(output) => CompletionState::Ready(output.clone()),
            HostOperationState::Failed(error) => CompletionState::Failed(error.clone()),
            HostOperationState::SignalWait {
                local_id,
                observed_generation,
                deadline,
            } => match self.kernel.signal_generation(*local_id) {
                Ok(generation) if generation > *observed_generation => {
                    operation.state =
                        HostOperationState::Ready(HostcallOutput::SignalGeneration(generation));
                    CompletionState::Ready(HostcallOutput::SignalGeneration(generation))
                }
                Ok(_) if Instant::now() >= *deadline => {
                    let error = AbiError::new(AbiErrorCode::Timeout, "signal wait timed out");
                    operation.state = HostOperationState::Failed(error.clone());
                    CompletionState::Failed(error)
                }
                Ok(_) => CompletionState::Pending { operation_id },
                Err(error) => CompletionState::Failed(kernel_error(error)),
            },
        }
    }

    pub fn drop_hostcall(&self, process_id: ProcessId, operation_id: OperationId) -> bool {
        let mut operations = self.operations.lock();
        if operations
            .get(&operation_id)
            .is_some_and(|operation| operation.process_id == process_id)
        {
            operations.remove(&operation_id);
            true
        } else {
            false
        }
    }

    fn dispatch_hostcall(
        &self,
        process_id: ProcessId,
        request: HostcallRequest,
    ) -> std::result::Result<HostOperationState, AbiError> {
        if !self.process_authorities.lock().contains_key(&process_id) {
            return Err(AbiError::new(
                AbiErrorCode::InvalidHandle,
                format!("unknown process authority {process_id}"),
            ));
        }

        match request {
            HostcallRequest::SignalCreate => {
                self.require(process_id, Capability::Signal, ResourceClass::Signal, None)?;
                let descriptor = self.kernel.create_signal();
                self.claim_signal(process_id, descriptor);
                Ok(HostOperationState::Ready(HostcallOutput::Signal(
                    descriptor,
                )))
            }
            HostcallRequest::SignalAttach { shared_id } => {
                self.require(
                    process_id,
                    Capability::Signal,
                    ResourceClass::Signal,
                    Some(ResourceIdentity::Shared(shared_id)),
                )?;
                let descriptor = self.kernel.attach_signal(shared_id).map_err(kernel_error)?;
                self.claim_local_handle(process_id, ResourceClass::Signal, descriptor.local_id);
                Ok(HostOperationState::Ready(HostcallOutput::Signal(
                    descriptor,
                )))
            }
            HostcallRequest::SignalClose { local_id } => {
                self.ensure_local_handle_owner(
                    process_id,
                    Capability::Signal,
                    ResourceClass::Signal,
                    local_id,
                )?;
                let shared_id = self
                    .kernel
                    .signal_shared_id(local_id)
                    .map_err(kernel_error)?;
                self.kernel.close_signal(local_id).map_err(kernel_error)?;
                self.release_local_handle(process_id, &ResourceClass::Signal, local_id);
                if self.kernel.signal_handle_count(shared_id) == 0 {
                    self.release_shared_resource(process_id, &ResourceClass::Signal, shared_id);
                }
                Ok(HostOperationState::Ready(HostcallOutput::Empty))
            }
            HostcallRequest::SignalNotify { local_id } => {
                self.ensure_local_handle_owner(
                    process_id,
                    Capability::Signal,
                    ResourceClass::Signal,
                    local_id,
                )?;
                let shared_id = self
                    .kernel
                    .signal_shared_id(local_id)
                    .map_err(kernel_error)?;
                self.require(
                    process_id,
                    Capability::Signal,
                    ResourceClass::Signal,
                    Some(ResourceIdentity::Shared(shared_id)),
                )?;
                let generation = self.kernel.notify_signal(local_id).map_err(kernel_error)?;
                Ok(HostOperationState::Ready(HostcallOutput::SignalGeneration(
                    generation,
                )))
            }
            HostcallRequest::SignalWait {
                local_id,
                observed_generation,
                timeout_ms,
            } => {
                self.ensure_local_handle_owner(
                    process_id,
                    Capability::Signal,
                    ResourceClass::Signal,
                    local_id,
                )?;
                let shared_id = self
                    .kernel
                    .signal_shared_id(local_id)
                    .map_err(kernel_error)?;
                self.require(
                    process_id,
                    Capability::Signal,
                    ResourceClass::Signal,
                    Some(ResourceIdentity::Shared(shared_id)),
                )?;
                let generation = self
                    .kernel
                    .signal_generation(local_id)
                    .map_err(kernel_error)?;
                if generation > observed_generation {
                    Ok(HostOperationState::Ready(HostcallOutput::SignalGeneration(
                        generation,
                    )))
                } else {
                    Ok(HostOperationState::SignalWait {
                        local_id,
                        observed_generation,
                        deadline: Instant::now() + Duration::from_millis(timeout_ms),
                    })
                }
            }
        }
    }

    fn wait_for_readiness(
        &self,
        process_id: selium_abi::ProcessId,
        condition: &ReadinessCondition,
    ) -> bool {
        match condition {
            ReadinessCondition::Immediate => true,
            ReadinessCondition::ActivityLogContains(fragment) => {
                let deadline = Instant::now() + Duration::from_millis(DEFAULT_READINESS_TIMEOUT_MS);
                let mut cursor = 0;
                loop {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    let events = self
                        .kernel
                        .wait_for_activity_from(cursor, remaining.as_millis() as u64);
                    cursor += events.len();
                    if events.iter().any(|event| {
                        event.process_id == Some(process_id) && event.message.contains(fragment)
                    }) {
                        return true;
                    }
                    if Instant::now() >= deadline {
                        return false;
                    }
                    thread::sleep(Duration::from_millis(DEFAULT_READINESS_POLL_MS));
                }
            }
        }
    }

    fn persist_process_authority(&self, process_id: ProcessId, grants: Vec<CapabilityGrant>) {
        self.process_authorities
            .lock()
            .insert(process_id, ProcessAuthority { grants });
    }

    fn validate_grants(&self, grants: &[CapabilityGrant]) -> Result<()> {
        for grant in grants {
            if grant.selectors.is_empty() {
                return Err(Error::InvalidGrant(grant.capability.clone()));
            }
        }
        Ok(())
    }

    fn rollback_bootstrapped(&self, report: &BootstrapReport) {
        for guest in report.guests.iter().rev() {
            let _ = self.stop_process(guest.process_id);
        }
    }

    fn cleanup_failed_process(&self, process_id: selium_abi::ProcessId) -> Result<()> {
        let _ = self.kernel.stop_process(process_id);
        self.operations
            .lock()
            .retain(|_, operation| operation.process_id != process_id);
        let _ = self.cleanup_process_resources(process_id);
        let _ = self.kernel.reap_process(process_id);
        self.process_authorities.lock().remove(&process_id);
        self.local_handle_owners
            .lock()
            .remove(&(ResourceClass::Process, process_id));
        self.shared_resource_owners
            .lock()
            .retain(|_, owners| !owners.contains(&process_id));
        Ok(())
    }

    fn cleanup_process_resources(&self, process_id: ProcessId) -> Result<()> {
        let owned_handles = self
            .local_handle_owners
            .lock()
            .iter()
            .filter_map(|((resource_class, local_id), owners)| {
                owners
                    .contains(&process_id)
                    .then_some((resource_class.clone(), *local_id))
            })
            .collect::<Vec<_>>();

        for (resource_class, local_id) in owned_handles {
            let should_reclaim = self.release_local_handle(process_id, &resource_class, local_id);
            if !should_reclaim {
                continue;
            }
            if resource_class == ResourceClass::Signal {
                let _ = self.kernel.close_signal(local_id);
            }
        }
        Ok(())
    }

    fn load_guest_module(
        &self,
        module_bytes: &[u8],
        process_id: selium_abi::ProcessId,
    ) -> Result<LoadedGuest> {
        let mut app = WasmApplication::new();
        let module_index = app
            .load_module_from_memory(module_bytes)
            .map_err(map_wasm_error)?;
        self.register_runtime_host_functions(&mut app, module_index, process_id)?;
        app.instantiate(module_index).map_err(map_wasm_error)?;
        app.execute_start(module_index).map_err(map_wasm_error)?;
        Ok(LoadedGuest {
            app,
            module_index,
            entrypoint_results: Vec::new(),
        })
    }

    fn register_runtime_host_functions(
        &self,
        app: &mut WasmApplication,
        module_index: u32,
        process_id: selium_abi::ProcessId,
    ) -> Result<()> {
        register_optional_host_function(
            app,
            module_index,
            "selium",
            "process_id",
            Box::new(ProcessIdHostFunc { process_id }),
            FunctionType::new(vec![], vec![ValType::Num(NumType::I64)]),
        )?;
        register_optional_host_function(
            app,
            module_index,
            "selium",
            "mark_ready",
            Box::new(MarkReadyHostFunc {
                runtime: self.clone(),
                process_id,
            }),
            FunctionType::empty(),
        )?;
        register_optional_host_function(
            app,
            module_index,
            "selium",
            "hostcall_create",
            Box::new(HostcallCreateHostFunc {
                runtime: self.clone(),
                process_id,
            }),
            FunctionType::new(
                vec![ValType::Num(NumType::I32), ValType::Num(NumType::I32)],
                vec![ValType::Num(NumType::I64)],
            ),
        )?;
        register_optional_host_function(
            app,
            module_index,
            "selium",
            "hostcall_poll",
            Box::new(HostcallPollHostFunc {
                runtime: self.clone(),
                process_id,
            }),
            FunctionType::new(
                vec![
                    ValType::Num(NumType::I64),
                    ValType::Num(NumType::I32),
                    ValType::Num(NumType::I32),
                ],
                vec![ValType::Num(NumType::I64)],
            ),
        )?;
        register_optional_host_function(
            app,
            module_index,
            "selium",
            "hostcall_drop",
            Box::new(HostcallDropHostFunc {
                runtime: self.clone(),
                process_id,
            }),
            FunctionType::new(
                vec![ValType::Num(NumType::I64)],
                vec![ValType::Num(NumType::I32)],
            ),
        )?;
        Ok(())
    }

    fn execute_entrypoint(
        &self,
        mut loaded_guest: LoadedGuest,
        descriptor: &SystemGuestDescriptor,
    ) -> Result<LoadedGuest> {
        let arguments = decode_wasm_arguments(&descriptor.arguments)?;
        let results = loaded_guest
            .app
            .call_function(
                loaded_guest.module_index,
                descriptor.entrypoint.as_str(),
                &arguments,
            )
            .map_err(map_wasm_error)?;
        loaded_guest.entrypoint_results = results;
        Ok(loaded_guest)
    }

    fn claim_signal(&self, process_id: ProcessId, descriptor: SignalDescriptor) {
        self.claim_shared_resource(process_id, ResourceClass::Signal, descriptor.shared_id);
        self.claim_local_handle(process_id, ResourceClass::Signal, descriptor.local_id);
    }

    fn claim_local_handle(
        &self,
        process_id: ProcessId,
        resource_class: ResourceClass,
        local_id: u64,
    ) {
        self.local_handle_owners
            .lock()
            .entry((resource_class, local_id))
            .or_default()
            .insert(process_id);
    }

    fn claim_shared_resource(
        &self,
        process_id: ProcessId,
        resource_class: ResourceClass,
        shared_id: u64,
    ) {
        self.shared_resource_owners
            .lock()
            .entry((resource_class, shared_id))
            .or_default()
            .insert(process_id);
    }

    fn release_local_handle(
        &self,
        process_id: ProcessId,
        resource_class: &ResourceClass,
        local_id: u64,
    ) -> bool {
        let mut local_handle_owners = self.local_handle_owners.lock();
        let Some(owners) = local_handle_owners.get_mut(&(resource_class.clone(), local_id)) else {
            return false;
        };
        owners.remove(&process_id);
        let should_reclaim = owners.is_empty();
        if should_reclaim {
            local_handle_owners.remove(&(resource_class.clone(), local_id));
        }
        should_reclaim
    }

    fn release_shared_resource(
        &self,
        process_id: ProcessId,
        resource_class: &ResourceClass,
        shared_id: u64,
    ) -> bool {
        let mut shared_resource_owners = self.shared_resource_owners.lock();
        let Some(owners) = shared_resource_owners.get_mut(&(resource_class.clone(), shared_id))
        else {
            return false;
        };
        owners.remove(&process_id);
        let should_reclaim = owners.is_empty();
        if should_reclaim {
            shared_resource_owners.remove(&(resource_class.clone(), shared_id));
        }
        should_reclaim
    }

    fn ensure_local_handle_owner(
        &self,
        process_id: ProcessId,
        capability: Capability,
        resource_class: ResourceClass,
        local_id: u64,
    ) -> std::result::Result<(), AbiError> {
        if self
            .local_handle_owners
            .lock()
            .get(&(resource_class, local_id))
            .is_some_and(|owners| owners.contains(&process_id))
        {
            Ok(())
        } else {
            Err(AbiError::new(
                AbiErrorCode::PermissionDenied,
                format!("permission denied for capability {capability:?}"),
            ))
        }
    }

    fn require(
        &self,
        process_id: ProcessId,
        capability: Capability,
        resource_class: ResourceClass,
        resource_id: Option<ResourceIdentity>,
    ) -> std::result::Result<(), AbiError> {
        let allowed = self.authorises(
            process_id,
            capability.clone(),
            &ScopeContext {
                locality: LocalityScope::Cluster,
                resource_class: Some(resource_class),
                resource_id,
                ..ScopeContext::default()
            },
        );
        if allowed {
            Ok(())
        } else {
            Err(AbiError::new(
                AbiErrorCode::PermissionDenied,
                format!("permission denied for capability {capability:?}"),
            ))
        }
    }

    fn next_operation_id(&self, operations: &HashMap<OperationId, HostOperation>) -> OperationId {
        let mut next_operation_id = self.next_operation_id.lock();
        let first_candidate = *next_operation_id;
        loop {
            let operation_id = *next_operation_id;
            *next_operation_id = operation_id.checked_add(1).unwrap_or(1);
            if operation_id != 0 && !operations.contains_key(&operation_id) {
                return operation_id;
            }
            if *next_operation_id == first_candidate {
                panic!("operation id space exhausted");
            }
        }
    }
}

impl Default for Runtime {
    fn default() -> Self {
        Self::new(Kernel::default())
    }
}

impl HostFunc for MarkReadyHostFunc {
    fn call(
        &self,
        _store: &mut Store,
        _args: &[WasmValue],
    ) -> wasmtiny::runtime::Result<Vec<WasmValue>> {
        self.runtime.kernel.record_activity(ActivityEvent {
            kind: selium_abi::ActivityKind::GuestReady,
            process_id: Some(self.process_id),
            message: "guest ready".to_string(),
        });
        Ok(Vec::new())
    }

    fn function_type(&self) -> Option<&FunctionType> {
        None
    }
}

impl HostFunc for ProcessIdHostFunc {
    fn call(
        &self,
        _store: &mut Store,
        _args: &[WasmValue],
    ) -> wasmtiny::runtime::Result<Vec<WasmValue>> {
        Ok(vec![WasmValue::I64(self.process_id as i64)])
    }

    fn function_type(&self) -> Option<&FunctionType> {
        None
    }
}

impl HostFunc for HostcallCreateHostFunc {
    fn call(
        &self,
        store: &mut Store,
        args: &[WasmValue],
    ) -> wasmtiny::runtime::Result<Vec<WasmValue>> {
        let ptr = wasm_i32_arg(args, 0)? as u32;
        let len = wasm_i32_arg(args, 1)? as usize;
        let request_bytes = read_guest_memory(store, ptr, len)?;
        let request = match selium_abi::decode_rkyv::<HostcallRequest>(&request_bytes) {
            Ok(request) => request,
            Err(_) => {
                let status = pack_hostcall_status(selium_abi::HOSTCALL_STATUS_FAILED, 0);
                return Ok(vec![WasmValue::I64(status as i64)]);
            }
        };
        let (status, operation_id) = self.runtime.begin_hostcall(self.process_id, request);
        Ok(vec![WasmValue::I64(
            pack_hostcall_status(status, operation_id as u32) as i64,
        )])
    }

    fn function_type(&self) -> Option<&FunctionType> {
        None
    }
}

impl HostFunc for HostcallPollHostFunc {
    fn call(
        &self,
        store: &mut Store,
        args: &[WasmValue],
    ) -> wasmtiny::runtime::Result<Vec<WasmValue>> {
        let operation_id = wasm_i64_arg(args, 0)? as OperationId;
        let out_ptr = wasm_i32_arg(args, 1)? as u32;
        let out_capacity = wasm_i32_arg(args, 2)? as usize;
        let state = self.runtime.poll_hostcall(self.process_id, operation_id);
        let status = match state {
            CompletionState::Ready(_) => selium_abi::HOSTCALL_STATUS_READY,
            CompletionState::Pending { .. } => selium_abi::HOSTCALL_STATUS_PENDING,
            CompletionState::Failed(_) => selium_abi::HOSTCALL_STATUS_FAILED,
        };
        let encoded = selium_abi::encode_rkyv(&state)
            .map_err(|error| WasmError::Runtime(error.to_string()))?;
        if encoded.len() > out_capacity {
            return Ok(vec![WasmValue::I64(pack_hostcall_status(
                selium_abi::HOSTCALL_STATUS_OUTPUT_TOO_SMALL,
                encoded.len() as u32,
            ) as i64)]);
        }
        write_guest_memory(store, out_ptr, &encoded)?;
        Ok(vec![WasmValue::I64(
            pack_hostcall_status(status, encoded.len() as u32) as i64,
        )])
    }

    fn function_type(&self) -> Option<&FunctionType> {
        None
    }
}

impl HostFunc for HostcallDropHostFunc {
    fn call(
        &self,
        _store: &mut Store,
        args: &[WasmValue],
    ) -> wasmtiny::runtime::Result<Vec<WasmValue>> {
        let operation_id = wasm_i64_arg(args, 0)? as OperationId;
        let dropped = self.runtime.drop_hostcall(self.process_id, operation_id);
        Ok(vec![WasmValue::I32(u32::from(dropped) as i32)])
    }

    fn function_type(&self) -> Option<&FunctionType> {
        None
    }
}

fn decode_wasm_arguments(arguments: &[Vec<u8>]) -> Result<Vec<WasmValue>> {
    arguments
        .iter()
        .map(|argument| {
            let Some((value, used)) = WasmValue::from_bytes(argument) else {
                return Err(Error::InvalidEntrypointArgument);
            };
            if used != argument.len() {
                return Err(Error::InvalidEntrypointArgument);
            }
            Ok(value)
        })
        .collect()
}

fn register_optional_host_function(
    app: &mut WasmApplication,
    module_index: u32,
    import_module: &str,
    name: &str,
    func: Box<dyn HostFunc>,
    func_type: FunctionType,
) -> Result<()> {
    match app.register_host_function(module_index, import_module, name, func, func_type) {
        Ok(()) => Ok(()),
        Err(WasmError::Instantiate(message))
            if message == format!("import {import_module}.{name} not found") =>
        {
            Ok(())
        }
        Err(error) => Err(map_wasm_error(error)),
    }
}

fn read_guest_memory(store: &Store, ptr: u32, len: usize) -> wasmtiny::runtime::Result<Vec<u8>> {
    let memory = store
        .instances
        .first()
        .and_then(|instance| instance.memory(0))
        .cloned()
        .ok_or_else(|| WasmError::Runtime("guest module does not expose memory".to_string()))?;
    let mut bytes = vec![0; len];
    memory
        .lock()
        .map_err(|_| WasmError::Runtime("guest memory lock poisoned".to_string()))?
        .read(ptr, &mut bytes)?;
    Ok(bytes)
}

fn write_guest_memory(store: &Store, ptr: u32, bytes: &[u8]) -> wasmtiny::runtime::Result<()> {
    let memory = store
        .instances
        .first()
        .and_then(|instance| instance.memory(0))
        .cloned()
        .ok_or_else(|| WasmError::Runtime("guest module does not expose memory".to_string()))?;
    memory
        .lock()
        .map_err(|_| WasmError::Runtime("guest memory lock poisoned".to_string()))?
        .write(ptr, bytes)
}

fn wasm_i32_arg(args: &[WasmValue], index: usize) -> wasmtiny::runtime::Result<i32> {
    args.get(index)
        .ok_or_else(|| WasmError::Runtime(format!("missing argument {index}")))?
        .i32()
}

fn wasm_i64_arg(args: &[WasmValue], index: usize) -> wasmtiny::runtime::Result<i64> {
    args.get(index)
        .ok_or_else(|| WasmError::Runtime(format!("missing argument {index}")))?
        .i64()
}

fn map_wasm_error(error: WasmError) -> Error {
    Error::Wasm(error.to_string())
}

fn kernel_error(error: selium_kernel::Error) -> AbiError {
    let code = match error {
        selium_kernel::Error::NotFound(_) => AbiErrorCode::NotFound,
        selium_kernel::Error::Timeout => AbiErrorCode::Timeout,
        selium_kernel::Error::AlreadyCompleted
        | selium_kernel::Error::ProcessStopped(_)
        | selium_kernel::Error::Wasm(_) => AbiErrorCode::Internal,
    };
    AbiError::new(code, error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use selium_abi::{MeteringObservation, ResourceSelector};

    fn module_with_entrypoint(entrypoint: &str, body: &str) -> Vec<u8> {
        wat::parse_str(format!("(module (func (export \"{entrypoint}\") {body}))"))
            .expect("compile wat")
    }

    fn module_with_runtime_bridge(entrypoint: &str) -> Vec<u8> {
        wat::parse_str(format!(
            "(module
                (import \"selium\" \"process_id\" (func $process_id (result i64)))
                (import \"selium\" \"mark_ready\" (func $mark_ready))
                (func (export \"{entrypoint}\") (result i64)
                    call $mark_ready
                    call $process_id))"
        ))
        .expect("compile runtime bridge wat")
    }

    #[test]
    fn runtime_bootstraps_guests_from_config() {
        let runtime = Runtime::default();
        let config = RuntimeConfig {
            system_guests: vec![SystemGuestDescriptor {
                name: "cluster".to_string(),
                module_id: "cluster-module".to_string(),
                module_bytes: module_with_entrypoint("boot", "(result i32) i32.const 7"),
                entrypoint: "boot".to_string(),
                arguments: Vec::new(),
                grants: vec![CapabilityGrant::new(
                    Capability::ProcessLifecycle,
                    vec![ResourceSelector::Locality(LocalityScope::Cluster)],
                )],
                dependencies: Vec::new(),
                readiness: ReadinessCondition::Immediate,
            }],
        };

        let report = runtime
            .bootstrap_system_guests(config)
            .expect("bootstrap guests");
        assert_eq!(report.guests.len(), 1);
        assert_eq!(runtime.loaded_guest_count(), 1);
        assert_eq!(
            runtime
                .entrypoint_results(report.guests[0].process_id)
                .expect("entrypoint results"),
            vec![WasmValue::I32(7)]
        );
    }

    #[test]
    fn runtime_registers_host_import_bridge_for_guest_modules() {
        let runtime = Runtime::default();
        let bootstrapped = runtime
            .spawn_system_guest(SystemGuestDescriptor {
                name: "bridged".to_string(),
                module_id: "bridged-module".to_string(),
                module_bytes: module_with_runtime_bridge("boot"),
                entrypoint: "boot".to_string(),
                arguments: Vec::new(),
                grants: vec![CapabilityGrant::new(
                    Capability::ActivityRead,
                    vec![ResourceSelector::Locality(LocalityScope::Cluster)],
                )],
                dependencies: Vec::new(),
                readiness: ReadinessCondition::ActivityLogContains("guest ready".to_string()),
            })
            .expect("spawn bridged guest");

        let results = runtime
            .entrypoint_results(bootstrapped.process_id)
            .expect("entrypoint results");
        assert_eq!(
            results,
            vec![WasmValue::I64(bootstrapped.process_id as i64)]
        );
    }

    #[test]
    fn hostcall_signal_vertical_slice_uses_operation_table() {
        let runtime = Runtime::default();
        let bootstrapped = runtime
            .spawn_system_guest(SystemGuestDescriptor {
                name: "signals".to_string(),
                module_id: "signals-module".to_string(),
                module_bytes: module_with_entrypoint("boot", ""),
                entrypoint: "boot".to_string(),
                arguments: Vec::new(),
                grants: vec![CapabilityGrant::new(
                    Capability::Signal,
                    vec![ResourceSelector::ResourceClass(ResourceClass::Signal)],
                )],
                dependencies: Vec::new(),
                readiness: ReadinessCondition::Immediate,
            })
            .expect("spawn signals guest");

        let (status, create_id) =
            runtime.begin_hostcall(bootstrapped.process_id, HostcallRequest::SignalCreate);
        assert_eq!(status, selium_abi::HOSTCALL_STATUS_READY);
        let CompletionState::Ready(HostcallOutput::Signal(signal)) =
            runtime.poll_hostcall(bootstrapped.process_id, create_id)
        else {
            panic!("expected created signal");
        };

        let (status, wait_id) = runtime.begin_hostcall(
            bootstrapped.process_id,
            HostcallRequest::SignalWait {
                local_id: signal.local_id,
                observed_generation: 0,
                timeout_ms: 1_000,
            },
        );
        assert_eq!(status, selium_abi::HOSTCALL_STATUS_PENDING);
        assert!(matches!(
            runtime.poll_hostcall(bootstrapped.process_id, wait_id),
            CompletionState::Pending { .. }
        ));

        runtime.begin_hostcall(
            bootstrapped.process_id,
            HostcallRequest::SignalNotify {
                local_id: signal.local_id,
            },
        );
        assert_eq!(
            runtime.poll_hostcall(bootstrapped.process_id, wait_id),
            CompletionState::Ready(HostcallOutput::SignalGeneration(1))
        );
    }

    #[test]
    fn operation_ids_roll_over_without_saturating() {
        let runtime = Runtime::default();
        let bootstrapped = runtime
            .spawn_system_guest(SystemGuestDescriptor {
                name: "rollover".to_string(),
                module_id: "rollover-module".to_string(),
                module_bytes: module_with_entrypoint("boot", ""),
                entrypoint: "boot".to_string(),
                arguments: Vec::new(),
                grants: vec![CapabilityGrant::new(
                    Capability::Signal,
                    vec![ResourceSelector::ResourceClass(ResourceClass::Signal)],
                )],
                dependencies: Vec::new(),
                readiness: ReadinessCondition::Immediate,
            })
            .expect("spawn rollover guest");
        *runtime.next_operation_id.lock() = OperationId::MAX;

        let (first_status, first_id) =
            runtime.begin_hostcall(bootstrapped.process_id, HostcallRequest::SignalCreate);
        let (second_status, second_id) =
            runtime.begin_hostcall(bootstrapped.process_id, HostcallRequest::SignalCreate);

        assert_eq!(first_status, selium_abi::HOSTCALL_STATUS_READY);
        assert_eq!(second_status, selium_abi::HOSTCALL_STATUS_READY);
        assert_eq!(first_id, OperationId::MAX);
        assert_eq!(second_id, 1);
    }

    #[test]
    fn activity_log_and_metering_are_projected() {
        let runtime = Runtime::default();
        let bootstrapped = runtime
            .spawn_system_guest(SystemGuestDescriptor {
                name: "discovery".to_string(),
                module_id: "discovery-module".to_string(),
                module_bytes: module_with_entrypoint("main", ""),
                entrypoint: "main".to_string(),
                arguments: Vec::new(),
                grants: vec![CapabilityGrant::new(
                    Capability::ActivityRead,
                    vec![ResourceSelector::Locality(LocalityScope::Cluster)],
                )],
                dependencies: Vec::new(),
                readiness: ReadinessCondition::Immediate,
            })
            .expect("spawn guest");
        runtime.project_metering(
            bootstrapped.process_id,
            MeteringObservation {
                cpu_micros: 11,
                memory_bytes: 22,
                storage_bytes: 33,
                bandwidth_bytes: 44,
            },
        );

        assert!(
            runtime
                .activity_log()
                .iter()
                .any(|event| event.message.contains("bootstrapped"))
        );
        assert_eq!(
            runtime
                .kernel()
                .metering_observation(bootstrapped.process_id)
                .expect("metering")
                .cpu_micros,
            11
        );
    }
}
