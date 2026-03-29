//! Selium runtime built on top of Wasmtiny and the Selium kernel.

use parking_lot::Mutex;
use selium_abi::{
    ActivityEvent, BlobStoreDescriptor, Capability, CapabilityGrant, DurableLogDescriptor,
    EntrypointMetadata, GuestHost, GuestLogEntry, HostError, HostFuture, HostResult, LocalityScope,
    MeteringObservation, NetworkListenerDescriptor, NetworkSessionDescriptor,
    NetworkStreamDescriptor, ProcessDescriptor, ProcessId, ResourceClass, ResourceIdentity,
    ResourceSelector, ScopeContext, SessionId, SharedMappingDescriptor, SharedRegionDescriptor,
    SignalDescriptor, StorageRecord,
};
use selium_kernel::Kernel;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use thiserror::Error;
use tracing::info;
use wasmtiny::runtime::{HostFunc, Store};
use wasmtiny::{FunctionType, NumType, ValType, WasmApplication, WasmError, WasmValue};

#[derive(Debug, Error)]
pub enum Error {
    #[error("system guest descriptor not found: {0}")]
    DescriptorNotFound(String),
    #[error("unknown session: {0}")]
    UnknownSession(SessionId),
    #[error("unknown dependency: {0}")]
    UnknownDependency(String),
    #[error("dependency cycle or unresolved dependency detected")]
    DependencyCycle,
    #[error("invalid grant for capability {0:?}")]
    InvalidGrant(Capability),
    #[error("child grant exceeds parent authority for capability {0:?}")]
    GrantEscalation(Capability),
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

pub type Result<T> = std::result::Result<T, Error>;

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

#[derive(Debug, Clone, Default)]
pub struct RuntimeConfig {
    pub system_guests: Vec<SystemGuestDescriptor>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadinessCondition {
    Immediate,
    ActivityLogContains(String),
}

#[derive(Debug, Clone)]
pub struct BootstrappedGuest {
    pub name: String,
    pub process_id: ProcessId,
    pub session_id: SessionId,
}

#[derive(Debug, Clone, Default)]
pub struct BootstrapReport {
    pub guests: Vec<BootstrappedGuest>,
}

#[derive(Debug, Clone)]
pub struct SessionRecord {
    pub grants: Vec<CapabilityGrant>,
}

struct LoadedGuest {
    app: WasmApplication,
    module_index: u32,
    entrypoint_results: Vec<WasmValue>,
}

type LocalHandleOwners = HashMap<(ResourceClass, u64), BTreeSet<SessionId>>;
type SharedResourceOwners = HashMap<(ResourceClass, u64), BTreeSet<SessionId>>;

struct SessionIdHostFunc {
    session_id: SessionId,
}

impl HostFunc for SessionIdHostFunc {
    fn call(
        &self,
        _store: &mut Store,
        _args: &[WasmValue],
    ) -> wasmtiny::runtime::Result<Vec<WasmValue>> {
        Ok(vec![WasmValue::I64(self.session_id as i64)])
    }

    fn function_type(&self) -> Option<&FunctionType> {
        None
    }
}

struct ProcessIdHostFunc {
    process_id: ProcessId,
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

struct MarkReadyHostFunc {
    runtime: Runtime,
    process_id: ProcessId,
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

pub struct Runtime {
    kernel: Kernel,
    sessions: Arc<Mutex<HashMap<SessionId, SessionRecord>>>,
    loaded_guests: Arc<Mutex<HashMap<ProcessId, LoadedGuest>>>,
    process_sessions: Arc<Mutex<HashMap<ProcessId, SessionId>>>,
    local_handle_owners: Arc<Mutex<LocalHandleOwners>>,
    shared_resource_owners: Arc<Mutex<SharedResourceOwners>>,
    pending_shared_region_reclaims: Arc<Mutex<BTreeSet<u64>>>,
    exchange_owners: Arc<Mutex<HashMap<u64, SessionId>>>,
    module_registry: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    next_session_id: Arc<Mutex<SessionId>>,
}

#[derive(Clone)]
pub struct RuntimeGuestHost {
    runtime: Runtime,
    session_id: SessionId,
    scope_context: ScopeContext,
}

const DEFAULT_READINESS_TIMEOUT_MS: u64 = 1_000;
const DEFAULT_READINESS_POLL_MS: u64 = 10;

impl Default for Runtime {
    fn default() -> Self {
        Self::new(Kernel::default())
    }
}

impl Clone for Runtime {
    fn clone(&self) -> Self {
        Self {
            kernel: self.kernel.clone(),
            sessions: self.sessions.clone(),
            loaded_guests: self.loaded_guests.clone(),
            process_sessions: self.process_sessions.clone(),
            local_handle_owners: self.local_handle_owners.clone(),
            shared_resource_owners: self.shared_resource_owners.clone(),
            pending_shared_region_reclaims: self.pending_shared_region_reclaims.clone(),
            exchange_owners: self.exchange_owners.clone(),
            module_registry: self.module_registry.clone(),
            next_session_id: self.next_session_id.clone(),
        }
    }
}

impl Runtime {
    pub fn new(kernel: Kernel) -> Self {
        Self {
            kernel,
            sessions: Arc::new(Mutex::new(HashMap::new())),
            loaded_guests: Arc::new(Mutex::new(HashMap::new())),
            process_sessions: Arc::new(Mutex::new(HashMap::new())),
            local_handle_owners: Arc::new(Mutex::new(HashMap::new())),
            shared_resource_owners: Arc::new(Mutex::new(HashMap::new())),
            pending_shared_region_reclaims: Arc::new(Mutex::new(BTreeSet::new())),
            exchange_owners: Arc::new(Mutex::new(HashMap::new())),
            module_registry: Arc::new(Mutex::new(HashMap::new())),
            next_session_id: Arc::new(Mutex::new(1)),
        }
    }

    pub fn kernel(&self) -> Kernel {
        self.kernel.clone()
    }

    fn claim_local_handle(
        &self,
        session_id: SessionId,
        resource_class: ResourceClass,
        local_id: u64,
    ) {
        self.local_handle_owners
            .lock()
            .entry((resource_class, local_id))
            .or_default()
            .insert(session_id);
    }

    fn claim_shared_resource(
        &self,
        session_id: SessionId,
        resource_class: ResourceClass,
        shared_id: u64,
    ) {
        self.shared_resource_owners
            .lock()
            .entry((resource_class, shared_id))
            .or_default()
            .insert(session_id);
    }

    fn local_handle_owned_by(
        &self,
        session_id: SessionId,
        resource_class: ResourceClass,
        local_id: u64,
    ) -> bool {
        self.local_handle_owners
            .lock()
            .get(&(resource_class, local_id))
            .is_some_and(|owners| owners.contains(&session_id))
    }

    fn shared_resource_owned_by(
        &self,
        session_id: SessionId,
        resource_class: ResourceClass,
        shared_id: u64,
    ) -> bool {
        self.shared_resource_owners
            .lock()
            .get(&(resource_class, shared_id))
            .is_some_and(|owners| owners.contains(&session_id))
    }

    fn release_local_handle(
        &self,
        session_id: SessionId,
        resource_class: &ResourceClass,
        local_id: u64,
    ) -> bool {
        let mut local_handle_owners = self.local_handle_owners.lock();
        let Some(owners) = local_handle_owners.get_mut(&(resource_class.clone(), local_id)) else {
            return false;
        };
        owners.remove(&session_id);
        let should_reclaim = owners.is_empty();
        if should_reclaim {
            local_handle_owners.remove(&(resource_class.clone(), local_id));
        }
        should_reclaim
    }

    fn release_shared_resource(
        &self,
        session_id: SessionId,
        resource_class: &ResourceClass,
        shared_id: u64,
    ) -> bool {
        let mut shared_resource_owners = self.shared_resource_owners.lock();
        let Some(owners) = shared_resource_owners.get_mut(&(resource_class.clone(), shared_id))
        else {
            return false;
        };
        owners.remove(&session_id);
        let should_reclaim = owners.is_empty();
        if should_reclaim {
            shared_resource_owners.remove(&(resource_class.clone(), shared_id));
        }
        should_reclaim
    }

    fn schedule_shared_region_reclaim(&self, shared_id: u64) {
        self.pending_shared_region_reclaims.lock().insert(shared_id);
    }

    fn try_reclaim_shared_region(&self, shared_id: u64) -> Result<()> {
        if self.kernel.shared_region_mapping_count(shared_id) > 0 {
            self.schedule_shared_region_reclaim(shared_id);
            return Ok(());
        }
        self.pending_shared_region_reclaims
            .lock()
            .remove(&shared_id);
        Ok(self.kernel.destroy_shared_region(shared_id)?)
    }

    pub fn guest_host(&self, session_id: SessionId) -> Result<RuntimeGuestHost> {
        if self.sessions.lock().contains_key(&session_id) {
            Ok(RuntimeGuestHost {
                runtime: self.clone(),
                session_id,
                scope_context: ScopeContext {
                    locality: LocalityScope::Cluster,
                    ..ScopeContext::default()
                },
            })
        } else {
            Err(Error::UnknownSession(session_id))
        }
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
                if descriptor
                    .dependencies
                    .iter()
                    .all(|dependency| ready.contains(dependency))
                {
                    Some(name.clone())
                } else {
                    None
                }
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
        let session_id = self.persist_session(descriptor.grants.clone());
        let process = self.kernel.start_process(
            descriptor.module_id.clone(),
            descriptor.entrypoint.clone(),
            descriptor.grants.clone(),
        );
        let loaded_guest =
            self.load_guest_module(&descriptor.module_bytes, session_id, process.local_id);
        let loaded_guest = match loaded_guest {
            Ok(loaded_guest) => loaded_guest,
            Err(error) => {
                self.cleanup_failed_process(process.local_id, session_id)?;
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
                self.cleanup_failed_process(process.local_id, session_id)?;
                return Err(error);
            }
        };
        self.loaded_guests
            .lock()
            .insert(process.local_id, loaded_guest);
        self.process_sessions
            .lock()
            .insert(process.local_id, session_id);
        self.claim_local_handle(session_id, ResourceClass::Process, process.local_id);
        if let Err(error) = self.register_module_bytes(
            descriptor.module_id.clone(),
            descriptor.module_bytes.clone(),
        ) {
            self.cleanup_failed_process(process.local_id, session_id)?;
            return Err(error);
        }
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
            session_id,
        })
    }

    pub fn stop_process(&self, process_id: ProcessId) -> Result<()> {
        self.kernel.stop_process(process_id)?;
        self.loaded_guests.lock().remove(&process_id);
        if let Some(session_id) = self.process_sessions.lock().remove(&process_id) {
            self.sessions.lock().remove(&session_id);
            self.exchange_owners
                .lock()
                .retain(|_, owner_session_id| *owner_session_id != session_id);
            self.cleanup_session_resources(session_id)?;
        }
        self.local_handle_owners
            .lock()
            .remove(&(ResourceClass::Process, process_id));
        self.kernel.reap_process(process_id)?;
        Ok(())
    }

    pub fn restore_session(&self, session_id: SessionId) -> Option<SessionRecord> {
        self.sessions.lock().get(&session_id).cloned()
    }

    pub fn authorises(
        &self,
        session_id: SessionId,
        capability: Capability,
        context: &ScopeContext,
    ) -> bool {
        self.sessions
            .lock()
            .get(&session_id)
            .map(|record| {
                record
                    .grants
                    .iter()
                    .any(|grant| grant.capability == capability && grant.allows(context))
            })
            .unwrap_or(false)
    }

    pub fn project_metering(&self, process_id: ProcessId, observation: MeteringObservation) {
        self.kernel.observe_metering(process_id, observation);
    }

    pub fn activity_log(&self) -> Vec<ActivityEvent> {
        self.kernel.read_activity_from(0)
    }

    pub fn loaded_entrypoint(&self, process_id: ProcessId) -> Option<u32> {
        self.loaded_guests
            .lock()
            .get(&process_id)
            .map(|guest| guest.module_index)
    }

    pub fn entrypoint_results(&self, process_id: ProcessId) -> Option<Vec<WasmValue>> {
        self.loaded_guests
            .lock()
            .get(&process_id)
            .map(|guest| guest.entrypoint_results.clone())
    }

    pub fn loaded_guest_count(&self) -> usize {
        self.loaded_guests.lock().len()
    }

    fn wait_for_readiness(&self, process_id: ProcessId, condition: &ReadinessCondition) -> bool {
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

    fn persist_session(&self, grants: Vec<CapabilityGrant>) -> SessionId {
        let mut next_session_id = self.next_session_id.lock();
        let session_id = *next_session_id;
        *next_session_id += 1;
        self.sessions
            .lock()
            .insert(session_id, SessionRecord { grants });
        session_id
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

    fn module_bytes(&self, module_id: &str) -> Result<Vec<u8>> {
        self.module_registry
            .lock()
            .get(module_id)
            .cloned()
            .ok_or_else(|| Error::UnknownModule(module_id.to_string()))
    }

    fn rollback_bootstrapped(&self, report: &BootstrapReport) {
        for guest in report.guests.iter().rev() {
            let _ = self.stop_process(guest.process_id);
        }
    }

    fn cleanup_failed_process(&self, process_id: ProcessId, session_id: SessionId) -> Result<()> {
        let _ = self.kernel.stop_process(process_id);
        self.exchange_owners
            .lock()
            .retain(|_, owner_session_id| *owner_session_id != session_id);
        let _ = self.cleanup_session_resources(session_id);
        let _ = self.kernel.reap_process(process_id);
        self.sessions.lock().remove(&session_id);
        self.process_sessions.lock().remove(&process_id);
        self.local_handle_owners
            .lock()
            .remove(&(ResourceClass::Process, process_id));
        self.shared_resource_owners
            .lock()
            .retain(|_, owners| !owners.contains(&session_id));
        Ok(())
    }

    fn cleanup_session_resources(&self, session_id: SessionId) -> Result<()> {
        let owned_handles = self
            .local_handle_owners
            .lock()
            .iter()
            .filter_map(|((resource_class, local_id), owners)| {
                owners
                    .contains(&session_id)
                    .then_some((resource_class.clone(), *local_id))
            })
            .collect::<Vec<_>>();

        for (resource_class, local_id) in owned_handles {
            let should_reclaim = self.release_local_handle(session_id, &resource_class, local_id);
            if !should_reclaim {
                continue;
            }
            match resource_class {
                ResourceClass::SharedMapping => self.kernel.detach_shared_region(local_id)?,
                ResourceClass::Signal => self.kernel.close_signal(local_id)?,
                ResourceClass::Listener => self.kernel.close_listener(local_id)?,
                ResourceClass::Session => self.kernel.close_session(local_id)?,
                ResourceClass::Stream => self.kernel.close_stream(local_id)?,
                ResourceClass::DurableLog => self.kernel.close_log(local_id)?,
                ResourceClass::BlobStore => self.kernel.close_blob_store(local_id)?,
                ResourceClass::Process => {}
                _ => {}
            }
        }

        let owned_shared_resources = self
            .shared_resource_owners
            .lock()
            .iter()
            .filter_map(|((resource_class, shared_id), owners)| {
                owners
                    .contains(&session_id)
                    .then_some((resource_class.clone(), *shared_id))
            })
            .collect::<Vec<_>>();

        for (resource_class, shared_id) in owned_shared_resources {
            let should_reclaim =
                self.release_shared_resource(session_id, &resource_class, shared_id);
            if !should_reclaim {
                continue;
            }
            match resource_class {
                ResourceClass::SharedRegion => self.try_reclaim_shared_region(shared_id)?,
                ResourceClass::Signal => {}
                _ => {}
            }
        }

        Ok(())
    }

    fn validate_grants(&self, grants: &[CapabilityGrant]) -> Result<()> {
        for grant in grants {
            if grant.selectors.is_empty() {
                return Err(Error::InvalidGrant(grant.capability.clone()));
            }
        }
        Ok(())
    }

    fn session_grants(&self, session_id: SessionId) -> Result<Vec<CapabilityGrant>> {
        self.sessions
            .lock()
            .get(&session_id)
            .map(|record| record.grants.clone())
            .ok_or(Error::UnknownSession(session_id))
    }

    fn load_guest_module(
        &self,
        module_bytes: &[u8],
        session_id: SessionId,
        process_id: ProcessId,
    ) -> Result<LoadedGuest> {
        let mut app = WasmApplication::new();
        let module_index = app
            .load_module_from_memory(module_bytes)
            .map_err(map_wasm_error)?;
        self.register_runtime_host_functions(&mut app, module_index, session_id, process_id)?;
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
        session_id: SessionId,
        process_id: ProcessId,
    ) -> Result<()> {
        let runtime = self.clone();
        register_optional_host_function(
            app,
            module_index,
            "selium",
            "session_id",
            Box::new(SessionIdHostFunc { session_id }),
            FunctionType::new(vec![], vec![ValType::Num(NumType::I64)]),
        )?;

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
                runtime,
                process_id,
            }),
            FunctionType::empty(),
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

fn parent_grant_covers_child(parent: &CapabilityGrant, child: &CapabilityGrant) -> bool {
    if parent.capability != child.capability {
        return false;
    }

    parent.selectors.iter().all(|parent_selector| match parent_selector {
        ResourceSelector::Tenant(parent_tenant) => child.selectors.iter().any(|selector| {
            matches!(selector, ResourceSelector::Tenant(child_tenant) if child_tenant == parent_tenant)
        }),
        ResourceSelector::UriPrefix(parent_prefix) => child.selectors.iter().any(|selector| {
            matches!(selector, ResourceSelector::UriPrefix(child_prefix) if child_prefix.starts_with(parent_prefix))
        }),
        ResourceSelector::Locality(parent_locality) => child.selectors.iter().any(|selector| {
            matches!(selector, ResourceSelector::Locality(child_locality) if parent_locality.matches(child_locality))
        }),
        ResourceSelector::ResourceClass(parent_class) => child.selectors.iter().any(|selector| {
            matches!(selector, ResourceSelector::ResourceClass(child_class) if child_class == parent_class)
        }),
        ResourceSelector::ExplicitResource(parent_identity) => child.selectors.iter().any(|selector| {
            matches!(selector, ResourceSelector::ExplicitResource(child_identity) if child_identity == parent_identity)
        }),
    })
}

fn map_wasm_error(error: WasmError) -> Error {
    Error::Wasm(error.to_string())
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

impl RuntimeGuestHost {
    fn require(
        &self,
        capability: Capability,
        resource_class: ResourceClass,
        resource_id: Option<ResourceIdentity>,
    ) -> HostResult<()> {
        let allowed = self.runtime.authorises(
            self.session_id,
            capability.clone(),
            &ScopeContext {
                tenant: self.scope_context.tenant.clone(),
                uri: self.scope_context.uri.clone(),
                locality: self.scope_context.locality.clone(),
                resource_class: Some(resource_class),
                resource_id,
            },
        );
        if allowed {
            Ok(())
        } else {
            Err(HostError::PermissionDenied(capability))
        }
    }

    fn host_error(error: selium_kernel::Error) -> HostError {
        HostError::Host(error.to_string())
    }

    fn ensure_local_handle_owner(
        &self,
        capability: Capability,
        resource_class: ResourceClass,
        local_id: u64,
    ) -> HostResult<()> {
        if self
            .runtime
            .local_handle_owned_by(self.session_id, resource_class, local_id)
        {
            Ok(())
        } else {
            Err(HostError::PermissionDenied(capability))
        }
    }

    fn ensure_shared_resource_owner(
        &self,
        capability: Capability,
        resource_class: ResourceClass,
        shared_id: u64,
    ) -> HostResult<()> {
        if self
            .runtime
            .shared_resource_owned_by(self.session_id, resource_class, shared_id)
        {
            Ok(())
        } else {
            Err(HostError::PermissionDenied(capability))
        }
    }

    fn namespaced_name(&self, name: &str) -> String {
        let tenant = self.scope_context.tenant.as_deref().unwrap_or("_");
        let uri = self.scope_context.uri.as_deref().unwrap_or("_");
        let locality = match &self.scope_context.locality {
            LocalityScope::Any => "any".to_string(),
            LocalityScope::Cluster => "cluster".to_string(),
            LocalityScope::Host(host) => format!("host:{host}"),
        };
        format!("{tenant}|{uri}|{locality}|{name}")
    }
}

impl GuestHost for RuntimeGuestHost {
    fn scoped(&self, scope_context: ScopeContext) -> Arc<dyn GuestHost> {
        Arc::new(Self {
            runtime: self.runtime.clone(),
            session_id: self.session_id,
            scope_context: ScopeContext {
                tenant: scope_context
                    .tenant
                    .or_else(|| self.scope_context.tenant.clone()),
                uri: scope_context.uri.or_else(|| self.scope_context.uri.clone()),
                locality: scope_context.locality,
                resource_class: scope_context.resource_class,
                resource_id: scope_context.resource_id,
            },
        })
    }

    fn authorises(&self, capability: Capability, scope_context: &ScopeContext) -> HostResult<bool> {
        Ok(self
            .runtime
            .authorises(self.session_id, capability, scope_context))
    }

    fn allocate_shared_region(
        &self,
        size: u32,
        alignment: u32,
    ) -> HostResult<SharedRegionDescriptor> {
        self.require(Capability::SharedMemory, ResourceClass::SharedRegion, None)?;
        let descriptor = self
            .runtime
            .kernel
            .allocate_shared_region(size, alignment)
            .map_err(Self::host_error)?;
        self.runtime.claim_shared_resource(
            self.session_id,
            ResourceClass::SharedRegion,
            descriptor.shared_id,
        );
        Ok(descriptor)
    }

    fn destroy_shared_region(&self, shared_id: u64) -> HostResult<()> {
        self.ensure_shared_resource_owner(
            Capability::SharedMemory,
            ResourceClass::SharedRegion,
            shared_id,
        )?;
        if self.runtime.kernel.shared_region_mapping_count(shared_id) > 0 {
            return Err(HostError::Host(
                "shared region still has attached mappings".to_string(),
            ));
        }
        self.runtime
            .kernel
            .destroy_shared_region(shared_id)
            .map_err(Self::host_error)?;
        self.runtime.release_shared_resource(
            self.session_id,
            &ResourceClass::SharedRegion,
            shared_id,
        );
        Ok(())
    }

    fn attach_shared_region(
        &self,
        shared_id: u64,
        offset: u32,
        len: u32,
    ) -> HostResult<SharedMappingDescriptor> {
        self.require(
            Capability::SharedMemory,
            ResourceClass::SharedMapping,
            Some(ResourceIdentity::Shared(shared_id)),
        )?;
        let descriptor = self
            .runtime
            .kernel
            .attach_shared_region(shared_id, offset, len)
            .map_err(Self::host_error)?;
        self.runtime.claim_local_handle(
            self.session_id,
            ResourceClass::SharedMapping,
            descriptor.local_id,
        );
        Ok(descriptor)
    }

    fn detach_shared_region(&self, local_id: u64) -> HostResult<()> {
        self.ensure_local_handle_owner(
            Capability::SharedMemory,
            ResourceClass::SharedMapping,
            local_id,
        )?;
        let shared_id = self
            .runtime
            .kernel
            .shared_mapping_shared_id(local_id)
            .map_err(Self::host_error)?;
        self.require(
            Capability::SharedMemory,
            ResourceClass::SharedMapping,
            Some(ResourceIdentity::Shared(shared_id)),
        )?;
        self.runtime
            .kernel
            .detach_shared_region(local_id)
            .map_err(Self::host_error)?;
        self.runtime
            .release_local_handle(self.session_id, &ResourceClass::SharedMapping, local_id);
        if self
            .runtime
            .pending_shared_region_reclaims
            .lock()
            .contains(&shared_id)
        {
            self.runtime
                .try_reclaim_shared_region(shared_id)
                .map_err(|error| HostError::Host(error.to_string()))?;
        }
        Ok(())
    }

    fn read_shared_memory(&self, local_id: u64, offset: u32, len: usize) -> HostResult<Vec<u8>> {
        self.ensure_local_handle_owner(
            Capability::SharedMemory,
            ResourceClass::SharedMapping,
            local_id,
        )?;
        let shared_id = self
            .runtime
            .kernel
            .shared_mapping_shared_id(local_id)
            .map_err(Self::host_error)?;
        self.require(
            Capability::SharedMemory,
            ResourceClass::SharedMapping,
            Some(ResourceIdentity::Shared(shared_id)),
        )?;
        self.runtime
            .kernel
            .read_shared_memory(local_id, offset, len)
            .map_err(Self::host_error)
    }

    fn write_shared_memory(&self, local_id: u64, offset: u32, bytes: &[u8]) -> HostResult<()> {
        self.ensure_local_handle_owner(
            Capability::SharedMemory,
            ResourceClass::SharedMapping,
            local_id,
        )?;
        let shared_id = self
            .runtime
            .kernel
            .shared_mapping_shared_id(local_id)
            .map_err(Self::host_error)?;
        self.require(
            Capability::SharedMemory,
            ResourceClass::SharedMapping,
            Some(ResourceIdentity::Shared(shared_id)),
        )?;
        self.runtime
            .kernel
            .write_shared_memory(local_id, offset, bytes)
            .map_err(Self::host_error)
    }

    fn create_signal(&self) -> HostResult<SignalDescriptor> {
        self.require(Capability::Signal, ResourceClass::Signal, None)?;
        let descriptor = self.runtime.kernel.create_signal();
        self.runtime.claim_shared_resource(
            self.session_id,
            ResourceClass::Signal,
            descriptor.shared_id,
        );
        self.runtime.claim_local_handle(
            self.session_id,
            ResourceClass::Signal,
            descriptor.local_id,
        );
        Ok(descriptor)
    }

    fn attach_signal(&self, shared_id: u64) -> HostResult<SignalDescriptor> {
        self.require(
            Capability::Signal,
            ResourceClass::Signal,
            Some(ResourceIdentity::Shared(shared_id)),
        )?;
        let descriptor = self
            .runtime
            .kernel
            .attach_signal(shared_id)
            .map_err(Self::host_error)?;
        self.runtime.claim_local_handle(
            self.session_id,
            ResourceClass::Signal,
            descriptor.local_id,
        );
        Ok(descriptor)
    }

    fn close_signal(&self, local_id: u64) -> HostResult<()> {
        self.ensure_local_handle_owner(Capability::Signal, ResourceClass::Signal, local_id)?;
        let shared_id = self
            .runtime
            .kernel
            .signal_shared_id(local_id)
            .map_err(Self::host_error)?;
        self.runtime
            .kernel
            .close_signal(local_id)
            .map_err(Self::host_error)?;
        self.runtime
            .release_local_handle(self.session_id, &ResourceClass::Signal, local_id);
        if self.runtime.kernel.signal_handle_count(shared_id) == 0 {
            self.runtime.release_shared_resource(
                self.session_id,
                &ResourceClass::Signal,
                shared_id,
            );
        }
        Ok(())
    }

    fn notify_signal(&self, local_id: u64) -> HostResult<u64> {
        self.ensure_local_handle_owner(Capability::Signal, ResourceClass::Signal, local_id)?;
        let shared_id = self
            .runtime
            .kernel
            .signal_shared_id(local_id)
            .map_err(Self::host_error)?;
        self.require(
            Capability::Signal,
            ResourceClass::Signal,
            Some(ResourceIdentity::Shared(shared_id)),
        )?;
        self.runtime
            .kernel
            .notify_signal(local_id)
            .map_err(Self::host_error)
    }

    fn wait_signal(
        &self,
        local_id: u64,
        observed_generation: u64,
        timeout_ms: u64,
    ) -> HostFuture<u64> {
        if let Err(error) =
            self.ensure_local_handle_owner(Capability::Signal, ResourceClass::Signal, local_id)
        {
            return Box::pin(async move { Err(error) });
        }
        let runtime = self.runtime.clone();
        let session_id = self.session_id;
        let scope_context = self.scope_context.clone();
        Box::pin(async move {
            let shared_id = runtime
                .kernel
                .signal_shared_id(local_id)
                .map_err(RuntimeGuestHost::host_error)?;
            RuntimeGuestHost {
                runtime: runtime.clone(),
                session_id,
                scope_context,
            }
            .require(
                Capability::Signal,
                ResourceClass::Signal,
                Some(ResourceIdentity::Shared(shared_id)),
            )?;
            runtime
                .kernel
                .wait_signal(local_id, observed_generation, timeout_ms)
                .await
                .map_err(RuntimeGuestHost::host_error)
        })
    }

    fn open_log(&self, name: String) -> HostResult<DurableLogDescriptor> {
        self.require(Capability::Storage, ResourceClass::DurableLog, None)?;
        let descriptor = self.runtime.kernel.open_log(self.namespaced_name(&name));
        self.runtime.claim_local_handle(
            self.session_id,
            ResourceClass::DurableLog,
            descriptor.local_id,
        );
        Ok(descriptor)
    }

    fn close_log(&self, local_id: u64) -> HostResult<()> {
        self.ensure_local_handle_owner(Capability::Storage, ResourceClass::DurableLog, local_id)?;
        self.runtime
            .kernel
            .close_log(local_id)
            .map_err(Self::host_error)?;
        self.runtime
            .release_local_handle(self.session_id, &ResourceClass::DurableLog, local_id);
        Ok(())
    }

    fn append_log(
        &self,
        local_id: u64,
        timestamp_ms: u64,
        headers: Vec<(String, String)>,
        payload: Vec<u8>,
    ) -> HostResult<u64> {
        self.ensure_local_handle_owner(Capability::Storage, ResourceClass::DurableLog, local_id)?;
        let shared_id = self
            .runtime
            .kernel
            .log_shared_id_public(local_id)
            .map_err(Self::host_error)?;
        self.require(
            Capability::Storage,
            ResourceClass::DurableLog,
            Some(ResourceIdentity::Shared(shared_id)),
        )?;
        self.runtime
            .kernel
            .append_log(local_id, timestamp_ms, headers, payload)
            .map_err(Self::host_error)
    }

    fn replay_log(
        &self,
        local_id: u64,
        from_sequence: Option<u64>,
        limit: usize,
    ) -> HostResult<Vec<StorageRecord>> {
        self.ensure_local_handle_owner(Capability::Storage, ResourceClass::DurableLog, local_id)?;
        let shared_id = self
            .runtime
            .kernel
            .log_shared_id_public(local_id)
            .map_err(Self::host_error)?;
        self.require(
            Capability::Storage,
            ResourceClass::DurableLog,
            Some(ResourceIdentity::Shared(shared_id)),
        )?;
        self.runtime
            .kernel
            .replay_log(local_id, from_sequence, limit)
            .map_err(Self::host_error)
    }

    fn checkpoint_log(&self, local_id: u64, name: String, sequence: u64) -> HostResult<()> {
        self.ensure_local_handle_owner(Capability::Storage, ResourceClass::DurableLog, local_id)?;
        let shared_id = self
            .runtime
            .kernel
            .log_shared_id_public(local_id)
            .map_err(Self::host_error)?;
        self.require(
            Capability::Storage,
            ResourceClass::DurableLog,
            Some(ResourceIdentity::Shared(shared_id)),
        )?;
        self.runtime
            .kernel
            .checkpoint_log(local_id, name, sequence)
            .map_err(Self::host_error)
    }

    fn checkpoint_sequence(&self, local_id: u64, name: &str) -> HostResult<Option<u64>> {
        self.ensure_local_handle_owner(Capability::Storage, ResourceClass::DurableLog, local_id)?;
        let shared_id = self
            .runtime
            .kernel
            .log_shared_id_public(local_id)
            .map_err(Self::host_error)?;
        self.require(
            Capability::Storage,
            ResourceClass::DurableLog,
            Some(ResourceIdentity::Shared(shared_id)),
        )?;
        self.runtime
            .kernel
            .checkpoint_sequence(local_id, name)
            .map_err(Self::host_error)
    }

    fn open_blob_store(&self, name: String) -> HostResult<BlobStoreDescriptor> {
        self.require(Capability::Storage, ResourceClass::BlobStore, None)?;
        let descriptor = self
            .runtime
            .kernel
            .open_blob_store(self.namespaced_name(&name));
        self.runtime.claim_local_handle(
            self.session_id,
            ResourceClass::BlobStore,
            descriptor.local_id,
        );
        Ok(descriptor)
    }

    fn close_blob_store(&self, local_id: u64) -> HostResult<()> {
        self.ensure_local_handle_owner(Capability::Storage, ResourceClass::BlobStore, local_id)?;
        self.runtime
            .kernel
            .close_blob_store(local_id)
            .map_err(Self::host_error)?;
        self.runtime
            .release_local_handle(self.session_id, &ResourceClass::BlobStore, local_id);
        Ok(())
    }

    fn put_blob(&self, local_id: u64, bytes: Vec<u8>) -> HostResult<String> {
        self.ensure_local_handle_owner(Capability::Storage, ResourceClass::BlobStore, local_id)?;
        let shared_id = self
            .runtime
            .kernel
            .blob_store_shared_id_public(local_id)
            .map_err(Self::host_error)?;
        self.require(
            Capability::Storage,
            ResourceClass::BlobStore,
            Some(ResourceIdentity::Shared(shared_id)),
        )?;
        self.runtime
            .kernel
            .put_blob(local_id, bytes)
            .map_err(Self::host_error)
    }

    fn get_blob(&self, local_id: u64, blob_id: &str) -> HostResult<Option<Vec<u8>>> {
        self.ensure_local_handle_owner(Capability::Storage, ResourceClass::BlobStore, local_id)?;
        let shared_id = self
            .runtime
            .kernel
            .blob_store_shared_id_public(local_id)
            .map_err(Self::host_error)?;
        self.require(
            Capability::Storage,
            ResourceClass::BlobStore,
            Some(ResourceIdentity::Shared(shared_id)),
        )?;
        self.runtime
            .kernel
            .get_blob(local_id, blob_id)
            .map_err(Self::host_error)
    }

    fn set_manifest(&self, local_id: u64, name: String, blob_id: String) -> HostResult<()> {
        self.ensure_local_handle_owner(Capability::Storage, ResourceClass::BlobStore, local_id)?;
        let shared_id = self
            .runtime
            .kernel
            .blob_store_shared_id_public(local_id)
            .map_err(Self::host_error)?;
        self.require(
            Capability::Storage,
            ResourceClass::BlobStore,
            Some(ResourceIdentity::Shared(shared_id)),
        )?;
        self.runtime
            .kernel
            .set_manifest(local_id, name, blob_id)
            .map_err(Self::host_error)
    }

    fn get_manifest(&self, local_id: u64, name: &str) -> HostResult<Option<String>> {
        self.ensure_local_handle_owner(Capability::Storage, ResourceClass::BlobStore, local_id)?;
        let shared_id = self
            .runtime
            .kernel
            .blob_store_shared_id_public(local_id)
            .map_err(Self::host_error)?;
        self.require(
            Capability::Storage,
            ResourceClass::BlobStore,
            Some(ResourceIdentity::Shared(shared_id)),
        )?;
        self.runtime
            .kernel
            .get_manifest(local_id, name)
            .map_err(Self::host_error)
    }

    fn connect(&self, authority: String) -> HostResult<NetworkSessionDescriptor> {
        self.require(Capability::Network, ResourceClass::Session, None)?;
        let descriptor = self.runtime.kernel.connect(authority);
        self.runtime.claim_local_handle(
            self.session_id,
            ResourceClass::Session,
            descriptor.local_id,
        );
        Ok(descriptor)
    }

    fn listen(&self, address: String) -> HostResult<NetworkListenerDescriptor> {
        self.require(Capability::Network, ResourceClass::Listener, None)?;
        let descriptor = self.runtime.kernel.listen(address);
        self.runtime.claim_local_handle(
            self.session_id,
            ResourceClass::Listener,
            descriptor.local_id,
        );
        Ok(descriptor)
    }

    fn close_listener(&self, local_id: u64) -> HostResult<()> {
        self.ensure_local_handle_owner(Capability::Network, ResourceClass::Listener, local_id)?;
        self.runtime
            .kernel
            .close_listener(local_id)
            .map_err(Self::host_error)?;
        self.runtime
            .release_local_handle(self.session_id, &ResourceClass::Listener, local_id);
        Ok(())
    }

    fn close_session(&self, local_id: u64) -> HostResult<()> {
        self.ensure_local_handle_owner(Capability::Network, ResourceClass::Session, local_id)?;
        self.runtime
            .kernel
            .close_session(local_id)
            .map_err(Self::host_error)?;
        self.runtime
            .release_local_handle(self.session_id, &ResourceClass::Session, local_id);
        Ok(())
    }

    fn open_stream(&self, session_id: u64) -> HostResult<NetworkStreamDescriptor> {
        self.ensure_local_handle_owner(Capability::Network, ResourceClass::Session, session_id)?;
        let shared_id = self
            .runtime
            .kernel
            .session_shared_id_public(session_id)
            .map_err(Self::host_error)?;
        self.require(
            Capability::Network,
            ResourceClass::Session,
            Some(ResourceIdentity::Shared(shared_id)),
        )?;
        let descriptor = self
            .runtime
            .kernel
            .open_stream(session_id)
            .map_err(Self::host_error)?;
        self.runtime.claim_local_handle(
            self.session_id,
            ResourceClass::Stream,
            descriptor.local_id,
        );
        Ok(descriptor)
    }

    fn close_stream(&self, local_id: u64) -> HostResult<()> {
        self.ensure_local_handle_owner(Capability::Network, ResourceClass::Stream, local_id)?;
        self.runtime
            .kernel
            .close_stream(local_id)
            .map_err(Self::host_error)?;
        self.runtime
            .release_local_handle(self.session_id, &ResourceClass::Stream, local_id);
        Ok(())
    }

    fn stream_session_shared_id(&self, stream_id: u64) -> HostResult<u64> {
        self.ensure_local_handle_owner(Capability::Network, ResourceClass::Stream, stream_id)?;
        let session_id = self
            .runtime
            .kernel
            .stream_session_id(stream_id)
            .map_err(Self::host_error)?;
        self.runtime
            .kernel
            .session_shared_id_public(session_id)
            .map_err(Self::host_error)
    }

    fn send_stream_chunk(&self, stream_id: u64, bytes: Vec<u8>) -> HostResult<()> {
        let session_shared_id = self.stream_session_shared_id(stream_id)?;
        self.require(
            Capability::Network,
            ResourceClass::Stream,
            Some(ResourceIdentity::Shared(session_shared_id)),
        )?;
        self.runtime
            .kernel
            .send_stream_chunk(stream_id, bytes)
            .map_err(Self::host_error)
    }

    fn recv_stream_chunk(&self, stream_id: u64) -> HostResult<Option<Vec<u8>>> {
        let session_shared_id = self.stream_session_shared_id(stream_id)?;
        self.require(
            Capability::Network,
            ResourceClass::Stream,
            Some(ResourceIdentity::Shared(session_shared_id)),
        )?;
        self.runtime
            .kernel
            .recv_stream_chunk(stream_id)
            .map_err(Self::host_error)
    }

    fn send_request(
        &self,
        session_id: u64,
        method: String,
        path: String,
        request_body: Vec<u8>,
    ) -> HostResult<u64> {
        self.ensure_local_handle_owner(Capability::Network, ResourceClass::Session, session_id)?;
        let shared_id = self
            .runtime
            .kernel
            .session_shared_id_public(session_id)
            .map_err(Self::host_error)?;
        self.require(
            Capability::Network,
            ResourceClass::RequestExchange,
            Some(ResourceIdentity::Shared(shared_id)),
        )?;
        self.runtime
            .kernel
            .send_request(session_id, method, path, request_body)
            .inspect(|exchange_id| {
                self.runtime
                    .exchange_owners
                    .lock()
                    .insert(*exchange_id, self.session_id);
            })
            .map_err(Self::host_error)
    }

    fn wait_request_response(
        &self,
        exchange_id: u64,
        timeout_ms: u64,
    ) -> HostFuture<(u16, Vec<u8>)> {
        let runtime = self.runtime.clone();
        let session_id = self.session_id;
        let scope_context = self.scope_context.clone();
        Box::pin(async move {
            let owner = runtime.exchange_owners.lock().get(&exchange_id).copied();
            if owner != Some(session_id) {
                return Err(HostError::PermissionDenied(Capability::Network));
            }
            let (request_session_id, _, _, _) = runtime
                .kernel
                .request_summary(exchange_id)
                .map_err(RuntimeGuestHost::host_error)?;
            let shared_id = runtime
                .kernel
                .session_shared_id_public(request_session_id)
                .map_err(RuntimeGuestHost::host_error)?;
            RuntimeGuestHost {
                runtime: runtime.clone(),
                session_id,
                scope_context,
            }
            .require(
                Capability::Network,
                ResourceClass::RequestExchange,
                Some(ResourceIdentity::Shared(shared_id)),
            )?;
            let result = runtime
                .kernel
                .wait_request_response(exchange_id, timeout_ms)
                .await
                .map_err(RuntimeGuestHost::host_error);
            runtime.exchange_owners.lock().remove(&exchange_id);
            result
        })
    }

    fn start_process(
        &self,
        module_id: String,
        entrypoint: String,
        arguments: Vec<Vec<u8>>,
        grants: Vec<CapabilityGrant>,
    ) -> HostResult<ProcessDescriptor> {
        self.require(Capability::ProcessLifecycle, ResourceClass::Process, None)?;
        self.runtime
            .validate_grants(&grants)
            .map_err(|error| HostError::Host(error.to_string()))?;
        let parent_grants = self
            .runtime
            .session_grants(self.session_id)
            .map_err(|error| HostError::Host(error.to_string()))?;
        for grant in &grants {
            if !parent_grants
                .iter()
                .any(|parent| parent_grant_covers_child(parent, grant))
            {
                return Err(HostError::Host(
                    Error::GrantEscalation(grant.capability.clone()).to_string(),
                ));
            }
        }
        let module_bytes = self
            .runtime
            .module_bytes(&module_id)
            .map_err(|error| HostError::Host(error.to_string()))?;
        let session_id = self.runtime.persist_session(grants.clone());
        let process = self.runtime.kernel.start_process(
            module_id.clone(),
            entrypoint.clone(),
            grants.clone(),
        );
        let loaded_guest =
            match self
                .runtime
                .load_guest_module(&module_bytes, session_id, process.local_id)
            {
                Ok(loaded_guest) => loaded_guest,
                Err(error) => {
                    self.runtime.kernel.record_activity(ActivityEvent {
                        kind: selium_abi::ActivityKind::ProcessExited,
                        process_id: Some(process.local_id),
                        message: format!("guest {module_id} trapped during load: {error}"),
                    });
                    let _ = self
                        .runtime
                        .cleanup_failed_process(process.local_id, session_id);
                    return Err(HostError::Host(error.to_string()));
                }
            };
        let descriptor = SystemGuestDescriptor {
            name: module_id.clone(),
            module_id: module_id.clone(),
            module_bytes,
            entrypoint: entrypoint.clone(),
            arguments,
            grants,
            dependencies: Vec::new(),
            readiness: ReadinessCondition::Immediate,
        };
        let loaded_guest = match self.runtime.execute_entrypoint(loaded_guest, &descriptor) {
            Ok(loaded_guest) => loaded_guest,
            Err(error) => {
                self.runtime.kernel.record_activity(ActivityEvent {
                    kind: selium_abi::ActivityKind::ProcessExited,
                    process_id: Some(process.local_id),
                    message: format!("guest {module_id} trapped: {error}"),
                });
                let _ = self
                    .runtime
                    .cleanup_failed_process(process.local_id, session_id);
                return Err(HostError::Host(error.to_string()));
            }
        };
        self.runtime
            .loaded_guests
            .lock()
            .insert(process.local_id, loaded_guest);
        self.runtime
            .process_sessions
            .lock()
            .insert(process.local_id, session_id);
        self.runtime
            .claim_local_handle(self.session_id, ResourceClass::Process, process.local_id);
        self.runtime
            .claim_local_handle(session_id, ResourceClass::Process, process.local_id);
        Ok(process)
    }

    fn stop_process(&self, process_id: ProcessId) -> HostResult<()> {
        self.ensure_local_handle_owner(
            Capability::ProcessLifecycle,
            ResourceClass::Process,
            process_id,
        )?;
        self.require(
            Capability::ProcessLifecycle,
            ResourceClass::Process,
            Some(ResourceIdentity::Local(process_id)),
        )?;
        self.runtime
            .stop_process(process_id)
            .map_err(|error| HostError::Host(error.to_string()))
    }

    fn metering_observation(
        &self,
        process_id: ProcessId,
    ) -> HostResult<Option<MeteringObservation>> {
        self.ensure_local_handle_owner(
            Capability::MeteringRead,
            ResourceClass::Process,
            process_id,
        )?;
        self.require(
            Capability::MeteringRead,
            ResourceClass::MeteringStream,
            Some(ResourceIdentity::Local(process_id)),
        )?;
        Ok(self.runtime.kernel.metering_observation(process_id))
    }

    fn read_activity_from(&self, cursor: usize) -> HostResult<Vec<ActivityEvent>> {
        self.require(Capability::ActivityRead, ResourceClass::ActivityLog, None)?;
        Ok(self.runtime.kernel.read_activity_from(cursor))
    }

    fn write_guest_log(&self, entry: GuestLogEntry) -> HostResult<()> {
        if let Some(process_id) = entry.process_id {
            self.ensure_local_handle_owner(
                Capability::GuestLogWrite,
                ResourceClass::Process,
                process_id,
            )?;
        }
        self.require(
            Capability::GuestLogWrite,
            ResourceClass::GuestLog,
            entry.process_id.map(ResourceIdentity::Local),
        )?;
        self.runtime.kernel.write_guest_log(entry);
        Ok(())
    }

    fn read_guest_logs_from(
        &self,
        cursor: usize,
        process_id: Option<ProcessId>,
    ) -> HostResult<Vec<GuestLogEntry>> {
        if let Some(process_id) = process_id {
            self.ensure_local_handle_owner(
                Capability::GuestLogRead,
                ResourceClass::Process,
                process_id,
            )?;
        }
        self.require(
            Capability::GuestLogRead,
            ResourceClass::GuestLog,
            process_id.map(ResourceIdentity::Local),
        )?;
        Ok(self
            .runtime
            .kernel
            .read_guest_logs_from(cursor)
            .into_iter()
            .filter(|entry| process_id.is_none() || entry.process_id == process_id)
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use selium_abi::{LocalityScope, ResourceSelector};

    fn module_with_entrypoint(entrypoint: &str, body: &str) -> Vec<u8> {
        wat::parse_str(format!("(module (func (export \"{entrypoint}\") {body}))"))
            .expect("compile wat")
    }

    fn module_with_runtime_bridge(entrypoint: &str) -> Vec<u8> {
        wat::parse_str(format!(
            "(module
                (import \"selium\" \"session_id\" (func $session_id (result i64)))
                (import \"selium\" \"process_id\" (func $process_id (result i64)))
                (import \"selium\" \"mark_ready\" (func $mark_ready))
                (func (export \"{entrypoint}\") (result i64 i64)
                    call $mark_ready
                    call $session_id
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
    fn grants_are_restored_and_checked_by_scope() {
        let runtime = Runtime::default();
        let bootstrapped = runtime
            .spawn_system_guest(SystemGuestDescriptor {
                name: "scheduler".to_string(),
                module_id: "scheduler-module".to_string(),
                module_bytes: module_with_entrypoint("main", ""),
                entrypoint: "main".to_string(),
                arguments: Vec::new(),
                grants: vec![CapabilityGrant::new(
                    Capability::ProcessLifecycle,
                    vec![
                        ResourceSelector::Tenant("acme".to_string()),
                        ResourceSelector::UriPrefix("sel://acme/workloads/".to_string()),
                    ],
                )],
                dependencies: Vec::new(),
                readiness: ReadinessCondition::Immediate,
            })
            .expect("spawn guest");

        let session = runtime
            .restore_session(bootstrapped.session_id)
            .expect("restore session");
        assert_eq!(session.grants.len(), 1);
        assert_eq!(
            runtime
                .kernel()
                .process_grants(bootstrapped.process_id)
                .expect("process grants")
                .len(),
            1
        );
        assert!(runtime.authorises(
            bootstrapped.session_id,
            Capability::ProcessLifecycle,
            &ScopeContext {
                tenant: Some("acme".to_string()),
                uri: Some("sel://acme/workloads/a".to_string()),
                locality: LocalityScope::Cluster,
                resource_class: None,
                resource_id: None,
            }
        ));
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

    #[test]
    fn duplicate_guest_names_are_rejected() {
        let runtime = Runtime::default();
        let result = runtime.bootstrap_system_guests(RuntimeConfig {
            system_guests: vec![
                SystemGuestDescriptor {
                    name: "dup".to_string(),
                    module_id: "one".to_string(),
                    module_bytes: module_with_entrypoint("boot", ""),
                    entrypoint: "boot".to_string(),
                    arguments: Vec::new(),
                    grants: vec![CapabilityGrant::new(
                        Capability::ProcessLifecycle,
                        vec![ResourceSelector::Locality(LocalityScope::Cluster)],
                    )],
                    dependencies: Vec::new(),
                    readiness: ReadinessCondition::Immediate,
                },
                SystemGuestDescriptor {
                    name: "dup".to_string(),
                    module_id: "two".to_string(),
                    module_bytes: module_with_entrypoint("boot", ""),
                    entrypoint: "boot".to_string(),
                    arguments: Vec::new(),
                    grants: vec![CapabilityGrant::new(
                        Capability::ProcessLifecycle,
                        vec![ResourceSelector::Locality(LocalityScope::Cluster)],
                    )],
                    dependencies: Vec::new(),
                    readiness: ReadinessCondition::Immediate,
                },
            ],
        });

        assert!(matches!(result, Err(Error::DuplicateDescriptor(name)) if name == "dup"));
    }

    #[test]
    fn duplicate_module_ids_with_different_bytes_are_rejected() {
        let runtime = Runtime::default();
        runtime
            .register_module_bytes("module".to_string(), module_with_entrypoint("boot", ""))
            .expect("register first module bytes");

        let result = runtime.register_module_bytes(
            "module".to_string(),
            module_with_entrypoint("boot", "(result i32) i32.const 1"),
        );

        assert!(matches!(result, Err(Error::ModuleConflict(name)) if name == "module"));
    }

    #[test]
    fn readiness_waits_for_later_activity() {
        let runtime = Runtime::default();
        let process = runtime.kernel().start_process(
            "module",
            "main",
            vec![CapabilityGrant::new(
                Capability::ActivityRead,
                vec![ResourceSelector::Locality(LocalityScope::Cluster)],
            )],
        );
        let runtime_clone = runtime.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(25));
            runtime_clone.kernel().record_activity(ActivityEvent {
                kind: selium_abi::ActivityKind::ProcessStarted,
                process_id: Some(process.local_id),
                message: "guest ready".to_string(),
            });
        });

        assert!(runtime.wait_for_readiness(
            process.local_id,
            &ReadinessCondition::ActivityLogContains("ready".to_string())
        ));
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
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], WasmValue::I64(bootstrapped.session_id as i64));
        assert_eq!(results[1], WasmValue::I64(bootstrapped.process_id as i64));
        assert!(runtime.activity_log().iter().any(|event| {
            event.process_id == Some(bootstrapped.process_id) && event.message == "guest ready"
        }));
    }

    #[test]
    fn child_process_grants_cannot_exceed_parent_authority() {
        let runtime = Runtime::default();
        let bootstrapped = runtime
            .spawn_system_guest(SystemGuestDescriptor {
                name: "parent".to_string(),
                module_id: "parent-module".to_string(),
                module_bytes: module_with_entrypoint("boot", ""),
                entrypoint: "boot".to_string(),
                arguments: Vec::new(),
                grants: vec![CapabilityGrant::new(
                    Capability::ProcessLifecycle,
                    vec![ResourceSelector::Locality(LocalityScope::Cluster)],
                )],
                dependencies: Vec::new(),
                readiness: ReadinessCondition::Immediate,
            })
            .expect("spawn parent guest");
        let host = runtime
            .guest_host(bootstrapped.session_id)
            .expect("guest host");

        let result = host.start_process(
            "child".to_string(),
            "main".to_string(),
            Vec::new(),
            vec![CapabilityGrant::new(
                Capability::Storage,
                vec![ResourceSelector::ResourceClass(ResourceClass::BlobStore)],
            )],
        );

        assert!(
            matches!(result, Err(HostError::Host(message)) if message.contains("GrantEscalation") || message.contains("child grant exceeds"))
        );
    }

    #[test]
    fn stopping_process_revokes_session_authority() {
        let runtime = Runtime::default();
        let bootstrapped = runtime
            .spawn_system_guest(SystemGuestDescriptor {
                name: "revoked".to_string(),
                module_id: "revoked-module".to_string(),
                module_bytes: module_with_entrypoint("boot", ""),
                entrypoint: "boot".to_string(),
                arguments: Vec::new(),
                grants: vec![CapabilityGrant::new(
                    Capability::SharedMemory,
                    vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
                )],
                dependencies: Vec::new(),
                readiness: ReadinessCondition::Immediate,
            })
            .expect("spawn guest");
        let host = runtime
            .guest_host(bootstrapped.session_id)
            .expect("guest host");

        runtime
            .stop_process(bootstrapped.process_id)
            .expect("stop process");

        assert!(matches!(
            host.allocate_shared_region(64, 8),
            Err(HostError::PermissionDenied(Capability::SharedMemory))
        ));
    }

    #[test]
    fn guest_log_writes_cannot_spoof_other_processes() {
        let runtime = Runtime::default();
        let grants = vec![
            CapabilityGrant::new(
                Capability::GuestLogWrite,
                vec![ResourceSelector::ResourceClass(ResourceClass::GuestLog)],
            ),
            CapabilityGrant::new(
                Capability::ProcessLifecycle,
                vec![ResourceSelector::Locality(LocalityScope::Cluster)],
            ),
        ];
        let first = runtime
            .spawn_system_guest(SystemGuestDescriptor {
                name: "one".to_string(),
                module_id: "one-module".to_string(),
                module_bytes: module_with_entrypoint("boot", ""),
                entrypoint: "boot".to_string(),
                arguments: Vec::new(),
                grants: grants.clone(),
                dependencies: Vec::new(),
                readiness: ReadinessCondition::Immediate,
            })
            .expect("spawn first guest");
        let second = runtime
            .spawn_system_guest(SystemGuestDescriptor {
                name: "two".to_string(),
                module_id: "two-module".to_string(),
                module_bytes: module_with_entrypoint("boot", ""),
                entrypoint: "boot".to_string(),
                arguments: Vec::new(),
                grants,
                dependencies: Vec::new(),
                readiness: ReadinessCondition::Immediate,
            })
            .expect("spawn second guest");
        let second_host = runtime
            .guest_host(second.session_id)
            .expect("guest host two");

        let result = second_host.write_guest_log(GuestLogEntry {
            process_id: Some(first.process_id),
            level: "INFO".to_string(),
            target: "test".to_string(),
            message: "spoof".to_string(),
        });

        assert!(matches!(
            result,
            Err(HostError::PermissionDenied(Capability::GuestLogWrite))
        ));
    }

    #[test]
    fn bootstrap_rolls_back_when_guest_spawn_fails() {
        let runtime = Runtime::default();
        let result = runtime.bootstrap_system_guests(RuntimeConfig {
            system_guests: vec![
                SystemGuestDescriptor {
                    name: "ok".to_string(),
                    module_id: "ok-module".to_string(),
                    module_bytes: module_with_entrypoint("boot", ""),
                    entrypoint: "boot".to_string(),
                    arguments: Vec::new(),
                    grants: vec![CapabilityGrant::new(
                        Capability::ProcessLifecycle,
                        vec![ResourceSelector::Locality(LocalityScope::Cluster)],
                    )],
                    dependencies: Vec::new(),
                    readiness: ReadinessCondition::Immediate,
                },
                SystemGuestDescriptor {
                    name: "bad".to_string(),
                    module_id: "bad-module".to_string(),
                    module_bytes: module_with_entrypoint("other", ""),
                    entrypoint: "boot".to_string(),
                    arguments: Vec::new(),
                    grants: vec![CapabilityGrant::new(
                        Capability::ProcessLifecycle,
                        vec![ResourceSelector::Locality(LocalityScope::Cluster)],
                    )],
                    dependencies: Vec::new(),
                    readiness: ReadinessCondition::Immediate,
                },
            ],
        });

        assert!(matches!(result, Err(Error::Wasm(_))));
        assert_eq!(runtime.loaded_guest_count(), 0);
    }

    #[test]
    fn explicit_shared_region_grant_allows_attach() {
        let runtime = Runtime::default();
        let owner = runtime
            .spawn_system_guest(SystemGuestDescriptor {
                name: "owner".to_string(),
                module_id: "owner-module".to_string(),
                module_bytes: module_with_entrypoint("boot", ""),
                entrypoint: "boot".to_string(),
                arguments: Vec::new(),
                grants: vec![CapabilityGrant::new(
                    Capability::SharedMemory,
                    vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
                )],
                dependencies: Vec::new(),
                readiness: ReadinessCondition::Immediate,
            })
            .expect("spawn owner guest");
        let owner_host = runtime.guest_host(owner.session_id).expect("owner host");
        let region = owner_host
            .allocate_shared_region(64, 8)
            .expect("allocate shared region");

        let child = runtime
            .spawn_system_guest(SystemGuestDescriptor {
                name: "child".to_string(),
                module_id: "child-module".to_string(),
                module_bytes: module_with_entrypoint("boot", ""),
                entrypoint: "boot".to_string(),
                arguments: Vec::new(),
                grants: vec![CapabilityGrant::new(
                    Capability::SharedMemory,
                    vec![
                        ResourceSelector::ResourceClass(ResourceClass::SharedMapping),
                        ResourceSelector::ExplicitResource(ResourceIdentity::Shared(
                            region.shared_id,
                        )),
                    ],
                )],
                dependencies: Vec::new(),
                readiness: ReadinessCondition::Immediate,
            })
            .expect("spawn child guest");
        let child_host = runtime.guest_host(child.session_id).expect("child host");

        let mapping = child_host
            .attach_shared_region(region.shared_id, 0, region.len)
            .expect("attach shared region");
        assert_eq!(mapping.shared_id, region.shared_id);
    }
}
