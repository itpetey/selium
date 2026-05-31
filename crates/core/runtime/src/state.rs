use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
    time::Instant,
};

use parking_lot::Mutex;
use selium_abi::{AbiError, HostcallOutput, OperationId, ProcessId, ResourceClass, TaskId};
use selium_kernel::Kernel;
use wasmtiny::{WasmApplication, WasmValue};

use crate::{config::ProcessAuthority, mailbox::GuestMailbox};

pub(crate) type LocalHandleOwners = HashMap<(ResourceClass, u64), BTreeSet<ProcessId>>;
pub(crate) type SharedResourceOwners = HashMap<(ResourceClass, u64), BTreeSet<ProcessId>>;

/// Runtime coordinating guest execution, hostcalls, and kernel resources.
#[derive(Clone)]
pub struct Runtime {
    pub(crate) kernel: Kernel,
    pub(crate) process_authorities: Arc<Mutex<HashMap<ProcessId, ProcessAuthority>>>,
    pub(crate) loaded_guests: Arc<Mutex<HashMap<ProcessId, LoadedGuest>>>,
    pub(crate) local_handle_owners: Arc<Mutex<LocalHandleOwners>>,
    pub(crate) shared_resource_owners: Arc<Mutex<SharedResourceOwners>>,
    pub(crate) module_registry: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    pub(crate) next_operation_id: Arc<Mutex<OperationId>>,
    pub(crate) operations: Arc<Mutex<HashMap<OperationId, HostOperation>>>,
    pub(crate) mailboxes: Arc<Mutex<HashMap<ProcessId, Arc<GuestMailbox>>>>,
}

pub(crate) struct LoadedGuest {
    pub(crate) app: WasmApplication,
    pub(crate) module_index: u32,
    pub(crate) entrypoint_results: Vec<WasmValue>,
}

#[derive(Debug, Clone)]
pub(crate) enum HostOperationState {
    Ready(HostcallOutput),
    Failed(AbiError),
    SignalWait {
        local_id: u64,
        shared_id: u64,
        observed_generation: u64,
        deadline: Instant,
    },
    HostQueueRecvWait {
        local_id: u64,
        deadline: Instant,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct HostOperation {
    pub(crate) process_id: ProcessId,
    pub(crate) task_id: Option<TaskId>,
    pub(crate) state: HostOperationState,
}

impl Runtime {
    /// Creates a runtime backed by the supplied kernel.
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
            mailboxes: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Returns a clone of the runtime kernel handle.
    pub fn kernel(&self) -> Kernel {
        self.kernel.clone()
    }
}

impl Default for Runtime {
    fn default() -> Self {
        Self::new(Kernel::default())
    }
}
