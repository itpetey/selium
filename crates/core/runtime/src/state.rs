use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
    time::Instant,
};

use parking_lot::Mutex;
use selium_abi::{
    AbiError, HostcallOutput, OperationId, ProcessId, ResourceClass, ResourceKind, TaskId,
};
use selium_kernel::Kernel;
use selium_shm::transport::ShmTransport;
use selium_wire::pubsub::Publisher;
use wasmtiny::{WasmApplication, WasmValue};

use crate::{
    config::ProcessAuthority, error::Result, mailbox::GuestMailbox,
    region_provider::RuntimeRegionProvider,
};

/// Publisher for the runtime→discovery pub/sub feed.
pub(crate) type DiscoveryPublisher = Publisher<Vec<u8>, ShmTransport>;
pub(crate) type LocalHandleOwners = HashMap<(ResourceClass, u64), BTreeSet<ProcessId>>;
/// Region purpose tracked per (process_id, region_id) so FreeRegion can revoke aliases.
pub(crate) type RegionPurposes = HashMap<(ProcessId, u64), ResourceKind>;
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
    /// Publisher for the runtime→discovery pub/sub feed, when discovery is enabled.
    pub(crate) discovery_publisher: Arc<Mutex<Option<DiscoveryPublisher>>>,
    /// Shared id of the discovery RPC listener, when discovery is enabled.
    pub(crate) discovery_listener_shared_id: Arc<Mutex<Option<u64>>>,
    /// Region purpose tracked per (process_id, region_id) so FreeRegion can revoke aliases.
    pub(crate) region_purposes: Arc<Mutex<RegionPurposes>>,
    /// Channel to the timer driver thread (send (deadline, process_id, task_id, operation_id)).
    pub(crate) timer_tx: Arc<Mutex<Option<std::sync::mpsc::Sender<TimerRequest>>>>,
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
    HostQueueRecvWait { local_id: u64, deadline: Instant },
    SleepWait { deadline: Instant },
}

#[derive(Debug, Clone)]
pub(crate) struct HostOperation {
    pub(crate) process_id: ProcessId,
    pub(crate) task_id: Option<TaskId>,
    pub(crate) state: HostOperationState,
}

/// Request sent to the timer driver thread.
#[derive(Debug, Clone)]
pub(crate) struct TimerRequest {
    pub(crate) deadline: Instant,
    pub(crate) process_id: ProcessId,
    pub(crate) task_id: TaskId,
    pub(crate) operation_id: OperationId,
}

impl Runtime {
    /// Creates a runtime backed by the supplied kernel.
    pub fn new(kernel: Kernel) -> Self {
        // Install the runtime's kernel-backed region provider so that the
        // runtime can use selium-shm directly (e.g. for discovery pub/sub).
        if selium_memory::region_provider().is_err() {
            drop(selium_memory::set_region_provider(Box::new(
                RuntimeRegionProvider::new(kernel.clone()),
            )));
        }

        let operations = Arc::new(Mutex::new(HashMap::<OperationId, HostOperation>::new()));
        let mailboxes = Arc::new(Mutex::new(HashMap::<ProcessId, Arc<GuestMailbox>>::new()));

        // Spawn the timer driver thread.
        let (timer_tx, timer_rx) = std::sync::mpsc::channel::<TimerRequest>();
        let timer_operations = Arc::clone(&operations);
        let timer_mailboxes = Arc::clone(&mailboxes);
        std::thread::spawn(move || {
            // Simple single-threaded timer driver: for each request,
            // sleep until the deadline, then wake the task.
            for request in timer_rx {
                let now = Instant::now();
                if now < request.deadline {
                    std::thread::sleep(request.deadline - now);
                }
                // Mark the operation as ready.
                if let Some(op) = timer_operations.lock().get_mut(&request.operation_id) {
                    op.state = HostOperationState::Ready(HostcallOutput::Empty);
                }
                // Wake the guest task via mailbox.
                if let Some(mailbox) = timer_mailboxes.lock().get(&request.process_id) {
                    drop(mailbox.enqueue(request.task_id));
                }
            }
        });

        Self {
            kernel,
            process_authorities: Arc::new(Mutex::new(HashMap::new())),
            loaded_guests: Arc::new(Mutex::new(HashMap::new())),
            local_handle_owners: Arc::new(Mutex::new(HashMap::new())),
            shared_resource_owners: Arc::new(Mutex::new(HashMap::new())),
            module_registry: Arc::new(Mutex::new(HashMap::new())),
            next_operation_id: Arc::new(Mutex::new(1)),
            operations,
            mailboxes,
            discovery_publisher: Arc::new(Mutex::new(None)),
            discovery_listener_shared_id: Arc::new(Mutex::new(None)),
            region_purposes: Arc::new(Mutex::new(HashMap::new())),
            timer_tx: Arc::new(Mutex::new(Some(timer_tx))),
        }
    }

    /// Returns a clone of the runtime kernel handle.
    pub fn kernel(&self) -> Kernel {
        self.kernel.clone()
    }

    /// Returns the shared region id of the discovery pub/sub feed ring, if discovery was started.
    pub fn discovery_feed_region_id(&self) -> Option<u64> {
        // The publisher holds a ShmTransport; read the write-side region id.
        self.discovery_publisher
            .lock()
            .as_ref()
            .map(|publisher| publisher.writer().inner().write_region_id())
    }

    /// Returns the shared id of the discovery RPC listener, if discovery was started.
    pub fn discovery_listener_shared_id(&self) -> Option<u64> {
        *self.discovery_listener_shared_id.lock()
    }

    /// Publishes a raw rkyv-encoded discovery operation to the discovery feed.
    ///
    /// Returns `Ok(())` if discovery is enabled and the publish succeeds. If
    /// discovery is not enabled, this is a no-op.
    pub(crate) fn publish_discovery_event(&self, bytes: Vec<u8>) -> Result<()> {
        let mut publisher = self.discovery_publisher.lock();
        if let Some(ref mut publisher) = *publisher {
            publisher
                .publish(&bytes)
                .map_err(|error| crate::Error::Host(format!("discovery publish failed: {error}")))
        } else {
            Ok(())
        }
    }

    // ------------------------------------------------------------------
    // Timer
    // ------------------------------------------------------------------

    /// Register a SleepWait timer. The timer driver thread will wake
    /// `task_id` in `process_id`'s mailbox when `deadline` arrives.
    pub(crate) fn register_timer(
        &self,
        deadline: Instant,
        process_id: ProcessId,
        task_id: TaskId,
        operation_id: OperationId,
    ) {
        if let Some(tx) = self.timer_tx.lock().as_ref() {
            // Channel send: ignore error if the receiver is gone (shutdown).
            drop(tx.send(TimerRequest {
                deadline,
                process_id,
                task_id,
                operation_id,
            }));
        }
    }
}

impl Default for Runtime {
    fn default() -> Self {
        Self::new(Kernel::default())
    }
}
