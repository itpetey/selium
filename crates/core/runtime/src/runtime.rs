use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::Arc,
};

use parking_lot::Mutex;
use selium_abi::{OperationId, ProcessId, ResourceClass, ResourceKind, TaskId};
use selium_kernel::Kernel;
use selium_shm::transport::ShmTransport;
use selium_wire::pubsub::Publisher;

use crate::{
    bootstrap::LoadedGuest, config::ProcessAuthority, error::Result, hostcall::HostOperation,
    mailbox::GuestMailbox, region_provider::RuntimeRegionProvider,
};

/// Publisher for the runtime→discovery pub/sub feed.
pub(crate) type DiscoveryPublisher = Publisher<Vec<u8>, ShmTransport>;
pub(crate) type LocalHandleOwners = HashMap<(ResourceClass, u64), BTreeSet<ProcessId>>;
/// Region purpose tracked per (process_id, region_id) so FreeRegion can revoke aliases.
pub(crate) type RegionPurposes = HashMap<(ProcessId, u64), ResourceKind>;
pub(crate) type SharedResourceOwners = HashMap<(ResourceClass, u64), BTreeSet<ProcessId>>;
/// Wait registry keyed by (process_id, region_id).
pub(crate) type WaitRegistry = HashMap<(ProcessId, u64), Vec<WaitEntry>>;

/// Wait registry entry: (process_id, task_id, generation) for a region.
/// When the host advances the generation on a region past a registered
/// generation, the registered task is woken via the mailbox.
#[derive(Debug, Clone)]
pub(crate) struct WaitEntry {
    pub(crate) process_id: ProcessId,
    pub(crate) task_id: TaskId,
    pub(crate) region_id: u64,
    pub(crate) generation: u64,
}

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
    /// Process id of the booted discovery system guest, if any. Only this
    /// process may call `RecordResolvedQueueFor` on behalf of resolvers.
    pub(crate) discovery_process: Arc<Mutex<Option<ProcessId>>>,
    /// Region purpose tracked per (process_id, region_id) so FreeRegion can revoke aliases.
    pub(crate) region_purposes: Arc<Mutex<RegionPurposes>>,
    /// Wait registry: guest tasks parked on host-writable rings.
    pub(crate) wait_registry: Arc<Mutex<WaitRegistry>>,
    /// Wait keys for active network outbound proxy threads.
    /// Each entry is `(shared_id, wait_key)` used to kick the proxy
    /// on guest→host transitions.
    pub(crate) network_wait_keys: Arc<Mutex<Vec<(u64, usize)>>>,
    /// Maps a host queue's local id to the process that owns its receiver,
    /// so kernel-side sends (e.g. an accepted connection enqueued by the
    /// network poller) can wake the parked receiving guest.
    pub(crate) queue_waiters: Arc<Mutex<HashMap<u64, u64>>>,
    /// Well-known discovery URIs provisioned at spawn time, keyed by the
    /// serving process: `(uri, listener shared id)`. Revoked (and the entry
    /// removed) when the process terminates.
    pub(crate) well_known_uris: Arc<Mutex<HashMap<ProcessId, (String, u64)>>>,
    /// Protocol schemes a booted system guest handles (e.g. `sel-http`),
    /// keyed by process id. Revoked when the process terminates.
    pub(crate) handler_schemes: Arc<Mutex<HashMap<ProcessId, Vec<String>>>>,
    /// Process ids whose guest reactor is currently being executed by a
    /// host thread. Guarantees at most one thread enters a guest's WASM
    /// store; losers of the race return and rely on the winner's
    /// pending-wake re-check (see `poll_guest_until_stalled`).
    pub(crate) executing_guests: Arc<Mutex<HashSet<ProcessId>>>,
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

        let runtime = Self {
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
            discovery_process: Arc::new(Mutex::new(None)),
            region_purposes: Arc::new(Mutex::new(HashMap::new())),
            wait_registry: Arc::new(Mutex::new(HashMap::new())),
            network_wait_keys: Arc::new(Mutex::new(Vec::new())),
            queue_waiters: Arc::new(Mutex::new(HashMap::new())),
            well_known_uris: Arc::new(Mutex::new(HashMap::new())),
            handler_schemes: Arc::new(Mutex::new(HashMap::new())),
            executing_guests: Arc::new(Mutex::new(HashSet::new())),
        };

        // Initialise the mio network poller if possible (best-effort).
        // Tests that don't need networking can use Runtime::default()
        // without a poller; it simply won't drive any sockets.
        if let Ok(poller) = runtime.kernel.init_poller() {
            let rt = runtime.clone();
            poller.set_generation_advance(move |region_id, new_gen| {
                rt.note_generation_advance(region_id, new_gen);
            });
            poller.start_background();
        }

        runtime
    }

    /// Returns a clone of the runtime kernel handle.
    pub fn kernel(&self) -> Kernel {
        self.kernel.clone()
    }

    /// Returns the shared region id of the discovery pub/sub feed ring, if discovery was started.
    pub fn discovery_feed_region_id(&self) -> Option<u64> {
        self.discovery_publisher
            .lock()
            .as_ref()
            .map(|publisher| publisher.writer().inner().write_region_id())
    }

    /// Returns the shared id of the discovery RPC listener, if discovery was started.
    pub fn discovery_listener_shared_id(&self) -> Option<u64> {
        *self.discovery_listener_shared_id.lock()
    }

    /// Returns the well-known discovery URI provisioned for `process_id`, if
    /// any, together with its host listener queue shared id.
    pub fn well_known_uri(&self, process_id: ProcessId) -> Option<(String, u64)> {
        self.well_known_uris.lock().get(&process_id).cloned()
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
}

impl Default for Runtime {
    fn default() -> Self {
        Self::new(Kernel::default())
    }
}
