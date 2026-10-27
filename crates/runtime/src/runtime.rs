use std::{
    collections::{BTreeSet, HashMap, HashSet},
    path::{Path, PathBuf},
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
};

use parking_lot::Mutex;
use selium_abi::{
    AttachDeclarationScope, OperationId, ProcessId, ResourceClass, SharedResourceId, TaskId,
};
use selium_kernel::Kernel;
use selium_service::DiscoveryRequest;
use selium_shm::transport::ShmTransport;
use selium_wire::{MessageTransport, pubsub::Publisher};

use crate::{
    bootstrap::LoadedGuest,
    config::ProcessAuthority,
    error::{Error, Result},
    hostcall::HostOperation,
    mailbox::GuestMailbox,
    region_provider::RuntimeRegionProvider,
};

/// Publisher for the runtime→discovery pub/sub feed.
pub(crate) type DiscoveryPublisher = Publisher<DiscoveryRequest, ShmTransport>;
/// Recorded guest-memory handles per (process, shared region) attachment.
pub(crate) type GuestRegionMappings =
    HashMap<(ProcessId, u64), Vec<wasmtiny::runtime::SharedMemory>>;
pub(crate) type LocalHandleOwners = HashMap<(ResourceClass, u64), BTreeSet<ProcessId>>;
/// Mint-time principal tenant per resource identity. Recorded synchronously
/// inside the minting hostcall, so it is the authority for tenant attribution
/// even when no registration has been projected.
pub(crate) type ResourceTenants = HashMap<(ResourceClass, u64), String>;
pub(crate) type SharedResourceOwners = HashMap<(ResourceClass, u64), BTreeSet<ProcessId>>;
/// Serving tenant tracked per (process_id, volume local id) so an append is
/// reserved against the tenant the volume was minted under, not the writer's.
pub(crate) type VolumeTenants = HashMap<(ProcessId, u64), String>;
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
    /// Mint-time principal tenant per resource identity (see
    /// [`ResourceTenants`]).
    pub(crate) resource_tenants: Arc<Mutex<ResourceTenants>>,
    pub(crate) next_operation_id: Arc<Mutex<OperationId>>,
    pub(crate) operations: Arc<Mutex<HashMap<OperationId, HostOperation>>>,
    pub(crate) mailboxes: Arc<Mutex<HashMap<ProcessId, Arc<GuestMailbox>>>>,
    /// Publisher for the runtime→discovery pub/sub feed, when discovery is enabled.
    pub(crate) discovery_publisher: Arc<Mutex<Option<DiscoveryPublisher>>>,
    /// Shared id of the discovery RPC listener, when discovery is enabled.
    pub(crate) discovery_listener_shared_id: Arc<Mutex<Option<u64>>>,
    /// Owner-declared attachable-by-resolution declarations, keyed by resource
    /// identity. While an entry holds, a resolve-recording naming a client
    /// process inside the declaration's scope becomes that process's attach
    /// basis. Entries are removed when the resource is freed or its owner
    /// exits.
    pub(crate) attachable_by_resolution:
        Arc<Mutex<HashMap<(ResourceClass, SharedResourceId), AttachDeclarationScope>>>,
    /// Serving tenant tracked per (process_id, volume local id) so an append is
    /// reserved against the tenant the volume was minted under.
    pub(crate) volume_tenants: Arc<Mutex<VolumeTenants>>,
    /// Tenant whose `ResourceClass::Process` quota a spawned child consumed,
    /// keyed by the child's process id. Recorded by `ProcessStart`; teardown
    /// releases the slot exactly once by removing (and reading) the entry.
    pub(crate) process_quota_tenants: Arc<Mutex<HashMap<ProcessId, String>>>,
    /// Wait registry: guest tasks parked on host-writable rings.
    pub(crate) wait_registry: Arc<Mutex<WaitRegistry>>,
    /// Region attachments: every process that mapped a shared region,
    /// recorded at `AttachRegion` and removed at process cleanup. Wake
    /// registration (`WaitRegister`/`GenerationAdvance`) authorises both
    /// owners and attachers: an attacher holds a live mapping of the
    /// region, so its parked readers must be woken by a writer's
    /// generation bump — notably a guest that attaches a region a peer
    /// allocated and handed off through a queue it does not own (the
    /// bridge channel attaching the connector's relayed stream region).
    pub(crate) region_attachments: Arc<Mutex<HashMap<u64, HashSet<ProcessId>>>>,
    /// Wait targets for active network outbound proxy threads.
    /// Each entry is `(shared_id, generation_offset)` — the absolute byte
    /// offset of the ring's generation word within the shared region — used
    /// to kick the proxy on guest→host transitions for regions without the
    /// fast path.
    pub(crate) network_wait_keys: Arc<Mutex<Vec<(u64, u64)>>>,
    /// Per-attachment shared-page fast-path eligibility: region id →
    /// attaching process id → whether that process's guest module is
    /// fast-path capable (engine registry support + module declares shared
    /// memory and contains atomic notify opcodes; see `module_probe`).
    ///
    /// A region's fast path is active only when **every** attacher is
    /// capable, so a stable-built guest sharing a region with an atomics
    /// guest keeps its transition kicks. Entries are recorded at attach and
    /// removed when the attacher releases the region or the region is
    /// destroyed.
    pub(crate) fast_path_attachments: Arc<Mutex<HashMap<u64, HashMap<ProcessId, bool>>>>,
    /// Per-process fast-path capability, probed from the guest's module
    /// bytes at spawn (see `module_probe`). Removed at process cleanup.
    pub(crate) process_fastpath: Arc<Mutex<HashMap<ProcessId, bool>>>,
    /// Guest→host transition kicks **delivered** per network region id.
    /// Suppressed regions (fast path active) are not counted. Observability
    /// for embedders and the fast-path end-to-end test, which asserts a
    /// fast-path region's count stays at zero while the guest's atomic
    /// notify carries its wakes.
    pub(crate) kick_counts: Arc<Mutex<HashMap<u64, u64>>>,
    /// Maps a host queue's local id to the process that owns its receiver,
    /// so kernel-side sends (e.g. an accepted connection enqueued by the
    /// network poller) can wake the parked receiving guest.
    pub(crate) queue_waiters: Arc<Mutex<HashMap<u64, u64>>>,
    /// Process ids whose guest reactor is currently being executed by a
    /// host thread. Guarantees at most one thread enters a guest's WASM
    /// store; losers of the race return and rely on the winner's
    /// pending-wake re-check (see `poll_guest_until_stalled`).
    pub(crate) executing_guests: Arc<Mutex<HashSet<ProcessId>>>,
    /// The runtime's timer driver, started lazily on the first guest-issued
    /// `Sleep`. Owns every guest sleep deadline so the completion wake never
    /// depends on an ambient executor context (see `timer`).
    pub(crate) timer_driver: Arc<OnceLock<crate::timer::TimerDriver>>,
    /// Host-held PKI keyring backing the certificate-signing hostcalls.
    /// Initialised at startup (see [`Runtime::initialise_keyring`]); the
    /// signing hostcalls fail loudly while it is absent.
    pub(crate) keyring: Arc<Mutex<Option<crate::keyring::Keyring>>>,
    /// Host-side metering projector: accumulates per-process cumulative
    /// cpu/bandwidth counters and storage gauges, projected into the kernel on
    /// each metering tick (see [`Runtime::metering_tick`]).
    pub(crate) metering: Arc<Mutex<crate::metering::MeteringProjector>>,
    /// Guards the one-shot metering ticker started at bootstrap: the host
    /// projects fresh per-process observations on the sampling cadence so
    /// the bookkeeper's `MeteringRead` polls observe live consumption.
    pub(crate) metering_ticker_started: Arc<std::sync::atomic::AtomicBool>,
    /// Per-guest worker-count overrides for multithreaded execution, keyed by
    /// system guest name. Absent entries default to the number of available
    /// CPU cores (the pool never exceeds it unless explicitly configured).
    pub(crate) worker_counts: Arc<Mutex<HashMap<String, usize>>>,
    /// Per-process CPU budget windows (see [`CpuBudgetWindow`]): the wall-clock
    /// minute a process's engine execution budget is anchored for. Written by
    /// the spawn-time anchor and the per-second `refresh_cpu_budgets`, removed
    /// at process teardown.
    pub(crate) cpu_budget_windows: Arc<Mutex<HashMap<ProcessId, CpuBudgetWindow>>>,
    /// Guards the one-shot state-root preparation: rooting the volume layer,
    /// loading or generating the host keyring, and reconstructing per-tenant
    /// volume usage all happen exactly once, before any guest runs.
    state_root_prepared: Arc<AtomicBool>,
    /// The discovery service's process id, recorded at bootstrap so its
    /// teardown can release the host-created listener queue.
    pub(crate) discovery_process_id: Arc<Mutex<Option<ProcessId>>>,
    /// Guest-memory handles for shared-region attachments taken on a
    /// process's behalf, one per attach. Teardown detaches each through the
    /// same `Memory` the attach used: the engine decrements a region's
    /// attachment count only on an explicit detach, and the guest instance may
    /// already be unloaded by then (see `selium_kernel::MemoryRegistry::
    /// detach_shared_region_from_memory`).
    pub(crate) guest_region_memories: Arc<Mutex<GuestRegionMappings>>,
    /// Parent regions a process handed to a receiver, keyed by the sender.
    /// On the sender's teardown the runtime marks each session's peer departed
    /// and wakes the receiver, so its parked handler closes and frees the
    /// region it owns.
    pub(crate) handed_off_sessions: Arc<Mutex<HashMap<ProcessId, Vec<u64>>>>,
    /// Test-only fault injection (see [`RuntimeFaults`]); off unless armed.
    pub(crate) faults: Arc<RuntimeFaults>,
}

/// Process-local fault switches used by the failure-injection suite.
///
/// Every field is off (zero) by default: arming one is an explicit test
/// action, and nothing in the product path consults them otherwise. They live
/// on the runtime rather than behind a feature flag so an integration test can
/// drive the same `Runtime` it bootstrapped, and are exposed only through
/// `#[doc(hidden)]` setters.
#[derive(Default)]
pub(crate) struct RuntimeFaults {
    /// Make the host's authoritative resource query fail as unavailable.
    pub(crate) authority_unavailable: std::sync::atomic::AtomicBool,
    /// Make a discovery-feed publish fail (teardown, revocation).
    pub(crate) discovery_publish_failure: std::sync::atomic::AtomicBool,
    /// Nanoseconds added to guest-visible clocks (`TimeNow`/`TimeMonotonic`),
    /// so a lease can be aged past its TTL without waiting for wall time.
    pub(crate) clock_offset_nanos: std::sync::atomic::AtomicU64,
}

/// Per-process CPU budget window: the wall-clock minute the process's engine
/// execution budget is currently anchored for, and its cumulative
/// instruction count at that window's start.
///
/// The runtime recomputes a process's budget as
/// `window_start_instructions + ceiling` on every refresh, so consumption in
/// one window never reduces the next window's ceiling while a ceiling authored
/// mid-window still takes effect against the current window's remaining
/// allowance. A change of `window_index` (a new wall-clock minute) re-anchors
/// `window_start_instructions` from the live count.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CpuBudgetWindow {
    /// Wall-clock minute index (`unix_seconds / 60`).
    pub(crate) window_index: u64,
    /// The process's cumulative executed-instruction count at this window's
    /// start.
    pub(crate) window_start_instructions: u64,
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
            resource_tenants: Arc::new(Mutex::new(HashMap::new())),
            next_operation_id: Arc::new(Mutex::new(1)),
            operations,
            mailboxes,
            discovery_publisher: Arc::new(Mutex::new(None)),
            discovery_listener_shared_id: Arc::new(Mutex::new(None)),
            attachable_by_resolution: Arc::new(Mutex::new(HashMap::new())),
            volume_tenants: Arc::new(Mutex::new(HashMap::new())),
            process_quota_tenants: Arc::new(Mutex::new(HashMap::new())),
            wait_registry: Arc::new(Mutex::new(HashMap::new())),
            region_attachments: Arc::new(Mutex::new(HashMap::new())),
            network_wait_keys: Arc::new(Mutex::new(Vec::new())),
            fast_path_attachments: Arc::new(Mutex::new(HashMap::new())),
            process_fastpath: Arc::new(Mutex::new(HashMap::new())),
            kick_counts: Arc::new(Mutex::new(HashMap::new())),
            queue_waiters: Arc::new(Mutex::new(HashMap::new())),
            executing_guests: Arc::new(Mutex::new(HashSet::new())),
            timer_driver: Arc::new(OnceLock::new()),
            keyring: Arc::new(Mutex::new(None)),
            metering: Arc::new(Mutex::new(crate::metering::MeteringProjector::default())),
            metering_ticker_started: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            worker_counts: Arc::new(Mutex::new(HashMap::new())),
            cpu_budget_windows: Arc::new(Mutex::new(HashMap::new())),
            state_root_prepared: Arc::new(AtomicBool::new(false)),
            discovery_process_id: Arc::new(Mutex::new(None)),
            guest_region_memories: Arc::new(Mutex::new(HashMap::new())),
            handed_off_sessions: Arc::new(Mutex::new(HashMap::new())),
            faults: Arc::new(RuntimeFaults::default()),
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

    /// Prepares the durable state root, once, before any guest runs.
    ///
    /// Roots the kernel's volume layer at `state_dir`, reconstructs per-tenant
    /// volume usage from the resident bytes there, and loads (or, on first
    /// boot, generates and persists) the host PKI hierarchy. A kernel built
    /// with a throwaway volume root is re-rooted here — legal precisely because
    /// no volume has been opened yet; see
    /// [`selium_kernel::VolumeRegistry::set_root`].
    pub(crate) fn prepare_state_root(&self, state_dir: Option<&Path>) -> Result<()> {
        if self.state_root_prepared.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        if let Some(dir) = state_dir {
            self.kernel.volumes().set_root(dir)?;
        }
        self.seed_volume_usage()?;
        if let Some(dir) = state_dir {
            self.load_or_generate_keyring(dir)?;
        }
        Ok(())
    }

    /// Seeds the quota counters with each tenant's resident volume bytes.
    ///
    /// Durable storage belongs to the tenant, not to a process, and the bytes
    /// are read back off the volume layout rather than remembered: without this
    /// a restart would let a tenant append its whole ceiling again on top of
    /// what it already holds.
    fn seed_volume_usage(&self) -> Result<()> {
        for (tenant, bytes) in self.kernel.volumes().usage_by_tenant()? {
            if bytes > 0 {
                // Force-accepted, not denied: the bytes are already resident,
                // so the counter reflects them and later allocations face the
                // authored ceiling.
                self.kernel
                    .quota()
                    .force_consume(&tenant, ResourceClass::Volume, bytes);
            }
        }
        Ok(())
    }

    /// Loads the host keyring from the state directory, generating and
    /// persisting a fresh hierarchy on first boot. An already-installed
    /// keyring is left in place.
    fn load_or_generate_keyring(&self, state_dir: &Path) -> Result<()> {
        if self.keyring.lock().is_some() {
            return Ok(());
        }
        let keyring = crate::keyring::KeyringStore::new(state_dir)
            .load_or_generate()
            .map_err(|error| Error::Host(error.to_string()))?;
        *self.keyring.lock() = Some(keyring);
        Ok(())
    }

    /// Returns a clone of the runtime kernel handle.
    pub fn kernel(&self) -> Kernel {
        self.kernel.clone()
    }

    /// Diagnostic: the process ids currently resident in the loaded-guest
    /// table. Used by the soak lane's residency assertions.
    #[doc(hidden)]
    pub fn resident_guest_ids(&self) -> Vec<ProcessId> {
        let mut ids: Vec<ProcessId> = self.loaded_guests.lock().keys().copied().collect();
        ids.sort_unstable();
        ids
    }

    /// Test-only: make the host authority answer an admission query as
    /// unavailable.
    ///
    /// The failure-injection suite arms this to prove discovery denies a
    /// registration instead of falling back to its projected store. Off by
    /// default; no product path sets it.
    #[doc(hidden)]
    pub fn inject_authority_failure(&self, armed: bool) {
        self.faults
            .authority_unavailable
            .store(armed, std::sync::atomic::Ordering::SeqCst);
    }

    /// Test-only: make discovery-feed publishes fail.
    ///
    /// Arms the retryable-teardown path: `stop_process` must report failure
    /// and a later retry must complete the staged revocations. Off by default.
    #[doc(hidden)]
    pub fn inject_discovery_publish_failure(&self, armed: bool) {
        self.faults
            .discovery_publish_failure
            .store(armed, std::sync::atomic::Ordering::SeqCst);
    }

    /// Test-only: advance every guest-visible clock by `offset`.
    ///
    /// The failure-injection suite uses this to age a lease past its TTL
    /// without waiting for wall time. Off by default.
    #[doc(hidden)]
    pub fn advance_guest_clock(&self, offset: core::time::Duration) {
        let nanos = u64::try_from(offset.as_nanos()).unwrap_or(u64::MAX);
        self.faults
            .clock_offset_nanos
            .fetch_add(nanos, std::sync::atomic::Ordering::SeqCst);
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

    /// Initialises the host PKI keyring from bootstrap material, failing
    /// loudly when the intermediate is missing or invalid. Identity depends
    /// on this at startup: a host that cannot sign tenant CAs must not
    /// bootstrap the mint authority.
    pub fn initialise_keyring(&self, bootstrap: crate::keyring::KeyringBootstrap) -> Result<()> {
        let keyring = crate::keyring::Keyring::initialise(
            Box::new(crate::keyring::InMemoryCaStore::default()),
            bootstrap,
        )
        .map_err(|error| Error::Host(error.to_string()))?;
        *self.keyring.lock() = Some(keyring);
        Ok(())
    }

    /// Loads or generates the host keyring under a state directory.
    pub fn initialise_keyring_from_state(&self, state_dir: impl Into<PathBuf>) -> Result<()> {
        self.load_or_generate_keyring(&state_dir.into())
    }

    /// Returns the installed keyring's intermediate certificate (public DER),
    /// or `None` when no keyring is installed.
    ///
    /// Observable state for embedders and tests; no private material is
    /// exposed.
    pub fn keyring_intermediate_cert(&self) -> Option<Vec<u8>> {
        self.keyring
            .lock()
            .as_ref()
            .map(|keyring| keyring.intermediate_cert().to_vec())
    }

    /// Generates a fresh root + intermediate hierarchy and installs it as the
    /// host keyring. The offline root is retained for operator bootstrap and
    /// never referenced by a hostcall.
    pub fn generate_keyring(&self) -> Result<()> {
        let keyring =
            crate::keyring::Keyring::generate().map_err(|error| Error::Host(error.to_string()))?;
        *self.keyring.lock() = Some(keyring);
        Ok(())
    }

    /// Publishes a typed discovery operation to the discovery feed.
    ///
    /// Returns `Ok(())` if discovery is enabled and the publish succeeds. If
    /// discovery is not enabled, this is a no-op.
    ///
    /// The host is itself a writer on the feed, so it owes its subscriber the
    /// same wake a guest writer owes: the commit advances the region's
    /// generation, and every process that registered interest in that region
    /// (discovery, via `WaitRegister`) must be signalled. Without this the feed
    /// is write-only in practice — discovery's feed loop stays parked until
    /// something unrelated re-enters the guest, so a Tier-1 event can sit
    /// unread indefinitely and the routes it removes stay resolvable.
    pub(crate) fn publish_discovery_event(&self, request: DiscoveryRequest) -> Result<()> {
        // Test seam: the failure-injection suite arms this to exercise the
        // retryable teardown path. Off unless explicitly armed.
        if self
            .faults
            .discovery_publish_failure
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(crate::Error::Host(
                "discovery publish failed (injected)".to_string(),
            ));
        }
        // Scoped so the publisher lock is released before the wake is
        // delivered: the wake re-enters the woken guest inline, and that guest
        // may itself publish to the feed.
        let committed = {
            let mut publisher = self.discovery_publisher.lock();
            let Some(publisher) = publisher.as_mut() else {
                return Ok(());
            };
            publisher.publish(&request).map_err(|error| {
                crate::Error::Host(format!("discovery publish failed: {error}"))
            })?;
            let transport = publisher.writer().inner();
            Some((transport.write_region_id(), transport.generation()))
        };
        if let Some((region_id, Ok(generation))) = committed {
            self.note_generation_advance(region_id, generation);
        }
        Ok(())
    }

    /// Returns the registered mailbox of `process_id`, if the guest has
    /// registered one (the guest calls `mailbox_register` during init).
    pub(crate) fn mailbox(&self, process_id: ProcessId) -> Option<Arc<GuestMailbox>> {
        self.mailboxes.lock().get(&process_id).cloned()
    }

    /// Configures the worker-pool size for a multithreaded system guest by
    /// name. Unconfigured guests default to the number of available CPU
    /// cores (never exceeding it unless this override is set).
    ///
    /// The count is clamped to at least one worker: a zero-worker pool would
    /// have no thread to run the guest, and the monitor would reap the
    /// process the instant it spawned.
    pub fn set_worker_count(&self, guest_name: &str, count: usize) {
        self.worker_counts
            .lock()
            .insert(guest_name.to_string(), count.max(1));
    }

    /// Resolves the worker-pool size for a multithreaded guest: the explicit
    /// per-guest override, or the available-core default.
    pub fn worker_count_for(&self, guest_name: &str) -> usize {
        self.worker_counts
            .lock()
            .get(guest_name)
            .copied()
            .unwrap_or_else(|| {
                std::thread::available_parallelism()
                    .map(std::num::NonZeroUsize::get)
                    .unwrap_or(1)
            })
    }

    /// Records a shared-region attachment taken into a guest's memory, so
    /// teardown can detach it later without the guest instance.
    pub(crate) fn record_guest_region_mapping(
        &self,
        process_id: ProcessId,
        region_id: u64,
        memory: &wasmtiny::runtime::SharedMemory,
    ) {
        self.guest_region_memories
            .lock()
            .entry((process_id, region_id))
            .or_default()
            .push(memory.clone());
    }

    /// Detaches every recorded guest-memory mapping for `process_id` (or for a
    /// single `region_id` when given), releasing the engine's attachment
    /// counts.
    pub(crate) fn detach_guest_region_mappings(
        &self,
        process_id: Option<ProcessId>,
        region_id: Option<u64>,
    ) {
        let handles: Vec<(u64, wasmtiny::runtime::SharedMemory)> = {
            let mut records = self.guest_region_memories.lock();
            let keys: Vec<(ProcessId, u64)> = records
                .keys()
                .filter(|(owner, region)| {
                    process_id.is_none_or(|expected| *owner == expected)
                        && region_id.is_none_or(|expected| *region == expected)
                })
                .copied()
                .collect();
            keys.into_iter()
                .flat_map(|key| {
                    records
                        .remove(&key)
                        .unwrap_or_default()
                        .into_iter()
                        .map(|memory| (key.1, memory))
                        .collect::<Vec<_>>()
                })
                .collect()
        };

        for (region, memory) in handles {
            let Ok(mut memory) = memory.lock() else {
                continue;
            };
            drop(
                self.kernel
                    .memory()
                    .detach_shared_region_from_memory(&mut memory, region),
            );
        }
    }

    /// Drops any sleeps the process left queued on the timer driver.
    pub(crate) fn cancel_process_timers(&self, process_id: ProcessId) {
        if let Some(driver) = self.timer_driver.get() {
            driver.cancel_process(process_id);
        }
    }

    /// Guest-visible clock offset armed by the failure-injection suite, in
    /// nanoseconds. Zero unless `advance_guest_clock` was called.
    pub(crate) fn guest_clock_offset_nanos(&self) -> u64 {
        self.faults
            .clock_offset_nanos
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Number of guest sleeps still queued on the timer driver, or zero when
    /// no timer has been scheduled.
    ///
    /// A reaped process must not strand its deadlines: the soak lane asserts
    /// this returns to zero after every spawn/stop cycle.
    pub fn pending_timer_count(&self) -> usize {
        self.timer_driver
            .get()
            .map_or(0, crate::timer::TimerDriver::pending_count)
    }

    /// Diagnostic: owners of every shared region, sorted by region id.
    ///
    /// Used by the soak lane to attribute a leaked region to the process that
    /// still owns it (an empty owner list means the resource is unattributed).
    pub fn region_owner_report(&self) -> Vec<(u64, Vec<u64>)> {
        let owners = self.shared_resource_owners.lock();
        let mut report: Vec<(u64, Vec<u64>)> = owners
            .iter()
            .filter(|((class, _), _)| *class == selium_abi::ResourceClass::SharedRegion)
            .map(|((_, id), processes)| {
                let mut processes: Vec<u64> = processes.iter().copied().collect();
                processes.sort_unstable();
                (*id, processes)
            })
            .collect();
        report.sort_unstable();
        report
    }
}

impl Default for Runtime {
    fn default() -> Self {
        Self::new(Kernel::default())
    }
}
