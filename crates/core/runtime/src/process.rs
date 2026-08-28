use std::collections::HashSet;

use selium_abi::{
    ActivityEvent, Capability, CapabilityGrant, DiscoveryRequest, LocalityScope, ProcessId,
    ResourceClass, ResourceIdentity, ResourceSelector, ScopeContext, TaskId, encode_rkyv,
};
use tracing::debug;
use wasmtiny::WasmValue;

use crate::{
    Error, Result, config::ProcessAuthority, hostcall::HostOperationState, runtime::Runtime,
};

impl Runtime {
    /// Stops a process and releases runtime-owned state for it.
    pub fn stop_process(&self, process_id: selium_abi::ProcessId) -> Result<()> {
        self.kernel.processes().stop_process(process_id)?;
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
            self.mailboxes.lock().remove(&process_id);
            self.cleanup_process_resources(process_id)?;
        }
        self.local_handle_owners
            .lock()
            .remove(&(ResourceClass::Process, process_id));
        // If the discovery service stopped, drop its recorded identity so
        // `RecordResolvedQueueFor` is no longer accepted from any caller.
        if *self.discovery_process.lock() == Some(process_id) {
            *self.discovery_process.lock() = None;
        }
        self.kernel.processes().reap_process(process_id)?;
        Ok(())
    }

    /// Returns the persisted authority for a process, if present.
    pub fn restore_process_authority(&self, process_id: ProcessId) -> Option<ProcessAuthority> {
        self.process_authorities.lock().get(&process_id).cloned()
    }

    /// Returns whether a process has a grant matching the capability and context.
    pub fn authorises(
        &self,
        process_id: ProcessId,
        capability: Capability,
        context: &ScopeContext,
    ) -> bool {
        // Clone the grants so we can release the lock before evaluating
        // the Children selector, which itself needs process_authorities.
        let grants = {
            self.process_authorities
                .lock()
                .get(&process_id)
                .map(|record| record.grants.clone())
        };
        grants
            .map(|grants| {
                grants.iter().any(|grant| {
                    grant.capability == capability
                        && grant.selectors.iter().all(|selector| match selector {
                            ResourceSelector::Children => {
                                self.selector_matches_children(process_id, context.resource_id)
                            }
                            _ => selector.matches(context),
                        })
                })
            })
            .unwrap_or(false)
    }

    /// Checks if `target` is a descendant of `ancestor` by walking the
    /// parent chain in the process authority table.
    pub fn is_descendant_of(&self, target: ProcessId, ancestor: ProcessId) -> bool {
        let authorities = self.process_authorities.lock();
        let mut current = target;
        // Bound traversal to the number of processes (no cycles expected).
        let max_depth = authorities.len();
        for _ in 0..=max_depth {
            match authorities.get(&current) {
                Some(auth) if auth.parent == Some(ancestor) => return true,
                Some(auth) if auth.parent.is_some() => {
                    current = auth.parent.expect("parent is Some");
                }
                _ => return false,
            }
        }
        false
    }

    fn selector_matches_children(
        &self,
        grantee: ProcessId,
        target: Option<ResourceIdentity>,
    ) -> bool {
        match target {
            Some(ResourceIdentity::Local(target_pid)) => self.is_descendant_of(target_pid, grantee),
            _ => false,
        }
    }

    /// Projects a metering observation into the kernel.
    pub fn project_metering(
        &self,
        process_id: selium_abi::ProcessId,
        observation: selium_abi::MeteringObservation,
    ) {
        self.kernel
            .processes()
            .observe_metering(process_id, observation);
    }

    /// Returns all activity log events currently held by the kernel.
    pub fn activity_log(&self) -> Vec<ActivityEvent> {
        self.kernel.processes().read_activity_from(0)
    }

    /// Returns the loaded module index for a process entrypoint, if loaded.
    pub fn loaded_entrypoint(&self, process_id: selium_abi::ProcessId) -> Option<u32> {
        self.loaded_guests
            .lock()
            .get(&process_id)
            .map(|guest| guest.module_index)
    }

    /// Returns entrypoint execution results for a loaded guest, if available.
    pub fn entrypoint_results(&self, process_id: selium_abi::ProcessId) -> Option<Vec<WasmValue>> {
        self.loaded_guests
            .lock()
            .get(&process_id)
            .map(|guest| guest.entrypoint_results.clone())
    }

    /// Returns the number of currently loaded guests.
    pub fn loaded_guest_count(&self) -> usize {
        self.loaded_guests.lock().len()
    }

    /// Registers module bytes under an id, rejecting conflicting bytes.
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

    pub(crate) fn persist_process_authority(
        &self,
        process_id: ProcessId,
        grants: Vec<CapabilityGrant>,
        tenant: Option<String>,
        parent: Option<ProcessId>,
    ) {
        self.process_authorities.lock().insert(
            process_id,
            ProcessAuthority {
                grants,
                tenant,
                parent,
                resolved_queue_ids: HashSet::new(),
            },
        );
    }

    /// Returns the tenant identity assigned to a process, if any.
    pub fn process_tenant(&self, process_id: ProcessId) -> Option<String> {
        self.process_authorities
            .lock()
            .get(&process_id)
            .and_then(|authority| authority.tenant.clone())
    }

    /// Validates grants against the enforcement admission matrix.
    ///
    /// Admitted selectors: `ResourceClass`, `Locality`, `ExplicitResource`,
    /// `Tenant`, `Children`.
    /// Admitted with constraints: `UriPrefix` (requires a network
    /// `ResourceClass` selector in the same grant).
    /// Empty selector list = unrestricted within the capability.
    pub(crate) fn validate_grants(&self, grants: &[CapabilityGrant]) -> Result<()> {
        for grant in grants {
            for selector in &grant.selectors {
                if !selector.is_evaluatable(&grant.selectors) {
                    return Err(Error::UnevaluatableSelector(
                        grant.capability.clone(),
                        format!("{selector:?}"),
                    ));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn cleanup_failed_process(&self, process_id: selium_abi::ProcessId) -> Result<()> {
        // Best-effort teardown: the process has already failed, so there's no
        // recovery path for individual cleanup steps. We discard each error and
        // continue with the remaining work to reclaim as much as possible.
        drop(self.kernel.processes().stop_process(process_id));
        self.operations
            .lock()
            .retain(|_, operation| operation.process_id != process_id);
        drop(self.cleanup_process_resources(process_id));
        drop(self.kernel.processes().reap_process(process_id));
        self.process_authorities.lock().remove(&process_id);
        self.mailboxes.lock().remove(&process_id);
        self.local_handle_owners
            .lock()
            .remove(&(ResourceClass::Process, process_id));
        // Remove the failed process from all shared-resource owner sets,
        // but preserve co-owners (fix: previously retain deleted entire
        // owner sets when one co-owner failed).
        {
            let mut shared_resource_owners = self.shared_resource_owners.lock();
            for owners in shared_resource_owners.values_mut() {
                owners.remove(&process_id);
            }
            shared_resource_owners.retain(|_, owners| !owners.is_empty());
        }
        Ok(())
    }

    pub(crate) fn cleanup_process_resources(&self, process_id: ProcessId) -> Result<()> {
        // Revoke all discovery URIs registered for this process by publishing
        // Revoke operations to the discovery feed.
        let region_purposes: Vec<(u64, selium_abi::ResourceKind)> = {
            let mut map = self.region_purposes.lock();
            let keys: Vec<(ProcessId, u64)> = map
                .keys()
                .filter(|(pid, _)| *pid == process_id)
                .copied()
                .collect();
            keys.into_iter()
                .filter_map(|key| map.remove(&key).map(|purpose| (key.1, purpose)))
                .collect()
        };
        for (shared_id, purpose) in region_purposes {
            let uris = crate::discovery::registration_uris(process_id, shared_id, purpose);
            for uri in uris {
                let request = DiscoveryRequest::Revoke { uri };
                let bytes = encode_rkyv(&request).map_err(|error| {
                    crate::Error::Host(format!("discovery encode failed: {error}"))
                })?;
                self.publish_discovery_event(bytes)?;
            }
        }

        // Revoke any well-known URI provisioned for this process, so a
        // terminated connector's channel stops resolving. Best-effort, like
        // the rest of this teardown path.
        if let Some((uri, _listener_shared_id)) = self.well_known_uris.lock().remove(&process_id) {
            let request = DiscoveryRequest::Revoke { uri };
            if let Ok(bytes) = encode_rkyv(&request) {
                drop(self.publish_discovery_event(bytes));
            }
        }

        // Revoke tier-1 registrations for host queues created by this process.
        let owned_queues = self
            .shared_resource_owners
            .lock()
            .iter()
            .filter_map(|((resource_class, shared_id), owners)| {
                if resource_class == &ResourceClass::HostQueue && owners.contains(&process_id) {
                    Some(*shared_id)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        for shared_id in owned_queues {
            let request = DiscoveryRequest::Revoke {
                uri: crate::discovery::queue_registration_uri(process_id, shared_id),
            };
            if let Ok(bytes) = encode_rkyv(&request) {
                drop(self.publish_discovery_event(bytes));
            }
        }

        // Revoke protocol handler registrations for this process.
        let handler_schemes = self.handler_schemes.lock().remove(&process_id);
        if let Some(schemes) = handler_schemes {
            for scheme in schemes {
                let request = DiscoveryRequest::RevokeHandler { protocol: scheme };
                if let Ok(bytes) = encode_rkyv(&request) {
                    drop(self.publish_discovery_event(bytes));
                }
            }
        }

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
            // Best-effort kernel resource cleanup: the process is terminating so
            // failures to detach/close individual kernel handles can't be recovered.
            // Each drop discards the Result — we move on and reclaim what we can.
            match resource_class {
                ResourceClass::SharedMapping => {
                    drop(self.kernel.memory().detach_shared_region(local_id));
                }
                ResourceClass::TcpListener => {
                    drop(self.kernel.close_tcp_listener(local_id));
                }
                ResourceClass::TcpStream => {
                    drop(self.kernel.close_tcp_stream(local_id));
                }
                ResourceClass::UdpSocket => {
                    drop(self.kernel.close_udp_socket(local_id));
                }
                ResourceClass::DurableLog => {
                    drop(self.kernel.storage().close_log(local_id));
                }
                ResourceClass::BlobStore => {
                    drop(self.kernel.storage().close_blob_store(local_id));
                }
                ResourceClass::Process => {}
                _ => {}
            }
        }

        // Auto-free shared regions owned by the terminated process.
        let owned_regions = self
            .shared_resource_owners
            .lock()
            .iter()
            .filter_map(|((resource_class, shared_id), owners)| {
                if resource_class == &ResourceClass::SharedRegion && owners.contains(&process_id) {
                    Some(*shared_id)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        for shared_id in owned_regions {
            self.release_shared_resource(process_id, &ResourceClass::SharedRegion, shared_id);
            if self.kernel.memory().shared_region_mapping_count(shared_id) == 0 {
                // Best-effort: the region has no remaining mappings, but if
                // destruction fails the region will be reclaimed by the kernel
                // on process exit anyway.
                drop(self.kernel.memory().destroy_shared_region(shared_id));
            }
        }

        Ok(())
    }

    pub(crate) fn claim_local_handle(
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

    pub(crate) fn claim_shared_resource(
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

    pub(crate) fn release_local_handle(
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

    pub(crate) fn release_shared_resource(
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

    pub(crate) fn ensure_local_handle_owner(
        &self,
        process_id: ProcessId,
        capability: Capability,
        resource_class: ResourceClass,
        local_id: u64,
    ) -> std::result::Result<(), selium_abi::AbiError> {
        if self
            .local_handle_owners
            .lock()
            .get(&(resource_class, local_id))
            .is_some_and(|owners| owners.contains(&process_id))
        {
            Ok(())
        } else {
            Err(selium_abi::AbiError::new(
                selium_abi::AbiErrorCode::PermissionDenied,
                format!("permission denied for capability {capability:?}"),
            ))
        }
    }

    pub(crate) fn require(
        &self,
        process_id: ProcessId,
        capability: Capability,
        resource_class: ResourceClass,
        resource_id: Option<ResourceIdentity>,
    ) -> std::result::Result<(), selium_abi::AbiError> {
        let tenant = self.process_tenant(process_id);
        let context = ScopeContext {
            tenant: tenant.clone(),
            uri: None, // Resource URI populated when known (discovery-driven attach)
            locality: LocalityScope::Cluster,
            resource_class: Some(resource_class),
            resource_id,
        };
        let allowed = self.authorises(process_id, capability.clone(), &context);
        if allowed {
            Ok(())
        } else {
            Err(selium_abi::AbiError::new(
                selium_abi::AbiErrorCode::PermissionDenied,
                format!(
                    "permission denied for capability {capability:?} (tenant: {tenant:?}, class: {:?}, identity: {resource_id:?})",
                    context.resource_class
                ),
            ))
        }
    }

    pub(crate) fn require_with_uri(
        &self,
        process_id: ProcessId,
        capability: Capability,
        resource_class: ResourceClass,
        resource_id: Option<ResourceIdentity>,
        uri: String,
    ) -> std::result::Result<(), selium_abi::AbiError> {
        let tenant = self.process_tenant(process_id);
        let context = ScopeContext {
            tenant: tenant.clone(),
            uri: Some(uri.clone()),
            locality: LocalityScope::Cluster,
            resource_class: Some(resource_class),
            resource_id,
        };
        let allowed = self.authorises(process_id, capability.clone(), &context);
        if allowed {
            Ok(())
        } else {
            Err(selium_abi::AbiError::new(
                selium_abi::AbiErrorCode::PermissionDenied,
                format!(
                    "permission denied for capability {capability:?} on {uri} (tenant: {tenant:?}, class: {:?}, identity: {resource_id:?})",
                    context.resource_class
                ),
            ))
        }
    }

    pub(crate) fn wake_process_task(&self, process_id: ProcessId, task_id: selium_abi::TaskId) {
        if let Some(mailbox) = self.mailboxes.lock().get(&process_id).cloned()
            && let Err(error) = mailbox.enqueue(task_id)
        {
            debug!(
                process_id,
                task_id,
                error = %error,
                "failed to enqueue guest task wake"
            );
            return;
        }
        self.poll_guest_until_stalled(process_id);
    }

    /// Records a guest task's interest in a generation advance on a region.
    ///
    /// Registrations are bounded: a parked task has exactly one outstanding
    /// interest per region, so a re-registration for the same task replaces
    /// its stale entry rather than accumulating duplicates.
    pub(crate) fn register_wait(
        &self,
        process_id: ProcessId,
        task_id: TaskId,
        region_id: u64,
        generation: u64,
    ) {
        let mut registry = self.wait_registry.lock();
        registry
            .entry((process_id, region_id))
            .or_default()
            .retain(|entry| entry.task_id != task_id);
        registry
            .entry((process_id, region_id))
            .or_default()
            .push(crate::runtime::WaitEntry {
                process_id,
                task_id,
                region_id,
                generation,
            });
    }

    /// Drops all wait registrations for a task. Called when the task is
    /// woken: a running task holds no park interests, and it re-registers
    /// via `register_wait` if it parks again. Without this, entries for
    /// regions the host never advances (guest-writable rings) would
    /// accumulate forever.
    fn cancel_waits_for_task(&self, process_id: ProcessId, task_id: TaskId) {
        let mut registry = self.wait_registry.lock();
        registry.retain(|_key, entries| {
            // Task ids are guest-local: match on both process and task.
            entries.retain(|entry| !(entry.process_id == process_id && entry.task_id == task_id));
            !entries.is_empty()
        });
    }

    /// Kicks all active network outbound proxy threads by notifying their
    /// condvar wait keys. Called on guest→host transitions to ensure the
    /// outbound drain runs promptly after a guest write.
    ///
    /// Notified under the lock (no snapshot clone on this hot path): waiters
    /// never take `network_wait_keys` when waking, so holding it across
    /// `host_notify` cannot deadlock.
    pub fn kick_network_waiters(&self) {
        for (_shared_id, key) in self.network_wait_keys.lock().iter() {
            selium_memory::host_notify(*key, 1);
        }
    }

    /// Called when the host advances a region's generation. Checks the wait
    /// registry and wakes any guest tasks whose registered generation has
    /// been surpassed.
    pub fn note_generation_advance(&self, region_id: u64, new_generation: u64) {
        let mut wakeups: Vec<(ProcessId, TaskId)> = Vec::new();
        {
            let mut registry = self.wait_registry.lock();
            registry.retain(|_key, entries| {
                entries.retain(|entry| {
                    if entry.region_id == region_id && entry.generation < new_generation {
                        wakeups.push((entry.process_id, entry.task_id));
                        false // remove matched entries
                    } else {
                        true
                    }
                });
                !entries.is_empty()
            });
        }
        // Cross-thread wakes (kernel poller threads) are safe here: see the
        // memory-model contract on `poll_guest_until_stalled`.
        let mut seen = std::collections::HashSet::new();
        for (process_id, task_id) in wakeups {
            if !seen.insert((process_id, task_id)) {
                continue;
            }
            self.cancel_waits_for_task(process_id, task_id);
            self.wake_process_task(process_id, task_id);
        }
    }

    pub(crate) fn module_bytes(&self, module_id: &str) -> Result<Vec<u8>> {
        self.module_registry
            .lock()
            .get(module_id)
            .cloned()
            .ok_or_else(|| Error::UnknownModule(module_id.to_string()))
    }

    /// Executes the guest reactor until it stalls.
    ///
    /// # Memory-model contract (single-entry invariant)
    ///
    /// Guest reactor state (task lists, waker queues, generation-wait map)
    /// lives in the guest instance's linear memory and is accessed without
    /// synchronisation. Correctness therefore requires that **at most one
    /// thread executes a given guest's WASM at a time**; that invariant is
    /// provided by [`Self::try_begin_guest_exec`] / `end_guest_exec` around
    /// every poll, plus exclusive removal of the [`LoadedGuest`] from its
    /// registry for the duration of each poll.
    ///
    /// Any thread may deliver a wake ([`Self::wake_process_task`]) from any
    /// thread: the mailbox is shared linear memory with a flag handshake.
    /// Callers that lose the execution-guard race return immediately; the
    /// guard holder re-checks pending mailbox state *after* releasing the
    /// guard, so a wake racing an in-flight poll is delivered by one of the
    /// two threads. No wake is lost.
    pub(crate) fn poll_guest_until_stalled(&self, process_id: ProcessId) {
        loop {
            if !self.try_begin_guest_exec(process_id) {
                return;
            }
            let polled = self.poll_guest_once(process_id);
            self.end_guest_exec(process_id);
            // Check outside the guard: if a wake lands after this point,
            // the waking thread acquires the free guard and polls itself.
            if !polled || !self.has_pending_wake(process_id) {
                return;
            }
        }
    }

    /// Runs one reactor pass. Returns false when no progress is possible —
    /// the guest is not loaded, or `__selium_guest_poll` trapped — so the
    /// caller must not keep looping on pending mailbox state (the guest
    /// cannot clear it).
    fn poll_guest_once(&self, process_id: ProcessId) -> bool {
        let Some(mut loaded_guest) = self.loaded_guests.lock().remove(&process_id) else {
            return false;
        };
        let result =
            loaded_guest
                .app
                .call_function(loaded_guest.module_index, "__selium_guest_poll", &[]);
        self.loaded_guests.lock().insert(process_id, loaded_guest);
        // Kick outbound network proxies on reactor stall — the guest may
        // have written outbound frames before parking.
        self.kick_network_waiters();
        match result {
            Ok(_) => true,
            Err(error) => {
                debug!(
                    process_id,
                    error = %error,
                    "guest poll after mailbox wake failed"
                );
                false
            }
        }
    }

    /// Registers the process that receives from a host queue, so kernel-side
    /// sends can wake it (see `wake_queue_waiter`).
    pub(crate) fn register_queue_waiter(&self, queue_local_id: u64, process_id: ProcessId) {
        self.queue_waiters.lock().insert(queue_local_id, process_id);
    }

    /// Wakes the guest task(s) parked receiving from `queue_local_id`.
    /// Called after a kernel-side `host_queue_send` (e.g. the network
    /// poller enqueuing an accepted connection): without this the parked
    /// `HostQueueRecvWait` would never be re-polled.
    ///
    /// Each waiter is woken through the mailbox so the exact parked guest
    /// task re-polls its hostcall; a queued item is visible to that poll.
    pub(crate) fn wake_queue_waiter(&self, queue_local_id: u64) {
        let targets: Vec<(ProcessId, TaskId)> = {
            let operations = self.operations.lock();
            operations
                .values()
                .filter_map(|operation| match operation.state {
                    HostOperationState::HostQueueRecvWait { local_id, .. }
                        if local_id == queue_local_id =>
                    {
                        operation
                            .task_id
                            .map(|task_id| (operation.process_id, task_id))
                    }
                    _ => None,
                })
                .collect()
        };
        if targets.is_empty() {
            // No tracked per-task waiter: poll the queue's owning process
            // directly.
            let process_id = self.queue_waiters.lock().get(&queue_local_id).copied();
            if let Some(process_id) = process_id {
                self.poll_guest_until_stalled(process_id);
            }
            return;
        }
        for (process_id, task_id) in targets {
            self.wake_process_task(process_id, task_id);
        }
    }

    /// Marks a guest as being executed. Returns false if another thread is
    /// currently inside this guest's reactor.
    fn try_begin_guest_exec(&self, process_id: ProcessId) -> bool {
        self.executing_guests.lock().insert(process_id)
    }

    fn end_guest_exec(&self, process_id: ProcessId) {
        self.executing_guests.lock().remove(&process_id);
    }

    /// Returns true if the process's mailbox holds at least one undelivered
    /// task wake (flag handshake: set by `enqueue`, cleared by guest drain).
    /// A stopped/unknown process has no pending wakes.
    fn has_pending_wake(&self, process_id: ProcessId) -> bool {
        let Some(mailbox) = self.mailboxes.lock().get(&process_id).cloned() else {
            return false;
        };
        let Ok(memory) = mailbox.memory.lock() else {
            return false;
        };
        memory
            .read_u32(mailbox.base + selium_abi::mailbox::FLAG_OFFSET as u32)
            .is_ok_and(|flag| flag != 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mailbox::GuestMailbox;
    use crate::{ReadinessCondition, Runtime, SystemGuestDescriptor};
    use selium_abi::{LocalityScope, MeteringObservation, ResourceSelector};
    use std::sync::Arc;
    use wasmtiny::runtime::{Limits, Memory as WasmMemory, MemoryType};

    /// Registers a mailbox for `process_id` backed by scratch linear memory
    /// so wake-delivery mechanics can be exercised without a real guest.
    fn install_scratch_mailbox(runtime: &Runtime, process_id: ProcessId) -> Arc<GuestMailbox> {
        let mem_type = MemoryType {
            limits: Limits::Min(1),
            shared: false,
        };
        let memory = Arc::new(std::sync::Mutex::new(
            WasmMemory::new(mem_type).expect("scratch memory"),
        ));
        let mailbox = Arc::new(crate::mailbox::GuestMailbox::new(memory.clone(), 0));
        runtime.register_mailbox(process_id, mailbox.clone());
        mailbox
    }

    /// Task 2.3: a wake delivered from another thread while the execution
    /// guard is held must not be lost — the mailbox flag stays set and the
    /// post-release re-check observes it.
    #[test]
    fn wake_while_guard_held_is_not_lost() {
        let runtime = Arc::new(Runtime::default());
        let pid: ProcessId = 7;
        let _mailbox = install_scratch_mailbox(&runtime, pid);

        // Hold the execution guard, simulating an in-flight reactor poll.
        assert!(
            runtime.try_begin_guest_exec(pid),
            "guard must be initially free"
        );

        // Deliver a wake from another thread while the guard is held.
        let rt = runtime.clone();
        let handle = std::thread::spawn(move || {
            rt.wake_process_task(pid, 1);
        });
        handle.join().expect("waker thread");

        // The waking thread must not have blocked on the guard, and the
        // wake must be pending in the mailbox (flag set by enqueue).
        assert!(
            !runtime.try_begin_guest_exec(pid),
            "our guard must still be held"
        );
        assert!(
            runtime.has_pending_wake(pid),
            "wake enqueued under contention must remain pending"
        );

        // Release the guard; the pending-wake re-check path must observe
        // the flag. Clear it manually (no real reactor to consume it) and
        // confirm the poll terminates cleanly instead of looping forever.
        runtime.end_guest_exec(pid);
        assert!(runtime.has_pending_wake(pid));
        {
            let mailboxes = runtime.mailboxes.lock();
            let mb = mailboxes.get(&pid).expect("mailbox registered");
            mb.memory
                .lock()
                .expect("memory lock")
                .write_u32(selium_abi::mailbox::FLAG_OFFSET as u32, 0)
                .expect("clear flag");
        }
        runtime.poll_guest_until_stalled(pid);
    }

    /// Task 4.2: many threads deliver wakes to one guest concurrently.
    /// Every wake must be enqueued exactly once (tail counts monotonically)
    /// and the runtime must stay consistent — no lost or corrupted wakes.
    #[test]
    fn concurrent_wake_delivery_never_loses_wakes() {
        let runtime = Arc::new(Runtime::default());
        let pid: ProcessId = 9;
        install_scratch_mailbox(&runtime, pid);

        const THREADS: usize = 8;
        const WAKES_PER_THREAD: usize = 50;

        let mut handles = Vec::new();
        for _ in 0..THREADS {
            let rt = runtime.clone();
            handles.push(std::thread::spawn(move || {
                for task in 0..WAKES_PER_THREAD {
                    rt.wake_process_task(pid, task as u32);
                }
            }));
        }
        for handle in handles {
            handle.join().expect("waker thread");
        }

        // Total delivered wakes == THREADS * WAKES_PER_THREAD, observable
        // as the mailbox tail counter.
        let mailboxes = runtime.mailboxes.lock();
        let mb = mailboxes.get(&pid).expect("mailbox registered");
        let tail = mb
            .memory
            .lock()
            .expect("memory lock")
            .read_u32(0 + selium_abi::mailbox::TAIL_OFFSET as u32)
            .expect("read tail");
        assert_eq!(
            tail as usize,
            THREADS * WAKES_PER_THREAD,
            "every concurrent wake must be enqueued exactly once"
        );
    }

    fn module_with_entrypoint(entrypoint: &str, body: &str) -> Vec<u8> {
        wat::parse_str(format!("(module (func (export \"{entrypoint}\") {body}))"))
            .expect("compile wat")
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
                tenant: None,
                well_known_uri: None,
                handlers: Vec::new(),
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
                .processes()
                .metering_observation(bootstrapped.process_id)
                .expect("metering")
                .cpu_micros,
            11
        );
    }

    #[test]
    fn cleanup_failed_process_preserves_co_owners() {
        let runtime = Runtime::default();

        let guest_a = runtime
            .spawn_system_guest(SystemGuestDescriptor {
                name: "owner-a".to_string(),
                module_id: "owner-a-module".to_string(),
                module_bytes: module_with_entrypoint("main", ""),
                entrypoint: "main".to_string(),
                arguments: Vec::new(),
                grants: vec![CapabilityGrant::new(
                    Capability::SharedMemory,
                    vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
                )],
                dependencies: Vec::new(),
                readiness: ReadinessCondition::Immediate,
                tenant: None,
                well_known_uri: None,
                handlers: Vec::new(),
            })
            .expect("spawn owner-a");

        let guest_b = runtime
            .spawn_system_guest(SystemGuestDescriptor {
                name: "owner-b".to_string(),
                module_id: "owner-b-module".to_string(),
                module_bytes: module_with_entrypoint("main", ""),
                entrypoint: "main".to_string(),
                arguments: Vec::new(),
                grants: vec![CapabilityGrant::new(
                    Capability::SharedMemory,
                    vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
                )],
                dependencies: Vec::new(),
                readiness: ReadinessCondition::Immediate,
                tenant: None,
                well_known_uri: None,
                handlers: Vec::new(),
            })
            .expect("spawn owner-b");

        // Allocate a shared region as guest_a.
        let (shared_id, _len) = runtime
            .kernel()
            .memory()
            .allocate_shared_region(64)
            .expect("allocate region");
        runtime.claim_shared_resource(guest_a.process_id, ResourceClass::SharedRegion, shared_id);
        // Simulate co-ownership: guest_b also owns the region.
        runtime.claim_shared_resource(guest_b.process_id, ResourceClass::SharedRegion, shared_id);

        // Cleanup guest_a (simulate failure).
        runtime
            .cleanup_failed_process(guest_a.process_id)
            .expect("cleanup");

        // guest_b should still own the region.
        let still_owns = runtime
            .shared_resource_owners
            .lock()
            .get(&(ResourceClass::SharedRegion, shared_id))
            .is_some_and(|owners| owners.contains(&guest_b.process_id));
        assert!(
            still_owns,
            "co-owner b should retain ownership after a fails"
        );
    }
}
