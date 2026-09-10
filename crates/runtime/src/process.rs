use std::collections::HashSet;

use selium_abi::{
    ActivityEvent, Capability, CapabilityGrant, LocalityScope, ProcessId, ResourceClass,
    ResourceIdentity, ResourceSelector, ScopeContext, TaskId,
};
use selium_service::DiscoveryRequest;
use tracing::debug;
use wasmtiny::WasmValue;

use crate::{
    Error, Result, config::ProcessAuthority, hostcall::HostOperationState, runtime::Runtime,
};

impl Runtime {
    /// Stops a process and releases runtime-owned state for it.
    ///
    /// Idempotent for teardown retries: if a previous stop failed part-way
    /// (e.g. a discovery publish error), the process authority is retained
    /// and the stop can be retried to complete the remaining revocations.
    pub fn stop_process(&self, process_id: selium_abi::ProcessId) -> Result<()> {
        // A retry of a partially torn-down process finds the kernel process
        // already stopped (or already reaped, when the failure path was
        // `cleanup_failed_process`); both are expected, not errors.
        if let Err(error) = self.kernel.processes().stop_process(process_id) {
            let resuming_teardown = matches!(error, selium_kernel::Error::ProcessStopped(_))
                || (matches!(error, selium_kernel::Error::NotFound(_))
                    && self.process_authorities.lock().contains_key(&process_id));
            if !resuming_teardown {
                return Err(error.into());
            }
        }
        self.loaded_guests.lock().remove(&process_id);
        // Capture the authority before it is dropped so cleanup can revoke
        // the process's typed URIs, and re-insert it if teardown fails so a
        // retry re-enters the cleanup path.
        let authority = self.process_authorities.lock().remove(&process_id);
        if let Some(authority) = authority {
            self.operations
                .lock()
                .retain(|_, operation| operation.process_id != process_id);
            self.mailboxes.lock().remove(&process_id);
            if let Err(error) = self.cleanup_process_resources(process_id, authority.tenant.clone())
            {
                self.process_authorities
                    .lock()
                    .insert(process_id, authority);
                return Err(error);
            }
        }
        self.local_handle_owners
            .lock()
            .remove(&(ResourceClass::Process, process_id));
        // If the discovery service stopped, drop its recorded identity so
        // `RecordResolvedQueueFor` is no longer accepted from any caller.
        if *self.discovery_process.lock() == Some(process_id) {
            *self.discovery_process.lock() = None;
        }
        // An already-reaped process (retry after `cleanup_failed_process`)
        // reports NotFound; the kernel state is already reclaimed.
        if let Err(error) = self.kernel.processes().reap_process(process_id)
            && !matches!(error, selium_kernel::Error::NotFound(_))
        {
            return Err(error.into());
        }
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
    /// Empty selector list = unrestricted within the capability — except
    /// `DelegateGrants`: an empty selector list would vacuously match every
    /// tenant (including root) and grant global cross-tenant delegation
    /// authority, so a `DelegateGrants` grant MUST carry at least one
    /// `Tenant` selector.
    pub(crate) fn validate_grants(&self, grants: &[CapabilityGrant]) -> Result<()> {
        for grant in grants {
            if grant.capability == Capability::DelegateGrants
                && !grant
                    .selectors
                    .iter()
                    .any(|selector| matches!(selector, ResourceSelector::Tenant(_)))
            {
                return Err(Error::InvalidGrant(grant.capability.clone()));
            }
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
        let process_tenant = self.process_tenant(process_id);
        // Discovery revocations stage their bookkeeping: a failed publish
        // leaves the pending entries intact. If teardown failed, retain the
        // process authority so a later `stop_process` retry can complete the
        // remaining revocations (its kernel record is already reaped below,
        // which `stop_process` tolerates for retained authorities).
        let cleanup_failed = self
            .cleanup_process_resources(process_id, process_tenant)
            .is_err();
        drop(self.kernel.processes().reap_process(process_id));
        if !cleanup_failed {
            self.process_authorities.lock().remove(&process_id);
        }
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

    pub(crate) fn cleanup_process_resources(
        &self,
        process_id: ProcessId,
        process_tenant: Option<String>,
    ) -> Result<()> {
        // The process's fast-path capability vote is moot once it is gone.
        self.process_fastpath.lock().remove(&process_id);

        // Its region attachments are moot too: drop it from every
        // attachment set so a later spawn of the same id cannot inherit
        // wake-registration rights for regions it never attached.
        {
            let mut region_attachments = self.region_attachments.lock();
            for attachers in region_attachments.values_mut() {
                attachers.remove(&process_id);
            }
            region_attachments.retain(|_, attachers| !attachers.is_empty());
        }

        // Revoke the process node first, so the process becomes unresolvable
        // before its resources are reclaimed.
        let tenant = process_tenant.as_deref().unwrap_or_default();
        let node_uri = crate::discovery::process_registration_uri(tenant, process_id);
        let request = DiscoveryRequest::Revoke { uri: node_uri };
        self.publish_discovery_event(request)?;

        // Revoke all region registrations minted for this process, publishing
        // Revoke operations to the discovery feed under the serving tenant.
        // Staged: each `region_tenants` entry is removed only after its
        // revocation publishes successfully, so a failed teardown can be
        // retried without losing bookkeeping.
        let region_keys: Vec<(ProcessId, u64)> = self
            .region_tenants
            .lock()
            .keys()
            .filter(|(pid, _)| *pid == process_id)
            .copied()
            .collect();
        for key in region_keys {
            let serving_tenant = self.region_tenants.lock().get(&key).cloned();
            if let Some(serving_tenant) = serving_tenant {
                let uri = crate::discovery::region_registration_uri(&serving_tenant, key.1);
                let request = DiscoveryRequest::Revoke { uri };
                self.publish_discovery_event(request)?;
                self.region_tenants.lock().remove(&key);
            }
        }

        // Owner-keyed revocation: ask discovery to revoke every route the
        // process registered itself (via `serve`). There is no runtime side
        // map of guest-registered routes — discovery records each route's
        // owner and revokes them on this event.
        let request = DiscoveryRequest::RevokeByOwner { process_id };
        self.publish_discovery_event(request)?;
        self.process_registrations.lock().remove(&process_id);

        // Revoke tier-1 registrations for host queues created by this
        // process, under the principal tenant each queue was minted for.
        // Staged like the region revocations above.
        let queue_keys: Vec<(ProcessId, u64)> = self
            .queue_tenants
            .lock()
            .keys()
            .filter(|(pid, _)| *pid == process_id)
            .copied()
            .collect();
        for key in queue_keys {
            let principal = self.queue_tenants.lock().get(&key).cloned();
            if let Some(principal) = principal {
                let request = DiscoveryRequest::Revoke {
                    uri: crate::discovery::queue_registration_uri(&principal, key.1),
                };
                self.publish_discovery_event(request)?;
                self.queue_tenants.lock().remove(&key);
            }
        }

        // Drop the protocol-handler pinning registry for this process.
        self.handler_schemes.lock().remove(&process_id);

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

    /// Records that `process_id` mapped the shared region `region_id`
    /// (at `AttachRegion`), so its parked readers can register generation
    /// wakes on it (see [`crate::runtime::Runtime::region_attachments`]).
    pub(crate) fn record_region_attachment(&self, process_id: ProcessId, region_id: u64) {
        self.region_attachments
            .lock()
            .entry(region_id)
            .or_default()
            .insert(process_id);
    }

    /// Returns whether `process_id` owns or has attached the shared
    /// region `region_id` — both carry a live mapping, so both may
    /// register generation waits and announce generation advances on it.
    pub(crate) fn owns_or_attached_region(&self, process_id: ProcessId, region_id: u64) -> bool {
        let owns = self
            .shared_resource_owners
            .lock()
            .get(&(ResourceClass::SharedRegion, region_id))
            .is_some_and(|owners| owners.contains(&process_id));
        let attached = self
            .region_attachments
            .lock()
            .get(&region_id)
            .is_some_and(|attachers| attachers.contains(&process_id));
        owns || attached
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
        // Fast-path eligibility is per-attachment: the departing process
        // no longer votes. Remaining all-capable attachers keep the fast
        // path; an empty voter set drops the region entry entirely.
        if resource_class == &ResourceClass::SharedRegion {
            let mut attachments = self.fast_path_attachments.lock();
            if let Some(voters) = attachments.get_mut(&shared_id) {
                voters.remove(&process_id);
                if voters.is_empty() {
                    attachments.remove(&shared_id);
                }
            }
        }
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

    /// Kicks active network outbound proxy threads by notifying the unified
    /// region waiter registry on each ring's generation word. Called on
    /// guest→host transitions to ensure the outbound drain runs promptly after
    /// a guest write.
    ///
    /// Regions with the shared-page fast path active are skipped: the writer
    /// guest's `memory.atomic.notify` already wakes the drainer directly, so a
    /// transition kick would be redundant. A missed kick can never stall a
    /// proxy — the bounded-timeout backstop in the drain loop re-checks.
    pub fn kick_network_waiters(&self) {
        let memory = self.kernel.memory();
        let fast_path = self.fast_path_attachments.lock().clone();
        for (shared_id, generation_offset) in self.network_wait_keys.lock().iter() {
            if region_fast_path_active(&fast_path, *shared_id) {
                continue;
            }
            drop(memory.notify_region(*shared_id, *generation_offset, 1));
            *self.kick_counts.lock().entry(*shared_id).or_insert(0) += 1;
        }
    }

    /// True when the shared-page fast path is active for `shared_id`: every
    /// attaching process's guest module is fast-path capable (shared memory
    /// declaration + atomic notify opcodes; see `module_probe`) and the
    /// engine advertises its per-region wait registry. Detection, not
    /// configuration — see the `shared-page-fastpath` capability spec.
    pub fn fast_path_region_active(&self, shared_id: u64) -> bool {
        let attachments = self.fast_path_attachments.lock().clone();
        region_fast_path_active(&attachments, shared_id)
    }

    /// Shared ids of the regions with active network outbound proxies (the
    /// kick targets), for observability and tests.
    pub fn network_wait_regions(&self) -> Vec<u64> {
        self.network_wait_keys
            .lock()
            .iter()
            .map(|(shared_id, _)| *shared_id)
            .collect()
    }

    /// Total guest→host transition kicks **delivered** since runtime
    /// creation (suppressed regions are not counted).
    pub fn kick_count(&self) -> u64 {
        self.kick_counts.lock().values().sum()
    }

    /// Guest→host transition kicks delivered to `shared_id`. Zero for a
    /// fast-path region means every wake was carried by the guest's atomic
    /// notify — the end-to-end fast-path assertion.
    pub fn region_kick_count(&self, shared_id: u64) -> u64 {
        self.kick_counts
            .lock()
            .get(&shared_id)
            .copied()
            .unwrap_or(0)
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

    /// Records a guest module's fast-path capability, probed from its module
    /// bytes at spawn (see `module_probe`). Consumed at region attach.
    pub(crate) fn record_process_fastpath(&self, process_id: ProcessId, capable: bool) {
        self.process_fastpath.lock().insert(process_id, capable);
    }

    /// True when the process's guest module is fast-path capable. Unknown
    /// processes (never spawned, or already cleaned up) are not — the
    /// portable kick path is the safe default.
    pub(crate) fn process_fastpath_capable(&self, process_id: ProcessId) -> bool {
        self.process_fastpath
            .lock()
            .get(&process_id)
            .copied()
            .unwrap_or(false)
    }

    /// Records the per-attachment fast-path eligibility vote for
    /// `(region_id, process_id)` at attach time. `capable` combines the
    /// engine's registry support with the attaching process's module probe;
    /// an incapable attacher votes `false` so the region's fast path stays
    /// off until every attacher is capable.
    pub(crate) fn record_fast_path_attachment(
        &self,
        region_id: u64,
        process_id: ProcessId,
        capable: bool,
    ) {
        self.fast_path_attachments
            .lock()
            .entry(region_id)
            .or_default()
            .insert(process_id, capable);
    }

    /// Drops all fast-path eligibility for a destroyed region. Called when
    /// the region is freed: every attachment is force-detached, so every
    /// vote is stale.
    pub(crate) fn clear_fast_path_attachments(&self, region_id: u64) {
        self.fast_path_attachments.lock().remove(&region_id);
    }
}

/// True when the per-attachment eligibility map marks every attacher of
/// `shared_id` as fast-path capable (and at least one attacher exists). See
/// [`Runtime::fast_path_region_active`].
fn region_fast_path_active(
    attachments: &std::collections::HashMap<u64, std::collections::HashMap<ProcessId, bool>>,
    shared_id: u64,
) -> bool {
    attachments
        .get(&shared_id)
        .is_some_and(|voters| !voters.is_empty() && voters.values().all(|capable| *capable))
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
            .read_u32(selium_abi::mailbox::TAIL_OFFSET as u32)
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
                serving_role: None,
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
                serving_role: None,
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
                serving_role: None,
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

    /// Teardown support: replaces the runtime's discovery publisher with one
    /// over a full Park channel, so every synchronous publish surfaces
    /// `BufferFull` instead of silently succeeding (the real feed is a Drop
    /// channel, which never fails). Returns the original publisher.
    fn swap_in_failing_publisher(runtime: &Runtime) -> Option<crate::runtime::DiscoveryPublisher> {
        let full_channel = selium_shm::Channel::create_with_backpressure(
            64,
            selium_shm::ChannelBackpressure::Park,
            selium_abi::ResourceKind::PubSubTopic,
        )
        .expect("park channel");
        let transport = selium_shm::transport::ShmTransport::new(&full_channel, &full_channel)
            .expect("transport");
        // Fill the ring so any further write parks (surfaced as BufferFull
        // by the synchronous write path).
        let mut filler = selium_wire::framed::FramedWrite::new(transport);
        while filler.write_frame(b"x", 0).is_ok() {}
        let publisher = selium_wire::pubsub::Publisher::new(filler);
        (*runtime.discovery_publisher.lock()).replace(publisher)
    }

    /// Spawns a tenant-scoped guest so teardown has a registered process
    /// node to revoke.
    fn spawn_tenant_guest(runtime: &Runtime, name: &str, tenant: &str) -> selium_abi::ProcessId {
        runtime
            .spawn_system_guest(SystemGuestDescriptor {
                name: name.to_string(),
                module_id: format!("{name}-module"),
                module_bytes: wat::parse_str("(module (func (export \"boot\")))").expect("wat"),
                entrypoint: "boot".to_string(),
                arguments: Vec::new(),
                grants: Vec::new(),
                dependencies: Vec::new(),
                readiness: ReadinessCondition::Immediate,
                tenant: Some(tenant.to_string()),
                serving_role: None,
                handlers: Vec::new(),
            })
            .expect("spawn guest")
            .process_id
    }

    /// Installs a fresh working publisher and returns a subscriber attached
    /// to its channel, so a retried teardown's revocations are observable.
    fn swap_in_working_publisher(
        runtime: &Runtime,
    ) -> selium_wire::pubsub::Subscriber<DiscoveryRequest, selium_shm::transport::ShmTransport>
    {
        let channel = selium_shm::Channel::create_with_backpressure(
            64 * 1024,
            selium_shm::ChannelBackpressure::Drop,
            selium_abi::ResourceKind::PubSubTopic,
        )
        .expect("channel");
        let writer =
            selium_shm::transport::ShmTransport::new(&channel, &channel).expect("writer transport");
        let reader =
            selium_shm::transport::ShmTransport::new(&channel, &channel).expect("reader transport");
        let publisher =
            selium_wire::pubsub::Publisher::new(selium_wire::framed::FramedWrite::new(writer));
        *runtime.discovery_publisher.lock() = Some(publisher);
        selium_wire::pubsub::Subscriber::new(
            selium_wire::framed::FramedRead::new(reader),
            Some(channel.ring().capacity()),
        )
    }

    /// Drains every discovery event currently readable from the subscriber.
    fn drain_feed(
        subscriber: &mut selium_wire::pubsub::Subscriber<
            DiscoveryRequest,
            selium_shm::transport::ShmTransport,
        >,
    ) -> Vec<DiscoveryRequest> {
        let mut events = Vec::new();
        loop {
            match subscriber.read_with_tag() {
                Ok((request, _tag)) => events.push(request),
                Err(selium_wire::error::Error::BufferEmpty) => break,
                Err(error) => panic!("feed read failed: {error}"),
            }
        }
        events
    }

    /// A discovery publish failure during `stop_process` must fail the stop
    /// (rather than silently skipping revocations), keep the process
    /// authority so the stop can be retried, and leave the pending
    /// revocations staged so the retry publishes them.
    #[test]
    fn stop_process_teardown_is_retryable_after_publish_failure() {
        let runtime = Runtime::default();
        runtime
            .bootstrap_system_guests(crate::RuntimeConfig {
                start_discovery: true,
                system_guests: Vec::new(),
                domain_table: Vec::new(),
            })
            .expect("bootstrap discovery");
        let pid = spawn_tenant_guest(&runtime, "retry-guest", "acme");

        // Every publish now fails: teardown cannot revoke the process node.
        swap_in_failing_publisher(&runtime);
        let stopped = runtime.stop_process(pid);
        assert!(stopped.is_err(), "teardown must fail while publishes fail");
        // The authority is retained so the stop is retryable, not lost.
        assert_eq!(
            runtime.process_tenant(pid).as_deref(),
            Some("acme"),
            "authority must be retained for a teardown retry"
        );

        // Install a working publisher and retry: the stop completes and the
        // process-node revocation is published.
        let mut subscriber = swap_in_working_publisher(&runtime);
        runtime.stop_process(pid).expect("retry stop");
        let events = drain_feed(&mut subscriber);
        let revoked = events
            .iter()
            .filter_map(|event| match event {
                DiscoveryRequest::Revoke { uri } => Some(uri.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert!(
            revoked.contains(&crate::discovery::process_registration_uri("acme", pid)),
            "expected the process-node revocation after retry, got: {revoked:?}"
        );
        assert!(
            runtime.process_tenant(pid).is_none(),
            "authority must be dropped after a successful teardown"
        );
    }

    /// A publish failure during `cleanup_failed_process` retains the process
    /// authority (best-effort teardown still reclaims what it can), and a
    /// later `stop_process` retry completes the pending revocations even
    /// though the kernel record is already reaped.
    #[test]
    fn cleanup_failed_process_retains_authority_for_teardown_retry() {
        let runtime = Runtime::default();
        runtime
            .bootstrap_system_guests(crate::RuntimeConfig {
                start_discovery: true,
                system_guests: Vec::new(),
                domain_table: Vec::new(),
            })
            .expect("bootstrap discovery");
        let pid = spawn_tenant_guest(&runtime, "failed-guest", "acme");

        // Every publish now fails: the failed-process teardown cannot revoke
        // the process node, but stays best-effort.
        swap_in_failing_publisher(&runtime);
        runtime
            .cleanup_failed_process(pid)
            .expect("best-effort cleanup");
        assert_eq!(
            runtime.process_tenant(pid).as_deref(),
            Some("acme"),
            "authority must be retained for a teardown retry"
        );

        // A later stop completes the pending revocation, tolerating the
        // already-reaped kernel record.
        let mut subscriber = swap_in_working_publisher(&runtime);
        runtime.stop_process(pid).expect("retry stop");
        let events = drain_feed(&mut subscriber);
        let revoked = events
            .iter()
            .filter_map(|event| match event {
                DiscoveryRequest::Revoke { uri } => Some(uri.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert!(
            revoked.contains(&crate::discovery::process_registration_uri("acme", pid)),
            "expected the process-node revocation after retry, got: {revoked:?}"
        );
    }

    /// A `DelegateGrants` grant with no `Tenant` selector would vacuously
    /// match every scope context (including root), granting global
    /// cross-tenant delegation authority; validation rejects it.
    #[test]
    fn delegate_grants_requires_a_tenant_selector() {
        let runtime = Runtime::default();

        let rejected = runtime.spawn_system_guest(SystemGuestDescriptor {
            name: "bad-delegator".to_string(),
            module_id: "bad-delegator-module".to_string(),
            module_bytes: wat::parse_str("(module (func (export \"boot\")))").expect("wat"),
            entrypoint: "boot".to_string(),
            arguments: Vec::new(),
            grants: vec![CapabilityGrant::new(Capability::DelegateGrants, Vec::new())],
            dependencies: Vec::new(),
            readiness: ReadinessCondition::Immediate,
            tenant: None,
            serving_role: None,
            handlers: Vec::new(),
        });
        assert!(
            matches!(
                rejected,
                Err(crate::Error::InvalidGrant(grant)) if grant == Capability::DelegateGrants
            ),
            "selector-less DelegateGrants must be rejected"
        );

        // A tenant-scoped delegation grant is admitted.
        let admitted = runtime.spawn_system_guest(SystemGuestDescriptor {
            name: "scoped-delegator".to_string(),
            module_id: "scoped-delegator-module".to_string(),
            module_bytes: wat::parse_str("(module (func (export \"boot\")))").expect("wat"),
            entrypoint: "boot".to_string(),
            arguments: Vec::new(),
            grants: vec![CapabilityGrant::new(
                Capability::DelegateGrants,
                vec![ResourceSelector::Tenant("acme".to_string())],
            )],
            dependencies: Vec::new(),
            readiness: ReadinessCondition::Immediate,
            tenant: None,
            serving_role: None,
            handlers: Vec::new(),
        });
        assert!(admitted.is_ok(), "tenant-scoped DelegateGrants is admitted");
    }
}
