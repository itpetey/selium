//! Process lifecycle, authority, and resource ownership.
//!
//! The runtime's process record is the authority root: spawn records grants and
//! the tenant, teardown revokes discovery registrations and reclaims quota,
//! and every capability check reads back from that record. Metering, resource
//! handles, scheduling/wakes, and fast-path detection live in sibling modules
//! but hang off the same `Runtime`.

use std::collections::HashSet;

use selium_abi::{
    Capability, CapabilityGrant, ProcessId, ResourceClass, ResourceIdentity, ResourceSelector,
    ScopeContext,
};
use selium_service::DiscoveryRequest;

use crate::{Error, Result, Runtime, config::ProcessAuthority};

mod fastpath;
mod metering;
mod resources;
mod scheduling;
#[cfg(test)]
mod tests;

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
        // A multithreaded guest's worker pool must be stopped and joined
        // before its resources are reclaimed. Cooperative guests are simply
        // unloaded (they are never polled again). The `loaded_guests` guard is
        // released before `stop_and_join` so a wedged pool's stop spin (up to
        // `STOP_BUDGET`) never holds the map lock — which wake delivery and
        // spawn also take.
        let multithreaded = self
            .loaded_guests
            .lock()
            .remove(&process_id)
            .and_then(|guest| match guest.execution {
                crate::bootstrap::GuestExecution::Multithreaded(mt) => Some(mt),
                _ => None,
            });
        if let Some(mt) = multithreaded {
            mt.stop_and_join(self, process_id);
        }
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
        self.resource_tenants
            .lock()
            .remove(&(ResourceClass::Process, process_id));
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

    pub(crate) fn persist_process_authority(
        &self,
        process_id: ProcessId,
        grants: Vec<CapabilityGrant>,
        tenant: Option<String>,
        parent: Option<ProcessId>,
    ) {
        // A process is a resource the host minted: record its mint-time tenant
        // synchronously so the authority read answers before the process has
        // published its own node.
        self.resource_tenants.lock().insert(
            (ResourceClass::Process, process_id),
            tenant.clone().unwrap_or_default(),
        );
        self.process_authorities.lock().insert(
            process_id,
            ProcessAuthority {
                grants,
                tenant,
                parent,
                resolved_resources: HashSet::new(),
            },
        );
    }

    /// Returns the exact reserved URIs the host provisioned `process_id` a
    /// `RouteClaim` for.
    ///
    /// The host knows the expectation independently of discovery publication,
    /// so a claimed name that was never published is observable as drift while
    /// the process stays fully accounted.
    pub fn expected_claims(&self, process_id: ProcessId) -> Vec<String> {
        self.process_authorities
            .lock()
            .get(&process_id)
            .map(|authority| {
                authority
                    .grants
                    .iter()
                    .filter(|grant| grant.capability == Capability::RouteClaim)
                    .flat_map(|grant| {
                        grant
                            .selectors
                            .iter()
                            .filter_map(|selector| match selector {
                                ResourceSelector::Uri(uri) => Some(uri.clone()),
                                _ => None,
                            })
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Answers the authoritative ownership question for `(class, id)` from the
    /// host's mint-time records.
    ///
    /// When the queried class is not owned, the answer lists the classes the
    /// process *does* own for the same id, so admission can distinguish a
    /// class mismatch (a permission failure) from an identity the caller does
    /// not own at all.
    pub fn resource_authority(
        &self,
        process_id: ProcessId,
        resource_class: ResourceClass,
        resource_id: u64,
    ) -> selium_abi::ResourceAuthority {
        if !self.owns_under(process_id, &resource_class, resource_id) {
            return selium_abi::ResourceAuthority {
                owned: false,
                tenant: None,
                other_classes: self.classes_owned_for(process_id, resource_id),
            };
        }
        let tenant = self
            .resource_tenants
            .lock()
            .get(&(resource_class, resource_id))
            .cloned()
            .or_else(|| self.process_tenant(process_id))
            .or_else(|| Some(String::new()));
        selium_abi::ResourceAuthority {
            owned: true,
            tenant,
            other_classes: Vec::new(),
        }
    }

    fn owns_under(
        &self,
        process_id: ProcessId,
        resource_class: &ResourceClass,
        resource_id: u64,
    ) -> bool {
        self.shared_resource_owners
            .lock()
            .get(&(resource_class.clone(), resource_id))
            .is_some_and(|owners| owners.contains(&process_id))
            || self
                .local_handle_owners
                .lock()
                .get(&(resource_class.clone(), resource_id))
                .is_some_and(|owners| owners.contains(&process_id))
    }

    fn classes_owned_for(&self, process_id: ProcessId, resource_id: u64) -> Vec<ResourceClass> {
        let mut classes: Vec<ResourceClass> = self
            .shared_resource_owners
            .lock()
            .iter()
            .filter(|((_, id), owners)| *id == resource_id && owners.contains(&process_id))
            .map(|((class, _), _)| class.clone())
            .collect();
        classes.extend(
            self.local_handle_owners
                .lock()
                .iter()
                .filter(|((_, id), owners)| *id == resource_id && owners.contains(&process_id))
                .map(|((class, _), _)| class.clone()),
        );
        classes.sort_by_key(ResourceClass::uri_segment);
        classes.dedup();
        classes
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
    /// `Tenant`, `Namespace`, `Children`.
    /// Admitted with constraints: `UriPrefix` (requires a network
    /// `ResourceClass` selector in the same grant).
    /// Empty selector list = unrestricted within the capability — except
    /// `DelegateGrants`: an empty selector list would vacuously match every
    /// tenant (including root) and grant global cross-tenant delegation
    /// authority, so a `DelegateGrants` grant MUST carry at least one
    /// tenant-scoping selector: a `Tenant`, a `Namespace::Tenant`, or an
    /// explicit `Namespace::Root` (root-wide delegation stays explicit and
    /// grantable rather than vacuous).
    pub(crate) fn validate_grants(&self, grants: &[CapabilityGrant]) -> Result<()> {
        for grant in grants {
            if grant.capability == Capability::DelegateGrants
                && !grant.selectors.iter().any(|selector| {
                    matches!(
                        selector,
                        ResourceSelector::Tenant(_) | ResourceSelector::Namespace(_)
                    )
                })
            {
                return Err(Error::InvalidGrant(grant.capability.clone()));
            }
            // A route claim grants one exact reserved name. A selector-less
            // grant would be vacuous, a multi-URI grant unsatisfiable under
            // intersection semantics, and a non-`Uri` selector could never be
            // evaluated as a name.
            if grant.capability == Capability::RouteClaim {
                let mut uris = grant
                    .selectors
                    .iter()
                    .filter_map(|selector| match selector {
                        ResourceSelector::Uri(uri) => Some(uri),
                        _ => None,
                    });
                let (Some(uri), None) = (uris.next(), uris.next()) else {
                    return Err(Error::InvalidGrant(grant.capability.clone()));
                };
                if grant.selectors.len() != 1 || !selium_abi::uri::is_root_uri(uri) {
                    return Err(Error::InvalidGrant(grant.capability.clone()));
                }
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
        // A failed guest is never polled again: unload it. Idempotent —
        // callers that already removed the `LoadedGuest` (e.g. the poll path)
        // find nothing here. As in `stop_process`, the `loaded_guests` guard
        // is released before `stop_and_join` so the wedged-pool stop spin does
        // not hold the map lock.
        let multithreaded = self
            .loaded_guests
            .lock()
            .remove(&process_id)
            .and_then(|guest| match guest.execution {
                crate::bootstrap::GuestExecution::Multithreaded(mt) => Some(mt),
                _ => None,
            });
        if let Some(mt) = multithreaded {
            mt.stop_and_join(self, process_id);
        }
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
        self.resource_tenants
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
        // Its metering accumulators are moot too.
        self.metering.lock().remove(process_id);
        // And so is its CPU budget window, so a later spawn reusing the id
        // cannot inherit an anchor from a dead process.
        self.cpu_budget_windows.lock().remove(&process_id);

        // Release the tenant-scoped process-quota slot reserved at spawn,
        // exactly once (the entry is removed here). Tenant-less processes and
        // never-ProcessStart-spawned system guests reserved no slot, so they
        // find no entry and release nothing.
        if let Some(tenant) = self.process_quota_tenants.lock().remove(&process_id) {
            self.kernel
                .quota()
                .release(&tenant, ResourceClass::Process, 1);
        }

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

        // The log-channel reader maps the guest's log region host-side. Release
        // it before the region reclaim pass: a region is only destroyed once its
        // mapping count reaches zero, so a mapping left here would keep the
        // guest's log region alive after the process is gone.
        self.kernel.processes().release_log_channel(process_id);

        // Release the guest-memory mappings this process took on shared
        // regions *before* the reclaim pass. The engine counts attachments and
        // only decrements them on an explicit detach with the same memory; the
        // guest instance is already unloaded by now, so the runtime recorded
        // the handles at attach time. Without this a stopped owner leaves every
        // region it mapped permanently un-destroyable (the soak lane's finding).
        self.detach_guest_region_mappings(Some(process_id), None);

        // A stopped guest never consumes a timer wake; drop its pending
        // deadlines so a long `Timer` does not outlive the process.
        self.cancel_process_timers(process_id);

        // A sender that died cannot withdraw the writer registration of an
        // RPC session it handed to a receiver, so the receiver's handler would
        // park forever and retain the connection region. Mark each handed-off
        // session's peer departed and wake the receiver; the handler then
        // closes the session and frees the region it owns.
        let handed_off = self
            .handed_off_sessions
            .lock()
            .remove(&process_id)
            .unwrap_or_default();
        for shared_id in handed_off {
            if let Ok(closed) = self.close_handed_off_session(shared_id) {
                self.note_generation_advance(closed.region_id, closed.generation);
            }
        }

        // The discovery service's listener queue is created host-side, so no
        // process local handle owns it: release it (and the feed publisher)
        // when the discovery guest stops.
        if *self.discovery_process_id.lock() == Some(process_id) {
            if let Some(shared_id) = self.discovery_listener_shared_id.lock().take() {
                self.kernel.queues().destroy_host_queue(shared_id);
            }
            *self.discovery_process_id.lock() = None;
            *self.discovery_publisher.lock() = None;
        }

        // Owner-keyed revocation first: the process node, its named routes,
        // aliases, health records, and any typed record it owns as itself
        // (its own process-node URI). Identity-keyed typed records (regions,
        // queues, volumes) survive this event and are removed by
        // `ResourceFreed` only when the host actually frees them, so a
        // co-owner exiting does not delete a record another still needs.
        self.publish_discovery_event(DiscoveryRequest::ProcessGone { process_id })?;

        // Snapshot the shared resources this process owns before the ownership
        // sets are mutated: used for identity-keyed revocation, for dropping
        // declarations, and for returning quota.
        let owner_tenant = process_tenant.clone().unwrap_or_default();
        let owned_shared: Vec<(ResourceClass, u64)> = self
            .shared_resource_owners
            .lock()
            .iter()
            .filter(|(_, owners)| owners.contains(&process_id))
            .map(|(key, _)| key.clone())
            .collect();

        // Revoke the registrations for the resource identities this process
        // actually frees. A resource a co-owner still holds is *not* freed, so
        // its identity-keyed record must survive this owner's exit; publishing
        // `ResourceFreed` for it would drop a record the co-owner still needs.
        let freed_shared: Vec<(ResourceClass, u64)> = {
            let owners = self.shared_resource_owners.lock();
            owned_shared
                .iter()
                .filter(|(class, id)| {
                    matches!(
                        class,
                        ResourceClass::SharedRegion | ResourceClass::HostQueue
                    ) && owners
                        .get(&(class.clone(), *id))
                        .is_some_and(|set| set.len() == 1)
                })
                .cloned()
                .collect()
        };
        for (class, id) in &freed_shared {
            let request = DiscoveryRequest::ResourceFreed {
                class: class.uri_segment().to_string(),
                id: *id,
            };
            self.publish_discovery_event(request)?;
        }

        // Quota facts for the reclaim passes below, keyed by the owning
        // process's tenant. Queues meter their queued items against the
        // owner; regions release through the quota ledger's own reservation
        // record, so no tenant fact is needed for them.
        let queue_quota_facts: Vec<(u64, String)> = owned_shared
            .iter()
            .filter(|(class, _)| *class == ResourceClass::HostQueue)
            .map(|(_, id)| (*id, owner_tenant.clone()))
            .collect();

        // Drop attachable-by-resolution declarations for resources this
        // process still owns: a dead owner confers nothing, and a later spawn
        // reusing the id must not inherit the declaration.
        if !owned_shared.is_empty() {
            let mut declarations = self.attachable_by_resolution.lock();
            for key in &owned_shared {
                declarations.remove(key);
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
                ResourceClass::HostQueue => {
                    // The queue's owner is gone and will never receive: the
                    // pipe slots held by its still-queued items return to the
                    // owner's quota (the kernel queue itself persists for
                    // any later attacher, unmetered from here on).
                    let shared_id = self.kernel.queues().host_queue_shared_id(local_id).ok();
                    if let (Some(shared_id), Ok(pending)) =
                        (shared_id, self.kernel.queues().host_queue_pending(local_id))
                        && let Some((_, principal)) = queue_quota_facts
                            .iter()
                            .find(|(id, _)| *id == shared_id)
                            .cloned()
                    {
                        self.kernel.quota().release(
                            &principal,
                            ResourceClass::HostQueue,
                            pending as u64,
                        );
                    }
                    // The queue's shared identity ends with its last handle, so
                    // the authority must stop reporting the dead process as its
                    // owner (the ResourceFreed event above already named it).
                    if let Some(shared_id) = shared_id {
                        self.release_shared_resource(
                            process_id,
                            &ResourceClass::HostQueue,
                            shared_id,
                        );
                        // The queue itself ends with its last local handle: a
                        // queue with no attacher left is unreachable, so
                        // reclaim it rather than accumulating one per listener
                        // that ever stopped.
                        self.kernel.queues().detach_host_queue(local_id);
                    }
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
                ResourceClass::Volume => {
                    // Closing the handle does not remove the volume: durable
                    // bytes belong to the tenant, not the process, and are
                    // released only when the volume is removed.
                    drop(self.kernel.volumes().close(local_id));
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
            let mapping_count = self.kernel.memory().shared_region_mapping_count(shared_id);
            let last_owner =
                self.release_shared_resource(process_id, &ResourceClass::SharedRegion, shared_id);
            // A co-owner keeps the region (and its reservation) alive; only the
            // last owner leaving can free it.
            if !last_owner {
                continue;
            }
            if mapping_count == 0 {
                // The region is destroyed: return its bytes to the ledger the
                // reservation was charged against — the tenant it was minted
                // for, which for a cross-tenant mint is not this process's own
                // tenant. (Regions with surviving mappings keep their
                // reservation — the resource is still live for its
                // co-attachers.)
                let region_len = self.kernel.memory().shared_region_len(shared_id).ok();
                drop(self.kernel.memory().destroy_shared_region(shared_id));
                self.resource_tenants
                    .lock()
                    .remove(&(ResourceClass::SharedRegion, shared_id));
                if !self
                    .kernel
                    .quota()
                    .release_reservation(ResourceClass::SharedRegion, shared_id)
                    && let Some(len) = region_len
                {
                    // No reservation was recorded (the platform tenant holds no
                    // tracked counter): fall back to the owner's own tenant.
                    self.kernel.quota().release(
                        &owner_tenant,
                        ResourceClass::SharedRegion,
                        u64::from(len),
                    );
                }
            }
        }

        Ok(())
    }
}
