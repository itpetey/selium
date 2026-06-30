use selium_abi::{
    ActivityEvent, Capability, CapabilityGrant, DiscoveryRequest, LocalityScope, ProcessId,
    ResourceClass, ResourceIdentity, ScopeContext, encode_rkyv,
};
use tracing::debug;
use wasmtiny::WasmValue;

use crate::{Error, Result, config::ProcessAuthority, state::Runtime};

impl Runtime {
    /// Stops a process and releases runtime-owned state for it.
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
            self.mailboxes.lock().remove(&process_id);
            self.cleanup_process_resources(process_id)?;
        }
        self.local_handle_owners
            .lock()
            .remove(&(ResourceClass::Process, process_id));
        self.kernel.reap_process(process_id)?;
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

    /// Projects a metering observation into the kernel.
    pub fn project_metering(
        &self,
        process_id: selium_abi::ProcessId,
        observation: selium_abi::MeteringObservation,
    ) {
        self.kernel.observe_metering(process_id, observation);
    }

    /// Returns all activity log events currently held by the kernel.
    pub fn activity_log(&self) -> Vec<ActivityEvent> {
        self.kernel.read_activity_from(0)
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
    ) {
        self.process_authorities
            .lock()
            .insert(process_id, ProcessAuthority { grants });
    }

    pub(crate) fn validate_grants(&self, grants: &[CapabilityGrant]) -> Result<()> {
        for grant in grants {
            if grant.selectors.is_empty() {
                return Err(Error::InvalidGrant(grant.capability.clone()));
            }
        }
        Ok(())
    }

    pub(crate) fn cleanup_failed_process(&self, process_id: selium_abi::ProcessId) -> Result<()> {
        // Best-effort teardown: the process has already failed, so there's no
        // recovery path for individual cleanup steps. We discard each error and
        // continue with the remaining work to reclaim as much as possible.
        drop(self.kernel.stop_process(process_id));
        self.operations
            .lock()
            .retain(|_, operation| operation.process_id != process_id);
        drop(self.cleanup_process_resources(process_id));
        drop(self.kernel.reap_process(process_id));
        self.process_authorities.lock().remove(&process_id);
        self.mailboxes.lock().remove(&process_id);
        self.local_handle_owners
            .lock()
            .remove(&(ResourceClass::Process, process_id));
        self.shared_resource_owners
            .lock()
            .retain(|_, owners| !owners.contains(&process_id));
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
                let bytes = encode_rkyv(&request)
                    .map_err(|error| crate::Error::Host(format!("discovery encode failed: {error}")))?;
                self.publish_discovery_event(bytes)?;
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
                    drop(self.kernel.detach_shared_region(local_id));
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
                    drop(self.kernel.close_log(local_id));
                }
                ResourceClass::BlobStore => {
                    drop(self.kernel.close_blob_store(local_id));
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
            if self.kernel.shared_region_mapping_count(shared_id) == 0 {
                // Best-effort: the region has no remaining mappings, but if
                // destruction fails the region will be reclaimed by the kernel
                // on process exit anyway.
                drop(self.kernel.destroy_shared_region(shared_id));
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
            Err(selium_abi::AbiError::new(
                selium_abi::AbiErrorCode::PermissionDenied,
                format!("permission denied for capability {capability:?}"),
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

    pub(crate) fn module_bytes(&self, module_id: &str) -> Result<Vec<u8>> {
        self.module_registry
            .lock()
            .get(module_id)
            .cloned()
            .ok_or_else(|| Error::UnknownModule(module_id.to_string()))
    }

    fn poll_guest_until_stalled(&self, process_id: ProcessId) {
        let Some(mut loaded_guest) = self.loaded_guests.lock().remove(&process_id) else {
            return;
        };
        let result =
            loaded_guest
                .app
                .call_function(loaded_guest.module_index, "__selium_guest_poll", &[]);
        self.loaded_guests.lock().insert(process_id, loaded_guest);
        if let Err(error) = result {
            debug!(
                process_id,
                error = %error,
                "guest poll after mailbox wake failed"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ReadinessCondition, Runtime, SystemGuestDescriptor};
    use selium_abi::{LocalityScope, MeteringObservation, ResourceSelector};

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
