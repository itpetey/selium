use std::{
    collections::{BTreeMap, BTreeSet},
    thread,
    time::{Duration, Instant},
};

use selium_abi::{
    ActivityEvent, Capability, CapabilityGrant, DiscoveryRequest, ResourceIdentity,
    ResourceSelector, ResourceTarget, encode_rkyv,
};
use selium_shm::{Channel, ChannelBackpressure, transport::ShmTransport};
use selium_wire::{framed::FramedWrite, pubsub::Publisher};
use tracing::info;
use wasmtiny::{WasmApplication, WasmValue};

use crate::{
    Error, Result,
    config::{
        BootstrapReport, BootstrappedGuest, ReadinessCondition, RuntimeConfig, SystemGuestArg,
        SystemGuestDescriptor,
    },
    error::map_wasm_error,
    runtime::{DiscoveryPublisher, Runtime},
};

const DEFAULT_READINESS_POLL_MS: u64 = 10;
const DEFAULT_READINESS_TIMEOUT_MS: u64 = 1_000;

pub(crate) struct LoadedGuest {
    pub(crate) app: WasmApplication,
    pub(crate) module_index: u32,
    pub(crate) entrypoint_results: Vec<WasmValue>,
}

impl Runtime {
    /// Boots all configured system guests in dependency order.
    pub fn bootstrap_system_guests(&self, mut config: RuntimeConfig) -> Result<BootstrapReport> {
        let (discovery_feed_region_id, discovery_listener_shared_id) = if config.start_discovery {
            let (feed_region_id, listener_shared_id) = self.setup_discovery()?;
            (Some(feed_region_id), Some(listener_shared_id))
        } else {
            (None, None)
        };

        if let Some(listener_shared_id) = discovery_listener_shared_id {
            let feed_region_id =
                discovery_feed_region_id.expect("discovery feed region id must be present");
            for descriptor in &mut config.system_guests {
                if descriptor.name == "discovery" {
                    descriptor.set_discovery_feed_and_handle(feed_region_id, listener_shared_id);
                    // Add explicit resource grants so the discovery guest can attach
                    // to the feed region and listener queue created by setup_discovery.
                    descriptor.grants.push(CapabilityGrant::new(
                        Capability::SharedMemory,
                        vec![ResourceSelector::ExplicitResource(
                            ResourceIdentity::Shared(feed_region_id),
                        )],
                    ));
                    descriptor.grants.push(CapabilityGrant::new(
                        Capability::HostQueue,
                        vec![ResourceSelector::ExplicitResource(
                            ResourceIdentity::Shared(listener_shared_id),
                        )],
                    ));
                } else if descriptor.arguments.is_empty() && descriptor.well_known_uri.is_none() {
                    // Other guests also need the discovery handle. Guests with
                    // a well-known channel receive their provisioned listener
                    // as the leading argument instead (injected at spawn).
                    descriptor.set_discovery_handle(listener_shared_id);
                    // Other guests also need explicit grant for the discovery listener.
                    descriptor.grants.push(CapabilityGrant::new(
                        Capability::HostQueue,
                        vec![ResourceSelector::ExplicitResource(
                            ResourceIdentity::Shared(listener_shared_id),
                        )],
                    ));
                }
            }
        }

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
                drop(self.stop_process(bootstrapped.process_id));
                self.rollback_bootstrapped(&report);
                return Err(Error::ReadinessUnsatisfied(descriptor.name));
            }
            ready.insert(name);
            report.guests.push(bootstrapped);
        }

        Ok(report)
    }

    /// Creates the discovery pub/sub feed ring and RPC listener.
    ///
    /// Stores the publisher and listener shared id in runtime state, and returns
    /// the feed region id and listener shared id so bootstrap can wire them into
    /// the discovery guest descriptor.
    fn setup_discovery(&self) -> Result<(u64, u64)> {
        let feed_channel = Channel::create_with_backpressure(
            64 * 1024,
            ChannelBackpressure::Drop,
            selium_abi::ResourceKind::PubSubTopic,
        )
        .map_err(|error| {
            Error::Host(format!("failed to create discovery feed channel: {error}"))
        })?;
        let feed_region_id = feed_channel.region_id();

        let transport = ShmTransport::new(&feed_channel, &feed_channel).map_err(|error| {
            Error::Host(format!(
                "failed to create discovery feed transport: {error}"
            ))
        })?;
        let publisher: DiscoveryPublisher = Publisher::new(FramedWrite::new(transport));
        *self.discovery_publisher.lock() = Some(publisher);

        let queues = self.kernel.queues();
        let memory = self.kernel.memory();
        let listener = queues.create_host_queue(&memory);
        *self.discovery_listener_shared_id.lock() = Some(listener.shared_id);

        self.kernel.processes().record_activity(ActivityEvent {
            kind: selium_abi::ActivityKind::GuestBootstrapped,
            process_id: None,
            message: format!(
                "discovery feed region={feed_region_id} listener={}",
                listener.shared_id
            ),
        });

        Ok((feed_region_id, listener.shared_id))
    }

    /// Starts and records a single system guest.
    pub fn spawn_system_guest(
        &self,
        mut descriptor: SystemGuestDescriptor,
    ) -> Result<BootstrappedGuest> {
        self.validate_grants(&descriptor.grants)?;

        // Provision the well-known channel, if the descriptor declares one:
        // create the host listener queue, inject its shared id as the leading
        // entrypoint argument, and grant the guest attach rights for it.
        // Registration with discovery happens below, once the guest is up.
        let well_known = match &descriptor.well_known_uri {
            Some(uri) => {
                let queues = self.kernel.queues();
                let memory = self.kernel.memory();
                let listener = queues.create_host_queue(&memory);
                descriptor
                    .arguments
                    .insert(0, SystemGuestArg::Integer(listener.shared_id));
                descriptor.grants.push(CapabilityGrant::new(
                    Capability::HostQueue,
                    vec![ResourceSelector::ExplicitResource(
                        ResourceIdentity::Shared(listener.shared_id),
                    )],
                ));
                Some((uri.clone(), listener.shared_id))
            }
            None => None,
        };

        let process = self.kernel.processes().start_process(
            descriptor.module_id.clone(),
            descriptor.entrypoint.clone(),
            descriptor.grants.clone(),
        );
        self.persist_process_authority(
            process.local_id,
            descriptor.grants.clone(),
            descriptor.tenant.clone(),
            None,
        );

        let loaded_guest = match self.load_guest_module(&descriptor.module_bytes, process.local_id)
        {
            Ok(loaded_guest) => loaded_guest,
            Err(error) => {
                self.cleanup_failed_process(process.local_id)?;
                return Err(error);
            }
        };
        let loaded_guest = match self.execute_entrypoint(loaded_guest, &descriptor) {
            Ok(loaded_guest) => {
                if loaded_guest.entrypoint_results == [WasmValue::I32(1)] {
                    self.kernel.processes().record_activity(ActivityEvent {
                        kind: selium_abi::ActivityKind::ProcessExited,
                        process_id: Some(process.local_id),
                        message: format!("guest {} entrypoint returned error", descriptor.name),
                    });
                    self.cleanup_failed_process(process.local_id)?;
                    return Err(Error::EntrypointFailed(descriptor.name.clone()));
                }
                loaded_guest
            }
            Err(error) => {
                self.kernel.processes().record_activity(ActivityEvent {
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
        self.claim_local_handle(
            process.local_id,
            selium_abi::ResourceClass::Process,
            process.local_id,
        );
        self.register_module_bytes(
            descriptor.module_id.clone(),
            descriptor.module_bytes.clone(),
        )?;
        self.kernel.processes().record_activity(ActivityEvent {
            kind: selium_abi::ActivityKind::GuestBootstrapped,
            process_id: Some(process.local_id),
            message: format!("guest {} bootstrapped", descriptor.name),
        });
        info!(
            guest = descriptor.name.as_str(),
            process_id = process.local_id,
            "bootstrapped system guest"
        );

        // Record the discovery service's process identity so the runtime can
        // restrict `RecordResolvedQueueFor` to the trusted discovery guest.
        if descriptor.name == "discovery" {
            *self.discovery_process.lock() = Some(process.local_id);
        }

        // Register the well-known URI with discovery now that the guest is up.
        // Publishing is a no-op when discovery is not enabled (the queue and
        // argument injection above still apply).
        if let Some((uri, listener_shared_id)) = well_known.clone()
            && let Err(error) =
                self.register_well_known_uri(process.local_id, uri, listener_shared_id)
        {
            self.cleanup_failed_process(process.local_id)?;
            return Err(error);
        }

        Ok(BootstrappedGuest {
            name: descriptor.name,
            process_id: process.local_id,
            well_known_listener: well_known.map(|(_, listener)| listener),
        })
    }

    /// Records and publishes the well-known registration for a spawned system
    /// guest: the URI maps to the provisioned listener queue so guests can
    /// resolve it and attach with a channel grant.
    fn register_well_known_uri(
        &self,
        process_id: selium_abi::ProcessId,
        uri: String,
        listener_shared_id: u64,
    ) -> Result<()> {
        let target = ResourceTarget {
            uri: uri.clone(),
            host_id: String::new(), // Runtime doesn't know host_id; discovery will fill it.
            resource_id: listener_shared_id,
            interface: None,
            tenant: self.process_tenant(process_id),
        };
        let request = DiscoveryRequest::Register {
            uri: uri.clone(),
            target,
        };
        let bytes = encode_rkyv(&request)
            .map_err(|error| Error::Host(format!("discovery encode failed: {error}")))?;
        self.publish_discovery_event(bytes)?;
        self.well_known_uris
            .lock()
            .insert(process_id, (uri.clone(), listener_shared_id));
        self.kernel.processes().record_activity(ActivityEvent {
            kind: selium_abi::ActivityKind::GuestBootstrapped,
            process_id: Some(process_id),
            message: format!("well-known uri={uri} listener={listener_shared_id}"),
        });
        Ok(())
    }

    pub(crate) fn load_guest_module(
        &self,
        module_bytes: &[u8],
        process_id: selium_abi::ProcessId,
    ) -> Result<LoadedGuest> {
        let store = self.kernel.memory().shared_store();
        let mut app = WasmApplication::with_store(store);
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

    pub(crate) fn execute_entrypoint(
        &self,
        mut loaded_guest: LoadedGuest,
        descriptor: &SystemGuestDescriptor,
    ) -> Result<LoadedGuest> {
        let arguments = crate::wasm::resolve_entrypoint_arguments(
            &mut loaded_guest.app,
            loaded_guest.module_index,
            &descriptor.arguments,
        )?;
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

    pub(crate) fn wait_for_readiness(
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
                        .processes()
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

    pub(crate) fn rollback_bootstrapped(&self, report: &BootstrapReport) {
        for guest in report.guests.iter().rev() {
            drop(self.stop_process(guest.process_id));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use selium_abi::{Capability, CapabilityGrant, LocalityScope, ResourceSelector};
    use wasmtiny::WasmValue;

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
            start_discovery: false,
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
                tenant: None,
                well_known_uri: None,
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
                tenant: None,
                well_known_uri: None,
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
    fn guest_with_i32_zero_entrypoint_result_bootstraps_normally() {
        let runtime = Runtime::default();
        let config = RuntimeConfig {
            start_discovery: false,
            system_guests: vec![SystemGuestDescriptor {
                name: "ok-guest".to_string(),
                module_id: "ok-module".to_string(),
                module_bytes: module_with_entrypoint("boot", "(result i32) i32.const 0"),
                entrypoint: "boot".to_string(),
                arguments: Vec::new(),
                grants: vec![CapabilityGrant::new(
                    Capability::ProcessLifecycle,
                    vec![ResourceSelector::Locality(LocalityScope::Cluster)],
                )],
                dependencies: Vec::new(),
                readiness: ReadinessCondition::Immediate,
                tenant: None,
                well_known_uri: None,
            }],
        };

        let report = runtime
            .bootstrap_system_guests(config)
            .expect("bootstrap guests");
        assert_eq!(report.guests.len(), 1);
        assert_eq!(
            runtime
                .entrypoint_results(report.guests[0].process_id)
                .expect("entrypoint results"),
            vec![WasmValue::I32(0)]
        );
    }

    #[test]
    fn guest_with_i32_one_entrypoint_result_fails_with_entrypoint_failed() {
        let runtime = Runtime::default();
        let config = RuntimeConfig {
            start_discovery: false,
            system_guests: vec![SystemGuestDescriptor {
                name: "fail-guest".to_string(),
                module_id: "fail-module".to_string(),
                module_bytes: module_with_entrypoint("boot", "(result i32) i32.const 1"),
                entrypoint: "boot".to_string(),
                arguments: Vec::new(),
                grants: vec![CapabilityGrant::new(
                    Capability::ProcessLifecycle,
                    vec![ResourceSelector::Locality(LocalityScope::Cluster)],
                )],
                dependencies: Vec::new(),
                readiness: ReadinessCondition::Immediate,
                tenant: None,
                well_known_uri: None,
            }],
        };

        let err = runtime
            .bootstrap_system_guests(config)
            .expect_err("should fail with EntrypointFailed");
        assert!(
            matches!(err, Error::EntrypointFailed(ref name) if name == "fail-guest"),
            "expected EntrypointFailed, got {err:?}"
        );
    }

    #[test]
    fn well_known_channel_is_provisioned_injected_and_revoked() {
        // The entrypoint echoes its first argument, proving the runtime
        // injected the provisioned listener id as the leading argument.
        let runtime = Runtime::default();
        let config = RuntimeConfig {
            start_discovery: false,
            system_guests: vec![SystemGuestDescriptor {
                name: "well-known".to_string(),
                module_id: "well-known-module".to_string(),
                module_bytes: module_with_entrypoint(
                    "boot",
                    "(param i64) (result i64) local.get 0",
                ),
                entrypoint: "boot".to_string(),
                arguments: Vec::new(),
                grants: vec![CapabilityGrant::new(
                    Capability::ProcessLifecycle,
                    vec![ResourceSelector::Locality(LocalityScope::Cluster)],
                )],
                dependencies: Vec::new(),
                readiness: ReadinessCondition::Immediate,
                tenant: None,
                well_known_uri: Some("sel://sys/dns/resolve".to_string()),
            }],
        };

        let report = runtime
            .bootstrap_system_guests(config)
            .expect("bootstrap guest");
        let guest = &report.guests[0];
        let listener = guest
            .well_known_listener
            .expect("runtime provisions the well-known listener");

        // The listener id was injected as the leading entrypoint argument.
        assert_eq!(
            runtime
                .entrypoint_results(guest.process_id)
                .expect("entrypoint results"),
            vec![WasmValue::I64(listener as i64)]
        );

        // The registration is recorded (and revoked on teardown).
        assert_eq!(
            runtime.well_known_uri(guest.process_id),
            Some(("sel://sys/dns/resolve".to_string(), listener))
        );
        runtime.stop_process(guest.process_id).expect("stop guest");
        assert!(
            runtime.well_known_uri(guest.process_id).is_none(),
            "well-known registration must be revoked at teardown"
        );
    }
}
