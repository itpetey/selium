use std::collections::HashSet;

use selium_abi::{CapabilityGrant, EntrypointMetadata, ProcessId};
use wasmtiny::WasmValue;

/// Condition used to decide when a bootstrapped system guest is ready.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadinessCondition {
    /// Guest is considered ready immediately after start.
    Immediate,
    /// Guest is ready once the activity log contains the supplied text.
    ActivityLogContains(String),
}

/// Configuration for a system guest that should be bootstrapped by the runtime.
#[derive(Debug, Clone)]
pub struct SystemGuestDescriptor {
    /// Stable system guest name.
    pub name: String,
    /// Module id used to register and start the guest.
    pub module_id: String,
    /// WebAssembly module bytes.
    pub module_bytes: Vec<u8>,
    /// Entrypoint export to invoke.
    pub entrypoint: String,
    /// Encoded entrypoint arguments.
    pub arguments: Vec<Vec<u8>>,
    /// Capability grants assigned to the guest process.
    pub grants: Vec<CapabilityGrant>,
    /// Names of system guests that must become ready first.
    pub dependencies: Vec<String>,
    /// Readiness condition for this guest.
    pub readiness: ReadinessCondition,
    /// Tenant identity for this guest. `None` means "platform tenant".
    /// Children spawned by this guest inherit this tenant.
    pub tenant: Option<String>,
}

/// Runtime bootstrap configuration.
#[derive(Debug, Clone, Default)]
pub struct RuntimeConfig {
    /// System guests to start during bootstrap.
    pub system_guests: Vec<SystemGuestDescriptor>,
    /// When true, the runtime creates the discovery pub/sub feed ring and
    /// discovery listener, and wires them into the discovery system guest.
    pub start_discovery: bool,
}

/// Guest successfully started during bootstrap.
#[derive(Debug, Clone)]
pub struct BootstrappedGuest {
    /// System guest name.
    pub name: String,
    /// Process id assigned to the guest.
    pub process_id: ProcessId,
}

/// Report returned after bootstrapping system guests.
#[derive(Debug, Clone, Default)]
pub struct BootstrapReport {
    /// Guests started successfully, in bootstrap order.
    pub guests: Vec<BootstrappedGuest>,
}

/// Capability authority persisted for a process.
#[derive(Debug, Clone)]
pub struct ProcessAuthority {
    /// Grants assigned to the process.
    pub grants: Vec<CapabilityGrant>,
    /// Tenant identity assigned at spawn (inherited from parent or explicit
    /// host assignment for system guests). `None` means "platform tenant".
    pub tenant: Option<String>,
    /// Parent process id, if spawned by another process. `None` for
    /// bootstrapped system guests. Used by the `Children` selector.
    pub parent: Option<ProcessId>,
    /// Queue ids returned to this process by successful discovery Resolve
    /// calls. Provides an authorisation basis for cross-process
    /// `HostQueueAttach` without requiring an `ExplicitResource` grant.
    pub resolved_queue_ids: HashSet<u64>,
}

impl SystemGuestDescriptor {
    /// Builds a system guest descriptor from generated entrypoint metadata.
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
            tenant: None,
        }
    }

    /// Sets the discovery handle (host queue shared_id) for this guest.
    ///
    /// The discovery handle is passed as the first entrypoint argument.
    /// Application guests use this to connect to the discovery service
    /// via `Context::from_raw(discovery_handle)`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create a host queue for discovery.
    /// let discovery_queue = kernel.create_host_queue();
    /// let discovery_shared_id = discovery_queue.shared_id;
    ///
    /// // Configure the discovery guest with the shared_id.
    /// let mut discovery_guest = SystemGuestDescriptor::from_entrypoint_metadata(
    ///     "discovery", "discovery-module", module_bytes, metadata, grants,
    /// );
    /// discovery_guest.set_discovery_handle(discovery_shared_id);
    ///
    /// // Configure application guests with the same shared_id.
    /// let mut app_guest = SystemGuestDescriptor::from_entrypoint_metadata(
    ///     "my-app", "app-module", app_bytes, app_metadata, app_grants,
    /// );
    /// app_guest.set_discovery_handle(discovery_shared_id);
    /// app_guest.dependencies.push("discovery".to_string());
    /// ```
    pub fn set_discovery_handle(&mut self, shared_id: u64) {
        self.arguments = vec![encode_u64_argument(shared_id)];
    }

    /// Sets both the discovery feed ring region id and the discovery RPC
    /// listener shared id for the discovery system guest itself.
    ///
    /// The feed ring id is passed as the first entrypoint argument and the
    /// listener shared id as the second.
    pub fn set_discovery_feed_and_handle(&mut self, feed_region_id: u64, listener_shared_id: u64) {
        self.arguments = vec![
            encode_u64_argument(feed_region_id),
            encode_u64_argument(listener_shared_id),
        ];
    }
}

/// Encodes a `u64` as a WASM `i64` entrypoint argument, using the runtime's
/// tagged `WasmValue` serialisation expected by `decode_wasm_arguments`.
fn encode_u64_argument(value: u64) -> Vec<u8> {
    let mut bytes = Vec::new();
    WasmValue::I64(value as i64).to_bytes(&mut bytes);
    bytes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoded_arguments_round_trip_through_wasm_value_codecs() {
        for value in [0_u64, 1, 7, u32::MAX as u64, u64::MAX] {
            let bytes = encode_u64_argument(value);
            let Some((decoded, used)) = WasmValue::from_bytes(&bytes) else {
                panic!("failed to decode argument {value}");
            };
            assert_eq!(used, bytes.len(), "trailing bytes for {value}");
            assert_eq!(decoded, WasmValue::I64(value as i64));
        }
    }
}
