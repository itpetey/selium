use selium_abi::{CapabilityGrant, EntrypointMetadata, ProcessId};

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
        // Encode the shared_id as a little-endian u64 argument.
        self.arguments = vec![shared_id.to_le_bytes().to_vec()];
    }

    /// Sets both the discovery feed ring region id and the discovery RPC
    /// listener shared id for the discovery system guest itself.
    ///
    /// The feed ring id is passed as the first entrypoint argument and the
    /// listener shared id as the second.
    pub fn set_discovery_feed_and_handle(&mut self, feed_region_id: u64, listener_shared_id: u64) {
        self.arguments = vec![
            feed_region_id.to_le_bytes().to_vec(),
            listener_shared_id.to_le_bytes().to_vec(),
        ];
    }
}
