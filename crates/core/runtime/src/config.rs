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
}
