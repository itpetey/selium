use selium_abi::{CapabilityGrant, EntrypointMetadata, ProcessId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadinessCondition {
    Immediate,
    ActivityLogContains(String),
}

#[derive(Debug, Clone)]
pub struct SystemGuestDescriptor {
    pub name: String,
    pub module_id: String,
    pub module_bytes: Vec<u8>,
    pub entrypoint: String,
    pub arguments: Vec<Vec<u8>>,
    pub grants: Vec<CapabilityGrant>,
    pub dependencies: Vec<String>,
    pub readiness: ReadinessCondition,
}

#[derive(Debug, Clone, Default)]
pub struct RuntimeConfig {
    pub system_guests: Vec<SystemGuestDescriptor>,
}

#[derive(Debug, Clone)]
pub struct BootstrappedGuest {
    pub name: String,
    pub process_id: ProcessId,
}

#[derive(Debug, Clone, Default)]
pub struct BootstrapReport {
    pub guests: Vec<BootstrappedGuest>,
}

#[derive(Debug, Clone)]
pub struct ProcessAuthority {
    pub grants: Vec<CapabilityGrant>,
}

impl SystemGuestDescriptor {
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
