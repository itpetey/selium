//! Runtime adaptor SPI to decouple execution engines.

use std::str::FromStr;

use selium_abi::Capability;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdaptorKind {
    /// Run workloads in the Wasmtime-based adaptor.
    Wasmtime,
    /// Run workloads in the microVM-based adaptor.
    Microvm,
}

/// Execution isolation profiles understood by runtime adaptors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionProfile {
    /// Standard process isolation.
    Standard,
    /// Hardened process isolation.
    Hardened,
    /// Dedicated microVM isolation.
    Microvm,
}

/// Validated module launch specification passed to runtime adaptors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModuleSpec {
    /// Logical module identifier or path.
    pub module_id: String,
    /// Guest entrypoint to invoke.
    pub entrypoint: String,
    /// Capabilities granted to the workload.
    pub capabilities: Vec<Capability>,
    /// Required execution profile.
    pub profile: ExecutionProfile,
}

/// Errors raised while resolving or validating adaptor execution.
#[derive(Debug, Error)]
pub enum AdaptorError {
    #[error("adaptor not configured")]
    NotConfigured,
    #[error("adaptor does not support profile `{0:?}`")]
    UnsupportedProfile(ExecutionProfile),
    #[error("module identifier `{0}` is invalid for this adaptor")]
    InvalidModuleId(String),
    #[error("adaptor `{0}` cannot execute workloads on this node")]
    NotExecutable(AdaptorKind),
}

/// Common interface implemented by runtime adaptors.
pub trait RuntimeAdaptor {
    /// Return the adaptor kind identifier.
    fn kind(&self) -> AdaptorKind;
    /// Return the human-readable adaptor name.
    fn adaptor_name(&self) -> &'static str;
    /// Return the execution profiles supported by this adaptor.
    fn supported_profiles(&self) -> &'static [ExecutionProfile];
    /// Validate a module specification before launch.
    fn validate(&self, spec: &ModuleSpec) -> Result<(), AdaptorError>;
    /// Report whether the adaptor can execute workloads on the current node.
    fn executable(&self) -> bool {
        true
    }
}

impl FromStr for AdaptorKind {
    type Err = AdaptorError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "wasmtime" => Ok(Self::Wasmtime),
            "microvm" => Ok(Self::Microvm),
            _ => Err(AdaptorError::NotConfigured),
        }
    }
}

impl std::fmt::Display for AdaptorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Wasmtime => write!(f, "wasmtime"),
            Self::Microvm => write!(f, "microvm"),
        }
    }
}
