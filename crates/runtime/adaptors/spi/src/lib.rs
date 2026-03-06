//! Runtime adaptor SPI to decouple execution engines.

use std::str::FromStr;

use selium_abi::Capability;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdaptorKind {
    Wasmtime,
    Microvm,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionProfile {
    Standard,
    Hardened,
    Microvm,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModuleSpec {
    pub module_id: String,
    pub entrypoint: String,
    pub capabilities: Vec<Capability>,
    pub profile: ExecutionProfile,
}

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

pub trait RuntimeAdaptor {
    fn kind(&self) -> AdaptorKind;
    fn adaptor_name(&self) -> &'static str;
    fn supported_profiles(&self) -> &'static [ExecutionProfile];
    fn validate(&self, spec: &ModuleSpec) -> Result<(), AdaptorError>;
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
