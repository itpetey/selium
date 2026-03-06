//! Runtime adapter SPI to decouple execution engines.

use std::str::FromStr;

use selium_abi::Capability;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdapterKind {
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
pub enum AdapterError {
    #[error("adapter not configured")]
    NotConfigured,
    #[error("adapter does not support profile `{0:?}`")]
    UnsupportedProfile(ExecutionProfile),
    #[error("module identifier `{0}` is invalid for this adapter")]
    InvalidModuleId(String),
    #[error("adapter `{0}` cannot execute workloads on this node")]
    NotExecutable(AdapterKind),
}

pub trait RuntimeAdapter {
    fn kind(&self) -> AdapterKind;
    fn adapter_name(&self) -> &'static str;
    fn supported_profiles(&self) -> &'static [ExecutionProfile];
    fn validate(&self, spec: &ModuleSpec) -> Result<(), AdapterError>;
    fn executable(&self) -> bool {
        true
    }
}

impl FromStr for AdapterKind {
    type Err = AdapterError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "wasmtime" => Ok(Self::Wasmtime),
            "microvm" => Ok(Self::Microvm),
            _ => Err(AdapterError::NotConfigured),
        }
    }
}

impl std::fmt::Display for AdapterKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Wasmtime => write!(f, "wasmtime"),
            Self::Microvm => write!(f, "microvm"),
        }
    }
}
