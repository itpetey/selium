//! Selium runtime built on top of Wasmtiny and the Selium kernel.

pub use config::{
    BootstrapReport, BootstrappedGuest, ProcessAuthority, ReadinessCondition, RuntimeConfig,
    SystemGuestArg, SystemGuestDescriptor,
};
pub use error::{Error, Result};
pub use runtime::Runtime;

mod bootstrap;
mod config;
pub mod discovery;
mod error;
mod host_functions;
mod hostcall;
mod mailbox;
mod network;
mod process;
mod region_provider;
mod runtime;
mod wasm;
