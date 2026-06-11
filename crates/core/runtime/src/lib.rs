//! Selium runtime built on top of Wasmtiny and the Selium kernel.

pub use config::{
    BootstrapReport, BootstrappedGuest, ProcessAuthority, ReadinessCondition, RuntimeConfig,
    SystemGuestDescriptor,
};
pub use error::{Error, Result};
pub use state::Runtime;

mod bootstrap;
mod config;
pub mod discovery;
mod error;
mod host_functions;
mod hostcall;
mod mailbox;
mod process;
mod state;
mod wasm;
