//! Selium kernel primitives.

pub use error::{Error, Result};
pub use state::Kernel;

mod error;
mod memory;
mod network;
mod process;
mod signal;
mod state;
mod storage;
