use selium_abi::GuestContext;
use wasmtime::Linker;

pub mod caps;
pub mod context;
pub mod network;
pub mod queue;
pub mod storage;
pub mod time;

pub use context::{AbiContext, ensure_capability};

#[allow(dead_code)]
pub fn add_to_linker(_linker: &mut Linker<GuestContext>) -> anyhow::Result<()> {
    Ok(())
}
