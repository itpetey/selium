use selium_abi::ProcessId;
use thiserror::Error;
use wasmtiny::runtime::WasmError;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("resource not found: {0}")]
    NotFound(String),
    #[error("signal wait timed out")]
    Timeout,
    #[error("request exchange already has a response")]
    AlreadyCompleted,
    #[error("process already stopped: {0}")]
    ProcessStopped(ProcessId),
    #[error("wasmtiny runtime error: {0}")]
    Wasm(String),
}

pub(crate) fn map_wasm_error(error: WasmError) -> Error {
    Error::Wasm(error.to_string())
}
