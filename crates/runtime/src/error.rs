use selium_abi::{AbiError, AbiErrorCode, Capability, ProcessId};
use thiserror::Error;
use wasmtiny::WasmError;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("system guest descriptor not found: {0}")]
    DescriptorNotFound(String),
    #[error("unknown process authority: {0}")]
    UnknownProcessAuthority(ProcessId),
    #[error("unknown dependency: {0}")]
    UnknownDependency(String),
    #[error("dependency cycle or unresolved dependency detected")]
    DependencyCycle,
    #[error("invalid grant for capability {0:?}")]
    InvalidGrant(Capability),
    #[error("duplicate system guest descriptor: {0}")]
    DuplicateDescriptor(String),
    #[error("module id already registered with different bytes: {0}")]
    ModuleConflict(String),
    #[error("module not registered: {0}")]
    UnknownModule(String),
    #[error("invalid entrypoint argument encoding")]
    InvalidEntrypointArgument,
    #[error("readiness condition not satisfied for guest `{0}`")]
    ReadinessUnsatisfied(String),
    #[error("kernel error: {0}")]
    Kernel(#[from] selium_kernel::Error),
    #[error("wasmtiny runtime error: {0}")]
    Wasm(String),
}

pub(crate) fn map_wasm_error(error: WasmError) -> Error {
    Error::Wasm(error.to_string())
}

pub(crate) fn kernel_error(error: selium_kernel::Error) -> AbiError {
    let code = match error {
        selium_kernel::Error::NotFound(_) => AbiErrorCode::NotFound,
        selium_kernel::Error::Timeout => AbiErrorCode::Timeout,
        selium_kernel::Error::AlreadyCompleted
        | selium_kernel::Error::ProcessStopped(_)
        | selium_kernel::Error::Wasm(_) => AbiErrorCode::Internal,
    };
    AbiError::new(code, error.to_string())
}
