use wasmtiny::{
    FunctionType, WasmApplication, WasmError, WasmValue,
    runtime::{HostCaller, HostFunc, SharedMemory},
};

use crate::{Error, Result, error::map_wasm_error};

pub(crate) fn decode_wasm_arguments(arguments: &[Vec<u8>]) -> Result<Vec<WasmValue>> {
    arguments
        .iter()
        .map(|argument| {
            let Some((value, used)) = WasmValue::from_bytes(argument) else {
                return Err(Error::InvalidEntrypointArgument);
            };
            if used != argument.len() {
                return Err(Error::InvalidEntrypointArgument);
            }
            Ok(value)
        })
        .collect()
}

pub(crate) fn guest_memory(caller: &HostCaller<'_>) -> wasmtiny::runtime::Result<SharedMemory> {
    caller
        .memory(0)
        .ok_or_else(|| WasmError::Runtime("guest module does not expose memory".to_string()))
}

pub(crate) fn read_guest_memory(
    caller: &HostCaller<'_>,
    ptr: u32,
    len: usize,
) -> wasmtiny::runtime::Result<Vec<u8>> {
    let memory = guest_memory(caller)?;
    let mut bytes = vec![0; len];
    memory
        .lock()
        .map_err(|_lock_err| WasmError::Runtime("guest memory lock poisoned".to_string()))?
        .read(ptr, &mut bytes)?;
    Ok(bytes)
}

pub(crate) fn register_optional_host_function(
    app: &mut WasmApplication,
    module_index: u32,
    import_module: &str,
    name: &str,
    func: Box<dyn HostFunc>,
    func_type: FunctionType,
) -> Result<()> {
    match app.register_host_function(module_index, import_module, name, func, func_type) {
        Ok(()) => Ok(()),
        Err(WasmError::Instantiate(message))
            if message == format!("import {import_module}.{name} not found") =>
        {
            Ok(())
        }
        Err(error) => Err(map_wasm_error(error)),
    }
}

pub(crate) fn wasm_i32_arg(args: &[WasmValue], index: usize) -> wasmtiny::runtime::Result<i32> {
    args.get(index)
        .ok_or_else(|| WasmError::Runtime(format!("missing argument {index}")))?
        .i32()
}

pub(crate) fn wasm_i64_arg(args: &[WasmValue], index: usize) -> wasmtiny::runtime::Result<i64> {
    args.get(index)
        .ok_or_else(|| WasmError::Runtime(format!("missing argument {index}")))?
        .i64()
}

pub(crate) fn write_guest_memory(
    caller: &HostCaller<'_>,
    ptr: u32,
    bytes: &[u8],
) -> wasmtiny::runtime::Result<()> {
    let memory = guest_memory(caller)?;
    memory
        .lock()
        .map_err(|_lock_err| WasmError::Runtime("guest memory lock poisoned".to_string()))?
        .write(ptr, bytes)
}
