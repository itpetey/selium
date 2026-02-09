pub use selium_abi::{GuestInt, GuestUint};
use wasmtime::Caller;

use selium_abi::{
    DRIVER_ERROR_MESSAGE_CODE, DRIVER_RESULT_PENDING, decode_rkyv, driver_encode_error,
    driver_encode_ready, encode_driver_error_message,
};
use selium_kernel::{
    KernelError,
    guest_error::{GuestError, GuestResult},
    registry::InstanceRegistry,
};

fn encode_for_guest(
    error: GuestError,
    caller: &mut Caller<'_, InstanceRegistry>,
    ptr: GuestInt,
    len: GuestUint,
) -> Result<GuestUint, KernelError> {
    if matches!(error, GuestError::WouldBlock) {
        return Ok(DRIVER_RESULT_PENDING);
    }

    let bytes = encode_driver_error_message(&error.to_string())
        .map_err(|err| KernelError::Driver(err.to_string()))?;
    write_encoded(caller, ptr, len, &bytes)?;
    Ok(driver_encode_error(DRIVER_ERROR_MESSAGE_CODE))
}

pub fn write_poll_result(
    caller: &mut Caller<'_, InstanceRegistry>,
    ptr: GuestInt,
    len: GuestUint,
    result: GuestResult<Vec<u8>>,
) -> Result<GuestUint, KernelError> {
    match result {
        Ok(bytes) => write_encoded(caller, ptr, len, &bytes),
        Err(err) => encode_for_guest(err, caller, ptr, len),
    }
}

pub fn read_rkyv_value<T>(
    caller: &mut Caller<'_, InstanceRegistry>,
    ptr: GuestInt,
    len: GuestUint,
) -> Result<T, KernelError>
where
    T: rkyv::Archive + Sized,
    for<'a> T::Archived: 'a
        + rkyv::Deserialize<T, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>
        + rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
{
    let bytes = read_guest_bytes(caller, ptr, len)?;
    decode_rkyv(&bytes).map_err(|err| KernelError::Driver(err.to_string()))
}

fn read_guest_bytes(
    caller: &mut Caller<'_, InstanceRegistry>,
    ptr: GuestInt,
    len: GuestUint,
) -> Result<Vec<u8>, KernelError> {
    let memory = caller
        .get_export("memory")
        .and_then(|export| export.into_memory())
        .ok_or(KernelError::MemoryMissing)?;

    let start = usize::try_from(ptr).map_err(KernelError::IntConvert)?;
    let len = usize::try_from(len).map_err(KernelError::IntConvert)?;
    let end = start.checked_add(len).ok_or(KernelError::MemoryCapacity)?;

    let data = memory
        .data(caller)
        .get(start..end)
        .ok_or(KernelError::MemoryCapacity)?;
    Ok(data.to_vec())
}

fn write_encoded(
    caller: &mut Caller<'_, InstanceRegistry>,
    ptr: GuestInt,
    len: GuestUint,
    bytes: &[u8],
) -> Result<GuestUint, KernelError> {
    let memory = caller
        .get_export("memory")
        .and_then(|export| export.into_memory())
        .ok_or(KernelError::MemoryMissing)?;
    let capacity = usize::try_from(len).map_err(KernelError::IntConvert)?;
    if capacity < bytes.len() {
        return Err(KernelError::MemoryCapacity);
    }

    let offset = usize::try_from(ptr).map_err(KernelError::IntConvert)?;
    memory
        .write(caller, offset, bytes)
        .map_err(|err| KernelError::MemoryAccess(err.to_string()))?;

    encode_ready_len(bytes.len())
}

pub fn encode_ready_len(len: usize) -> Result<GuestUint, KernelError> {
    let guest_len = GuestUint::try_from(len).map_err(|_| KernelError::MemoryCapacity)?;
    driver_encode_ready(guest_len).ok_or(KernelError::MemoryCapacity)
}
