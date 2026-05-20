//! Selium guest SDK.

use std::future::Future;

use rkyv::{
    api::high::{HighDeserializer, HighValidator},
    rancor::Error as RancorError,
};
use selium_abi::{
    AbiError, CompletionState, HostcallOutput, HostcallRequest, SignalDescriptor, decode_rkyv,
    deframe_bytes, encode_rkyv, frame_bytes, unpack_hostcall_status,
};
use thiserror::Error;

pub use selium_abi::{
    Capability, CapabilityGrant, EntrypointMetadata, InterfaceMetadata, LocalityScope,
    ResourceClass, ResourceIdentity, ResourceSelector, ScopeContext,
};
pub use selium_guest_macros::{entrypoint, pattern_interface};
pub use tracing::{debug, error, info, trace, warn};

pub type Result<T> = std::result::Result<T, GuestError>;

#[derive(Debug, Error)]
pub enum GuestError {
    #[error("host error: {0}")]
    Host(String),
    #[error("codec error: {0}")]
    Codec(#[from] selium_abi::RkyvError),
    #[error("permission denied for capability {0:?}")]
    PermissionDenied(Capability),
    #[error("unexpected hostcall output")]
    UnexpectedHostcallOutput,
}

#[derive(Clone, Debug)]
pub struct Signal {
    descriptor: SignalDescriptor,
}

impl Signal {
    pub fn create() -> Result<Self> {
        match hostcall(HostcallRequest::SignalCreate)? {
            HostcallOutput::Signal(descriptor) => Ok(Self { descriptor }),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }

    pub fn attach(shared_id: u64) -> Result<Self> {
        match hostcall(HostcallRequest::SignalAttach { shared_id })? {
            HostcallOutput::Signal(descriptor) => Ok(Self { descriptor }),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }

    pub fn descriptor(&self) -> SignalDescriptor {
        self.descriptor
    }

    pub fn local_id(&self) -> u64 {
        self.descriptor.local_id
    }

    pub fn shared_id(&self) -> u64 {
        self.descriptor.shared_id
    }

    pub fn notify(&self) -> Result<u64> {
        match hostcall(HostcallRequest::SignalNotify {
            local_id: self.descriptor.local_id,
        })? {
            HostcallOutput::SignalGeneration(generation) => Ok(generation),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }

    pub fn wait(&self, observed_generation: u64, timeout_ms: u64) -> Result<u64> {
        match hostcall(HostcallRequest::SignalWait {
            local_id: self.descriptor.local_id,
            observed_generation,
            timeout_ms,
        })? {
            HostcallOutput::SignalGeneration(generation) => Ok(generation),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }

    pub fn close(self) -> Result<()> {
        match hostcall(HostcallRequest::SignalClose {
            local_id: self.descriptor.local_id,
        })? {
            HostcallOutput::Empty => Ok(()),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }
}

pub fn process_id() -> u64 {
    unsafe { selium_process_id() }
}

pub fn mark_ready() {
    unsafe { selium_mark_ready() }
}

pub fn decode_typed<T>(bytes: &[u8]) -> Result<T>
where
    T: rkyv::Archive + Sized,
    for<'a> T::Archived: rkyv::Deserialize<T, HighDeserializer<RancorError>>
        + rkyv::bytecheck::CheckBytes<HighValidator<'a, RancorError>>,
{
    let payload = deframe_bytes(bytes).map_err(abi_error_to_guest_error)?;
    Ok(decode_rkyv(payload)?)
}

pub fn encode_typed<T>(value: &T) -> Result<Vec<u8>>
where
    T: selium_abi::RkyvEncode,
{
    frame_bytes(&encode_rkyv(value)?).map_err(abi_error_to_guest_error)
}

pub fn run_entrypoint_safely<F>(future: F)
where
    F: Future<Output = ()>,
{
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        futures::executor::block_on(future)
    }));
    if result.is_err() {
        std::process::abort();
    }
}

fn hostcall(request: HostcallRequest) -> Result<HostcallOutput> {
    let request = encode_rkyv(&request)?;
    let create_status = unsafe { selium_hostcall_create(request.as_ptr(), request.len()) };
    let (status, operation_id) = unpack_hostcall_status(create_status);
    if status == selium_abi::HOSTCALL_STATUS_FAILED {
        return Err(GuestError::Host("hostcall create failed".to_string()));
    }

    let mut output = vec![0_u8; 4096];
    loop {
        let poll_status =
            unsafe { selium_hostcall_poll(operation_id as u64, output.as_mut_ptr(), output.len()) };
        let (status, len) = unpack_hostcall_status(poll_status);
        if status == selium_abi::HOSTCALL_STATUS_OUTPUT_TOO_SMALL {
            output.resize(len as usize, 0);
            continue;
        }
        let state: CompletionState = decode_rkyv(&output[..len as usize])?;
        match state {
            CompletionState::Ready(output) => {
                unsafe { selium_hostcall_drop(operation_id as u64) };
                return Ok(output);
            }
            CompletionState::Pending { .. } => continue,
            CompletionState::Failed(error) => {
                unsafe { selium_hostcall_drop(operation_id as u64) };
                return Err(abi_error_to_guest_error(error));
            }
        }
    }
}

fn abi_error_to_guest_error(error: AbiError) -> GuestError {
    if error.code == selium_abi::AbiErrorCode::PermissionDenied {
        GuestError::PermissionDenied(Capability::Signal)
    } else {
        GuestError::Host(format!("{:?}: {}", error.code, error.message))
    }
}

#[cfg(target_arch = "wasm32")]
#[link(wasm_import_module = "selium")]
unsafe extern "C" {
    #[link_name = "process_id"]
    fn selium_process_id() -> u64;
    #[link_name = "mark_ready"]
    fn selium_mark_ready();
    #[link_name = "hostcall_create"]
    fn selium_hostcall_create(request_ptr: *const u8, request_len: usize) -> u64;
    #[link_name = "hostcall_poll"]
    fn selium_hostcall_poll(operation_id: u64, out_ptr: *mut u8, out_capacity: usize) -> u64;
    #[link_name = "hostcall_drop"]
    fn selium_hostcall_drop(operation_id: u64) -> u32;
}

#[cfg(not(target_arch = "wasm32"))]
unsafe fn selium_process_id() -> u64 {
    0
}

#[cfg(not(target_arch = "wasm32"))]
unsafe fn selium_mark_ready() {}

#[cfg(not(target_arch = "wasm32"))]
unsafe fn selium_hostcall_create(_request_ptr: *const u8, _request_len: usize) -> u64 {
    selium_abi::pack_hostcall_status(selium_abi::HOSTCALL_STATUS_FAILED, 0)
}

#[cfg(not(target_arch = "wasm32"))]
unsafe fn selium_hostcall_poll(_operation_id: u64, _out_ptr: *mut u8, _out_capacity: usize) -> u64 {
    selium_abi::pack_hostcall_status(selium_abi::HOSTCALL_STATUS_FAILED, 0)
}

#[cfg(not(target_arch = "wasm32"))]
unsafe fn selium_hostcall_drop(_operation_id: u64) -> u32 {
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
    #[rkyv(bytecheck())]
    struct DemoPayload {
        message: String,
    }

    #[test]
    fn typed_codec_round_trips() {
        let payload = DemoPayload {
            message: "hello".to_string(),
        };

        let encoded = encode_typed(&payload).expect("encode payload");
        let decoded: DemoPayload = decode_typed(&encoded).expect("decode payload");

        assert_eq!(decoded, payload);
    }

    #[test]
    fn native_hostcalls_are_unavailable() {
        let result = Signal::create();

        assert!(matches!(result, Err(GuestError::Host(_))));
    }
}
