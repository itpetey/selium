//! Random-byte hostcall.
//!
//! The length is bounded so a guest cannot ask the host for an arbitrarily
//! large buffer in one call.

use std::io::Read;

use selium_abi::{AbiError, AbiErrorCode, HostcallOutput};

use super::HostOperationState;
use crate::Runtime;

impl Runtime {
    pub(super) fn hostcall_random_bytes(
        &self,
        len: u32,
    ) -> std::result::Result<HostOperationState, AbiError> {
        const MAX_RANDOM_BYTES: u32 = 4096;
        if len > MAX_RANDOM_BYTES {
            return Err(AbiError::new(
                AbiErrorCode::MalformedPayload,
                format!("RandomBytes length {len} exceeds maximum {MAX_RANDOM_BYTES}"),
            ));
        }
        let len = len as usize;
        let mut buf = vec![0u8; len];
        let mut file = std::fs::File::open("/dev/urandom").map_err(|error| {
            AbiError::new(
                AbiErrorCode::Internal,
                format!("failed to open /dev/urandom: {error}"),
            )
        })?;
        file.read_exact(&mut buf).map_err(|error| {
            AbiError::new(
                AbiErrorCode::Internal,
                format!("failed to read /dev/urandom: {error}"),
            )
        })?;
        Ok(HostOperationState::Ready(HostcallOutput::RandomBytes(buf)))
    }
}
