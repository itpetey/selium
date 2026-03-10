//! Guest-facing helpers for session lifecycle hostcalls.
//!
//! These functions let a guest manage child sessions: create them, remove them, grant or revoke
//! capabilities, and attach or detach specific resources under those capabilities.

use selium_abi::{
    Capability, GuestResourceId, GuestUint, SessionCreate, SessionEntitlement, SessionRemove,
    SessionResource,
};

use crate::driver::{DriverError, DriverFuture, RkyvDecoder, encode_args};

/// Create a child session beneath `session_id` with the provided public key.
///
/// Returns the new child session identifier assigned by the runtime.
pub async fn create(session_id: GuestUint, pubkey: [u8; 32]) -> Result<GuestUint, DriverError> {
    let args = encode_args(&SessionCreate { session_id, pubkey })?;
    DriverFuture::<session_create::Module, RkyvDecoder<GuestUint>>::new(
        &args,
        8,
        RkyvDecoder::new(),
    )?
    .await
}

/// Remove child session `target_id` from parent session `session_id`.
pub async fn remove(session_id: GuestUint, target_id: GuestUint) -> Result<(), DriverError> {
    let args = encode_args(&SessionRemove {
        session_id,
        target_id,
    })?;
    DriverFuture::<session_remove::Module, RkyvDecoder<()>>::new(&args, 0, RkyvDecoder::new())?
        .await?;
    Ok(())
}

/// Grant `capability` from parent session `session_id` to child session `target_id`.
pub async fn add_entitlement(
    session_id: GuestUint,
    target_id: GuestUint,
    capability: Capability,
) -> Result<(), DriverError> {
    let args = encode_args(&SessionEntitlement {
        session_id,
        target_id,
        capability,
    })?;
    DriverFuture::<session_add_entitlement::Module, RkyvDecoder<()>>::new(
        &args,
        0,
        RkyvDecoder::new(),
    )?
    .await?;
    Ok(())
}

/// Revoke `capability` from child session `target_id`.
pub async fn rm_entitlement(
    session_id: GuestUint,
    target_id: GuestUint,
    capability: Capability,
) -> Result<(), DriverError> {
    let args = encode_args(&SessionEntitlement {
        session_id,
        target_id,
        capability,
    })?;
    DriverFuture::<session_rm_entitlement::Module, RkyvDecoder<()>>::new(
        &args,
        0,
        RkyvDecoder::new(),
    )?
    .await?;
    Ok(())
}

/// Grant `resource_id` under `capability` from `session_id` to `target_id`.
///
/// Returns `true` when the child's resource set changed.
pub async fn add_resource(
    session_id: GuestUint,
    target_id: GuestUint,
    capability: Capability,
    resource_id: GuestResourceId,
) -> Result<bool, DriverError> {
    let args = encode_args(&SessionResource {
        session_id,
        target_id,
        capability,
        resource_id,
    })?;
    let changed = DriverFuture::<session_add_resource::Module, RkyvDecoder<GuestUint>>::new(
        &args,
        8,
        RkyvDecoder::new(),
    )?
    .await?;
    Ok(changed != 0)
}

/// Remove `resource_id` under `capability` from child session `target_id`.
///
/// Returns `true` when the child's resource set changed.
pub async fn rm_resource(
    session_id: GuestUint,
    target_id: GuestUint,
    capability: Capability,
    resource_id: GuestResourceId,
) -> Result<bool, DriverError> {
    let args = encode_args(&SessionResource {
        session_id,
        target_id,
        capability,
        resource_id,
    })?;
    let changed = DriverFuture::<session_rm_resource::Module, RkyvDecoder<GuestUint>>::new(
        &args,
        8,
        RkyvDecoder::new(),
    )?
    .await?;
    Ok(changed != 0)
}

driver_module!(session_create, "selium::session::create");
driver_module!(session_remove, "selium::session::remove");
driver_module!(session_add_entitlement, "selium::session::add_entitlement");
driver_module!(session_rm_entitlement, "selium::session::rm_entitlement");
driver_module!(session_add_resource, "selium::session::add_resource");
driver_module!(session_rm_resource, "selium::session::rm_resource");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_returns_kernel_error_with_native_stub_driver() {
        let err = crate::block_on(create(1, [0; 32])).expect_err("stub should fail");
        assert!(matches!(err, DriverError::Kernel(2)));
    }

    #[test]
    fn add_resource_returns_kernel_error_with_native_stub_driver() {
        let err = crate::block_on(add_resource(1, 2, Capability::TimeRead, 3))
            .expect_err("stub should fail");
        assert!(matches!(err, DriverError::Kernel(2)));
    }
}
