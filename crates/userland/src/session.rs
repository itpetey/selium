//! Guest-facing helpers for session lifecycle hostcalls.

use selium_abi::{
    Capability, GuestResourceId, GuestUint, SessionCreate, SessionEntitlement, SessionRemove,
    SessionResource,
};

use crate::driver::{DriverError, DriverFuture, RkyvDecoder, encode_args};

/// Create a new session under `session_id` with the provided public key.
pub async fn create(session_id: GuestUint, pubkey: [u8; 32]) -> Result<GuestUint, DriverError> {
    let args = encode_args(&SessionCreate { session_id, pubkey })?;
    DriverFuture::<session_create::Module, RkyvDecoder<GuestUint>>::new(
        &args,
        8,
        RkyvDecoder::new(),
    )?
    .await
}

/// Remove a child session.
pub async fn remove(session_id: GuestUint, target_id: GuestUint) -> Result<(), DriverError> {
    let args = encode_args(&SessionRemove {
        session_id,
        target_id,
    })?;
    DriverFuture::<session_remove::Module, RkyvDecoder<()>>::new(&args, 0, RkyvDecoder::new())?
        .await?;
    Ok(())
}

/// Grant a capability to a child session.
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

/// Remove a capability from a child session.
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

/// Grant a resource to a child session capability set.
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

/// Remove a resource from a child session capability set.
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

driver_module!(session_create, SESSION_CREATE, "selium::session::create");
driver_module!(session_remove, SESSION_REMOVE, "selium::session::remove");
driver_module!(
    session_add_entitlement,
    SESSION_ADD_ENTITLEMENT,
    "selium::session::add_entitlement"
);
driver_module!(
    session_rm_entitlement,
    SESSION_RM_ENTITLEMENT,
    "selium::session::rm_entitlement"
);
driver_module!(
    session_add_resource,
    SESSION_ADD_RESOURCE,
    "selium::session::add_resource"
);
driver_module!(
    session_rm_resource,
    SESSION_RM_RESOURCE,
    "selium::session::rm_resource"
);
