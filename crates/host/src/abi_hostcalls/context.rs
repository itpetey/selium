use selium_abi::{Capability, GuestContext};

pub type AbiContext = GuestContext;

pub fn ensure_capability(
    ctx: &AbiContext,
    capability: Capability,
) -> Result<(), selium_abi::GuestError> {
    if ctx.has_capability(capability) {
        Ok(())
    } else {
        Err(selium_abi::GuestError::PermissionDenied)
    }
}
