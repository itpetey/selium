//! Guest-facing helpers for shared-memory hostcalls.

use selium_abi::{
    GuestResourceId, GuestUint, ShmAlloc, ShmAttach, ShmDescriptor, ShmDetach, ShmShare,
};

use crate::driver::{DriverError, DriverFuture, RkyvDecoder, encode_args};

/// Allocate a shared-memory region.
pub async fn alloc(size: GuestUint, align: GuestUint) -> Result<ShmDescriptor, DriverError> {
    let args = encode_args(&ShmAlloc { size, align })?;
    DriverFuture::<shm_alloc::Module, RkyvDecoder<ShmDescriptor>>::new(
        &args,
        20,
        RkyvDecoder::new(),
    )?
    .await
}

/// Share a local shared-memory resource.
pub async fn share(resource_id: GuestUint) -> Result<GuestResourceId, DriverError> {
    let args = encode_args(&ShmShare { resource_id })?;
    DriverFuture::<shm_share::Module, RkyvDecoder<GuestResourceId>>::new(
        &args,
        8,
        RkyvDecoder::new(),
    )?
    .await
}

/// Attach a shared-memory resource by shared handle.
pub async fn attach(shared_id: GuestResourceId) -> Result<ShmDescriptor, DriverError> {
    let args = encode_args(&ShmAttach { shared_id })?;
    DriverFuture::<shm_attach::Module, RkyvDecoder<ShmDescriptor>>::new(
        &args,
        20,
        RkyvDecoder::new(),
    )?
    .await
}

/// Detach a local shared-memory handle.
pub async fn detach(resource_id: GuestUint) -> Result<(), DriverError> {
    let args = encode_args(&ShmDetach { resource_id })?;
    DriverFuture::<shm_detach::Module, RkyvDecoder<()>>::new(&args, 0, RkyvDecoder::new())?.await?;
    Ok(())
}

driver_module!(shm_alloc, SHM_ALLOC, "selium::shm::alloc");
driver_module!(shm_share, SHM_SHARE, "selium::shm::share");
driver_module!(shm_attach, SHM_ATTACH, "selium::shm::attach");
driver_module!(shm_detach, SHM_DETACH, "selium::shm::detach");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alloc_returns_kernel_error_with_native_stub_driver() {
        let err = crate::block_on(alloc(64, 8)).expect_err("stub should fail");
        assert!(matches!(err, DriverError::Kernel(2)));
    }

    #[test]
    fn attach_returns_kernel_error_with_native_stub_driver() {
        let err = crate::block_on(attach(1)).expect_err("stub should fail");
        assert!(matches!(err, DriverError::Kernel(2)));
    }
}
