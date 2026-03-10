//! Low-level shared-memory hostcalls for guest modules.
//!
//! Shared memory is typically used together with [`crate::queue`] for larger payload transfer. Most
//! applications can prefer [`crate::io`] unless they need to manage offsets and attachments
//! directly.

use rkyv::Archive;
use selium_abi::{
    GuestResourceId, GuestUint, ShmAlloc, ShmAttach, ShmDescriptor, ShmDetach, ShmRead, ShmShare,
    ShmWrite,
};

use crate::driver::{DriverError, DriverFuture, RKYV_VEC_OVERHEAD, RkyvDecoder, encode_args};

const SHM_DESCRIPTOR_CAPACITY: usize = core::mem::size_of::<<ShmDescriptor as Archive>::Archived>();
const RESOURCE_ID_CAPACITY: usize = core::mem::size_of::<<GuestResourceId as Archive>::Archived>();
const READ_RESULT_OVERHEAD: usize = RKYV_VEC_OVERHEAD + core::mem::size_of::<u64>();

/// Allocate a guest-owned shared-memory region.
///
/// The returned descriptor includes both the local resource id and the shareable id used by other
/// participants to [`attach`].
pub async fn alloc(size: GuestUint, align: GuestUint) -> Result<ShmDescriptor, DriverError> {
    let args = encode_args(&ShmAlloc { size, align })?;
    DriverFuture::<shm_alloc::Module, RkyvDecoder<ShmDescriptor>>::new(
        &args,
        SHM_DESCRIPTOR_CAPACITY,
        RkyvDecoder::new(),
    )?
    .await
}

/// Convert an existing local shared-memory resource id into a shareable identifier.
pub async fn share(resource_id: GuestUint) -> Result<GuestResourceId, DriverError> {
    let args = encode_args(&ShmShare { resource_id })?;
    DriverFuture::<shm_share::Module, RkyvDecoder<GuestResourceId>>::new(
        &args,
        RESOURCE_ID_CAPACITY,
        RkyvDecoder::new(),
    )?
    .await
}

/// Attach to a shared-memory region using its shared identifier.
pub async fn attach(shared_id: GuestResourceId) -> Result<ShmDescriptor, DriverError> {
    let args = encode_args(&ShmAttach { shared_id })?;
    DriverFuture::<shm_attach::Module, RkyvDecoder<ShmDescriptor>>::new(
        &args,
        SHM_DESCRIPTOR_CAPACITY,
        RkyvDecoder::new(),
    )?
    .await
}

/// Detach a local shared-memory handle when it is no longer needed.
pub async fn detach(resource_id: GuestUint) -> Result<(), DriverError> {
    let args = encode_args(&ShmDetach { resource_id })?;
    DriverFuture::<shm_detach::Module, RkyvDecoder<()>>::new(&args, 0, RkyvDecoder::new())?.await?;
    Ok(())
}

/// Read bytes from a shared-memory region.
///
/// `offset` and `len` are interpreted relative to the attached region described by the resource id.
pub async fn read(
    resource_id: GuestUint,
    offset: GuestUint,
    len: GuestUint,
) -> Result<Vec<u8>, DriverError> {
    let args = encode_args(&ShmRead {
        resource_id,
        offset,
        len,
    })?;
    let capacity = usize::try_from(len).map_err(|_| DriverError::InvalidArgument)?;
    DriverFuture::<shm_read::Module, RkyvDecoder<Vec<u8>>>::new(
        &args,
        capacity + READ_RESULT_OVERHEAD,
        RkyvDecoder::new(),
    )?
    .await
}

/// Write bytes into a shared-memory region starting at the given offset.
pub async fn write(
    resource_id: GuestUint,
    offset: GuestUint,
    bytes: impl Into<Vec<u8>>,
) -> Result<(), DriverError> {
    let args = encode_args(&ShmWrite {
        resource_id,
        offset,
        bytes: bytes.into(),
    })?;
    DriverFuture::<shm_write::Module, RkyvDecoder<()>>::new(&args, 0, RkyvDecoder::new())?.await?;
    Ok(())
}

driver_module!(shm_alloc, "selium::shm::alloc");
driver_module!(shm_share, "selium::shm::share");
driver_module!(shm_attach, "selium::shm::attach");
driver_module!(shm_detach, "selium::shm::detach");
driver_module!(shm_read, "selium::shm::read");
driver_module!(shm_write, "selium::shm::write");

#[cfg(test)]
mod tests {
    use super::*;
    use selium_abi::{ShmRegion, encode_rkyv};

    #[test]
    fn alloc_returns_kernel_error_with_native_stub_driver() {
        let err = crate::block_on(alloc(64, 8)).expect_err("stub should fail");
        assert!(matches!(err, DriverError::Kernel(2)));
    }

    #[test]
    fn read_returns_kernel_error_with_native_stub_driver() {
        let err = crate::block_on(read(1, 0, 4)).expect_err("stub should fail");
        assert!(matches!(err, DriverError::Kernel(2)));
    }

    #[test]
    fn descriptor_capacity_covers_archived_payload() {
        let descriptor = ShmDescriptor {
            resource_id: 3,
            shared_id: 9,
            region: ShmRegion {
                offset: 16,
                len: 64,
            },
        };
        let encoded = encode_rkyv(&descriptor).expect("encode descriptor");
        assert!(encoded.len() <= SHM_DESCRIPTOR_CAPACITY);
    }

    #[test]
    fn read_overhead_covers_vec_archive_metadata() {
        let payload = vec![1u8, 2, 3, 4, 5];
        let encoded = encode_rkyv(&payload).expect("encode vec");
        assert!(encoded.len() <= payload.len() + READ_RESULT_OVERHEAD);
    }
}
