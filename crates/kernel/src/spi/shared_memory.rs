//! Shared memory SPI contracts.

use std::sync::Arc;

use selium_abi::{GuestUint, ShmAlloc, ShmRegion};

use crate::guest_error::GuestError;

/// Capability responsible for shared memory region allocation.
pub trait SharedMemoryCapability {
    /// Runtime-specific error type.
    type Error: Into<GuestError>;

    /// Allocate a region in the shared memory arena.
    fn alloc(&self, request: ShmAlloc) -> Result<ShmRegion, Self::Error>;

    /// Read bytes from a previously allocated region.
    fn read(
        &self,
        region: ShmRegion,
        offset: GuestUint,
        len: GuestUint,
    ) -> Result<Vec<u8>, Self::Error>;

    /// Write bytes into a previously allocated region.
    fn write(&self, region: ShmRegion, offset: GuestUint, bytes: &[u8]) -> Result<(), Self::Error>;
}

impl<T> SharedMemoryCapability for Arc<T>
where
    T: SharedMemoryCapability,
{
    type Error = T::Error;

    fn alloc(&self, request: ShmAlloc) -> Result<ShmRegion, Self::Error> {
        self.as_ref().alloc(request)
    }

    fn read(
        &self,
        region: ShmRegion,
        offset: GuestUint,
        len: GuestUint,
    ) -> Result<Vec<u8>, Self::Error> {
        self.as_ref().read(region, offset, len)
    }

    fn write(&self, region: ShmRegion, offset: GuestUint, bytes: &[u8]) -> Result<(), Self::Error> {
        self.as_ref().write(region, offset, bytes)
    }
}
