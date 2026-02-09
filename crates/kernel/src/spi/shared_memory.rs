//! Shared memory SPI contracts.

use std::sync::Arc;

use selium_abi::{ShmAlloc, ShmRegion};

use crate::guest_error::GuestError;

/// Capability responsible for shared memory region allocation.
pub trait SharedMemoryCapability {
    /// Runtime-specific error type.
    type Error: Into<GuestError>;

    /// Allocate a region in the shared memory arena.
    fn alloc(&self, request: ShmAlloc) -> Result<ShmRegion, Self::Error>;
}

impl<T> SharedMemoryCapability for Arc<T>
where
    T: SharedMemoryCapability,
{
    type Error = T::Error;

    fn alloc(&self, request: ShmAlloc) -> Result<ShmRegion, Self::Error> {
        self.as_ref().alloc(request)
    }
}
