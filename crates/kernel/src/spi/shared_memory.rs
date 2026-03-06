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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct Driver {
        calls: Mutex<Vec<&'static str>>,
    }

    impl SharedMemoryCapability for Driver {
        type Error = GuestError;

        fn alloc(&self, request: ShmAlloc) -> Result<ShmRegion, Self::Error> {
            self.calls.lock().expect("calls lock").push("alloc");
            Ok(ShmRegion {
                offset: request.align,
                len: request.size,
            })
        }

        fn read(
            &self,
            _region: ShmRegion,
            _offset: GuestUint,
            len: GuestUint,
        ) -> Result<Vec<u8>, Self::Error> {
            self.calls.lock().expect("calls lock").push("read");
            Ok(vec![0; len as usize])
        }

        fn write(
            &self,
            _region: ShmRegion,
            _offset: GuestUint,
            _bytes: &[u8],
        ) -> Result<(), Self::Error> {
            self.calls.lock().expect("calls lock").push("write");
            Ok(())
        }
    }

    #[test]
    fn arc_wrapper_forwards_all_calls() {
        let driver = Arc::new(Driver {
            calls: Mutex::new(Vec::new()),
        });
        let region = driver.alloc(ShmAlloc { size: 8, align: 4 }).expect("alloc");
        assert_eq!(region.offset, 4);
        assert_eq!(driver.read(region, 0, 2).expect("read"), vec![0, 0]);
        driver.write(region, 0, &[1, 2]).expect("write");

        let calls = driver.calls.lock().expect("calls lock");
        assert_eq!(calls.as_slice(), ["alloc", "read", "write"]);
    }
}
