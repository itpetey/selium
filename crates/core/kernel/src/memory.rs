use std::sync::atomic::Ordering;

use selium_abi::{SharedMappingDescriptor, SharedRegionDescriptor, SharedResourceId};

use crate::{
    Error, Result,
    error::map_wasm_error,
    state::{Kernel, SharedMappingState, SharedRegionRecord},
};

impl Kernel {
    /// Allocates a shared memory region and returns its descriptor.
    pub fn allocate_shared_region(
        &self,
        size: u64,
        alignment: u64,
    ) -> Result<SharedRegionDescriptor> {
        let region_id = self
            .inner
            .store
            .lock()
            .allocate_shared_region(size, alignment)
            .map_err(map_wasm_error)?;
        let shared_id = self.next_shared_id();
        let len = self
            .inner
            .store
            .lock()
            .shared_region_len(region_id)
            .map_err(map_wasm_error)?;
        self.inner
            .shared_regions
            .lock()
            .insert(shared_id, SharedRegionRecord { region_id });

        Ok(SharedRegionDescriptor { shared_id, len })
    }

    /// Attaches a local mapping to a shared memory region.
    pub fn attach_shared_region(
        &self,
        shared_id: SharedResourceId,
        region_offset: u64,
        len: u64,
    ) -> Result<SharedMappingDescriptor> {
        let region = self
            .inner
            .shared_regions
            .lock()
            .get(&shared_id)
            .map(|record| record.region_id)
            .ok_or_else(|| Error::NotFound(format!("shared region {shared_id}")))?;
        let mapping = self
            .inner
            .store
            .lock()
            .attach_shared_region(region, region_offset, len)
            .map_err(map_wasm_error)?;
        let local_id = self.next_local_id();
        self.inner
            .shared_mappings
            .lock()
            .insert(local_id, SharedMappingState { mapping, shared_id });

        Ok(SharedMappingDescriptor {
            local_id,
            shared_id,
            len,
        })
    }

    /// Destroys a shared memory region when no mappings remain.
    pub fn destroy_shared_region(&self, shared_id: SharedResourceId) -> Result<()> {
        if self.shared_region_mapping_count(shared_id) > 0 {
            return Err(Error::Wasm(
                "shared region still has attached mappings".to_string(),
            ));
        }
        let region_id = self
            .inner
            .shared_regions
            .lock()
            .get(&shared_id)
            .map(|region| region.region_id)
            .ok_or_else(|| Error::NotFound(format!("shared region {shared_id}")))?;
        self.inner
            .store
            .lock()
            .destroy_shared_region(region_id)
            .map_err(map_wasm_error)?;
        self.inner.shared_regions.lock().remove(&shared_id);
        Ok(())
    }

    /// Detaches a local shared memory mapping.
    pub fn detach_shared_region(&self, local_id: u64) -> Result<()> {
        let mapping = self
            .inner
            .shared_mappings
            .lock()
            .remove(&local_id)
            .ok_or_else(|| Error::NotFound(format!("shared mapping {local_id}")))?;
        self.inner
            .store
            .lock()
            .detach_shared_region(mapping.mapping)
            .map_err(map_wasm_error)
    }

    /// Reads bytes from a local shared memory mapping.
    pub fn read_shared_memory(&self, local_id: u64, offset: u64, len: usize) -> Result<Vec<u8>> {
        let mapping = self.shared_mapping(local_id)?;
        let mut bytes = vec![0_u8; len];
        self.inner
            .store
            .lock()
            .read_shared_region(mapping.mapping, offset, &mut bytes)
            .map_err(map_wasm_error)?;
        Ok(bytes)
    }

    /// Writes bytes to a local shared memory mapping.
    pub fn write_shared_memory(&self, local_id: u64, offset: u64, bytes: &[u8]) -> Result<()> {
        let mapping = self.shared_mapping(local_id)?;
        self.inner
            .store
            .lock()
            .write_shared_region(mapping.mapping, offset, bytes)
            .map_err(map_wasm_error)
    }

    /// Atomically adds to a little-endian `u64` in a local shared memory mapping.
    pub fn fetch_add_shared_memory_u64(
        &self,
        local_id: u64,
        offset: u64,
        value: u64,
    ) -> Result<u64> {
        let mapping = self.shared_mapping(local_id)?;
        let store = self.inner.store.lock();
        let mut bytes = [0_u8; 8];
        store
            .read_shared_region(mapping.mapping, offset, &mut bytes)
            .map_err(map_wasm_error)?;
        let previous = u64::from_le_bytes(bytes);
        let next = previous.wrapping_add(value);
        store
            .write_shared_region(mapping.mapping, offset, &next.to_le_bytes())
            .map_err(map_wasm_error)?;
        Ok(previous)
    }

    /// Atomically compares and exchanges a little-endian `u64` in a local shared memory mapping.
    pub fn compare_exchange_shared_memory_u64(
        &self,
        local_id: u64,
        offset: u64,
        current: u64,
        new: u64,
    ) -> Result<u64> {
        let mapping = self.shared_mapping(local_id)?;
        let store = self.inner.store.lock();
        let mut bytes = [0_u8; 8];
        store
            .read_shared_region(mapping.mapping, offset, &mut bytes)
            .map_err(map_wasm_error)?;
        let previous = u64::from_le_bytes(bytes);
        if previous == current {
            store
                .write_shared_region(mapping.mapping, offset, &new.to_le_bytes())
                .map_err(map_wasm_error)?;
        }
        Ok(previous)
    }

    /// Returns the shared region id backing a local mapping.
    pub fn shared_mapping_shared_id(&self, local_id: u64) -> Result<SharedResourceId> {
        Ok(self.shared_mapping(local_id)?.shared_id)
    }

    /// Returns the number of local mappings attached to a shared region.
    pub fn shared_region_mapping_count(&self, shared_id: SharedResourceId) -> usize {
        self.inner
            .shared_mappings
            .lock()
            .values()
            .filter(|mapping| mapping.shared_id == shared_id)
            .count()
    }

    pub(crate) fn shared_mapping(&self, local_id: u64) -> Result<SharedMappingState> {
        self.inner
            .shared_mappings
            .lock()
            .get(&local_id)
            .copied()
            .ok_or_else(|| Error::NotFound(format!("shared mapping {local_id}")))
    }

    pub(crate) fn next_local_id(&self) -> u64 {
        self.inner.next_local_id.fetch_add(1, Ordering::SeqCst) + 1
    }

    pub(crate) fn next_shared_id(&self) -> u64 {
        self.inner.next_shared_id.fetch_add(1, Ordering::SeqCst) + 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn shared_memory_round_trips_between_attachments() {
        let kernel = Kernel::default();
        let region = kernel
            .allocate_shared_region(64, 8)
            .expect("allocate region");
        let left = kernel
            .attach_shared_region(region.shared_id, 0, 64)
            .expect("attach left");
        let right = kernel
            .attach_shared_region(region.shared_id, 0, 64)
            .expect("attach right");

        kernel
            .write_shared_memory(left.local_id, 0, b"hello")
            .expect("write left");
        let bytes = kernel
            .read_shared_memory(right.local_id, 0, 5)
            .expect("read right");
        assert_eq!(bytes, b"hello");
        assert!(matches!(
            kernel.destroy_shared_region(region.shared_id),
            Err(Error::Wasm(_))
        ));
    }

    #[tokio::test]
    async fn shared_memory_u64_atomics_are_visible_between_attachments() {
        let kernel = Kernel::default();
        let region = kernel
            .allocate_shared_region(64, 8)
            .expect("allocate region");
        let left = kernel
            .attach_shared_region(region.shared_id, 0, 64)
            .expect("attach left");
        let right = kernel
            .attach_shared_region(region.shared_id, 0, 64)
            .expect("attach right");

        assert_eq!(
            kernel
                .fetch_add_shared_memory_u64(left.local_id, 8, 3)
                .expect("fetch add"),
            0
        );
        assert_eq!(
            kernel
                .compare_exchange_shared_memory_u64(right.local_id, 8, 3, 7)
                .expect("compare exchange"),
            3
        );
        assert_eq!(
            kernel
                .read_shared_memory(left.local_id, 8, 8)
                .expect("read value"),
            7_u64.to_le_bytes()
        );
    }
}
