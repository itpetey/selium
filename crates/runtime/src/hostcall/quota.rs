//! Quota reservations and queue item accounting.
//!
//! `QuotaReservation` releases its bytes on drop unless committed, so an
//! allocation that fails after reserving cannot leak the tenant's ceiling.

use selium_abi::{AbiError, AbiErrorCode, ResourceClass};

use crate::Runtime;

/// A quota reservation that releases its bytes on drop unless committed.
pub(crate) struct QuotaReservation {
    kernel: selium_kernel::Kernel,
    tenant: String,
    class: ResourceClass,
    amount: u64,
    committed: bool,
}

impl QuotaReservation {
    pub(crate) fn commit(&mut self) {
        self.committed = true;
    }
}

impl Drop for QuotaReservation {
    fn drop(&mut self) {
        if !self.committed {
            self.kernel
                .quota()
                .release(&self.tenant, self.class.clone(), self.amount);
        }
    }
}

impl Runtime {
    /// Reserves `amount` of the tenant's quota ceiling for `class`, denying
    /// the allocation (before any resource is granted) when it would exceed
    /// the authored ceiling. The platform tenant (empty name) is unrestricted.
    pub(super) fn enforce_quota(
        &self,
        tenant: &str,
        class: ResourceClass,
        amount: u64,
    ) -> std::result::Result<(), AbiError> {
        self.kernel
            .quota()
            .try_consume(tenant, class, amount)
            .map_err(|error| AbiError::new(AbiErrorCode::QuotaExceeded, error.to_string()))
    }

    /// Reserves quota and returns a guard that releases it unless committed.
    pub(super) fn reserve_quota(
        &self,
        tenant: &str,
        class: ResourceClass,
        amount: u64,
    ) -> std::result::Result<QuotaReservation, AbiError> {
        self.enforce_quota(tenant, class.clone(), amount)?;
        Ok(QuotaReservation {
            kernel: self.kernel.clone(),
            tenant: tenant.to_string(),
            class,
            amount,
            committed: false,
        })
    }

    /// Returns the tenant whose pipe quota meters a queue: the tenant of the
    /// process that owns the queue. Items pending in a tenant's queues count
    /// against that tenant, whoever sent them.
    fn queue_owner_tenant(&self, shared_id: u64) -> Option<String> {
        let owner = self
            .shared_resource_owners
            .lock()
            .get(&(ResourceClass::HostQueue, shared_id))
            .and_then(|owners| owners.iter().next().copied())?;
        self.process_tenant(owner)
    }

    /// Reserves one pipe slot against the queue owner's (serving) tenant for
    /// an item enqueued onto `shared_id`, denying the enqueue when the
    /// tenant's ceiling is exhausted.
    pub(crate) fn reserve_queue_item(
        &self,
        shared_id: u64,
    ) -> std::result::Result<Option<QuotaReservation>, AbiError> {
        match self.queue_owner_tenant(shared_id) {
            Some(tenant) => self
                .reserve_quota(&tenant, ResourceClass::HostQueue, 1)
                .map(Some),
            None => Ok(None),
        }
    }

    /// Releases the pipe slot reserved for a delivered (dequeued) item: the
    /// item has left the queue and entered the receiving process's resource
    /// table.
    pub(crate) fn release_queue_item(&self, shared_id: u64) {
        if let Some(tenant) = self.queue_owner_tenant(shared_id) {
            self.kernel
                .quota()
                .release(&tenant, ResourceClass::HostQueue, 1);
        }
    }
}
