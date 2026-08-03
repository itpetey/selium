use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use parking_lot::{Condvar, Mutex};
use selium_abi::{HostQueueDescriptor, SharedResourceId};

use crate::memory::MemoryRegistry;
use crate::{Error, Result};

#[derive(Clone)]
pub struct HostQueueRegistry {
    pub(crate) inner: Arc<HostQueueRegistryInner>,
}

pub(crate) struct HostQueueRegistryInner {
    pub(crate) queues_by_shared: Mutex<HashMap<SharedResourceId, Arc<HostQueueState>>>,
    pub(crate) local_queues: Mutex<HashMap<u64, SharedResourceId>>,
}

pub(crate) struct HostQueueState {
    pub(crate) entries: Mutex<VecDeque<(u64, u64)>>,
    pub(crate) notify: Condvar,
}

impl HostQueueRegistry {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(HostQueueRegistryInner {
                queues_by_shared: Mutex::new(HashMap::new()),
                local_queues: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Returns the shared id for a local host queue handle.
    pub fn host_queue_shared_id(&self, local_id: u64) -> Result<SharedResourceId> {
        self.inner
            .local_queues
            .lock()
            .get(&local_id)
            .copied()
            .ok_or(Error::NotFound(format!("host queue handle {local_id}")))
    }

    /// Creates a new host-mediated connection queue.
    pub fn create_host_queue(&self, memory: &MemoryRegistry) -> HostQueueDescriptor {
        let local_id = memory.next_local_id();
        let shared_id = memory.next_shared_id();
        self.inner.queues_by_shared.lock().insert(
            shared_id,
            std::sync::Arc::new(HostQueueState {
                entries: parking_lot::Mutex::new(VecDeque::new()),
                notify: parking_lot::Condvar::new(),
            }),
        );
        self.inner.local_queues.lock().insert(local_id, shared_id);
        HostQueueDescriptor {
            local_id,
            shared_id,
        }
    }

    /// Attaches a local handle to an existing host queue.
    pub fn attach_host_queue(
        &self,
        memory: &MemoryRegistry,
        shared_id: SharedResourceId,
    ) -> Result<HostQueueDescriptor> {
        let queues = self.inner.queues_by_shared.lock();
        if !queues.contains_key(&shared_id) {
            return Err(Error::NotFound(format!("host queue {shared_id}")));
        }
        let local_id = memory.next_local_id();
        self.inner.local_queues.lock().insert(local_id, shared_id);
        Ok(HostQueueDescriptor {
            local_id,
            shared_id,
        })
    }

    /// Enqueues a value into a host queue.
    pub fn host_queue_send(&self, local_id: u64, client_process_id: u64, value: u64) -> Result<()> {
        let shared_id = self
            .inner
            .local_queues
            .lock()
            .get(&local_id)
            .copied()
            .ok_or(Error::NotFound(format!("host queue handle {local_id}")))?;
        let queues = self.inner.queues_by_shared.lock();
        let queue = queues
            .get(&shared_id)
            .ok_or(Error::NotFound(format!("host queue {shared_id}")))?;
        queue.entries.lock().push_back((client_process_id, value));
        queue.notify.notify_all();
        Ok(())
    }

    /// Tries to dequeue the next value from a host queue without waiting.
    pub fn try_host_queue_recv(&self, local_id: u64) -> Result<Option<(u64, u64)>> {
        let shared_id = self
            .inner
            .local_queues
            .lock()
            .get(&local_id)
            .copied()
            .ok_or(Error::NotFound(format!("host queue handle {local_id}")))?;
        let queue = self
            .inner
            .queues_by_shared
            .lock()
            .get(&shared_id)
            .cloned()
            .ok_or(Error::NotFound(format!("host queue {shared_id}")))?;
        Ok(queue.entries.lock().pop_front())
    }
}
