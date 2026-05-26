use std::collections::VecDeque;

use selium_abi::{HostQueueDescriptor, SharedResourceId};
use tokio::sync::Notify;

use crate::{
    Error, Result,
    state::{HostQueueState, Kernel},
};

impl Kernel {
    /// Returns the shared id for a local host queue handle.
    pub fn host_queue_shared_id(&self, local_id: u64) -> Result<SharedResourceId> {
        self.inner
            .local_host_queues
            .lock()
            .get(&local_id)
            .copied()
            .ok_or(Error::NotFound(format!("host queue handle {local_id}")))
    }

    /// Creates a new host-mediated connection queue.
    pub fn create_host_queue(&self) -> HostQueueDescriptor {
        let local_id = self.next_local_id();
        let shared_id = self.next_shared_id();
        self.inner.host_queues_by_shared.lock().insert(
            shared_id,
            std::sync::Arc::new(HostQueueState {
                entries: parking_lot::Mutex::new(VecDeque::new()),
                notify: Notify::new(),
            }),
        );
        self.inner
            .local_host_queues
            .lock()
            .insert(local_id, shared_id);
        HostQueueDescriptor {
            local_id,
            shared_id,
        }
    }

    /// Attaches a local handle to an existing host queue.
    pub fn attach_host_queue(&self, shared_id: SharedResourceId) -> Result<HostQueueDescriptor> {
        let queues = self.inner.host_queues_by_shared.lock();
        if !queues.contains_key(&shared_id) {
            return Err(Error::NotFound(format!("host queue {shared_id}")));
        }
        let local_id = self.next_local_id();
        self.inner
            .local_host_queues
            .lock()
            .insert(local_id, shared_id);
        Ok(HostQueueDescriptor {
            local_id,
            shared_id,
        })
    }

    /// Enqueues a value into a host queue.
    pub fn host_queue_send(&self, local_id: u64, client_process_id: u64, value: u64) -> Result<()> {
        let shared_id = self
            .inner
            .local_host_queues
            .lock()
            .get(&local_id)
            .copied()
            .ok_or(Error::NotFound(format!("host queue handle {local_id}")))?;
        let queues = self.inner.host_queues_by_shared.lock();
        let queue = queues
            .get(&shared_id)
            .ok_or(Error::NotFound(format!("host queue {shared_id}")))?;
        queue.entries.lock().push_back((client_process_id, value));
        queue.notify.notify_one();
        Ok(())
    }

    /// Tries to dequeue the next value from a host queue without waiting.
    pub fn try_host_queue_recv(&self, local_id: u64) -> Result<Option<(u64, u64)>> {
        let shared_id = self
            .inner
            .local_host_queues
            .lock()
            .get(&local_id)
            .copied()
            .ok_or(Error::NotFound(format!("host queue handle {local_id}")))?;
        let queue = self
            .inner
            .host_queues_by_shared
            .lock()
            .get(&shared_id)
            .cloned()
            .ok_or(Error::NotFound(format!("host queue {shared_id}")))?;
        Ok(queue.entries.lock().pop_front())
    }

    /// Dequeues the next value from a host queue, waiting if empty.
    pub async fn host_queue_recv(&self, local_id: u64) -> Result<(u64, u64)> {
        let shared_id = self
            .inner
            .local_host_queues
            .lock()
            .get(&local_id)
            .copied()
            .ok_or(Error::NotFound(format!("host queue handle {local_id}")))?;
        let queue = self
            .inner
            .host_queues_by_shared
            .lock()
            .get(&shared_id)
            .cloned()
            .ok_or(Error::NotFound(format!("host queue {shared_id}")))?;
        loop {
            let entry = queue.entries.lock().pop_front();
            if let Some(entry) = entry {
                return Ok(entry);
            }
            queue.notify.notified().await;
        }
    }
}
