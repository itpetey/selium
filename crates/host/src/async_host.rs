//! Async host extension for executing I/O operations.
//!
//! This module provides the capability to execute async I/O operations
//! on behalf of guests, allowing guests to yield to the host while
//! operations complete.

use parking_lot::RwLock;
use std::future::Future;
use std::sync::Arc;

use crate::guest::GuestId;
use crate::kernel::Capability;

pub type TaskId = u64;

static NEXT_TASK_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

pub fn next_task_id() -> TaskId {
    NEXT_TASK_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

#[derive(Clone)]
pub struct AsyncHostExtension {
    guest_tasks: Arc<RwLock<std::collections::HashMap<GuestId, tokio::sync::mpsc::Sender<TaskId>>>>,
}

impl AsyncHostExtension {
    pub fn new() -> Self {
        Self {
            guest_tasks: Arc::new(RwLock::new(std::collections::HashMap::new())),
        }
    }

    pub fn register_guest(
        &self,
        guest_id: GuestId,
    ) -> (
        tokio::sync::mpsc::Sender<TaskId>,
        tokio::sync::mpsc::Receiver<TaskId>,
    ) {
        let (tx, rx) = tokio::sync::mpsc::channel(32);
        let mut tasks = self.guest_tasks.write();
        tasks.insert(guest_id, tx.clone());
        (tx, rx)
    }

    pub fn unregister_guest(&self, guest_id: &GuestId) {
        let mut tasks = self.guest_tasks.write();
        tasks.remove(guest_id);
    }

    pub fn spawn_task<F>(&self, guest_id: GuestId, future: F) -> TaskId
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let task_id = next_task_id();

        let tasks = self.guest_tasks.read();
        if let Some(tx) = tasks.get(&guest_id) {
            let tx = tx.clone();
            tokio::spawn(async move {
                future.await;
                let _ = tx.send(task_id).await;
            });
        }

        task_id
    }

    #[allow(dead_code)]
    pub async fn wait_for_wake(rx: &mut tokio::sync::mpsc::Receiver<TaskId>) -> Option<TaskId> {
        rx.recv().await
    }
}

impl Default for AsyncHostExtension {
    fn default() -> Self {
        Self::new()
    }
}

impl Capability for AsyncHostExtension {
    fn name(&self) -> &'static str {
        "selium::async_host"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_next_task_id_increments() {
        let id1 = next_task_id();
        let id2 = next_task_id();
        let id3 = next_task_id();

        assert!(id2 > id1);
        assert!(id3 > id2);
    }

    #[test]
    fn test_async_host_extension_new() {
        let ext = AsyncHostExtension::new();
        assert_eq!(ext.name(), "selium::async_host");
    }

    #[tokio::test]
    async fn test_spawn_task() {
        use crate::guest::GuestId;

        let ext = AsyncHostExtension::new();
        let guest_id = GuestId::new(1);

        let (_tx, mut rx) = ext.register_guest(guest_id.clone());

        let task_id = ext.spawn_task(guest_id.clone(), async {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        });

        assert!(task_id > 0);

        let woken_id = rx.recv().await;
        assert_eq!(woken_id, Some(task_id));
    }
}
