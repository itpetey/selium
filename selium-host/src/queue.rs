//! Queue capability for async message passing.
//!
//! Provides async queue operations that yield to the host.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

use crate::kernel::Capability;

#[derive(Debug, Clone)]
pub enum QueueError {
    Closed,
    Disconnected,
    IoError(String),
}

impl std::fmt::Display for QueueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueueError::Closed => write!(f, "Queue is closed"),
            QueueError::Disconnected => write!(f, "Queue disconnected"),
            QueueError::IoError(msg) => write!(f, "IO error: {}", msg),
        }
    }
}

impl std::error::Error for QueueError {}

pub type QueueResult<T> = std::result::Result<T, QueueError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct QueueHandle {
    id: u64,
}

impl QueueHandle {
    pub fn new(id: u64) -> Self {
        Self { id }
    }

    pub fn id(&self) -> u64 {
        self.id
    }
}

struct QueueInner {
    sender: mpsc::Sender<Vec<u8>>,
    receiver: Arc<tokio::sync::RwLock<Option<mpsc::Receiver<Vec<u8>>>>>,
}

impl QueueInner {
    fn new(capacity: usize) -> (Self, QueueHandle) {
        static NEXT_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let id = NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let (sender, receiver): (mpsc::Sender<Vec<u8>>, mpsc::Receiver<Vec<u8>>) = mpsc::channel(capacity);

        (
            Self {
                sender,
                receiver: Arc::new(tokio::sync::RwLock::new(Some(receiver))),
            },
            QueueHandle::new(id),
        )
    }
}

pub struct QueueCapability {
    queues: Arc<tokio::sync::RwLock<HashMap<u64, Arc<QueueInner>>>>,
    senders: Arc<tokio::sync::RwLock<HashMap<u64, mpsc::Sender<Vec<u8>>>>>,
}

impl QueueCapability {
    pub fn new() -> Self {
        Self {
            queues: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            senders: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
        }
    }

    pub async fn create(&self, capacity: usize) -> QueueResult<QueueHandle> {
        let (queue, handle) = QueueInner::new(capacity);
        let sender = queue.sender.clone();
        let queue_arc = Arc::new(queue);

        {
            let mut queues = self.queues.write().await;
            queues.insert(handle.id, queue_arc.clone());
        }
        {
            let mut senders = self.senders.write().await;
            senders.insert(handle.id, sender);
        }

        Ok(handle)
    }

    pub async fn send(&self, handle: &QueueHandle, msg: Vec<u8>) -> QueueResult<()> {
        let sender = {
            let senders = self.senders.read().await;
            senders.get(&handle.id).cloned()
        };

        match sender {
            Some(sender) => {
                if sender.send(msg).await.is_err() {
                    Err(QueueError::Disconnected)
                } else {
                    Ok(())
                }
            }
            None => Err(QueueError::Closed),
        }
    }

    pub async fn recv(&self, handle: &QueueHandle) -> QueueResult<Vec<u8>> {
        let receiver = {
            let queues = self.queues.read().await;
            queues.get(&handle.id).map(|q| q.receiver.clone())
        };

        match receiver {
            Some(receiver) => {
                let mut guard: tokio::sync::RwLockWriteGuard<Option<mpsc::Receiver<Vec<u8>>>> = receiver.write().await;
                match guard.as_mut() {
                    Some(rx) => {
                        match rx.recv().await {
                            Some(v) => Ok(v),
                            None => Err(QueueError::Closed),
                        }
                    }
                    None => Err(QueueError::Closed),
                }
            }
            None => Err(QueueError::Closed),
        }
    }

    pub async fn close(&self, handle: &QueueHandle) -> QueueResult<()> {
        let mut queues = self.queues.write().await;
        if let Some(_queue) = queues.remove(&handle.id) {
        }
        let mut senders = self.senders.write().await;
        senders.remove(&handle.id);
        Ok(())
    }
}

impl Default for QueueCapability {
    fn default() -> Self {
        Self::new()
    }
}

impl Capability for QueueCapability {
    fn name(&self) -> &'static str {
        "selium::queue"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_queue_send_recv() {
        let queue = QueueCapability::new();

        let handle = queue.create(10).await.unwrap();

        queue.send(&handle, b"hello".to_vec()).await.unwrap();
        queue.send(&handle, b"world".to_vec()).await.unwrap();

        let msg1 = queue.recv(&handle).await.unwrap();
        let msg2 = queue.recv(&handle).await.unwrap();

        assert_eq!(msg1, b"hello");
        assert_eq!(msg2, b"world");
    }

    #[tokio::test]
    async fn test_queue_close() {
        let queue = QueueCapability::new();

        let handle = queue.create(10).await.unwrap();

        queue.send(&handle, b"test".to_vec()).await.unwrap();
        queue.close(&handle).await.unwrap();

        let result = queue.send(&handle, b"after close".to_vec()).await;
        assert!(matches!(result, Err(QueueError::Closed)));
    }
}
