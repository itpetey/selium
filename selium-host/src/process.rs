//! Process lifecycle management.
//!
//! Provides spawn, stop, and JoinHandle for guest processes.

use std::sync::Arc;
use parking_lot::RwLock;
use tokio::sync::oneshot;

use crate::guest::GuestId;
use crate::error::GuestExitStatus;

/// A handle to a spawned guest process.
pub struct ProcessHandle {
    pub(crate) id: GuestId,
    exit_receiver: Arc<RwLock<Option<oneshot::Receiver<GuestExitStatus>>>>,
}

impl ProcessHandle {
    pub(crate) fn new(id: GuestId, exit_receiver: oneshot::Receiver<GuestExitStatus>) -> Self {
        Self {
            id,
            exit_receiver: Arc::new(RwLock::new(Some(exit_receiver))),
        }
    }

    /// Get the process ID.
    pub fn id(&self) -> GuestId {
        self.id.clone()
    }

    /// Wait for the process to exit and get its status.
    pub async fn wait(&mut self) -> GuestExitStatus {
        let mut receiver_guard = self.exit_receiver.write();
        if let Some(receiver) = receiver_guard.take() {
            receiver.await.unwrap_or(GuestExitStatus::Error("Channel closed".to_string()))
        } else {
            GuestExitStatus::Error("Already waited".to_string())
        }
    }
}

/// Unique identifier for a process.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ProcessId(pub u64);

impl ProcessId {
    pub fn new(id: u64) -> Self {
        Self(id)
    }
}
