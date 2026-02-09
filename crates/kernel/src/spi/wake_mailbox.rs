//! Engine-neutral mailbox interface for guest task wake-ups.

use std::task::Waker;
use std::{future::Future, pin::Pin};

/// Mailbox abstraction used by the registry to drive guest async wake-ups.
pub trait WakeMailbox: Send + Sync {
    /// Refresh the underlying guest memory base pointer after growth.
    fn refresh_base(&self, base: usize);

    /// Close the mailbox and wake waiting host tasks.
    fn close(&self);

    /// Build a waker for the given guest task id.
    fn waker(&'static self, task_id: usize) -> Waker;

    /// Return whether the mailbox has been closed.
    fn is_closed(&self) -> bool;

    /// Return whether the mailbox currently has a signal pending.
    fn is_signalled(&self) -> bool;

    /// Wait until the mailbox receives a new signal.
    fn wait_for_signal<'a>(&'a self) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
}
