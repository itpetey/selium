//! FutureSharedState bridges host async operations to guest futures.
//!
//! When a guest calls an async hostcall:
//! 1. Guest creates a FutureSharedState and stores task_id
//! 2. Host spawns a tokio task that executes the async operation
//! 3. On completion, host enqueues the task_id to the guest's mailbox
//! 4. Guest's executor wakes, polls the future, and retrieves the result

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::{Context, Poll};

/// Shared state between a guest future and a host async operation.
pub struct FutureSharedState<T, E> {
    result: Mutex<Option<std::result::Result<T, E>>>,
    wakers: Mutex<Vec<std::task::Waker>>,
}

impl<T, E> FutureSharedState<T, E> {
    /// Create a new FutureSharedState.
    pub fn new() -> Self {
        Self {
            result: Mutex::new(None),
            wakers: Mutex::new(Vec::new()),
        }
    }

    /// Complete the future with a result.
    /// Called by the host when the async operation finishes.
    pub fn complete(&self, result: std::result::Result<T, E>) {
        *self.result.lock().unwrap() = Some(result);
        for waker in self.wakers.lock().unwrap().drain(..) {
            waker.wake();
        }
    }

    /// Check if the future is complete.
    #[allow(dead_code)]
    pub fn is_complete(&self) -> bool {
        self.result.lock().unwrap().is_some()
    }

    /// Get the result if available, consuming it.
    pub fn take_result(&self) -> Option<std::result::Result<T, E>> {
        self.result.lock().unwrap().take()
    }

    /// Register a waker to be notified when the future completes.
    pub fn register_waker(&self, waker: std::task::Waker) {
        let mut wakers = self.wakers.lock().unwrap();
        if !wakers.iter().any(|w| w.will_wake(&waker)) {
            wakers.push(waker);
        }
    }
}

impl<T, E> Default for FutureSharedState<T, E> {
    fn default() -> Self {
        Self::new()
    }
}

/// A future that waits on FutureSharedState.
pub struct SharedFuture<'a, T, E> {
    state: &'a Arc<FutureSharedState<T, E>>,
}

impl<'a, T, E> SharedFuture<'a, T, E> {
    /// Create a new SharedFuture.
    pub fn new(state: &'a Arc<FutureSharedState<T, E>>) -> Self {
        Self { state }
    }
}

impl<T, E> Future for SharedFuture<'_, T, E> {
    type Output = std::result::Result<T, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(result) = self.state.take_result() {
            Poll::Ready(result)
        } else {
            let mut wakers = self.state.wakers.lock().unwrap();
            if !wakers.iter().any(|w| w.will_wake(cx.waker())) {
                wakers.push(cx.waker().clone());
            }
            Poll::Pending
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_future_shared_state_new() {
        let state = FutureSharedState::<i32, String>::new();
        assert!(!state.is_complete());
    }

    #[test]
    fn test_future_shared_state_complete() {
        let state = Arc::new(FutureSharedState::<i32, String>::new());
        state.complete(Ok(42));
        assert!(state.is_complete());
        assert_eq!(state.take_result(), Some(Ok(42)));
    }

    #[test]
    fn test_future_shared_state_error() {
        let state = Arc::new(FutureSharedState::<i32, String>::new());
        state.complete(Err("failed".to_string()));
        assert!(state.is_complete());
        assert_eq!(state.take_result(), Some(Err("failed".to_string())));
    }

    #[tokio::test]
    async fn test_shared_future_completes() {
        let state = Arc::new(FutureSharedState::<i32, String>::new());
        let mut future = Box::pin(SharedFuture::new(&state));

        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        state.complete(Ok(100));

        assert!(Pin::new(&mut future).poll(&mut cx).is_ready());
    }
}
