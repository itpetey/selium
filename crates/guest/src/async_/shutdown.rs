use core::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

thread_local! {
    static SHUTDOWN: RefCell<ShutdownState> = const { RefCell::new(ShutdownState::new()) };
}

struct ShutdownState {
    signalled: bool,
    wakers: Vec<std::task::Waker>,
}

impl ShutdownState {
    const fn new() -> Self {
        Self {
            signalled: false,
            wakers: Vec::new(),
        }
    }

    fn register(&mut self, waker: &std::task::Waker) {
        if self.wakers.iter().any(|existing| existing.will_wake(waker)) {
            return;
        }
        self.wakers.push(waker.clone());
    }

    #[allow(dead_code)]
    fn signal(&mut self) {
        self.signalled = true;
        for waker in self.wakers.drain(..) {
            waker.wake();
        }
    }
}

struct ShutdownFuture;

impl Future for ShutdownFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        SHUTDOWN.with(|state| {
            let mut state = state.borrow_mut();
            if state.signalled {
                Poll::Ready(())
            } else {
                state.register(cx.waker());
                Poll::Pending
            }
        })
    }
}

#[cfg(target_arch = "wasm32")]
mod wasm_shutdown {
    #[link(wasm_import_module = "selium::async")]
    extern "C" {
        pub fn wait_for_shutdown();
    }
}

#[cfg(target_arch = "wasm32")]
pub async fn shutdown() {
    unsafe { wasm_shutdown::wait_for_shutdown() }
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn shutdown() {
    ShutdownFuture.await;
}

#[cfg(test)]
pub fn __signal_shutdown_for_tests() {
    SHUTDOWN.with(|state| {
        let mut state = state.borrow_mut();
        state.signal();
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::task::{RawWaker, RawWakerVTable};

    #[test]
    fn test_shutdown_state_new() {
        let state = ShutdownState::new();
        assert!(!state.signalled);
        assert!(state.wakers.is_empty());
    }

    #[test]
    fn test_shutdown_state_register() {
        fn clone(_: *const ()) -> RawWaker {
            RawWaker::new(std::ptr::null(), &VTABLE)
        }
        fn wake(_: *const ()) {}
        fn wake_by_ref(_: *const ()) {}
        fn drop(_: *const ()) {}

        static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop);

        let state = &mut ShutdownState::new();
        let raw = RawWaker::new(std::ptr::null(), &VTABLE);
        let waker = unsafe { std::task::Waker::from_raw(raw) };

        state.register(&waker);
        assert_eq!(state.wakers.len(), 1);

        state.register(&waker);
        assert_eq!(state.wakers.len(), 1);
    }

    #[test]
    fn test_shutdown_state_signal() {
        let mut state = ShutdownState::new();

        state.signal();
        assert!(state.signalled);
    }
}
