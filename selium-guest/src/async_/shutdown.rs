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

    fn signal(&mut self) {
        self.signalled = true;
        for waker in self.wakers.drain(..) {
            waker.wake();
        }
    }
}

/// Future that resolves when the host signals shutdown.
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

/// Wait until the runtime begins shutting the current guest down.
///
/// This is typically called at the end of a service's main loop
/// to gracefully shut down when the host requests it.
#[cfg(target_arch = "wasm32")]
pub async fn shutdown() {
    extern "C" {
        #[link_name = "wait_for_shutdown"]
        fn raw_wait_for_shutdown();
    }
    
    unsafe { raw_wait_for_shutdown() }
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn shutdown() {
    ShutdownFuture.await;
}

/// Signal shutdown (for testing).
#[cfg(test)]
pub fn __signal_shutdown_for_tests() {
    SHUTDOWN.with(|state| {
        let mut state = state.borrow_mut();
        state.signal();
    });
}
