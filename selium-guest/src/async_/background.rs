//! Background task management.

use core::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use super::TaskId;

/// A background task in the executor.
struct BackgroundTask {
    future: Pin<Box<dyn Future<Output = ()>>>,
}

/// Shared state for JoinHandle.
struct JoinState<T> {
    result: Option<T>,
    waker: Option<Waker>,
}

impl<T> JoinState<T> {
    fn new() -> Self {
        Self { result: None, waker: None }
    }
}

/// Handle returned by spawn.
pub struct JoinHandle<T> {
    state: Rc<RefCell<JoinState<T>>>,
}

impl<T> Future for JoinHandle<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.state.borrow_mut();
        if let Some(value) = state.result.take() {
            Poll::Ready(value)
        } else {
            state.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

impl<T> JoinState<T> {
    fn complete(&mut self, value: T) {
        self.result = Some(value);
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }
}

thread_local! {
    static BACKGROUND: RefCell<Vec<BackgroundTask>> = const { RefCell::new(Vec::new()) };
    static SPAWN_QUEUE: RefCell<Vec<BackgroundTask>> = const { RefCell::new(Vec::new()) };
}

/// Spawn a future as a background task.
pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + 'static,
{
    let state = Rc::new(RefCell::new(JoinState::new()));
    let state_clone = Rc::clone(&state);
    let task = async move {
        let output = future.await;
        state_clone.borrow_mut().complete(output);
    };

    let task = BackgroundTask {
        future: Box::pin(task),
    };

    BACKGROUND.with(|tasks| {
        if let Ok(mut tasks) = tasks.try_borrow_mut() {
            tasks.push(task);
        } else {
            SPAWN_QUEUE.with(|queue| {
                queue.borrow_mut().push(task);
            });
        }
    });

    JoinHandle { state }
}

/// Poll background tasks.
fn poll_backgrounds(cx: &mut Context<'_>) -> bool {
    let mut progress = merge_spawn_queue();

    BACKGROUND.with(|tasks| match tasks.try_borrow_mut() {
        Ok(mut tasks) => {
            let mut i = 0;
            while i < tasks.len() {
                match tasks[i].future.as_mut().poll(cx) {
                    Poll::Ready(()) => {
                        tasks.swap_remove(i);
                        progress = true;
                    }
                    Poll::Pending => i += 1,
                }
            }
        }
        Err(_) => {
            // Borrow conflict, skip
        }
    });

    progress
}

fn merge_spawn_queue() -> bool {
    SPAWN_QUEUE.with(|queue| {
        let mut queued = queue.borrow_mut();
        if queued.is_empty() {
            return false;
        }

        BACKGROUND.with(|tasks| {
            tasks.borrow_mut().extend(queued.drain(..));
        });

        true
    })
}

/// Block on a future to completion using the guest executor.
pub fn block_on<F: Future>(fut: F) -> F::Output {
    use futures::task::waker_ref;
    use futures::ArcWake;

    pin_mut!(fut);
    
    struct LocalWake {
        notified: std::sync::atomic::AtomicBool,
    }

    impl LocalWake {
        fn new() -> Self {
            Self {
                notified: std::sync::atomic::AtomicBool::new(false),
            }
        }
    }

    impl ArcWake for LocalWake {
        fn wake_by_ref(arc_self: &Arc<Self>) {
            arc_self.notified.store(true, std::sync::atomic::Ordering::Release);
        }
    }

    let wake_state = Arc::new(LocalWake::new());
    let waker = waker_ref(&Arc::clone(&wake_state));
    let mut cx = Context::from_waker(&waker);

    loop {
        if let Poll::Ready(val) = fut.as_mut().poll(&mut cx) {
            return val;
        }

        let progressed = poll_backgrounds(&mut cx);
        if progressed || wake_state.notified.swap(false, std::sync::atomic::Ordering::AcqRel) {
            continue;
        }
        
        // Wait for host to enqueue a wake
        super::wait();
    }
}
