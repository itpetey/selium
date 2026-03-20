//! Async runtime for guests.
//!
//! Provides cooperative multitasking within a single guest instance.
//!
//! Key concepts:
//! - `spawn()` adds futures to a background task queue
//! - `yield_now()` cooperatively yields to allow other tasks to run
//! - `wait()` blocks until the host enqueues a wake signal
//! - `shutdown()` blocks until the host signals shutdown
//! - `FutureSharedState` bridges host async operations to guest futures

pub mod background;
pub mod future;
pub mod yield_;
pub mod shutdown;
pub mod wait;

pub use background::{spawn, JoinHandle, block_on};
pub use future::{FutureSharedState, SharedFuture};
pub use yield_::yield_now;
pub use shutdown::shutdown;
pub use wait::wait;

use std::task::Context;

/// Trait for types that can be used as task identifiers.
pub trait TaskId: Copy + Eq + std::hash::Hash + 'static {
    fn into_usize(self) -> usize;
    fn from_usize(id: usize) -> Self;
}

impl TaskId for usize {
    fn into_usize(self) -> usize { self }
    fn from_usize(id: usize) -> Self { id }
}

impl TaskId for u64 {
    fn into_usize(self) -> usize { self as usize }
    fn from_usize(id: usize) -> Self { id as u64 }
}

/// Register the current waker and return a task identifier.
#[cfg(target_arch = "wasm32")]
pub fn register_waker(cx: &mut Context<'_>) -> usize {
    let waker = cx.waker().clone();
    let boxed = Box::new(waker);
    Box::into_raw(boxed) as usize
}

#[cfg(not(target_arch = "wasm32"))]
mod native_waker_registry {
    use std::collections::HashMap;
    use std::sync::{Mutex, OnceLock};
    use std::task::Waker;

    pub struct Registry {
        pub next: usize,
        pub wakers: HashMap<usize, Waker>,
    }

    impl Default for Registry {
        fn default() -> Self {
            Self::new()
        }
    }

    impl Registry {
        pub fn new() -> Self {
            Self { next: 1, wakers: HashMap::new() }
        }
    }

    pub fn registry() -> &'static Mutex<Registry> {
        static REGISTRY: OnceLock<Mutex<Registry>> = OnceLock::new();
        REGISTRY.get_or_init(|| Mutex::new(Registry::new()))
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub use native_waker_registry::{registry, Registry};

/// Register the current waker (non-WASM implementation for testing).
#[cfg(not(target_arch = "wasm32"))]
pub fn register_waker(cx: &mut Context<'_>) -> usize {
    use crate::async_::native_waker_registry::registry;
    
    let mut guard = registry().lock().unwrap();
    let id = guard.next;
    guard.next += 1;
    guard.wakers.insert(id, cx.waker().clone());
    id
}

/// Wake a registered task by ID.
#[cfg(target_arch = "wasm32")]
pub fn wake_task(id: usize) {
    let waker = unsafe { Box::from_raw(id as *mut Waker) };
    waker.wake();
}

/// Wake a registered task by ID (non-WASM implementation).
#[cfg(not(target_arch = "wasm32"))]
pub fn wake_task(id: usize) {
    use crate::async_::native_waker_registry::registry;
    
    if let Ok(mut guard) = registry().lock() {
        if let Some(waker) = guard.wakers.remove(&id) {
            waker.wake();
        }
    }
}
