//! Async runtime for guests.
//!
//! Provides cooperative multitasking within a single guest instance.
//!
//! Key concepts:
//! - `spawn()` adds futures to a background task queue
//! - `yield_now()` cooperatively yields to allow other tasks to run
//! - `wait()` blocks until the host enqueues a wake signal
//! - `shutdown()` blocks until the host signals shutdown

use core::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

mod background;
mod yield_;
mod shutdown;
mod wait;

pub use background::{spawn, JoinHandle, block_on};
pub use yield_::yield_now;
pub use shutdown::shutdown;
pub use wait::wait;

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

/// Register the current waker (non-WASM implementation for testing).
#[cfg(not(target_arch = "wasm32"))]
pub fn register_waker(cx: &mut Context<'_>) -> usize {
    use std::collections::HashMap;
    use std::sync::{Mutex, OnceLock};
    
    struct Registry {
        next: usize,
        wakers: HashMap<usize, Waker>,
    }
    
    impl Registry {
        fn new() -> Self {
            Self { next: 1, wakers: HashMap::new() }
        }
    }
    
    fn registry() -> &'static Mutex<Registry> {
        static REGISTRY: OnceLock<Mutex<Registry>> = OnceLock::new();
        REGISTRY.get_or_init(|| Mutex::new(Registry::new()))
    }
    
    let mut guard = registry().lock().unwrap();
    let id = guard.next;
    guard.next += 1;
    guard.wakers.insert(id, cx.waker().clone());
    id
}

/// Wake a registered task by ID.
pub fn wake_task(id: usize) {
    #[cfg(target_arch = "wasm32")]
    {
        let waker = unsafe { Box::from_raw(id as *mut Waker) };
        waker.wake();
    }
    
    #[cfg(not(target_arch = "wasm32"))]
    {
        use std::collections::HashMap;
        use std::sync::{Mutex, OnceLock};
        
        fn registry() -> &'static Mutex<HashMap<usize, Waker>> {
            static REGISTRY: OnceLock<Mutex<HashMap<usize, Waker>>> = OnceLock::new();
            REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
        }
        
        if let Ok(mut guard) = registry().lock() {
            if let Some(waker) = guard.remove(&id) {
                waker.wake();
            }
        }
    }
}
