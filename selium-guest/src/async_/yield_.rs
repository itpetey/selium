pub use selium_guest_macros::yield_now as yield_now;

#[cfg(target_arch = "wasm32")]
mod wasm_yield {
    #[link(wasm_import_module = "selium::async")]
    unsafe extern "C" {
        pub fn yield_now();
    }
    
    pub unsafe fn do_yield() {
        unsafe { yield_now() }
    }
}

#[cfg(not(target_arch = "wasm32"))]
mod host_yield {
    pub fn do_yield() {
        // On host, yield is a no-op (future will be polled)
    }
}

/// Cooperatively yield once so other guest tasks can make progress.
pub async fn yield_now() {
    struct YieldNow {
        yielded: bool,
    }

    impl Future for YieldNow {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            if self.yielded {
                Poll::Ready(())
            } else {
                self.yielded = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }

    #[cfg(target_arch = "wasm32")]
    {
        // On WASM, we also call the host yield
        unsafe { wasm_yield::do_yield() }
    }
    
    YieldNow { yielded: false }.await;
}
