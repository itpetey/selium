use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_yield_now_completes() {
        yield_now().await;
    }

    #[tokio::test]
    async fn test_yield_now_allows_progress() {
        let mut counter = 0;
        for _ in 0..10 {
            yield_now().await;
            counter += 1;
        }
        assert_eq!(counter, 10);
    }
}

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
    #[allow(dead_code)]
    pub fn do_yield() {}
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
        unsafe { wasm_yield::do_yield() }
    }

    YieldNow { yielded: false }.await;
}
