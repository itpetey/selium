use std::sync::Arc;

use selium_kernel::{
    KernelError,
    registry::InstanceRegistry,
    spi::wake_mailbox::WakeMailbox,
};
use tokio::{select, sync::Notify};
use wasmtime::{Caller, Linker};

/// Host-side support for guest async helpers.
pub struct GuestAsync {
    shutdown: Arc<Notify>,
}

impl GuestAsync {
    /// Create a new guest async capability.
    pub fn new(notify: Arc<Notify>) -> Self {
        Self { shutdown: notify }
    }

    /// Link the `selium::async` host functions into the Wasmtime linker.
    pub fn link(&self, linker: &mut Linker<InstanceRegistry>) -> Result<(), KernelError> {
        let shutdown = Arc::clone(&self.shutdown);
        linker
            .func_wrap_async(
                "selium::async",
                "yield_now",
                move |caller: Caller<'_, InstanceRegistry>, ()| {
                    let Some(mailbox) = caller.data().mailbox() else {
                        return Box::new(async {});
                    };
                    let shutdown = Arc::clone(&shutdown);
                    Box::new(async move {
                        loop {
                            if mailbox.is_closed() || mailbox.is_signalled() {
                                break;
                            }
                            select! {
                                _ = shutdown.notified() => {
                                    break;
                                }
                                _ = mailbox.wait_for_signal() => {}
                            }
                        }
                    })
                },
            )
            .map_err(|err| KernelError::Engine(err.to_string()))?;
        let shutdown = Arc::clone(&self.shutdown);
        linker
            .func_wrap_async(
                "selium::async",
                "wait_for_shutdown",
                move |caller: Caller<'_, InstanceRegistry>, ()| {
                    Box::new(shutdown_wait(Arc::clone(&shutdown), caller.data().mailbox()))
                },
            )
            .map_err(|err| KernelError::Engine(err.to_string()))?;
        Ok(())
    }
}

async fn shutdown_wait(
    shutdown: Arc<Notify>,
    mailbox: Option<&'static dyn WakeMailbox>,
) {
    let Some(mailbox) = mailbox else {
        shutdown.notified().await;
        return;
    };

    loop {
        if mailbox.is_closed() {
            break;
        }
        select! {
            _ = shutdown.notified() => {
                break;
            }
            _ = mailbox.wait_for_signal() => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{pin::Pin, sync::atomic::{AtomicBool, Ordering}, task::Waker, time::Duration};

    struct TestMailbox {
        closed: AtomicBool,
        notify: Notify,
    }

    impl TestMailbox {
        fn new() -> Self {
            Self {
                closed: AtomicBool::new(false),
                notify: Notify::new(),
            }
        }
    }

    impl WakeMailbox for TestMailbox {
        fn refresh_base(&self, _base: usize) {}

        fn close(&self) {
            self.closed.store(true, Ordering::Release);
            self.notify.notify_waiters();
        }

        fn waker(&'static self, _task_id: usize) -> Waker {
            Waker::noop().clone()
        }

        fn is_closed(&self) -> bool {
            self.closed.load(Ordering::Acquire)
        }

        fn is_signalled(&self) -> bool {
            false
        }

        fn wait_for_signal<'a>(&'a self) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
            Box::pin(async move {
                self.notify.notified().await;
            })
        }
    }

    #[test]
    fn link_registers_guest_async_module() {
        let engine = wasmtime::Engine::default();
        let mut linker = Linker::<InstanceRegistry>::new(&engine);
        let notify = Arc::new(Notify::new());
        let guest_async = GuestAsync::new(notify);

        guest_async.link(&mut linker).expect("link guest async");
    }

    #[tokio::test]
    async fn shutdown_wait_future_resolves_on_runtime_shutdown() {
        let shutdown = Arc::new(Notify::new());
        let task = tokio::spawn(shutdown_wait(Arc::clone(&shutdown), None));

        tokio::task::yield_now().await;
        shutdown.notify_waiters();

        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("shutdown wait should complete")
            .expect("join task");
    }

    #[tokio::test]
    async fn shutdown_wait_future_resolves_when_mailbox_closes() {
        let shutdown = Arc::new(Notify::new());
        let mailbox: &'static TestMailbox = Box::leak(Box::new(TestMailbox::new()));
        let task = tokio::spawn(shutdown_wait(
            Arc::clone(&shutdown),
            Some(mailbox as &'static dyn WakeMailbox),
        ));

        tokio::task::yield_now().await;
        mailbox.close();

        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("mailbox close should complete wait")
            .expect("join task");
    }
}
