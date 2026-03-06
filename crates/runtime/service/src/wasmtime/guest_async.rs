use std::sync::Arc;

use selium_kernel::{KernelError, registry::InstanceRegistry};
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
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn link_registers_guest_async_module() {
        let mut config = wasmtime::Config::new();
        config.async_support(true);
        let engine = wasmtime::Engine::new(&config).expect("engine");
        let mut linker = Linker::<InstanceRegistry>::new(&engine);
        let notify = Arc::new(Notify::new());
        let guest_async = GuestAsync::new(notify);

        guest_async.link(&mut linker).expect("link guest async");
    }
}
