use std::{
    sync::Arc,
    sync::atomic::{AtomicU64, Ordering},
};

use selium_abi::{SharedResourceId, SignalDescriptor};
use tokio::{
    sync::Notify,
    time::{Duration, timeout},
};

use crate::{
    Error, Result,
    state::{Kernel, SignalState},
};

impl Kernel {
    pub fn create_signal(&self) -> SignalDescriptor {
        let local_id = self.next_local_id();
        let shared_id = self.next_shared_id();
        self.inner.signals_by_shared.lock().insert(
            shared_id,
            Arc::new(SignalState {
                generation: AtomicU64::new(0),
                notify: Notify::new(),
            }),
        );
        self.inner.local_signals.lock().insert(local_id, shared_id);
        SignalDescriptor {
            local_id,
            shared_id,
        }
    }

    pub fn attach_signal(&self, shared_id: SharedResourceId) -> Result<SignalDescriptor> {
        let signals_by_shared = self.inner.signals_by_shared.lock();
        if !signals_by_shared.contains_key(&shared_id) {
            return Err(Error::NotFound(format!("signal {shared_id}")));
        }
        let local_id = self.next_local_id();
        let mut local_signals = self.inner.local_signals.lock();
        local_signals.insert(local_id, shared_id);
        Ok(SignalDescriptor {
            local_id,
            shared_id,
        })
    }

    pub fn notify_signal(&self, local_id: u64) -> Result<u64> {
        let state = self.signal_state(local_id)?;
        let generation = state.generation.fetch_add(1, Ordering::SeqCst) + 1;
        state.notify.notify_waiters();
        Ok(generation)
    }

    pub async fn wait_signal(
        &self,
        local_id: u64,
        observed_generation: u64,
        timeout_ms: u64,
    ) -> Result<u64> {
        let state = self.signal_state(local_id)?;
        let notified = state.notify.notified();
        let current_generation = state.generation.load(Ordering::SeqCst);
        if current_generation > observed_generation {
            return Ok(current_generation);
        }
        timeout(Duration::from_millis(timeout_ms), notified)
            .await
            .map_err(|_| Error::Timeout)?;
        Ok(state.generation.load(Ordering::SeqCst))
    }

    pub fn close_signal(&self, local_id: u64) -> Result<()> {
        let mut signals_by_shared = self.inner.signals_by_shared.lock();
        let mut local_signals = self.inner.local_signals.lock();
        let shared_id = local_signals
            .remove(&local_id)
            .ok_or_else(|| Error::NotFound(format!("signal {local_id}")))?;
        if !local_signals.values().any(|id| *id == shared_id) {
            signals_by_shared.remove(&shared_id);
        }
        Ok(())
    }

    pub fn signal_shared_id(&self, local_id: u64) -> Result<SharedResourceId> {
        let shared_id = self
            .inner
            .local_signals
            .lock()
            .get(&local_id)
            .copied()
            .ok_or_else(|| Error::NotFound(format!("signal {local_id}")))?;
        Ok(shared_id)
    }

    pub fn signal_handle_count(&self, shared_id: SharedResourceId) -> usize {
        self.inner
            .local_signals
            .lock()
            .values()
            .filter(|id| **id == shared_id)
            .count()
    }

    pub fn signal_generation(&self, local_id: u64) -> Result<u64> {
        Ok(self
            .signal_state(local_id)?
            .generation
            .load(Ordering::SeqCst))
    }

    pub(crate) fn signal_state(&self, local_id: u64) -> Result<Arc<SignalState>> {
        let shared_id = self
            .inner
            .local_signals
            .lock()
            .get(&local_id)
            .copied()
            .ok_or_else(|| Error::NotFound(format!("signal {local_id}")))?;
        self.inner
            .signals_by_shared
            .lock()
            .get(&shared_id)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("signal {shared_id}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn signal_wait_resumes_after_notify() {
        let kernel = Kernel::default();
        let signal = kernel.create_signal();
        let waiter = {
            let kernel = kernel.clone();
            tokio::spawn(async move { kernel.wait_signal(signal.local_id, 0, 1_000).await })
        };

        kernel
            .notify_signal(signal.local_id)
            .expect("notify signal");
        let generation = waiter.await.expect("join waiter").expect("wait result");
        assert_eq!(generation, 1);
    }
}
