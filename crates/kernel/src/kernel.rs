use std::{
    io,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use parking_lot::Mutex;

use crate::{
    host_queue::HostQueueRegistry, memory::MemoryRegistry, network::NetworkState, poller::Poller,
    process::ProcessTable, quota::QuotaTable, volume::VolumeRegistry,
};

#[derive(Clone)]
pub struct Kernel {
    pub(crate) inner: Arc<KernelInner>,
}

pub(crate) struct KernelInner {
    pub(crate) memory: MemoryRegistry,
    pub(crate) processes: ProcessTable,
    pub(crate) volumes: VolumeRegistry,
    pub(crate) network: NetworkState,
    pub(crate) queues: HostQueueRegistry,
    pub(crate) quota: QuotaTable,
    pub(crate) poller: Mutex<Option<Poller>>,
}

impl Kernel {
    /// Creates a kernel whose durable volume layer is rooted at `root`.
    pub fn with_state_dir(seed: u64, root: impl Into<std::path::PathBuf>) -> Self {
        Self {
            inner: Arc::new(KernelInner {
                memory: MemoryRegistry::new(seed),
                processes: ProcessTable::new(seed),
                volumes: VolumeRegistry::new(root),
                network: NetworkState::new(),
                queues: HostQueueRegistry::new(),
                quota: QuotaTable::new(),
                poller: Mutex::new(None),
            }),
        }
    }

    /// Creates a kernel with a throwaway state directory.
    ///
    /// The durable volume layer still lives on the filesystem (the host has no
    /// in-memory storage mode), but the directory is removed when the last
    /// kernel handle drops. Boot with [`Kernel::with_state_dir`] to persist.
    pub fn with_seed(seed: u64) -> Self {
        Self {
            inner: Arc::new(KernelInner {
                memory: MemoryRegistry::new(seed),
                processes: ProcessTable::new(seed),
                volumes: VolumeRegistry::ephemeral(),
                network: NetworkState::new(),
                queues: HostQueueRegistry::new(),
                quota: QuotaTable::new(),
                poller: Mutex::new(None),
            }),
        }
    }

    /// Initialises the mio event poller if not already initialised and
    /// returns it. Idempotent: repeated calls return the existing poller
    /// rather than creating (and orphaning) another one.
    pub fn init_poller(&self) -> io::Result<Poller> {
        let mut guard = self.inner.poller.lock();
        if let Some(existing) = guard.as_ref() {
            return Ok(existing.clone());
        }
        let poller = Poller::new()?;
        *guard = Some(poller.clone());
        Ok(poller)
    }

    /// Returns the poller if initialised.
    pub fn poller(&self) -> Option<Poller> {
        self.inner.poller.lock().clone()
    }

    pub fn memory(&self) -> MemoryRegistry {
        self.inner.memory.clone()
    }
    pub fn processes(&self) -> ProcessTable {
        self.inner.processes.clone()
    }
    pub fn volumes(&self) -> VolumeRegistry {
        self.inner.volumes.clone()
    }
    pub fn network(&self) -> NetworkState {
        self.inner.network.clone()
    }
    pub fn queues(&self) -> HostQueueRegistry {
        self.inner.queues.clone()
    }
    pub fn quota(&self) -> QuotaTable {
        self.inner.quota.clone()
    }
}

impl Default for Kernel {
    fn default() -> Self {
        Self::with_seed(random_seed())
    }
}

pub(crate) fn hashed_id(seed: u64, counter: u64) -> u64 {
    let mut z = seed.wrapping_add(counter);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

fn random_seed() -> u64 {
    let time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    let pid = std::process::id() as u64;
    time ^ pid.rotate_left(17)
}
