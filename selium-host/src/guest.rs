//! Guest instance management.
//!
//! Each guest is identified by a unique GuestId and has:
//! - A wasmtime instance
//! - Granted capability handles
//! - A mailbox for host → guest communication
//! - Usage metering

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use wasmtime::{Engine, Instance, Memory, Module, Store};

use crate::{error::Result, GuestExitStatus};

/// Unique identifier for a guest instance.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GuestId(pub u64);

impl GuestId {
    pub fn new(id: u64) -> Self {
        Self(id)
    }
}

/// Generate the next unique guest ID.
pub fn next_guest_id() -> GuestId {
    static NEXT_ID: AtomicU64 = AtomicU64::new(0);
    GuestId::new(NEXT_ID.fetch_add(1, Ordering::Relaxed))
}

/// A handle to a spawned guest process.
pub struct Guest {
    id: GuestId,
    instance: Option<Instance>,
    memory: Memory,
    store: Store<()>,
    module: Module,
    handles: HashMap<String, u32>,
    exit_status: Arc<RwLock<Option<GuestExitStatus>>>,
}

impl Guest {
    /// Spawn a new guest from a WASM module.
    ///
    /// This is a placeholder - full implementation will link capabilities
    /// and instantiate the module properly.
    pub fn spawn(engine: &Engine, module: &Module, id: GuestId) -> Result<Self> {
        let mut store = Store::new(engine, ());

        // Allocate memory for the guest
        let memory = Memory::new(&mut store, wasmtime::MemoryType::new(1, None))
            .map_err(|e| crate::Error::Wasm(e.to_string()))?;

        // TODO: Link capabilities and instantiate module
        // For now, we'll skip actual instantiation
        let instance = None;

        Ok(Self {
            id,
            instance,
            memory,
            store,
            module: module.clone(),
            handles: HashMap::new(),
            exit_status: Arc::new(RwLock::new(None)),
        })
    }

    /// Get the guest's unique identifier.
    pub fn id(&self) -> GuestId {
        self.id.clone()
    }

    /// Get the guest's linear memory.
    pub fn memory(&self) -> &Memory {
        &self.memory
    }

    /// Signal the guest to shut down.
    pub fn signal_shutdown(&self) {
        // TODO: Signal the guest's shutdown future
    }

    /// Set the guest's exit status.
    pub fn set_exit_status(&self, status: GuestExitStatus) {
        let mut exit = self.exit_status.write();
        *exit = Some(status);
    }

    /// Get the guest's exit status.
    pub fn exit_status(&self) -> Option<GuestExitStatus> {
        let exit = self.exit_status.read();
        exit.clone()
    }

    /// Poll the guest's executor.
    pub fn poll(&mut self) {
        // TODO: Poll the guest's async executor
    }
}
