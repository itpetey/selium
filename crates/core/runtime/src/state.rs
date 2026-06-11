use std::{
    collections::{BTreeSet, HashMap, VecDeque},
    sync::Arc,
    time::Instant,
};

use parking_lot::Mutex;
use selium_abi::{
    AbiError, DiscoveryRequest, HostcallOutput, OperationId, ProcessId, ResourceClass, TaskId,
};
use selium_kernel::Kernel;
use wasmtiny::{WasmApplication, WasmValue};

use crate::{config::ProcessAuthority, error::Result, mailbox::GuestMailbox};

pub(crate) type LocalHandleOwners = HashMap<(ResourceClass, u64), BTreeSet<ProcessId>>;
pub(crate) type SharedResourceOwners = HashMap<(ResourceClass, u64), BTreeSet<ProcessId>>;
/// URIs registered in discovery per process (for revocation on termination).
pub(crate) type ProcessDiscoveryUris = HashMap<ProcessId, Vec<String>>;
/// Queue of pending discovery operations to be flushed asynchronously.
pub(crate) type PendingDiscoveryOps = VecDeque<DiscoveryRequest>;

/// Runtime coordinating guest execution, hostcalls, and kernel resources.
#[derive(Clone)]
pub struct Runtime {
    pub(crate) kernel: Kernel,
    pub(crate) process_authorities: Arc<Mutex<HashMap<ProcessId, ProcessAuthority>>>,
    pub(crate) loaded_guests: Arc<Mutex<HashMap<ProcessId, LoadedGuest>>>,
    pub(crate) local_handle_owners: Arc<Mutex<LocalHandleOwners>>,
    pub(crate) shared_resource_owners: Arc<Mutex<SharedResourceOwners>>,
    pub(crate) module_registry: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    pub(crate) next_operation_id: Arc<Mutex<OperationId>>,
    pub(crate) operations: Arc<Mutex<HashMap<OperationId, HostOperation>>>,
    pub(crate) mailboxes: Arc<Mutex<HashMap<ProcessId, Arc<GuestMailbox>>>>,
    /// URIs registered in discovery per process, for revocation on termination.
    pub(crate) process_discovery_uris: Arc<Mutex<ProcessDiscoveryUris>>,
    /// Pending discovery operations to be flushed asynchronously.
    pub(crate) pending_discovery_ops: Arc<Mutex<PendingDiscoveryOps>>,
}

pub(crate) struct LoadedGuest {
    pub(crate) app: WasmApplication,
    pub(crate) module_index: u32,
    pub(crate) entrypoint_results: Vec<WasmValue>,
}

#[derive(Debug, Clone)]
pub(crate) enum HostOperationState {
    Ready(HostcallOutput),
    Failed(AbiError),
    HostQueueRecvWait { local_id: u64, deadline: Instant },
    SleepWait { deadline: Instant },
}

#[derive(Debug, Clone)]
pub(crate) struct HostOperation {
    pub(crate) process_id: ProcessId,
    pub(crate) task_id: Option<TaskId>,
    pub(crate) state: HostOperationState,
}

impl Runtime {
    /// Creates a runtime backed by the supplied kernel.
    pub fn new(kernel: Kernel) -> Self {
        Self {
            kernel,
            process_authorities: Arc::new(Mutex::new(HashMap::new())),
            loaded_guests: Arc::new(Mutex::new(HashMap::new())),
            local_handle_owners: Arc::new(Mutex::new(HashMap::new())),
            shared_resource_owners: Arc::new(Mutex::new(HashMap::new())),
            module_registry: Arc::new(Mutex::new(HashMap::new())),
            next_operation_id: Arc::new(Mutex::new(1)),
            operations: Arc::new(Mutex::new(HashMap::new())),
            mailboxes: Arc::new(Mutex::new(HashMap::new())),
            process_discovery_uris: Arc::new(Mutex::new(HashMap::new())),
            pending_discovery_ops: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    /// Returns a clone of the runtime kernel handle.
    pub fn kernel(&self) -> Kernel {
        self.kernel.clone()
    }

    /// Returns a reference to the process discovery URIs map.
    ///
    /// Used by integration tests to verify Tier-1 registration tracking.
    pub fn process_discovery_uris(
        &self,
    ) -> parking_lot::MutexGuard<'_, std::collections::HashMap<ProcessId, Vec<String>>> {
        self.process_discovery_uris.lock()
    }

    /// Returns the number of pending discovery operations.
    pub fn pending_discovery_ops_count(&self) -> usize {
        self.pending_discovery_ops.lock().len()
    }

    /// Drains all pending discovery operations from the queue.
    ///
    /// Returns the operations in FIFO order. The caller is responsible for
    /// sending them to the discovery service via an RpcClient.
    pub fn drain_pending_discovery_ops(&self) -> Vec<DiscoveryRequest> {
        let mut ops = self.pending_discovery_ops.lock();
        ops.drain(..).collect()
    }

    /// Flushes pending discovery operations to the discovery service.
    ///
    /// This method drains the pending queue and sends each operation via the
    /// provided RpcClient. Returns the number of operations flushed.
    ///
    /// # Errors
    ///
    /// Returns an error if any RPC request fails. Remaining operations are
    /// re-enqueued at the front of the queue.
    pub async fn flush_discovery_ops(
        &self,
        client: &mut selium_guest::io::rpc::RpcClient<
            DiscoveryRequest,
            selium_abi::DiscoveryResponse,
        >,
    ) -> Result<usize> {
        let ops = self.drain_pending_discovery_ops();
        let count = ops.len();

        for op in ops {
            match client.request(op.clone()).await {
                Ok(_) => {}
                Err(e) => {
                    // Re-enqueue the failed operation and any remaining.
                    tracing::warn!("discovery flush failed: {e}, re-enqueuing");
                    self.pending_discovery_ops.lock().push_front(op);
                    return Err(crate::Error::Host(format!("discovery flush failed: {e}")));
                }
            }
        }

        Ok(count)
    }
}

impl Default for Runtime {
    fn default() -> Self {
        Self::new(Kernel::default())
    }
}
