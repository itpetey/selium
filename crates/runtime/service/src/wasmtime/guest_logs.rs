use std::sync::Arc;

use selium_abi::{
    GuestLogStream, ProcessLogBindings, QueueAttach, QueueCommit, QueueReserve, QueueRole,
    QueueStatusCode, ShmAlloc,
};
use selium_kernel::{
    KernelError,
    registry::{InstanceRegistry, Registry, ResourceHandle, ResourceType},
    services::{
        queue_service::{QueueService, QueueState},
        shared_memory_service::SharedMemoryDriver,
    },
    spi::{queue::QueueCapability, shared_memory::SharedMemoryCapability},
};
use tracing::warn;
use wasmtime::{Caller, Linker};

use super::guest_data::{GuestInt, GuestUint, read_guest_bytes};

const GUEST_LOG_QUEUE_TIMEOUT_MS: u32 = 5_000;

pub(crate) struct GuestLogState {
    stdout: ManagedGuestLogStream,
    stderr: ManagedGuestLogStream,
}

struct ManagedGuestLogStream {
    queue_shared_id: Option<u64>,
}

impl GuestLogState {
    pub(crate) fn new(bindings: ProcessLogBindings) -> Self {
        Self {
            stdout: ManagedGuestLogStream::new(bindings.stdout_queue_shared_id),
            stderr: ManagedGuestLogStream::new(bindings.stderr_queue_shared_id),
        }
    }

    pub(crate) async fn write(
        &self,
        registry: &Arc<Registry>,
        shared_memory: &SharedMemoryDriver,
        stream: GuestLogStream,
        payload: Vec<u8>,
    ) -> Result<(), KernelError> {
        self.stream(stream)
            .write(registry, shared_memory, payload)
            .await
    }

    pub(crate) async fn close_all(
        &self,
        _registry: &Arc<Registry>,
        _shared_memory: &SharedMemoryDriver,
    ) -> Result<(), KernelError> {
        Ok(())
    }

    fn stream(&self, stream: GuestLogStream) -> &ManagedGuestLogStream {
        match stream {
            GuestLogStream::Stdout => &self.stdout,
            GuestLogStream::Stderr => &self.stderr,
        }
    }
}

impl ManagedGuestLogStream {
    fn new(queue_shared_id: Option<u64>) -> Self {
        Self { queue_shared_id }
    }

    async fn write(
        &self,
        registry: &Arc<Registry>,
        shared_memory: &SharedMemoryDriver,
        payload: Vec<u8>,
    ) -> Result<(), KernelError> {
        let Some(queue_shared_id) = self.queue_shared_id else {
            return Ok(());
        };

        enqueue_log_payload(registry, shared_memory, queue_shared_id, &payload).await?;
        Ok(())
    }
}

pub(crate) fn link_guest_logs(
    linker: &mut Linker<InstanceRegistry>,
    shared_memory: Arc<SharedMemoryDriver>,
    state: Arc<GuestLogState>,
) -> Result<(), wasmtime::Error> {
    linker.func_wrap_async(
        "selium::log",
        "write",
        move |mut caller: Caller<'_, InstanceRegistry>,
              (stream_raw, ptr, len): (GuestUint, GuestInt, GuestUint)| {
            let state = Arc::clone(&state);
            let shared_memory = Arc::clone(&shared_memory);
            Box::new(async move {
                let Some(stream) = GuestLogStream::from_raw(stream_raw) else {
                    warn!(
                        stream_raw,
                        "guest attempted to write to an unknown log stream"
                    );
                    return;
                };
                let registry = caller.data().registry_arc();
                let payload = match read_guest_bytes(&mut caller, ptr, len) {
                    Ok(payload) => payload,
                    Err(err) => {
                        warn!(?stream, "failed to read guest log payload: {err}");
                        return;
                    }
                };

                if let Err(err) = state
                    .write(&registry, shared_memory.as_ref(), stream, payload)
                    .await
                {
                    warn!(?stream, "failed to forward guest log: {err}");
                }
            })
        },
    )?;
    Ok(())
}

async fn enqueue_log_payload(
    registry: &Arc<Registry>,
    shared_memory: &SharedMemoryDriver,
    queue_shared_id: u64,
    payload: &[u8],
) -> Result<(), KernelError> {
    let payload_len = u32::try_from(payload.len()).map_err(KernelError::IntConvert)?;
    let queue_resource_id = registry.resolve_shared(queue_shared_id).ok_or_else(|| {
        KernelError::Driver(format!("queue shared id {queue_shared_id} not found"))
    })?;
    let queue = registry
        .with(
            ResourceHandle::<QueueState>::new(queue_resource_id),
            |queue: &mut QueueState| queue.clone(),
        )
        .ok_or_else(|| {
            KernelError::Driver(format!("queue resource {queue_resource_id} missing"))
        })?;
    let writer = QueueService
        .attach(
            &queue,
            QueueAttach {
                shared_id: queue_shared_id,
                role: QueueRole::Writer { writer_id: 1 },
            },
        )
        .map_err(|err| KernelError::Driver(err.to_string()))?;
    let reserved = QueueService
        .reserve(
            &writer,
            QueueReserve {
                endpoint_id: 0,
                len: payload_len,
                timeout_ms: GUEST_LOG_QUEUE_TIMEOUT_MS,
            },
        )
        .await
        .map_err(|err| KernelError::Driver(err.to_string()))?;
    if reserved.code != QueueStatusCode::Ok {
        return Err(KernelError::Driver(format!(
            "failed to reserve queue frame for guest log: {:?}",
            reserved.code
        )));
    }
    let reservation = reserved
        .reservation
        .ok_or_else(|| KernelError::Driver("queue reserve omitted reservation".to_string()))?;
    let region = shared_memory
        .alloc(ShmAlloc {
            size: payload_len,
            align: 8,
        })
        .map_err(|err| KernelError::Driver(err.to_string()))?;
    shared_memory
        .write(region, 0, payload)
        .map_err(|err| KernelError::Driver(err.to_string()))?;
    let shm_handle = registry.add(region, None, ResourceType::SharedMemory)?;
    let shm_shared_id = registry.share_handle(shm_handle.into_id())?;
    QueueService
        .commit(
            &writer,
            QueueCommit {
                endpoint_id: 0,
                reservation_id: reservation.reservation_id,
                shm_shared_id,
                offset: 0,
                len: payload_len,
            },
        )
        .map_err(|err| KernelError::Driver(err.to_string()))?;
    Ok(())
}
