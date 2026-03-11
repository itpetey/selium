use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Result;
use selium_abi::Capability;
use selium_kernel::{
    Kernel,
    hostcalls::{network, process, queue, session, shm, storage, time},
    services::queue_service::QueueService,
    services::shared_memory_service::SharedMemoryDriver,
    services::time_service::SystemTimeService,
};
use selium_runtime_network::NetworkService;
use selium_runtime_storage::StorageService;
use tokio::sync::Notify;

use crate::{
    providers::module_repository_fs::FilesystemModuleRepository,
    usage::RuntimeUsageCollector,
    wasmtime::runtime::{WasmtimeProcessDriver, WasmtimeRuntime},
    wasmtime::{
        guest_async::GuestAsync,
        hostcall_linker::{LinkableOperation, WasmtimeOperationExt},
    },
};

/// Where WASM modules are stored.
const MODULES_SUBDIR: &str = "modules";
const USAGE_SAMPLE_PERIOD: std::time::Duration = std::time::Duration::from_secs(60);

pub fn build(work_dir: impl AsRef<Path>) -> Result<(Kernel, Arc<Notify>)> {
    let modules_dir: PathBuf = work_dir.as_ref().join(MODULES_SUBDIR);
    let mut builder = Kernel::build();
    let mut capability_ops: HashMap<Capability, Vec<Arc<dyn LinkableOperation>>> = HashMap::new();

    // Session lifecycle.
    let session_driver = builder
        .add_capability(selium_kernel::services::session_service::SessionLifecycleDriver::new());
    let session = session::operations(session_driver);
    capability_ops
        .entry(Capability::SessionLifecycle)
        .or_default()
        .extend([
            session.0.as_linkable(),
            session.1.as_linkable(),
            session.2.as_linkable(),
            session.3.as_linkable(),
            session.4.as_linkable(),
            session.5.as_linkable(),
        ]);

    // Time.
    let time = time::operations(SystemTimeService);
    capability_ops
        .entry(Capability::TimeRead)
        .or_default()
        .extend([time.0.as_linkable(), time.1.as_linkable()]);

    // Shared memory.
    let shm_driver = builder.add_capability(SharedMemoryDriver::new());
    let shm = shm::operations(shm_driver);
    capability_ops
        .entry(Capability::SharedMemory)
        .or_default()
        .extend([
            shm.0.as_linkable(),
            shm.1.as_linkable(),
            shm.2.as_linkable(),
            shm.3.as_linkable(),
            shm.4.as_linkable(),
            shm.5.as_linkable(),
        ]);

    // Queue control plane.
    let queue = queue::operations(QueueService);
    capability_ops
        .entry(Capability::QueueLifecycle)
        .or_default()
        .extend([
            queue.0.as_linkable(),
            queue.1.as_linkable(),
            queue.2.as_linkable(),
            queue.3.as_linkable(),
            queue.4.as_linkable(),
        ]);
    capability_ops
        .entry(Capability::QueueWriter)
        .or_default()
        .extend([
            queue.5.as_linkable(),
            queue.6.as_linkable(),
            queue.7.as_linkable(),
        ]);
    capability_ops
        .entry(Capability::QueueReader)
        .or_default()
        .extend([queue.8.as_linkable(), queue.9.as_linkable()]);

    // Protocol-neutral network I/O.
    let network_driver = builder.add_capability(NetworkService::new());
    let network = network::operations(network_driver);
    capability_ops
        .entry(Capability::NetworkLifecycle)
        .or_default()
        .extend([network.0.as_linkable(), network.1.as_linkable()]);
    capability_ops
        .entry(Capability::NetworkConnect)
        .or_default()
        .extend([network.2.as_linkable()]);
    capability_ops
        .entry(Capability::NetworkAccept)
        .or_default()
        .extend([network.3.as_linkable(), network.5.as_linkable()]);
    capability_ops
        .entry(Capability::NetworkStreamWrite)
        .or_default()
        .extend([network.4.as_linkable(), network.6.as_linkable()]);
    capability_ops
        .entry(Capability::NetworkStreamRead)
        .or_default()
        .extend([network.7.as_linkable()]);
    capability_ops
        .entry(Capability::NetworkRpcClient)
        .or_default()
        .extend([
            network.8.as_linkable(),
            network.9.as_linkable(),
            network.10.as_linkable(),
            network.11.as_linkable(),
        ]);
    capability_ops
        .entry(Capability::NetworkRpcServer)
        .or_default()
        .extend([
            network.12.as_linkable(),
            network.13.as_linkable(),
            network.14.as_linkable(),
            network.15.as_linkable(),
        ]);

    // Runtime-managed storage.
    let storage_driver = builder.add_capability(StorageService::new());
    let storage = storage::operations(storage_driver);
    capability_ops
        .entry(Capability::StorageLifecycle)
        .or_default()
        .extend([
            storage.0.as_linkable(),
            storage.1.as_linkable(),
            storage.2.as_linkable(),
        ]);
    capability_ops
        .entry(Capability::StorageLogWrite)
        .or_default()
        .extend([storage.3.as_linkable(), storage.4.as_linkable()]);
    capability_ops
        .entry(Capability::StorageLogRead)
        .or_default()
        .extend([
            storage.5.as_linkable(),
            storage.6.as_linkable(),
            storage.7.as_linkable(),
        ]);
    capability_ops
        .entry(Capability::StorageBlobWrite)
        .or_default()
        .extend([storage.8.as_linkable(), storage.9.as_linkable()]);
    capability_ops
        .entry(Capability::StorageBlobRead)
        .or_default()
        .extend([storage.10.as_linkable(), storage.11.as_linkable()]);

    let shutdown = Arc::new(Notify::new());
    let guest_async = builder.add_capability(Arc::new(GuestAsync::new(Arc::clone(&shutdown))));
    let usage = RuntimeUsageCollector::file_backed(work_dir.as_ref(), USAGE_SAMPLE_PERIOD)?;
    let wasmtime_runtime = Arc::new(WasmtimeRuntime::new(
        capability_ops.clone(),
        Arc::clone(&guest_async),
        usage,
    )?);
    let module_repository: Arc<FilesystemModuleRepository> =
        builder.add_capability(Arc::new(FilesystemModuleRepository::new(&modules_dir)));
    let wasmtime = builder.add_capability(WasmtimeProcessDriver::new(
        wasmtime_runtime.clone(),
        module_repository,
    ));

    let process = process::lifecycle_ops(wasmtime);
    wasmtime_runtime.extend_capability(
        Capability::ProcessLifecycle,
        vec![process.0.as_linkable(), process.1.as_linkable()],
    )?;

    Ok((builder.build()?, shutdown))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    use crate::wasmtime::runtime::WasmtimeProcessDriver;

    fn temp_dir() -> PathBuf {
        let id = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("selium-kernel-build-{id}"));
        fs::create_dir_all(path.join("modules")).expect("create modules dir");
        path
    }

    #[test]
    fn build_registers_runtime_capabilities() {
        let dir = temp_dir();
        let (kernel, _shutdown) = build(&dir).expect("build kernel");
        let process_driver = kernel.get::<WasmtimeProcessDriver>();
        assert!(process_driver.is_some());
    }
}
