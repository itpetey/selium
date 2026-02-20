use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Result;
use selium_abi::Capability;
use selium_kernel::{
    Kernel,
    hostcalls::{process, session, shm, singleton, time},
    services::shared_memory_service::SharedMemoryDriver,
    services::singleton_service::SingletonRegistryService,
    services::time_service::SystemTimeService,
};
use tokio::sync::Notify;

use crate::{
    RuntimeEngine,
    providers::module_repository_fs::FilesystemModuleRepository,
    wamr::hostcall_linker::{WamrHostcallOperation, WamrOperationExt},
    wamr::runtime::{WamrProcessDriver, WamrRuntime},
    wasmtime::runtime::{WasmtimeProcessDriver, WasmtimeRuntime},
    wasmtime::{
        guest_async::GuestAsync,
        hostcall_linker::{LinkableOperation, WasmtimeOperationExt},
    },
};

/// Where WASM modules are stored.
const MODULES_SUBDIR: &str = "modules";

pub fn build(work_dir: impl AsRef<Path>, engine: RuntimeEngine) -> Result<(Kernel, Arc<Notify>)> {
    let modules_dir: PathBuf = work_dir.as_ref().join(MODULES_SUBDIR);
    let mut builder = Kernel::build();
    let mut capability_ops: HashMap<Capability, Vec<Arc<dyn LinkableOperation>>> = HashMap::new();
    let mut wamr_ops: HashMap<Capability, Vec<Arc<dyn WamrHostcallOperation>>> = HashMap::new();

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
    wamr_ops
        .entry(Capability::SessionLifecycle)
        .or_default()
        .extend([
            session.0.as_wamr_operation(),
            session.1.as_wamr_operation(),
            session.2.as_wamr_operation(),
            session.3.as_wamr_operation(),
            session.4.as_wamr_operation(),
            session.5.as_wamr_operation(),
        ]);

    // Singleton registry.
    let singleton = singleton::operations(SingletonRegistryService);
    capability_ops
        .entry(Capability::SingletonRegistry)
        .or_default()
        .push(singleton.0.as_linkable());
    capability_ops
        .entry(Capability::SingletonLookup)
        .or_default()
        .push(singleton.1.as_linkable());
    wamr_ops
        .entry(Capability::SingletonRegistry)
        .or_default()
        .push(singleton.0.as_wamr_operation());
    wamr_ops
        .entry(Capability::SingletonLookup)
        .or_default()
        .push(singleton.1.as_wamr_operation());

    // Time.
    let time = time::operations(SystemTimeService);
    capability_ops
        .entry(Capability::TimeRead)
        .or_default()
        .extend([time.0.as_linkable(), time.1.as_linkable()]);
    wamr_ops
        .entry(Capability::TimeRead)
        .or_default()
        .extend([time.0.as_wamr_operation(), time.1.as_wamr_operation()]);

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
    wamr_ops
        .entry(Capability::SharedMemory)
        .or_default()
        .extend([
            shm.0.as_wamr_operation(),
            shm.1.as_wamr_operation(),
            shm.2.as_wamr_operation(),
            shm.3.as_wamr_operation(),
            shm.4.as_wamr_operation(),
            shm.5.as_wamr_operation(),
        ]);

    let module_repository: Arc<FilesystemModuleRepository> =
        builder.add_capability(Arc::new(FilesystemModuleRepository::new(&modules_dir)));
    let shutdown = Arc::new(Notify::new());

    match engine {
        RuntimeEngine::Wamr => {
            let wamr_runtime = Arc::new(WamrRuntime::new(wamr_ops)?);
            let wamr = builder.add_capability(WamrProcessDriver::new(
                Arc::clone(&wamr_runtime),
                module_repository,
            ));
            let process = process::lifecycle_ops(wamr);
            wamr_runtime.extend_capability(
                Capability::ProcessLifecycle,
                vec![process.0.as_wamr_operation(), process.1.as_wamr_operation()],
            )?;
        }
        RuntimeEngine::Wasmtime => {
            let guest_async =
                builder.add_capability(Arc::new(GuestAsync::new(Arc::clone(&shutdown))));
            let wasmtime_runtime = Arc::new(WasmtimeRuntime::new(
                capability_ops.clone(),
                Arc::clone(&guest_async),
            )?);
            let wasmtime = builder.add_capability(WasmtimeProcessDriver::new(
                wasmtime_runtime.clone(),
                module_repository,
            ));
            let process = process::lifecycle_ops(wasmtime);
            wasmtime_runtime.extend_capability(
                Capability::ProcessLifecycle,
                vec![process.0.as_linkable(), process.1.as_linkable()],
            )?;
        }
    }

    Ok((builder.build()?, shutdown))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    use crate::{wamr::runtime::WamrProcessDriver, wasmtime::runtime::WasmtimeProcessDriver};

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
        let (wamr_kernel, _shutdown) = build(&dir, RuntimeEngine::Wamr).expect("build kernel");
        assert!(wamr_kernel.get::<WamrProcessDriver>().is_some());

        let (wasmtime_kernel, _shutdown) =
            build(&dir, RuntimeEngine::Wasmtime).expect("build kernel");
        assert!(wasmtime_kernel.get::<WasmtimeProcessDriver>().is_some());
    }
}
