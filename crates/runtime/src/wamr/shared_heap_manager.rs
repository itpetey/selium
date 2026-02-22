//! WAMR shared-heap management and shared-memory capability implementation.

use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::c_void,
    sync::{Arc, Mutex},
};

use selium_abi::{
    GuestResourceId, GuestUint, SHM_RING_CAPACITY_OFFSET, SHM_RING_FLAGS_OFFSET,
    SHM_RING_HEAD_OFFSET, SHM_RING_HEADER_BYTES, SHM_RING_MAGIC, SHM_RING_MAGIC_OFFSET,
    SHM_RING_SEQUENCE_OFFSET, SHM_RING_TAIL_OFFSET, SHM_RING_VERSION, SHM_RING_VERSION_OFFSET,
    ShmAlloc, ShmNotify, ShmReady, ShmRegion, ShmWaitCondition,
};
use selium_kernel::{
    guest_error::GuestError,
    spi::shared_memory::{SharedMemoryBindingContext, SharedMemoryCapability},
};
use tokio::sync::Notify;
use wamr_rust_sdk::sys::{
    SharedHeapInitArgs, wasm_module_inst_t, wasm_runtime_addr_app_to_native,
    wasm_runtime_addr_native_to_app, wasm_runtime_attach_shared_heap,
    wasm_runtime_chain_shared_heaps, wasm_runtime_create_shared_heap,
    wasm_runtime_detach_shared_heap, wasm_shared_heap_t,
};

/// Default shared memory arena size for WAMR-backed allocations.
const DEFAULT_ARENA_BYTES: u64 = 256 * 1024 * 1024;

struct HeapRecord {
    region: ShmRegion,
    buffer: Box<[u8]>,
    shared_heap: usize,
    notify: Arc<Notify>,
}

#[derive(Default)]
struct InstanceAttachment {
    attached: BTreeSet<GuestResourceId>,
    mappings: BTreeMap<GuestResourceId, GuestUint>,
}

#[derive(Debug, Clone, Copy)]
struct RingSnapshot {
    head: GuestUint,
    tail: GuestUint,
    sequence: GuestUint,
    readable: GuestUint,
    writable: GuestUint,
}

/// Runtime manager that owns WAMR shared heaps and per-instance attachment state.
pub struct SharedHeapManager {
    heaps: Mutex<BTreeMap<GuestResourceId, HeapRecord>>,
    instances: Mutex<BTreeMap<usize, InstanceAttachment>>,
}

impl SharedHeapManager {
    /// Create a new shared-heap manager.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            heaps: Mutex::new(BTreeMap::new()),
            instances: Mutex::new(BTreeMap::new()),
        })
    }

    /// Ensure a shared heap exists for `shared_id` and matches `region`.
    pub fn ensure_heap(
        &self,
        shared_id: GuestResourceId,
        region: ShmRegion,
    ) -> Result<(), GuestError> {
        let mut heaps = self
            .heaps
            .lock()
            .map_err(|_| GuestError::Subsystem("shared heap lock poisoned".to_string()))?;

        if let Some(record) = heaps.get(&shared_id) {
            if record.region != region {
                return Err(GuestError::InvalidArgument);
            }
            return Ok(());
        }

        let size = usize::try_from(region.len).map_err(|_| GuestError::InvalidArgument)?;
        let mut buffer = vec![0u8; size].into_boxed_slice();
        initialise_ring_header(&mut buffer)?;

        let mut init_args = SharedHeapInitArgs {
            pre_allocated_addr: buffer.as_mut_ptr().cast::<c_void>(),
            size: region.len,
        };

        let shared_heap = unsafe { wasm_runtime_create_shared_heap(&mut init_args) };
        if shared_heap.is_null() {
            return Err(GuestError::Subsystem(
                "create wamr shared heap failed".to_string(),
            ));
        }

        heaps.insert(
            shared_id,
            HeapRecord {
                region,
                buffer,
                shared_heap: shared_heap as usize,
                notify: Arc::new(Notify::new()),
            },
        );

        Ok(())
    }

    /// Attach a shared heap to the given module instance and return the mapping base offset.
    pub fn attach_for_instance(
        &self,
        module_inst: wasm_module_inst_t,
        shared_id: GuestResourceId,
    ) -> Result<GuestUint, GuestError> {
        let mut heaps = self
            .heaps
            .lock()
            .map_err(|_| GuestError::Subsystem("shared heap lock poisoned".to_string()))?;
        let mut instances = self
            .instances
            .lock()
            .map_err(|_| GuestError::Subsystem("shared heap instance lock poisoned".to_string()))?;

        if !heaps.contains_key(&shared_id) {
            return Err(GuestError::NotFound);
        }

        let key = module_inst as usize;
        let instance = instances.entry(key).or_default();
        if let Some(offset) = instance.mappings.get(&shared_id).copied() {
            return Ok(offset);
        }

        instance.attached.insert(shared_id);
        let rebuild = rebuild_instance_attachments(module_inst, instance, &mut heaps);
        if let Err(error) = rebuild {
            instance.attached.remove(&shared_id);
            return Err(error);
        }

        instance
            .mappings
            .get(&shared_id)
            .copied()
            .ok_or(GuestError::NotFound)
    }

    /// Detach a shared heap from the given module instance.
    pub fn detach_for_instance(
        &self,
        module_inst: wasm_module_inst_t,
        shared_id: GuestResourceId,
    ) -> Result<(), GuestError> {
        let mut heaps = self
            .heaps
            .lock()
            .map_err(|_| GuestError::Subsystem("shared heap lock poisoned".to_string()))?;
        let mut instances = self
            .instances
            .lock()
            .map_err(|_| GuestError::Subsystem("shared heap instance lock poisoned".to_string()))?;

        let key = module_inst as usize;
        let Some(instance) = instances.get_mut(&key) else {
            return Ok(());
        };

        if !instance.attached.remove(&shared_id) {
            return Ok(());
        }

        if instance.attached.is_empty() {
            unsafe {
                wasm_runtime_detach_shared_heap(module_inst);
            }
            instance.mappings.clear();
            instances.remove(&key);
            return Ok(());
        }

        rebuild_instance_attachments(module_inst, instance, &mut heaps)
    }

    /// Drop all attachment state for a module instance.
    pub fn clear_instance(&self, module_inst: wasm_module_inst_t) {
        if let Ok(mut instances) = self.instances.lock() {
            instances.remove(&(module_inst as usize));
        }
    }

    /// Wait for a ring readiness condition.
    pub async fn wait(
        &self,
        shared_id: GuestResourceId,
        region: ShmRegion,
        condition: ShmWaitCondition,
    ) -> Result<ShmReady, GuestError> {
        loop {
            let (snapshot, notifier) = self.snapshot(shared_id, region)?;
            if condition_ready(snapshot, condition) {
                return Ok(to_ready(snapshot));
            }
            notifier.notified().await;
        }
    }

    /// Notify waiters observing the ring.
    pub fn notify(
        &self,
        shared_id: GuestResourceId,
        region: ShmRegion,
        notify: ShmNotify,
    ) -> Result<ShmReady, GuestError> {
        let (snapshot, notifier) = self.snapshot(shared_id, region)?;
        if notify.sequence < snapshot.sequence {
            return Err(GuestError::InvalidArgument);
        }
        notifier.notify_waiters();
        Ok(to_ready(snapshot))
    }

    fn snapshot(
        &self,
        shared_id: GuestResourceId,
        region: ShmRegion,
    ) -> Result<(RingSnapshot, Arc<Notify>), GuestError> {
        let heaps = self
            .heaps
            .lock()
            .map_err(|_| GuestError::Subsystem("shared heap lock poisoned".to_string()))?;
        let record = heaps.get(&shared_id).ok_or(GuestError::NotFound)?;
        if record.region != region {
            return Err(GuestError::InvalidArgument);
        }

        let snapshot = snapshot_ring_header(&record.buffer, record.region)?;
        Ok((snapshot, Arc::clone(&record.notify)))
    }
}

/// WAMR-backed shared-memory capability for kernel SHM hostcalls.
pub struct WamrSharedMemoryService {
    next_offset: std::sync::atomic::AtomicU64,
    arena_bytes: u64,
    allocations: Mutex<BTreeMap<GuestUint, ShmRegion>>,
    manager: Arc<SharedHeapManager>,
}

impl WamrSharedMemoryService {
    /// Create a new shared-memory service.
    pub fn new(manager: Arc<SharedHeapManager>) -> Arc<Self> {
        Arc::new(Self {
            next_offset: std::sync::atomic::AtomicU64::new(0),
            arena_bytes: DEFAULT_ARENA_BYTES,
            allocations: Mutex::new(BTreeMap::new()),
            manager,
        })
    }

    fn validate_region(&self, region: ShmRegion) -> Result<(), GuestError> {
        let allocations = self.allocations.lock().map_err(|_| {
            GuestError::Subsystem("shared memory allocation lock poisoned".to_string())
        })?;
        let known = allocations
            .get(&region.offset)
            .ok_or(GuestError::NotFound)?;
        if *known != region {
            return Err(GuestError::InvalidArgument);
        }
        Ok(())
    }
}

impl SharedMemoryCapability for WamrSharedMemoryService {
    type Error = GuestError;

    fn alloc(&self, request: ShmAlloc) -> Result<ShmRegion, Self::Error> {
        if request.size == 0 {
            return Err(GuestError::InvalidArgument);
        }

        if request.align == 0 || !request.align.is_power_of_two() {
            return Err(GuestError::InvalidArgument);
        }

        let minimum = GuestUint::try_from(SHM_RING_HEADER_BYTES + 1)
            .map_err(|_| GuestError::InvalidArgument)?;
        if request.size < minimum {
            return Err(GuestError::InvalidArgument);
        }

        let size = u64::from(request.size);
        let align = u64::from(request.align);

        loop {
            let current = self.next_offset.load(std::sync::atomic::Ordering::Acquire);
            let aligned = align_up(current, align).ok_or(GuestError::InvalidArgument)?;
            let end = aligned
                .checked_add(size)
                .ok_or(GuestError::InvalidArgument)?;
            if end > self.arena_bytes {
                return Err(GuestError::WouldBlock);
            }

            if self
                .next_offset
                .compare_exchange(
                    current,
                    end,
                    std::sync::atomic::Ordering::AcqRel,
                    std::sync::atomic::Ordering::Acquire,
                )
                .is_ok()
            {
                let region = ShmRegion {
                    offset: GuestUint::try_from(aligned)
                        .map_err(|_| GuestError::InvalidArgument)?,
                    len: GuestUint::try_from(size).map_err(|_| GuestError::InvalidArgument)?,
                };
                let mut allocations = self.allocations.lock().map_err(|_| {
                    GuestError::Subsystem("shared memory allocation lock poisoned".to_string())
                })?;
                allocations.insert(region.offset, region);
                return Ok(region);
            }
        }
    }

    fn attach_mapping(
        &self,
        binding: &mut dyn SharedMemoryBindingContext,
        shared_id: GuestResourceId,
        region: ShmRegion,
    ) -> Result<GuestUint, Self::Error> {
        self.validate_region(region)?;
        self.manager.ensure_heap(shared_id, region)?;
        binding.attach_mapping(shared_id)
    }

    fn detach_mapping(
        &self,
        binding: &mut dyn SharedMemoryBindingContext,
        shared_id: GuestResourceId,
        region: ShmRegion,
    ) -> Result<(), Self::Error> {
        self.validate_region(region)?;
        binding.detach_mapping(shared_id)
    }

    fn wait(
        &self,
        shared_id: GuestResourceId,
        region: ShmRegion,
        condition: ShmWaitCondition,
    ) -> impl std::future::Future<Output = Result<ShmReady, Self::Error>> + Send {
        let manager = Arc::clone(&self.manager);
        async move { manager.wait(shared_id, region, condition).await }
    }

    fn notify(
        &self,
        shared_id: GuestResourceId,
        region: ShmRegion,
        notify: ShmNotify,
    ) -> Result<ShmReady, Self::Error> {
        self.manager.notify(shared_id, region, notify)
    }
}

fn rebuild_instance_attachments(
    module_inst: wasm_module_inst_t,
    instance: &mut InstanceAttachment,
    heaps: &mut BTreeMap<GuestResourceId, HeapRecord>,
) -> Result<(), GuestError> {
    let mut attached = instance.attached.iter();
    let Some(first_id) = attached.next() else {
        unsafe {
            wasm_runtime_detach_shared_heap(module_inst);
        }
        instance.mappings.clear();
        return Ok(());
    };

    let first_heap =
        heaps.get(first_id).ok_or(GuestError::NotFound)?.shared_heap as wasm_shared_heap_t;
    let mut chain = first_heap;
    for shared_id in attached {
        let heap = heaps
            .get(shared_id)
            .ok_or(GuestError::NotFound)?
            .shared_heap as wasm_shared_heap_t;
        chain = unsafe { wasm_runtime_chain_shared_heaps(chain, heap) };
        if chain.is_null() {
            return Err(GuestError::Subsystem(
                "chain wamr shared heaps failed".to_string(),
            ));
        }
    }

    let attached = unsafe { wasm_runtime_attach_shared_heap(module_inst, chain) };
    if !attached {
        return Err(GuestError::Subsystem(
            "attach wamr shared heap failed".to_string(),
        ));
    }

    let mut mappings = BTreeMap::new();
    for shared_id in &instance.attached {
        let record = heaps.get_mut(shared_id).ok_or(GuestError::NotFound)?;
        let native = record.buffer.as_mut_ptr().cast::<c_void>();
        let app_offset = unsafe { wasm_runtime_addr_native_to_app(module_inst, native) };
        let translated = unsafe { wasm_runtime_addr_app_to_native(module_inst, app_offset) };
        if translated != native {
            return Err(GuestError::Subsystem(
                "translate shared heap pointer failed".to_string(),
            ));
        }

        let offset = GuestUint::try_from(app_offset).map_err(|_| GuestError::InvalidArgument)?;
        mappings.insert(*shared_id, offset);
    }

    instance.mappings = mappings;
    Ok(())
}

fn initialise_ring_header(buffer: &mut [u8]) -> Result<(), GuestError> {
    if buffer.len() < SHM_RING_HEADER_BYTES + 1 {
        return Err(GuestError::InvalidArgument);
    }

    write_field(buffer, SHM_RING_MAGIC_OFFSET, SHM_RING_MAGIC)?;
    write_field(buffer, SHM_RING_VERSION_OFFSET, SHM_RING_VERSION)?;
    let payload_len = buffer
        .len()
        .checked_sub(SHM_RING_HEADER_BYTES)
        .ok_or(GuestError::InvalidArgument)?;
    let capacity = GuestUint::try_from(payload_len).map_err(|_| GuestError::InvalidArgument)?;
    write_field(buffer, SHM_RING_CAPACITY_OFFSET, capacity)?;
    write_field(buffer, SHM_RING_HEAD_OFFSET, 0)?;
    write_field(buffer, SHM_RING_TAIL_OFFSET, 0)?;
    write_field(buffer, SHM_RING_SEQUENCE_OFFSET, 0)?;
    write_field(buffer, SHM_RING_FLAGS_OFFSET, 0)?;
    Ok(())
}

fn snapshot_ring_header(buffer: &[u8], region: ShmRegion) -> Result<RingSnapshot, GuestError> {
    if usize::try_from(region.len).map_err(|_| GuestError::InvalidArgument)? != buffer.len() {
        return Err(GuestError::InvalidArgument);
    }

    let magic = read_field(buffer, SHM_RING_MAGIC_OFFSET)?;
    if magic != SHM_RING_MAGIC {
        return Err(GuestError::InvalidArgument);
    }

    let version = read_field(buffer, SHM_RING_VERSION_OFFSET)?;
    if version != SHM_RING_VERSION {
        return Err(GuestError::InvalidArgument);
    }

    let capacity = read_field(buffer, SHM_RING_CAPACITY_OFFSET)?;
    let payload_len = region
        .len
        .checked_sub(
            GuestUint::try_from(SHM_RING_HEADER_BYTES).map_err(|_| GuestError::InvalidArgument)?,
        )
        .ok_or(GuestError::InvalidArgument)?;
    if capacity == 0 || capacity > payload_len {
        return Err(GuestError::InvalidArgument);
    }

    let head = read_field(buffer, SHM_RING_HEAD_OFFSET)?;
    let tail = read_field(buffer, SHM_RING_TAIL_OFFSET)?;
    if tail < head {
        return Err(GuestError::InvalidArgument);
    }
    let readable = tail.checked_sub(head).ok_or(GuestError::InvalidArgument)?;
    if readable > capacity {
        return Err(GuestError::InvalidArgument);
    }

    let writable = capacity
        .checked_sub(readable)
        .ok_or(GuestError::InvalidArgument)?;
    let sequence = read_field(buffer, SHM_RING_SEQUENCE_OFFSET)?;
    let _flags = read_field(buffer, SHM_RING_FLAGS_OFFSET)?;

    Ok(RingSnapshot {
        head,
        tail,
        sequence,
        readable,
        writable,
    })
}

fn condition_ready(snapshot: RingSnapshot, condition: ShmWaitCondition) -> bool {
    match condition {
        ShmWaitCondition::DataAvailable => snapshot.readable > 0,
        ShmWaitCondition::SpaceAvailable => snapshot.writable > 0,
        ShmWaitCondition::SequenceAtLeast(sequence) => snapshot.sequence >= sequence,
    }
}

fn to_ready(snapshot: RingSnapshot) -> ShmReady {
    ShmReady {
        head: snapshot.head,
        tail: snapshot.tail,
        sequence: snapshot.sequence,
        readable: snapshot.readable,
        writable: snapshot.writable,
    }
}

fn align_up(value: u64, align: u64) -> Option<u64> {
    let mask = align.checked_sub(1)?;
    value.checked_add(mask).map(|aligned| aligned & !mask)
}

fn read_field(bytes: &[u8], offset: usize) -> Result<GuestUint, GuestError> {
    let end = offset
        .checked_add(core::mem::size_of::<GuestUint>())
        .ok_or(GuestError::InvalidArgument)?;
    let slice = bytes.get(offset..end).ok_or(GuestError::InvalidArgument)?;
    let mut value = [0u8; core::mem::size_of::<GuestUint>()];
    value.copy_from_slice(slice);
    Ok(GuestUint::from_le_bytes(value))
}

fn write_field(bytes: &mut [u8], offset: usize, value: GuestUint) -> Result<(), GuestError> {
    let end = offset
        .checked_add(core::mem::size_of::<GuestUint>())
        .ok_or(GuestError::InvalidArgument)?;
    let slot = bytes
        .get_mut(offset..end)
        .ok_or(GuestError::InvalidArgument)?;
    slot.copy_from_slice(&value.to_le_bytes());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ring_header_initialiser_sets_expected_defaults() {
        let mut bytes = vec![0u8; SHM_RING_HEADER_BYTES + 8];
        initialise_ring_header(&mut bytes).expect("header initialises");
        let region = ShmRegion {
            offset: 0,
            len: GuestUint::try_from(bytes.len()).expect("len fits in u32"),
        };
        let snapshot = snapshot_ring_header(&bytes, region).expect("snapshot");
        assert_eq!(snapshot.sequence, 0);
        assert_eq!(snapshot.readable, 0);
        assert!(snapshot.writable > 0);
    }

    #[test]
    fn atomic_viability_gate_shared_heap_offsets_resolve_for_two_instances() {
        let runtime = wamr_rust_sdk::runtime::Runtime::builder()
            .use_system_allocator()
            .build()
            .expect("runtime");

        let producer_module_bytes = wat::parse_str(
            r#"
            (module
                (memory (export "memory") 1)
                (func (export "touch") (param i32) (result i32)
                    local.get 0
                    i32.const 1
                    i32.atomic.rmw.add
                    drop
                    local.get 0
                    i32.atomic.load))
            "#,
        )
        .expect("producer wat");
        let producer_module = match wamr_rust_sdk::module::Module::from_vec(
            &runtime,
            producer_module_bytes,
            "atomic_producer",
        ) {
            Ok(module) => module,
            Err(error) => {
                panic!("atomic viability gate failed: WAMR cannot load atomics module ({error})")
            }
        };

        let consumer_module_bytes = wat::parse_str(
            r#"
            (module
                (memory (export "memory") 1)
                (func (export "read") (param i32) (result i32)
                    local.get 0
                    i32.atomic.load))
            "#,
        )
        .expect("consumer wat");
        let consumer_module = match wamr_rust_sdk::module::Module::from_vec(
            &runtime,
            consumer_module_bytes,
            "atomic_consumer",
        ) {
            Ok(module) => module,
            Err(error) => {
                panic!("atomic viability gate failed: WAMR cannot load atomics module ({error})")
            }
        };

        let producer =
            wamr_rust_sdk::instance::Instance::new(&runtime, &producer_module, 64 * 1024)
                .expect("producer instance");
        let consumer =
            wamr_rust_sdk::instance::Instance::new(&runtime, &consumer_module, 64 * 1024)
                .expect("consumer instance");

        let manager = SharedHeapManager::new();
        let region = ShmRegion {
            offset: 0,
            len: 64 * 1024,
        };
        manager.ensure_heap(1, region).expect("heap");

        manager
            .attach_for_instance(producer.get_inner_instance(), 1)
            .expect("attach producer");
        manager
            .attach_for_instance(consumer.get_inner_instance(), 1)
            .expect("attach consumer");

        let producer_func = wamr_rust_sdk::function::Function::find_export_func(&producer, "touch")
            .expect("touch fn");
        let consumer_func = wamr_rust_sdk::function::Function::find_export_func(&consumer, "read")
            .expect("read fn");

        let ptr = producer
            .app_addr_to_native(
                GuestUint::try_from(SHM_RING_HEAD_OFFSET).expect("offset fits") as u64,
            )
            .expect("native head pointer");
        unsafe {
            *(ptr.cast::<u32>()) = 0;
        }

        let offset = GuestUint::try_from(SHM_RING_HEAD_OFFSET).expect("offset fits") as i32;
        let touched = producer_func
            .call(
                &producer,
                &vec![wamr_rust_sdk::value::WasmValue::I32(offset)],
            )
            .expect("touch call");
        assert_eq!(touched, vec![wamr_rust_sdk::value::WasmValue::I32(1)]);

        let read = consumer_func
            .call(
                &consumer,
                &vec![wamr_rust_sdk::value::WasmValue::I32(offset)],
            )
            .expect("read call");
        assert_eq!(read, vec![wamr_rust_sdk::value::WasmValue::I32(1)]);
    }
}
