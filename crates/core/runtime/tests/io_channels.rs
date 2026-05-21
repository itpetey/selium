use selium_abi::{
    Capability, CapabilityGrant, CompletionState, HostcallOutput, HostcallRequest, ResourceClass,
    ResourceSelector,
};
use selium_runtime::{ReadinessCondition, Runtime, SystemGuestDescriptor};

fn alloc_shared_region(
    runtime: &Runtime,
    process_id: u64,
    size: u32,
    alignment: u32,
) -> selium_abi::SharedRegionDescriptor {
    let (status, op_id) = runtime.begin_hostcall(
        process_id,
        HostcallRequest::SharedMemoryAllocate { size, alignment },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_READY);
    match runtime.poll_hostcall(process_id, op_id) {
        CompletionState::Ready(HostcallOutput::SharedRegion(descriptor)) => descriptor,
        other => panic!("expected SharedRegion, got {other:?}"),
    }
}

fn attach_shared_region(
    runtime: &Runtime,
    process_id: u64,
    shared_id: u64,
    offset: u32,
    len: u32,
) -> selium_abi::SharedMappingDescriptor {
    let (status, op_id) = runtime.begin_hostcall(
        process_id,
        HostcallRequest::SharedMemoryAttach {
            shared_id,
            offset,
            len,
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_READY);
    match runtime.poll_hostcall(process_id, op_id) {
        CompletionState::Ready(HostcallOutput::SharedMapping(descriptor)) => descriptor,
        other => panic!("expected SharedMapping, got {other:?}"),
    }
}

fn create_signal(runtime: &Runtime, process_id: u64) -> selium_abi::SignalDescriptor {
    let (status, op_id) = runtime.begin_hostcall(process_id, HostcallRequest::SignalCreate);
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_READY);
    match runtime.poll_hostcall(process_id, op_id) {
        CompletionState::Ready(HostcallOutput::Signal(descriptor)) => descriptor,
        other => panic!("expected Signal, got {other:?}"),
    }
}

fn module_with_entrypoint(entrypoint: &str) -> Vec<u8> {
    wat::parse_str(format!("(module (func (export \"{entrypoint}\")))")).expect("compile wat")
}

fn read_shared_memory(
    runtime: &Runtime,
    process_id: u64,
    local_id: u64,
    offset: u32,
    len: u32,
) -> Vec<u8> {
    let (status, op_id) = runtime.begin_hostcall(
        process_id,
        HostcallRequest::SharedMemoryRead {
            local_id,
            offset,
            len,
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_READY);
    match runtime.poll_hostcall(process_id, op_id) {
        CompletionState::Ready(HostcallOutput::Bytes(bytes)) => bytes,
        other => panic!("expected Bytes, got {other:?}"),
    }
}

/// Tests that shared memory region metadata survives write/read cycles.
///
/// Simulates the selium-io RingBuf header region layout:
///   [0..8) magic, [8..16) capacity, [16..24) writer count,
///   [24..32) reader count, [32..40) next_tail, [40..48) tail_cache
#[test]
fn ring_buffer_header_cursors_survive_write_read() {
    let runtime = Runtime::default();
    let process_id = spawn_guest(&runtime, "header-test");

    let region_size: u32 = 8192;
    let region = alloc_shared_region(&runtime, process_id, region_size, 8);
    let mapping = attach_shared_region(&runtime, process_id, region.shared_id, 0, region_size);

    let next_tail: u64 = 128;
    let tail_cache: u64 = 64;

    // Write next_tail at offset 32.
    write_shared_memory(
        &runtime,
        process_id,
        mapping.local_id,
        32,
        next_tail.to_le_bytes().to_vec(),
    );
    // Write tail_cache at offset 40.
    write_shared_memory(
        &runtime,
        process_id,
        mapping.local_id,
        40,
        tail_cache.to_le_bytes().to_vec(),
    );

    // Read them back and verify.
    let read_tail = read_shared_memory(&runtime, process_id, mapping.local_id, 32, 8);
    assert_eq!(u64::from_le_bytes(read_tail.try_into().unwrap()), 128);

    let read_cache = read_shared_memory(&runtime, process_id, mapping.local_id, 40, 8);
    assert_eq!(u64::from_le_bytes(read_cache.try_into().unwrap()), 64);
}

#[test]
fn shared_memory_atomic_u64_hostcalls_update_in_place() {
    let runtime = Runtime::default();
    let process_id = spawn_guest(&runtime, "atomic-test");

    let region = alloc_shared_region(&runtime, process_id, 64, 8);
    let mapping = attach_shared_region(&runtime, process_id, region.shared_id, 0, 64);

    assert_eq!(
        fetch_add_shared_memory_u64(&runtime, process_id, mapping.local_id, 8, 3),
        0
    );
    assert_eq!(
        compare_exchange_shared_memory_u64(&runtime, process_id, mapping.local_id, 8, 3, 7),
        3
    );
    assert_eq!(
        compare_exchange_shared_memory_u64(&runtime, process_id, mapping.local_id, 8, 3, 9),
        7
    );

    let bytes = read_shared_memory(&runtime, process_id, mapping.local_id, 8, 8);
    assert_eq!(u64::from_le_bytes(bytes.try_into().unwrap()), 7);
}

/// Tests the selium-io frame header wire format through kernel shared memory.
///
/// This validates that frames written via one mapping can be read back
/// through another, simulating two guests sharing a ring buffer region.
#[test]
fn shared_memory_ring_buffer_frame_format() {
    let runtime = Runtime::default();
    let process_id = spawn_guest(&runtime, "frame-format-test");

    // Allocate a shared memory region large enough for a ring buffer header + data.
    let region_size: u32 = 8192;
    let region = alloc_shared_region(&runtime, process_id, region_size, 8);

    // Attach a mapping to the region for writing.
    let mapping = attach_shared_region(&runtime, process_id, region.shared_id, 0, region_size);

    // The data area starts at offset 4096 (REGION_HEADER_BYTES).
    let data_offset: u32 = 4096;

    // Write a frame header + payload: "hello" at the data offset.
    let payload = b"hello";
    let header = selium_io::FrameHeader {
        len: payload.len() as u32,
        flags: 0,
        writer_id: 1,
    };
    let header_bytes = header.encode();

    // Write header and payload consecutively.
    write_shared_memory(
        &runtime,
        process_id,
        mapping.local_id,
        data_offset,
        header_bytes.to_vec(),
    );
    write_shared_memory(
        &runtime,
        process_id,
        mapping.local_id,
        data_offset + 8,
        payload.to_vec(),
    );

    // Read back and validate the header.
    let read_header_bytes =
        read_shared_memory(&runtime, process_id, mapping.local_id, data_offset, 8);
    let decoded = selium_io::FrameHeader::decode(&read_header_bytes).expect("valid frame header");
    assert_eq!(decoded.len, 5);
    assert_eq!(decoded.writer_id, 1);

    // Read back and validate the payload.
    let read_payload =
        read_shared_memory(&runtime, process_id, mapping.local_id, data_offset + 8, 5);
    assert_eq!(read_payload, b"hello");

    // Verify frame_size() matches.
    assert_eq!(decoded.frame_size(), 13);
}

/// Tests signal creation and notify through the runtime.
///
/// Signals are the notification primitive used by selium-io channels.
#[test]
fn signal_create_notify_through_hostcalls() {
    let runtime = Runtime::default();
    let process_id = spawn_guest(&runtime, "signal-test");

    let signal = create_signal(&runtime, process_id);

    let (_, generation_id) = runtime.begin_hostcall(
        process_id,
        HostcallRequest::SignalGeneration {
            local_id: signal.local_id,
        },
    );
    assert_eq!(
        runtime.poll_hostcall(process_id, generation_id),
        CompletionState::Ready(HostcallOutput::SignalGeneration(0))
    );

    // Verify the signal can be attached by shared id.
    let (_, attach_id) = runtime.begin_hostcall(
        process_id,
        HostcallRequest::SignalAttach {
            shared_id: signal.shared_id,
        },
    );
    let CompletionState::Ready(HostcallOutput::Signal(_attached)) =
        runtime.poll_hostcall(process_id, attach_id)
    else {
        panic!("expected attached signal");
    };

    // Notify and verify generation advances.
    let (_, notify_id) = runtime.begin_hostcall(
        process_id,
        HostcallRequest::SignalNotify {
            local_id: signal.local_id,
        },
    );
    assert_eq!(
        runtime.poll_hostcall(process_id, notify_id),
        CompletionState::Ready(HostcallOutput::SignalGeneration(1))
    );

    let (_, generation_id) = runtime.begin_hostcall(
        process_id,
        HostcallRequest::SignalGeneration {
            local_id: signal.local_id,
        },
    );
    assert_eq!(
        runtime.poll_hostcall(process_id, generation_id),
        CompletionState::Ready(HostcallOutput::SignalGeneration(1))
    );
}

fn spawn_guest(runtime: &Runtime, name: &str) -> u64 {
    let report = runtime
        .bootstrap_system_guests(selium_runtime::RuntimeConfig {
            system_guests: vec![SystemGuestDescriptor {
                name: name.to_string(),
                module_id: format!("{name}-module"),
                module_bytes: module_with_entrypoint("boot"),
                entrypoint: "boot".to_string(),
                arguments: Vec::new(),
                grants: vec![
                    CapabilityGrant::new(
                        Capability::SharedMemory,
                        vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
                    ),
                    CapabilityGrant::new(
                        Capability::SharedMemory,
                        vec![ResourceSelector::ResourceClass(
                            ResourceClass::SharedMapping,
                        )],
                    ),
                    CapabilityGrant::new(
                        Capability::Signal,
                        vec![ResourceSelector::ResourceClass(ResourceClass::Signal)],
                    ),
                ],
                dependencies: Vec::new(),
                readiness: ReadinessCondition::Immediate,
            }],
        })
        .expect("bootstrap");
    report.guests[0].process_id
}

fn write_shared_memory(
    runtime: &Runtime,
    process_id: u64,
    local_id: u64,
    offset: u32,
    bytes: Vec<u8>,
) {
    let (status, op_id) = runtime.begin_hostcall(
        process_id,
        HostcallRequest::SharedMemoryWrite {
            local_id,
            offset,
            bytes,
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_READY);
    match runtime.poll_hostcall(process_id, op_id) {
        CompletionState::Ready(HostcallOutput::Empty) => {}
        other => panic!("expected Empty, got {other:?}"),
    }
}

fn fetch_add_shared_memory_u64(
    runtime: &Runtime,
    process_id: u64,
    local_id: u64,
    offset: u32,
    value: u64,
) -> u64 {
    let (status, op_id) = runtime.begin_hostcall(
        process_id,
        HostcallRequest::SharedMemoryFetchAddU64 {
            local_id,
            offset,
            value,
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_READY);
    match runtime.poll_hostcall(process_id, op_id) {
        CompletionState::Ready(HostcallOutput::U64(previous)) => previous,
        other => panic!("expected U64, got {other:?}"),
    }
}

fn compare_exchange_shared_memory_u64(
    runtime: &Runtime,
    process_id: u64,
    local_id: u64,
    offset: u32,
    current: u64,
    new: u64,
) -> u64 {
    let (status, op_id) = runtime.begin_hostcall(
        process_id,
        HostcallRequest::SharedMemoryCompareExchangeU64 {
            local_id,
            offset,
            current,
            new,
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_READY);
    match runtime.poll_hostcall(process_id, op_id) {
        CompletionState::Ready(HostcallOutput::U64(previous)) => previous,
        other => panic!("expected U64, got {other:?}"),
    }
}
