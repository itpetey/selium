use selium_abi::{
    Capability, CapabilityGrant, CompletionState, HostcallOutput, HostcallRequest, ResourceClass,
    ResourceSelector,
};
use selium_runtime::{ReadinessCondition, Runtime, SystemGuestDescriptor};

#[expect(clippy::panic, reason = "test helper unreachable branch")]
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

#[expect(clippy::panic, reason = "test helper unreachable branch")]
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

#[expect(clippy::panic, reason = "test helper unreachable branch")]
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

#[expect(clippy::panic, reason = "test helper unreachable branch")]
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
        tag: 1,
        flags: 0,
        _reserved: [0; 3],
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
        data_offset + 12,
        payload.to_vec(),
    );

    // Read back and validate the header.
    let read_header_bytes =
        read_shared_memory(&runtime, process_id, mapping.local_id, data_offset, 12);
    let decoded = selium_io::FrameHeader::decode(&read_header_bytes).expect("valid frame header");
    assert_eq!(decoded.len, 5);
    assert_eq!(decoded.tag, 1);

    // Read back and validate the payload.
    let read_payload =
        read_shared_memory(&runtime, process_id, mapping.local_id, data_offset + 12, 5);
    assert_eq!(read_payload, b"hello");

    // Verify frame_size() matches.
    assert_eq!(decoded.frame_size(), 17);
}

/// Tests multi-memory shared region layout header write/read through the runtime.
///
/// Validates that a layout written by SharedRegionBuilder can be read back
/// by an attaching party.
#[test]
fn shared_memory_multi_memory_layout_discovery() {
    let runtime = Runtime::default();
    let process_id = spawn_guest(&runtime, "layout-test");

    let region_size: u32 = 8192;
    let region = alloc_shared_region(&runtime, process_id, region_size, 8);
    let mapping = attach_shared_region(&runtime, process_id, region.shared_id, 0, region_size);

    // Write a SharedRegionBuilder-style layout header.
    let magic: u64 = 0x53454C49554D454D;
    write_shared_memory(
        &runtime,
        process_id,
        mapping.local_id,
        0,
        magic.to_le_bytes().to_vec(),
    );
    write_shared_memory(
        &runtime,
        process_id,
        mapping.local_id,
        8,
        (region_size as u64).to_le_bytes().to_vec(),
    );
    write_shared_memory(
        &runtime,
        process_id,
        mapping.local_id,
        16,
        2u32.to_le_bytes().to_vec(),
    );
    write_shared_memory(
        &runtime,
        process_id,
        mapping.local_id,
        20,
        0u32.to_le_bytes().to_vec(),
    );

    // Memory 0: offset 32, len 4096
    write_shared_memory(
        &runtime,
        process_id,
        mapping.local_id,
        24,
        32u32.to_le_bytes().to_vec(),
    );
    write_shared_memory(
        &runtime,
        process_id,
        mapping.local_id,
        28,
        4096u32.to_le_bytes().to_vec(),
    );

    // Memory 1: offset 4128, len 4096
    write_shared_memory(
        &runtime,
        process_id,
        mapping.local_id,
        32,
        4128u32.to_le_bytes().to_vec(),
    );
    write_shared_memory(
        &runtime,
        process_id,
        mapping.local_id,
        36,
        4096u32.to_le_bytes().to_vec(),
    );

    // Attach a second mapping and read back the layout.
    let mapping2 = attach_shared_region(&runtime, process_id, region.shared_id, 0, region_size);

    let read_magic = read_shared_memory(&runtime, process_id, mapping2.local_id, 0, 8);
    assert_eq!(u64::from_le_bytes(read_magic.try_into().unwrap()), magic);

    let read_count = read_shared_memory(&runtime, process_id, mapping2.local_id, 16, 4);
    assert_eq!(u32::from_le_bytes(read_count.try_into().unwrap()), 2);

    let read_offset0 = read_shared_memory(&runtime, process_id, mapping2.local_id, 24, 4);
    let read_len0 = read_shared_memory(&runtime, process_id, mapping2.local_id, 28, 4);
    assert_eq!(u32::from_le_bytes(read_offset0.try_into().unwrap()), 32);
    assert_eq!(u32::from_le_bytes(read_len0.try_into().unwrap()), 4096);

    let read_offset1 = read_shared_memory(&runtime, process_id, mapping2.local_id, 32, 4);
    let read_len1 = read_shared_memory(&runtime, process_id, mapping2.local_id, 36, 4);
    assert_eq!(u32::from_le_bytes(read_offset1.try_into().unwrap()), 4128);
    assert_eq!(u32::from_le_bytes(read_len1.try_into().unwrap()), 4096);
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

#[expect(
    clippy::indexing_slicing,
    reason = "test helper always bootstraps one guest"
)]
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

#[expect(clippy::panic, reason = "test helper unreachable branch")]
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

#[expect(clippy::panic, reason = "test helper unreachable branch")]
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

#[expect(clippy::panic, reason = "test helper unreachable branch")]
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

#[expect(
    clippy::indexing_slicing,
    reason = "test helper always bootstraps one guest"
)]
fn spawn_guest_with_grants(runtime: &Runtime, name: &str, grants: Vec<CapabilityGrant>) -> u64 {
    let report = runtime
        .bootstrap_system_guests(selium_runtime::RuntimeConfig {
            system_guests: vec![SystemGuestDescriptor {
                name: name.to_string(),
                module_id: format!("{name}-module"),
                module_bytes: module_with_entrypoint("boot"),
                entrypoint: "boot".to_string(),
                arguments: Vec::new(),
                grants,
                dependencies: Vec::new(),
                readiness: ReadinessCondition::Immediate,
            }],
        })
        .expect("bootstrap");
    report.guests[0].process_id
}

/// Tests host queue create and attach hostcalls.
#[test]
fn host_queue_create_and_attach() {
    let runtime = Runtime::default();
    let process_id = spawn_guest_with_grants(
        &runtime,
        "queue-test",
        vec![CapabilityGrant::new(
            Capability::HostQueue,
            vec![ResourceSelector::ResourceClass(ResourceClass::HostQueue)],
        )],
    );

    let (_, create_id) = runtime.begin_hostcall(process_id, HostcallRequest::HostQueueCreate);
    let descriptor = match runtime.poll_hostcall(process_id, create_id) {
        CompletionState::Ready(HostcallOutput::HostQueue(d)) => d,
        other => panic!("expected HostQueue descriptor, got {other:?}"),
    };

    let (_, attach_id) = runtime.begin_hostcall(
        process_id,
        HostcallRequest::HostQueueAttach {
            shared_id: descriptor.shared_id,
        },
    );
    let attached = match runtime.poll_hostcall(process_id, attach_id) {
        CompletionState::Ready(HostcallOutput::HostQueue(d)) => d,
        other => panic!("expected HostQueue descriptor, got {other:?}"),
    };

    assert_eq!(attached.shared_id, descriptor.shared_id);
}

/// Tests host queue send and recv hostcalls.
#[test]
fn host_queue_send_and_recv() {
    let runtime = Runtime::default();
    let process_id = spawn_guest_with_grants(
        &runtime,
        "queue-send-recv-test",
        vec![CapabilityGrant::new(
            Capability::HostQueue,
            vec![ResourceSelector::ResourceClass(ResourceClass::HostQueue)],
        )],
    );

    let (_, create_id) = runtime.begin_hostcall(process_id, HostcallRequest::HostQueueCreate);
    let descriptor = match runtime.poll_hostcall(process_id, create_id) {
        CompletionState::Ready(HostcallOutput::HostQueue(d)) => d,
        other => panic!("expected HostQueue descriptor, got {other:?}"),
    };

    let (_, send_id) = runtime.begin_hostcall(
        process_id,
        HostcallRequest::HostQueueSend {
            local_id: descriptor.local_id,
            value: 42,
        },
    );
    assert_eq!(
        runtime.poll_hostcall(process_id, send_id),
        CompletionState::Ready(HostcallOutput::Empty)
    );

    let (_, recv_id) = runtime.begin_hostcall(
        process_id,
        HostcallRequest::HostQueueRecv {
            local_id: descriptor.local_id,
        },
    );
    match runtime.poll_hostcall(process_id, recv_id) {
        CompletionState::Ready(HostcallOutput::ConnectionInfo {
            client_process_id,
            value,
        }) => {
            assert_eq!(client_process_id, process_id);
            assert_eq!(value, 42);
        }
        other => panic!("expected ConnectionInfo, got {other:?}"),
    }
}

/// Tests host queue recv on an empty queue returns pending.
#[test]
fn host_queue_recv_empty_returns_pending() {
    let runtime = Runtime::default();
    let process_id = spawn_guest_with_grants(
        &runtime,
        "queue-pending-test",
        vec![CapabilityGrant::new(
            Capability::HostQueue,
            vec![ResourceSelector::ResourceClass(ResourceClass::HostQueue)],
        )],
    );

    let (_, create_id) = runtime.begin_hostcall(process_id, HostcallRequest::HostQueueCreate);
    let descriptor = match runtime.poll_hostcall(process_id, create_id) {
        CompletionState::Ready(HostcallOutput::HostQueue(d)) => d,
        other => panic!("expected HostQueue descriptor, got {other:?}"),
    };

    let (_, recv_id) = runtime.begin_hostcall(
        process_id,
        HostcallRequest::HostQueueRecv {
            local_id: descriptor.local_id,
        },
    );
    match runtime.poll_hostcall(process_id, recv_id) {
        CompletionState::Pending { .. } => {}
        other => panic!("expected Pending, got {other:?}"),
    }
}
