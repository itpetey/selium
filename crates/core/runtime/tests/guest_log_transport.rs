#![cfg(test)]

//! Integration tests for the guest log transport change.
//!
//! Tests the end-to-end behaviour of:
//! - Tier-1 discovery registration on AllocRegion
//! - URI revocation on process termination
//! - GuestLogRegister hostcall validation
//! - Deprecated GuestLog::write/read_from still functioning
//! - Drop backpressure channel semantics

use selium_abi::{
    Capability, CapabilityGrant, CompletionState, HostcallOutput, HostcallRequest, ProcessId,
    RegionProt, ResourceClass, ResourceKind, ResourceSelector,
};
use selium_guest::io::channels::ChannelBackpressure;
use selium_runtime::{ReadinessCondition, Runtime, SystemGuestDescriptor};

fn module_with_entrypoint(entrypoint: &str) -> Vec<u8> {
    wat::parse_str(format!("(module (func (export \"{entrypoint}\")))")).expect("compile wat")
}

fn spawn_guest(runtime: &Runtime, name: &str, grants: Vec<CapabilityGrant>) -> ProcessId {
    runtime
        .spawn_system_guest(SystemGuestDescriptor {
            name: name.to_string(),
            module_id: format!("{name}-module"),
            module_bytes: module_with_entrypoint("boot"),
            entrypoint: "boot".to_string(),
            arguments: Vec::new(),
            grants,
            dependencies: Vec::new(),
            readiness: ReadinessCondition::Immediate,
        })
        .expect("spawn guest")
        .process_id
}

fn spawn_shared_memory_guest(runtime: &Runtime, name: &str) -> ProcessId {
    spawn_guest(
        runtime,
        name,
        vec![CapabilityGrant::new(
            Capability::SharedMemory,
            vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
        )],
    )
}

fn alloc_region(runtime: &Runtime, process_id: ProcessId, purpose: ResourceKind) -> u64 {
    let (status, op_id) = runtime.begin_hostcall(
        process_id,
        HostcallRequest::AllocRegion {
            pages: 1,
            prot: RegionProt::ReadWrite,
            purpose,
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_READY);
    match runtime.poll_hostcall(process_id, op_id) {
        CompletionState::Ready(HostcallOutput::RegionAlloc(alloc)) => alloc.region_id,
        other => panic!("expected RegionAlloc, got {other:?}"),
    }
}

#[test]
fn alloc_region_with_log_channel_tracks_discovery_uris() {
    let runtime = Runtime::default();
    let process_id = spawn_shared_memory_guest(&runtime, "log-channel-guest");

    let region_id = alloc_region(&runtime, process_id, ResourceKind::LogChannel);

    // Verify the runtime tracked discovery URIs for this process.
    let uris = runtime
        .process_discovery_uris()
        .get(&process_id)
        .cloned()
        .unwrap_or_default();

    assert!(
        uris.contains(&format!("sel://process/{process_id}/regions/{region_id}")),
        "expected region URI to be tracked"
    );
    assert!(
        uris.contains(&format!("sel://process/{process_id}/logs")),
        "expected log alias URI to be tracked"
    );
}

#[test]
fn alloc_region_with_shared_memory_tracks_only_region_uri() {
    let runtime = Runtime::default();
    let process_id = spawn_shared_memory_guest(&runtime, "generic-guest");

    let region_id = alloc_region(&runtime, process_id, ResourceKind::SharedMemory);

    let uris = runtime
        .process_discovery_uris()
        .get(&process_id)
        .cloned()
        .unwrap_or_default();

    assert!(
        uris.contains(&format!("sel://process/{process_id}/regions/{region_id}")),
        "expected region URI to be tracked"
    );
    // SharedMemory has no purpose alias.
    assert_eq!(uris.len(), 1, "expected only one URI for SharedMemory");
}

#[test]
fn process_termination_revokes_discovery_uris() {
    let runtime = Runtime::default();
    let process_id = spawn_shared_memory_guest(&runtime, "terminating-guest");

    // Allocate regions to generate discovery URIs.
    let _r1 = alloc_region(&runtime, process_id, ResourceKind::LogChannel);
    let _r2 = alloc_region(&runtime, process_id, ResourceKind::SharedMemory);

    // Verify URIs are tracked.
    assert!(
        runtime.process_discovery_uris().contains_key(&process_id),
        "expected URIs to be tracked before termination"
    );

    // Stop the process — this should revoke all URIs.
    runtime.stop_process(process_id).expect("stop process");

    // Verify URIs are removed.
    assert!(
        !runtime.process_discovery_uris().contains_key(&process_id),
        "expected URIs to be revoked after termination"
    );
}

#[test]
fn deprecated_guest_log_write_still_functions() {
    let runtime = Runtime::default();
    let process_id = spawn_guest(
        &runtime,
        "legacy-log",
        vec![
            CapabilityGrant::new(
                Capability::GuestLogWrite,
                vec![ResourceSelector::ResourceClass(ResourceClass::GuestLog)],
            ),
            CapabilityGrant::new(
                Capability::GuestLogRead,
                vec![ResourceSelector::ResourceClass(ResourceClass::GuestLog)],
            ),
        ],
    );

    // Write a log entry via the legacy hostcall.
    let entry = selium_abi::GuestLogEntry {
        process_id: Some(process_id),
        level: "INFO".to_string(),
        target: "test".to_string(),
        message: "legacy log entry".to_string(),
    };
    let (status, write_op) =
        runtime.begin_hostcall(process_id, HostcallRequest::GuestLogWrite { entry });
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_READY);
    assert_eq!(
        match runtime.poll_hostcall(process_id, write_op) {
            CompletionState::Ready(output) => output,
            other => panic!("expected ready, got {other:?}"),
        },
        HostcallOutput::Empty
    );

    // Read it back.
    let (status, read_op) = runtime.begin_hostcall(
        process_id,
        HostcallRequest::GuestLogRead {
            cursor: 0,
            process_id: Some(process_id),
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_READY);
    let HostcallOutput::GuestLogEntries(entries) = (match runtime.poll_hostcall(process_id, read_op)
    {
        CompletionState::Ready(output) => output,
        other => panic!("expected ready, got {other:?}"),
    }) else {
        panic!("expected GuestLogEntries");
    };
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].message, "legacy log entry");
}

#[test]
fn drop_backpressure_channel_writer_never_blocks() {
    use selium_abi::ResourceKind;
    use selium_guest::io::channels::Channel;

    let channel =
        Channel::create_with_backpressure(64, ChannelBackpressure::Drop, ResourceKind::LogChannel)
            .expect("create drop channel");

    // Writer should be created successfully.
    let writer = channel.writer().expect("writer");
    assert_eq!(writer.backpressure(), ChannelBackpressure::Drop);

    // blocking_writer should be rejected.
    let result = channel.blocking_writer();
    assert!(
        matches!(
            result,
            Err(selium_guest::io::Error::BackpressureNotSupported)
        ),
        "expected BackpressureNotSupported on Drop channel"
    );
}

#[test]
fn park_backpressure_channel_accepts_blocking_writer() {
    use selium_abi::ResourceKind;
    use selium_guest::io::channels::Channel;

    let channel =
        Channel::create_with_backpressure(64, ChannelBackpressure::Park, ResourceKind::RpcRing)
            .expect("create park channel");

    // blocking_writer should succeed.
    let _bw = channel
        .blocking_writer()
        .expect("blocking writer on Park channel");
}

#[test]
fn guest_log_register_end_to_end() {
    let runtime = Runtime::default();
    let process_id = spawn_shared_memory_guest(&runtime, "log-register-e2e");

    // Allocate a region with LogChannel purpose.
    let region_id = alloc_region(&runtime, process_id, ResourceKind::LogChannel);

    // Register it as a log channel.
    let (status, reg_op) = runtime.begin_hostcall(
        process_id,
        HostcallRequest::GuestLogRegister {
            shared_id: region_id,
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_READY);
    assert_eq!(
        (match runtime.poll_hostcall(process_id, reg_op) {
            CompletionState::Ready(output) => output,
            other => panic!("expected ready, got {other:?}"),
        }),
        HostcallOutput::Empty
    );

    // Verify the kernel recorded the log channel.
    assert_eq!(
        runtime.kernel().log_channel_shared_id(process_id),
        Some(region_id)
    );
}

#[test]
fn purpose_alias_mapping_table() {
    use selium_runtime::discovery;

    assert_eq!(
        discovery::purpose_alias(ResourceKind::LogChannel),
        Some("logs")
    );
    assert_eq!(
        discovery::purpose_alias(ResourceKind::LiveTable),
        Some("tables")
    );
    assert_eq!(discovery::purpose_alias(ResourceKind::RpcRing), Some("rpc"));
    assert_eq!(
        discovery::purpose_alias(ResourceKind::PubSubTopic),
        Some("pubsub")
    );
    assert_eq!(discovery::purpose_alias(ResourceKind::NetworkBuffer), None);
    assert_eq!(discovery::purpose_alias(ResourceKind::DurableLog), None);
    assert_eq!(discovery::purpose_alias(ResourceKind::BlobStore), None);
    assert_eq!(discovery::purpose_alias(ResourceKind::SharedMemory), None);
}
