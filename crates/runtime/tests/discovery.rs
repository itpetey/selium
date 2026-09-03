//! Discovery bootstrap integration test.
//!
//! Deploys the real `selium-discovery` and `selium-discovery-probe` WASM
//! guests together and asserts on the control-plane slice end-to-end:
//! bootstrap with discovery wiring, Tier-1 registration events via the
//! discovery feed, guest→discovery rendezvous, readiness signalling, and
//! URI revocation on process exit.
//!
//! Cross-guest shared-memory RPC wake is not yet implemented (tracked by
//! `channel-wake-wait`), so Tier-2 register/lookup is deferred. The
//! existing `shm_transport` tests cover the RPC codec paths.
//!
//! This test is `#[ignore]`d by default because it requires both WASM
//! guests to be built for `wasm32-unknown-unknown` first:
//!
//! ```sh
//! cargo build --target wasm32-unknown-unknown -p selium-discovery -p selium-discovery-probe
//! cargo test -p selium-runtime --test discovery -- --ignored
//! ```

use std::path::PathBuf;

use selium_abi::{
    Capability, CapabilityGrant, CompletionState, DiscoveryRequest, HostcallOutput,
    HostcallRequest, ProcessId, RegionProt, ResourceClass, ResourceKind, ResourceSelector,
    decode_rkyv,
};
use selium_encoding::FlatMsg;
use selium_proto_dns::RESOLVE_URI;
use selium_runtime::{ReadinessCondition, Runtime, RuntimeConfig, SystemGuestDescriptor};
use selium_shm::{Channel, transport::ShmTransport};
use selium_wire::{framed::FramedRead, pubsub::Subscriber};

#[expect(clippy::panic, reason = "unexpected hostcall output indicates a bug")]
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

fn attach_feed_subscriber(runtime: &Runtime) -> Subscriber<Vec<u8>, ShmTransport> {
    let feed_region_id = runtime
        .discovery_feed_region_id()
        .expect("discovery feed region id");
    let channel = Channel::attach(feed_region_id).expect("attach to discovery feed");
    let capacity = channel.ring().capacity();
    let transport = ShmTransport::new(&channel, &channel).expect("feed transport");
    Subscriber::new(FramedRead::new(transport), Some(capacity))
}

#[test]
#[ignore = "requires both discovery and discovery-probe guests built for wasm32-unknown-unknown"]
fn discovery_bootstrap_slice_end_to_end() {
    let discovery_wasm = read_wasm(&discovery_wasm_path());
    let probe_wasm = read_wasm(&discovery_probe_wasm_path());

    let runtime = Runtime::default();
    let report = runtime
        .bootstrap_system_guests(RuntimeConfig {
            start_discovery: true,
            system_guests: vec![
                discovery_descriptor(discovery_wasm),
                discovery_probe_descriptor(probe_wasm),
            ],
        })
        .expect("bootstrap system guests");

    // All guests should be in the report.
    assert_eq!(report.guests.len(), 2);
    let discovery_guest = report
        .guests
        .iter()
        .find(|g| g.name == "discovery")
        .expect("discovery guest in report");
    let probe_guest = report
        .guests
        .iter()
        .find(|g| g.name == "discovery-probe")
        .expect("probe guest in report");

    // --- 2.2: Assert both guests reached readiness ---
    let activity = runtime.activity_log();
    assert!(
        activity
            .iter()
            .any(|event| event.process_id == Some(discovery_guest.process_id)
                && event.message.contains("guest ready")),
        "expected discovery GuestReady, got: {activity:?}"
    );
    assert!(
        activity
            .iter()
            .any(|event| event.process_id == Some(probe_guest.process_id)
                && event.message.contains("guest ready")),
        "expected probe GuestReady, got: {activity:?}"
    );

    // --- Drain probe log and verify the guest ran ---
    let probe_messages = drain_log_messages(&runtime, probe_guest.process_id);
    assert!(
        probe_messages.iter().any(|message| message == "booting"),
        "expected 'booting' in probe log, got: {probe_messages:?}"
    );
    assert!(
        probe_messages
            .iter()
            .any(|message| message.contains("region allocated")),
        "expected 'region allocated' in probe log, got: {probe_messages:?}"
    );

    // --- Drain discovery log to confirm wiring ---
    let discovery_messages = drain_log_messages(&runtime, discovery_guest.process_id);
    assert!(
        discovery_messages
            .iter()
            .any(|message| message.contains("feed and listener attached")),
        "expected discovery feed/listener attach, got: {discovery_messages:?}"
    );

    // --- 2.3: Assert Tier-1 flow ---
    // Attach to the discovery feed and allocate a region from the host for
    // the probe process to observe Tier-1 register events.
    let mut subscriber = attach_feed_subscriber(&runtime);
    let host_region_id = alloc_region(&runtime, probe_guest.process_id, ResourceKind::SharedMemory);
    let expected_uri = format!(
        "sel://_sys/proc/{}/regions/{host_region_id}",
        probe_guest.process_id
    );

    let registered = drain_register_uris(&mut subscriber);
    assert!(
        registered.contains(&expected_uri),
        "expected Tier-1 register URI {expected_uri}, got: {registered:?}"
    );

    // --- Well-known connector channel provisioning ---
    // Spawn a stub guest standing in for the DNS connector after attaching
    // the subscriber, so its provision-time Register is observable on the
    // feed. The runtime provisions the channel (host listener queue +
    // leading entrypoint argument) and publishes the well-known URI.
    let stub = runtime
        .spawn_system_guest(well_known_stub_descriptor())
        .expect("spawn well-known stub");
    let stub_listener = stub
        .well_known_listener
        .expect("runtime provisions the well-known listener");
    assert_eq!(
        runtime.well_known_uri(stub.process_id),
        Some((RESOLVE_URI.to_string(), stub_listener))
    );
    let registered = drain_register_uris(&mut subscriber);
    assert!(
        registered.contains(RESOLVE_URI),
        "expected well-known register URI {RESOLVE_URI}, got: {registered:?}"
    );

    // --- 2.4: Assert revocation ---
    // Stop the probe process — the runtime must publish Revoke events.
    runtime
        .stop_process(probe_guest.process_id)
        .expect("stop probe process");

    let revoked = drain_revoke_uris(&mut subscriber);
    assert!(
        revoked.contains(&expected_uri),
        "expected revoke for {expected_uri}, got: {revoked:?}"
    );

    // Stopping the well-known stub revokes its well-known URI too.
    runtime
        .stop_process(stub.process_id)
        .expect("stop well-known stub");
    let revoked = drain_revoke_uris(&mut subscriber);
    assert!(
        revoked.contains(RESOLVE_URI),
        "expected revoke for {RESOLVE_URI}, got: {revoked:?}"
    );
    assert!(runtime.well_known_uri(stub.process_id).is_none());

    // Verify the probe process is fully gone.
    assert_eq!(runtime.loaded_guest_count(), 1); // only discovery remains

    // Cleanup: stop discovery too.
    runtime
        .stop_process(discovery_guest.process_id)
        .expect("stop discovery process");
    assert_eq!(runtime.loaded_guest_count(), 0);
}

fn discovery_descriptor(module_bytes: Vec<u8>) -> SystemGuestDescriptor {
    SystemGuestDescriptor {
        name: "discovery".to_string(),
        module_id: "discovery-module".to_string(),
        module_bytes,
        entrypoint: "discovery_main".to_string(),
        arguments: Vec::new(), // populated by bootstrap via set_discovery_feed_and_handle
        grants: vec![
            CapabilityGrant::new(
                Capability::SharedMemory,
                vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
            ),
            CapabilityGrant::new(
                Capability::HostQueue,
                vec![ResourceSelector::ResourceClass(ResourceClass::HostQueue)],
            ),
        ],
        dependencies: Vec::new(),
        readiness: ReadinessCondition::ActivityLogContains("guest ready".to_string()),
        tenant: None,
        well_known_uri: None,
        handlers: Vec::new(),
    }
}

fn discovery_probe_descriptor(module_bytes: Vec<u8>) -> SystemGuestDescriptor {
    SystemGuestDescriptor {
        name: "discovery-probe".to_string(),
        module_id: "discovery-probe-module".to_string(),
        module_bytes,
        entrypoint: "discovery_probe".to_string(),
        arguments: Vec::new(), // populated by bootstrap via set_discovery_handle
        grants: vec![
            CapabilityGrant::new(
                Capability::SharedMemory,
                vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
            ),
            CapabilityGrant::new(
                Capability::HostQueue,
                vec![ResourceSelector::ResourceClass(ResourceClass::HostQueue)],
            ),
        ],
        dependencies: vec!["discovery".to_string()],
        readiness: ReadinessCondition::ActivityLogContains("guest ready".to_string()),
        tenant: None,
        well_known_uri: None,
        handlers: Vec::new(),
    }
}

fn discovery_probe_wasm_path() -> PathBuf {
    target_dir().join("wasm32-unknown-unknown/debug/selium_discovery_probe.wasm")
}

fn discovery_wasm_path() -> PathBuf {
    target_dir().join("wasm32-unknown-unknown/debug/selium_discovery.wasm")
}

fn drain_log_messages(runtime: &Runtime, process_id: u64) -> Vec<String> {
    let frames = runtime
        .kernel()
        .processes()
        .drain_log_channel(process_id)
        .expect("drain log channel");
    frames
        .iter()
        .map(|frame| {
            selium_encoding::log::LogRecord::decode(frame)
                .expect("decode log record")
                .message
        })
        .collect()
}

#[expect(clippy::panic, reason = "feed read errors in test indicate a bug")]
fn drain_register_uris(
    subscriber: &mut Subscriber<Vec<u8>, ShmTransport>,
) -> std::collections::HashSet<String> {
    let mut uris = std::collections::HashSet::new();
    loop {
        match subscriber.read_with_tag() {
            Ok((bytes, _tag)) => {
                let request: DiscoveryRequest =
                    decode_rkyv(&bytes).expect("decode discovery request");
                if let DiscoveryRequest::Register { uri, .. } = request {
                    uris.insert(uri);
                }
            }
            Err(selium_wire::error::Error::BufferEmpty) => break,
            Err(error) => panic!("feed read failed: {error}"),
        }
    }
    uris
}

#[expect(clippy::panic, reason = "feed read errors in test indicate a bug")]
fn drain_revoke_uris(
    subscriber: &mut Subscriber<Vec<u8>, ShmTransport>,
) -> std::collections::HashSet<String> {
    let mut uris = std::collections::HashSet::new();
    loop {
        match subscriber.read_with_tag() {
            Ok((bytes, _tag)) => {
                let request: DiscoveryRequest =
                    decode_rkyv(&bytes).expect("decode discovery request");
                if let DiscoveryRequest::Revoke { uri } = request {
                    uris.insert(uri);
                }
            }
            Err(selium_wire::error::Error::BufferEmpty) => break,
            Err(error) => panic!("feed read failed: {error}"),
        }
    }
    uris
}

#[expect(
    clippy::panic,
    reason = "missing build artifact is a hard test failure"
)]
fn read_wasm(path: &std::path::Path) -> Vec<u8> {
    std::fs::read(path).unwrap_or_else(|_error| {
        panic!(
            "guest not found at {}.\n\
             Build it first:\n  \
             cargo build --target wasm32-unknown-unknown -p selium-discovery -p selium-discovery-probe",
            path.display()
        )
    })
}

fn target_dir() -> PathBuf {
    let target_dir = std::env::var("CARGO_TARGET_DIR")
        .unwrap_or_else(|_error| concat!(env!("CARGO_MANIFEST_DIR"), "/../../target").to_string());
    PathBuf::from(target_dir)
}

/// A minimal guest serving the DNS connector's well-known URI: its
/// entrypoint takes the runtime-injected listener id (one `i64` param).
fn well_known_stub_descriptor() -> SystemGuestDescriptor {
    let module_bytes = wat::parse_str(r#"(module (func (export "boot") (param i64)))"#)
        .expect("compile well-known stub wat");

    SystemGuestDescriptor {
        name: "dns-stub".to_string(),
        module_id: "dns-stub-module".to_string(),
        module_bytes,
        entrypoint: "boot".to_string(),
        arguments: Vec::new(), // populated by provisioning via the well-known listener
        grants: Vec::new(),    // provisioning adds the listener HostQueue grant
        dependencies: Vec::new(),
        readiness: ReadinessCondition::Immediate,
        tenant: None,
        well_known_uri: Some(RESOLVE_URI.to_string()),
        handlers: Vec::new(),
    }
}
