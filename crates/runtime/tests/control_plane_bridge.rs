//! Control-plane external-reachability integration test.
//!
//! Drives the full external control path end-to-end on one host: a native
//! `selium-client` completes a TLS 1.3 (mTLS) handshake against the real
//! `selium-connector-quic` guest, names the bridge route (`bridge.acme`,
//! routed by SNI through the real discovery guest), and the connector
//! delivers the stream to the real `selium-bridge` server, which maps the
//! authenticated client identity to grants and spawns a real
//! `selium-bridge-channel`. The bridge-channel resolves the control plane's
//! served route (`sel://acme/control`) through discovery and takes the
//! host-queue rendezvous: it allocates the two-ring session region, enqueues
//! it into the control plane's listener queue, and splices the client stream
//! into it — so the `selium-control-plane` guest's `rpc::accept` session
//! serves the typed `deploy`/`status` round trip over shared memory.
//!
//! This is the first test where an externally authenticated client reaches a
//! served host-queue route through the whole bridge; every earlier seam
//! (bridge pipe rendezvous, control-plane RPC loop, bootstrap wiring) is
//! exercised by focused native tests. It covers, composed:
//!
//! - mTLS edge termination with per-tenant client anchors and identity
//!   derivation (fingerprint → grants) in the bridge server,
//! - SNI routing (`bridge.acme` → `sel://acme/bridge`) through discovery,
//! - the delegated bridge-channel spawn (client grants + explicit-resource
//!   grants for the handed-off stream and the discovery listener),
//! - the protocol-aware bridge dispatch on the resolved target's class
//!   (host queue → rendezvous; session allocation, queue handoff, accept
//!   gates, teardown),
//! - the control plane's typed control surface: a `deploy` records desired
//!   state (durable log + projection) and returns the typed delegated
//!   status; a `status` reads the projection back.
//!
//! `#[ignore]`d by default because it requires the guests built for
//! `wasm32-unknown-unknown` first (release profile required — the TLS
//! handshake runs through the wasm interpreter, which is too slow at debug
//! optimization for the quinn timeouts):
//!
//! ```sh
//! cargo build --release --target wasm32-unknown-unknown \
//!   -p selium-discovery -p selium-connector-quic -p selium-bridge \
//!   -p selium-bridge-channel -p selium-control-plane
//! cargo test -p selium-runtime --test control_plane_bridge -- --ignored
//! ```
//!
//! The connector binds a fixed listener (`0.0.0.0:4433`, see its
//! `QUIC_LISTEN_ADDR`), so only one QUIC-spine-style test may run at a
//! time.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use selium_abi::{Capability, CapabilityGrant, ResourceClass, ResourceSelector};
use selium_client::{ConnectOptions, FlatMsg as _};
use selium_runtime::{ReadinessCondition, Runtime, RuntimeConfig, SystemGuestDescriptor};
use selium_service::{ControlRequest, ControlResponse, DelegationStatus, Deployment};

mod common;

/// The bridge route's server certificate (SAN `bridge.acme`), provisioned
/// into the `tls-certs` blob store before the connector guest boots.
const BRIDGE_CERT_PEM: &[u8] =
    include_bytes!("../../../guests/connector-quic/tests/fixtures/bridge_cert.pem");
const BRIDGE_KEY_PEM: &[u8] =
    include_bytes!("../../../guests/connector-quic/tests/fixtures/bridge_key.pem");
/// The bridge test client's certificate: its SPKI fingerprint is the one the
/// bridge-server's interim identity source recognises, and (being
/// self-signed) it doubles as the `acme` tenant's client trust anchor.
const CLIENT_CERT_PEM: &[u8] =
    include_bytes!("../../../guests/connector-quic/tests/fixtures/client_cert.pem");
const CLIENT_KEY_PEM: &[u8] =
    include_bytes!("../../../guests/connector-quic/tests/fixtures/client_key.pem");
/// The connector's fixed listener (its `QUIC_LISTEN_ADDR` const).
const CONNECTOR_ADDR: &str = "127.0.0.1:4433";
/// The control plane's served route, named in the bridge handshake.
const CONTROL_URI: &str = "sel://acme/control";
/// The day-1 scheduler seam's typed deferred context.
const SCHEDULER_DEFERRED: &str = "scheduler service not yet online";
/// SNI / TLS server name: the synthetic tenant wire name for the acme
/// bridge route (resolved by the connector to `sel://acme/bridge`).
const SERVER_NAME: &str = "bridge.acme";

fn bridge_channel_wasm() -> Vec<u8> {
    read_wasm("selium-bridge-channel", "selium_bridge_channel.wasm")
}

/// The bridge server system guest: tenant `acme`, self-registered
/// `sel://acme/bridge` route, pinned to the `sel-quic` connector. Its
/// process-lifecycle and delegation grants let it spawn bridge-channels
/// under the client's conferred grants.
fn bridge_server_descriptor(module_bytes: Vec<u8>) -> SystemGuestDescriptor {
    SystemGuestDescriptor {
        name: "bridge-server".to_string(),
        module_id: "bridge-server-module".to_string(),
        module_bytes,
        entrypoint: "bridge_server".to_string(),
        arguments: Vec::new(), // discovery handle injected by bootstrap
        grants: vec![
            CapabilityGrant::new(
                Capability::ProcessLifecycle,
                vec![ResourceSelector::ResourceClass(ResourceClass::Process)],
            ),
            CapabilityGrant::new(
                Capability::DelegateGrants,
                vec![ResourceSelector::Tenant("acme".to_string())],
            ),
            CapabilityGrant::new(
                Capability::HostQueue,
                vec![ResourceSelector::ResourceClass(ResourceClass::HostQueue)],
            ),
            CapabilityGrant::new(
                Capability::SharedMemory,
                vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
            ),
        ],
        // The server pins its listener to the registered `sel-quic`
        // handler, so the connector must be up first.
        dependencies: vec!["discovery".to_string(), "quic-connector".to_string()],
        readiness: ReadinessCondition::ActivityLogContains("guest ready".to_string()),
        tenant: Some("acme".to_string()),
        serving_role: None,
        handlers: Vec::new(),
    }
}

fn bridge_wasm() -> Vec<u8> {
    read_wasm("selium-bridge", "selium_bridge.wasm")
}

/// Builds the `selium-client` connection options: trust the connector's
/// bridge-route certificate, present the bridge test client's identity for
/// mTLS, and keep the transport patient (the wasm32 guests run on an
/// interpreter, so the TLS handshake takes far longer than the quinn
/// defaults assume).
fn client_options() -> ConnectOptions {
    let mut transport = quinn::TransportConfig::default();
    transport.max_idle_timeout(Some(quinn::IdleTimeout::from(quinn::VarInt::from(
        300_000u32,
    ))));
    transport.initial_rtt(Duration::from_millis(250));

    ConnectOptions {
        server_name: SERVER_NAME.to_string(),
        server_root: selium_client::certificates_from_pem(BRIDGE_CERT_PEM)
            .expect("parse bridge certificate PEM"),
        identity: Some(selium_client::ClientIdentity {
            cert_chain: selium_client::certificates_from_pem(CLIENT_CERT_PEM)
                .expect("parse client certificate PEM"),
            key: selium_client::private_key_from_pem(CLIENT_KEY_PEM).expect("parse client key PEM"),
        }),
        transport: Some(Arc::new(transport)),
    }
}

/// The QUIC connector system guest, exactly as the QUIC spine test deploys
/// it: Tier-1 `sel-quic` protocol handler, TLS material from the
/// `tls-certs` blob store, and a fixed UDP listener.
fn connector_descriptor(module_bytes: Vec<u8>) -> SystemGuestDescriptor {
    SystemGuestDescriptor {
        name: "quic-connector".to_string(),
        module_id: "quic-connector-module".to_string(),
        module_bytes,
        entrypoint: "connector_quic".to_string(),
        arguments: Vec::new(), // discovery handle injected by bootstrap
        grants: vec![
            CapabilityGrant::new(
                Capability::Network,
                vec![ResourceSelector::ResourceClass(ResourceClass::UdpSocket)],
            ),
            CapabilityGrant::new(
                Capability::SharedMemory,
                vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
            ),
            CapabilityGrant::new(
                Capability::HostQueue,
                vec![ResourceSelector::ResourceClass(ResourceClass::HostQueue)],
            ),
            CapabilityGrant::new(
                Capability::Storage,
                vec![ResourceSelector::ResourceClass(ResourceClass::BlobStore)],
            ),
        ],
        dependencies: vec!["discovery".to_string()],
        readiness: ReadinessCondition::Immediate,
        tenant: None,
        serving_role: None,
        handlers: vec!["sel-quic".to_string()],
    }
}

fn connector_wasm() -> Vec<u8> {
    read_wasm("selium-connector-quic", "selium_connector_quic.wasm")
}

/// The control-plane system guest, exactly as the control-plane bootstrap
/// test deploys it: storage (durable log + module blob store), shared
/// memory (RPC session rings), and host queue (serving listener), all
/// tenant-scoped.
fn control_plane_descriptor(module_bytes: Vec<u8>) -> SystemGuestDescriptor {
    SystemGuestDescriptor {
        name: "control-plane".to_string(),
        module_id: "control-plane-module".to_string(),
        module_bytes,
        entrypoint: "control_plane_main".to_string(),
        arguments: Vec::new(), // discovery handle injected by bootstrap
        grants: vec![
            CapabilityGrant::new(
                Capability::Storage,
                vec![
                    ResourceSelector::Tenant("acme".to_string()),
                    ResourceSelector::ResourceClass(ResourceClass::DurableLog),
                ],
            ),
            CapabilityGrant::new(
                Capability::Storage,
                vec![
                    ResourceSelector::Tenant("acme".to_string()),
                    ResourceSelector::ResourceClass(ResourceClass::BlobStore),
                ],
            ),
            CapabilityGrant::new(
                Capability::SharedMemory,
                vec![
                    ResourceSelector::Tenant("acme".to_string()),
                    ResourceSelector::ResourceClass(ResourceClass::SharedRegion),
                ],
            ),
            CapabilityGrant::new(
                Capability::HostQueue,
                vec![
                    ResourceSelector::Tenant("acme".to_string()),
                    ResourceSelector::ResourceClass(ResourceClass::HostQueue),
                ],
            ),
        ],
        dependencies: vec!["discovery".to_string()],
        readiness: ReadinessCondition::ActivityLogContains("guest ready".to_string()),
        tenant: Some("acme".to_string()),
        serving_role: None,
        handlers: Vec::new(),
    }
}

fn control_plane_wasm() -> Vec<u8> {
    read_wasm("selium-control-plane", "selium_control_plane.wasm")
}

/// The discovery system guest, exactly as the discovery integration test
/// deploys it: the guest is named `"discovery"` so bootstrap wires the feed
/// region and listener handle into its arguments and grants.
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
        serving_role: None,
        handlers: Vec::new(),
    }
}

fn discovery_wasm() -> Vec<u8> {
    read_wasm("selium-discovery", "selium_discovery.wasm")
}

/// Drains a guest's log channel and decodes each frame as a `LogRecord`.
fn drain_logs(runtime: &Runtime, process_id: u64) -> Vec<String> {
    runtime
        .kernel()
        .processes()
        .drain_log_channel(process_id)
        .expect("drain log channel")
        .iter()
        .map(|frame| {
            selium_service::log::LogRecord::decode(frame)
                .expect("decode log record")
                .message
        })
        .collect()
}

/// Golden path: external mTLS client → connector → bridge server →
/// bridge-channel rendezvous → control plane's served route, exchanging a
/// typed `deploy`/`status` round trip.
// The connector's quinn timers (loss detection, retransmit) sleep via the
// `Sleep` hostcall, whose host-side wake is driven by `tokio::spawn`, so the
// test provides a Tokio runtime.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the discovery, connector, bridge and control-plane guests built for wasm32-unknown-unknown"]
async fn external_client_reaches_control_plane_through_the_bridge() {
    let runtime = Runtime::default();
    seed_tls_blob_store(&runtime);

    let report = runtime
        .bootstrap_system_guests(RuntimeConfig {
            start_discovery: true,
            domain_table: Vec::new(),
            system_guests: vec![
                discovery_descriptor(discovery_wasm()),
                connector_descriptor(connector_wasm()),
                bridge_server_descriptor(bridge_wasm()),
                control_plane_descriptor(control_plane_wasm()),
            ],
        })
        .unwrap_or_else(|error| {
            // Best-effort diagnostics: every guest the activity log knows
            // about, its drained log channel (stopped guests have none), and
            // the activity itself.
            let activity = runtime.activity_log();
            let mut logs: Vec<String> = activity
                .iter()
                .map(|event| format!("activity: {:?}", event.message))
                .collect();
            for event in &activity {
                if let Some(process_id) = event.process_id
                    && let Ok(messages) = runtime.kernel().processes().drain_log_channel(process_id)
                {
                    logs.extend(messages.into_iter().filter_map(|frame| {
                        selium_service::log::LogRecord::decode(&frame)
                            .ok()
                            .map(|record| format!("pid {process_id}: {}", record.message))
                    }));
                }
            }
            panic!(
                "bootstrap discovery, connector, bridge and control-plane guests: {error}; \
                 state: {logs:#?}"
            )
        });

    let find = |name: &str| {
        report
            .guests
            .iter()
            .find(|guest| guest.name == name)
            .unwrap_or_else(|| panic!("bootstrap report contains guest {name}"))
            .process_id
    };
    let discovery = find("discovery");
    let connector = find("quic-connector");
    let bridge = find("bridge-server");
    let control_plane = find("control-plane");

    // The bridge server spawns one bridge-channel per client stream by
    // module id; register the real guest module under that id.
    runtime
        .register_module_bytes("bridge-channel-module".to_string(), bridge_channel_wasm())
        .expect("register bridge-channel module");

    // Both served routes are registered before the client connects
    // (readiness fires only after registration).
    assert!(
        runtime.has_registration(bridge, "sel://acme/bridge"),
        "bridge route registered"
    );
    assert!(
        runtime.has_registration(control_plane, CONTROL_URI),
        "control route registered"
    );

    // The connector's readiness is immediate; wait for its UDP listener
    // before connecting.
    let _ = wait_for_logs(
        &runtime,
        connector,
        &[("quic-connector: listening on", 1)],
        Duration::from_secs(30),
    );

    // Diagnostic collector: drains guest logs on a side thread so the
    // failure path never touches kernel locks (which can be held by a guest
    // executing inline on a poller thread).
    let collected: Arc<std::sync::Mutex<Vec<String>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
    let stop_collector = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let collector = {
        let runtime = runtime.clone();
        let collected = collected.clone();
        let stop = stop_collector.clone();
        std::thread::spawn(move || {
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                std::thread::sleep(Duration::from_millis(250));
                for process_id in [connector, bridge, control_plane] {
                    for message in drain_logs(&runtime, process_id) {
                        collected
                            .lock()
                            .expect("collector lock")
                            .push(format!("pid {process_id}: {message}"));
                    }
                }
            }
        })
    };
    let logs = || {
        let mut snapshot = collected.lock().expect("collector lock").clone();
        // Also drain any process the activity log knows about (notably the
        // bridge-channel children, whose pids the test never learns
        // directly); stopped processes have no channel and are skipped.
        for event in runtime.activity_log() {
            if let Some(process_id) = event.process_id
                && let Ok(messages) = runtime.kernel().processes().drain_log_channel(process_id)
            {
                snapshot.extend(messages.into_iter().filter_map(|frame| {
                    selium_service::log::LogRecord::decode(&frame)
                        .ok()
                        .map(|record| format!("pid {process_id}: {}", record.message))
                }));
            }
        }
        snapshot.sort();
        snapshot
    };
    let dump = || {
        let mut out = logs();
        out.extend(
            runtime
                .activity_log()
                .into_iter()
                .map(|event| format!("activity: {:?}", event.message)),
        );
        out
    };

    // TLS 1.3 handshake (with client identity) against the wasm connector.
    let connecting = selium_client::connect(
        CONNECTOR_ADDR.parse().expect("connector address"),
        client_options(),
    );
    let client = match tokio::time::timeout(Duration::from_secs(240), connecting).await {
        Ok(result) => result.expect("client connection"),
        Err(_elapsed) => panic!("handshake timed out; state: {:#?}", dump()),
    };

    // A typed RPC session to the control plane's served route, opened
    // through the bridge: the handshake names the control route, the
    // bridge-channel resolves it and rendezvouses, and the deterministic
    // accepted reply surfaces at open.
    let opening = client.rpc::<ControlRequest, ControlResponse>(CONTROL_URI);
    let mut rpc = match tokio::time::timeout(Duration::from_secs(120), opening).await {
        Ok(result) => result.expect("control route channel open"),
        Err(_elapsed) => panic!("channel open timed out; state: {:#?}", dump()),
    };

    // Typed deploy round trip: desired state is recorded (durable log +
    // projection) and the scheduler delegation returns its typed deferred
    // status.
    let deploying = rpc.request(ControlRequest::Deploy {
        workload_id: "api".to_string(),
        replicas: 3,
        module: "api/v1".to_string(),
    });
    let accepted = match tokio::time::timeout(Duration::from_secs(120), deploying).await {
        Ok(result) => result.expect("deploy round trip"),
        Err(_elapsed) => panic!("deploy timed out; guest logs: {:#?}", logs()),
    };
    assert_eq!(
        accepted,
        ControlResponse::Accepted {
            workload_id: "api".to_string(),
            replicas: 3,
            module: "api/v1".to_string(),
            delegated: DelegationStatus {
                step: "scheduler".to_string(),
                applied: false,
                context: SCHEDULER_DEFERRED.to_string(),
            },
        },
        "deploy accepted with the typed deferred scheduler status; guest logs: {:#?}",
        logs(),
    );

    // Typed status round trip over the same session: the projection returns
    // the last accepted desired state.
    let status = rpc
        .request(ControlRequest::Status {
            workload_id: "api".to_string(),
        })
        .await
        .expect("status round trip");
    assert_eq!(
        status,
        ControlResponse::Status {
            deployment: Some(Deployment {
                workload_id: "api".to_string(),
                replicas: 3,
                module: "api/v1".to_string(),
            }),
        },
        "status returns the recorded desired state; guest logs: {:#?}",
        logs(),
    );

    // No guest in the relay path logged a failure.
    stop_collector.store(true, std::sync::atomic::Ordering::Relaxed);
    collector.join().expect("collector thread");
    for (process_id, name) in [
        (discovery, "discovery"),
        (connector, "quic-connector"),
        (bridge, "bridge-server"),
        (control_plane, "control-plane"),
    ] {
        let logs = drain_logs(&runtime, process_id);
        assert!(
            !logs.iter().any(|message| message.contains("failed")),
            "{name} guest logged an error: {logs:?}"
        );
    }

    // Cleanup: the client's streams end, the bridge channels tear down, and
    // the system guests stop.
    drop(rpc);
    drop(client);
    runtime
        .stop_process(control_plane)
        .expect("stop control-plane");
    runtime.stop_process(bridge).expect("stop bridge-server");
    runtime.stop_process(connector).expect("stop connector");
    runtime.stop_process(discovery).expect("stop discovery");
}

/// Reads a guest's wasm module, preferring (and building) the release
/// profile: this test drives a TLS 1.3 handshake through the wasm
/// interpreter, which is too slow at debug optimization for the quinn
/// timeouts.
fn read_wasm(crate_name: &str, file_name: &str) -> Vec<u8> {
    common::read_guest_wasm(crate_name, file_name)
}

/// Provisions the connector's TLS material into the `tls-certs` blob store:
/// the bridge-route server certificate/key and the `acme` tenant's client
/// trust anchor list (mTLS is opt-in; without the anchor manifests the
/// connector would serve unauthenticated, and the bridge would refuse every
/// handoff).
fn seed_tls_blob_store(runtime: &Runtime) {
    let storage = runtime.kernel().storage();
    let store = storage.open_blob_store(&runtime.kernel().memory(), "tls-certs");

    let cert_id = storage
        .put_blob(store.local_id, BRIDGE_CERT_PEM.to_vec())
        .expect("put bridge cert blob");
    let key_id = storage
        .put_blob(store.local_id, BRIDGE_KEY_PEM.to_vec())
        .expect("put bridge key blob");
    let anchor_id = storage
        .put_blob(store.local_id, CLIENT_CERT_PEM.to_vec())
        .expect("put client anchor blob");
    let tenants_id = storage
        .put_blob(store.local_id, b"acme\n".to_vec())
        .expect("put anchor tenant list blob");

    storage
        .set_manifest(store.local_id, "cert-pem", cert_id)
        .expect("cert manifest");
    storage
        .set_manifest(store.local_id, "key-pem", key_id)
        .expect("key manifest");
    storage
        .set_manifest(store.local_id, "client-ca-acme", anchor_id)
        .expect("client anchor manifest");
    storage
        .set_manifest(store.local_id, "client-ca-tenants", tenants_id)
        .expect("client anchor tenant list manifest");
}

/// Polls a guest's log channel until every `(needle, count)` pair is
/// satisfied, then returns every drained message.
#[expect(clippy::panic, reason = "test helper")]
fn wait_for_logs(
    runtime: &Runtime,
    process_id: u64,
    needles: &[(&str, usize)],
    timeout: Duration,
) -> Vec<String> {
    let mut seen: Vec<String> = Vec::new();
    let start = Instant::now();
    while start.elapsed() < timeout {
        seen.extend(drain_logs(runtime, process_id));
        if needles.iter().all(|(needle, count)| {
            seen.iter()
                .filter(|message| message.contains(needle))
                .count()
                >= *count
        }) {
            return seen;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    panic!("timed out waiting for {needles:?} in guest log; got {seen:?}");
}
