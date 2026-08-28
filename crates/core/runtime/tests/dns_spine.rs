//! DNS resolution spine test.
//!
//! Deploys the real `selium-connector-dns` and `selium-dns-demo` WASM guests
//! against a loopback fake DNS resolver and asserts the resolution data path
//! end-to-end: the demo guest resolves `example.test` through the connector
//! (typed `DnsQuery` → UDP/53 → typed `DnsResponse`) and then connects to the
//! resolved literal.
//!
//! `#[ignore]`d by default because it requires both guests built for
//! `wasm32-unknown-unknown` first:
//!
//! ```sh
//! cargo build --target wasm32-unknown-unknown -p selium-connector-dns -p selium-dns-demo
//! cargo test -p selium-runtime --test dns_spine -- --ignored
//! ```

use std::{
    path::PathBuf,
    time::{Duration, Instant},
};

use selium_abi::{Capability, CapabilityGrant, ResourceClass, ResourceIdentity, ResourceSelector};
use selium_encoding::FlatMsg;
use selium_proto_dns::RESOLVE_URI;
use selium_runtime::{ReadinessCondition, Runtime, SystemGuestArg, SystemGuestDescriptor};

fn drain_logs(runtime: &Runtime, process_id: u64) -> Vec<String> {
    runtime
        .kernel()
        .processes()
        .drain_log_channel(process_id)
        .expect("drain log channel")
        .iter()
        .map(|frame| {
            selium_encoding::log::LogRecord::decode(frame)
                .expect("decode log record")
                .message
        })
        .collect()
}

// The connector's resolve timeout uses the guest `Timer` (a `Sleep` hostcall),
// whose host-side wake is driven by `tokio::spawn`, so the test provides a
// Tokio runtime.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the connector and demo guests built for wasm32-unknown-unknown"]
async fn guest_resolves_via_connector_then_connects_by_literal() {
    let runtime = Runtime::default();

    // The connector's well-known channel is provisioned by the runtime at
    // spawn time (host listener queue + leading entrypoint argument +
    // discovery registration), exactly like the discovery listener.
    let resolver_addr = spawn_fake_dns_resolver();
    // A loopback TCP server the demo connects to after resolving.
    let tcp_addr = spawn_fake_tcp_server();

    let connector = runtime
        .spawn_system_guest(connector_descriptor(
            connector_wasm(),
            format!("udp://{resolver_addr}"),
        ))
        .expect("bootstrap dns connector");
    let connector_listener_id = connector
        .well_known_listener
        .expect("runtime provisions the connector's well-known listener");

    let demo = runtime
        .spawn_system_guest(demo_descriptor(
            demo_wasm(),
            connector_listener_id,
            tcp_addr.to_string(),
        ))
        .expect("bootstrap dns demo");

    // The demo resolves the name through the connector and then connects to
    // the literal; both surface as guest log markers.
    wait_for_logs(
        &runtime,
        demo.process_id,
        &[
            "resolved example.test -> 127.0.0.1",
            "connected to 127.0.0.1",
        ],
        Duration::from_secs(10),
    );

    let logs = drain_logs(&runtime, demo.process_id);
    assert!(
        !logs.iter().any(|message| message.contains("failed")),
        "demo guest logged an error: {logs:?}"
    );

    runtime.stop_process(demo.process_id).expect("stop demo");
    runtime
        .stop_process(connector.process_id)
        .expect("stop connector");
}

/// A loopback UDP server that answers every A query for `example.test` with
/// `127.0.0.1`, echoing the client's transaction id.
fn spawn_fake_dns_resolver() -> std::net::SocketAddr {
    let socket = std::net::UdpSocket::bind("127.0.0.1:0").expect("bind fake resolver");
    socket
        .set_read_timeout(Some(Duration::from_secs(20)))
        .expect("resolver timeout");
    let addr = socket.local_addr().expect("resolver addr");

    std::thread::spawn(move || {
        let mut buf = [0u8; 512];
        while let Ok((len, src)) = socket.recv_from(&mut buf) {
            let Some(query) = buf.get(..len) else {
                continue;
            };
            if query.len() < 12 {
                continue;
            }
            let (Some(&txid_hi), Some(&txid_lo)) = (query.first(), query.get(1)) else {
                continue;
            };
            let Some(question) = query.get(12..) else {
                continue;
            };

            // Build a response: header (same id, QR|RD|RA, qd=1, an=1) +
            // echoed question + one A record for 127.0.0.1.
            let mut response = Vec::with_capacity(query.len() + 16);
            response.extend_from_slice(&[txid_hi, txid_lo]);
            response.extend_from_slice(&[0x81, 0x80]);
            response.extend_from_slice(&1u16.to_be_bytes()); // qdcount
            response.extend_from_slice(&1u16.to_be_bytes()); // ancount
            response.extend_from_slice(&[0, 0, 0, 0]); // nscount, arcount
            response.extend_from_slice(question); // echo question
            response.extend_from_slice(&[0xC0, 0x0C]); // name compression pointer
            response.extend_from_slice(&1u16.to_be_bytes()); // type A
            response.extend_from_slice(&1u16.to_be_bytes()); // class IN
            response.extend_from_slice(&60u32.to_be_bytes()); // ttl
            response.extend_from_slice(&4u16.to_be_bytes()); // rdlength
            response.extend_from_slice(&[127, 0, 0, 1]); // 127.0.0.1

            drop(socket.send_to(&response, src));
        }
    });

    addr
}

/// A loopback TCP server that accepts a single connection and drops it.
fn spawn_fake_tcp_server() -> std::net::SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind fake tcp server");
    let addr = listener.local_addr().expect("tcp addr");

    std::thread::spawn(move || {
        if let Ok((stream, _)) = listener.accept() {
            drop(stream);
        }
    });

    addr
}

fn connector_descriptor(module_bytes: Vec<u8>, resolver: String) -> SystemGuestDescriptor {
    SystemGuestDescriptor {
        name: "dns-connector".to_string(),
        module_id: "dns-connector-module".to_string(),
        module_bytes,
        entrypoint: "dns_connector".to_string(),
        // Only the resolver pointer argument: the runtime injects the
        // well-known listener as the leading integer argument.
        arguments: vec![SystemGuestArg::Pointer(resolver.into_bytes())],
        grants: vec![
            CapabilityGrant::new(
                Capability::Network,
                vec![ResourceSelector::ResourceClass(ResourceClass::UdpSocket)],
            ),
            CapabilityGrant::new(
                Capability::SharedMemory,
                vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
            ),
        ],
        dependencies: Vec::new(),
        readiness: ReadinessCondition::Immediate,
        tenant: None,
        well_known_uri: Some(RESOLVE_URI.to_string()),
    }
}

fn demo_descriptor(
    module_bytes: Vec<u8>,
    connector: u64,
    connect: String,
) -> SystemGuestDescriptor {
    SystemGuestDescriptor {
        name: "dns-demo".to_string(),
        module_id: "dns-demo-module".to_string(),
        module_bytes,
        entrypoint: "resolve_demo".to_string(),
        arguments: vec![
            SystemGuestArg::Integer(connector),
            SystemGuestArg::Pointer(connect.into_bytes()),
        ],
        grants: vec![
            CapabilityGrant::new(
                Capability::Network,
                vec![ResourceSelector::ResourceClass(ResourceClass::TcpStream)],
            ),
            CapabilityGrant::new(
                Capability::SharedMemory,
                vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
            ),
            CapabilityGrant::new(
                Capability::HostQueue,
                vec![ResourceSelector::ExplicitResource(
                    ResourceIdentity::Shared(connector),
                )],
            ),
        ],
        dependencies: Vec::new(),
        readiness: ReadinessCondition::Immediate,
        tenant: None,
        well_known_uri: None,
    }
}

#[expect(
    clippy::panic,
    reason = "missing build artifact is a hard test failure"
)]
fn read_wasm(crate_name: &str, file_name: &str) -> Vec<u8> {
    let target_dir =
        std::env::var("CARGO_TARGET_DIR").unwrap_or_else(|_e| "../../../target".to_string());
    let path = PathBuf::from(target_dir).join(file_name);
    std::fs::read(&path).unwrap_or_else(|_error| {
        panic!(
            "{crate_name} guest not found at {}.\n\
             Build it first:\n  \
             cargo build --target wasm32-unknown-unknown -p {crate_name}",
            path.display()
        )
    })
}

fn connector_wasm() -> Vec<u8> {
    read_wasm(
        "selium-connector-dns",
        "wasm32-unknown-unknown/debug/selium_connector_dns.wasm",
    )
}

fn demo_wasm() -> Vec<u8> {
    read_wasm(
        "selium-dns-demo",
        "wasm32-unknown-unknown/debug/selium_dns_demo.wasm",
    )
}

/// Polls the guest log channel until every needle appears in the drained
/// messages.
#[expect(clippy::panic, reason = "test helper")]
fn wait_for_logs(
    runtime: &Runtime,
    process_id: u64,
    needles: &[&str],
    timeout: Duration,
) -> Duration {
    let mut seen: Vec<String> = Vec::new();
    let start = Instant::now();
    while start.elapsed() < timeout {
        seen.extend(drain_logs(runtime, process_id));
        if needles
            .iter()
            .all(|needle| seen.iter().any(|message| message.contains(needle)))
        {
            return start.elapsed();
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    panic!("timed out waiting for {needles:?} in guest log; got {seen:?}");
}
