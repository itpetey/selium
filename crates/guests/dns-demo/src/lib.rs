//! DNS resolution demo guest.
//!
//! Attaches to the DNS connector's well-known channel, resolves a name, and
//! then connects to the resolved literal — exercising the resolution data
//! path end-to-end inside a real WASM guest. The `selium-runtime` `dns_spine`
//! integration test deploys this guest against a loopback fake resolver and
//! asserts on its log output.

use std::net::SocketAddr;

use selium_guest::{ResourceSender, TcpStream, entrypoint, error, info, mark_ready};
use selium_proto_dns::{DnsOutcome, DnsQuery, DnsRecordType, DnsResponse};
use selium_shm::rpc;

/// The name the demo resolves.
const DEMO_NAME: &str = "example.test";

#[entrypoint]
async fn resolve_demo(connector: u64, connect: (u64, u64)) {
    drop(selium_guest::log::init());
    info!("dns-demo: booting");

    // Attach to the DNS connector's channel and ask it to resolve the name.
    let sender = match ResourceSender::attach(connector) {
        Ok(sender) => sender,
        Err(e) => {
            error!("dns-demo: attach to connector failed: {e}");
            return;
        }
    };

    let mut client = match rpc::connect::<DnsQuery, DnsResponse, _>(sender, 0, 0).await {
        Ok(client) => client,
        Err(e) => {
            error!("dns-demo: connect to connector failed: {e}");
            return;
        }
    };

    let response = match client
        .request(DnsQuery::from_str(DEMO_NAME, DnsRecordType::A))
        .await
    {
        Ok(response) => response,
        Err(e) => {
            error!("dns-demo: resolve request failed: {e}");
            return;
        }
    };

    if response.outcome != DnsOutcome::Ok {
        error!("dns-demo: unexpected outcome {:?}", response.outcome);
        return;
    }

    for address in &response.addresses {
        info!("resolved {} -> {}", DEMO_NAME, address);
    }

    // Then connect to the resolved literal (the name's A record points at
    // loopback; the TCP test server listens on that address).
    let connect_addr = match read_connect_addr(connect) {
        Some(addr) => addr,
        None => {
            error!("dns-demo: invalid connect address argument");
            return;
        }
    };

    match TcpStream::connect(&connect_addr.to_string()).await {
        Ok(_stream) => info!("connected to {}", connect_addr),
        Err(e) => error!("dns-demo: connect failed: {e}"),
    }

    mark_ready();
}

/// Reads the target `ip:port` from a pointer argument.
fn read_connect_addr(connect: (u64, u64)) -> Option<SocketAddr> {
    // SAFETY: the `(address, length)` pair was written into this guest's
    // linear memory by the runtime for this entrypoint invocation.
    let text = unsafe { selium_guest::args::str(connect.0, connect.1) }?;
    text.trim().parse().ok()
}
