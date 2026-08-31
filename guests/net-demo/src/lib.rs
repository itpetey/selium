//! Network demo guest — event-driven proxy integration fixture.
//!
//! Used by the `selium-runtime` `net_wake` integration test to verify the
//! WaitRegister wake bridge and the stall kick end-to-end through a real
//! WASM guest:
//!
//! 1. Binds a listener (the test discovers the bound port via the runtime).
//! 2. Accepts one connection and parks a read on its inbound ring — this
//!    issues a `WaitRegister` hostcall for the parked task.
//! 3. When data arrives, logs `read done`, echoes the bytes back on the
//!    outbound ring, then parks on a second read so the reactor stalls
//!    right after having written outbound frames.

use selium_guest::{
    entrypoint, error, info,
    net::tcp::{TcpListener, TcpStream},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[entrypoint]
async fn net_demo() {
    drop(selium_guest::log::init());
    info!("net-demo started");

    let listener = match TcpListener::bind("127.0.0.1:0") {
        Ok(listener) => listener,
        Err(e) => {
            error!("net-demo: bind failed: {e}");
            return;
        }
    };
    // Readiness anchor for the integration test.
    info!("net-demo: bound");

    let mut stream: TcpStream = match listener.accept().await {
        Ok(stream) => {
            info!("net-demo: accepted");
            stream
        }
        Err(e) => {
            error!("net-demo: accept failed: {e}");
            return;
        }
    };

    // Park on the inbound ring until the test writes request bytes. The
    // wake must come from the host's WaitRegister/mailbox bridge, not from
    // any guest-side polling.
    let mut buf = [0_u8; 64];
    match stream.read(&mut buf).await {
        Ok(0) => {
            error!("net-demo: unexpected EOF before request");
            return;
        }
        Ok(n) => {
            info!("net-demo: read done ({n} bytes)");
            let chunk = match buf.get(..n) {
                Some(chunk) => chunk,
                None => {
                    error!("net-demo: read returned out-of-bounds length {n}");
                    return;
                }
            };
            if let Err(e) = stream.write_all(chunk).await {
                error!("net-demo: echo write failed: {e}");
                return;
            }
            drop(stream.flush().await);
        }
        Err(e) => {
            error!("net-demo: read failed: {e}");
            return;
        }
    }

    info!("net-demo: echoed");

    // Stall the reactor immediately after writing outbound frames: the
    // runtime's stall kick (not the bounded backstop) must drain them.
    let mut buf2 = [0_u8; 64];
    match stream.read(&mut buf2).await {
        Ok(n) => info!("net-demo: second read done ({n} bytes)"),
        Err(e) => error!("net-demo: second read failed: {e}"),
    }
}
