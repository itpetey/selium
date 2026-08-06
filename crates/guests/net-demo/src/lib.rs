//! Network demo guest — smoke test for WASM hostcall path.

use selium_guest::{TcpListener, entrypoint, info};

#[entrypoint]
async fn net_demo() {
    drop(selium_guest::log::init());
    info!("net-demo started");

    // Test 1: Try TcpListener::bind
    match TcpListener::bind("127.0.0.1:0") {
        Ok(_listener) => {
            info!("net-demo: tcp bind ok");
        }
        Err(e) => {
            selium_guest::error!("tcp bind failed: {e}");
            selium_guest::mark_ready();
            return;
        }
    }

    info!("net-demo: done");
    selium_guest::mark_ready();
}
