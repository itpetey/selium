//! HTTPS RPC loopback example for the Selium guest network API.

use std::{collections::BTreeMap, future::Future, time::Duration};

use anyhow::{Context, Result, anyhow, ensure};
use selium_abi::{
    NetworkRpcRequest, NetworkRpcRequestHead, NetworkRpcResponse, NetworkRpcResponseHead,
};
use selium_guest::{network, spawn, time};

const BINDING_NAME: &str = "example-http-loopback";
const PROFILE_NAME: &str = "example-http-loopback";
const AUTHORITY: &str = "127.0.0.1:7443@localhost";
const ACCEPT_TIMEOUT_MS: u32 = 100;
const IO_TIMEOUT_MS: u32 = 5_000;

#[selium_guest::entrypoint]
pub async fn start() -> Result<()> {
    let listener = network::listen(BINDING_NAME)
        .await
        .context("listen on HTTPS loopback binding")?;
    spawn_checked("http rpc server", run_server(listener));

    time::sleep(Duration::from_millis(100))
        .await
        .context("wait for HTTPS listener to start")?;

    let session = network::http::connect(PROFILE_NAME, AUTHORITY)
        .await
        .context("connect HTTPS loopback session")?;
    let response = network::rpc::invoke(
        &session,
        NetworkRpcRequest {
            head: NetworkRpcRequestHead {
                method: "POST".to_string(),
                path: "/echo".to_string(),
                metadata: BTreeMap::from([("content-type".to_string(), "text/plain".to_string())]),
            },
            body: b"hello over http".to_vec(),
        },
        IO_TIMEOUT_MS,
    )
    .await
    .context("invoke HTTPS RPC")?
    .ok_or_else(|| anyhow!("HTTPS RPC invocation would block"))?;

    ensure!(response.head.status == 200, "unexpected HTTP status");
    ensure!(
        response.body == b"http echo: hello over http".to_vec(),
        "unexpected HTTP response body"
    );

    session
        .close()
        .await
        .context("close HTTPS client session")?;
    idle_forever().await
}

async fn run_server(listener: network::Listener) -> Result<()> {
    loop {
        let Some(session) = listener
            .accept(ACCEPT_TIMEOUT_MS)
            .await
            .context("accept HTTPS session")?
        else {
            continue;
        };
        spawn_checked("http rpc session", handle_session(session));
    }
}

async fn handle_session(session: network::Session) -> Result<()> {
    loop {
        let exchange = match network::rpc::accept(&session, ACCEPT_TIMEOUT_MS).await {
            Ok(Some(exchange)) => exchange,
            Ok(None) => continue,
            Err(_) => return Ok(()),
        };

        let request = exchange
            .buffered_request(4_096, IO_TIMEOUT_MS)
            .await
            .context("read HTTP RPC request")?;
        ensure!(request.head.method == "POST", "unexpected HTTP method");
        ensure!(request.head.path.ends_with("/echo"), "unexpected HTTP path");

        let body = String::from_utf8(request.body).context("decode HTTP request body")?;
        exchange
            .respond(
                NetworkRpcResponse {
                    head: NetworkRpcResponseHead {
                        status: 200,
                        metadata: BTreeMap::from([(
                            "content-type".to_string(),
                            "text/plain".to_string(),
                        )]),
                    },
                    body: format!("http echo: {body}").into_bytes(),
                },
                IO_TIMEOUT_MS,
            )
            .await
            .context("write HTTP RPC response")?;
    }
}

fn spawn_checked<F>(name: &'static str, future: F)
where
    F: Future<Output = Result<()>> + 'static,
{
    spawn(async move {
        if let Err(err) = future.await {
            panic!("{name} failed: {err:#}");
        }
    });
}

async fn idle_forever() -> Result<()> {
    loop {
        time::sleep(Duration::from_secs(60))
            .await
            .context("sleep while HTTP example is idle")?;
    }
}
