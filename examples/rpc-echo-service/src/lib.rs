//! Minimal request/reply RPC inside one Selium guest module.
//! Startup proves the request path and response path both work before the module idles.

use std::{future::Future, time::Duration};

use anyhow::{Context, Result, anyhow, ensure};
use selium_abi::{DataValue, decode_rkyv, encode_rkyv};
use selium_guest::{io, spawn, time};

#[allow(dead_code)]
mod bindings;

use bindings::{EchoRequest, EchoResponse};

const SEND_TIMEOUT_MS: u32 = 1_000;
const RECV_TIMEOUT_MS: u32 = 5_000;

#[selium_guest::entrypoint]
pub async fn start(bindings: DataValue) -> Result<()> {
    let bindings = encode_rkyv(&bindings).context("encode rpc managed-event bindings")?;

    // The client and server still show the request path and reply path explicitly, but now
    // they bind to contract-defined public endpoints instead of guest-created queues.
    spawn_checked("rpc server", run_server(bindings.clone()));

    let mut request_writer = io::managed_event_writer(&bindings, bindings::EVENT_ECHO_REQUESTED, 1)
        .await
        .context("attach request writer")?;
    let mut response_reader = io::managed_event_reader(&bindings, bindings::EVENT_ECHO_RESPONDED)
        .await
        .context("attach response reader")?;

    let request = EchoRequest {
        correlation_id: 7,
        body: "hello from selium".to_string(),
    };
    request_writer
        .send(
            &encode_rkyv(&request).context("encode request")?,
            SEND_TIMEOUT_MS,
        )
        .await
        .context("send request")?;

    let frame = response_reader
        .recv(RECV_TIMEOUT_MS)
        .await
        .context("receive response")?
        .ok_or_else(|| anyhow!("rpc response timed out"))?;
    let response = decode_rkyv::<EchoResponse>(&frame.payload).context("decode response")?;

    ensure!(
        response.correlation_id == request.correlation_id,
        "unexpected correlation id in response"
    );
    ensure!(
        response.echoed == request.body,
        "echo response body mismatch"
    );

    idle_forever().await
}

async fn run_server(bindings: Vec<u8>) -> Result<()> {
    let mut request_reader = io::managed_event_reader(&bindings, bindings::EVENT_ECHO_REQUESTED)
        .await
        .context("attach request reader")?;
    let mut response_writer =
        io::managed_event_writer(&bindings, bindings::EVENT_ECHO_RESPONDED, 41)
            .await
            .context("attach response writer")?;

    loop {
        let Some(frame) = request_reader
            .recv(RECV_TIMEOUT_MS)
            .await
            .context("receive request")?
        else {
            continue;
        };

        let request = decode_rkyv::<EchoRequest>(&frame.payload).context("decode request")?;
        // The contract-generated types are plain Rust structs once serialized over the queue.
        let response = EchoResponse {
            correlation_id: request.correlation_id,
            echoed: request.body,
        };

        response_writer
            .send(
                &encode_rkyv(&response).context("encode response")?,
                SEND_TIMEOUT_MS,
            )
            .await
            .context("send response")?;
    }
}

fn spawn_checked<F>(name: &'static str, future: F)
where
    F: Future<Output = Result<()>> + 'static,
{
    // Background tasks panic the whole module on failure so startup never reports success
    // while one half of the example flow has already died.
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
            .context("sleep while idle")?;
    }
}
