//! Minimal request/reply RPC inside one Selium guest module.
//! Startup proves the request path and response path both work before the module idles.

use std::{future::Future, time::Duration};

use anyhow::{Context, Result, anyhow, ensure};
use selium_abi::{decode_rkyv, encode_rkyv};
use selium_guest::{io, spawn, time};

#[allow(dead_code)]
mod bindings;

use bindings::{EchoRequest, EchoResponse};

const FRAME_BYTES: u32 = 512;
const SEND_TIMEOUT_MS: u32 = 1_000;
const RECV_TIMEOUT_MS: u32 = 5_000;

#[selium_guest::entrypoint]
pub async fn start() -> Result<()> {
    // The client and server share two explicit channels so the RPC request path and
    // reply path are visible in the example instead of being hidden behind a helper.
    let requests = io::create_channel(16, FRAME_BYTES)
        .await
        .context("create request channel")?;
    let responses = io::create_channel(16, FRAME_BYTES)
        .await
        .context("create response channel")?;

    spawn_checked(
        "rpc server",
        run_server(requests.queue_shared_id, responses.queue_shared_id),
    );

    let mut request_writer = io::attach_writer(&descriptor(requests.queue_shared_id), 1)
        .await
        .context("attach request writer")?;
    let mut response_reader = io::attach_reader(&descriptor(responses.queue_shared_id))
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

async fn run_server(requests_shared_id: u64, responses_shared_id: u64) -> Result<()> {
    let mut request_reader = io::attach_reader(&descriptor(requests_shared_id))
        .await
        .context("attach request reader")?;
    let mut response_writer = io::attach_writer(&descriptor(responses_shared_id), 41)
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

fn descriptor(shared_id: u64) -> io::ChannelDescriptor {
    // Examples pass shared queue ids around and rebuild the descriptor at the attachment site.
    io::ChannelDescriptor {
        queue_shared_id: shared_id,
        max_frame_bytes: FRAME_BYTES,
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
