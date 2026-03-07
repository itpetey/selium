//! QUIC stream loopback example for the Selium guest network API.

use std::{future::Future, time::Duration};

use anyhow::{Context, Result, ensure};
use selium_guest::{network, spawn, time};

#[allow(dead_code)]
mod bindings;

use bindings::{EchoChunk, quic_echo};

const BINDING_NAME: &str = "example-quic-loopback";
const PROFILE_NAME: &str = "example-quic-loopback";
const AUTHORITY: &str = "127.0.0.1:7400@localhost";
const ACCEPT_TIMEOUT_MS: u32 = 100;
const IO_TIMEOUT_MS: u32 = 5_000;
const CHUNK_BYTES: u32 = 4_096;

#[selium_guest::entrypoint]
pub async fn start() -> Result<()> {
    let listener = network::listen(BINDING_NAME)
        .await
        .context("listen on QUIC loopback binding")?;
    spawn_checked("quic stream server", run_server(listener));

    time::sleep(Duration::from_millis(100))
        .await
        .context("wait for QUIC listener to start")?;

    let session = network::quic::connect(PROFILE_NAME, AUTHORITY)
        .await
        .context("connect QUIC loopback session")?;
    let stream = wait_for_open_stream(&session).await?;

    let payload = EchoChunk {
        payload: b"hello over quic".to_vec(),
    };
    quic_echo::send(&stream, &payload, IO_TIMEOUT_MS)
        .await
        .context("send QUIC request")?;
    let response = quic_echo::recv(&stream, CHUNK_BYTES, IO_TIMEOUT_MS)
        .await
        .context("decode QUIC response")?;
    ensure!(response == payload, "unexpected QUIC response body");

    stream.close().await.context("close QUIC client stream")?;
    session.close().await.context("close QUIC client session")?;
    idle_forever().await
}

async fn run_server(listener: network::Listener) -> Result<()> {
    loop {
        let Some(session) = listener
            .accept(ACCEPT_TIMEOUT_MS)
            .await
            .context("accept QUIC session")?
        else {
            continue;
        };
        spawn_checked("quic session", handle_session(session));
    }
}

async fn handle_session(session: network::Session) -> Result<()> {
    loop {
        let stream = match quic_echo::accept(&session, ACCEPT_TIMEOUT_MS).await {
            Ok(Some(stream)) => stream,
            Ok(None) => continue,
            Err(_) => return Ok(()),
        };

        let request = quic_echo::recv(&stream, CHUNK_BYTES, IO_TIMEOUT_MS)
            .await
            .context("decode QUIC request")?;
        quic_echo::send(&stream, &request, IO_TIMEOUT_MS)
            .await
            .context("send QUIC response")?;
        stream.close().await.context("close QUIC server stream")?;
    }
}

async fn wait_for_open_stream(session: &network::Session) -> Result<network::StreamChannel> {
    loop {
        if let Some(stream) = network::stream::open(session)
            .await
            .context("open QUIC stream")?
        {
            return Ok(stream);
        }
        time::sleep(Duration::from_millis(25))
            .await
            .context("wait for QUIC stream availability")?;
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
            .context("sleep while QUIC example is idle")?;
    }
}
