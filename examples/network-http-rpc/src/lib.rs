//! HTTPS RPC loopback example for the Selium guest network API.

use std::{future::Future, time::Duration};

use anyhow::{Context, Result, anyhow, ensure};
use selium_guest::{network, spawn, time};

#[allow(dead_code)]
mod bindings;

use bindings::{UploadReceipt, UploadRequest, upload};

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

    let upload = UploadRequest {
        file_name: "demo.txt".to_string(),
        content_type: "text/plain".to_string(),
    };
    let session = network::http::connect(PROFILE_NAME, AUTHORITY)
        .await
        .context("connect HTTPS loopback session")?;
    let client = upload::Client::new(&session);
    let pending = client
        .start(&upload, IO_TIMEOUT_MS)
        .await
        .context("start HTTPS upload RPC")?
        .ok_or_else(|| anyhow!("HTTPS upload RPC would block"))?;
    pending
        .request_body()
        .send(b"hello ".to_vec(), false, IO_TIMEOUT_MS)
        .await
        .context("send first upload chunk")?;
    pending
        .request_body()
        .send(b"over http".to_vec(), true, IO_TIMEOUT_MS)
        .await
        .context("send final upload chunk")?;
    let receipt: UploadReceipt = pending
        .await_buffered_response(4_096, IO_TIMEOUT_MS)
        .await
        .context("await HTTPS upload response")?
        .ok_or_else(|| anyhow!("HTTPS upload response would block"))?;
    ensure!(
        receipt.file_name == upload.file_name,
        "unexpected uploaded file name"
    );
    ensure!(
        receipt.bytes_received == 15,
        "unexpected uploaded byte count"
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
        let exchange = match upload::accept(&session, ACCEPT_TIMEOUT_MS).await {
            Ok(Some(exchange)) => exchange,
            Ok(None) => continue,
            Err(_) => return Ok(()),
        };

        let upload = exchange.request().clone();
        ensure!(
            upload.content_type == "text/plain",
            "unexpected uploaded content type"
        );
        let bytes_received = read_body_bytes(exchange.request_body()).await?;
        let receipt = UploadReceipt {
            file_name: upload.file_name,
            bytes_received,
        };
        exchange
            .respond_buffered(&receipt, IO_TIMEOUT_MS)
            .await
            .context("write HTTP RPC response")?;
    }
}

async fn read_body_bytes(body: &network::BodyReader) -> Result<u64> {
    let mut bytes_received = 0_u64;
    loop {
        let Some(chunk) = body
            .recv(4_096, IO_TIMEOUT_MS)
            .await
            .context("read HTTP request body chunk")?
        else {
            continue;
        };
        bytes_received += chunk.bytes.len() as u64;
        if chunk.finish {
            return Ok(bytes_received);
        }
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
