use std::time::Duration;

use anyhow::{Context, Result, ensure};
use selium_abi::{decode_rkyv, encode_rkyv};
use selium_guest::{io, time};

#[allow(dead_code)]
mod bindings;

use bindings::LaunchRecord;

const FRAME_BYTES: u32 = 512;
const SEND_TIMEOUT_MS: u32 = 1_000;
const RECV_TIMEOUT_MS: u32 = 5_000;

#[selium_guest::entrypoint]
pub async fn start() -> Result<()> {
    run_invocation("default-service", 1, "default-start").await
}

#[selium_guest::entrypoint]
pub async fn launch(service: &str, retries: i32, mode: &str) -> Result<()> {
    run_invocation(service, retries, mode).await
}

#[selium_guest::entrypoint]
pub async fn reconfigure(service: &str, retries: i32) -> Result<()> {
    run_invocation(service, retries, "reconfigure").await
}

async fn run_invocation(service: &str, retries: i32, mode: &str) -> Result<()> {
    // All entrypoints funnel through one helper so the example can show that Selium passes
    // typed arguments into different entrypoints without changing the guest-side logic.
    let channel = io::create_channel(16, FRAME_BYTES)
        .await
        .context("create launch channel")?;
    let mut writer = io::attach_writer(&descriptor(channel.queue_shared_id), 9)
        .await
        .context("attach launch writer")?;
    let mut reader = io::attach_reader(&descriptor(channel.queue_shared_id))
        .await
        .context("attach launch reader")?;

    let record = LaunchRecord {
        service: service.to_string(),
        retries,
        mode: mode.to_string(),
    };
    // The round-trip check makes the entrypoint arguments visible as normal contract-defined
    // data after they cross the ABI boundary into guest code.
    writer
        .send(
            &encode_rkyv(&record).context("encode launch record")?,
            SEND_TIMEOUT_MS,
        )
        .await
        .context("send launch record")?;

    let frame = reader
        .recv(RECV_TIMEOUT_MS)
        .await
        .context("receive launch record")?
        .context("missing launch record frame")?;
    let observed = decode_rkyv::<LaunchRecord>(&frame.payload).context("decode launch record")?;
    ensure!(
        observed.service == record.service
            && observed.retries == record.retries
            && observed.mode == record.mode,
        "typed entrypoint record mismatch"
    );

    idle_forever().await
}

fn descriptor(shared_id: u64) -> io::ChannelDescriptor {
    io::ChannelDescriptor {
        queue_shared_id: shared_id,
        max_frame_bytes: FRAME_BYTES,
    }
}

async fn idle_forever() -> Result<()> {
    loop {
        time::sleep(Duration::from_secs(60))
            .await
            .context("sleep while idle")?;
    }
}
