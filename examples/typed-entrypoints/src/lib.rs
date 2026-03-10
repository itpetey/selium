//! Multiple typed Selium entrypoints funneled into one guest implementation.
//! The module proves that different entrypoint signatures arrive as normal typed Rust values.

use std::time::Duration;

use anyhow::{Context, Result, ensure};
use selium_abi::{DataValue, decode_rkyv, encode_rkyv};
use selium_guest::{io, time};

#[allow(dead_code)]
mod bindings;

use bindings::LaunchRecord;

const SEND_TIMEOUT_MS: u32 = 1_000;
const RECV_TIMEOUT_MS: u32 = 5_000;

#[selium_guest::entrypoint]
pub async fn start(bindings: DataValue) -> Result<()> {
    run_invocation(&bindings, "default-service", 1, "default-start").await
}

#[selium_guest::entrypoint]
pub async fn launch(bindings: DataValue, service: &str, retries: i32, mode: &str) -> Result<()> {
    run_invocation(&bindings, service, retries, mode).await
}

#[selium_guest::entrypoint]
pub async fn reconfigure(bindings: DataValue, service: &str, retries: i32) -> Result<()> {
    run_invocation(&bindings, service, retries, "reconfigure").await
}

async fn run_invocation(
    bindings: &DataValue,
    service: &str,
    retries: i32,
    mode: &str,
) -> Result<()> {
    // All entrypoints funnel through one helper so the example can show that Selium passes
    // typed arguments into different entrypoints without changing the guest-side logic.
    let bindings = encode_rkyv(bindings).context("encode launch managed-event bindings")?;
    let mut writer = io::managed_event_writer(&bindings, bindings::EVENT_LAUNCH_RECORDED, 9)
        .await
        .context("attach launch writer")?;
    let mut reader = io::managed_event_reader(&bindings, bindings::EVENT_LAUNCH_RECORDED)
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

async fn idle_forever() -> Result<()> {
    loop {
        time::sleep(Duration::from_secs(60))
            .await
            .context("sleep while idle")?;
    }
}
