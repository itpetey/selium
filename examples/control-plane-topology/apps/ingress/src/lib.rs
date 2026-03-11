//! Ingress app used by the control-plane topology example.
//! It performs a local self-check so the deployed module is functional even before cross-app routing exists.

use std::time::Duration;

use anyhow::{Context, Result};
use selium_abi::DataValue;
use selium_guest::{io, time};

#[allow(dead_code)]
mod bindings;

use bindings::Frame;

const SEND_TIMEOUT_MS: u32 = 1_000;

#[selium_guest::entrypoint]
pub async fn start(bindings: DataValue) -> Result<()> {
    let mut writer = io::managed_event_writer(&bindings, bindings::EVENT_INGEST_FRAMES, 1)
        .await
        .context("attach ingress managed-event writer")?;

    let frame = Frame {
        source: "ingress".to_string(),
        seq: 1,
    };
    writer
        .send_typed(&frame, SEND_TIMEOUT_MS)
        .await
        .context("send ingress frame 1")?;

    idle_forever().await
}

async fn idle_forever() -> Result<()> {
    loop {
        time::sleep(Duration::from_secs(60))
            .await
            .context("sleep while idle")?;
    }
}
