//! Sink app used by the control-plane topology example.
//! It mirrors the other apps with a local contract-level self-check before idling under control-plane management.

use std::time::Duration;

use anyhow::{Context, Result, ensure};
use selium_abi::DataValue;
use selium_guest::{io, time};

#[allow(dead_code)]
mod bindings;

use bindings::EnrichedFrame;

const RECV_TIMEOUT_MS: u32 = 1_000;

#[selium_guest::entrypoint]
pub async fn start(bindings: DataValue) -> Result<()> {
    let mut reader = io::managed_event_reader(&bindings, bindings::EVENT_PROCESS_ENRICHED)
        .await
        .context("attach sink enriched reader")?;

    let decoded = loop {
        let Some(frame) = reader
            .recv_typed::<EnrichedFrame>(RECV_TIMEOUT_MS)
            .await
            .context("receive enriched frame")?
        else {
            continue;
        };
        break frame.payload;
    };
    ensure!(
        decoded.source == "ingress" && decoded.seq > 0 && decoded.stage == "normalized",
        "unexpected enriched frame: source={} seq={} stage={}",
        decoded.source,
        decoded.seq,
        decoded.stage,
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
