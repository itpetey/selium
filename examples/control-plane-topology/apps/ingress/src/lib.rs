//! Ingress app used by the control-plane topology example.
//! It performs a local self-check so the deployed module is functional even before cross-app routing exists.

use std::time::Duration;

use anyhow::{Context, Result};
use selium_abi::{DataValue, encode_rkyv};
use selium_guest::{io, time};

#[allow(dead_code)]
mod bindings;

use bindings::Frame;

const FRAME_BYTES: u32 = 512;
const SEND_TIMEOUT_MS: u32 = 1_000;
const SEND_INTERVAL: Duration = Duration::from_millis(250);

#[selium_guest::entrypoint]
pub async fn start(bindings: DataValue) -> Result<()> {
    let bindings = encode_rkyv(&bindings).context("encode ingress managed-event bindings")?;
    let mut writer = io::managed_event_writer(&bindings, bindings::EVENT_INGEST_FRAMES, 1)
        .await
        .context("attach ingress managed-event writer")?;

    let mut seq = 1_u32;
    loop {
        let frame = Frame {
            source: "ingress".to_string(),
            seq,
        };
        writer
            .send(
                &encode_rkyv(&frame).context("encode ingress frame")?,
                SEND_TIMEOUT_MS,
            )
            .await
            .with_context(|| format!("send ingress frame {seq}"))?;
        seq = seq.saturating_add(1);
        time::sleep(SEND_INTERVAL)
            .await
            .context("sleep between ingress frames")?;
    }
}
