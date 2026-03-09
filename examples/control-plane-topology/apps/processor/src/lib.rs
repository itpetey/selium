//! Processor app used by the control-plane topology example.
//! Today it validates its own contract types locally while the control plane models the deployment graph.

use std::time::Duration;

use anyhow::{Context, Result};
use selium_abi::{DataValue, decode_rkyv, encode_rkyv};
use selium_guest::{io, time};

#[allow(dead_code)]
mod bindings;

use bindings::{EnrichedFrame, Frame};

const FRAME_BYTES: u32 = 512;
const SEND_TIMEOUT_MS: u32 = 1_000;
const RECV_TIMEOUT_MS: u32 = 1_000;

#[selium_guest::entrypoint]
pub async fn start(bindings: DataValue) -> Result<()> {
    let bindings = encode_rkyv(&bindings).context("encode processor managed-event bindings")?;
    let mut reader = io::managed_event_reader(&bindings, bindings::EVENT_INGEST_FRAMES)
        .await
        .context("attach processor ingest reader")?;
    let mut writer = io::managed_event_writer(&bindings, bindings::EVENT_PROCESS_ENRICHED, 2)
        .await
        .context("attach processor enriched writer")?;

    loop {
        let Some(frame) = reader
            .recv(RECV_TIMEOUT_MS)
            .await
            .context("receive ingest frame")?
        else {
            continue;
        };
        let frame = decode_rkyv::<Frame>(&frame.payload).context("decode ingest frame")?;
        let enriched = EnrichedFrame {
            source: frame.source,
            seq: frame.seq,
            stage: "normalized".to_string(),
        };
        writer
            .send(
                &encode_rkyv(&enriched).context("encode enriched frame")?,
                SEND_TIMEOUT_MS,
            )
            .await
            .with_context(|| format!("forward enriched frame {}", enriched.seq))?;
    }
}
