//! Sink app used by the control-plane topology example.
//! It mirrors the other apps with a local contract-level self-check before idling under control-plane management.

use std::time::Duration;

use anyhow::{Context, Result, ensure};
use selium_abi::{decode_rkyv, encode_rkyv};
use selium_guest::{io, time};

#[allow(dead_code)]
mod bindings;

use bindings::EnrichedFrame;

const FRAME_BYTES: u32 = 512;
const SEND_TIMEOUT_MS: u32 = 1_000;
const RECV_TIMEOUT_MS: u32 = 5_000;

#[selium_guest::entrypoint]
pub async fn start() -> Result<()> {
    // The sink mirrors the other apps with a local self-check so the module is functional on
    // its own even though the top-level example is primarily about contract and topology setup.
    let channel = io::create_channel(16, FRAME_BYTES)
        .await
        .context("create sink verification channel")?;
    let mut writer = io::attach_writer(&descriptor(channel.queue_shared_id), 3)
        .await
        .context("attach sink writer")?;
    let mut reader = io::attach_reader(&descriptor(channel.queue_shared_id))
        .await
        .context("attach sink reader")?;

    let frame = EnrichedFrame {
        source: "sink".to_string(),
        seq: 1,
        stage: "materialized".to_string(),
    };
    writer
        .send(
            &encode_rkyv(&frame).context("encode sink frame")?,
            SEND_TIMEOUT_MS,
        )
        .await
        .context("send sink frame")?;
    let observed = reader
        .recv(RECV_TIMEOUT_MS)
        .await
        .context("receive sink frame")?
        .context("missing sink frame")?;
    let decoded = decode_rkyv::<EnrichedFrame>(&observed.payload).context("decode sink frame")?;
    ensure!(
        decoded.source == frame.source && decoded.seq == frame.seq && decoded.stage == frame.stage,
        "sink self-check mismatch"
    );

    idle_forever().await
}

fn descriptor(shared_id: u64) -> io::ChannelDescriptor {
    // The sink receives the same kind of queue handle as any other guest task or process.
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
