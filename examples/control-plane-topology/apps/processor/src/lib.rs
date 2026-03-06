//! Processor app used by the control-plane topology example.
//! Today it validates its own contract types locally while the control plane models the deployment graph.

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
    // This is intentionally a local verification loop: deployment edges are modeled by the
    // control plane today, while application message routing across deployments is separate work.
    let channel = io::create_channel(16, FRAME_BYTES)
        .await
        .context("create processor verification channel")?;
    let mut writer = io::attach_writer(&descriptor(channel.queue_shared_id), 2)
        .await
        .context("attach processor writer")?;
    let mut reader = io::attach_reader(&descriptor(channel.queue_shared_id))
        .await
        .context("attach processor reader")?;

    let frame = EnrichedFrame {
        source: "processor".to_string(),
        seq: 1,
        stage: "normalized".to_string(),
    };
    writer
        .send(
            &encode_rkyv(&frame).context("encode processor frame")?,
            SEND_TIMEOUT_MS,
        )
        .await
        .context("send processor frame")?;
    let observed = reader
        .recv(RECV_TIMEOUT_MS)
        .await
        .context("receive processor frame")?
        .context("missing processor frame")?;
    let decoded =
        decode_rkyv::<EnrichedFrame>(&observed.payload).context("decode processor frame")?;
    ensure!(
        decoded.source == frame.source && decoded.seq == frame.seq && decoded.stage == frame.stage,
        "processor self-check mismatch"
    );

    idle_forever().await
}

fn descriptor(shared_id: u64) -> io::ChannelDescriptor {
    // Keeping descriptor construction local makes each app independent of who created the queue.
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
