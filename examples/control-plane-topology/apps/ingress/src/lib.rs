//! Ingress app used by the control-plane topology example.
//! It performs a local self-check so the deployed module is functional even before cross-app routing exists.

use std::time::Duration;

use anyhow::{Context, Result, ensure};
use selium_abi::{decode_rkyv, encode_rkyv};
use selium_guest::{io, time};

#[allow(dead_code)]
mod bindings;

use bindings::Frame;

const FRAME_BYTES: u32 = 512;
const SEND_TIMEOUT_MS: u32 = 1_000;
const RECV_TIMEOUT_MS: u32 = 5_000;

#[selium_guest::entrypoint]
pub async fn start() -> Result<()> {
    // This crate self-checks locally at startup. The example's control-plane README then
    // shows how this module participates in a larger deployed topology.
    let channel = io::create_channel(16, FRAME_BYTES)
        .await
        .context("create ingress verification channel")?;
    let mut writer = io::attach_writer(&descriptor(channel.queue_shared_id), 1)
        .await
        .context("attach ingress writer")?;
    let mut reader = io::attach_reader(&descriptor(channel.queue_shared_id))
        .await
        .context("attach ingress reader")?;

    let frame = Frame {
        source: "ingress".to_string(),
        seq: 1,
    };
    writer
        .send(
            &encode_rkyv(&frame).context("encode ingress frame")?,
            SEND_TIMEOUT_MS,
        )
        .await
        .context("send ingress frame")?;
    let observed = reader
        .recv(RECV_TIMEOUT_MS)
        .await
        .context("receive ingress frame")?
        .context("missing ingress frame")?;
    let decoded = decode_rkyv::<Frame>(&observed.payload).context("decode ingress frame")?;
    ensure!(
        decoded.source == frame.source && decoded.seq == frame.seq,
        "ingress self-check mismatch"
    );

    idle_forever().await
}

fn descriptor(shared_id: u64) -> io::ChannelDescriptor {
    // The control-plane example still uses the same queue attachment model as the other examples.
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
