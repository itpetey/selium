//! Checkpoint handoff and resume using typed Selium entrypoint arguments.
//! This models guest-managed recovery state, not runtime-provided persistence.

use std::{future::Future, time::Duration};

use anyhow::{Context, Result, ensure};
use selium_abi::{decode_rkyv, encode_rkyv};
use selium_guest::{io, spawn, time};

#[allow(dead_code)]
mod bindings;

use bindings::{CounterCheckpoint, CounterDelta};

const FRAME_BYTES: u32 = 512;
const SEND_TIMEOUT_MS: u32 = 1_000;
const RECV_TIMEOUT_MS: u32 = 5_000;

#[selium_guest::entrypoint]
pub async fn start() -> Result<()> {
    run_counter(0, 0, "cold-start").await
}

#[selium_guest::entrypoint]
pub async fn resume(last_seq: u32, running_total: u32) -> Result<()> {
    // `resume` demonstrates re-entering the module with previously observed state supplied
    // as typed entrypoint arguments, not runtime-managed durable replay.
    run_counter(last_seq, running_total, "resume").await
}

async fn run_counter(last_seq: u32, running_total: u32, mode: &str) -> Result<()> {
    // This example keeps the event stream and checkpoint stream separate so readers can see
    // the difference between incoming changes and emitted recovery state.
    let delta_channel = io::create_channel(16, FRAME_BYTES)
        .await
        .context("create delta channel")?;
    let checkpoint_channel = io::create_channel(16, FRAME_BYTES)
        .await
        .context("create checkpoint channel")?;

    spawn_checked(
        "delta producer",
        publish_deltas(delta_channel.queue_shared_id),
    );

    let mut delta_reader = io::attach_reader(&descriptor(delta_channel.queue_shared_id))
        .await
        .context("attach delta reader")?;
    let mut checkpoint_writer =
        io::attach_writer(&descriptor(checkpoint_channel.queue_shared_id), 5)
            .await
            .context("attach checkpoint writer")?;
    let mut checkpoint_reader = io::attach_reader(&descriptor(checkpoint_channel.queue_shared_id))
        .await
        .context("attach checkpoint reader")?;

    let mut checkpoint = CounterCheckpoint {
        last_seq,
        running_total,
        mode: mode.to_string(),
    };
    let expected_remaining = sample_deltas()
        .into_iter()
        .filter(|delta| delta.seq > last_seq)
        .count();

    let mut processed = 0usize;
    while processed < expected_remaining {
        let frame = delta_reader
            .recv(RECV_TIMEOUT_MS)
            .await
            .context("receive delta")?
            .context("missing delta frame")?;
        let delta = decode_rkyv::<CounterDelta>(&frame.payload).context("decode delta")?;
        if delta.seq <= checkpoint.last_seq {
            continue;
        }

        checkpoint.last_seq = delta.seq;
        checkpoint.running_total = checkpoint.running_total.saturating_add(delta.amount);
        checkpoint.mode = mode.to_string();

        // The checkpoint is written back through a queue and read again immediately so the
        // example proves the persisted shape is the same data a future resume would consume.
        checkpoint_writer
            .send(
                &encode_rkyv(&checkpoint).context("encode checkpoint")?,
                SEND_TIMEOUT_MS,
            )
            .await
            .context("publish checkpoint")?;

        let echoed = checkpoint_reader
            .recv(RECV_TIMEOUT_MS)
            .await
            .context("receive checkpoint")?
            .context("missing checkpoint frame")?;
        let observed =
            decode_rkyv::<CounterCheckpoint>(&echoed.payload).context("decode checkpoint")?;
        ensure!(
            observed.last_seq == checkpoint.last_seq
                && observed.running_total == checkpoint.running_total
                && observed.mode == checkpoint.mode,
            "checkpoint round-trip mismatch"
        );
        processed += 1;
    }

    ensure!(
        checkpoint.running_total == expected_total(last_seq, running_total),
        "unexpected running total after checkpoint replay"
    );

    idle_forever().await
}

async fn publish_deltas(delta_shared_id: u64) -> Result<()> {
    let mut writer = io::attach_writer(&descriptor(delta_shared_id), 4)
        .await
        .context("attach delta writer")?;
    for delta in sample_deltas() {
        writer
            .send(
                &encode_rkyv(&delta).context("encode delta")?,
                SEND_TIMEOUT_MS,
            )
            .await
            .with_context(|| format!("send delta {}", delta.seq))?;
    }
    Ok(())
}

fn sample_deltas() -> Vec<CounterDelta> {
    vec![
        CounterDelta { seq: 1, amount: 5 },
        CounterDelta { seq: 2, amount: 10 },
        CounterDelta { seq: 3, amount: 15 },
        CounterDelta { seq: 4, amount: 20 },
    ]
}

fn expected_total(last_seq: u32, running_total: u32) -> u32 {
    sample_deltas()
        .into_iter()
        .filter(|delta| delta.seq > last_seq)
        .fold(running_total, |total, delta| {
            total.saturating_add(delta.amount)
        })
}

fn descriptor(shared_id: u64) -> io::ChannelDescriptor {
    // Queue ids are the stable values worth keeping in checkpoints or passing between tasks.
    io::ChannelDescriptor {
        queue_shared_id: shared_id,
        max_frame_bytes: FRAME_BYTES,
    }
}

fn spawn_checked<F>(name: &'static str, future: F)
where
    F: Future<Output = Result<()>> + 'static,
{
    // The producer is part of the startup proof, so its failure should surface immediately.
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
            .context("sleep while idle")?;
    }
}
