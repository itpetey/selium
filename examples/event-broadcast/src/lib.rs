use std::{collections::BTreeSet, future::Future, time::Duration};

use anyhow::{Context, Result, anyhow, ensure};
use selium_abi::{decode_rkyv, encode_rkyv};
use selium_guest::{io, spawn, time};

#[allow(dead_code)]
mod bindings;

use bindings::{DeliveryAck, InventoryAdjusted};

const FRAME_BYTES: u32 = 512;
const SEND_TIMEOUT_MS: u32 = 1_000;
const RECV_TIMEOUT_MS: u32 = 5_000;
const EVENT_COUNT: u32 = 3;

#[selium_guest::entrypoint]
pub async fn start() -> Result<()> {
    // One events channel fans out to both subscribers, while a second channel gathers
    // typed acknowledgements so startup can prove every consumer saw every event.
    let events = io::create_channel(16, FRAME_BYTES)
        .await
        .context("create events channel")?;
    let acknowledgements = io::create_channel(16, FRAME_BYTES)
        .await
        .context("create ack channel")?;

    spawn_checked(
        "audit subscriber",
        run_subscriber(
            "audit",
            events.queue_shared_id,
            acknowledgements.queue_shared_id,
        ),
    );
    spawn_checked(
        "notifications subscriber",
        run_subscriber(
            "notifications",
            events.queue_shared_id,
            acknowledgements.queue_shared_id,
        ),
    );

    let mut event_writer = io::attach_writer(&descriptor(events.queue_shared_id), 7)
        .await
        .context("attach event writer")?;
    let mut ack_reader = io::attach_reader(&descriptor(acknowledgements.queue_shared_id))
        .await
        .context("attach ack reader")?;

    for event_id in 1..=EVENT_COUNT {
        let event = InventoryAdjusted {
            event_id,
            sku: format!("SKU-{event_id}"),
            delta: i32::try_from(event_id).unwrap_or_default(),
        };
        event_writer
            .send(
                &encode_rkyv(&event).context("encode event")?,
                SEND_TIMEOUT_MS,
            )
            .await
            .with_context(|| format!("send event {event_id}"))?;
    }

    let expected = expected_acks();
    let mut seen = BTreeSet::new();
    while seen.len() < expected.len() {
        let frame = ack_reader
            .recv(RECV_TIMEOUT_MS)
            .await
            .context("receive ack")?
            .ok_or_else(|| anyhow!("timed out waiting for subscriber acks"))?;
        let ack = decode_rkyv::<DeliveryAck>(&frame.payload).context("decode ack")?;
        seen.insert((ack.consumer, ack.event_id));
    }

    ensure!(seen == expected, "broadcast ack set mismatch");
    idle_forever().await
}

async fn run_subscriber(
    consumer: &'static str,
    events_shared_id: u64,
    ack_shared_id: u64,
) -> Result<()> {
    // Multiple readers attach to the same queue descriptor; this example treats that as
    // the broadcast surface and then reports delivery on a separate acknowledgement queue.
    let mut reader = io::attach_reader(&descriptor(events_shared_id))
        .await
        .context("attach event reader")?;
    let mut ack_writer = io::attach_writer(&descriptor(ack_shared_id), writer_id_for(consumer))
        .await
        .context("attach ack writer")?;

    loop {
        let Some(frame) = reader
            .recv(RECV_TIMEOUT_MS)
            .await
            .context("receive broadcast event")?
        else {
            continue;
        };

        let event = decode_rkyv::<InventoryAdjusted>(&frame.payload).context("decode event")?;
        let ack = DeliveryAck {
            consumer: consumer.to_string(),
            event_id: event.event_id,
        };
        ack_writer
            .send(&encode_rkyv(&ack).context("encode ack")?, SEND_TIMEOUT_MS)
            .await
            .with_context(|| format!("send ack from {consumer}"))?;
    }
}

fn expected_acks() -> BTreeSet<(String, u32)> {
    let mut expected = BTreeSet::new();
    for consumer in ["audit", "notifications"] {
        for event_id in 1..=EVENT_COUNT {
            expected.insert((consumer.to_string(), event_id));
        }
    }
    expected
}

fn writer_id_for(consumer: &str) -> u32 {
    match consumer {
        "audit" => 11,
        _ => 12,
    }
}

fn descriptor(shared_id: u64) -> io::ChannelDescriptor {
    io::ChannelDescriptor {
        queue_shared_id: shared_id,
        max_frame_bytes: FRAME_BYTES,
    }
}

fn spawn_checked<F>(name: &'static str, future: F)
where
    F: Future<Output = Result<()>> + 'static,
{
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
