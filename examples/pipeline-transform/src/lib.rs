//! Two-stage pipeline processing inside one Selium guest module.
//! The example turns raw orders into reservation work and then into projection updates.

use std::{collections::BTreeSet, future::Future, time::Duration};

use anyhow::{Context, Result, anyhow, ensure};
use selium_abi::{decode_rkyv, encode_rkyv};
use selium_guest::{io, spawn, time};

#[allow(dead_code)]
mod bindings;

use bindings::{ProjectionApplied, RawOrder, ReservationRequest};

const FRAME_BYTES: u32 = 768;
const SEND_TIMEOUT_MS: u32 = 1_000;
const RECV_TIMEOUT_MS: u32 = 5_000;

#[selium_guest::entrypoint]
pub async fn start() -> Result<()> {
    // The pipeline is modeled as three queues: raw ingress, normalized work, and final
    // projections. Keeping the stages explicit makes the message handoff visible.
    let ingress = io::create_channel(16, FRAME_BYTES)
        .await
        .context("create ingress channel")?;
    let normalized = io::create_channel(16, FRAME_BYTES)
        .await
        .context("create normalized channel")?;
    let projections = io::create_channel(16, FRAME_BYTES)
        .await
        .context("create projection channel")?;

    spawn_checked(
        "normalize stage",
        run_normalizer(ingress.queue_shared_id, normalized.queue_shared_id),
    );
    spawn_checked(
        "projection stage",
        run_projection(normalized.queue_shared_id, projections.queue_shared_id),
    );

    let mut ingress_writer = io::attach_writer(&descriptor(ingress.queue_shared_id), 21)
        .await
        .context("attach ingress writer")?;
    let mut projection_reader = io::attach_reader(&descriptor(projections.queue_shared_id))
        .await
        .context("attach projection reader")?;

    for order in demo_orders() {
        ingress_writer
            .send(
                &encode_rkyv(&order).context("encode raw order")?,
                SEND_TIMEOUT_MS,
            )
            .await
            .with_context(|| format!("send raw order {}", order.order_id))?;
    }

    let expected = expected_projection_keys();
    let mut seen = BTreeSet::new();
    while seen.len() < expected.len() {
        let frame = projection_reader
            .recv(RECV_TIMEOUT_MS)
            .await
            .context("receive projection")?
            .ok_or_else(|| anyhow!("timed out waiting for projections"))?;
        let projection =
            decode_rkyv::<ProjectionApplied>(&frame.payload).context("decode projection")?;
        seen.insert(projection.projection_key);
    }

    ensure!(seen == expected, "projection output mismatch");
    idle_forever().await
}

async fn run_normalizer(ingress_shared_id: u64, normalized_shared_id: u64) -> Result<()> {
    let mut reader = io::attach_reader(&descriptor(ingress_shared_id))
        .await
        .context("attach ingress reader")?;
    let mut writer = io::attach_writer(&descriptor(normalized_shared_id), 22)
        .await
        .context("attach normalized writer")?;

    loop {
        let Some(frame) = reader
            .recv(RECV_TIMEOUT_MS)
            .await
            .context("receive raw order")?
        else {
            continue;
        };

        let order = decode_rkyv::<RawOrder>(&frame.payload).context("decode raw order")?;
        // Stage 1 converts an external-looking order into the internal reservation request
        // shape that the next stage expects.
        let request = ReservationRequest {
            order_id: order.order_id,
            reservation_key: format!("inventory/{}", order.sku.to_lowercase()),
            units: order.quantity,
        };
        writer
            .send(
                &encode_rkyv(&request).context("encode reservation request")?,
                SEND_TIMEOUT_MS,
            )
            .await
            .with_context(|| format!("forward order {}", order.order_id))?;
    }
}

async fn run_projection(normalized_shared_id: u64, projection_shared_id: u64) -> Result<()> {
    let mut reader = io::attach_reader(&descriptor(normalized_shared_id))
        .await
        .context("attach normalized reader")?;
    let mut writer = io::attach_writer(&descriptor(projection_shared_id), 23)
        .await
        .context("attach projection writer")?;

    loop {
        let Some(frame) = reader
            .recv(RECV_TIMEOUT_MS)
            .await
            .context("receive reservation request")?
        else {
            continue;
        };

        let request = decode_rkyv::<ReservationRequest>(&frame.payload)
            .context("decode reservation request")?;
        // Stage 2 materializes the derived projection so startup can assert the complete
        // pipeline output rather than only checking that the queue stayed alive.
        let projection = ProjectionApplied {
            order_id: request.order_id,
            projection_key: format!("projection/{}:{}", request.reservation_key, request.units),
        };
        writer
            .send(
                &encode_rkyv(&projection).context("encode projection")?,
                SEND_TIMEOUT_MS,
            )
            .await
            .with_context(|| format!("project order {}", request.order_id))?;
    }
}

fn demo_orders() -> Vec<RawOrder> {
    vec![
        RawOrder {
            order_id: 101,
            sku: "WIDGET-A".to_string(),
            quantity: 2,
        },
        RawOrder {
            order_id: 102,
            sku: "WIDGET-B".to_string(),
            quantity: 5,
        },
    ]
}

fn expected_projection_keys() -> BTreeSet<String> {
    BTreeSet::from([
        "projection/inventory/widget-a:2".to_string(),
        "projection/inventory/widget-b:5".to_string(),
    ])
}

fn descriptor(shared_id: u64) -> io::ChannelDescriptor {
    // Each stage receives only the queue id it needs and reconstructs the full descriptor locally.
    io::ChannelDescriptor {
        queue_shared_id: shared_id,
        max_frame_bytes: FRAME_BYTES,
    }
}

fn spawn_checked<F>(name: &'static str, future: F)
where
    F: Future<Output = Result<()>> + 'static,
{
    // Any stage failure is treated as a startup failure because partial pipelines are misleading.
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
