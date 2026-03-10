//! Minimal event broadcast with two subscribers and explicit delivery acknowledgements.
//! Startup only succeeds once every subscriber has confirmed every published event.

use std::{
    collections::BTreeSet,
    future::{Future, poll_fn},
    pin::Pin,
    task::Poll,
};

use anyhow::{Context, Result, anyhow, ensure};
use selium_abi::DataValue;
use selium_guest::{io, shutdown, spawn};

#[allow(dead_code)]
mod bindings;

use bindings::{DeliveryAck, InventoryAdjusted};

const SEND_TIMEOUT_MS: u32 = 1_000;
const RECV_TIMEOUT_MS: u32 = 5_000;
const EVENT_COUNT: u32 = 3;

type ServiceTask = Pin<Box<dyn Future<Output = Result<()>> + 'static>>;

#[selium_guest::entrypoint]
pub async fn start(bindings: DataValue) -> Result<()> {
    // Both flows now bind to contract-defined managed event endpoints instead of guest-created
    // queues, while keeping the same fan-out plus acknowledgement startup proof.
    let audit = spawn_service(
        "audit subscriber",
        run_subscriber("audit", bindings.clone()),
    );
    let notifications = spawn_service(
        "notifications subscriber",
        run_subscriber("notifications", bindings.clone()),
    );

    let mut event_writer =
        io::managed_event_writer(&bindings, bindings::EVENT_INVENTORY_ADJUSTED, 7)
            .await
            .context("attach managed event writer")?;
    let mut ack_reader =
        io::managed_event_reader(&bindings, bindings::EVENT_INVENTORY_DELIVERY_ACKS)
            .await
            .context("attach managed ack reader")?;

    for event_id in 1..=EVENT_COUNT {
        let event = InventoryAdjusted {
            event_id,
            sku: format!("SKU-{event_id}"),
            delta: i32::try_from(event_id).unwrap_or_default(),
        };
        event_writer
            .send_typed(&event, SEND_TIMEOUT_MS)
            .await
            .with_context(|| format!("send event {event_id}"))?;
    }

    let expected = expected_acks();
    let mut seen = BTreeSet::new();
    while seen.len() < expected.len() {
        let ack = ack_reader
            .recv_typed::<DeliveryAck>(RECV_TIMEOUT_MS)
            .await
            .context("receive ack")?
            .ok_or_else(|| anyhow!("timed out waiting for subscriber acks"))?
            .payload;
        seen.insert((ack.consumer, ack.event_id));
    }

    ensure!(seen == expected, "broadcast ack set mismatch");
    await_services_or_shutdown([audit, notifications]).await
}

async fn run_subscriber(consumer: &'static str, bindings: DataValue) -> Result<()> {
    // Multiple readers attach to the same managed event endpoint, preserving the fan-out surface
    // while acknowledgements flow back on a separate managed endpoint.
    let mut reader = io::managed_event_reader(&bindings, bindings::EVENT_INVENTORY_ADJUSTED)
        .await
        .context("attach managed event reader")?;
    let mut ack_writer = io::managed_event_writer(
        &bindings,
        bindings::EVENT_INVENTORY_DELIVERY_ACKS,
        writer_id_for(consumer),
    )
    .await
    .context("attach managed ack writer")?;

    loop {
        let Some(event) = reader
            .recv_typed::<InventoryAdjusted>(RECV_TIMEOUT_MS)
            .await
            .context("receive broadcast event")?
        else {
            continue;
        };

        let ack = DeliveryAck {
            consumer: consumer.to_string(),
            event_id: event.payload.event_id,
        };
        ack_writer
            .send_typed(&ack, SEND_TIMEOUT_MS)
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

fn spawn_service<F>(name: &'static str, future: F) -> ServiceTask
where
    F: Future<Output = Result<()>> + 'static,
{
    Box::pin(spawn(async move {
        future.await.with_context(|| format!("{name} failed"))
    }))
}

async fn await_services_or_shutdown<const N: usize>(mut services: [ServiceTask; N]) -> Result<()> {
    let mut shutdown = std::pin::pin!(shutdown());
    poll_fn(|cx| {
        for service in &mut services {
            if let Poll::Ready(result) = service.as_mut().poll(cx) {
                return Poll::Ready(result);
            }
        }
        if shutdown.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Ok(()));
        }
        Poll::Pending
    })
    .await
}
