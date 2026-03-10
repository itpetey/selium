//! Two-stage pipeline processing inside one Selium guest module.
//! The example turns raw orders into reservation work and then into projection updates.

use std::{
    collections::BTreeSet,
    future::{Future, poll_fn},
    pin::Pin,
    task::Poll,
};

use anyhow::{Context, Result, anyhow, ensure};
use selium_abi::{DataValue, decode_rkyv, encode_rkyv};
use selium_guest::{io, shutdown, spawn};

#[allow(dead_code)]
mod bindings;

use bindings::{ProjectionApplied, RawOrder, ReservationRequest};

const SEND_TIMEOUT_MS: u32 = 1_000;
const RECV_TIMEOUT_MS: u32 = 5_000;

type ServiceTask = Pin<Box<dyn Future<Output = Result<()>> + 'static>>;

#[selium_guest::entrypoint]
pub async fn start(bindings: DataValue) -> Result<()> {
    // The three pipeline stages still hand work off explicitly, but now through the managed
    // contract endpoints that the control plane binds for this workload.
    let bindings = encode_rkyv(&bindings).context("encode managed event bindings")?;

    let normalizer = spawn_service("normalize stage", run_normalizer(bindings.clone()));
    let projection = spawn_service("projection stage", run_projection(bindings.clone()));

    let mut ingress_writer =
        io::managed_event_writer(&bindings, bindings::EVENT_INGRESS_ORDERS, 21)
            .await
            .context("attach ingress writer")?;
    let mut projection_reader =
        io::managed_event_reader(&bindings, bindings::EVENT_PROJECTIONS_APPLIED)
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
    await_services_or_shutdown([normalizer, projection]).await
}

async fn run_normalizer(bindings: Vec<u8>) -> Result<()> {
    let mut reader = io::managed_event_reader(&bindings, bindings::EVENT_INGRESS_ORDERS)
        .await
        .context("attach ingress reader")?;
    let mut writer =
        io::managed_event_writer(&bindings, bindings::EVENT_INVENTORY_RESERVATIONS, 22)
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

async fn run_projection(bindings: Vec<u8>) -> Result<()> {
    let mut reader = io::managed_event_reader(&bindings, bindings::EVENT_INVENTORY_RESERVATIONS)
        .await
        .context("attach normalized reader")?;
    let mut writer = io::managed_event_writer(&bindings, bindings::EVENT_PROJECTIONS_APPLIED, 23)
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
