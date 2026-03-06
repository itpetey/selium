use std::{collections::BTreeMap, future::Future, time::Duration};

use anyhow::{Context, Result, anyhow, ensure};
use selium_abi::{decode_rkyv, encode_rkyv};
use selium_guest::{io, spawn, time};

#[allow(dead_code)]
mod bindings;

use bindings::{QuoteRequest, QuoteResponse};

const FRAME_BYTES: u32 = 512;
const SEND_TIMEOUT_MS: u32 = 1_000;
const RECV_TIMEOUT_MS: u32 = 5_000;

#[selium_guest::entrypoint]
pub async fn start() -> Result<()> {
    // Scatter-gather is modeled as one request queue per worker and one shared results
    // queue for aggregation, which keeps the fan-out and fan-in steps separate.
    let worker_a_requests = io::create_channel(16, FRAME_BYTES)
        .await
        .context("create worker A request channel")?;
    let worker_b_requests = io::create_channel(16, FRAME_BYTES)
        .await
        .context("create worker B request channel")?;
    let results = io::create_channel(16, FRAME_BYTES)
        .await
        .context("create results channel")?;

    spawn_checked(
        "worker A",
        run_worker(
            "worker-a",
            125,
            worker_a_requests.queue_shared_id,
            results.queue_shared_id,
        ),
    );
    spawn_checked(
        "worker B",
        run_worker(
            "worker-b",
            175,
            worker_b_requests.queue_shared_id,
            results.queue_shared_id,
        ),
    );

    let mut worker_a_writer = io::attach_writer(&descriptor(worker_a_requests.queue_shared_id), 31)
        .await
        .context("attach worker A writer")?;
    let mut worker_b_writer = io::attach_writer(&descriptor(worker_b_requests.queue_shared_id), 32)
        .await
        .context("attach worker B writer")?;
    let mut result_reader = io::attach_reader(&descriptor(results.queue_shared_id))
        .await
        .context("attach result reader")?;

    let requests = vec![
        QuoteRequest {
            request_id: 1,
            quantity: 2,
        },
        QuoteRequest {
            request_id: 2,
            quantity: 3,
        },
        QuoteRequest {
            request_id: 3,
            quantity: 1,
        },
        QuoteRequest {
            request_id: 4,
            quantity: 4,
        },
    ];

    for request in &requests {
        // The routing rule is intentionally simple: odd requests go to worker A, even
        // requests go to worker B, so the gather step can validate both workers replied.
        let writer = if request.request_id % 2 == 0 {
            &mut worker_b_writer
        } else {
            &mut worker_a_writer
        };
        writer
            .send(
                &encode_rkyv(request).context("encode request")?,
                SEND_TIMEOUT_MS,
            )
            .await
            .with_context(|| format!("send request {}", request.request_id))?;
    }

    let mut totals = BTreeMap::new();
    while totals.len() < requests.len() {
        let frame = result_reader
            .recv(RECV_TIMEOUT_MS)
            .await
            .context("receive quote result")?
            .ok_or_else(|| anyhow!("timed out waiting for quote results"))?;
        let response = decode_rkyv::<QuoteResponse>(&frame.payload).context("decode result")?;
        totals.insert(response.request_id, response.total_cents);
    }

    ensure!(
        totals == BTreeMap::from([(1, 250), (2, 525), (3, 125), (4, 700)]),
        "aggregated result mismatch"
    );
    idle_forever().await
}

async fn run_worker(
    worker: &'static str,
    unit_price_cents: u32,
    requests_shared_id: u64,
    results_shared_id: u64,
) -> Result<()> {
    // Each worker only sees its own request queue but publishes into the shared results
    // queue, which is the core shape of a scatter-gather workflow.
    let mut request_reader = io::attach_reader(&descriptor(requests_shared_id))
        .await
        .context("attach worker request reader")?;
    let mut result_writer =
        io::attach_writer(&descriptor(results_shared_id), writer_id_for(worker))
            .await
            .context("attach result writer")?;

    loop {
        let Some(frame) = request_reader
            .recv(RECV_TIMEOUT_MS)
            .await
            .context("receive quote request")?
        else {
            continue;
        };

        let request = decode_rkyv::<QuoteRequest>(&frame.payload).context("decode request")?;
        let response = QuoteResponse {
            request_id: request.request_id,
            worker: worker.to_string(),
            total_cents: request.quantity.saturating_mul(unit_price_cents),
        };
        result_writer
            .send(
                &encode_rkyv(&response).context("encode response")?,
                SEND_TIMEOUT_MS,
            )
            .await
            .with_context(|| format!("send response from {worker}"))?;
    }
}

fn writer_id_for(worker: &str) -> u32 {
    match worker {
        "worker-a" => 41,
        _ => 42,
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
