//! Parent/child orchestration with `ProcessBuilder`.
//! The supervisor starts two child entrypoints, receives a status signal from each, and stops them again.

use std::time::Duration;

use anyhow::{Context, Result, ensure};
use selium_abi::{AbiParam, AbiScalarType, AbiSignature, DataValue, decode_rkyv, encode_rkyv};
use selium_guest::{
    io,
    process::{Capability, ProcessBuilder},
    time,
};

#[allow(dead_code)]
mod bindings;

use bindings::WorkerStatus;

const SEND_TIMEOUT_MS: u32 = 1_000;
const RECV_TIMEOUT_MS: u32 = 5_000;
const MODULE_ID: &str = "process_supervisor.wasm";

#[selium_guest::entrypoint]
pub async fn start(bindings: DataValue) -> Result<()> {
    let bindings = encode_rkyv(&bindings).context("encode worker managed-event bindings")?;
    let mut status_reader =
        io::managed_event_reader(&bindings, bindings::EVENT_SUPERVISOR_WORKER_STATUS)
            .await
            .context("attach status reader")?;

    let signature = AbiSignature::new(
        vec![AbiParam::Scalar(AbiScalarType::I32), AbiParam::Buffer],
        Vec::new(),
    );

    let mut handles = Vec::new();
    for worker_id in 1..=2 {
        // The supervisor launches the same module again at the `worker` entrypoint and
        // supplies the ABI signature explicitly so the child can decode its arguments.
        let handle = ProcessBuilder::new(MODULE_ID, "worker")
            .signature(signature.clone())
            .capability(Capability::TimeRead)
            .capability(Capability::SharedMemory)
            .capability(Capability::QueueWriter)
            .arg_i32(worker_id)
            .arg_buffer(bindings.clone())
            .start()
            .await
            .with_context(|| format!("start worker {worker_id}"))?;
        handles.push(handle);
    }

    let mut started = Vec::new();
    while started.len() < 2 {
        let frame = status_reader
            .recv(RECV_TIMEOUT_MS)
            .await
            .context("receive worker status")?
            .context("missing worker status frame")?;
        let status = decode_rkyv::<WorkerStatus>(&frame.payload).context("decode worker status")?;
        ensure!(status.phase == "started", "unexpected worker phase");
        if !started.contains(&status.worker_id) {
            started.push(status.worker_id);
        }
    }

    for handle in handles {
        handle.stop().await.context("stop worker")?;
    }

    idle_forever().await
}

#[selium_guest::entrypoint]
pub async fn worker(worker_id: i32, bindings: DataValue) -> Result<()> {
    // Child processes receive the parent's managed bindings buffer so they can publish onto the
    // same discovery-managed endpoint without constructing local queue topology themselves.
    let bindings = encode_rkyv(&bindings).context("encode forwarded worker bindings")?;
    let mut writer = io::managed_event_writer(
        &bindings,
        bindings::EVENT_SUPERVISOR_WORKER_STATUS,
        worker_id as u32,
    )
    .await
    .context("attach worker status writer")?;
    let payload = WorkerStatus {
        worker_id: worker_id as u32,
        phase: "started".to_string(),
    };
    writer
        .send(
            &encode_rkyv(&payload).context("encode worker status")?,
            SEND_TIMEOUT_MS,
        )
        .await
        .context("send worker status")?;

    idle_forever().await
}

async fn idle_forever() -> Result<()> {
    loop {
        time::sleep(Duration::from_secs(60))
            .await
            .context("sleep while idle")?;
    }
}
