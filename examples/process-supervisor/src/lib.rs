use std::time::Duration;

use anyhow::{Context, Result, ensure};
use selium_abi::{AbiParam, AbiScalarType, AbiSignature, decode_rkyv, encode_rkyv};
use selium_guest::{
    io,
    process::{Capability, ProcessBuilder},
    time,
};

#[allow(dead_code)]
mod bindings;

use bindings::WorkerStatus;

const FRAME_BYTES: u32 = 512;
const SEND_TIMEOUT_MS: u32 = 1_000;
const RECV_TIMEOUT_MS: u32 = 5_000;
const MODULE_ID: &str = "process_supervisor.wasm";

#[selium_guest::entrypoint]
pub async fn start() -> Result<()> {
    let status_channel = io::create_channel(16, FRAME_BYTES)
        .await
        .context("create status channel")?;
    let mut status_reader = io::attach_reader(&descriptor(status_channel.queue_shared_id))
        .await
        .context("attach status reader")?;

    let signature = AbiSignature::new(
        vec![
            AbiParam::Scalar(AbiScalarType::I32),
            AbiParam::Scalar(AbiScalarType::U64),
        ],
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
            .capability(Capability::QueueLifecycle)
            .capability(Capability::QueueWriter)
            .arg_i32(worker_id)
            .arg_resource(status_channel.queue_shared_id)
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
        if !started.iter().any(|id| *id == status.worker_id) {
            started.push(status.worker_id);
        }
    }

    for handle in handles {
        handle.stop().await.context("stop worker")?;
    }

    idle_forever().await
}

#[selium_guest::entrypoint]
pub async fn worker(worker_id: i32, status_channel_shared_id: u64) -> Result<()> {
    // Child processes receive the shared queue id as a resource handle, then attach to it
    // just like the parent would. That is the handoff mechanism between related processes.
    let mut writer = io::attach_writer(&descriptor(status_channel_shared_id), worker_id as u32)
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

fn descriptor(shared_id: u64) -> io::ChannelDescriptor {
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
