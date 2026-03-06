use anyhow::{Context, anyhow};
use selium_guest::io;

#[selium_guest::entrypoint]
pub async fn start() -> anyhow::Result<()> {
    let channel = io::create_channel(16, 512)
        .await
        .context("create channel")?;

    let mut writer = io::attach_writer(&channel, 7)
        .await
        .context("attach writer")?;
    writer
        .send(b"guest-io-smoke", 1_000)
        .await
        .context("send frame")?;
    writer.close().await.context("close writer")?;

    let mut reader = io::attach_reader(&channel)
        .await
        .context("attach reader")?;
    let message = reader
        .recv(1_000)
        .await
        .context("receive frame")?
        .ok_or_else(|| anyhow!("expected a frame from loopback queue"))?;

    if message.payload.as_slice() != b"guest-io-smoke" {
        return Err(anyhow!("payload mismatch in guest I/O smoke"));
    }

    reader.close().await.context("close reader")?;

    loop {
        selium_guest::yield_now().await;
    }
}
