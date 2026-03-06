//! Minimal guest-side I/O example module using `selium_guest::io`.

use selium_guest::io;

#[selium_guest::entrypoint]
pub async fn start() -> anyhow::Result<()> {
    let channel = io::create_channel(64, 4096).await?;
    let mut writer = io::attach_writer(&channel, 1).await?;
    let _ = writer.send(b"hello from guest io demo", 1_000).await?;
    writer.close().await?;
    Ok(())
}
