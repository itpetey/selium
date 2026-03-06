use anyhow::Result;
use selium_io_core::ChannelKind;
use selium_io_durability::{ReplayStart, RetentionPolicy};
use selium_sdk_rust::Context;

#[tokio::main]
async fn main() -> Result<()> {
    let context = Context::new();
    context.ensure_channel(
        "media.raw.frames",
        ChannelKind::Stream,
        RetentionPolicy {
            max_entries: Some(32),
        },
    )?;

    let publisher = context.byte_publisher("media.raw.frames");
    publisher.publish(vec![0xde, 0xad, 0xbe, 0xef])?;

    let mut subscriber = context.byte_subscriber("media.raw.frames", ReplayStart::Earliest)?;
    let frame = subscriber.recv().await?;

    println!("byte frame => {} bytes", frame.len());
    Ok(())
}
