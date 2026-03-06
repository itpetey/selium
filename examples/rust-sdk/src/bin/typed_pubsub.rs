use anyhow::Result;
use rkyv::{Archive, Deserialize, Serialize};
use selium_io_core::ChannelKind;
use selium_io_durability::{ReplayStart, RetentionPolicy};
use selium_sdk_rust::Context;

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
struct EchoRequested {
    correlation_id: String,
    msg: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let context = Context::new();
    context.ensure_channel(
        "messaging.echo.requested",
        ChannelKind::Event,
        RetentionPolicy {
            max_entries: Some(128),
        },
    )?;

    let publisher = context.publisher::<EchoRequested>("messaging.echo.requested");
    publisher.publish(EchoRequested {
        correlation_id: "req-1".to_string(),
        msg: "hello".to_string(),
    })?;

    let mut subscriber =
        context.subscriber::<EchoRequested>("messaging.echo.requested", ReplayStart::Earliest)?;
    let message = subscriber.recv().await?;

    println!(
        "typed message => correlation_id={}, msg={}",
        message.correlation_id, message.msg
    );

    Ok(())
}
