#[selium_guest::entrypoint]
pub async fn start() -> anyhow::Result<()> {
    loop {
        selium_guest::yield_now().await;
    }
}
