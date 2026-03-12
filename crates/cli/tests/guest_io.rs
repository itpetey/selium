mod support;

use std::time::Duration;

use anyhow::Result;

use support::cluster_harness::{ClusterHarness, ClusterHarnessConfig};

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires local runtime binaries plus wasm32 build target"]
async fn guest_side_io_module_starts_and_stops_cleanly() -> Result<()> {
    let mut harness = ClusterHarness::new(ClusterHarnessConfig::new("guest-io"))?;
    harness.prepare()?;
    harness.wait_for_consensus_ready().await?;

    let daemon_a = harness.daemon_addr_for("node-a")?;

    let start_out = harness.run_cli(
        &daemon_a,
        &[
            "start",
            "--node",
            "node-a",
            "--replica-key",
            "io-a",
            "--event-reader",
            "inventory.adjusted",
            "--event-writer",
            "inventory.adjusted",
            "--event-reader",
            "inventory.delivery_acks",
            "--event-writer",
            "inventory.delivery_acks",
            "--module",
            harness.user_io_module_relative_path(),
        ],
    )?;
    assert!(
        start_out.contains("start status=ok"),
        "unexpected start output: {start_out}"
    );

    tokio::time::sleep(Duration::from_millis(800)).await;

    let logs = harness.node_logs("node-a")?;
    assert!(
        !logs.contains("entrypoint start failed"),
        "guest I/O module crashed:\n{logs}"
    );

    let stop_out = harness.run_cli(
        &daemon_a,
        &["stop", "--node", "node-a", "--replica-key", "io-a"],
    )?;
    assert!(
        stop_out.contains("stop status=ok"),
        "unexpected stop output: {stop_out}"
    );

    Ok(())
}
