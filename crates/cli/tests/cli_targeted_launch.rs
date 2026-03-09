mod support;

use anyhow::Result;

use support::cluster_harness::{ClusterHarness, ClusterHarnessConfig};

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires local runtime binaries plus wasm32 build target"]
async fn cli_can_launch_user_module_on_either_node() -> Result<()> {
    let mut harness = ClusterHarness::new(ClusterHarnessConfig::new("cli-targeted-launch"))?;
    harness.prepare()?;
    harness.wait_for_consensus_ready().await?;

    let daemon_a = harness.daemon_addr_for("node-a")?;
    let daemon_b = harness.daemon_addr_for("node-b")?;

    harness.run_cli(
        &daemon_a,
        &[
            "start",
            "--node",
            "node-a",
            "--replica-key",
            "user-a",
            "--module",
            harness.user_module_relative_path(),
        ],
    )?;

    harness.run_cli(
        &daemon_b,
        &[
            "start",
            "--node",
            "node-b",
            "--replica-key",
            "user-b",
            "--module",
            harness.user_module_relative_path(),
        ],
    )?;

    let list_a = harness.run_cli(&daemon_a, &["list", "--node", "node-a"])?;
    let list_b = harness.run_cli(&daemon_b, &["list", "--node", "node-b"])?;

    assert!(list_a.lines().any(|line| line.starts_with("user-a ")));
    assert!(list_b.lines().any(|line| line.starts_with("user-b ")));

    harness.run_cli(
        &daemon_a,
        &["stop", "--node", "node-a", "--replica-key", "user-a"],
    )?;
    harness.run_cli(
        &daemon_b,
        &["stop", "--node", "node-b", "--replica-key", "user-b"],
    )?;

    Ok(())
}
