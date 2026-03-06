mod support;

use anyhow::Result;

use support::cluster_harness::{ClusterHarness, ClusterHarnessConfig};

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires local runtime binaries plus wasm32 build target"]
async fn goal1_two_node_cluster_starts_user_module_on_both_nodes() -> Result<()> {
    let mut harness = ClusterHarness::new(ClusterHarnessConfig::new("goal1-cluster"))?;
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
            "--instance-id",
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
            "--instance-id",
            "user-b",
            "--module",
            harness.user_module_relative_path(),
        ],
    )?;

    let node_a_list = harness.run_cli(&daemon_a, &["list", "--node", "node-a"])?;
    let node_b_list = harness.run_cli(&daemon_b, &["list", "--node", "node-b"])?;

    assert!(node_a_list.lines().any(|line| line.starts_with("user-a ")));
    assert!(node_b_list.lines().any(|line| line.starts_with("user-b ")));

    harness.run_cli(
        &daemon_a,
        &["stop", "--node", "node-a", "--instance-id", "user-a"],
    )?;
    harness.run_cli(
        &daemon_b,
        &["stop", "--node", "node-b", "--instance-id", "user-b"],
    )?;

    let node_a_after = harness.run_cli(&daemon_a, &["list", "--node", "node-a"])?;
    let node_b_after = harness.run_cli(&daemon_b, &["list", "--node", "node-b"])?;

    assert!(
        node_a_after.trim().is_empty(),
        "node-a should be empty after stop"
    );
    assert!(
        node_b_after.trim().is_empty(),
        "node-b should be empty after stop"
    );

    Ok(())
}
