mod support;

use std::collections::BTreeSet;

use anyhow::Result;

use support::cluster_harness::{ClusterHarness, ClusterHarnessConfig};

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires local runtime binaries plus wasm32 build target"]
async fn one_runtime_daemon_per_node_is_running() -> Result<()> {
    let mut harness = ClusterHarness::new(ClusterHarnessConfig::new("runtime-topology"))?;
    harness.prepare()?;
    harness.wait_for_consensus_ready().await?;

    let pids = harness.runtime_pids();
    assert_eq!(pids.len(), 2, "expected two runtime daemons");
    let unique = pids.iter().copied().collect::<BTreeSet<_>>();
    assert_eq!(unique.len(), 2, "expected unique daemon PIDs per node");

    let daemon_a = harness.daemon_addr_for("node-a")?;
    let daemon_b = harness.daemon_addr_for("node-b")?;

    let live_a = harness.live_nodes(&daemon_a)?;
    let live_b = harness.live_nodes(&daemon_b)?;

    assert!(
        live_a.contains("node-a"),
        "node-a daemon should report node-a live; got {live_a:?}"
    );
    assert!(
        live_b.contains("node-b"),
        "node-b daemon should report node-b live; got {live_b:?}"
    );

    Ok(())
}
