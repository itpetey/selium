mod support;

use std::{env, time::SystemTime};

use anyhow::Result;

use support::cluster_harness::{ClusterHarness, ClusterHarnessConfig};

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires local runtime binaries plus wasm32 build target"]
async fn agent_once_reconciles_node_b_via_follower_snapshot() -> Result<()> {
    let mut harness = ClusterHarness::new(ClusterHarnessConfig::new("control-plane-agent"))?;
    harness.prepare()?;
    harness.wait_for_consensus_ready().await?;

    let daemon_b = harness.daemon_addr_for("node-b")?;
    let agent_state = env::temp_dir().join(format!(
        "selium-agent-node-b-{}.rkyv",
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    let agent_state = agent_state.to_string_lossy().into_owned();

    harness.run_cli(
        &daemon_b,
        &[
            "agent",
            "--node",
            "node-b",
            "--once",
            "--agent-state",
            &agent_state,
        ],
    )?;

    Ok(())
}
