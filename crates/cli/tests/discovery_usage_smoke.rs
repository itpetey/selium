mod support;

use std::time::Duration;

use anyhow::Result;

use support::cluster_harness::{ClusterHarness, ClusterHarnessConfig};

const CONSENSUS_TIMEOUT: Duration = Duration::from_secs(180);
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(90);
const USAGE_TIMEOUT: Duration = Duration::from_secs(180);
const WORKLOAD_KEY: &str = "tenant-a/analytics/topology-ingress";
const ENDPOINT_KEY: &str = "tenant-a/analytics/topology-ingress#event:ingest.frames";
const INGRESS_REPLICA: &str =
    "tenant=tenant-a;namespace=analytics;workload=topology-ingress;replica=0";
const PROCESSOR_REPLICA: &str =
    "tenant=tenant-a;namespace=analytics;workload=topology-processor;replica=0";
const SINK_REPLICA: &str = "tenant=tenant-a;namespace=analytics;workload=topology-sink;replica=0";
const PROCESSOR_TO_SINK_EDGE: &str = "tenant-a/analytics/topology-processor#process.enriched";
const SINK_EDGE: &str = "tenant-a/analytics/topology-sink#process.enriched";

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires local runtime binaries plus wasm32 build target"]
async fn discovery_to_usage_cli_flow_stays_machine_consumable() -> Result<()> {
    let mut harness = ClusterHarness::new(ClusterHarnessConfig {
        name: "discovery-usage-smoke".to_string(),
        consensus_timeout: CONSENSUS_TIMEOUT,
        ..ClusterHarnessConfig::new("discovery-usage-smoke")
    })?;
    harness.prepare()?;
    harness.prepare_control_plane_topology()?;
    harness.wait_for_consensus_ready().await?;

    let daemon_a = harness.daemon_addr_for("node-a")?;
    let daemon_b = harness.daemon_addr_for("node-b")?;
    let control_daemons = [daemon_a.as_str(), daemon_b.as_str()];
    let agent_state_a = harness.agent_state_path("node-a")?;
    let agent_state_b = harness.agent_state_path("node-b")?;
    let agent_state_a = agent_state_a.to_string_lossy().into_owned();
    let agent_state_b = agent_state_b.to_string_lossy().into_owned();

    let publish_out = harness.run_cli(
        &daemon_a,
        &[
            "idl",
            "publish",
            "--input",
            harness.topology_contract_path(),
        ],
    )?;
    assert!(
        publish_out.contains("published IDL"),
        "unexpected publish output: {publish_out}"
    );

    harness.run_cli(
        &daemon_a,
        &[
            "deploy",
            "--tenant",
            "tenant-a",
            "--namespace",
            "analytics",
            "--workload",
            "topology-ingress",
            "--module",
            harness.topology_ingress_module_relative_path(),
            "--contract",
            "analytics.topology/ingest.frames@v1",
        ],
    )?;
    harness.run_cli(
        &daemon_a,
        &[
            "deploy",
            "--tenant",
            "tenant-a",
            "--namespace",
            "analytics",
            "--workload",
            "topology-processor",
            "--module",
            harness.topology_processor_module_relative_path(),
            "--contract",
            "analytics.topology/ingest.frames@v1",
            "--contract",
            "analytics.topology/process.enriched@v1",
        ],
    )?;
    harness.run_cli(
        &daemon_a,
        &[
            "deploy",
            "--tenant",
            "tenant-a",
            "--namespace",
            "analytics",
            "--workload",
            "topology-sink",
            "--module",
            harness.topology_sink_module_relative_path(),
            "--contract",
            "analytics.topology/process.enriched@v1",
        ],
    )?;
    harness.run_cli(
        &daemon_a,
        &[
            "connect",
            "--pipeline",
            "analytics-demo",
            "--tenant",
            "tenant-a",
            "--namespace",
            "analytics",
            "--from-workload",
            "topology-ingress",
            "--to-workload",
            "topology-processor",
            "--endpoint",
            "ingest.frames",
            "--contract",
            "analytics.topology/ingest.frames@v1",
        ],
    )?;
    harness.run_cli(
        &daemon_a,
        &[
            "connect",
            "--pipeline",
            "analytics-demo",
            "--tenant",
            "tenant-a",
            "--namespace",
            "analytics",
            "--from-workload",
            "topology-processor",
            "--to-workload",
            "topology-sink",
            "--endpoint",
            "process.enriched",
            "--contract",
            "analytics.topology/process.enriched@v1",
        ],
    )?;

    let (_, observe) = harness
        .wait_for_any_cli_contains(
            &control_daemons,
            &["observe"],
            "analytics-demo",
            DELIVERY_TIMEOUT,
        )
        .await?;
    assert!(
        observe.contains("topology-ingress")
            && observe.contains("topology-processor")
            && observe.contains("topology-sink"),
        "unexpected observe output:\n{observe}"
    );

    harness
        .wait_for_control_plane_state(&daemon_b, true, DELIVERY_TIMEOUT, |state| {
            state.pipelines.values().any(|pipeline| {
                pipeline.edges.iter().any(|edge| {
                    edge.from.endpoint.key() == PROCESSOR_TO_SINK_EDGE
                        && edge.to.endpoint.key() == SINK_EDGE
                })
            })
        })
        .await?;

    let (leader_daemon, leader_observe) = harness
        .wait_for_any_cli_contains(
            &control_daemons,
            &["observe"],
            "role=Leader",
            DELIVERY_TIMEOUT,
        )
        .await?;
    assert!(
        leader_observe.contains("analytics-demo"),
        "leader observe output should reflect deployed topology:\n{leader_observe}"
    );

    harness.run_cli(
        &daemon_b,
        &[
            "agent",
            "--node",
            "node-b",
            "--once",
            "--agent-state",
            &agent_state_b,
        ],
    )?;
    let list_b = harness
        .wait_for_cli_contains(
            &daemon_b,
            &["list", "--node", "node-b"],
            PROCESSOR_REPLICA,
            DELIVERY_TIMEOUT,
        )
        .await?;
    assert!(
        list_b.contains(PROCESSOR_REPLICA) && list_b.contains(SINK_REPLICA),
        "expected processor and sink replicas on node-b\nlist-b:\n{list_b}"
    );

    harness.run_cli(
        &daemon_a,
        &[
            "agent",
            "--node",
            "node-a",
            "--once",
            "--agent-state",
            &agent_state_a,
        ],
    )?;

    let running = harness
        .wait_for_cli_contains(
            &daemon_a,
            &["list", "--node", "node-a"],
            INGRESS_REPLICA,
            DELIVERY_TIMEOUT,
        )
        .await?;
    assert!(
        running.contains(INGRESS_REPLICA),
        "unexpected node-a list:\n{running}"
    );

    let discovered = harness.run_cli(
        &leader_daemon,
        &[
            "discover",
            "--workload-prefix",
            "tenant-a/analytics/topology-",
            "--json",
        ],
    )?;
    assert!(
        discovered.contains(WORKLOAD_KEY) && discovered.contains(ENDPOINT_KEY),
        "unexpected discovery state output:\n{discovered}"
    );

    let resolved = harness.run_cli(
        &leader_daemon,
        &["discover", "--resolve-endpoint", ENDPOINT_KEY, "--json"],
    )?;
    assert!(
        resolved.contains("\"kind\":\"endpoint\"")
            && resolved.contains(ENDPOINT_KEY)
            && resolved.contains(WORKLOAD_KEY),
        "unexpected discovery resolution output:\n{resolved}"
    );

    let nodes = harness.run_cli(
        &leader_daemon,
        &["nodes", "--max-staleness-ms", "20000", "--json"],
    )?;
    assert!(
        nodes.contains("\"name\":\"node-a\"")
            && nodes.contains("\"daemon_server_name\":")
            && nodes.contains("\"live\":true"),
        "unexpected nodes output:\n{nodes}"
    );

    let replay = harness
        .wait_for_cli_contains(
            &leader_daemon,
            &[
                "replay",
                "--workload-key",
                WORKLOAD_KEY,
                "--limit",
                "1",
                "--json",
            ],
            "\"events\":[{",
            DELIVERY_TIMEOUT,
        )
        .await?;
    assert!(
        replay.contains(WORKLOAD_KEY) && replay.contains("\"next_sequence\":"),
        "unexpected replay output:\n{replay}"
    );

    let usage = harness
        .wait_for_cli_contains(
            &daemon_a,
            &[
                "usage",
                "--node",
                "node-a",
                "--workload",
                WORKLOAD_KEY,
                "--limit",
                "1",
                "--json",
            ],
            WORKLOAD_KEY,
            USAGE_TIMEOUT,
        )
        .await?;
    assert!(
        usage.contains("\"records\":[{") && usage.contains("\"next_sequence\":"),
        "unexpected usage output:\n{usage}"
    );

    Ok(())
}
