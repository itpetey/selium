mod support;

use std::time::Duration;

use anyhow::{Context, Result, ensure};
use selium_control_plane_api::{
    BandwidthProfile, DeploymentSpec, ExternalAccountRef, IsolationProfile, WorkloadRef,
};
use selium_control_plane_core::Mutation;

use support::cluster_harness::{ClusterHarness, ClusterHarnessConfig};

const ACCOUNT: &str = "acct-123";
const MODULE: &str = "ingest.wasm";
const WORKLOAD_KEY: &str = "tenant-a/media/ingest";
const CONSENSUS_TIMEOUT: Duration = Duration::from_secs(180);

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires local runtime binaries plus wasm32 build target"]
async fn inventory_bootstrap_hands_off_to_replay_from_post_snapshot_cursor() -> Result<()> {
    let mut harness = ClusterHarness::new(ClusterHarnessConfig {
        name: "inventory-bootstrap-replay".to_string(),
        consensus_timeout: CONSENSUS_TIMEOUT,
        ..ClusterHarnessConfig::new("inventory-bootstrap-replay")
    })?;
    harness.prepare()?;
    harness.wait_for_consensus_ready().await?;

    let daemon_a = harness.daemon_addr_for("node-a")?;
    let workload = WorkloadRef {
        tenant: "tenant-a".to_string(),
        namespace: "media".to_string(),
        name: "ingest".to_string(),
    };

    let response = harness
        .mutate_control_plane(
            &daemon_a,
            "inventory-bootstrap-seed",
            Mutation::UpsertDeployment {
                spec: DeploymentSpec {
                    workload: workload.clone(),
                    module: MODULE.to_string(),
                    replicas: 1,
                    contracts: Vec::new(),
                    isolation: IsolationProfile::Standard,
                    cpu_millis: 250,
                    memory_mib: 128,
                    ephemeral_storage_mib: 0,
                    bandwidth_profile: BandwidthProfile::Standard,
                    volume_mounts: Vec::new(),
                    external_account_ref: Some(ExternalAccountRef {
                        key: ACCOUNT.to_string(),
                    }),
                },
            },
        )
        .await?;
    ensure!(
        response.committed,
        "seed deployment was not committed: {response:?}"
    );

    let scale_before_snapshot = harness.run_cli(
        &daemon_a,
        &[
            "scale",
            "--tenant",
            "tenant-a",
            "--namespace",
            "media",
            "--workload",
            "ingest",
            "--replicas",
            "2",
        ],
    )?;
    assert!(
        scale_before_snapshot.contains("scaled tenant-a/media/ingest to 2 replicas"),
        "unexpected pre-snapshot scale output: {scale_before_snapshot}"
    );

    let inventory = harness.run_cli(
        &daemon_a,
        &[
            "inventory",
            "--external-account-ref",
            ACCOUNT,
            "--workload",
            WORKLOAD_KEY,
            "--json",
        ],
    )?;
    assert!(
        inventory.contains(&format!("\"external_account_ref\":\"{ACCOUNT}\""))
            && inventory.contains(&format!("\"workload\":\"{WORKLOAD_KEY}\"")),
        "unexpected inventory output: {inventory}"
    );
    let last_applied = extract_u64_after(&inventory, "\"last_applied\":")?;
    let replay_since = last_applied.saturating_add(1);

    let scale_after_snapshot = harness.run_cli(
        &daemon_a,
        &[
            "scale",
            "--tenant",
            "tenant-a",
            "--namespace",
            "media",
            "--workload",
            "ingest",
            "--replicas",
            "3",
        ],
    )?;
    assert!(
        scale_after_snapshot.contains("scaled tenant-a/media/ingest to 3 replicas"),
        "unexpected post-snapshot scale output: {scale_after_snapshot}"
    );

    let replay = harness.run_cli(
        &daemon_a,
        &[
            "replay",
            "--since-sequence",
            &replay_since.to_string(),
            "--external-account-ref",
            ACCOUNT,
            "--workload-key",
            WORKLOAD_KEY,
            "--json",
        ],
    )?;
    assert_eq!(
        extract_u64_after(&replay, "\"start_sequence\":")?,
        replay_since
    );

    let events = events_json_slice(&replay)?;
    assert_eq!(
        events.match_indices("\"sequence\":").count(),
        1,
        "unexpected replay output: {replay}"
    );
    assert!(
        events.contains("\"mutation_kind\":\"set_scale\"")
            && events.contains("\"replicas\":3")
            && events.contains(&format!("\"external_account_ref\":\"{ACCOUNT}\"")),
        "unexpected replay events payload: {replay}"
    );
    assert!(
        !events.contains("\"replicas\":2"),
        "replay should exclude the pre-snapshot scale event: {replay}"
    );

    let replayed_sequence = extract_u64_after(events, "\"sequence\":")?;
    assert!(
        replayed_sequence >= replay_since,
        "replay event sequence {replayed_sequence} should be at or after resume cursor {replay_since}"
    );
    assert_eq!(
        extract_u64_after(&replay, "\"next_sequence\":")?,
        replayed_sequence.saturating_add(1),
        "unexpected replay cursor advance: {replay}"
    );

    Ok(())
}

fn extract_u64_after(haystack: &str, needle: &str) -> Result<u64> {
    let start = haystack
        .find(needle)
        .with_context(|| format!("missing `{needle}` in {haystack}"))?
        + needle.len();
    let digits = haystack[start..]
        .chars()
        .take_while(|ch| ch.is_ascii_digit())
        .collect::<String>();
    ensure!(
        !digits.is_empty(),
        "missing digits after `{needle}` in {haystack}"
    );
    digits
        .parse::<u64>()
        .with_context(|| format!("parse integer after `{needle}` from {haystack}"))
}

fn events_json_slice(replay: &str) -> Result<&str> {
    let marker = "\"events\":";
    let start = replay
        .find(marker)
        .with_context(|| format!("missing `{marker}` in {replay}"))?
        + marker.len();
    let bytes = replay.as_bytes();
    ensure!(
        bytes.get(start) == Some(&b'['),
        "expected array after `{marker}` in {replay}"
    );

    let mut depth = 0usize;
    for (offset, byte) in bytes[start..].iter().enumerate() {
        match byte {
            b'[' => depth += 1,
            b']' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Ok(&replay[start + 1..start + offset]);
                }
            }
            _ => {}
        }
    }

    anyhow::bail!("unterminated events array in {replay}")
}
