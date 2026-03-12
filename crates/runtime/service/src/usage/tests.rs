use super::*;
use std::path::PathBuf;
use tokio::time::sleep;

#[tokio::test]
async fn emits_interval_samples_with_accumulated_counters() {
    let collector = RuntimeUsageCollector::in_memory(Duration::from_millis(20));
    let handle = collector
        .register_process(
            "workload-a",
            "process-a",
            attribution("instance-a", "module-a", Some("acct-a")),
        )
        .await
        .expect("register process");
    handle.set_memory_high_watermark_bytes(4096);
    handle.record_network_ingress(7);
    handle.record_network_egress(11);
    handle.record_storage_read(13);
    handle.record_storage_write(17);
    sleep(Duration::from_millis(35)).await;
    handle.finish().await.expect("finish usage handle");

    let first = &collector.read_samples().await[0];
    assert_eq!(first.trigger, RuntimeUsageSampleTrigger::Interval);
    assert_eq!(first.ingress_bytes, 7);
    assert_eq!(first.egress_bytes, 11);
    assert_eq!(first.storage_read_bytes, 13);
    assert_eq!(first.storage_write_bytes, 17);
    assert_eq!(first.attribution.module_id, "module-a");
    assert_eq!(
        first.attribution.external_account_ref.as_deref(),
        Some("acct-a")
    );
    assert!(first.memory_byte_millis > 0);
}

#[tokio::test]
async fn replay_usage_filters_by_attribution_and_time_window() {
    let collector = RuntimeUsageCollector::in_memory(Duration::from_secs(60));
    let first = collector
        .register_process(
            "workload-a",
            "process-a",
            attribution("instance-a", "module-a", Some("acct-a")),
        )
        .await
        .expect("register first process");
    first.record_network_ingress(3);
    sleep(Duration::from_millis(2)).await;
    first.finish().await.expect("finish first process");
    sleep(Duration::from_millis(2)).await;

    let second = collector
        .register_process(
            "workload-b",
            "process-b",
            attribution("instance-b", "module-b", Some("acct-b")),
        )
        .await
        .expect("register second process");
    second.record_network_ingress(5);
    sleep(Duration::from_millis(2)).await;
    second.finish().await.expect("finish second process");

    let all = replay_all(&collector).await;
    let first_record = all.records.first().expect("first usage record");
    let result = collector
        .replay_usage(&RuntimeUsageQuery {
            start: RuntimeUsageReplayStart::Earliest,
            save_checkpoint: None,
            limit: 10,
            external_account_ref: Some("acct-a".to_string()),
            workload: Some("workload-a".to_string()),
            module: Some("module-a".to_string()),
            window_start_ms: Some(first_record.sample.window_start_ms),
            window_end_ms: Some(first_record.sample.window_end_ms.saturating_add(1)),
        })
        .await
        .expect("query filtered usage");

    assert_eq!(result.records.len(), 1);
    let record = &result.records[0];
    assert_eq!(record.sample.process_id, "process-a");
    assert_eq!(
        record.headers.get("module_id").map(String::as_str),
        Some("module-a")
    );
    assert_eq!(
        record.headers.get("instance_id").map(String::as_str),
        Some("instance-a")
    );
    assert_eq!(
        record
            .headers
            .get("external_account_ref")
            .map(String::as_str),
        Some("acct-a")
    );
    assert_eq!(result.high_watermark, all.high_watermark);
}

#[tokio::test]
async fn restart_recovery_advances_next_sample_window() {
    let root = temp_dir();
    let collector = RuntimeUsageCollector::file_backed(&root, Duration::from_millis(20))
        .expect("create collector");
    let first = collector
        .register_process(
            "workload-a",
            "process-a",
            attribution("instance-a", "module-a", Some("acct-a")),
        )
        .await
        .expect("register first process");
    first.set_memory_high_watermark_bytes(2048);
    first.record_network_ingress(3);
    sleep(Duration::from_millis(25)).await;
    first.finish().await.expect("finish first process");
    sleep(Duration::from_millis(5)).await;

    let recovered = RuntimeUsageCollector::file_backed(&root, Duration::from_millis(20))
        .expect("reopen collector");
    let second = recovered
        .register_process(
            "workload-a",
            "process-b",
            attribution("instance-a", "module-a", Some("acct-a")),
        )
        .await
        .expect("register recovered process");
    second.set_memory_high_watermark_bytes(2048);
    second.record_network_ingress(5);
    sleep(Duration::from_millis(25)).await;
    second.finish().await.expect("finish recovered process");

    let samples = recovered.read_samples().await;
    let second_start = samples
        .iter()
        .find(|sample| sample.process_id == "process-b")
        .expect("sample for recovered process")
        .window_start_ms;
    assert!(second_start >= samples[0].window_end_ms);

    let replayed = recovered
        .replay_usage(&RuntimeUsageQuery {
            start: RuntimeUsageReplayStart::Earliest,
            save_checkpoint: None,
            limit: 10,
            external_account_ref: Some("acct-a".to_string()),
            workload: Some("workload-a".to_string()),
            module: Some("module-a".to_string()),
            window_start_ms: None,
            window_end_ms: None,
        })
        .await
        .expect("replay recovered usage");
    assert!(
        replayed
            .records
            .iter()
            .any(|record| record.sample.process_id == "process-b")
    );
    assert!(
        replayed
            .records
            .iter()
            .all(|record| record.sample.attribution.module_id == "module-a")
    );
}

async fn replay_all(collector: &RuntimeUsageCollector) -> RuntimeUsageQueryResult {
    collector
        .replay_usage(&RuntimeUsageQuery {
            start: RuntimeUsageReplayStart::Earliest,
            save_checkpoint: None,
            limit: 10,
            external_account_ref: None,
            workload: None,
            module: None,
            window_start_ms: None,
            window_end_ms: None,
        })
        .await
        .expect("replay all usage")
}

#[tokio::test]
async fn usage_headers_include_instance_and_external_account_metadata() {
    let collector = RuntimeUsageCollector::in_memory(Duration::from_secs(60));
    let handle = collector
        .register_process(
            "tenant-a/media/ingest",
            "process-a",
            attribution("tenant-a/media/ingest/0", "module-a", Some("acct-a")),
        )
        .await
        .expect("register process");
    sleep(Duration::from_millis(2)).await;
    handle.finish().await.expect("finish process");

    let records = replay_all(&collector).await.records;
    let headers = &records[0].headers;
    assert_eq!(
        headers.get("workload_key").map(String::as_str),
        Some("tenant-a/media/ingest")
    );
    assert_eq!(
        headers.get("module_id").map(String::as_str),
        Some("module-a")
    );
    assert_eq!(
        headers.get("instance_id").map(String::as_str),
        Some("tenant-a/media/ingest/0")
    );
    assert_eq!(
        headers.get("external_account_ref").map(String::as_str),
        Some("acct-a")
    );
}

fn attribution(
    instance_id: &str,
    module_id: &str,
    external_account_ref: Option<&str>,
) -> ProcessUsageAttribution {
    ProcessUsageAttribution {
        instance_id: Some(instance_id.to_string()),
        external_account_ref: external_account_ref.map(ToString::to_string),
        module_id: module_id.to_string(),
    }
}

fn temp_dir() -> PathBuf {
    let id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    std::env::temp_dir().join(format!("selium-runtime-usage-tests-{id}"))
}
