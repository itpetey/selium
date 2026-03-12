use super::*;

pub(super) fn observe_value(
    summary: DataValue,
    status: &StatusApiResponse,
    metrics: &MetricsApiResponse,
) -> DataValue {
    DataValue::Map(BTreeMap::from([
        ("summary".to_string(), summary),
        ("health".to_string(), status_value(status)),
        ("metrics".to_string(), metrics_value(metrics)),
    ]))
}

pub(super) fn status_value(status: &StatusApiResponse) -> DataValue {
    DataValue::Map(BTreeMap::from([
        (
            "node_id".to_string(),
            DataValue::from(status.node_id.clone()),
        ),
        ("role".to_string(), DataValue::from(status.role.clone())),
        (
            "current_term".to_string(),
            DataValue::from(status.current_term),
        ),
        (
            "leader_id".to_string(),
            status
                .leader_id
                .clone()
                .map(DataValue::from)
                .unwrap_or(DataValue::Null),
        ),
        (
            "commit_index".to_string(),
            DataValue::from(status.commit_index),
        ),
        (
            "last_applied".to_string(),
            DataValue::from(status.last_applied),
        ),
        (
            "peers".to_string(),
            DataValue::List(status.peers.iter().cloned().map(DataValue::from).collect()),
        ),
        (
            "table_count".to_string(),
            DataValue::from(status.table_count),
        ),
        (
            "durable_events".to_string(),
            status
                .durable_events
                .map(DataValue::from)
                .unwrap_or(DataValue::Null),
        ),
    ]))
}

pub(super) fn metrics_value(metrics: &MetricsApiResponse) -> DataValue {
    DataValue::Map(BTreeMap::from([
        (
            "node_id".to_string(),
            DataValue::from(metrics.node_id.clone()),
        ),
        (
            "deployment_count".to_string(),
            DataValue::from(metrics.deployment_count),
        ),
        (
            "pipeline_count".to_string(),
            DataValue::from(metrics.pipeline_count),
        ),
        (
            "node_count".to_string(),
            DataValue::from(metrics.node_count),
        ),
        (
            "peer_count".to_string(),
            DataValue::from(metrics.peer_count),
        ),
        (
            "table_count".to_string(),
            DataValue::from(metrics.table_count),
        ),
        (
            "commit_index".to_string(),
            DataValue::from(metrics.commit_index),
        ),
        (
            "last_applied".to_string(),
            DataValue::from(metrics.last_applied),
        ),
        (
            "durable_events".to_string(),
            metrics
                .durable_events
                .map(DataValue::from)
                .unwrap_or(DataValue::Null),
        ),
    ]))
}

pub(super) fn optional_u64_value(value: Option<u64>) -> DataValue {
    value.map(DataValue::from).unwrap_or(DataValue::Null)
}

pub(super) fn replay_response_value(replay: &ReplayApiResponse) -> DataValue {
    DataValue::Map(BTreeMap::from([
        (
            "start_sequence".to_string(),
            optional_u64_value(replay.start_sequence),
        ),
        (
            "next_sequence".to_string(),
            optional_u64_value(replay.next_sequence),
        ),
        (
            "high_watermark".to_string(),
            optional_u64_value(replay.high_watermark),
        ),
        ("events".to_string(), DataValue::List(replay.events.clone())),
    ]))
}

pub(super) fn optional_json_u64(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "null".to_string())
}

pub(super) fn optional_json_str(value: Option<&str>) -> String {
    value
        .map(|value| format!("\"{}\"", escape_json(value)))
        .unwrap_or_else(|| "null".to_string())
}

pub(super) fn string_map_json(map: &BTreeMap<String, String>) -> String {
    let pairs = map
        .iter()
        .map(|(key, value)| format!("\"{}\":\"{}\"", escape_json(key), escape_json(value)))
        .collect::<Vec<_>>()
        .join(",");
    format!("{{{pairs}}}")
}

pub(super) fn runtime_usage_record_json(record: &RuntimeUsageRecord) -> String {
    let sample = &record.sample;
    format!(
        concat!(
            "{{",
            "\"sequence\":{},",
            "\"timestamp_ms\":{},",
            "\"headers\":{},",
            "\"sample\":{{",
            "\"workload_key\":\"{}\",",
            "\"process_id\":\"{}\",",
            "\"attribution\":{{\"external_account_ref\":{},\"module_id\":\"{}\"}},",
            "\"window_start_ms\":{},",
            "\"window_end_ms\":{},",
            "\"trigger\":\"{:?}\",",
            "\"cpu_time_millis\":{},",
            "\"memory_high_watermark_bytes\":{},",
            "\"memory_byte_millis\":{},",
            "\"ingress_bytes\":{},",
            "\"egress_bytes\":{},",
            "\"storage_read_bytes\":{},",
            "\"storage_write_bytes\":{}",
            "}}",
            "}}"
        ),
        record.sequence,
        record.timestamp_ms,
        string_map_json(&record.headers),
        escape_json(&sample.workload_key),
        escape_json(&sample.process_id),
        optional_json_str(sample.attribution.external_account_ref.as_deref()),
        escape_json(&sample.attribution.module_id),
        sample.window_start_ms,
        sample.window_end_ms,
        sample.trigger,
        sample.cpu_time_millis,
        sample.memory_high_watermark_bytes,
        sample.memory_byte_millis,
        sample.ingress_bytes,
        sample.egress_bytes,
        sample.storage_read_bytes,
        sample.storage_write_bytes,
    )
}

pub(super) fn runtime_usage_response_json(response: &RuntimeUsageApiResponse) -> String {
    let records = response
        .records
        .iter()
        .map(runtime_usage_record_json)
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "{{\"records\":[{records}],\"next_sequence\":{},\"high_watermark\":{}}}",
        optional_json_u64(response.next_sequence),
        optional_json_u64(response.high_watermark)
    )
}

pub(super) fn write_json_string_content(out: &mut String, value: &str) {
    use std::fmt::Write as _;

    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            ch if ch.is_control() => {
                let _ = write!(out, "\\u{:04x}", ch as u32);
            }
            ch => out.push(ch),
        }
    }
}

pub(super) fn escape_json(value: &str) -> String {
    let mut out = String::new();
    write_json_string_content(&mut out, value);
    out
}

pub(super) fn print_instance_map(instances: &BTreeMap<String, usize>) {
    for (instance, pid) in instances {
        println!("{instance} {pid}");
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub(super) fn render_guest_log_payload(
    endpoint: &PublicEndpointRef,
    payload: &[u8],
    label_streams: bool,
) -> Vec<u8> {
    if !label_streams {
        return payload.to_vec();
    }

    let prefix = format!("[{}] ", endpoint.name);
    let text = String::from_utf8_lossy(payload);
    let mut rendered = Vec::with_capacity(payload.len() + prefix.len());
    for line in text.split_inclusive('\n') {
        if line.is_empty() {
            continue;
        }
        rendered.extend_from_slice(prefix.as_bytes());
        rendered.extend_from_slice(line.as_bytes());
    }
    rendered
}

pub(super) fn format_contract(contract: ContractRef) -> String {
    format!(
        "{}/{}:{}@{}",
        contract.namespace,
        contract.kind.as_str(),
        contract.name,
        contract.version
    )
}
