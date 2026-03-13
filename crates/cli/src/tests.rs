use super::*;

#[test]
fn module_spec_contains_expected_capabilities() {
    let spec = build_module_spec(
        "echo.wasm",
        AdaptorArg::Wasmtime,
        IsolationArg::Standard,
        default_runtime_capabilities(),
    );
    assert!(spec.contains("path=echo.wasm"));
    assert!(spec.contains("adaptor=wasmtime"));
    assert!(spec.contains("queue_writer"));
}

#[test]
fn observe_value_groups_health_and_metrics() {
    let value = observe_value(
        DataValue::Map(BTreeMap::from([(
            "deployments".to_string(),
            DataValue::List(vec![DataValue::from("tenant-a/media/router")]),
        )])),
        &StatusApiResponse {
            node_id: "node-a".to_string(),
            role: "Leader".to_string(),
            current_term: 3,
            leader_id: Some("node-a".to_string()),
            commit_index: 9,
            last_applied: 9,
            peers: vec!["node-b".to_string()],
            table_count: 2,
            durable_events: Some(10),
        },
        &MetricsApiResponse {
            node_id: "node-a".to_string(),
            deployment_count: 1,
            pipeline_count: 0,
            node_count: 2,
            peer_count: 1,
            table_count: 2,
            commit_index: 9,
            last_applied: 9,
            durable_events: Some(10),
        },
    );

    assert_eq!(
        value
            .get("health")
            .and_then(|health| health.get("role"))
            .and_then(DataValue::as_str),
        Some("Leader")
    );
    assert_eq!(
        value
            .get("metrics")
            .and_then(|metrics| metrics.get("deployment_count"))
            .and_then(DataValue::as_u64),
        Some(1)
    );
}

#[test]
fn data_value_to_json_escapes_strings() {
    let value = DataValue::Map(BTreeMap::from([(
        "note".to_string(),
        DataValue::from("line\n\"two\""),
    )]));

    assert_eq!(
        data_value_to_json(&value),
        "{\"note\":\"line\\n\\\"two\\\"\"}"
    );
}

#[test]
fn replay_response_value_preserves_cursor_fields() {
    let value = replay_response_value(&ReplayApiResponse {
        events: vec![DataValue::Map(BTreeMap::from([(
            "sequence".to_string(),
            DataValue::from(41_u64),
        )]))],
        start_sequence: Some(41),
        next_sequence: Some(42),
        high_watermark: Some(56),
    });

    assert_eq!(
        value.get("start_sequence").and_then(DataValue::as_u64),
        Some(41)
    );
    assert_eq!(
        value.get("next_sequence").and_then(DataValue::as_u64),
        Some(42)
    );
    assert_eq!(
        value.get("high_watermark").and_then(DataValue::as_u64),
        Some(56)
    );
    assert_eq!(list_len(&value, "events"), 1);
}

#[test]
fn replay_json_preserves_external_consumer_cursor_and_event_fields() {
    let json = data_value_to_json(&replay_response_value(&ReplayApiResponse {
        events: vec![DataValue::Map(BTreeMap::from([
            (
                "external_account_ref".to_string(),
                DataValue::from("acct-123"),
            ),
            (
                "headers".to_string(),
                DataValue::Map(BTreeMap::from([(
                    "mutation_kind".to_string(),
                    DataValue::from("set_scale"),
                )])),
            ),
            ("replicas".to_string(), DataValue::from(3_u64)),
            ("sequence".to_string(), DataValue::from(41_u64)),
            (
                "workload".to_string(),
                DataValue::from("tenant-a/media/ingest"),
            ),
        ]))],
        start_sequence: Some(41),
        next_sequence: Some(42),
        high_watermark: Some(56),
    }));

    assert_eq!(
        json,
        concat!(
            "{\"events\":[{",
            "\"external_account_ref\":\"acct-123\",",
            "\"headers\":{\"mutation_kind\":\"set_scale\"},",
            "\"replicas\":3,",
            "\"sequence\":41,",
            "\"workload\":\"tenant-a/media/ingest\"",
            "}],\"high_watermark\":56,\"next_sequence\":42,\"start_sequence\":41}"
        )
    );
}

#[test]
fn data_value_to_json_preserves_inventory_snapshot_marker() {
    let value = DataValue::Map(BTreeMap::from([
        (
            "snapshot_marker".to_string(),
            DataValue::Map(BTreeMap::from([(
                "last_applied".to_string(),
                DataValue::from(41_u64),
            )])),
        ),
        ("workloads".to_string(), DataValue::List(Vec::new())),
    ]));

    assert_eq!(
        data_value_to_json(&value),
        "{\"snapshot_marker\":{\"last_applied\":41},\"workloads\":[]}"
    );
}

#[test]
fn inventory_json_preserves_external_consumer_filter_and_resume_fields() {
    let scheduled_instance = DataValue::Map(BTreeMap::from([
        (
            "deployment".to_string(),
            DataValue::from("tenant-a/media/ingest"),
        ),
        (
            "external_account_ref".to_string(),
            DataValue::from("acct-123"),
        ),
        (
            "instance_id".to_string(),
            DataValue::from("node-a/ingest-0"),
        ),
        ("isolation".to_string(), DataValue::from("Standard")),
        ("module".to_string(), DataValue::from("ingest.wasm")),
        ("node".to_string(), DataValue::from("node-a")),
        (
            "workload".to_string(),
            DataValue::from("tenant-a/media/ingest"),
        ),
    ]));
    let value = DataValue::Map(BTreeMap::from([
        (
            "snapshot_marker".to_string(),
            DataValue::Map(BTreeMap::from([(
                "last_applied".to_string(),
                DataValue::from(41_u64),
            )])),
        ),
        (
            "filters".to_string(),
            DataValue::Map(BTreeMap::from([
                (
                    "external_account_ref".to_string(),
                    DataValue::from("acct-123"),
                ),
                ("module".to_string(), DataValue::from("ingest.wasm")),
                ("node".to_string(), DataValue::from("node-a")),
                (
                    "pipeline".to_string(),
                    DataValue::from("tenant-a/media/camera"),
                ),
                (
                    "workload".to_string(),
                    DataValue::from("tenant-a/media/ingest"),
                ),
            ])),
        ),
        (
            "workloads".to_string(),
            DataValue::List(vec![DataValue::Map(BTreeMap::from([
                ("contracts".to_string(), DataValue::List(Vec::new())),
                (
                    "external_account_ref".to_string(),
                    DataValue::from("acct-123"),
                ),
                ("isolation".to_string(), DataValue::from("Standard")),
                ("module".to_string(), DataValue::from("ingest.wasm")),
                (
                    "nodes".to_string(),
                    DataValue::List(vec![DataValue::from("node-a")]),
                ),
                ("replicas".to_string(), DataValue::from(1_u64)),
                (
                    "resources".to_string(),
                    DataValue::Map(BTreeMap::from([
                        ("bandwidth_profile".to_string(), DataValue::from("Standard")),
                        ("cpu_millis".to_string(), DataValue::from(250_u64)),
                        ("ephemeral_storage_mib".to_string(), DataValue::from(0_u64)),
                        ("memory_mib".to_string(), DataValue::from(128_u64)),
                    ])),
                ),
                (
                    "scheduled_instances".to_string(),
                    DataValue::List(vec![scheduled_instance.clone()]),
                ),
                (
                    "workload".to_string(),
                    DataValue::from("tenant-a/media/ingest"),
                ),
            ]))]),
        ),
        (
            "pipelines".to_string(),
            DataValue::List(vec![DataValue::Map(BTreeMap::from([
                ("edge_count".to_string(), DataValue::from(0_u64)),
                ("edges".to_string(), DataValue::List(Vec::new())),
                (
                    "external_account_ref".to_string(),
                    DataValue::from("acct-123"),
                ),
                (
                    "pipeline".to_string(),
                    DataValue::from("tenant-a/media/camera"),
                ),
                ("workloads".to_string(), DataValue::List(Vec::new())),
            ]))]),
        ),
        (
            "modules".to_string(),
            DataValue::List(vec![DataValue::Map(BTreeMap::from([
                ("deployment_count".to_string(), DataValue::from(1_u64)),
                (
                    "external_account_refs".to_string(),
                    DataValue::List(vec![DataValue::from("acct-123")]),
                ),
                ("module".to_string(), DataValue::from("ingest.wasm")),
                (
                    "nodes".to_string(),
                    DataValue::List(vec![DataValue::from("node-a")]),
                ),
                (
                    "workloads".to_string(),
                    DataValue::List(vec![DataValue::from("tenant-a/media/ingest")]),
                ),
            ]))]),
        ),
        (
            "nodes".to_string(),
            DataValue::List(vec![DataValue::Map(BTreeMap::from([
                (
                    "allocatable_cpu_millis".to_string(),
                    DataValue::from(2_000_u64),
                ),
                (
                    "allocatable_memory_mib".to_string(),
                    DataValue::from(4_096_u64),
                ),
                ("capacity_slots".to_string(), DataValue::from(8_u64)),
                ("daemon_addr".to_string(), DataValue::from("127.0.0.1:7200")),
                (
                    "daemon_server_name".to_string(),
                    DataValue::from("selium-node-a"),
                ),
                (
                    "external_account_refs".to_string(),
                    DataValue::List(vec![DataValue::from("acct-123")]),
                ),
                ("last_heartbeat_ms".to_string(), DataValue::from(100_u64)),
                ("name".to_string(), DataValue::from("node-a")),
                (
                    "scheduled_instances".to_string(),
                    DataValue::List(vec![scheduled_instance]),
                ),
                (
                    "supported_isolation".to_string(),
                    DataValue::List(vec![DataValue::from("Standard")]),
                ),
            ]))]),
        ),
    ]));

    assert_eq!(
        data_value_to_json(&value),
        concat!(
            "{\"filters\":{\"external_account_ref\":\"acct-123\",\"module\":\"ingest.wasm\",\"node\":\"node-a\",\"pipeline\":\"tenant-a/media/camera\",\"workload\":\"tenant-a/media/ingest\"},",
            "\"modules\":[{\"deployment_count\":1,\"external_account_refs\":[\"acct-123\"],\"module\":\"ingest.wasm\",\"nodes\":[\"node-a\"],\"workloads\":[\"tenant-a/media/ingest\"]}],",
            "\"nodes\":[{\"allocatable_cpu_millis\":2000,\"allocatable_memory_mib\":4096,\"capacity_slots\":8,\"daemon_addr\":\"127.0.0.1:7200\",\"daemon_server_name\":\"selium-node-a\",\"external_account_refs\":[\"acct-123\"],\"last_heartbeat_ms\":100,\"name\":\"node-a\",\"scheduled_instances\":[{\"deployment\":\"tenant-a/media/ingest\",\"external_account_ref\":\"acct-123\",\"instance_id\":\"node-a/ingest-0\",\"isolation\":\"Standard\",\"module\":\"ingest.wasm\",\"node\":\"node-a\",\"workload\":\"tenant-a/media/ingest\"}],\"supported_isolation\":[\"Standard\"]}],",
            "\"pipelines\":[{\"edge_count\":0,\"edges\":[],\"external_account_ref\":\"acct-123\",\"pipeline\":\"tenant-a/media/camera\",\"workloads\":[]}],",
            "\"snapshot_marker\":{\"last_applied\":41},",
            "\"workloads\":[{\"contracts\":[],\"external_account_ref\":\"acct-123\",\"isolation\":\"Standard\",\"module\":\"ingest.wasm\",\"nodes\":[\"node-a\"],\"replicas\":1,\"resources\":{\"bandwidth_profile\":\"Standard\",\"cpu_millis\":250,\"ephemeral_storage_mib\":0,\"memory_mib\":128},\"scheduled_instances\":[{\"deployment\":\"tenant-a/media/ingest\",\"external_account_ref\":\"acct-123\",\"instance_id\":\"node-a/ingest-0\",\"isolation\":\"Standard\",\"module\":\"ingest.wasm\",\"node\":\"node-a\",\"workload\":\"tenant-a/media/ingest\"}],\"workload\":\"tenant-a/media/ingest\"}]}"
        )
    );
}

#[test]
fn discovery_state_value_preserves_machine_consumable_output_shape() {
    let workload = workload_ref(
        "tenant-a".to_string(),
        "media".to_string(),
        "router".to_string(),
    );
    let endpoint = PublicEndpointRef {
        workload: workload.clone(),
        kind: ContractKind::Service,
        name: "camera.detect".to_string(),
    };
    let discovery = DiscoveryState {
        workloads: vec![DiscoverableWorkload {
            workload: workload.clone(),
            endpoints: vec![endpoint.clone()],
        }],
        endpoints: vec![DiscoverableEndpoint {
            endpoint: endpoint.clone(),
            contract: Some(ContractRef {
                namespace: "media.pipeline".to_string(),
                kind: ContractKind::Service,
                name: "camera.detect".to_string(),
                version: "v1".to_string(),
            }),
        }],
    };

    assert_eq!(
        data_value_to_json(&discovery_state_value(&discovery)),
        "{\"endpoints\":[{\"contract\":{\"kind\":\"service\",\"name\":\"camera.detect\",\"namespace\":\"media.pipeline\",\"version\":\"v1\"},\"endpoint\":{\"endpoint\":\"tenant-a/media/router#service:camera.detect\",\"kind\":\"service\",\"name\":\"camera.detect\",\"workload\":\"tenant-a/media/router\"}}],\"workloads\":[{\"endpoints\":[{\"endpoint\":\"tenant-a/media/router#service:camera.detect\",\"kind\":\"service\",\"name\":\"camera.detect\",\"workload\":\"tenant-a/media/router\"}],\"workload\":\"tenant-a/media/router\"}]}"
    );
}

#[test]
fn data_value_to_json_preserves_nodes_live_capability_fields() {
    let value = DataValue::Map(BTreeMap::from([
        ("now_ms".to_string(), DataValue::from(100_u64)),
        (
            "nodes".to_string(),
            DataValue::List(vec![DataValue::Map(BTreeMap::from([
                ("name".to_string(), DataValue::from("node-a")),
                ("daemon_addr".to_string(), DataValue::from("127.0.0.1:7200")),
                (
                    "daemon_server_name".to_string(),
                    DataValue::from("selium-node-a"),
                ),
                ("live".to_string(), DataValue::from(true)),
                (
                    "supported_isolation".to_string(),
                    DataValue::List(vec![
                        DataValue::from("standard"),
                        DataValue::from("hardened"),
                    ]),
                ),
            ]))]),
        ),
    ]));

    assert_eq!(
        data_value_to_json(&value),
        "{\"nodes\":[{\"daemon_addr\":\"127.0.0.1:7200\",\"daemon_server_name\":\"selium-node-a\",\"live\":true,\"name\":\"node-a\",\"supported_isolation\":[\"standard\",\"hardened\"]}],\"now_ms\":100}"
    );
}

#[test]
fn discover_query_defaults_scope_to_resolved_endpoint_workload() {
    let DiscoverQuery::Resolve(request) = discover_query(&DiscoverArgs {
        workloads: Vec::new(),
        workload_prefixes: Vec::new(),
        resolve_workload: None,
        resolve_endpoint: Some("tenant-a/media/router#service:camera.detect".to_string()),
        resolve_running_workload: None,
        resolve_replica_key: None,
        allow_operational_processes: false,
        json: true,
    })
    .expect("build discovery request") else {
        panic!("expected resolve request");
    };

    assert_eq!(request.operation, DiscoveryOperation::Bind);
    assert_eq!(request.scope.operations, vec![DiscoveryOperation::Bind]);
    assert_eq!(request.scope.workloads.len(), 1);
    assert_eq!(
        request.scope.workloads[0],
        DiscoveryPattern::Exact("tenant-a/media/router".to_string())
    );
}

#[test]
fn discovery_resolution_value_preserves_running_process_fields() {
    let value = discovery_resolution_value(&DiscoveryResolution::RunningProcess(
        OperationalProcessRecord {
            workload: workload_ref(
                "tenant-a".to_string(),
                "media".to_string(),
                "router".to_string(),
            ),
            replica_key: "router-0001".to_string(),
            node: "node-a".to_string(),
        },
    ));

    assert_eq!(
        value.get("kind").and_then(DataValue::as_str),
        Some("running_process")
    );
    assert_eq!(
        value.get("workload").and_then(DataValue::as_str),
        Some("tenant-a/media/router")
    );
    assert_eq!(
        value.get("replica_key").and_then(DataValue::as_str),
        Some("router-0001")
    );
    assert_eq!(
        value.get("node").and_then(DataValue::as_str),
        Some("node-a")
    );
}

#[test]
fn discover_resolution_json_preserves_endpoint_contract_fields() {
    let json = data_value_to_json(&discovery_resolution_value(&DiscoveryResolution::Endpoint(
        ResolvedEndpoint {
            endpoint: PublicEndpointRef {
                workload: workload_ref(
                    "tenant-a".to_string(),
                    "analytics".to_string(),
                    "topology-ingress".to_string(),
                ),
                kind: ContractKind::Event,
                name: "ingest.frames".to_string(),
            },
            contract: Some(ContractRef {
                namespace: "analytics.topology".to_string(),
                kind: ContractKind::Event,
                name: "ingest.frames".to_string(),
                version: "v1".to_string(),
            }),
        },
    )));

    assert_eq!(
        json,
        concat!(
            "{\"contract\":{\"kind\":\"event\",\"name\":\"ingest.frames\",\"namespace\":\"analytics.topology\",\"version\":\"v1\"},",
            "\"endpoint\":{\"endpoint\":\"tenant-a/analytics/topology-ingress#event:ingest.frames\",\"kind\":\"event\",\"name\":\"ingest.frames\",\"workload\":\"tenant-a/analytics/topology-ingress\"},",
            "\"kind\":\"endpoint\"}"
        )
    );
}

#[test]
fn replay_request_prefers_workload_key_filter() {
    let request = replay_request(&ReplayArgs {
        limit: 25,
        since_sequence: Some(41),
        external_account_ref: Some("acct-123".to_string()),
        workload_key: Some("tenant-a/media/ingest".to_string()),
        tenant: None,
        namespace: None,
        workload: None,
        module: Some("ingest.wasm".to_string()),
        pipeline: Some("tenant-a/media/camera".to_string()),
        node: Some("node-a".to_string()),
        json: true,
    })
    .expect("build replay request");

    assert_eq!(request.since_sequence, Some(41));
    assert_eq!(request.external_account_ref.as_deref(), Some("acct-123"));
    assert_eq!(request.workload.as_deref(), Some("tenant-a/media/ingest"));
    assert_eq!(request.module.as_deref(), Some("ingest.wasm"));
    assert_eq!(request.pipeline.as_deref(), Some("tenant-a/media/camera"));
    assert_eq!(request.node.as_deref(), Some("node-a"));
}

#[test]
fn replay_request_rejects_mixed_workload_filters() {
    let err = replay_request(&ReplayArgs {
        limit: 10,
        since_sequence: None,
        external_account_ref: None,
        workload_key: Some("tenant-a/media/ingest".to_string()),
        tenant: Some("tenant-a".to_string()),
        namespace: Some("media".to_string()),
        workload: Some("ingest".to_string()),
        module: None,
        pipeline: None,
        node: None,
        json: false,
    })
    .expect_err("mixed workload filters should fail");

    assert!(
        err.to_string()
            .contains("either --workload-key or --tenant/--namespace/--workload")
    );
}

#[test]
fn runtime_usage_request_preserves_filters_and_cursor() {
    let request = runtime_usage_request(&UsageArgs {
        node: "node-a".to_string(),
        limit: 25,
        latest: false,
        since_sequence: Some(41),
        since_timestamp_ms: None,
        external_account_ref: Some("acct-123".to_string()),
        workload: Some("tenant-a/media/ingest".to_string()),
        module: Some("ingest.wasm".to_string()),
        window_start_ms: Some(1_000),
        window_end_ms: Some(2_000),
        json: true,
    })
    .expect("build runtime usage request");

    assert_eq!(request.node_id, "node-a");
    assert_eq!(request.query.start, RuntimeUsageReplayStart::Sequence(41));
    assert_eq!(
        request.query.external_account_ref.as_deref(),
        Some("acct-123")
    );
    assert_eq!(
        request.query.workload.as_deref(),
        Some("tenant-a/media/ingest")
    );
    assert_eq!(request.query.module.as_deref(), Some("ingest.wasm"));
    assert_eq!(request.query.window_start_ms, Some(1_000));
    assert_eq!(request.query.window_end_ms, Some(2_000));
}

#[test]
fn runtime_usage_response_json_preserves_cursor_fields() {
    let json = runtime_usage_response_json(&RuntimeUsageApiResponse {
        records: vec![RuntimeUsageRecord {
            sequence: 41,
            timestamp_ms: 1_500,
            headers: BTreeMap::from([("external_account_ref".to_string(), "acct-123".to_string())]),
            sample: selium_abi::RuntimeUsageSample {
                workload_key: "tenant-a/media/ingest".to_string(),
                process_id: "process-7".to_string(),
                attribution: selium_abi::RuntimeUsageAttribution {
                    external_account_ref: Some("acct-123".to_string()),
                    module_id: "ingest.wasm".to_string(),
                },
                window_start_ms: 1_000,
                window_end_ms: 2_000,
                trigger: selium_abi::RuntimeUsageSampleTrigger::Interval,
                cpu_time_millis: 10,
                memory_high_watermark_bytes: 4096,
                memory_byte_millis: 8192,
                ingress_bytes: 7,
                egress_bytes: 11,
                storage_read_bytes: 13,
                storage_write_bytes: 17,
            },
        }],
        next_sequence: Some(42),
        high_watermark: Some(56),
    });

    assert!(json.contains("\"next_sequence\":42"));
    assert!(json.contains("\"high_watermark\":56"));
    assert!(json.contains("\"memory_byte_millis\":8192"));
    assert!(json.contains("\"module_id\":\"ingest.wasm\""));
}

#[test]
fn usage_json_preserves_external_consumer_cursor_and_attribution_fields() {
    let json = runtime_usage_response_json(&RuntimeUsageApiResponse {
        records: vec![RuntimeUsageRecord {
            sequence: 41,
            timestamp_ms: 1_500,
            headers: BTreeMap::from([("external_account_ref".to_string(), "acct-123".to_string())]),
            sample: selium_abi::RuntimeUsageSample {
                workload_key: "tenant-a/media/ingest".to_string(),
                process_id: "process-7".to_string(),
                attribution: selium_abi::RuntimeUsageAttribution {
                    external_account_ref: Some("acct-123".to_string()),
                    module_id: "ingest.wasm".to_string(),
                },
                window_start_ms: 1_000,
                window_end_ms: 2_000,
                trigger: selium_abi::RuntimeUsageSampleTrigger::Interval,
                cpu_time_millis: 10,
                memory_high_watermark_bytes: 4_096,
                memory_byte_millis: 8_192,
                ingress_bytes: 7,
                egress_bytes: 11,
                storage_read_bytes: 13,
                storage_write_bytes: 17,
            },
        }],
        next_sequence: Some(42),
        high_watermark: Some(56),
    });

    assert_eq!(
        json,
        concat!(
            "{\"records\":[{\"sequence\":41,\"timestamp_ms\":1500,\"headers\":{\"external_account_ref\":\"acct-123\"},",
            "\"sample\":{\"workload_key\":\"tenant-a/media/ingest\",\"process_id\":\"process-7\",\"attribution\":{\"external_account_ref\":\"acct-123\",\"module_id\":\"ingest.wasm\"},\"window_start_ms\":1000,\"window_end_ms\":2000,\"trigger\":\"Interval\",\"cpu_time_millis\":10,\"memory_high_watermark_bytes\":4096,\"memory_byte_millis\":8192,\"ingress_bytes\":7,\"egress_bytes\":11,\"storage_read_bytes\":13,\"storage_write_bytes\":17}}],\"next_sequence\":42,\"high_watermark\":56}"
        )
    );
}

#[test]
fn parse_guest_log_attach_selector_accepts_exact_guest_stream() {
    let selector =
        parse_guest_log_attach_selector("tenant-a/media/camera#event:stdout").expect("selector");

    assert_eq!(
        selector.workload,
        workload_ref("tenant-a".into(), "media".into(), "camera".into())
    );
    assert_eq!(
        selector.stream_names().expect("streams"),
        vec!["stdout".to_string()]
    );
}

#[test]
fn parse_guest_log_attach_selector_expands_prefix_to_guest_streams_only() {
    let selector =
        parse_guest_log_attach_selector("tenant-a/media/camera#event:std*").expect("selector");

    assert_eq!(
        selector.stream_names().expect("streams"),
        vec!["stdout".to_string(), "stderr".to_string()]
    );
}

#[test]
fn parse_guest_log_attach_selector_rejects_non_guest_stream_targets() {
    let err = parse_guest_log_attach_selector("tenant-a/media/camera#event:camera.frames")
        .expect("selector")
        .stream_names()
        .expect_err("non-guest stream should be rejected");

    assert!(err.to_string().contains("only supports guest log streams"));
}

#[test]
fn render_guest_log_payload_labels_each_line_when_multiple_streams_are_attached() {
    let endpoint = PublicEndpointRef {
        workload: workload_ref("tenant-a".into(), "media".into(), "camera".into()),
        kind: ContractKind::Event,
        name: "stderr".to_string(),
    };

    let rendered = render_guest_log_payload(&endpoint, b"first\nsecond\n", true);
    let rendered = String::from_utf8(rendered).expect("utf8");

    assert_eq!(rendered, "[stderr] first\n[stderr] second\n");
}
