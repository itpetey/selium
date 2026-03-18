use super::*;
use selium_control_plane_api::{
    BandwidthProfile, ContractRef, DeploymentSpec, DiscoveryCapabilityScope, DiscoveryOperation,
    DiscoveryRequest, DiscoveryTarget, EventEndpointRef, ExternalAccountRef,
    OperationalProcessSelector, PipelineEdge, PipelineEndpoint, PipelineSpec, PlacementMode,
    VolumeMount, WorkloadRef, parse_idl,
};
use selium_control_plane_core::{AttributedInfrastructureFilter, Query};
use selium_control_plane_protocol::{ListResponse, ManagedEndpointBindingType, ReplayApiRequest};
use selium_control_plane_scheduler::ScheduledEndpointBridgeIntent;

const SAMPLE_IDL: &str = r#"
package media.pipeline.v1;

schema Frame {
  camera_id: string;
}

event camera.frames(Frame) {
  partitions: 1;
  delivery: at_least_once;
}
"#;

#[test]
fn reconcile_generates_start_and_stop_actions() {
    let mut state = ControlPlaneState::new_local_default();
    state
        .upsert_deployment(DeploymentSpec {
            workload: selium_control_plane_api::WorkloadRef {
                tenant: "tenant-a".to_string(),
                namespace: "default".to_string(),
                name: "echo".to_string(),
            },
            module: "echo.wasm".to_string(),
            replicas: 2,
            contracts: Vec::new(),
            isolation: IsolationProfile::Standard,
            placement_mode: PlacementMode::ElasticPack,
            cpu_millis: 0,
            memory_mib: 0,
            ephemeral_storage_mib: 0,
            bandwidth_profile: BandwidthProfile::Standard,
            volume_mounts: Vec::new(),
            external_account_ref: None,
        })
        .expect("deployment");

    let desired = build_plan(&state).expect("schedule");
    let mut current = AgentState {
        running_instances: BTreeMap::from([("old-0".to_string(), "old".to_string())]),
        active_bridges: BTreeSet::new(),
    };

    let actions = reconcile("local-node", &state, &desired, &current);
    assert_eq!(actions.len(), 3);

    apply(&mut current, &actions);
    assert_eq!(current.running_instances.len(), 2);
    assert!(current.active_bridges.is_empty());
    assert!(
        current
            .running_instances
            .contains_key("tenant=tenant-a;namespace=default;workload=echo;replica=0")
    );
    assert!(
        current
            .running_instances
            .contains_key("tenant=tenant-a;namespace=default;workload=echo;replica=1")
    );
}

#[test]
fn reconcile_generates_endpoint_bridge_actions() {
    let mut state = ControlPlaneState::new_local_default();
    state
        .registry
        .register_package(parse_idl(SAMPLE_IDL).expect("parse idl"))
        .expect("register package");

    let ingest = WorkloadRef {
        tenant: "tenant-a".to_string(),
        namespace: "media".to_string(),
        name: "ingest".to_string(),
    };
    let detector = WorkloadRef {
        tenant: "tenant-a".to_string(),
        namespace: "media".to_string(),
        name: "detector".to_string(),
    };
    let contract = ContractRef {
        namespace: "media.pipeline".to_string(),
        kind: selium_control_plane_api::ContractKind::Event,
        name: "camera.frames".to_string(),
        version: "v1".to_string(),
    };
    for workload in [ingest.clone(), detector.clone()] {
        state
            .upsert_deployment(DeploymentSpec {
                workload,
                module: "module.wasm".to_string(),
                replicas: 1,
                contracts: vec![contract.clone()],
                isolation: IsolationProfile::Standard,
                placement_mode: PlacementMode::ElasticPack,
                cpu_millis: 0,
                memory_mib: 0,
                ephemeral_storage_mib: 0,
                bandwidth_profile: BandwidthProfile::Standard,
                volume_mounts: Vec::new(),
                external_account_ref: None,
            })
            .expect("deployment");
    }
    state.upsert_pipeline(PipelineSpec {
        name: "camera".to_string(),
        tenant: "tenant-a".to_string(),
        namespace: "media".to_string(),
        edges: vec![PipelineEdge {
            from: PipelineEndpoint {
                endpoint: EventEndpointRef {
                    workload: ingest,
                    name: "camera.frames".to_string(),
                },
                contract: contract.clone(),
            },
            to: PipelineEndpoint {
                endpoint: EventEndpointRef {
                    workload: detector,
                    name: "camera.frames".to_string(),
                },
                contract,
            },
        }],
        external_account_ref: None,
    });

    let desired = build_plan(&state).expect("schedule");
    let current = AgentState::default();
    let actions = reconcile("local-node", &state, &desired, &current);

    assert!(
        actions
            .iter()
            .any(|action| matches!(action, ReconcileAction::EnsureEndpointBridge(_)))
    );
}

#[test]
fn reconcile_orders_starts_before_endpoint_bridges() {
    let mut state = ControlPlaneState::new_local_default();
    state
        .registry
        .register_package(parse_idl(SAMPLE_IDL).expect("parse idl"))
        .expect("register package");

    let ingest = WorkloadRef {
        tenant: "tenant-a".to_string(),
        namespace: "media".to_string(),
        name: "ingest".to_string(),
    };
    let detector = WorkloadRef {
        tenant: "tenant-a".to_string(),
        namespace: "media".to_string(),
        name: "detector".to_string(),
    };
    let contract = ContractRef {
        namespace: "media.pipeline".to_string(),
        kind: selium_control_plane_api::ContractKind::Event,
        name: "camera.frames".to_string(),
        version: "v1".to_string(),
    };
    for workload in [ingest.clone(), detector.clone()] {
        state
            .upsert_deployment(DeploymentSpec {
                workload,
                module: "module.wasm".to_string(),
                replicas: 1,
                contracts: vec![contract.clone()],
                isolation: IsolationProfile::Standard,
                placement_mode: PlacementMode::ElasticPack,
                cpu_millis: 0,
                memory_mib: 0,
                ephemeral_storage_mib: 0,
                bandwidth_profile: BandwidthProfile::Standard,
                volume_mounts: Vec::new(),
                external_account_ref: None,
            })
            .expect("deployment");
    }
    state.upsert_pipeline(PipelineSpec {
        name: "camera".to_string(),
        tenant: "tenant-a".to_string(),
        namespace: "media".to_string(),
        edges: vec![PipelineEdge {
            from: PipelineEndpoint {
                endpoint: EventEndpointRef {
                    workload: ingest,
                    name: "camera.frames".to_string(),
                },
                contract: contract.clone(),
            },
            to: PipelineEndpoint {
                endpoint: EventEndpointRef {
                    workload: detector,
                    name: "camera.frames".to_string(),
                },
                contract,
            },
        }],
        external_account_ref: None,
    });

    let desired = build_plan(&state).expect("schedule");
    let actions = reconcile("local-node", &state, &desired, &AgentState::default());
    let start_idx = actions
        .iter()
        .position(|action| matches!(action, ReconcileAction::Start { .. }))
        .expect("start action");
    let ensure_idx = actions
        .iter()
        .position(|action| matches!(action, ReconcileAction::EnsureEndpointBridge(_)))
        .expect("ensure action");

    assert!(
        start_idx < ensure_idx,
        "expected start before ensure actions, got {actions:?}"
    );
}

#[test]
fn managed_endpoint_bindings_keep_same_name_cross_kind_entries_distinct() {
    let bindings = managed_endpoint_bindings_for_instance(
        "instance-a",
        &[
            ScheduledEndpointBridgeIntent {
                bridge_id: "bridge-event".to_string(),
                source_instance_id: "instance-a".to_string(),
                source_node: "local-node".to_string(),
                source_endpoint: PublicEndpointRef {
                    workload: WorkloadRef {
                        tenant: "tenant-a".to_string(),
                        namespace: "media".to_string(),
                        name: "ingest".to_string(),
                    },
                    kind: ContractKind::Event,
                    name: "shared".to_string(),
                },
                target_instance_id: "instance-b".to_string(),
                target_node: "local-node".to_string(),
                target_endpoint: PublicEndpointRef {
                    workload: WorkloadRef {
                        tenant: "tenant-a".to_string(),
                        namespace: "media".to_string(),
                        name: "detector".to_string(),
                    },
                    kind: ContractKind::Event,
                    name: "shared".to_string(),
                },
                contract: ContractRef {
                    namespace: "media.pipeline".to_string(),
                    kind: ContractKind::Event,
                    name: "shared".to_string(),
                    version: "v1".to_string(),
                },
            },
            ScheduledEndpointBridgeIntent {
                bridge_id: "bridge-service".to_string(),
                source_instance_id: "instance-a".to_string(),
                source_node: "local-node".to_string(),
                source_endpoint: PublicEndpointRef {
                    workload: WorkloadRef {
                        tenant: "tenant-a".to_string(),
                        namespace: "media".to_string(),
                        name: "ingest".to_string(),
                    },
                    kind: ContractKind::Service,
                    name: "shared".to_string(),
                },
                target_instance_id: "instance-c".to_string(),
                target_node: "remote-node".to_string(),
                target_endpoint: PublicEndpointRef {
                    workload: WorkloadRef {
                        tenant: "tenant-a".to_string(),
                        namespace: "media".to_string(),
                        name: "uploader".to_string(),
                    },
                    kind: ContractKind::Service,
                    name: "shared".to_string(),
                },
                contract: ContractRef {
                    namespace: "media.pipeline".to_string(),
                    kind: ContractKind::Service,
                    name: "shared".to_string(),
                    version: "v1".to_string(),
                },
            },
            ScheduledEndpointBridgeIntent {
                bridge_id: "bridge-stream".to_string(),
                source_instance_id: "instance-a".to_string(),
                source_node: "local-node".to_string(),
                source_endpoint: PublicEndpointRef {
                    workload: WorkloadRef {
                        tenant: "tenant-a".to_string(),
                        namespace: "media".to_string(),
                        name: "ingest".to_string(),
                    },
                    kind: ContractKind::Stream,
                    name: "shared".to_string(),
                },
                target_instance_id: "instance-d".to_string(),
                target_node: "local-node".to_string(),
                target_endpoint: PublicEndpointRef {
                    workload: WorkloadRef {
                        tenant: "tenant-a".to_string(),
                        namespace: "media".to_string(),
                        name: "streamer".to_string(),
                    },
                    kind: ContractKind::Stream,
                    name: "shared".to_string(),
                },
                contract: ContractRef {
                    namespace: "media.pipeline".to_string(),
                    kind: ContractKind::Stream,
                    name: "shared".to_string(),
                    version: "v1".to_string(),
                },
            },
        ],
    );

    assert_eq!(bindings.len(), 3);
    assert!(bindings.iter().any(|binding| {
        binding.endpoint_kind == ContractKind::Event
            && binding.binding_type == ManagedEndpointBindingType::OneWay
    }));
    assert!(bindings.iter().any(|binding| {
        binding.endpoint_kind == ContractKind::Service
            && binding.binding_type == ManagedEndpointBindingType::RequestResponse
    }));
    assert!(bindings.iter().any(|binding| {
        binding.endpoint_kind == ContractKind::Stream
            && binding.binding_type == ManagedEndpointBindingType::Session
    }));
}

#[test]
fn replay_record_value_includes_structured_operator_audit_fields() {
    let workload = WorkloadRef {
        tenant: "tenant-a".to_string(),
        namespace: "media".to_string(),
        name: "router".to_string(),
    };
    let workload_key = workload.to_string();
    let mut state = ControlPlaneState::default();
    state
        .upsert_deployment(DeploymentSpec {
            workload: workload.clone(),
            module: "router.wasm".to_string(),
            replicas: 1,
            contracts: Vec::new(),
            isolation: IsolationProfile::Standard,
            placement_mode: PlacementMode::ElasticPack,
            cpu_millis: 0,
            memory_mib: 0,
            ephemeral_storage_mib: 0,
            bandwidth_profile: BandwidthProfile::Standard,
            volume_mounts: Vec::new(),
            external_account_ref: Some(ExternalAccountRef {
                key: "acct-123".to_string(),
            }),
        })
        .expect("seed deployment");
    let envelope = MutationEnvelope {
        idempotency_key: "audit-1".to_string(),
        mutation: Mutation::SetScale {
            workload: workload.clone(),
            replicas: 3,
        },
    };
    let committed = DurableCommittedEntry {
        idempotency_key: envelope.idempotency_key.clone(),
        index: 7,
        term: 2,
        payload: ControlPlaneEngine::encode_mutation(&envelope).expect("encode mutation"),
    };
    let record = StorageLogRecord {
        sequence: 11,
        timestamp_ms: 42,
        headers: audit_headers(Some(&state), &envelope.mutation),
        payload: encode_rkyv(&committed).expect("encode committed entry"),
    };

    let value = replay_record_value(record);
    assert_eq!(value.get("sequence").and_then(DataValue::as_u64), Some(11));
    assert_eq!(value.get("index").and_then(DataValue::as_u64), Some(7));
    let audit = value.get("audit").expect("audit section");
    assert_eq!(
        audit.get("actor_kind").and_then(DataValue::as_str),
        Some("operator")
    );
    assert_eq!(
        audit.get("mutation_kind").and_then(DataValue::as_str),
        Some("set_scale")
    );
    assert_eq!(
        audit.get("workload").and_then(DataValue::as_str),
        Some(workload_key.as_str())
    );
    assert_eq!(
        audit.get("module").and_then(DataValue::as_str),
        Some("router.wasm")
    );
    assert_eq!(audit.get("replicas").and_then(DataValue::as_u64), Some(3));
    assert_eq!(
        audit
            .get("external_account_ref")
            .and_then(DataValue::as_str),
        Some("acct-123")
    );
}

#[test]
fn replay_record_value_includes_declared_resource_fields() {
    let envelope = MutationEnvelope {
        idempotency_key: "audit-2".to_string(),
        mutation: Mutation::UpsertDeployment {
            spec: DeploymentSpec {
                workload: WorkloadRef {
                    tenant: "tenant-a".to_string(),
                    namespace: "media".to_string(),
                    name: "router".to_string(),
                },
                module: "router.wasm".to_string(),
                replicas: 1,
                contracts: Vec::new(),
                isolation: IsolationProfile::Standard,
                placement_mode: PlacementMode::ElasticPack,
                cpu_millis: 600,
                memory_mib: 512,
                ephemeral_storage_mib: 128,
                bandwidth_profile: BandwidthProfile::High,
                volume_mounts: vec![VolumeMount {
                    name: "scratch".to_string(),
                    mount_path: "/scratch".to_string(),
                    read_only: false,
                }],
                external_account_ref: Some(ExternalAccountRef {
                    key: "acct-789".to_string(),
                }),
            },
        },
    };
    let committed = DurableCommittedEntry {
        idempotency_key: envelope.idempotency_key.clone(),
        index: 8,
        term: 2,
        payload: ControlPlaneEngine::encode_mutation(&envelope).expect("encode mutation"),
    };
    let record = StorageLogRecord {
        sequence: 12,
        timestamp_ms: 43,
        headers: audit_headers(None, &envelope.mutation),
        payload: encode_rkyv(&committed).expect("encode committed entry"),
    };

    let event = replay_record_value(record);
    let audit = event.get("audit").expect("audit section");

    assert_eq!(
        audit
            .get("resources")
            .and_then(|resources| resources.get("cpu_millis"))
            .and_then(DataValue::as_u64),
        Some(600)
    );
    assert_eq!(
        audit
            .get("resources")
            .and_then(|resources| resources.get("bandwidth_profile"))
            .and_then(DataValue::as_str),
        Some("high")
    );
    assert_eq!(
        audit
            .get("external_account_ref")
            .and_then(DataValue::as_str),
        Some("acct-789")
    );
    assert_eq!(
        event
            .get("headers")
            .and_then(|headers| headers.get("external_account_ref"))
            .and_then(DataValue::as_str),
        Some("acct-789")
    );
}

#[test]
fn replay_record_value_includes_pipeline_external_account_reference() {
    let contract = ContractRef {
        namespace: "media.pipeline".to_string(),
        kind: selium_control_plane_api::ContractKind::Event,
        name: "camera.frames".to_string(),
        version: "v1".to_string(),
    };
    let workload = WorkloadRef {
        tenant: "tenant-a".to_string(),
        namespace: "media".to_string(),
        name: "router".to_string(),
    };
    let envelope = MutationEnvelope {
        idempotency_key: "audit-3".to_string(),
        mutation: Mutation::UpsertPipeline {
            spec: PipelineSpec {
                name: "camera".to_string(),
                tenant: "tenant-a".to_string(),
                namespace: "media".to_string(),
                edges: vec![PipelineEdge {
                    from: PipelineEndpoint {
                        endpoint: EventEndpointRef {
                            workload: workload.clone(),
                            name: contract.name.clone(),
                        },
                        contract: contract.clone(),
                    },
                    to: PipelineEndpoint {
                        endpoint: EventEndpointRef {
                            workload,
                            name: contract.name.clone(),
                        },
                        contract,
                    },
                }],
                external_account_ref: Some(ExternalAccountRef {
                    key: "acct-321".to_string(),
                }),
            },
        },
    };
    let committed = DurableCommittedEntry {
        idempotency_key: envelope.idempotency_key.clone(),
        index: 9,
        term: 2,
        payload: ControlPlaneEngine::encode_mutation(&envelope).expect("encode mutation"),
    };
    let record = StorageLogRecord {
        sequence: 13,
        timestamp_ms: 44,
        headers: audit_headers(None, &envelope.mutation),
        payload: encode_rkyv(&committed).expect("encode committed entry"),
    };

    let event = replay_record_value(record);
    let audit = event.get("audit").expect("audit section");

    assert_eq!(
        audit.get("pipeline").and_then(DataValue::as_str),
        Some("tenant-a/media/camera")
    );
    assert_eq!(
        audit
            .get("external_account_ref")
            .and_then(DataValue::as_str),
        Some("acct-321")
    );
    assert_eq!(
        event
            .get("headers")
            .and_then(|headers| headers.get("external_account_ref"))
            .and_then(DataValue::as_str),
        Some("acct-321")
    );
}

#[test]
fn audit_headers_classify_node_upserts_as_system() {
    let headers = audit_headers(
        None,
        &Mutation::UpsertNode {
            spec: NodeSpec {
                name: "node-a".to_string(),
                capacity_slots: 64,
                allocatable_cpu_millis: Some(2_000),
                allocatable_memory_mib: Some(4_096),
                reserve_cpu_utilisation_ppm: 800_000,
                reserve_memory_utilisation_ppm: 800_000,
                reserve_slots_utilisation_ppm: 800_000,
                observed_running_instances: None,
                observed_active_bridges: None,
                observed_memory_mib: None,
                observed_workloads: BTreeMap::new(),
                observed_workload_memory_mib: BTreeMap::new(),
                supported_isolation: vec![IsolationProfile::Standard],
                daemon_addr: "127.0.0.1:7100".to_string(),
                daemon_server_name: "localhost".to_string(),
                last_heartbeat_ms: 0,
            },
        },
    );

    assert_eq!(headers.get("actor_kind"), Some(&"system".to_string()));
    assert_eq!(
        headers.get("mutation_kind"),
        Some(&"upsert_node".to_string())
    );
    assert_eq!(headers.get("node"), Some(&"node-a".to_string()));
}

#[test]
fn merge_observed_node_load_preserves_previous_values_when_legacy_fields_are_missing() {
    let previous = ObservedNodeLoad {
        running_instances: 2,
        active_bridges: 1,
        memory_mib: 768,
        observed_workloads: BTreeMap::from([("tenant-a/default/echo".to_string(), 2)]),
        observed_workload_memory_mib: BTreeMap::from([("tenant-a/default/echo".to_string(), 768)]),
    };

    let merged = merge_observed_node_load(
        previous,
        ListResponse {
            instances: BTreeMap::from([
                ("tenant-a/default/echo/0".to_string(), 1usize),
                ("tenant-a/default/echo/1".to_string(), 2usize),
            ]),
            active_bridges: vec!["bridge-1".to_string()],
            observed_memory_bytes: None,
            observed_workloads: BTreeMap::new(),
            observed_workload_memory_bytes: BTreeMap::new(),
        },
    );

    assert_eq!(merged.running_instances, 2);
    assert_eq!(merged.active_bridges, 1);
    assert_eq!(merged.memory_mib, 768);
    assert_eq!(
        merged.observed_workloads,
        BTreeMap::from([("tenant-a/default/echo".to_string(), 2)])
    );
    assert_eq!(
        merged.observed_workload_memory_mib,
        BTreeMap::from([("tenant-a/default/echo".to_string(), 768)])
    );
}

#[test]
fn clear_stale_observed_node_load_keeps_recent_usage_and_drops_old_usage() {
    let mut snapshot = ControlPlaneState::default();
    snapshot
        .upsert_node(NodeSpec {
            name: "fresh-node".to_string(),
            capacity_slots: 64,
            allocatable_cpu_millis: Some(2_000),
            allocatable_memory_mib: Some(4_096),
            reserve_cpu_utilisation_ppm: 800_000,
            reserve_memory_utilisation_ppm: 800_000,
            reserve_slots_utilisation_ppm: 800_000,
            observed_running_instances: Some(2),
            observed_active_bridges: Some(1),
            observed_memory_mib: Some(768),
            observed_workloads: BTreeMap::from([("tenant-a/default/echo".to_string(), 2)]),
            observed_workload_memory_mib: BTreeMap::from([(
                "tenant-a/default/echo".to_string(),
                768,
            )]),
            supported_isolation: vec![IsolationProfile::Standard],
            daemon_addr: "127.0.0.1:7101".to_string(),
            daemon_server_name: "fresh-node".to_string(),
            last_heartbeat_ms: 900,
        })
        .expect("fresh node");
    snapshot
        .upsert_node(NodeSpec {
            name: "stale-node".to_string(),
            capacity_slots: 64,
            allocatable_cpu_millis: Some(2_000),
            allocatable_memory_mib: Some(4_096),
            reserve_cpu_utilisation_ppm: 800_000,
            reserve_memory_utilisation_ppm: 800_000,
            reserve_slots_utilisation_ppm: 800_000,
            observed_running_instances: Some(2),
            observed_active_bridges: Some(1),
            observed_memory_mib: Some(768),
            observed_workloads: BTreeMap::from([("tenant-a/default/echo".to_string(), 2)]),
            observed_workload_memory_mib: BTreeMap::from([(
                "tenant-a/default/echo".to_string(),
                768,
            )]),
            supported_isolation: vec![IsolationProfile::Standard],
            daemon_addr: "127.0.0.1:7102".to_string(),
            daemon_server_name: "stale-node".to_string(),
            last_heartbeat_ms: 100,
        })
        .expect("stale node");

    clear_stale_observed_node_load(&mut snapshot, 1_000, 300);

    let fresh = snapshot.nodes.get("fresh-node").expect("fresh node");
    assert_eq!(fresh.observed_running_instances, Some(2));
    assert_eq!(fresh.observed_memory_mib, Some(768));
    assert_eq!(
        fresh.observed_workload_memory_mib,
        BTreeMap::from([("tenant-a/default/echo".to_string(), 768)])
    );

    let stale = snapshot.nodes.get("stale-node").expect("stale node");
    assert_eq!(stale.observed_running_instances, None);
    assert_eq!(stale.observed_active_bridges, None);
    assert_eq!(stale.observed_memory_mib, None);
    assert!(stale.observed_workloads.is_empty());
    assert!(stale.observed_workload_memory_mib.is_empty());
}

#[test]
fn workload_bytes_to_mib_keeps_sum_within_total_mib() {
    let observed = workload_bytes_to_mib(&BTreeMap::from([
        ("tenant-a/default/alpha".to_string(), 1u64),
        ("tenant-a/default/bravo".to_string(), 1u64),
    ]));

    assert_eq!(observed.values().copied().sum::<u32>(), 1);
    assert_eq!(observed.len(), 2);
}

#[test]
fn scheduling_query_detection_matches_schedule_sensitive_queries() {
    assert!(query_uses_observed_load_for_scheduling(
        &Query::ControlPlaneSummary
    ));
    assert!(query_uses_observed_load_for_scheduling(
        &Query::AttributedInfrastructureInventory {
            filter: AttributedInfrastructureFilter::default(),
        }
    ));
    assert!(query_uses_observed_load_for_scheduling(
        &Query::ResolveDiscovery {
            request: DiscoveryRequest {
                operation: DiscoveryOperation::Discover,
                target: DiscoveryTarget::RunningProcess(OperationalProcessSelector::ReplicaKey(
                    "replica-0".to_string()
                ),),
                scope: DiscoveryCapabilityScope::allow_all(),
            },
        }
    ));
    assert!(!query_uses_observed_load_for_scheduling(
        &Query::ResolveDiscovery {
            request: DiscoveryRequest {
                operation: DiscoveryOperation::Discover,
                target: DiscoveryTarget::Workload(WorkloadRef {
                    tenant: "tenant-a".to_string(),
                    namespace: "default".to_string(),
                    name: "echo".to_string(),
                }),
                scope: DiscoveryCapabilityScope::allow_all(),
            },
        }
    ));
    assert!(!query_uses_observed_load_for_scheduling(
        &Query::ControlPlaneState
    ));
    assert!(!query_uses_observed_load_for_scheduling(
        &Query::NodesLive {
            now_ms: 1,
            max_staleness_ms: 1,
        }
    ));
}

#[test]
fn replay_selection_respects_since_sequence_and_header_filters() {
    let request = ReplayApiRequest {
        limit: 2,
        since_sequence: Some(12),
        checkpoint: None,
        save_checkpoint: None,
        external_account_ref: Some("acct-123".to_string()),
        workload: None,
        module: None,
        pipeline: None,
        node: None,
    };
    let bounds = StorageLogBoundsResult {
        code: selium_abi::StorageStatusCode::Ok,
        first_sequence: Some(10),
        latest_sequence: Some(14),
        next_sequence: 15,
    };
    assert_eq!(replay_start_sequence(&bounds, &request, None), Some(12));

    let selected = select_replay_records(
        vec![
            StorageLogRecord {
                sequence: 12,
                timestamp_ms: 1,
                headers: BTreeMap::from([(
                    "external_account_ref".to_string(),
                    "acct-123".to_string(),
                )]),
                payload: Vec::new(),
            },
            StorageLogRecord {
                sequence: 13,
                timestamp_ms: 2,
                headers: BTreeMap::from([(
                    "external_account_ref".to_string(),
                    "acct-999".to_string(),
                )]),
                payload: Vec::new(),
            },
            StorageLogRecord {
                sequence: 14,
                timestamp_ms: 3,
                headers: BTreeMap::from([(
                    "external_account_ref".to_string(),
                    "acct-123".to_string(),
                )]),
                payload: Vec::new(),
            },
        ],
        &request,
    );

    assert_eq!(
        selected
            .iter()
            .map(|record| record.sequence)
            .collect::<Vec<_>>(),
        vec![12, 14]
    );
    assert_eq!(
        replay_next_sequence(&request, &selected, Some(14)),
        Some(15)
    );
}

#[test]
fn inventory_snapshot_marker_hands_off_to_replay_since_sequence() {
    let account = ExternalAccountRef {
        key: "acct-123".to_string(),
    };
    let workload = WorkloadRef {
        tenant: "tenant-a".to_string(),
        namespace: "media".to_string(),
        name: "ingest".to_string(),
    };
    let mut engine = ControlPlaneEngine::default();
    engine
        .apply_mutation(
            1,
            Mutation::UpsertDeployment {
                spec: DeploymentSpec {
                    workload: workload.clone(),
                    module: "ingest.wasm".to_string(),
                    replicas: 1,
                    contracts: Vec::new(),
                    isolation: IsolationProfile::Standard,
                    placement_mode: PlacementMode::ElasticPack,
                    cpu_millis: 250,
                    memory_mib: 128,
                    ephemeral_storage_mib: 0,
                    bandwidth_profile: BandwidthProfile::Standard,
                    volume_mounts: Vec::new(),
                    external_account_ref: Some(account.clone()),
                },
            },
        )
        .expect("deployment");
    engine
        .apply_mutation(
            2,
            Mutation::SetScale {
                workload: workload.clone(),
                replicas: 2,
            },
        )
        .expect("scale to snapshot state");

    let inventory = engine
        .query(Query::AttributedInfrastructureInventory {
            filter: selium_control_plane_core::AttributedInfrastructureFilter {
                external_account_ref: Some(account.key.clone()),
                ..Default::default()
            },
        })
        .expect("inventory query");
    let last_applied = inventory
        .result
        .get("snapshot_marker")
        .and_then(|marker| marker.get("last_applied"))
        .and_then(DataValue::as_u64)
        .expect("snapshot marker last_applied");

    let request = ReplayApiRequest {
        limit: 10,
        since_sequence: Some(last_applied.saturating_add(1)),
        checkpoint: None,
        save_checkpoint: None,
        external_account_ref: Some(account.key.clone()),
        workload: Some(workload.to_string()),
        module: None,
        pipeline: None,
        node: None,
    };
    let bounds = StorageLogBoundsResult {
        code: selium_abi::StorageStatusCode::Ok,
        first_sequence: Some(1),
        latest_sequence: Some(3),
        next_sequence: 4,
    };

    assert_eq!(last_applied, 2);
    let start_sequence = replay_start_sequence(&bounds, &request, None).expect("start sequence");
    assert_eq!(start_sequence, 3);
    assert_eq!(
        replay_fetch_limit(&bounds, start_sequence, request.limit),
        1
    );

    let snapshot_state = engine.snapshot().control_plane;
    let delta_record = StorageLogRecord {
        sequence: 3,
        timestamp_ms: 3,
        headers: audit_headers(
            Some(&snapshot_state),
            &Mutation::SetScale {
                workload: workload.clone(),
                replicas: 3,
            },
        ),
        payload: encode_rkyv(&DurableCommittedEntry {
            idempotency_key: "scale-3".to_string(),
            index: 3,
            term: 1,
            payload: ControlPlaneEngine::encode_mutation(&MutationEnvelope {
                idempotency_key: "scale-3".to_string(),
                mutation: Mutation::SetScale {
                    workload: workload.clone(),
                    replicas: 3,
                },
            })
            .expect("encode delta mutation"),
        })
        .expect("encode delta record"),
    };

    let selected = select_replay_records(vec![delta_record], &request);
    assert_eq!(
        selected
            .iter()
            .map(|record| record.sequence)
            .collect::<Vec<_>>(),
        vec![3]
    );
    assert_eq!(replay_next_sequence(&request, &selected, Some(3)), Some(4));

    let event = replay_record_value(selected.into_iter().next().expect("delta record"));
    assert_eq!(event.get("sequence").and_then(DataValue::as_u64), Some(3));
    assert_eq!(event.get("index").and_then(DataValue::as_u64), Some(3));
    assert_eq!(
        event
            .get("audit")
            .and_then(|audit| audit.get("replicas"))
            .and_then(DataValue::as_u64),
        Some(3)
    );
}

#[test]
fn replay_selection_with_filters_and_no_cursor_pages_forward() {
    let request = ReplayApiRequest {
        limit: 2,
        since_sequence: None,
        checkpoint: None,
        save_checkpoint: None,
        external_account_ref: Some("acct-123".to_string()),
        workload: None,
        module: None,
        pipeline: None,
        node: None,
    };

    let selected = select_replay_records(
        vec![
            StorageLogRecord {
                sequence: 11,
                timestamp_ms: 1,
                headers: BTreeMap::from([(
                    "external_account_ref".to_string(),
                    "acct-123".to_string(),
                )]),
                payload: Vec::new(),
            },
            StorageLogRecord {
                sequence: 12,
                timestamp_ms: 2,
                headers: BTreeMap::from([(
                    "external_account_ref".to_string(),
                    "acct-999".to_string(),
                )]),
                payload: Vec::new(),
            },
            StorageLogRecord {
                sequence: 13,
                timestamp_ms: 3,
                headers: BTreeMap::from([(
                    "external_account_ref".to_string(),
                    "acct-123".to_string(),
                )]),
                payload: Vec::new(),
            },
            StorageLogRecord {
                sequence: 14,
                timestamp_ms: 4,
                headers: BTreeMap::from([(
                    "external_account_ref".to_string(),
                    "acct-123".to_string(),
                )]),
                payload: Vec::new(),
            },
        ],
        &request,
    );

    assert_eq!(
        selected
            .iter()
            .map(|record| record.sequence)
            .collect::<Vec<_>>(),
        vec![11, 13]
    );
    assert_eq!(
        replay_next_sequence(&request, &selected, Some(14)),
        Some(14)
    );
}

#[test]
fn replay_next_sequence_preserves_cursor_ahead_of_high_watermark() {
    let request = ReplayApiRequest {
        limit: 5,
        since_sequence: Some(25),
        checkpoint: None,
        save_checkpoint: None,
        external_account_ref: Some("acct-123".to_string()),
        workload: None,
        module: None,
        pipeline: None,
        node: None,
    };

    assert_eq!(replay_next_sequence(&request, &[], Some(14)), Some(25));
}

#[test]
fn replay_checkpoint_pages_forward_and_advances_cursor() {
    let request = ReplayApiRequest {
        limit: 2,
        since_sequence: None,
        checkpoint: Some("replay-cursor".to_string()),
        save_checkpoint: Some("replay-cursor-next".to_string()),
        external_account_ref: None,
        workload: None,
        module: None,
        pipeline: None,
        node: None,
    };
    let bounds = StorageLogBoundsResult {
        code: selium_abi::StorageStatusCode::Ok,
        first_sequence: Some(10),
        latest_sequence: Some(14),
        next_sequence: 15,
    };

    assert_eq!(replay_start_sequence(&bounds, &request, Some(13)), Some(13));

    let selected = select_replay_records(
        vec![
            StorageLogRecord {
                sequence: 13,
                timestamp_ms: 3,
                headers: BTreeMap::new(),
                payload: Vec::new(),
            },
            StorageLogRecord {
                sequence: 14,
                timestamp_ms: 4,
                headers: BTreeMap::new(),
                payload: Vec::new(),
            },
        ],
        &request,
    );

    assert_eq!(
        selected
            .iter()
            .map(|record| record.sequence)
            .collect::<Vec<_>>(),
        vec![13, 14]
    );
    let next_sequence = replay_next_sequence(&request, &selected, Some(14));
    assert_eq!(next_sequence, Some(15));
    assert_eq!(
        replay_checkpoint_sequence_to_save(&request, Some(13), next_sequence, bounds.next_sequence),
        15
    );
}

#[test]
fn replay_checkpoint_errors_are_deterministic() {
    let invalid = ReplayApiRequest {
        limit: 10,
        since_sequence: Some(7),
        checkpoint: Some("named".to_string()),
        save_checkpoint: None,
        external_account_ref: None,
        workload: None,
        module: None,
        pipeline: None,
        node: None,
    };
    assert_eq!(
        replay_requested_checkpoint_name(&invalid)
            .expect_err("mixed cursor should fail")
            .to_string(),
        "replay request cannot set both since_sequence and checkpoint"
    );

    assert_eq!(
        replay_checkpoint_sequence_or_err("missing", None)
            .expect_err("unknown checkpoint should fail")
            .to_string(),
        "checkpoint `missing` does not exist"
    );

    let request = ReplayApiRequest {
        limit: 10,
        since_sequence: None,
        checkpoint: Some("missing".to_string()),
        save_checkpoint: None,
        external_account_ref: None,
        workload: None,
        module: None,
        pipeline: None,
        node: None,
    };
    assert_eq!(
        replay_checkpoint_sequence_to_save(&request, Some(12), None, 15),
        12
    );
}
