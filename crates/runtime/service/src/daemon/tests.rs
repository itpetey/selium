use super::*;
use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use clap::Parser;
use selium_abi::{
    PrincipalKind, PrincipalRef, RuntimeUsageQuery, RuntimeUsageReplayStart, decode_rkyv,
};
use selium_control_plane_api::{
    ContractRef, ControlPlaneState, DeploymentSpec, DiscoveryCapabilityScope, DiscoveryPattern,
    IsolationProfile, NodeSpec, OperationalProcessSelector, WorkloadRef, parse_idl,
};
use selium_control_plane_core::{Mutation, Query};
use selium_control_plane_runtime::ControlPlaneEngine;
use tokio::io::duplex;

use crate::auth::AccessPolicy;
use crate::config::{ServerCommand, ServerOptions};
use crate::usage::{ProcessUsageAttribution, RuntimeUsageCollector};

#[test]
fn resolve_internal_tls_paths_fall_back_to_server_identity_when_client_identity_missing() {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock")
        .as_nanos();
    let work_dir = std::env::temp_dir().join(format!("selium-daemon-peer-fallback-{unique}"));
    fs::create_dir_all(work_dir.join("certs")).expect("create cert dir");

    let (cert, key) = resolve_internal_client_tls_paths(&work_dir, None, None)
        .expect("resolve internal client tls");

    assert!(cert.is_none());
    assert!(key.is_none());
}

#[test]
fn resolve_peer_tls_paths_prefers_generated_client_identity_when_available() {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock")
        .as_nanos();
    let work_dir = std::env::temp_dir().join(format!("selium-daemon-peer-client-{unique}"));
    fs::create_dir_all(work_dir.join("certs")).expect("create cert dir");
    fs::write(work_dir.join("certs/peer.crt"), b"cert").expect("write peer cert");
    fs::write(work_dir.join("certs/peer.key"), b"key").expect("write peer key");

    let (cert, key) = resolve_peer_client_tls_paths(&work_dir, None, None)
        .expect("resolve peer tls")
        .expect("generated peer identity should be selected when present");

    assert_eq!(cert, work_dir.join("certs/peer.crt"));
    assert_eq!(key, work_dir.join("certs/peer.key"));
}

#[test]
fn resolve_peer_tls_paths_require_a_client_identity() {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock")
        .as_nanos();
    let work_dir = std::env::temp_dir().join(format!("selium-daemon-peer-missing-{unique}"));
    fs::create_dir_all(work_dir.join("certs")).expect("create cert dir");

    let identity =
        resolve_peer_client_tls_paths(&work_dir, None, None).expect("resolve peer tls paths");

    assert!(identity.is_none());
}

#[test]
fn ensure_workload_authorised_rejects_out_of_scope_workloads() {
    let policy = AccessPolicy::from_specs(&[
        "principal=machine:client.localhost;methods=control_write;workloads=tenant-a/*".to_string(),
    ])
    .expect("policy");
    let request_context = policy.resolve(
        PrincipalRef::new(PrincipalKind::Machine, "client.localhost"),
        "fp".to_string(),
    );

    ensure_workload_authorised(
        &request_context,
        Method::StartInstance,
        "tenant-a/api",
        "instance start",
    )
    .expect("authorised workload");
    let err = ensure_workload_authorised(
        &request_context,
        Method::StartInstance,
        "tenant-b/api",
        "instance start",
    )
    .expect_err("workload should be denied");
    assert!(
        err.to_string()
            .contains("unauthorised workload `tenant-b/api` for instance start")
    );
}

#[test]
fn default_access_grant_does_not_include_peer_rights() {
    let grant = default_access_grant_spec(&PrincipalRef::new(
        PrincipalKind::Machine,
        "client.localhost",
    ));

    assert!(!grant.contains("peer"));
}

#[test]
fn default_peer_access_grant_is_reserved_for_peer_identity() {
    let grant = default_peer_access_grant_spec();

    assert!(grant.contains("principal=runtime-peer:*"));
    assert!(grant.contains("methods=peer"));
    assert!(grant.contains("workloads=*"));
    assert!(grant.contains("endpoints=*"));
}

#[test]
fn build_access_policy_preserves_peer_grant_when_custom_grants_are_configured() {
    let opts = ServerOptions::try_parse_from([
        "selium-runtime",
        "daemon",
        "--access-grant",
        "principal=machine:client.localhost;methods=control_read;workloads=tenant-a/*",
    ])
    .expect("parse opts");
    let Some(ServerCommand::Daemon(args)) = opts.command else {
        panic!("expected daemon command");
    };
    let policy = build_access_policy(Path::new("."), &args).expect("policy");
    let peer = policy.resolve(
        PrincipalRef::new(PrincipalKind::RuntimePeer, "peer.localhost"),
        "fp".to_string(),
    );

    assert!(peer.allows(Method::RaftAppendEntries));
    assert!(peer.allows(Method::DeliverBridgeMessage));
}

#[test]
fn build_access_policy_uses_custom_generated_cert_principals() {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock")
        .as_nanos();
    let work_dir = std::env::temp_dir().join(format!("selium-daemon-policy-{unique}"));
    let cert_dir = work_dir.join("certs");
    fs::create_dir_all(&cert_dir).expect("create cert dir");
    crate::certs::generate_certificates(
        &cert_dir,
        "Test CA",
        "localhost",
        "client.localhost",
        "machine",
        "operator-123",
        "peer.localhost",
        "runtime-peer",
        "raft-peer-456",
    )
    .expect("generate certs");

    let opts = ServerOptions::try_parse_from(["selium-runtime", "daemon"]).expect("parse opts");
    let Some(ServerCommand::Daemon(args)) = opts.command else {
        panic!("expected daemon command");
    };
    let policy = build_access_policy(&work_dir, &args).expect("policy");
    let operator = policy.resolve(
        PrincipalRef::new(PrincipalKind::Machine, "operator-123"),
        "fp".to_string(),
    );
    let peer = policy.resolve(
        PrincipalRef::new(PrincipalKind::RuntimePeer, "raft-peer-456"),
        "fp".to_string(),
    );

    assert!(operator.allows(Method::ControlQuery));
    assert!(peer.allows(Method::RaftAppendEntries));
}

#[test]
fn build_access_policy_keeps_default_access_grant_bound_to_client_cert() {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock")
        .as_nanos();
    let work_dir = std::env::temp_dir().join(format!("selium-daemon-policy-override-{unique}"));
    let cert_dir = work_dir.join("certs");
    fs::create_dir_all(&cert_dir).expect("create cert dir");
    crate::certs::generate_certificates(
        &cert_dir,
        "Test CA",
        "localhost",
        "client.localhost",
        "machine",
        "operator-123",
        "peer.localhost",
        "runtime-peer",
        "raft-peer-456",
    )
    .expect("generate certs");

    let opts = ServerOptions::try_parse_from([
        "selium-runtime",
        "daemon",
        "--quic-peer-cert",
        "certs/peer.crt",
        "--quic-peer-key",
        "certs/peer.key",
    ])
    .expect("parse opts");
    let Some(ServerCommand::Daemon(args)) = opts.command else {
        panic!("expected daemon command");
    };
    let policy = build_access_policy(&work_dir, &args).expect("policy");
    let operator = policy.resolve(
        PrincipalRef::new(PrincipalKind::Machine, "operator-123"),
        "fp".to_string(),
    );
    let peer = policy.resolve(
        PrincipalRef::new(PrincipalKind::RuntimePeer, "raft-peer-456"),
        "fp".to_string(),
    );

    assert!(operator.allows(Method::ControlQuery));
    assert!(operator.allows(Method::StartInstance));
    assert!(peer.allows(Method::RaftAppendEntries));
    assert!(!peer.allows(Method::ControlQuery));
}

#[test]
fn generated_certificates_use_runtime_peer_principal_for_peer_identity() {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock")
        .as_nanos();
    let work_dir = std::env::temp_dir().join(format!("selium-daemon-generated-peer-{unique}"));
    let cert_dir = work_dir.join("certs");
    fs::create_dir_all(&cert_dir).expect("create cert dir");
    crate::certs::generate_certificates(
        &cert_dir,
        "Test CA",
        "localhost",
        "client.localhost",
        "machine",
        "client.localhost",
        "peer.localhost",
        "runtime-peer",
        "peer.localhost",
    )
    .expect("generate certs");

    let principal = resolve_cert_principal(
        &cert_dir.join("peer.crt"),
        PrincipalRef::new(PrincipalKind::Machine, "fallback"),
    )
    .expect("resolve peer principal");

    assert_eq!(
        principal,
        PrincipalRef::new(PrincipalKind::RuntimePeer, "peer.localhost")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn forward_control_plane_request_classifies_local_decode_and_auth_errors() {
    let state = sample_state("node-a");
    let bad_policy = AccessPolicy::from_specs(&[
        "principal=machine:client.localhost;methods=control_read;workloads=tenant-a/*".to_string(),
    ])
    .expect("policy");
    let request_context = bad_policy.resolve(
        PrincipalRef::new(PrincipalKind::Machine, "client.localhost"),
        "fp".to_string(),
    );

    let bad_frame = encode_request(Method::ControlQuery, 1, &Empty {}).expect("encode frame");
    let bad_envelope = decode_envelope(&bad_frame).expect("decode envelope");
    let bad_err =
        forward_control_plane_request(&state, &request_context, &bad_frame, &bad_envelope)
            .await
            .expect_err("decode should fail locally");
    assert!(matches!(
        bad_err,
        ForwardControlPlaneError::Local {
            status: 400,
            retryable: false,
            ..
        }
    ));

    let forbidden_frame = encode_request(
        Method::ControlQuery,
        2,
        &QueryApiRequest {
            query: Query::TableGet {
                table: "deployments".to_string(),
                key: "tenant-b/media/api".to_string(),
            },
            allow_stale: false,
        },
    )
    .expect("encode forbidden frame");
    let forbidden_envelope = decode_envelope(&forbidden_frame).expect("decode envelope");
    let forbidden_err = forward_control_plane_request(
        &state,
        &request_context,
        &forbidden_frame,
        &forbidden_envelope,
    )
    .await
    .expect_err("query should be forbidden locally");
    assert!(matches!(
        forbidden_err,
        ForwardControlPlaneError::Local {
            status: 403,
            retryable: false,
            ..
        }
    ));
}

#[test]
fn bridge_visibility_follows_visible_instance_scope() {
    let policy = AccessPolicy::from_specs(&[
        "principal=machine:client.localhost;methods=node_manage;workloads=tenant-a/*".to_string(),
    ])
    .expect("policy");
    let request_context = policy.resolve(
        PrincipalRef::new(PrincipalKind::Machine, "client.localhost"),
        "fp".to_string(),
    );
    let visible_instances = BTreeSet::from(["source-1".to_string()]);
    let local_instances = BTreeSet::from(["source-1".to_string(), "target-1".to_string()]);
    let hidden_target = ActiveEndpointBridgeSpec::Event(ActiveEventEndpointBridgeSpec {
        source_instance_id: "source-1".to_string(),
        source_endpoint: sample_endpoint("router", "frames"),
        target_instance_id: "target-1".to_string(),
        target_endpoint: PublicEndpointRef {
            workload: WorkloadRef {
                tenant: "tenant-b".to_string(),
                namespace: "media".to_string(),
                name: "detector".to_string(),
            },
            kind: ContractKind::Event,
            name: "frames".to_string(),
        },
        mode: EndpointBridgeMode::Local,
        target_node: "node-a".to_string(),
        target_daemon_addr: "127.0.0.1:7100".to_string(),
        target_daemon_server_name: "localhost".to_string(),
    });

    assert!(!bridge_visible_for_list_instances(
        &request_context,
        &visible_instances,
        &local_instances,
        &hidden_target,
    ));
}

#[test]
fn rewrite_mutation_requires_full_access_for_unscoped_mutations() {
    let policy = AccessPolicy::from_specs(&[
        "principal=machine:client.localhost;methods=control_write;workloads=tenant-a/*".to_string(),
    ])
    .expect("policy");
    let request_context = policy.resolve(
        PrincipalRef::new(PrincipalKind::Machine, "client.localhost"),
        "fp".to_string(),
    );

    let err = rewrite_mutation_for_authorisation(
        Mutation::UpsertNode {
            spec: NodeSpec {
                name: "node-a".to_string(),
                capacity_slots: 1,
                allocatable_cpu_millis: None,
                allocatable_memory_mib: None,
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
        &request_context,
    )
    .expect_err("node mutation should require full access");
    assert!(
        err.to_string()
            .contains("mutation requires full workload access")
    );
}

#[test]
fn rewrite_query_rejects_raw_queries_without_full_access() {
    let policy = AccessPolicy::from_specs(&[
        "principal=machine:client.localhost;methods=control_read;workloads=tenant-a/*".to_string(),
    ])
    .expect("policy");
    let request_context = policy.resolve(
        PrincipalRef::new(PrincipalKind::Machine, "client.localhost"),
        "fp".to_string(),
    );

    let err = rewrite_query_for_authorisation(
        Query::TableGet {
            table: "deployments".to_string(),
            key: "tenant-b/media/api".to_string(),
        },
        &request_context,
        Method::ControlQuery,
    )
    .expect_err("raw table query should require full access");
    assert!(
        err.to_string()
            .contains("query requires full workload and endpoint access")
    );
}

#[test]
fn rewrite_query_rejects_raw_queries_without_full_endpoint_access() {
    let policy = AccessPolicy::from_specs(&[
        "principal=machine:client.localhost;methods=control_read;workloads=*;endpoints=tenant-a/media/router#service:*".to_string(),
    ])
    .expect("policy");
    let request_context = policy.resolve(
        PrincipalRef::new(PrincipalKind::Machine, "client.localhost"),
        "fp".to_string(),
    );

    let err = rewrite_query_for_authorisation(
        Query::ControlPlaneState,
        &request_context,
        Method::ControlQuery,
    )
    .expect_err("raw state query should require full endpoint access");
    assert!(
        err.to_string()
            .contains("query requires full workload and endpoint access")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn instance_endpoint_authorisation_rejects_bridge_targets_outside_scope() {
    let state = sample_state("node-a");
    state.instance_workloads.lock().await.insert(
        "target-1".to_string(),
        "tenant-b/media/detector".to_string(),
    );
    let policy = AccessPolicy::from_specs(&[
        "principal=machine:client.localhost;methods=bridge_manage;endpoints=tenant-a/media/ingest#event:*".to_string(),
    ])
    .expect("policy");
    let request_context = policy.resolve(
        PrincipalRef::new(PrincipalKind::Machine, "client.localhost"),
        "fp".to_string(),
    );

    let err = ensure_instance_endpoint_authorised(
        &state,
        &request_context,
        Method::DeliverBridgeMessage,
        "target-1",
        &sample_endpoint("detector", "camera.frames"),
        "bridge target endpoint",
    )
    .await
    .expect_err("bridge target should be denied");

    assert!(
        err.to_string()
            .contains("unauthorised endpoint `tenant-a/media/detector#event:camera.frames`")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn instance_endpoint_authorisation_rejects_workload_mismatches() {
    let state = sample_state("node-a");
    state
        .instance_workloads
        .lock()
        .await
        .insert("target-1".to_string(), "tenant-a/media/router".to_string());
    let policy = AccessPolicy::from_specs(&[
        "principal=machine:client.localhost;methods=bridge_manage;workloads=tenant-a/*;endpoints=*"
            .to_string(),
    ])
    .expect("policy");
    let request_context = policy.resolve(
        PrincipalRef::new(PrincipalKind::Machine, "client.localhost"),
        "fp".to_string(),
    );

    let err = ensure_instance_endpoint_authorised(
        &state,
        &request_context,
        Method::DeliverBridgeMessage,
        "target-1",
        &sample_endpoint("detector", "camera.frames"),
        "bridge target endpoint",
    )
    .await
    .expect_err("mismatched workload should be denied");

    assert!(
        err.to_string().contains(
            "endpoint `tenant-a/media/detector#event:camera.frames` does not match instance `target-1` workload `tenant-a/media/router`"
        )
    );
}

#[tokio::test(flavor = "current_thread")]
async fn activate_endpoint_bridge_forwards_local_event_frames() {
    LocalSet::new()
        .run_until(async {
            let state = sample_state("local-node");
            state
                .processes
                .lock()
                .await
                .insert("source-1".to_string(), 7);
            ensure_managed_endpoint_bindings(
                &state,
                "source-1",
                &[ManagedEndpointBinding {
                    endpoint_name: "camera.frames".to_string(),
                    endpoint_kind: ContractKind::Event,
                    role: ManagedEndpointRole::Egress,
                    binding_type: selium_control_plane_protocol::ManagedEndpointBindingType::OneWay,
                }],
            )
            .await
            .expect("register source binding");
            ensure_managed_endpoint_bindings(
                &state,
                "target-1",
                &[ManagedEndpointBinding {
                    endpoint_name: "camera.frames".to_string(),
                    endpoint_kind: ContractKind::Event,
                    role: ManagedEndpointRole::Ingress,
                    binding_type: selium_control_plane_protocol::ManagedEndpointBindingType::OneWay,
                }],
            )
            .await
            .expect("register target binding");

            let response = activate_endpoint_bridge(
                &state,
                &ActivateEndpointBridgeRequest {
                    node_id: "local-node".to_string(),
                    bridge_id: "bridge-1".to_string(),
                    source_instance_id: "source-1".to_string(),
                    source_endpoint: sample_endpoint("ingress", "camera.frames"),
                    target_instance_id: "target-1".to_string(),
                    target_node: "local-node".to_string(),
                    target_daemon_addr: "127.0.0.1:7100".to_string(),
                    target_daemon_server_name: "localhost".to_string(),
                    target_endpoint: sample_endpoint("detector", "camera.frames"),
                    semantics: selium_control_plane_protocol::EndpointBridgeSemantics::Event(
                        selium_control_plane_protocol::EventBridgeSemantics {
                            delivery: selium_control_plane_protocol::EventDeliveryMode::Frame,
                        },
                    ),
                },
            )
            .await
            .expect("activate route");
            assert_eq!(response.mode, EndpointBridgeMode::Local);

            let source = state
                .source_bindings
                .lock()
                .await
                .get(&endpoint_key(
                    "source-1",
                    &sample_endpoint("ingress", "camera.frames"),
                ))
                .cloned()
                .expect("source binding present");
            let target = state
                .target_bindings
                .lock()
                .await
                .get(&endpoint_key(
                    "target-1",
                    &sample_endpoint("detector", "camera.frames"),
                ))
                .cloned()
                .expect("target binding present");

            enqueue_managed_event_frame(&state, &source.queue, b"frame-local")
                .await
                .expect("enqueue source frame");

            let reader = QueueService
                .attach(
                    &target.queue,
                    QueueAttach {
                        shared_id: 0,
                        role: QueueRole::Reader,
                    },
                )
                .expect("attach target reader");
            let waited = QueueService
                .wait(&reader, 2_000)
                .await
                .expect("wait target frame");
            let frame = waited.frame.expect("frame available");
            let payload =
                read_managed_event_frame(&state, frame.shm_shared_id, frame.offset, frame.len)
                    .await
                    .expect("read target frame");
            assert_eq!(payload, b"frame-local");
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn deliver_bridge_message_event_writes_target_queue() {
    let state = sample_state("remote-node");
    ensure_managed_endpoint_bindings(
        &state,
        "target-1",
        &[ManagedEndpointBinding {
            endpoint_name: "camera.frames".to_string(),
            endpoint_kind: ContractKind::Event,
            role: ManagedEndpointRole::Ingress,
            binding_type: selium_control_plane_protocol::ManagedEndpointBindingType::OneWay,
        }],
    )
    .await
    .expect("register target binding");

    let delivered = deliver_bridge_message_local(
        &state,
        "target-1",
        &sample_endpoint("detector", "camera.frames"),
        &BridgeMessage::Event(EventBridgeMessage {
            payload: b"frame-remote".to_vec(),
        }),
    )
    .await
    .expect("deliver bridge message");
    assert!(matches!(
        delivered,
        BridgeMessageDelivery {
            delivered: true,
            message: None,
        }
    ));

    let target = state
        .target_bindings
        .lock()
        .await
        .get(&endpoint_key(
            "target-1",
            &sample_endpoint("detector", "camera.frames"),
        ))
        .cloned()
        .expect("target binding present");
    let reader = QueueService
        .attach(
            &target.queue,
            QueueAttach {
                shared_id: 0,
                role: QueueRole::Reader,
            },
        )
        .expect("attach target reader");
    let waited = QueueService
        .wait(&reader, 2_000)
        .await
        .expect("wait target frame");
    let frame = waited.frame.expect("frame available");
    let payload = read_managed_event_frame(&state, frame.shm_shared_id, frame.offset, frame.len)
        .await
        .expect("read target frame");
    assert_eq!(payload, b"frame-remote");
}

#[tokio::test(flavor = "current_thread")]
async fn activate_endpoint_bridge_forwards_local_guest_log_event_frames() {
    LocalSet::new()
        .run_until(async {
            let state = sample_state("local-node");
            state
                .processes
                .lock()
                .await
                .insert("source-1".to_string(), 7);
            ensure_guest_log_bindings(&state, "source-1")
                .await
                .expect("register guest log bindings");
            ensure_managed_endpoint_bindings(
                &state,
                "target-1",
                &[ManagedEndpointBinding {
                    endpoint_name: GUEST_LOG_STDOUT_ENDPOINT.to_string(),
                    endpoint_kind: ContractKind::Event,
                    role: ManagedEndpointRole::Ingress,
                    binding_type: selium_control_plane_protocol::ManagedEndpointBindingType::OneWay,
                }],
            )
            .await
            .expect("register target binding");

            let response = activate_endpoint_bridge(
                &state,
                &ActivateEndpointBridgeRequest {
                    node_id: "local-node".to_string(),
                    bridge_id: "bridge-guest-log".to_string(),
                    source_instance_id: "source-1".to_string(),
                    source_endpoint: sample_endpoint("ingest", GUEST_LOG_STDOUT_ENDPOINT),
                    target_instance_id: "target-1".to_string(),
                    target_node: "local-node".to_string(),
                    target_daemon_addr: "127.0.0.1:7100".to_string(),
                    target_daemon_server_name: "localhost".to_string(),
                    target_endpoint: sample_endpoint("attach", GUEST_LOG_STDOUT_ENDPOINT),
                    semantics: selium_control_plane_protocol::EndpointBridgeSemantics::Event(
                        selium_control_plane_protocol::EventBridgeSemantics {
                            delivery: selium_control_plane_protocol::EventDeliveryMode::Frame,
                        },
                    ),
                },
            )
            .await
            .expect("activate guest log route");
            assert_eq!(response.mode, EndpointBridgeMode::Local);

            let source = state
                .source_bindings
                .lock()
                .await
                .get(&endpoint_key(
                    "source-1",
                    &sample_endpoint("ingest", GUEST_LOG_STDOUT_ENDPOINT),
                ))
                .cloned()
                .expect("guest log source binding present");
            let target = state
                .target_bindings
                .lock()
                .await
                .get(&endpoint_key(
                    "target-1",
                    &sample_endpoint("attach", GUEST_LOG_STDOUT_ENDPOINT),
                ))
                .cloned()
                .expect("target binding present");

            enqueue_managed_event_frame(&state, &source.queue, b"guest stdout")
                .await
                .expect("enqueue guest log frame");

            let reader = QueueService
                .attach(
                    &target.queue,
                    QueueAttach {
                        shared_id: 0,
                        role: QueueRole::Reader,
                    },
                )
                .expect("attach target reader");
            let waited = QueueService
                .wait(&reader, 2_000)
                .await
                .expect("wait target frame");
            let frame = waited.frame.expect("frame available");
            let payload =
                read_managed_event_frame(&state, frame.shm_shared_id, frame.offset, frame.len)
                    .await
                    .expect("read target frame");
            assert_eq!(payload, b"guest stdout");
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn resolve_guest_log_subscription_accepts_exact_stdout_and_stderr() {
    let engine = guest_log_resolution_engine(1);
    let resolved = resolve_guest_log_subscription_with(
        "local-node",
        &SubscribeGuestLogsRequest {
            target: OperationalProcessSelector::ReplicaKey(
                "tenant=tenant-a;namespace=media;workload=ingest;replica=0".to_string(),
            ),
            stream_names: vec![
                GUEST_LOG_STDOUT_ENDPOINT.to_string(),
                GUEST_LOG_STDERR_ENDPOINT.to_string(),
            ],
        },
        DiscoveryCapabilityScope::allow_all(),
        |query| async { engine_query(&engine, query) },
    )
    .await
    .expect("resolve guest log subscription");

    assert_eq!(resolved.target.node, "local-node");
    assert_eq!(
        resolved.target.replica_key,
        "tenant=tenant-a;namespace=media;workload=ingest;replica=0"
    );
    assert_eq!(
        resolved
            .streams
            .iter()
            .map(|endpoint| endpoint.name.as_str())
            .collect::<Vec<_>>(),
        vec![GUEST_LOG_STDOUT_ENDPOINT, GUEST_LOG_STDERR_ENDPOINT]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn resolve_guest_log_subscription_reports_ambiguous_workloads() {
    let engine = guest_log_resolution_engine(2);
    let err = resolve_guest_log_subscription_with(
        "local-node",
        &SubscribeGuestLogsRequest {
            target: OperationalProcessSelector::Workload(WorkloadRef {
                tenant: "tenant-a".to_string(),
                namespace: "media".to_string(),
                name: "ingest".to_string(),
            }),
            stream_names: vec![GUEST_LOG_STDOUT_ENDPOINT.to_string()],
        },
        DiscoveryCapabilityScope::allow_all(),
        |query| async { engine_query(&engine, query) },
    )
    .await
    .expect_err("ambiguous workload target");

    assert!(err.to_string().contains("specify a replica key"));
}

#[tokio::test(flavor = "current_thread")]
async fn resolve_guest_log_subscription_reports_unknown_streams() {
    let engine = guest_log_resolution_engine(1);
    let err = resolve_guest_log_subscription_with(
        "local-node",
        &SubscribeGuestLogsRequest {
            target: OperationalProcessSelector::ReplicaKey(
                "tenant=tenant-a;namespace=media;workload=ingest;replica=0".to_string(),
            ),
            stream_names: vec!["stdx".to_string()],
        },
        DiscoveryCapabilityScope::allow_all(),
        |query| async { engine_query(&engine, query) },
    )
    .await
    .expect_err("unknown stream target");

    assert!(err.to_string().contains("unknown endpoint"));
    assert!(err.to_string().contains("stdx"));
}

#[tokio::test(flavor = "current_thread")]
async fn guest_log_resolution_classifies_unauthorised_scope_as_forbidden() {
    let engine = guest_log_resolution_engine(1);
    let err = resolve_guest_log_subscription_with(
        "local-node",
        &SubscribeGuestLogsRequest {
            target: OperationalProcessSelector::ReplicaKey(
                "tenant=tenant-a;namespace=media;workload=ingest;replica=0".to_string(),
            ),
            stream_names: vec![GUEST_LOG_STDOUT_ENDPOINT.to_string()],
        },
        DiscoveryCapabilityScope {
            operations: vec![DiscoveryOperation::Discover, DiscoveryOperation::Bind],
            workloads: vec![DiscoveryPattern::Prefix("tenant-a/".to_string())],
            endpoints: vec![DiscoveryPattern::Prefix(String::new())],
            allow_operational_processes: false,
        },
        |query| async { engine_query(&engine, query) },
    )
    .await
    .expect_err("operational process discovery should be denied");

    assert_eq!(classify_guest_log_resolution_error(&err), 403);
}

#[tokio::test(flavor = "current_thread")]
async fn guest_log_subscription_streams_live_stdout_and_stderr_frames() {
    LocalSet::new()
        .run_until(async {
            let state = sample_state("local-node");
            state
                .processes
                .lock()
                .await
                .insert("source-1".to_string(), 7);
            ensure_guest_log_bindings(&state, "source-1")
                .await
                .expect("register guest log bindings");

            let resolved = ResolvedGuestLogSubscription {
                target: OperationalProcessRecord {
                    workload: WorkloadRef {
                        tenant: "tenant-a".to_string(),
                        namespace: "media".to_string(),
                        name: "ingest".to_string(),
                    },
                    replica_key: "source-1".to_string(),
                    node: "local-node".to_string(),
                },
                source_node: sample_node("local-node", "127.0.0.1:7100", "localhost"),
                local_node: sample_node("local-node", "127.0.0.1:7100", "localhost"),
                streams: vec![
                    sample_endpoint("ingest", GUEST_LOG_STDOUT_ENDPOINT),
                    sample_endpoint("ingest", GUEST_LOG_STDERR_ENDPOINT),
                ],
            };
            let subscription = activate_guest_log_subscription(&state, &resolved)
                .await
                .expect("activate guest log subscription");

            let stdout_queue = state
                .source_bindings
                .lock()
                .await
                .get(&endpoint_key(
                    "source-1",
                    &sample_endpoint("ingest", GUEST_LOG_STDOUT_ENDPOINT),
                ))
                .cloned()
                .expect("stdout queue present");
            let stderr_queue = state
                .source_bindings
                .lock()
                .await
                .get(&endpoint_key(
                    "source-1",
                    &sample_endpoint("ingest", GUEST_LOG_STDERR_ENDPOINT),
                ))
                .cloned()
                .expect("stderr queue present");

            let (mut client, mut server) = duplex(4096);
            let state_for_task = Rc::clone(&state);
            let instance_id = subscription.instance_id.clone();
            let streams = resolved.streams.clone();
            let stream_task = spawn_local(async move {
                stream_guest_log_events(&state_for_task, &instance_id, &streams, &mut server).await
            });

            enqueue_managed_event_frame(&state, &stdout_queue.queue, b"hello stdout")
                .await
                .expect("enqueue stdout frame");
            enqueue_managed_event_frame(&state, &stderr_queue.queue, b"hello stderr")
                .await
                .expect("enqueue stderr frame");

            let first: GuestLogEvent =
                decode_rkyv(&read_framed(&mut client).await.expect("read first frame"))
                    .expect("decode first guest log event");
            let second: GuestLogEvent =
                decode_rkyv(&read_framed(&mut client).await.expect("read second frame"))
                    .expect("decode second guest log event");
            assert_eq!(first.endpoint.name, GUEST_LOG_STDOUT_ENDPOINT);
            assert_eq!(first.payload, b"hello stdout");
            assert_eq!(second.endpoint.name, GUEST_LOG_STDERR_ENDPOINT);
            assert_eq!(second.payload, b"hello stderr");

            drop(client);
            enqueue_managed_event_frame(&state, &stdout_queue.queue, b"goodbye")
                .await
                .expect("enqueue disconnect frame");
            let stream_result = stream_task.await.expect("stream task join");
            assert!(stream_result.is_err());

            deactivate_guest_log_subscription(&state, &resolved, &subscription)
                .await
                .expect("deactivate guest log subscription");
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn activate_endpoint_bridge_forwards_local_service_request_response() {
    LocalSet::new()
        .run_until(async {
            let state = sample_state("local-node");
            state
                .processes
                .lock()
                .await
                .insert("source-1".to_string(), 7);
            ensure_managed_endpoint_bindings(
                &state,
                "source-1",
                &[ManagedEndpointBinding {
                    endpoint_name: "shared".to_string(),
                    endpoint_kind: ContractKind::Service,
                    role: ManagedEndpointRole::Egress,
                    binding_type: ManagedEndpointBindingType::RequestResponse,
                }],
            )
            .await
            .expect("register source binding");
            ensure_managed_endpoint_bindings(
                &state,
                "target-1",
                &[ManagedEndpointBinding {
                    endpoint_name: "shared".to_string(),
                    endpoint_kind: ContractKind::Service,
                    role: ManagedEndpointRole::Ingress,
                    binding_type: ManagedEndpointBindingType::RequestResponse,
                }],
            )
            .await
            .expect("register target binding");

            let response = activate_endpoint_bridge(
                &state,
                &ActivateEndpointBridgeRequest {
                    node_id: "local-node".to_string(),
                    bridge_id: "bridge-service".to_string(),
                    source_instance_id: "source-1".to_string(),
                    source_endpoint: sample_endpoint_kind(
                        ContractKind::Service,
                        "ingest",
                        "shared",
                    ),
                    target_instance_id: "target-1".to_string(),
                    target_node: "local-node".to_string(),
                    target_daemon_addr: "127.0.0.1:7100".to_string(),
                    target_daemon_server_name: "localhost".to_string(),
                    target_endpoint: sample_endpoint_kind(
                        ContractKind::Service,
                        "detector",
                        "shared",
                    ),
                    semantics: EndpointBridgeSemantics::Service(
                        selium_control_plane_protocol::ServiceBridgeSemantics {
                            correlation:
                                selium_control_plane_protocol::ServiceCorrelationMode::RequestId,
                        },
                    ),
                },
            )
            .await
            .expect("activate service route");
            assert_eq!(response.mode, EndpointBridgeMode::Local);

            let source_request = state
                .source_bindings
                .lock()
                .await
                .get(&endpoint_key(
                    "source-1",
                    &sample_endpoint_kind(ContractKind::Service, "ingest", "shared"),
                ))
                .cloned()
                .expect("source request binding present");
            let source_response = state
                .service_response_bindings
                .lock()
                .await
                .get(&endpoint_key(
                    "source-1",
                    &sample_endpoint_kind(ContractKind::Service, "ingest", "shared"),
                ))
                .cloned()
                .expect("source response binding present");
            let target_request = state
                .target_bindings
                .lock()
                .await
                .get(&endpoint_key(
                    "target-1",
                    &sample_endpoint_kind(ContractKind::Service, "detector", "shared"),
                ))
                .cloned()
                .expect("target request binding present");
            let target_response = state
                .service_response_bindings
                .lock()
                .await
                .get(&endpoint_key(
                    "target-1",
                    &sample_endpoint_kind(ContractKind::Service, "detector", "shared"),
                ))
                .cloned()
                .expect("target response binding present");

            let service_state = Rc::clone(&state);
            spawn_local(async move {
                let reader = QueueService
                    .attach(
                        &target_request.queue,
                        QueueAttach {
                            shared_id: 0,
                            role: QueueRole::Reader,
                        },
                    )
                    .expect("attach target request reader");
                let waited = QueueService
                    .wait(&reader, 2_000)
                    .await
                    .expect("wait target request");
                let frame = waited.frame.expect("target request frame");
                let request = read_managed_service_frame(
                    &service_state,
                    frame.shm_shared_id,
                    frame.offset,
                    frame.len,
                )
                .await
                .expect("decode service request");
                assert_eq!(request.exchange_id, "req-42");
                assert_eq!(request.phase, ServiceMessagePhase::Request);
                assert_eq!(request.payload, b"detect".to_vec());
                QueueService
                    .ack(
                        &reader,
                        QueueAck {
                            endpoint_id: 0,
                            seq: frame.seq,
                        },
                    )
                    .expect("ack target request");

                enqueue_managed_service_frame(
                    &service_state,
                    &target_response.queue,
                    &ServiceBridgeMessage {
                        exchange_id: request.exchange_id,
                        phase: ServiceMessagePhase::Response,
                        sequence: request.sequence + 1,
                        complete: true,
                        payload: b"ok".to_vec(),
                    },
                )
                .await
                .expect("enqueue target response");
            });

            enqueue_managed_service_frame(
                &state,
                &source_request.queue,
                &ServiceBridgeMessage {
                    exchange_id: "req-42".to_string(),
                    phase: ServiceMessagePhase::Request,
                    sequence: 0,
                    complete: true,
                    payload: b"detect".to_vec(),
                },
            )
            .await
            .expect("enqueue source request");

            let reader = QueueService
                .attach(
                    &source_response.queue,
                    QueueAttach {
                        shared_id: 0,
                        role: QueueRole::Reader,
                    },
                )
                .expect("attach source response reader");
            let waited = QueueService
                .wait(&reader, 2_000)
                .await
                .expect("wait source response");
            let frame = waited.frame.expect("source response frame");
            let response =
                read_managed_service_frame(&state, frame.shm_shared_id, frame.offset, frame.len)
                    .await
                    .expect("decode source response");
            assert_eq!(response.exchange_id, "req-42");
            assert_eq!(response.phase, ServiceMessagePhase::Response);
            assert_eq!(response.payload, b"ok".to_vec());
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn activate_endpoint_bridge_forwards_local_stream_lifecycle_frames() {
    LocalSet::new()
        .run_until(async {
            let state = sample_state("local-node");
            state
                .processes
                .lock()
                .await
                .insert("source-1".to_string(), 7);
            ensure_managed_endpoint_bindings(
                &state,
                "source-1",
                &[ManagedEndpointBinding {
                    endpoint_name: "shared".to_string(),
                    endpoint_kind: ContractKind::Stream,
                    role: ManagedEndpointRole::Egress,
                    binding_type: ManagedEndpointBindingType::Session,
                }],
            )
            .await
            .expect("register source binding");
            ensure_managed_endpoint_bindings(
                &state,
                "target-1",
                &[ManagedEndpointBinding {
                    endpoint_name: "shared".to_string(),
                    endpoint_kind: ContractKind::Stream,
                    role: ManagedEndpointRole::Ingress,
                    binding_type: ManagedEndpointBindingType::Session,
                }],
            )
            .await
            .expect("register target binding");

            let response = activate_endpoint_bridge(
                &state,
                &ActivateEndpointBridgeRequest {
                    node_id: "local-node".to_string(),
                    bridge_id: "bridge-stream".to_string(),
                    source_instance_id: "source-1".to_string(),
                    source_endpoint: sample_endpoint_kind(ContractKind::Stream, "ingest", "shared"),
                    target_instance_id: "target-1".to_string(),
                    target_node: "local-node".to_string(),
                    target_daemon_addr: "127.0.0.1:7100".to_string(),
                    target_daemon_server_name: "localhost".to_string(),
                    target_endpoint: sample_endpoint_kind(
                        ContractKind::Stream,
                        "detector",
                        "shared",
                    ),
                    semantics: EndpointBridgeSemantics::Stream(
                        selium_control_plane_protocol::StreamBridgeSemantics {
                            lifecycle:
                                selium_control_plane_protocol::StreamLifecycleMode::SessionFrames,
                        },
                    ),
                },
            )
            .await
            .expect("activate stream route");
            assert_eq!(response.mode, EndpointBridgeMode::Local);

            let source = state
                .source_bindings
                .lock()
                .await
                .get(&endpoint_key(
                    "source-1",
                    &sample_endpoint_kind(ContractKind::Stream, "ingest", "shared"),
                ))
                .cloned()
                .expect("source binding present");
            let target = state
                .target_bindings
                .lock()
                .await
                .get(&endpoint_key(
                    "target-1",
                    &sample_endpoint_kind(ContractKind::Stream, "detector", "shared"),
                ))
                .cloned()
                .expect("target binding present");

            for message in [
                StreamBridgeMessage {
                    session_id: "session-7".to_string(),
                    lifecycle: selium_control_plane_protocol::StreamLifecycle::Open,
                    sequence: 0,
                    payload: b"hello".to_vec(),
                },
                StreamBridgeMessage {
                    session_id: "session-7".to_string(),
                    lifecycle: selium_control_plane_protocol::StreamLifecycle::Data,
                    sequence: 1,
                    payload: b"chunk".to_vec(),
                },
                StreamBridgeMessage {
                    session_id: "session-7".to_string(),
                    lifecycle: selium_control_plane_protocol::StreamLifecycle::Close,
                    sequence: 2,
                    payload: Vec::new(),
                },
            ] {
                enqueue_managed_stream_frame(&state, &source.queue, &message)
                    .await
                    .expect("enqueue source stream frame");
            }

            let reader = QueueService
                .attach(
                    &target.queue,
                    QueueAttach {
                        shared_id: 0,
                        role: QueueRole::Reader,
                    },
                )
                .expect("attach target reader");

            for (expected_lifecycle, expected_sequence, expected_payload) in [
                (
                    selium_control_plane_protocol::StreamLifecycle::Open,
                    0,
                    b"hello".as_slice(),
                ),
                (
                    selium_control_plane_protocol::StreamLifecycle::Data,
                    1,
                    b"chunk".as_slice(),
                ),
                (
                    selium_control_plane_protocol::StreamLifecycle::Close,
                    2,
                    b"".as_slice(),
                ),
            ] {
                let waited = QueueService
                    .wait(&reader, 2_000)
                    .await
                    .expect("wait target stream frame");
                let frame = waited.frame.expect("stream frame available");
                let message =
                    read_managed_stream_frame(&state, frame.shm_shared_id, frame.offset, frame.len)
                        .await
                        .expect("read target stream frame");
                assert_eq!(message.session_id, "session-7");
                assert_eq!(message.lifecycle, expected_lifecycle);
                assert_eq!(message.sequence, expected_sequence);
                assert_eq!(message.payload, expected_payload);
                QueueService
                    .ack(
                        &reader,
                        QueueAck {
                            endpoint_id: 0,
                            seq: frame.seq,
                        },
                    )
                    .expect("ack target stream frame");
            }
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn deliver_bridge_message_stream_writes_target_queue() {
    let state = sample_state("remote-node");
    ensure_managed_endpoint_bindings(
        &state,
        "target-1",
        &[ManagedEndpointBinding {
            endpoint_name: "shared".to_string(),
            endpoint_kind: ContractKind::Stream,
            role: ManagedEndpointRole::Ingress,
            binding_type: ManagedEndpointBindingType::Session,
        }],
    )
    .await
    .expect("register target binding");

    let delivered = deliver_bridge_message_local(
        &state,
        "target-1",
        &sample_endpoint_kind(ContractKind::Stream, "detector", "shared"),
        &BridgeMessage::Stream(StreamBridgeMessage {
            session_id: "session-9".to_string(),
            lifecycle: selium_control_plane_protocol::StreamLifecycle::Abort,
            sequence: 3,
            payload: b"cancel".to_vec(),
        }),
    )
    .await
    .expect("deliver bridge stream message");
    assert!(matches!(
        delivered,
        BridgeMessageDelivery {
            delivered: true,
            message: None,
        }
    ));

    let target = state
        .target_bindings
        .lock()
        .await
        .get(&endpoint_key(
            "target-1",
            &sample_endpoint_kind(ContractKind::Stream, "detector", "shared"),
        ))
        .cloned()
        .expect("target binding present");
    let reader = QueueService
        .attach(
            &target.queue,
            QueueAttach {
                shared_id: 0,
                role: QueueRole::Reader,
            },
        )
        .expect("attach target reader");
    let waited = QueueService
        .wait(&reader, 2_000)
        .await
        .expect("wait target stream frame");
    let frame = waited.frame.expect("stream frame available");
    let message = read_managed_stream_frame(&state, frame.shm_shared_id, frame.offset, frame.len)
        .await
        .expect("read target stream frame");
    assert_eq!(message.session_id, "session-9");
    assert_eq!(
        message.lifecycle,
        selium_control_plane_protocol::StreamLifecycle::Abort
    );
    assert_eq!(message.sequence, 3);
    assert_eq!(message.payload, b"cancel");
}

#[tokio::test(flavor = "current_thread")]
async fn managed_endpoint_binding_payload_partitions_same_name_bindings_by_kind() {
    let state = sample_state("remote-node");
    let bindings = ensure_managed_endpoint_bindings(
        &state,
        "target-1",
        &[
            ManagedEndpointBinding {
                endpoint_name: "shared".to_string(),
                endpoint_kind: ContractKind::Event,
                role: ManagedEndpointRole::Ingress,
                binding_type: selium_control_plane_protocol::ManagedEndpointBindingType::OneWay,
            },
            ManagedEndpointBinding {
                endpoint_name: "shared".to_string(),
                endpoint_kind: ContractKind::Service,
                role: ManagedEndpointRole::Ingress,
                binding_type:
                    selium_control_plane_protocol::ManagedEndpointBindingType::RequestResponse,
            },
            ManagedEndpointBinding {
                endpoint_name: "shared".to_string(),
                endpoint_kind: ContractKind::Stream,
                role: ManagedEndpointRole::Ingress,
                binding_type: ManagedEndpointBindingType::Session,
            },
        ],
    )
    .await
    .expect("register bindings")
    .expect("bindings payload");

    let decoded = decode_rkyv::<DataValue>(&bindings).expect("decode bindings payload");
    let event = decoded
        .get("readers")
        .and_then(|section| section.get("event"))
        .and_then(|section| section.get("shared"))
        .expect("event binding present");
    let service_reader = decoded
        .get("readers")
        .and_then(|section| section.get("service"))
        .and_then(|section| section.get("shared"))
        .expect("service reader binding present");
    let service_writer = decoded
        .get("writers")
        .and_then(|section| section.get("service"))
        .and_then(|section| section.get("shared"))
        .expect("service writer binding present");
    let stream = decoded
        .get("readers")
        .and_then(|section| section.get("stream"))
        .and_then(|section| section.get("shared"))
        .expect("stream binding present");

    assert_ne!(
        event
            .get("queue_shared_id")
            .and_then(DataValue::as_u64)
            .expect("event queue id"),
        service_reader
            .get("queue_shared_id")
            .and_then(DataValue::as_u64)
            .expect("service reader queue id")
    );
    assert_ne!(
        service_reader
            .get("queue_shared_id")
            .and_then(DataValue::as_u64)
            .expect("service reader queue id"),
        service_writer
            .get("queue_shared_id")
            .and_then(DataValue::as_u64)
            .expect("service writer queue id")
    );
    assert_ne!(
        event
            .get("queue_shared_id")
            .and_then(DataValue::as_u64)
            .expect("event queue id"),
        stream
            .get("queue_shared_id")
            .and_then(DataValue::as_u64)
            .expect("stream queue id")
    );
    assert_ne!(
        service_reader
            .get("queue_shared_id")
            .and_then(DataValue::as_u64)
            .expect("service reader queue id"),
        stream
            .get("queue_shared_id")
            .and_then(DataValue::as_u64)
            .expect("stream queue id")
    );
    assert_eq!(
        event
            .get("endpoint_kind")
            .and_then(DataValue::as_str)
            .expect("event kind"),
        "event"
    );
    assert_eq!(
        service_reader
            .get("endpoint_kind")
            .and_then(DataValue::as_str)
            .expect("service kind"),
        "service"
    );
    assert_eq!(
        service_writer
            .get("endpoint_kind")
            .and_then(DataValue::as_str)
            .expect("service writer kind"),
        "service"
    );
    assert_eq!(
        stream
            .get("endpoint_kind")
            .and_then(DataValue::as_str)
            .expect("stream kind"),
        "stream"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn read_managed_event_frame_respects_committed_length() {
    let state = sample_state("remote-node");
    let queue = QueueService
        .create(QueueCreate {
            capacity_frames: 8,
            max_frame_bytes: 512,
            delivery: QueueDelivery::Lossless,
            overflow: QueueOverflow::Block,
        })
        .expect("create queue");
    let writer = QueueService
        .attach(
            &queue,
            QueueAttach {
                shared_id: 0,
                role: QueueRole::Writer { writer_id: 1 },
            },
        )
        .expect("attach writer");
    let reader = QueueService
        .attach(
            &queue,
            QueueAttach {
                shared_id: 0,
                role: QueueRole::Reader,
            },
        )
        .expect("attach reader");
    let driver = state
        .kernel
        .get::<SharedMemoryDriver>()
        .expect("shared memory driver");
    let region = driver
        .alloc(ShmAlloc {
            size: 512,
            align: 8,
        })
        .expect("allocate shared memory");
    driver
        .write(region, 0, b"frame-remote")
        .expect("write payload");
    driver
        .write(region, 12, &[0; 4])
        .expect("write trailing padding");
    let shm = state
        .registry
        .add(region, None, ResourceType::SharedMemory)
        .expect("register shared memory");
    let shm_shared_id = state
        .registry
        .share_handle(shm.into_id())
        .expect("share shared memory");
    let reserved = QueueService
        .reserve(
            &writer,
            QueueReserve {
                endpoint_id: 0,
                len: 12,
                timeout_ms: 1_000,
            },
        )
        .await
        .expect("reserve queue slot");
    let reservation = reserved.reservation.expect("queue reservation");
    QueueService
        .commit(
            &writer,
            QueueCommit {
                endpoint_id: 0,
                reservation_id: reservation.reservation_id,
                shm_shared_id,
                offset: 0,
                len: 12,
            },
        )
        .expect("commit queue slot");

    let waited = QueueService
        .wait(&reader, 2_000)
        .await
        .expect("wait for frame");
    let frame = waited.frame.expect("frame available");
    let payload = read_managed_event_frame(&state, frame.shm_shared_id, frame.offset, frame.len)
        .await
        .expect("read committed payload");
    assert_eq!(payload, b"frame-remote");
}

#[test]
fn append_managed_endpoint_bindings_arg_uses_typed_buffer_argument() {
    let spec = append_managed_endpoint_bindings_arg("path=demo.wasm", Some(&[0x41, 0x42]))
        .expect("append bindings arg");
    assert_eq!(spec, "path=demo.wasm;args=buffer:hex:4142;params=buffer");
}

#[test]
fn append_managed_endpoint_bindings_arg_prepends_existing_params_and_args() {
    let spec = append_managed_endpoint_bindings_arg(
        "path=demo.wasm;params=utf8,i32;args=billing,3",
        Some(&[0x41, 0x42]),
    )
    .expect("append bindings arg");
    assert_eq!(
        spec,
        "path=demo.wasm;params=buffer,utf8,i32;args=buffer:hex:4142,billing,3"
    );
}

#[test]
fn append_managed_endpoint_bindings_arg_keeps_typed_arg_inference_when_params_are_omitted() {
    let spec = append_managed_endpoint_bindings_arg(
        "path=demo.wasm;args=utf8:search,i32:5",
        Some(&[0x41, 0x42]),
    )
    .expect("append bindings arg");
    assert_eq!(
        spec,
        "path=demo.wasm;args=buffer:hex:4142,utf8:search,i32:5"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn runtime_usage_query_returns_bounded_records_and_resume_cursor() {
    let state = sample_state("node-a");
    let collector = state
        .kernel
        .get::<RuntimeUsageCollector>()
        .expect("runtime usage collector")
        .clone();
    let first = collector
        .register_process(
            "tenant-a/media/ingest",
            "process-a",
            ProcessUsageAttribution {
                instance_id: Some("tenant-a/media/ingest/0".to_string()),
                external_account_ref: Some("acct-a".to_string()),
                module_id: "module-a".to_string(),
            },
        )
        .await
        .expect("register first process");
    sleep(Duration::from_millis(2)).await;
    first.finish().await.expect("finish first process");

    sleep(Duration::from_millis(2)).await;

    let second = collector
        .register_process(
            "tenant-a/media/ingest",
            "process-b",
            ProcessUsageAttribution {
                instance_id: Some("tenant-a/media/ingest/1".to_string()),
                external_account_ref: Some("acct-a".to_string()),
                module_id: "module-a".to_string(),
            },
        )
        .await
        .expect("register second process");
    sleep(Duration::from_millis(2)).await;
    second.finish().await.expect("finish second process");

    let response = query_runtime_usage(
        &state,
        &RuntimeUsageApiRequest {
            node_id: "node-a".to_string(),
            query: RuntimeUsageQuery {
                start: RuntimeUsageReplayStart::Earliest,
                save_checkpoint: None,
                limit: 1,
                external_account_ref: Some("acct-a".to_string()),
                workload: Some("tenant-a/media/ingest".to_string()),
                module: Some("module-a".to_string()),
                window_start_ms: None,
                window_end_ms: None,
            },
        },
    )
    .await
    .expect("query runtime usage");

    assert_eq!(response.records.len(), 1);
    assert_eq!(response.records[0].sample.process_id, "process-a");
    assert_eq!(response.records[0].sample.attribution.module_id, "module-a");
    let high_watermark = response.high_watermark.expect("high watermark");
    assert!(high_watermark >= response.records[0].sequence);
    assert_eq!(
        response.next_sequence,
        Some(response.records[0].sequence.saturating_add(1))
    );

    let resumed = query_runtime_usage(
        &state,
        &RuntimeUsageApiRequest {
            node_id: "node-a".to_string(),
            query: RuntimeUsageQuery {
                start: RuntimeUsageReplayStart::Sequence(
                    response.next_sequence.expect("resume sequence"),
                ),
                save_checkpoint: None,
                limit: 10,
                external_account_ref: Some("acct-a".to_string()),
                workload: Some("tenant-a/media/ingest".to_string()),
                module: Some("module-a".to_string()),
                window_start_ms: None,
                window_end_ms: None,
            },
        },
    )
    .await
    .expect("resume runtime usage query");

    assert_eq!(resumed.records.len(), 1);
    assert_eq!(resumed.records[0].sample.process_id, "process-b");
    assert_eq!(resumed.high_watermark, Some(high_watermark));
    assert_eq!(
        resumed.next_sequence,
        Some(resumed.records[0].sequence.saturating_add(1))
    );
}

#[tokio::test]
async fn runtime_usage_query_zero_limit_returns_watermark_without_advancing_cursor() {
    let state = sample_state("node-a");
    let collector = state
        .kernel
        .get::<crate::usage::RuntimeUsageCollector>()
        .cloned()
        .expect("collector available");
    let handle = collector
        .register_process(
            "tenant-a/media/ingest",
            "123",
            ProcessUsageAttribution {
                instance_id: Some("tenant-a/media/ingest/0".to_string()),
                external_account_ref: Some("acct-a".to_string()),
                module_id: "module-a".to_string(),
            },
        )
        .await
        .expect("register process");
    sleep(Duration::from_millis(2)).await;
    handle.finish().await.expect("finish process");

    let response = query_runtime_usage(
        &state,
        &RuntimeUsageApiRequest {
            node_id: "node-a".to_string(),
            query: RuntimeUsageQuery {
                start: RuntimeUsageReplayStart::Sequence(1),
                save_checkpoint: None,
                limit: 0,
                external_account_ref: Some("acct-a".to_string()),
                workload: Some("tenant-a/media/ingest".to_string()),
                module: Some("module-a".to_string()),
                window_start_ms: None,
                window_end_ms: None,
            },
        },
    )
    .await
    .expect("query runtime usage");

    assert!(response.records.is_empty());
    assert!(response.high_watermark.is_some());
    assert_eq!(response.next_sequence, None);
}

#[tokio::test]
async fn runtime_usage_query_can_save_and_resume_named_checkpoint() {
    let state = sample_state("node-a");
    let collector = state
        .kernel
        .get::<crate::usage::RuntimeUsageCollector>()
        .cloned()
        .expect("collector available");
    let first = collector
        .register_process(
            "tenant-a/media/ingest",
            "process-a",
            ProcessUsageAttribution {
                instance_id: Some("tenant-a/media/ingest/0".to_string()),
                external_account_ref: Some("acct-a".to_string()),
                module_id: "module-a".to_string(),
            },
        )
        .await
        .expect("register first process");
    sleep(Duration::from_millis(2)).await;
    first.finish().await.expect("finish first process");

    let second = collector
        .register_process(
            "tenant-a/media/ingest",
            "process-b",
            ProcessUsageAttribution {
                instance_id: Some("tenant-a/media/ingest/1".to_string()),
                external_account_ref: Some("acct-a".to_string()),
                module_id: "module-a".to_string(),
            },
        )
        .await
        .expect("register second process");
    sleep(Duration::from_millis(2)).await;
    second.finish().await.expect("finish second process");

    let response = query_runtime_usage(
        &state,
        &RuntimeUsageApiRequest {
            node_id: "node-a".to_string(),
            query: RuntimeUsageQuery {
                start: RuntimeUsageReplayStart::Earliest,
                save_checkpoint: Some("usage-export".to_string()),
                limit: 1,
                external_account_ref: Some("acct-a".to_string()),
                workload: Some("tenant-a/media/ingest".to_string()),
                module: Some("module-a".to_string()),
                window_start_ms: None,
                window_end_ms: None,
            },
        },
    )
    .await
    .expect("query runtime usage");

    assert_eq!(response.records.len(), 1);
    assert_eq!(response.records[0].sample.process_id, "process-a");
    assert_eq!(
        collector.checkpoint_sequence("usage-export").await,
        response.next_sequence
    );

    let resumed = query_runtime_usage(
        &state,
        &RuntimeUsageApiRequest {
            node_id: "node-a".to_string(),
            query: RuntimeUsageQuery {
                start: RuntimeUsageReplayStart::Checkpoint("usage-export".to_string()),
                save_checkpoint: Some("usage-export".to_string()),
                limit: 10,
                external_account_ref: Some("acct-a".to_string()),
                workload: Some("tenant-a/media/ingest".to_string()),
                module: Some("module-a".to_string()),
                window_start_ms: None,
                window_end_ms: None,
            },
        },
    )
    .await
    .expect("resume named checkpoint");

    assert_eq!(resumed.records.len(), 1);
    assert_eq!(resumed.records[0].sample.process_id, "process-b");
    assert_eq!(
        collector.checkpoint_sequence("usage-export").await,
        resumed.next_sequence
    );
}

#[tokio::test(flavor = "current_thread")]
async fn list_instances_reports_visible_runtime_memory_pressure() {
    let state = sample_state("node-a");
    let collector = state
        .kernel
        .get::<RuntimeUsageCollector>()
        .expect("runtime usage collector")
        .clone();
    let handle = collector
        .register_process(
            "tenant-a/media/ingest",
            "123",
            ProcessUsageAttribution {
                instance_id: Some("tenant-a/media/ingest/0".to_string()),
                external_account_ref: Some("acct-a".to_string()),
                module_id: "module-a".to_string(),
            },
        )
        .await
        .expect("register process");
    handle.set_memory_high_watermark_bytes(5 * 1024 * 1024);
    assert_eq!(
        collector
            .observed_load_for_processes(["123"])
            .await
            .memory_bytes,
        5 * 1024 * 1024
    );
    state
        .processes
        .lock()
        .await
        .insert("tenant-a/media/ingest/0".to_string(), 123);
    state.instance_workloads.lock().await.insert(
        "tenant-a/media/ingest/0".to_string(),
        "tenant-a/media/ingest".to_string(),
    );

    let policy = AccessPolicy::from_specs(&[
        "principal=machine:client.localhost;methods=node_manage;workloads=tenant-a/*".to_string(),
    ])
    .expect("policy");
    let request_context = policy.resolve(
        PrincipalRef::new(PrincipalKind::Machine, "client.localhost"),
        "fp".to_string(),
    );
    let frame = encode_request(
        Method::ListInstances,
        1,
        &ListRequest {
            node_id: "node-a".to_string(),
        },
    )
    .expect("encode list request");
    let envelope = decode_envelope(&frame).expect("decode envelope");

    let response = handle_stream_request(state, request_context, &frame, envelope)
        .await
        .expect("list instances");
    let list_envelope = decode_envelope(&response).expect("decode response envelope");
    let list: ListResponse = decode_payload(&list_envelope).expect("decode list response");

    assert_eq!(
        list.instances,
        BTreeMap::from([("tenant-a/media/ingest/0".to_string(), 123usize)])
    );
    assert_eq!(list.observed_memory_bytes, Some(5 * 1024 * 1024));
    assert_eq!(
        list.observed_workload_memory_bytes,
        BTreeMap::from([("tenant-a/media/ingest".to_string(), 5 * 1024 * 1024)])
    );

    handle.finish().await.expect("finish process");
}

#[tokio::test]
async fn runtime_usage_query_missing_checkpoint_fails_deterministically() {
    let state = sample_state("node-a");
    let err = query_runtime_usage(
        &state,
        &RuntimeUsageApiRequest {
            node_id: "node-a".to_string(),
            query: RuntimeUsageQuery {
                start: RuntimeUsageReplayStart::Checkpoint("missing".to_string()),
                save_checkpoint: None,
                limit: 10,
                external_account_ref: None,
                workload: None,
                module: None,
                window_start_ms: None,
                window_end_ms: None,
            },
        },
    )
    .await
    .expect_err("missing checkpoint should fail");

    assert!(err.chain().any(|cause| {
        cause
            .to_string()
            .contains("checkpoint `missing` does not exist")
    }));
}

#[test]
fn append_guest_log_bindings_adds_hidden_queue_ids() {
    let spec = append_guest_log_bindings(
        "path=demo.wasm;capabilities=time_read",
        ProcessLogBindings {
            stdout_queue_shared_id: Some(11),
            stderr_queue_shared_id: Some(22),
        },
    );

    assert_eq!(
        spec,
        "path=demo.wasm;capabilities=time_read;guest_log_stdout_queue_shared_id=11;guest_log_stderr_queue_shared_id=22"
    );
}

fn sample_node(name: &str, daemon_addr: &str, daemon_server_name: &str) -> NodeSpec {
    NodeSpec {
        name: name.to_string(),
        capacity_slots: 8,
        allocatable_cpu_millis: None,
        allocatable_memory_mib: None,
        reserve_cpu_utilisation_ppm: 800_000,
        reserve_memory_utilisation_ppm: 800_000,
        reserve_slots_utilisation_ppm: 800_000,
        observed_running_instances: None,
        observed_active_bridges: None,
        observed_memory_mib: None,
        observed_workloads: BTreeMap::new(),
        observed_workload_memory_mib: BTreeMap::new(),
        supported_isolation: vec![IsolationProfile::Standard],
        daemon_addr: daemon_addr.to_string(),
        daemon_server_name: daemon_server_name.to_string(),
        last_heartbeat_ms: 0,
    }
}

fn guest_log_resolution_engine(replicas: usize) -> ControlPlaneEngine {
    let mut state = ControlPlaneState::new_local_default();
    let package = parse_idl(
        r#"
            package media.camera.v1;
            schema Frame { payload: bytes; }
            event camera.frames(Frame) { replay: enabled; }
            "#,
    )
    .expect("parse package");
    state
        .registry
        .register_package(package)
        .expect("register package");
    state
        .upsert_deployment(DeploymentSpec {
            workload: WorkloadRef {
                tenant: "tenant-a".to_string(),
                namespace: "media".to_string(),
                name: "ingest".to_string(),
            },
            module: "ingest.wasm".to_string(),
            replicas: replicas as u32,
            contracts: vec![ContractRef {
                namespace: "media.camera".to_string(),
                kind: ContractKind::Event,
                name: "camera.frames".to_string(),
                version: "v1".to_string(),
            }],
            isolation: IsolationProfile::Standard,
            placement_mode: selium_control_plane_api::PlacementMode::ElasticPack,
            cpu_millis: 0,
            memory_mib: 0,
            ephemeral_storage_mib: 0,
            bandwidth_profile: selium_control_plane_api::BandwidthProfile::Standard,
            volume_mounts: Vec::new(),
            external_account_ref: None,
        })
        .expect("register deployment");
    ControlPlaneEngine::new(state)
}

fn engine_query(engine: &ControlPlaneEngine, query: Query) -> Result<DataValue> {
    Ok(engine
        .query(query)
        .map_err(|err| anyhow!(err.to_string()))?
        .result)
}

fn sample_state(node_id: &str) -> Rc<DaemonState> {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock")
        .as_nanos();
    let work_dir = std::env::temp_dir().join(format!("selium-daemon-test-{unique}"));
    fs::create_dir_all(work_dir.join("modules")).expect("create temp modules dir");
    let (kernel, _) = crate::kernel::build(&work_dir).expect("build kernel");
    let registry = Registry::new();
    let endpoint = Endpoint::client("127.0.0.1:0".parse().expect("client bind"))
        .expect("create client endpoint");
    Rc::new(DaemonState {
        node_id: node_id.to_string(),
        kernel,
        registry,
        work_dir,
        processes: Mutex::new(BTreeMap::new()),
        instance_workloads: Mutex::new(BTreeMap::new()),
        source_bindings: Mutex::new(BTreeMap::new()),
        target_bindings: Mutex::new(BTreeMap::new()),
        service_response_bindings: Mutex::new(BTreeMap::new()),
        active_bridges: Mutex::new(BTreeMap::new()),
        host_subscription_id: AtomicU64::new(1),
        control_plane: Arc::new(LocalControlPlaneClient {
            endpoint,
            addr: "127.0.0.1:1".parse().expect("dummy addr"),
            server_name: "dummy.local".to_string(),
            connection: Mutex::new(None),
            request_id: AtomicU64::new(1),
        }),
        control_plane_process_id: 0,
        tls_paths: ManagedEventTlsPaths {
            ca_cert: PathBuf::new(),
            client_cert: None,
            client_key: None,
        },
        access_policy: AccessPolicy::default(),
    })
}

fn sample_endpoint(workload: &str, endpoint: &str) -> PublicEndpointRef {
    sample_endpoint_kind(ContractKind::Event, workload, endpoint)
}

fn sample_endpoint_kind(kind: ContractKind, workload: &str, endpoint: &str) -> PublicEndpointRef {
    PublicEndpointRef {
        workload: WorkloadRef {
            tenant: "tenant-a".to_string(),
            namespace: "media".to_string(),
            name: workload.to_string(),
        },
        kind,
        name: endpoint.to_string(),
    }
}
