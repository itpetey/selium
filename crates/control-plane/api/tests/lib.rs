use selium_control_plane_api::*;

const SAMPLE: &str = r#"
package media.pipeline.v1;

schema Frame {
  camera_id: string;
  ts_ms: u64;
  jpeg: bytes;
}

schema UploadHead {
  camera_id: string;
  ts_ms: u64;
}

event camera.frames(Frame) {
  partitions: 12;
  retention: "24h";
  delivery: at_least_once;
  replay: enabled;
}

service detect(Frame) -> Frame;
service mirror(Frame) -> Frame {
  protocol: quic;
  method: mirror;
  request-body: buffered<Frame>;
  response-body: buffered<Frame>;
}
service upload(UploadHead) -> Frame {
  protocol: http;
  method: POST;
  path: "/upload/{camera_id}";
  request-header: ts_ms = "x-ts-ms";
  request-body: stream<bytes>;
  response-body: buffered<Frame>;
}
stream camera.raw(Frame);
"#;

#[test]
fn parses_package_and_events() {
    let package = parse_idl(SAMPLE).expect("parse");
    assert_eq!(package.namespace, "media.pipeline");
    assert_eq!(package.version, "v1");
    assert_eq!(package.schemas.len(), 2);
    assert_eq!(package.events.len(), 1);
    assert_eq!(package.services.len(), 3);
    assert_eq!(package.services[0].name, "detect");
    assert_eq!(package.streams.len(), 1);
    assert_eq!(package.events[0].name, "camera.frames");
    assert!(package.events[0].replay_enabled);
}

#[test]
fn parses_service_block_with_streaming_body() {
    let package = parse_idl(SAMPLE).expect("parse");
    assert_eq!(package.services.len(), 3);
    let upload = package
        .services
        .iter()
        .find(|service| service.name == "upload")
        .expect("upload service");
    assert_eq!(upload.protocol.as_deref(), Some("http"));
    assert_eq!(upload.method.as_deref(), Some("POST"));
    assert_eq!(upload.path.as_deref(), Some("/upload/{camera_id}"));
    assert_eq!(
        upload.request_headers,
        vec![ServiceFieldBinding {
            field: "ts_ms".to_string(),
            target: "x-ts-ms".to_string(),
        }]
    );
    assert_eq!(upload.request_body.mode, ServiceBodyMode::Stream);
    assert_eq!(upload.request_body.schema.as_deref(), Some("bytes"));
    assert_eq!(upload.response_body.mode, ServiceBodyMode::Buffered);
    assert_eq!(upload.response_body.schema.as_deref(), Some("Frame"));
}

#[test]
fn preserves_double_slash_inside_string_literals() {
    let package = parse_idl(
        r#"
package media.pipeline.v1;

schema UploadHead {
  camera_id: string
  ts_ms: u64
}

service upload(UploadHead) -> UploadHead {
  protocol: http
  path: "/upload//raw/{camera_id}"
  request-header: ts_ms = "x-ts-ms"
}
"#,
    )
    .expect("parse");

    let upload = package
        .services
        .iter()
        .find(|service| service.name == "upload")
        .expect("upload service");
    assert_eq!(upload.path.as_deref(), Some("/upload//raw/{camera_id}"));
}

#[test]
fn registry_validates_backward_compatibility() {
    let mut registry = ContractRegistry::default();
    let v1 = parse_idl(SAMPLE).expect("parse v1");
    registry.register_package(v1).expect("register v1");

    let incompatible = parse_idl(
        "package media.pipeline.v2;\n\
         schema Frame { camera_id: string; }\n\
         event camera.frames(Frame) { replay: enabled; }",
    )
    .expect("parse v2");

    let err = registry
        .register_package(incompatible)
        .expect_err("must fail");
    assert!(err.to_string().contains("removed field"));
}

#[test]
fn rust_bindings_generation_is_non_empty() {
    let package = parse_idl(SAMPLE).expect("parse");
    let generated = generate_rust_bindings(&package);
    assert!(generated.contains("pub struct Frame"));
    assert!(generated.contains("EVENT_CAMERA_FRAMES"));
    assert!(generated.contains("pub const SERVICE_UPLOAD"));
    assert!(generated.contains("ServiceBodyMode::Stream"));
    assert!(generated.contains("pub const STREAM_CAMERA_RAW"));
    assert!(generated.contains("pub mod upload"));
    assert!(generated.contains("pub mod mirror"));
    assert!(generated.contains("pub mod camera_raw"));
    assert!(generated.contains("SELIUM_CONTRACT_CODEC_MAJOR_VERSION"));
    assert!(generated.contains("impl selium_abi::CanonicalSerialize for Frame"));
    assert!(generated.contains("encode_canonical"));
}

#[test]
fn shared_conformance_fixture_suite_passes_reference_codec() {
    let report = run_fixture_suite(&ReferenceCodec, &canonical_contract_fixture_suite());
    report
        .into_result()
        .expect("shared conformance suite should pass reference codec");
}

#[test]
fn contract_ref_parser_rejects_invalid_shape() {
    assert!(parse_contract_ref("bad").is_err());
    let contract = parse_contract_ref("media.pipeline/camera.frames@v1").expect("parse");
    assert_eq!(contract.namespace, "media.pipeline");
    assert_eq!(contract.kind, ContractKind::Event);

    let service = parse_contract_ref("media.pipeline/service:camera.frames@v1").expect("parse");
    assert_eq!(service.kind, ContractKind::Service);
}

#[test]
fn pipeline_consistency_checks_registry_and_deployments() {
    let mut state = ControlPlaneState::new_local_default();
    let package = parse_idl(SAMPLE).expect("parse sample");
    state.registry.register_package(package).expect("register");

    state
        .upsert_deployment(DeploymentSpec {
            workload: WorkloadRef {
                tenant: "tenant-a".to_string(),
                namespace: "media".to_string(),
                name: "ingest".to_string(),
            },
            module: "ingest.wasm".to_string(),
            replicas: 1,
            contracts: vec![ContractRef {
                namespace: "media.pipeline".to_string(),
                kind: ContractKind::Event,
                name: "camera.frames".to_string(),
                version: "v1".to_string(),
            }],
            isolation: IsolationProfile::Standard,
        })
        .expect("deployment");
    state
        .upsert_deployment(DeploymentSpec {
            workload: WorkloadRef {
                tenant: "tenant-a".to_string(),
                namespace: "media".to_string(),
                name: "detector".to_string(),
            },
            module: "detector.wasm".to_string(),
            replicas: 1,
            contracts: vec![ContractRef {
                namespace: "media.pipeline".to_string(),
                kind: ContractKind::Event,
                name: "camera.frames".to_string(),
                version: "v1".to_string(),
            }],
            isolation: IsolationProfile::Standard,
        })
        .expect("deployment");

    state.upsert_pipeline(PipelineSpec {
        name: "p".to_string(),
        tenant: "tenant-a".to_string(),
        namespace: "media".to_string(),
        edges: vec![PipelineEdge {
            from: PipelineEndpoint {
                endpoint: EventEndpointRef {
                    workload: WorkloadRef {
                        tenant: "tenant-a".to_string(),
                        namespace: "media".to_string(),
                        name: "ingest".to_string(),
                    },
                    name: "camera.frames".to_string(),
                },
                contract: ContractRef {
                    namespace: "media.pipeline".to_string(),
                    kind: ContractKind::Event,
                    name: "camera.frames".to_string(),
                    version: "v1".to_string(),
                },
            },
            to: PipelineEndpoint {
                endpoint: EventEndpointRef {
                    workload: WorkloadRef {
                        tenant: "tenant-a".to_string(),
                        namespace: "media".to_string(),
                        name: "detector".to_string(),
                    },
                    name: "camera.frames".to_string(),
                },
                contract: ContractRef {
                    namespace: "media.pipeline".to_string(),
                    kind: ContractKind::Event,
                    name: "camera.frames".to_string(),
                    version: "v1".to_string(),
                },
            },
        }],
    });

    ensure_pipeline_consistency(&state).expect("consistent");
}

#[test]
fn pipeline_consistency_rejects_cross_tenant_endpoint() {
    let mut state = ControlPlaneState::new_local_default();
    let package = parse_idl(SAMPLE).expect("parse sample");
    state.registry.register_package(package).expect("register");

    state
        .upsert_deployment(DeploymentSpec {
            workload: WorkloadRef {
                tenant: "tenant-a".to_string(),
                namespace: "media".to_string(),
                name: "ingest".to_string(),
            },
            module: "ingest.wasm".to_string(),
            replicas: 1,
            contracts: vec![ContractRef {
                namespace: "media.pipeline".to_string(),
                kind: ContractKind::Event,
                name: "camera.frames".to_string(),
                version: "v1".to_string(),
            }],
            isolation: IsolationProfile::Standard,
        })
        .expect("deployment");
    state
        .upsert_deployment(DeploymentSpec {
            workload: WorkloadRef {
                tenant: "tenant-b".to_string(),
                namespace: "media".to_string(),
                name: "detector".to_string(),
            },
            module: "detector.wasm".to_string(),
            replicas: 1,
            contracts: vec![ContractRef {
                namespace: "media.pipeline".to_string(),
                kind: ContractKind::Event,
                name: "camera.frames".to_string(),
                version: "v1".to_string(),
            }],
            isolation: IsolationProfile::Standard,
        })
        .expect("deployment");

    state.upsert_pipeline(PipelineSpec {
        name: "p".to_string(),
        tenant: "tenant-a".to_string(),
        namespace: "media".to_string(),
        edges: vec![PipelineEdge {
            from: PipelineEndpoint {
                endpoint: EventEndpointRef {
                    workload: WorkloadRef {
                        tenant: "tenant-a".to_string(),
                        namespace: "media".to_string(),
                        name: "ingest".to_string(),
                    },
                    name: "camera.frames".to_string(),
                },
                contract: ContractRef {
                    namespace: "media.pipeline".to_string(),
                    kind: ContractKind::Event,
                    name: "camera.frames".to_string(),
                    version: "v1".to_string(),
                },
            },
            to: PipelineEndpoint {
                endpoint: EventEndpointRef {
                    workload: WorkloadRef {
                        tenant: "tenant-b".to_string(),
                        namespace: "media".to_string(),
                        name: "detector".to_string(),
                    },
                    name: "camera.frames".to_string(),
                },
                contract: ContractRef {
                    namespace: "media.pipeline".to_string(),
                    kind: ContractKind::Event,
                    name: "camera.frames".to_string(),
                    version: "v1".to_string(),
                },
            },
        }],
    });

    let err = ensure_pipeline_consistency(&state).expect_err("must fail");
    assert!(err.to_string().contains("must stay within pipeline tenant"));
}
