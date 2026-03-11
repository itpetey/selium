use std::collections::{BTreeMap, BTreeSet};

use crate::{
    ApiError, ContractKind, ContractRef, ControlPlaneState, DeploymentSpec, DiscoverableEndpoint,
    DiscoverableWorkload, DiscoveryState, GUEST_LOG_STDERR_ENDPOINT, GUEST_LOG_STDOUT_ENDPOINT,
    ExternalAccountRef, IsolationProfile, NodeSpec, PipelineEndpoint, PipelineSpec,
    PublicEndpointRef, WorkloadRef,
};

impl ControlPlaneState {
    pub fn new_local_default() -> Self {
        let mut nodes = BTreeMap::new();
        nodes.insert(
            "local-node".to_string(),
            NodeSpec {
                name: "local-node".to_string(),
                capacity_slots: 64,
                allocatable_cpu_millis: None,
                allocatable_memory_mib: None,
                supported_isolation: vec![
                    IsolationProfile::Standard,
                    IsolationProfile::Hardened,
                    IsolationProfile::Microvm,
                ],
                daemon_addr: "127.0.0.1:7100".to_string(),
                daemon_server_name: "localhost".to_string(),
                last_heartbeat_ms: 0,
            },
        );

        Self {
            nodes,
            ..Self::default()
        }
    }

    pub fn upsert_deployment(
        &mut self,
        deployment: DeploymentSpec,
    ) -> std::result::Result<(), ApiError> {
        deployment.workload.validate("deployment")?;
        if deployment.module.trim().is_empty() {
            return Err(ApiError::InvalidDeployment(
                "module must not be empty".to_string(),
            ));
        }
        if deployment.replicas == 0 {
            return Err(ApiError::InvalidDeployment(
                "replicas must be > 0".to_string(),
            ));
        }
        validate_optional_external_account_ref(
            &deployment.external_account_ref,
            "deployment external account reference",
            ApiError::InvalidDeployment,
        )?;

        self.deployments
            .insert(deployment.workload.key(), deployment);
        Ok(())
    }

    pub fn set_scale(
        &mut self,
        workload: &WorkloadRef,
        replicas: u32,
    ) -> std::result::Result<(), ApiError> {
        workload.validate("scale target")?;
        let key = workload.key();
        let deployment = self
            .deployments
            .get_mut(&key)
            .ok_or_else(|| ApiError::UnknownDeployment(key.clone()))?;
        deployment.replicas = replicas.max(1);
        Ok(())
    }

    pub fn upsert_pipeline(&mut self, pipeline: PipelineSpec) {
        self.pipelines.insert(pipeline.key(), pipeline);
    }

    pub fn upsert_node(&mut self, node: NodeSpec) -> std::result::Result<(), ApiError> {
        if node.name.trim().is_empty() {
            return Err(ApiError::InvalidNode(
                "node name must not be empty".to_string(),
            ));
        }
        if node.daemon_addr.trim().is_empty() {
            return Err(ApiError::InvalidNode(
                "daemon address must not be empty".to_string(),
            ));
        }

        self.nodes.insert(node.name.clone(), node);
        Ok(())
    }
}

pub fn ensure_pipeline_consistency(state: &ControlPlaneState) -> std::result::Result<(), ApiError> {
    for pipeline in state.pipelines.values() {
        if pipeline.name.trim().is_empty() {
            return Err(ApiError::InvalidPipeline(
                "pipeline name must not be empty".to_string(),
            ));
        }
        if pipeline.tenant.trim().is_empty() {
            return Err(ApiError::InvalidPipeline(
                "pipeline tenant must not be empty".to_string(),
            ));
        }
        if pipeline.namespace.trim().is_empty() {
            return Err(ApiError::InvalidPipeline(
                "pipeline namespace must not be empty".to_string(),
            ));
        }
        let mut expected_external_account_ref = validate_optional_external_account_ref(
            &pipeline.external_account_ref,
            "pipeline external account reference",
            ApiError::InvalidPipeline,
        )?;

        for edge in &pipeline.edges {
            validate_pipeline_endpoint(state, pipeline, &edge.from, "source")?;
            validate_pipeline_endpoint(state, pipeline, &edge.to, "target")?;

            let from_key = edge.from.endpoint.workload.key();
            let from_deployment = state
                .deployments
                .get(&from_key)
                .ok_or_else(|| ApiError::UnknownDeployment(from_key.clone()))?;
            ensure_pipeline_external_account_ref(
                pipeline,
                from_deployment,
                "source",
                &mut expected_external_account_ref,
            )?;

            let to_key = edge.to.endpoint.workload.key();
            let to_deployment = state
                .deployments
                .get(&to_key)
                .ok_or_else(|| ApiError::UnknownDeployment(to_key.clone()))?;
            ensure_pipeline_external_account_ref(
                pipeline,
                to_deployment,
                "target",
                &mut expected_external_account_ref,
            )?;
        }
    }

    Ok(())
}

pub fn build_discovery_state(
    state: &ControlPlaneState,
) -> std::result::Result<DiscoveryState, ApiError> {
    ensure_pipeline_consistency(state)?;

    let mut workloads = Vec::new();
    let mut endpoints = Vec::new();

    for deployment in state.deployments.values() {
        let mut workload_endpoints = deployment_public_endpoints(state, deployment)?;
        workload_endpoints.sort_by(|lhs, rhs| lhs.endpoint.cmp(&rhs.endpoint));

        workloads.push(DiscoverableWorkload {
            workload: deployment.workload.clone(),
            endpoints: workload_endpoints
                .iter()
                .map(|record| record.endpoint.clone())
                .collect(),
        });
        endpoints.extend(workload_endpoints);
    }

    workloads.sort_by(|lhs, rhs| lhs.workload.cmp(&rhs.workload));
    endpoints.sort_by(|lhs, rhs| lhs.endpoint.cmp(&rhs.endpoint));

    Ok(DiscoveryState {
        workloads,
        endpoints,
    })
}

pub fn collect_contracts_for_workload(
    state: &ControlPlaneState,
    workload: &WorkloadRef,
) -> BTreeSet<ContractRef> {
    let mut out = BTreeSet::new();
    for pipeline in state.pipelines.values() {
        for edge in &pipeline.edges {
            if edge.from.endpoint.workload == *workload {
                out.insert(edge.from.contract.clone());
            }
            if edge.to.endpoint.workload == *workload {
                out.insert(edge.to.contract.clone());
            }
        }
    }
    out
}

pub fn collect_contracts_for_app(state: &ControlPlaneState, app: &str) -> BTreeSet<ContractRef> {
    let mut out = BTreeSet::new();
    for deployment in state.deployments.values() {
        if deployment.workload.name == app {
            out.extend(collect_contracts_for_workload(state, &deployment.workload));
        }
    }
    out
}

fn validate_pipeline_endpoint(
    state: &ControlPlaneState,
    pipeline: &PipelineSpec,
    endpoint: &PipelineEndpoint,
    role: &str,
) -> std::result::Result<(), ApiError> {
    endpoint
        .endpoint
        .validate(&format!("{role} pipeline endpoint"))?;

    if endpoint.endpoint.workload.tenant != pipeline.tenant {
        return Err(ApiError::InvalidPipeline(format!(
            "{role} endpoint `{}` must stay within pipeline tenant `{}`",
            endpoint.endpoint, pipeline.tenant
        )));
    }

    if endpoint.endpoint.workload.namespace != pipeline.namespace {
        return Err(ApiError::InvalidPipeline(format!(
            "{role} endpoint `{}` must stay within pipeline namespace `{}`",
            endpoint.endpoint, pipeline.namespace
        )));
    }

    let resolved = state
        .registry
        .resolve(&endpoint.contract)
        .ok_or(ApiError::InvalidContract)?;
    if resolved.kind != ContractKind::Event {
        return Err(ApiError::InvalidPipeline(format!(
            "{role} endpoint `{}` must reference an event contract",
            endpoint.endpoint
        )));
    }
    if endpoint.endpoint.name != endpoint.contract.name {
        return Err(ApiError::InvalidPipeline(format!(
            "{role} endpoint `{}` must match contract-defined event `{}`",
            endpoint.endpoint, endpoint.contract.name
        )));
    }

    let deployment_key = endpoint.endpoint.workload.key();
    let deployment = state
        .deployments
        .get(&deployment_key)
        .ok_or_else(|| ApiError::UnknownDeployment(deployment_key.clone()))?;
    if !deployment.contracts.contains(&endpoint.contract) {
        return Err(ApiError::InvalidPipeline(format!(
            "{role} endpoint `{}` must be declared by workload `{}`",
            endpoint.endpoint, endpoint.endpoint.workload
        )));
    }

    Ok(())
}

fn validate_optional_external_account_ref(
    reference: &Option<ExternalAccountRef>,
    subject: &str,
    invalid: fn(String) -> ApiError,
) -> std::result::Result<Option<ExternalAccountRef>, ApiError> {
    if let Some(reference) = reference {
        reference.validate(subject, invalid)?;
    }

    Ok(reference.clone())
}

fn ensure_pipeline_external_account_ref(
    pipeline: &PipelineSpec,
    deployment: &DeploymentSpec,
    role: &str,
    expected: &mut Option<ExternalAccountRef>,
) -> std::result::Result<(), ApiError> {
    let Some(actual) = deployment.external_account_ref.as_ref() else {
        return Ok(());
    };

    if let Some(expected_ref) = expected.as_ref() {
        if actual != expected_ref {
            let baseline = pipeline.external_account_ref.as_ref().map_or_else(
                || format!("baseline workload attribution `{expected_ref}`"),
                |pipeline_ref| format!("pipeline attribution `{pipeline_ref}`"),
            );
            return Err(ApiError::InvalidPipeline(format!(
                "{role} workload `{}` external account reference `{actual}` must match {baseline}",
                deployment.workload
            )));
        }
    } else {
        *expected = Some(actual.clone());
    }

    Ok(())
}

fn deployment_public_endpoints(
    state: &ControlPlaneState,
    deployment: &DeploymentSpec,
) -> std::result::Result<Vec<DiscoverableEndpoint>, ApiError> {
    let mut endpoints = BTreeMap::new();

    for contract in &deployment.contracts {
        state
            .registry
            .resolve(contract)
            .ok_or(ApiError::InvalidContract)?;

        let endpoint = PublicEndpointRef {
            workload: deployment.workload.clone(),
            kind: contract.kind,
            name: contract.name.clone(),
        };
        endpoints.insert(
            endpoint.key(),
            DiscoverableEndpoint {
                endpoint,
                contract: Some(contract.clone()),
            },
        );
    }

    for name in [GUEST_LOG_STDERR_ENDPOINT, GUEST_LOG_STDOUT_ENDPOINT] {
        let endpoint = PublicEndpointRef {
            workload: deployment.workload.clone(),
            kind: ContractKind::Event,
            name: name.to_string(),
        };
        endpoints.insert(
            endpoint.key(),
            DiscoverableEndpoint {
                endpoint,
                contract: None,
            },
        );
    }

    Ok(endpoints.into_values().collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ContractRef, EventEndpointRef, PipelineEdge, parse_idl};

    fn event_contract() -> ContractRef {
        ContractRef {
            namespace: "media.pipeline".to_string(),
            kind: ContractKind::Event,
            name: "camera.frames".to_string(),
            version: "v1".to_string(),
        }
    }

    fn service_contract() -> ContractRef {
        ContractRef {
            namespace: "media.pipeline".to_string(),
            kind: ContractKind::Service,
            name: "camera.detect".to_string(),
            version: "v1".to_string(),
        }
    }

    fn stream_contract() -> ContractRef {
        ContractRef {
            namespace: "media.pipeline".to_string(),
            kind: ContractKind::Stream,
            name: "camera.raw".to_string(),
            version: "v1".to_string(),
        }
    }

    fn shared_name_contract(kind: ContractKind) -> ContractRef {
        ContractRef {
            namespace: "media.pipeline".to_string(),
            kind,
            name: "camera.shared".to_string(),
            version: "v1".to_string(),
        }
    }

    fn sample_state() -> ControlPlaneState {
        let mut state = ControlPlaneState::new_local_default();
        state
            .registry
            .register_package(
                parse_idl(
                    "package media.pipeline.v1;\n\
                     schema Frame { camera_id: string; }\n\
                     event camera.frames(Frame) { replay: enabled; }",
                )
                .expect("parse"),
            )
            .expect("register");
        state
    }

    fn mixed_state() -> ControlPlaneState {
        let mut state = ControlPlaneState::new_local_default();
        state
            .registry
            .register_package(
                parse_idl(
                    "package media.pipeline.v1;\n\
                     schema Frame { camera_id: string; }\n\
                     event camera.frames(Frame) { replay: enabled; }\n\
                     service camera.detect(Frame) -> Frame;\n\
                     stream camera.raw(Frame);",
                )
                .expect("parse"),
            )
            .expect("register");
        state
    }

    #[test]
    fn discovery_state_keeps_tenant_qualified_namespace_collisions_separate() {
        let mut state = sample_state();
        let contract = event_contract();

        for (tenant, namespace) in [("tenant-a", "media"), ("tenant-b", "media")] {
            state
                .upsert_deployment(DeploymentSpec {
                    workload: WorkloadRef {
                        tenant: tenant.to_string(),
                        namespace: namespace.to_string(),
                        name: "ingest".to_string(),
                    },
                    module: format!("{tenant}-ingest.wasm"),
                    replicas: 1,
                    contracts: vec![contract.clone()],
                    isolation: IsolationProfile::Standard,
                    cpu_millis: 0,
                    memory_mib: 0,
                    ephemeral_storage_mib: 0,
                    bandwidth_profile: crate::BandwidthProfile::Standard,
                    volume_mounts: Vec::new(),
                    external_account_ref: None,
                })
                .expect("deployment");
        }

        let discovery = build_discovery_state(&state).expect("discovery");
        let workload_keys = discovery
            .workloads
            .iter()
            .map(|record| record.workload.key())
            .collect::<Vec<_>>();
        let endpoint_keys = discovery
            .endpoints
            .iter()
            .map(|record| record.endpoint.key())
            .collect::<Vec<_>>();

        assert_eq!(workload_keys.len(), 2);
        assert!(workload_keys.contains(&"tenant-a/media/ingest".to_string()));
        assert!(workload_keys.contains(&"tenant-b/media/ingest".to_string()));
        assert!(endpoint_keys.contains(&"tenant-a/media/ingest#event:camera.frames".to_string()));
        assert!(endpoint_keys.contains(&"tenant-b/media/ingest#event:camera.frames".to_string()));
    }

    #[test]
    fn discovery_state_includes_event_service_and_stream_endpoints() {
        let mut state = mixed_state();
        let workload = WorkloadRef {
            tenant: "tenant-a".to_string(),
            namespace: "media".to_string(),
            name: "ingest".to_string(),
        };

        state
            .upsert_deployment(DeploymentSpec {
                workload: workload.clone(),
                module: "ingest.wasm".to_string(),
                replicas: 1,
                contracts: vec![event_contract(), service_contract(), stream_contract()],
                isolation: IsolationProfile::Standard,
                cpu_millis: 0,
                memory_mib: 0,
                ephemeral_storage_mib: 0,
                bandwidth_profile: crate::BandwidthProfile::Standard,
                volume_mounts: Vec::new(),
                external_account_ref: None,
            })
            .expect("deployment");

        let discovery = build_discovery_state(&state).expect("discovery");
        let workload_endpoint_keys = discovery.workloads[0]
            .endpoints
            .iter()
            .map(|endpoint| endpoint.key())
            .collect::<Vec<_>>();
        let endpoint_keys = discovery
            .endpoints
            .iter()
            .map(|record| record.endpoint.key())
            .collect::<Vec<_>>();

        assert_eq!(discovery.workloads.len(), 1);
        assert_eq!(discovery.workloads[0].workload, workload);
        assert_eq!(
            workload_endpoint_keys,
            vec![
                "tenant-a/media/ingest#event:camera.frames".to_string(),
                "tenant-a/media/ingest#event:stderr".to_string(),
                "tenant-a/media/ingest#event:stdout".to_string(),
                "tenant-a/media/ingest#service:camera.detect".to_string(),
                "tenant-a/media/ingest#stream:camera.raw".to_string(),
            ]
        );
        assert_eq!(endpoint_keys, workload_endpoint_keys);
    }

    #[test]
    fn discovery_state_includes_synthetic_guest_log_endpoints() {
        let mut state = sample_state();
        let workload = WorkloadRef {
            tenant: "tenant-a".to_string(),
            namespace: "media".to_string(),
            name: "ingest".to_string(),
        };

        state
            .upsert_deployment(DeploymentSpec {
                workload: workload.clone(),
                module: "ingest.wasm".to_string(),
                replicas: 1,
                contracts: vec![event_contract()],
                isolation: IsolationProfile::Standard,
                cpu_millis: 0,
                memory_mib: 0,
                ephemeral_storage_mib: 0,
                bandwidth_profile: crate::BandwidthProfile::Standard,
                volume_mounts: Vec::new(),
                external_account_ref: None,
            })
            .expect("deployment");

        let discovery = build_discovery_state(&state).expect("discovery");
        let endpoints = discovery
            .endpoints
            .into_iter()
            .filter(|record| record.endpoint.workload == workload)
            .collect::<Vec<_>>();

        assert_eq!(
            endpoints,
            vec![
                DiscoverableEndpoint {
                    endpoint: PublicEndpointRef {
                        workload: workload.clone(),
                        kind: ContractKind::Event,
                        name: "camera.frames".to_string(),
                    },
                    contract: Some(event_contract()),
                },
                DiscoverableEndpoint {
                    endpoint: PublicEndpointRef {
                        workload: workload.clone(),
                        kind: ContractKind::Event,
                        name: GUEST_LOG_STDERR_ENDPOINT.to_string(),
                    },
                    contract: None,
                },
                DiscoverableEndpoint {
                    endpoint: PublicEndpointRef {
                        workload,
                        kind: ContractKind::Event,
                        name: GUEST_LOG_STDOUT_ENDPOINT.to_string(),
                    },
                    contract: None,
                },
            ]
        );
    }

    #[test]
    fn public_endpoint_identity_includes_kind_for_same_name_collisions() {
        let workload = WorkloadRef {
            tenant: "tenant-a".to_string(),
            namespace: "media".to_string(),
            name: "ingest".to_string(),
        };
        let event = PublicEndpointRef {
            workload: workload.clone(),
            kind: ContractKind::Event,
            name: "camera.shared".to_string(),
        };
        let service = PublicEndpointRef {
            workload: workload.clone(),
            kind: ContractKind::Service,
            name: "camera.shared".to_string(),
        };
        let stream = PublicEndpointRef {
            workload,
            kind: ContractKind::Stream,
            name: "camera.shared".to_string(),
        };

        assert_eq!(event.key(), "tenant-a/media/ingest#event:camera.shared");
        assert_eq!(service.key(), "tenant-a/media/ingest#service:camera.shared");
        assert_eq!(stream.key(), "tenant-a/media/ingest#stream:camera.shared");
        assert_ne!(event, service);
        assert_ne!(service, stream);
        assert_ne!(event, stream);
    }

    #[test]
    fn discovery_state_preserves_same_name_cross_kind_contracts() {
        let mut state = ControlPlaneState::new_local_default();
        state
            .registry
            .register_package(
                parse_idl(
                    "package media.pipeline.v1;\n\
                     schema Frame { camera_id: string; }\n\
                     event camera.shared(Frame) { replay: enabled; }\n\
                     service camera.shared(Frame) -> Frame;\n\
                     stream camera.shared(Frame);",
                )
                .expect("parse"),
            )
            .expect("register");

        state
            .upsert_deployment(DeploymentSpec {
                workload: WorkloadRef {
                    tenant: "tenant-a".to_string(),
                    namespace: "media".to_string(),
                    name: "router".to_string(),
                },
                module: "router.wasm".to_string(),
                replicas: 1,
                contracts: vec![
                    shared_name_contract(ContractKind::Event),
                    shared_name_contract(ContractKind::Service),
                    shared_name_contract(ContractKind::Stream),
                ],
                isolation: IsolationProfile::Standard,
                cpu_millis: 0,
                memory_mib: 0,
                ephemeral_storage_mib: 0,
                bandwidth_profile: crate::BandwidthProfile::Standard,
                volume_mounts: Vec::new(),
                external_account_ref: None,
            })
            .expect("deployment");

        let discovery = build_discovery_state(&state).expect("discovery");

        assert_eq!(
            discovery
                .endpoints
                .iter()
                .map(|record| record.endpoint.key())
                .collect::<Vec<_>>(),
            vec![
                "tenant-a/media/router#event:camera.shared".to_string(),
                "tenant-a/media/router#event:stderr".to_string(),
                "tenant-a/media/router#event:stdout".to_string(),
                "tenant-a/media/router#service:camera.shared".to_string(),
                "tenant-a/media/router#stream:camera.shared".to_string(),
            ]
        );
        assert_eq!(
            discovery
                .endpoints
                .iter()
                .map(|record| record.contract.as_ref().map(|contract| contract.kind))
                .collect::<Vec<_>>(),
            vec![
                Some(ContractKind::Event),
                None,
                None,
                Some(ContractKind::Service),
                Some(ContractKind::Stream),
            ]
        );
    }

    #[test]
    fn pipeline_endpoint_must_be_declared_by_workload_contracts() {
        let mut state = sample_state();
        let contract = event_contract();
        let workload = WorkloadRef {
            tenant: "tenant-a".to_string(),
            namespace: "media".to_string(),
            name: "ingest".to_string(),
        };
        state
            .upsert_deployment(DeploymentSpec {
                workload: workload.clone(),
                module: "ingest.wasm".to_string(),
                replicas: 1,
                contracts: Vec::new(),
                isolation: IsolationProfile::Standard,
                cpu_millis: 0,
                memory_mib: 0,
                ephemeral_storage_mib: 0,
                bandwidth_profile: crate::BandwidthProfile::Standard,
                volume_mounts: Vec::new(),
                external_account_ref: None,
            })
            .expect("deployment");

        state.upsert_pipeline(PipelineSpec {
            name: "pipeline".to_string(),
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
            external_account_ref: None,
        });

        let err = ensure_pipeline_consistency(&state).expect_err("undeclared contract rejected");
        assert!(err.to_string().contains("must be declared by workload"));
    }

    #[test]
    fn deployment_rejects_blank_external_account_reference() {
        let mut state = sample_state();
        let err = state
            .upsert_deployment(DeploymentSpec {
                workload: WorkloadRef {
                    tenant: "tenant-a".to_string(),
                    namespace: "media".to_string(),
                    name: "ingest".to_string(),
                },
                module: "ingest.wasm".to_string(),
                replicas: 1,
                contracts: Vec::new(),
                isolation: IsolationProfile::Standard,
                cpu_millis: 0,
                memory_mib: 0,
                ephemeral_storage_mib: 0,
                bandwidth_profile: crate::BandwidthProfile::Standard,
                volume_mounts: Vec::new(),
                external_account_ref: Some(ExternalAccountRef {
                    key: "   ".to_string(),
                }),
            })
            .expect_err("blank external account reference rejected");

        assert!(err.to_string().contains("must not be empty"));
    }

    #[test]
    fn pipeline_consistency_rejects_mismatched_external_account_references() {
        let mut state = sample_state();
        let contract = event_contract();
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

        for (workload, account) in [(ingest.clone(), "acct-a"), (detector.clone(), "acct-b")] {
            state
                .upsert_deployment(DeploymentSpec {
                    workload,
                    module: "module.wasm".to_string(),
                    replicas: 1,
                    contracts: vec![contract.clone()],
                    isolation: IsolationProfile::Standard,
                    cpu_millis: 0,
                    memory_mib: 0,
                    ephemeral_storage_mib: 0,
                    bandwidth_profile: crate::BandwidthProfile::Standard,
                    volume_mounts: Vec::new(),
                    external_account_ref: Some(ExternalAccountRef {
                        key: account.to_string(),
                    }),
                })
                .expect("deployment");
        }

        state.upsert_pipeline(PipelineSpec {
            name: "pipeline".to_string(),
            tenant: "tenant-a".to_string(),
            namespace: "media".to_string(),
            edges: vec![PipelineEdge {
                from: PipelineEndpoint {
                    endpoint: EventEndpointRef {
                        workload: ingest,
                        name: contract.name.clone(),
                    },
                    contract: contract.clone(),
                },
                to: PipelineEndpoint {
                    endpoint: EventEndpointRef {
                        workload: detector,
                        name: contract.name.clone(),
                    },
                    contract,
                },
            }],
            external_account_ref: None,
        });

        let err = ensure_pipeline_consistency(&state).expect_err("mismatch rejected");
        assert!(err.to_string().contains("external account reference"));
    }
}
