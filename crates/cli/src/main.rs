mod config;
mod daemon_client;
mod output;

use std::{
    collections::BTreeMap,
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, anyhow};
use selium_abi::{
    DataValue, RuntimeUsageQuery, RuntimeUsageRecord, RuntimeUsageReplayStart, data_value_to_json,
    decode_rkyv, encode_rkyv,
};
use selium_control_plane_agent::{
    AgentState, ReconcileAction, apply, build_module_spec as build_runtime_module_spec,
    default_capabilities as default_runtime_capabilities,
    deployment_module_spec as build_deployment_module_spec, endpoint_bridge_semantics, reconcile,
};
use selium_control_plane_api::{
    ApiError, BandwidthProfile, ContractKind, ContractRef, ControlPlaneState, DeploymentSpec,
    DiscoverableEndpoint, DiscoverableWorkload, DiscoveryCapabilityScope, DiscoveryOperation,
    DiscoveryPattern, DiscoveryRequest, DiscoveryResolution, DiscoveryState, DiscoveryTarget,
    EventEndpointRef, ExternalAccountRef, OperationalProcessRecord, OperationalProcessSelector,
    PipelineEdge, PipelineEndpoint, PipelineSpec, PublicEndpointRef, ResolvedEndpoint,
    ResolvedWorkload, WorkloadRef, build_discovery_state, collect_contracts_for_workload,
    ensure_pipeline_consistency, generate_rust_bindings, parse_contract_ref, parse_idl,
};
use selium_control_plane_core::{AttributedInfrastructureFilter, Mutation, Query};
use selium_control_plane_protocol::{
    Empty, ListResponse, ManagedEndpointBinding, ManagedEndpointBindingType, ManagedEndpointRole,
    Method, MetricsApiResponse, MutateApiRequest, MutateApiResponse, QueryApiRequest,
    QueryApiResponse, ReplayApiRequest, ReplayApiResponse, RuntimeUsageApiRequest,
    RuntimeUsageApiResponse, StartRequest, StartResponse, StatusApiResponse, StopRequest,
    StopResponse, SubscribeGuestLogsRequest,
};
use selium_control_plane_scheduler::build_plan;
use tokio::{signal, time::sleep};

use crate::{
    config::{
        AdaptorArg, AgentArgs, AttachArgs, Command, ConnectArgs, DaemonConnectionArgs, DeployArgs,
        DiscoverArgs, IdlArgs, IdlCommand, IdlCompileArgs, IdlPublishArgs, InventoryArgs,
        IsolationArg, ListArgs, NodesArgs, ObserveArgs, ReplayArgs, ScaleArgs, StartArgs, StopArgs,
        UsageArgs, load_cli,
    },
    daemon_client::{DaemonQuicClient, parse_daemon_addr},
    output::{
        format_contract, observe_value, print_instance_map, render_guest_log_payload,
        replay_response_value, runtime_usage_response_json,
    },
};

const GUEST_LOG_STREAMS: [&str; 2] = ["stdout", "stderr"];

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("error: {err:#}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let cli = load_cli()?;

    match cli.command {
        Command::Idl(IdlArgs {
            command: IdlCommand::Compile(args),
        }) => cmd_idl_compile(args),
        command => {
            let daemon = Arc::new(DaemonQuicClient::from_args(&cli.daemon)?);
            match command {
                Command::Deploy(args) => cmd_deploy(daemon, args).await,
                Command::Connect(args) => cmd_connect(daemon, args).await,
                Command::Scale(args) => cmd_scale(daemon, args).await,
                Command::Observe(args) => cmd_observe(daemon, args).await,
                Command::Replay(args) => cmd_replay(daemon, args).await,
                Command::Discover(args) => cmd_discover(daemon, args).await,
                Command::Inventory(args) => cmd_inventory(daemon, args).await,
                Command::Usage(args) => cmd_usage(daemon, args).await,
                Command::Attach(args) => cmd_attach(daemon, args).await,
                Command::Nodes(args) => cmd_nodes(daemon, args).await,
                Command::Start(args) => cmd_start(daemon, args).await,
                Command::Stop(args) => cmd_stop(daemon, args).await,
                Command::List(args) => cmd_list(daemon, &cli.daemon, args).await,
                Command::Agent(args) => cmd_agent(daemon, &cli.daemon, args).await,
                Command::Idl(IdlArgs {
                    command: IdlCommand::Publish(args),
                }) => cmd_idl_publish(daemon, args).await,
                Command::Idl(IdlArgs {
                    command: IdlCommand::Compile(_),
                }) => unreachable!(),
            }
        }
    }
}

async fn cmd_deploy(daemon: Arc<DaemonQuicClient>, args: DeployArgs) -> Result<()> {
    let workload = workload_ref(args.tenant, args.namespace, args.workload);
    let contracts = args
        .contracts
        .iter()
        .map(|raw| parse_contract_ref(raw))
        .collect::<std::result::Result<Vec<_>, _>>()?;

    cp_mutate(
        &daemon,
        Mutation::UpsertDeployment {
            spec: DeploymentSpec {
                workload: workload.clone(),
                module: args.module,
                replicas: args.replicas,
                contracts,
                isolation: args.isolation.into(),
                placement_mode: args.placement_mode.into(),
                cpu_millis: args.cpu_millis,
                memory_mib: args.memory_mib,
                ephemeral_storage_mib: args.ephemeral_storage_mib,
                bandwidth_profile: BandwidthProfile::from(args.bandwidth_profile),
                volume_mounts: Vec::new(),
                external_account_ref: args
                    .external_account_ref
                    .map(|key| ExternalAccountRef { key }),
            },
        },
    )
    .await?;

    println!("deployment upserted {}", workload);
    Ok(())
}

async fn cmd_connect(daemon: Arc<DaemonQuicClient>, args: ConnectArgs) -> Result<()> {
    let mut state = cp_query_state(&daemon, false).await?;
    let contract = parse_contract_ref(&args.contract)?;
    let pipeline_key = qualified_pipeline_key(&args.tenant, &args.namespace, &args.pipeline);
    let from_workload = workload_ref(
        args.tenant.clone(),
        args.namespace.clone(),
        args.from_workload,
    );
    let to_workload = workload_ref(
        args.tenant.clone(),
        args.namespace.clone(),
        args.to_workload,
    );

    let edge = PipelineEdge {
        from: PipelineEndpoint {
            endpoint: EventEndpointRef {
                workload: from_workload,
                name: args.endpoint.clone(),
            },
            contract: contract.clone(),
        },
        to: PipelineEndpoint {
            endpoint: EventEndpointRef {
                workload: to_workload,
                name: args.endpoint.clone(),
            },
            contract,
        },
    };

    let pipeline_spec = {
        let pipeline = state.pipelines.entry(pipeline_key).or_insert(PipelineSpec {
            name: args.pipeline,
            tenant: args.tenant,
            namespace: args.namespace,
            edges: Vec::new(),
            external_account_ref: None,
        });
        pipeline.edges.push(edge);
        pipeline.clone()
    };

    ensure_pipeline_consistency(&state)
        .with_context(|| "pipeline references unknown workload or event contract")?;

    cp_mutate(
        &daemon,
        Mutation::UpsertPipeline {
            spec: pipeline_spec,
        },
    )
    .await?;

    println!("pipeline event edge added");
    Ok(())
}

async fn cmd_scale(daemon: Arc<DaemonQuicClient>, args: ScaleArgs) -> Result<()> {
    let workload = workload_ref(args.tenant, args.namespace, args.workload);
    cp_mutate(
        &daemon,
        Mutation::SetScale {
            workload: workload.clone(),
            replicas: args.replicas,
        },
    )
    .await?;
    println!("scaled {} to {} replicas", workload, args.replicas.max(1));
    Ok(())
}

async fn cmd_observe(daemon: Arc<DaemonQuicClient>, args: ObserveArgs) -> Result<()> {
    let snapshot = cp_query_value(&daemon, Query::ControlPlaneSummary, false).await?;
    let status: StatusApiResponse = daemon.request(Method::ControlStatus, &Empty {}).await?;
    let metrics: MetricsApiResponse = daemon.request(Method::ControlMetrics, &Empty {}).await?;

    if args.json {
        println!(
            "{}",
            data_value_to_json(&observe_value(snapshot, &status, &metrics))
        );
    } else {
        println!("{:#?}", snapshot);
        println!(
            "health node={} role={} leader={} term={} commit_index={} last_applied={}",
            status.node_id,
            status.role,
            status.leader_id.as_deref().unwrap_or("none"),
            status.current_term,
            status.commit_index,
            status.last_applied,
        );
        println!(
            "metrics deployments={} pipelines={} nodes={} peers={} tables={} durable_events={}",
            metrics.deployment_count,
            metrics.pipeline_count,
            metrics.node_count,
            metrics.peer_count,
            metrics.table_count,
            metrics.durable_events.unwrap_or(0),
        );
    }
    Ok(())
}

async fn cmd_replay(daemon: Arc<DaemonQuicClient>, args: ReplayArgs) -> Result<()> {
    let replay_request = replay_request(&args)?;
    let replay: ReplayApiResponse = daemon
        .request(Method::ControlReplay, &replay_request)
        .await?;

    if args.json {
        println!("{}", data_value_to_json(&replay_response_value(&replay)));
        return Ok(());
    }

    println!(
        "replay events={} start_sequence={} high_watermark={}",
        replay.events.len(),
        replay
            .start_sequence
            .map(|value| value.to_string())
            .unwrap_or_else(|| "none".to_string()),
        replay
            .high_watermark
            .map(|value| value.to_string())
            .unwrap_or_else(|| "none".to_string())
    );
    if let Some(workload) = replay_selected_workload(args)? {
        let state = cp_query_state(&daemon, true).await?;
        let contracts = collect_contracts_for_workload(&state, &workload)
            .into_iter()
            .map(format_contract)
            .collect::<Vec<_>>();
        println!("workload contracts: {}", contracts.join(", "));
    }
    println!("{:#?}", replay.events);
    Ok(())
}

async fn cmd_discover(daemon: Arc<DaemonQuicClient>, args: DiscoverArgs) -> Result<()> {
    match discover_query(&args)? {
        DiscoverQuery::State(scope) => {
            let state = cp_query_state(&daemon, true).await?;
            let discovery = local_discovery_state(&state, &scope)?;
            if args.json {
                println!("{}", data_value_to_json(&discovery_state_value(&discovery)));
                return Ok(());
            }

            println!(
                "discover workloads={} endpoints={}",
                discovery.workloads.len(),
                discovery.endpoints.len()
            );
            for workload in &discovery.workloads {
                println!(
                    "{} endpoints={}",
                    workload.workload.key(),
                    workload.endpoints.len()
                );
            }
        }
        DiscoverQuery::Resolve(request) => {
            let state = cp_query_state(&daemon, true).await?;
            let resolution = local_discovery_resolution(&state, request)?;
            if args.json {
                println!(
                    "{}",
                    data_value_to_json(&discovery_resolution_value(&resolution))
                );
                return Ok(());
            }

            match resolution {
                DiscoveryResolution::Workload(workload) => {
                    println!(
                        "workload {} endpoints={}",
                        workload.workload.key(),
                        workload.endpoints.len()
                    );
                }
                DiscoveryResolution::Endpoint(endpoint) => {
                    println!(
                        "endpoint {} contract={}",
                        endpoint.endpoint.key(),
                        contract_ref_key(endpoint.contract.as_ref())
                    );
                }
                DiscoveryResolution::RunningProcess(process) => {
                    println!(
                        "running_process workload={} replica_key={} node={}",
                        process.workload.key(),
                        process.replica_key,
                        process.node
                    );
                }
            }
        }
    }

    Ok(())
}

async fn cmd_inventory(daemon: Arc<DaemonQuicClient>, args: InventoryArgs) -> Result<()> {
    let inventory = cp_query_value(
        &daemon,
        Query::AttributedInfrastructureInventory {
            filter: AttributedInfrastructureFilter {
                external_account_ref: args.external_account_ref,
                workload: args.workload,
                module: args.module,
                pipeline: args.pipeline,
                node: args.node,
            },
        },
        true,
    )
    .await?;

    if args.json {
        println!("{}", data_value_to_json(&inventory));
        return Ok(());
    }

    println!(
        "inventory workloads={} pipelines={} modules={} nodes={}",
        list_len(&inventory, "workloads"),
        list_len(&inventory, "pipelines"),
        list_len(&inventory, "modules"),
        list_len(&inventory, "nodes")
    );
    Ok(())
}

async fn cmd_usage(daemon: Arc<DaemonQuicClient>, args: UsageArgs) -> Result<()> {
    let usage: RuntimeUsageApiResponse = daemon
        .request(Method::RuntimeUsageQuery, &runtime_usage_request(&args)?)
        .await?;

    if args.json {
        println!("{}", runtime_usage_response_json(&usage));
        return Ok(());
    }

    println!(
        "usage records={} next_sequence={} high_watermark={}",
        usage.records.len(),
        usage
            .next_sequence
            .map(|value| value.to_string())
            .unwrap_or_else(|| "none".to_string()),
        usage
            .high_watermark
            .map(|value| value.to_string())
            .unwrap_or_else(|| "none".to_string())
    );
    Ok(())
}

async fn cmd_attach(daemon: Arc<DaemonQuicClient>, args: AttachArgs) -> Result<()> {
    let selector = parse_guest_log_attach_selector(&args.target)?;
    let stream_names = selector.stream_names()?;
    let target = OperationalProcessSelector::Workload(selector.workload);
    let mut subscription = daemon
        .subscribe_guest_logs(&SubscribeGuestLogsRequest {
            target,
            stream_names,
        })
        .await?;
    let label_streams = subscription.response.streams.len() > 1;
    let mut out = std::io::stdout();

    loop {
        tokio::select! {
            _ = signal::ctrl_c() => break,
            event = subscription.next_event() => {
                match event? {
                    Some(event) => {
                        let rendered = render_guest_log_payload(&event.endpoint, &event.payload, label_streams);
                        if !rendered.is_empty() {
                            out.write_all(&rendered).context("write attach output")?;
                            out.flush().context("flush attach output")?;
                        }
                    }
                    None => break,
                }
            }
        }
    }

    Ok(())
}

async fn cmd_nodes(daemon: Arc<DaemonQuicClient>, args: NodesArgs) -> Result<()> {
    let nodes = cp_query_value(
        &daemon,
        Query::NodesLive {
            now_ms: unix_ms(),
            max_staleness_ms: args.max_staleness_ms,
        },
        true,
    )
    .await?;

    if args.json {
        println!("{}", data_value_to_json(&nodes));
    } else {
        let list = nodes
            .get("nodes")
            .and_then(DataValue::as_array)
            .map(|slice| slice.to_vec())
            .unwrap_or_default();
        for node in list {
            println!(
                "{} addr={} live={} age_ms={}",
                node.get("name")
                    .and_then(DataValue::as_str)
                    .unwrap_or("unknown"),
                node.get("daemon_addr")
                    .and_then(DataValue::as_str)
                    .unwrap_or(""),
                node.get("live")
                    .and_then(DataValue::as_bool)
                    .unwrap_or(false),
                node.get("age_ms").and_then(DataValue::as_u64).unwrap_or(0)
            );
        }
    }

    Ok(())
}

async fn cmd_start(daemon: Arc<DaemonQuicClient>, args: StartArgs) -> Result<()> {
    let managed_endpoint_bindings = args
        .event_writers
        .iter()
        .map(|endpoint| ManagedEndpointBinding {
            endpoint_name: endpoint.clone(),
            endpoint_kind: ContractKind::Event,
            role: ManagedEndpointRole::Egress,
            binding_type: ManagedEndpointBindingType::OneWay,
        })
        .chain(
            args.event_readers
                .iter()
                .map(|endpoint| ManagedEndpointBinding {
                    endpoint_name: endpoint.clone(),
                    endpoint_kind: ContractKind::Event,
                    role: ManagedEndpointRole::Ingress,
                    binding_type: ManagedEndpointBindingType::OneWay,
                }),
        )
        .collect::<Vec<_>>();
    let module_spec = if let Some(spec) = args.module_spec {
        spec
    } else {
        let module = args
            .module
            .ok_or_else(|| anyhow!("provide --module-spec or --module"))?;
        build_module_spec(
            &module,
            args.adaptor,
            args.isolation,
            if args.capabilities.is_empty() {
                default_runtime_capabilities()
            } else {
                args.capabilities
            },
        )
    };

    let response: StartResponse = daemon
        .request(
            Method::StartInstance,
            &StartRequest {
                node_id: args.node,
                workload_key: args.replica_key.clone(),
                instance_id: args.replica_key,
                module_spec,
                external_account_ref: None,
                managed_endpoint_bindings,
            },
        )
        .await?;

    println!(
        "start status={} replica_key={} process_id={} already_running={}",
        response.status, response.instance_id, response.process_id, response.already_running
    );
    Ok(())
}

async fn cmd_stop(daemon: Arc<DaemonQuicClient>, args: StopArgs) -> Result<()> {
    let response: StopResponse = daemon
        .request(
            Method::StopInstance,
            &StopRequest {
                node_id: args.node,
                instance_id: args.replica_key,
            },
        )
        .await?;

    println!(
        "stop status={} replica_key={} process_id={}",
        response.status,
        response.instance_id,
        response
            .process_id
            .map(|id| id.to_string())
            .unwrap_or_else(|| "none".to_string())
    );
    Ok(())
}

async fn cmd_list(
    daemon: Arc<DaemonQuicClient>,
    conn_args: &DaemonConnectionArgs,
    args: ListArgs,
) -> Result<()> {
    if let Some(node) = args.node {
        let list = daemon.list_instances(node).await?;
        print_instance_map(&list.instances);
        return Ok(());
    }

    let state = cp_query_state(&daemon, true).await?;
    for node in state.nodes.values() {
        let client = DaemonQuicClient::new_from_material(
            parse_daemon_addr(&node.daemon_addr)?,
            node.daemon_server_name.clone(),
            &conn_args.ca_cert,
            &conn_args.client_cert,
            &conn_args.client_key,
        )?;
        let list = client
            .list_instances(node.name.clone())
            .await
            .unwrap_or(ListResponse {
                instances: BTreeMap::new(),
                active_bridges: Vec::new(),
                observed_memory_bytes: None,
                observed_workloads: BTreeMap::new(),
                observed_workload_memory_bytes: BTreeMap::new(),
            });
        println!("node={}", node.name);
        print_instance_map(&list.instances);
    }

    Ok(())
}

async fn cmd_agent(
    daemon: Arc<DaemonQuicClient>,
    conn_args: &DaemonConnectionArgs,
    args: AgentArgs,
) -> Result<()> {
    let agent_state_path = args
        .agent_state
        .clone()
        .unwrap_or_else(|| PathBuf::from(format!(".selium/agent-{}.rkyv", args.node)));
    let mut agent_state = load_agent_state(&agent_state_path)?;

    loop {
        let state = cp_query_state(&daemon, true).await?;
        ensure_pipeline_consistency(&state)?;
        let plan = build_plan(&state)?;

        let actions = reconcile(&args.node, &state, &plan, &agent_state);
        execute_daemon_actions(&daemon, conn_args, &state, &actions, &args.node).await?;
        apply(&mut agent_state, &actions);
        save_agent_state(&agent_state_path, &agent_state)?;

        if !actions.is_empty() {
            println!(
                "node {} reconciled {} actions (running={})",
                args.node,
                actions.len(),
                agent_state.running_instances.len()
            );
        }

        if args.once {
            break;
        }

        tokio::select! {
            _ = sleep(Duration::from_millis(args.interval_ms.max(250))) => {}
            _ = signal::ctrl_c() => {
                println!("agent loop interrupted");
                break;
            }
        }
    }

    Ok(())
}

async fn cmd_idl_publish(daemon: Arc<DaemonQuicClient>, args: IdlPublishArgs) -> Result<()> {
    let source = fs::read_to_string(&args.input)
        .with_context(|| format!("read IDL file {}", args.input.display()))?;
    cp_mutate(&daemon, Mutation::PublishIdl { idl: source }).await?;
    println!("published IDL {}", args.input.display());
    Ok(())
}

fn cmd_idl_compile(args: IdlCompileArgs) -> Result<()> {
    let source = fs::read_to_string(&args.input)
        .with_context(|| format!("read IDL file {}", args.input.display()))?;
    let package =
        parse_idl(&source).with_context(|| format!("parse IDL file {}", args.input.display()))?;
    let generated = generate_rust_bindings(&package);
    if let Some(parent) = args.output.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("create output dir {}", parent.display()))?;
    }
    fs::write(&args.output, generated)
        .with_context(|| format!("write generated bindings {}", args.output.display()))?;
    println!(
        "compiled {} -> {}",
        args.input.display(),
        args.output.display()
    );
    Ok(())
}

async fn cp_mutate(daemon: &DaemonQuicClient, mutation: Mutation) -> Result<DataValue> {
    static IDEMPOTENCY_COUNTER: AtomicU64 = AtomicU64::new(1);
    let key = format!(
        "cli-{}-{}",
        unix_ms(),
        IDEMPOTENCY_COUNTER.fetch_add(1, Ordering::Relaxed)
    );

    let response: MutateApiResponse = daemon
        .request(
            Method::ControlMutate,
            &MutateApiRequest {
                idempotency_key: key,
                mutation,
            },
        )
        .await?;

    if response.committed {
        return Ok(response.result.unwrap_or(DataValue::Null));
    }

    let message = response
        .error
        .unwrap_or_else(|| "mutation not committed".to_string());
    Err(anyhow!(
        "control-plane mutate failed: {} (leader_hint={:?})",
        message,
        response.leader_hint
    ))
}

async fn cp_query_value(
    daemon: &DaemonQuicClient,
    query: Query,
    allow_stale: bool,
) -> Result<DataValue> {
    let response: QueryApiResponse = daemon
        .request(
            Method::ControlQuery,
            &QueryApiRequest { query, allow_stale },
        )
        .await?;

    if let Some(error) = response.error {
        return Err(anyhow!(
            "control-plane query failed: {} (leader_hint={:?})",
            error,
            response.leader_hint
        ));
    }

    response
        .result
        .ok_or_else(|| anyhow!("control-plane query returned no result"))
}

async fn cp_query_state(daemon: &DaemonQuicClient, allow_stale: bool) -> Result<ControlPlaneState> {
    let value = cp_query_value(daemon, Query::ControlPlaneState, allow_stale).await?;
    let bytes = match value {
        DataValue::Bytes(bytes) => bytes,
        other => {
            return Err(anyhow!(
                "invalid control-plane state payload (expected bytes), got {other:?}"
            ));
        }
    };
    decode_rkyv(&bytes).context("decode control-plane state")
}

fn local_discovery_state(
    state: &ControlPlaneState,
    scope: &DiscoveryCapabilityScope,
) -> Result<DiscoveryState> {
    let discovery = build_discovery_state(state)?;
    let workloads = discovery
        .workloads
        .into_iter()
        .filter(|record| scope.allows_workload(DiscoveryOperation::Discover, &record.workload))
        .collect();
    let endpoints = discovery
        .endpoints
        .into_iter()
        .filter(|record| scope.allows_endpoint(DiscoveryOperation::Discover, &record.endpoint))
        .collect();

    Ok(DiscoveryState {
        workloads,
        endpoints,
    })
}

fn local_discovery_resolution(
    state: &ControlPlaneState,
    request: DiscoveryRequest,
) -> Result<DiscoveryResolution> {
    let discovery = build_discovery_state(state)?;

    match request.target {
        DiscoveryTarget::Workload(workload) => {
            if !request.scope.allows_workload(request.operation, &workload) {
                return Err(ApiError::Unauthorised {
                    operation: request.operation.as_str().to_string(),
                    subject: workload.key(),
                }
                .into());
            }

            if !discovery
                .workloads
                .iter()
                .any(|record| record.workload == workload)
            {
                return Err(ApiError::UnknownDeployment(workload.key()).into());
            }

            let endpoints = discovery
                .endpoints
                .iter()
                .filter(|record| record.endpoint.workload == workload)
                .cloned()
                .collect();

            Ok(DiscoveryResolution::Workload(ResolvedWorkload {
                workload,
                endpoints,
            }))
        }
        DiscoveryTarget::Endpoint(endpoint) => {
            if !request.scope.allows_endpoint(request.operation, &endpoint) {
                return Err(ApiError::Unauthorised {
                    operation: request.operation.as_str().to_string(),
                    subject: endpoint.key(),
                }
                .into());
            }

            let endpoint_record = discovery
                .endpoints
                .into_iter()
                .find(|record| record.endpoint == endpoint)
                .ok_or_else(|| ApiError::UnknownEndpoint(endpoint.key()))?;

            Ok(DiscoveryResolution::Endpoint(ResolvedEndpoint {
                endpoint,
                contract: endpoint_record.contract,
            }))
        }
        DiscoveryTarget::RunningProcess(selector) => {
            if request.operation == DiscoveryOperation::Bind {
                return Err(ApiError::InvalidBindTarget(
                    "running process discovery is operational-only".to_string(),
                )
                .into());
            }

            let plan = build_plan(state).map_err(|err| anyhow!("scheduler error: {err}"))?;
            let record = match selector {
                OperationalProcessSelector::ReplicaKey(replica_key) => {
                    let instance = plan
                        .instances
                        .iter()
                        .find(|instance| instance.instance_id == replica_key)
                        .ok_or_else(|| ApiError::UnknownDeployment(replica_key.clone()))?;
                    let workload = state
                        .deployments
                        .get(&instance.deployment)
                        .map(|deployment| deployment.workload.clone())
                        .ok_or_else(|| ApiError::UnknownDeployment(instance.deployment.clone()))?;
                    if !request
                        .scope
                        .allows_operational_process_discovery(&workload)
                    {
                        return Err(ApiError::Unauthorised {
                            operation: DiscoveryOperation::Discover.as_str().to_string(),
                            subject: workload.key(),
                        }
                        .into());
                    }

                    OperationalProcessRecord {
                        workload,
                        replica_key: instance.instance_id.clone(),
                        node: instance.node.clone(),
                    }
                }
                OperationalProcessSelector::Workload(workload) => {
                    if !request
                        .scope
                        .allows_operational_process_discovery(&workload)
                    {
                        return Err(ApiError::Unauthorised {
                            operation: DiscoveryOperation::Discover.as_str().to_string(),
                            subject: workload.key(),
                        }
                        .into());
                    }

                    let mut matches = plan
                        .instances
                        .iter()
                        .filter(|instance| instance.deployment == workload.key())
                        .map(|instance| OperationalProcessRecord {
                            workload: workload.clone(),
                            replica_key: instance.instance_id.clone(),
                            node: instance.node.clone(),
                        })
                        .collect::<Vec<_>>();

                    if matches.is_empty() {
                        return Err(ApiError::UnknownDeployment(workload.key()).into());
                    }
                    if matches.len() > 1 {
                        return Err(ApiError::AmbiguousProcess(format!(
                            "{} has {} replicas; specify a replica key",
                            workload,
                            matches.len()
                        ))
                        .into());
                    }

                    matches.remove(0)
                }
            };

            Ok(DiscoveryResolution::RunningProcess(record))
        }
    }
}

async fn node_client(
    daemon: &DaemonQuicClient,
    conn_args: &DaemonConnectionArgs,
    node: &str,
) -> Result<DaemonQuicClient> {
    let state = cp_query_state(daemon, true).await?;
    let node_spec = state
        .nodes
        .get(node)
        .ok_or_else(|| anyhow!("unknown node `{node}`"))?;
    let target_addr = parse_daemon_addr(&node_spec.daemon_addr)?;
    let current_addr = parse_daemon_addr(&conn_args.daemon_addr)?;
    let target_server_name = node_spec.daemon_server_name.clone();
    if target_addr == current_addr && target_server_name == conn_args.daemon_server_name {
        daemon.reset_connection().await;
    }

    DaemonQuicClient::new_from_material(
        target_addr,
        target_server_name,
        &conn_args.ca_cert,
        &conn_args.client_cert,
        &conn_args.client_key,
    )
}

async fn execute_daemon_actions(
    daemon: &DaemonQuicClient,
    conn_args: &DaemonConnectionArgs,
    state: &ControlPlaneState,
    actions: &[ReconcileAction],
    node: &str,
) -> Result<()> {
    let node_client = node_client(daemon, conn_args, node).await?;
    for action in actions {
        match action {
            ReconcileAction::Start {
                instance_id,
                deployment,
                managed_endpoint_bindings,
            } => {
                let spec = state
                    .deployments
                    .get(deployment)
                    .ok_or_else(|| anyhow!("missing deployment `{deployment}`"))?;
                let module_spec = build_deployment_module_spec(spec);
                let _ = node_client
                    .request::<_, StartResponse>(
                        Method::StartInstance,
                        &StartRequest {
                            node_id: node.to_string(),
                            workload_key: deployment.clone(),
                            instance_id: instance_id.clone(),
                            module_spec,
                            external_account_ref: spec
                                .external_account_ref
                                .as_ref()
                                .map(|reference| reference.key.clone()),
                            managed_endpoint_bindings: managed_endpoint_bindings.clone(),
                        },
                    )
                    .await?;
            }
            ReconcileAction::Stop { instance_id } => {
                let _ = node_client
                    .request::<_, StopResponse>(
                        Method::StopInstance,
                        &StopRequest {
                            node_id: node.to_string(),
                            instance_id: instance_id.clone(),
                        },
                    )
                    .await?;
            }
            ReconcileAction::EnsureEndpointBridge(action) => {
                let target_spec = state
                    .nodes
                    .get(&action.target_node)
                    .ok_or_else(|| anyhow!("unknown target node `{}`", action.target_node))?;
                let _ = node_client
                    .request::<_, selium_control_plane_protocol::ActivateEndpointBridgeResponse>(
                        Method::ActivateEndpointBridge,
                        &selium_control_plane_protocol::ActivateEndpointBridgeRequest {
                            node_id: node.to_string(),
                            bridge_id: action.bridge_id.clone(),
                            source_instance_id: action.source_instance_id.clone(),
                            source_endpoint: action.source_endpoint.clone(),
                            target_instance_id: action.target_instance_id.clone(),
                            target_node: action.target_node.clone(),
                            target_daemon_addr: target_spec.daemon_addr.clone(),
                            target_daemon_server_name: target_spec.daemon_server_name.clone(),
                            target_endpoint: action.target_endpoint.clone(),
                            semantics: endpoint_bridge_semantics(action.source_endpoint.kind),
                        },
                    )
                    .await?;
            }
            ReconcileAction::RemoveEndpointBridge { bridge_id } => {
                let _ = node_client
                    .request::<_, selium_control_plane_protocol::DeactivateEndpointBridgeResponse>(
                        Method::DeactivateEndpointBridge,
                        &selium_control_plane_protocol::DeactivateEndpointBridgeRequest {
                            node_id: node.to_string(),
                            bridge_id: bridge_id.clone(),
                        },
                    )
                    .await?;
            }
        }
    }

    Ok(())
}

fn build_module_spec(
    module: &str,
    adaptor: AdaptorArg,
    isolation: IsolationArg,
    capabilities: Vec<String>,
) -> String {
    let adaptor = match adaptor {
        AdaptorArg::Wasmtime => "wasmtime",
        AdaptorArg::Microvm => "microvm",
    };
    let profile = match isolation {
        IsolationArg::Standard => "standard",
        IsolationArg::Hardened => "hardened",
        IsolationArg::Microvm => "microvm",
    };

    build_runtime_module_spec(module, adaptor, profile, capabilities)
}

fn workload_ref(tenant: String, namespace: String, workload: String) -> WorkloadRef {
    WorkloadRef {
        tenant,
        namespace,
        name: workload,
    }
}

fn replay_request(args: &ReplayArgs) -> Result<ReplayApiRequest> {
    Ok(ReplayApiRequest {
        limit: args.limit,
        since_sequence: args.since_sequence,
        checkpoint: None,
        save_checkpoint: None,
        external_account_ref: args.external_account_ref.clone(),
        workload: replay_workload_key(args)?,
        module: args.module.clone(),
        pipeline: args.pipeline.clone(),
        node: args.node.clone(),
    })
}

fn runtime_usage_request(args: &UsageArgs) -> Result<RuntimeUsageApiRequest> {
    Ok(RuntimeUsageApiRequest {
        node_id: args.node.clone(),
        query: RuntimeUsageQuery {
            start: runtime_usage_start(args)?,
            save_checkpoint: None,
            limit: args.limit,
            external_account_ref: args.external_account_ref.clone(),
            workload: args.workload.clone(),
            module: args.module.clone(),
            window_start_ms: args.window_start_ms,
            window_end_ms: args.window_end_ms,
        },
    })
}

fn runtime_usage_start(args: &UsageArgs) -> Result<RuntimeUsageReplayStart> {
    match (args.latest, args.since_sequence, args.since_timestamp_ms) {
        (false, None, None) => Ok(RuntimeUsageReplayStart::Earliest),
        (true, None, None) => Ok(RuntimeUsageReplayStart::Latest),
        (false, Some(sequence), None) => Ok(RuntimeUsageReplayStart::Sequence(sequence)),
        (false, None, Some(timestamp_ms)) => Ok(RuntimeUsageReplayStart::Timestamp(timestamp_ms)),
        _ => Err(anyhow!(
            "provide at most one of --latest, --since-sequence, or --since-timestamp-ms when exporting runtime usage"
        )),
    }
}

fn replay_workload_key(args: &ReplayArgs) -> Result<Option<String>> {
    match (
        args.workload_key.as_deref(),
        args.tenant.as_deref(),
        args.namespace.as_deref(),
        args.workload.as_deref(),
    ) {
        (Some(workload_key), None, None, None) => Ok(Some(workload_key.to_string())),
        (None, Some(tenant), Some(namespace), Some(workload)) => Ok(Some(
            workload_ref(
                tenant.to_string(),
                namespace.to_string(),
                workload.to_string(),
            )
            .key(),
        )),
        (None, None, None, None) => Ok(None),
        (Some(_), _, _, _) => Err(anyhow!(
            "provide either --workload-key or --tenant/--namespace/--workload when filtering replay output"
        )),
        _ => Err(anyhow!(
            "provide --tenant, --namespace, and --workload together when filtering replay output"
        )),
    }
}

fn replay_selected_workload(args: ReplayArgs) -> Result<Option<WorkloadRef>> {
    match (
        args.workload_key,
        args.tenant,
        args.namespace,
        args.workload,
    ) {
        (Some(workload_key), None, None, None) => parse_workload_key(&workload_key).map(Some),
        (None, Some(tenant), Some(namespace), Some(workload)) => {
            Ok(Some(workload_ref(tenant, namespace, workload)))
        }
        (None, None, None, None) => Ok(None),
        (Some(_), _, _, _) => Err(anyhow!(
            "provide either --workload-key or --tenant/--namespace/--workload when filtering replay output"
        )),
        _ => Err(anyhow!(
            "provide --tenant, --namespace, and --workload together when filtering replay output"
        )),
    }
}

enum DiscoverQuery {
    State(DiscoveryCapabilityScope),
    Resolve(DiscoveryRequest),
}

fn discover_query(args: &DiscoverArgs) -> Result<DiscoverQuery> {
    if let Some(workload_key) = args.resolve_workload.as_deref() {
        let workload = parse_workload_key(workload_key)?;
        return Ok(DiscoverQuery::Resolve(DiscoveryRequest {
            scope: discovery_scope(
                args,
                Some(DiscoveryPattern::Exact(workload.key())),
                DiscoveryOperation::Discover,
                false,
            ),
            operation: DiscoveryOperation::Discover,
            target: DiscoveryTarget::Workload(workload),
        }));
    }

    if let Some(endpoint_key) = args.resolve_endpoint.as_deref() {
        let endpoint = parse_public_endpoint_key(endpoint_key)?;
        return Ok(DiscoverQuery::Resolve(DiscoveryRequest {
            scope: discovery_scope(
                args,
                Some(DiscoveryPattern::Exact(endpoint.workload.key())),
                DiscoveryOperation::Bind,
                false,
            ),
            operation: DiscoveryOperation::Bind,
            target: DiscoveryTarget::Endpoint(endpoint),
        }));
    }

    if let Some(workload_key) = args.resolve_running_workload.as_deref() {
        let workload = parse_workload_key(workload_key)?;
        return Ok(DiscoverQuery::Resolve(DiscoveryRequest {
            scope: discovery_scope(
                args,
                Some(DiscoveryPattern::Exact(workload.key())),
                DiscoveryOperation::Discover,
                true,
            ),
            operation: DiscoveryOperation::Discover,
            target: DiscoveryTarget::RunningProcess(OperationalProcessSelector::Workload(workload)),
        }));
    }

    if let Some(replica_key) = args.resolve_replica_key.as_deref() {
        return Ok(DiscoverQuery::Resolve(DiscoveryRequest {
            scope: discovery_scope(args, None, DiscoveryOperation::Discover, true),
            operation: DiscoveryOperation::Discover,
            target: DiscoveryTarget::RunningProcess(OperationalProcessSelector::ReplicaKey(
                replica_key.to_string(),
            )),
        }));
    }

    Ok(DiscoverQuery::State(discovery_scope(
        args,
        None,
        DiscoveryOperation::Discover,
        false,
    )))
}

fn discovery_scope(
    args: &DiscoverArgs,
    default_workload: Option<DiscoveryPattern>,
    operation: DiscoveryOperation,
    require_operational_processes: bool,
) -> DiscoveryCapabilityScope {
    let mut workloads = args
        .workloads
        .iter()
        .cloned()
        .map(DiscoveryPattern::Exact)
        .collect::<Vec<_>>();
    workloads.extend(
        args.workload_prefixes
            .iter()
            .cloned()
            .map(DiscoveryPattern::Prefix),
    );
    if workloads.is_empty() {
        workloads.push(default_workload.unwrap_or_else(|| DiscoveryPattern::Prefix(String::new())));
    }

    DiscoveryCapabilityScope {
        operations: vec![operation],
        workloads,
        endpoints: Vec::new(),
        allow_operational_processes: args.allow_operational_processes
            || require_operational_processes,
    }
}

fn parse_workload_key(value: &str) -> Result<WorkloadRef> {
    let mut parts = value.split('/');
    let (Some(tenant), Some(namespace), Some(workload), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(anyhow!(
            "workload keys must use tenant/namespace/workload form, got {value}"
        ));
    };
    Ok(workload_ref(
        tenant.to_string(),
        namespace.to_string(),
        workload.to_string(),
    ))
}

fn parse_public_endpoint_key(value: &str) -> Result<PublicEndpointRef> {
    let (workload_key, endpoint_key) = value.split_once('#').ok_or_else(|| {
        anyhow!(
            "public endpoint keys must use tenant/namespace/workload#kind:name form, got {value}"
        )
    })?;
    let workload = parse_workload_key(workload_key)?;
    let (kind, name) = endpoint_key.split_once(':').ok_or_else(|| {
        anyhow!(
            "public endpoint keys must use tenant/namespace/workload#kind:name form, got {value}"
        )
    })?;
    let kind = match kind {
        "event" => ContractKind::Event,
        "service" => ContractKind::Service,
        "stream" => ContractKind::Stream,
        other => {
            return Err(anyhow!(
                "unsupported public endpoint kind {other}; expected event, service, or stream"
            ));
        }
    };
    Ok(PublicEndpointRef {
        workload,
        kind,
        name: name.to_string(),
    })
}

fn contract_ref_value(contract: Option<&ContractRef>) -> DataValue {
    let Some(contract) = contract else {
        return DataValue::Null;
    };

    DataValue::Map(BTreeMap::from([
        (
            "namespace".to_string(),
            DataValue::from(contract.namespace.to_string()),
        ),
        (
            "kind".to_string(),
            DataValue::from(contract.kind.as_str().to_string()),
        ),
        (
            "name".to_string(),
            DataValue::from(contract.name.to_string()),
        ),
        (
            "version".to_string(),
            DataValue::from(contract.version.to_string()),
        ),
    ]))
}

fn contract_ref_key(contract: Option<&ContractRef>) -> String {
    contract.map_or_else(
        || "synthetic".to_string(),
        |contract| {
            format!(
                "{}/{}:{}@{}",
                contract.namespace,
                contract.kind.as_str(),
                contract.name,
                contract.version
            )
        },
    )
}

fn public_endpoint_value(endpoint: &PublicEndpointRef) -> DataValue {
    DataValue::Map(BTreeMap::from([
        ("endpoint".to_string(), DataValue::from(endpoint.key())),
        (
            "workload".to_string(),
            DataValue::from(endpoint.workload.key()),
        ),
        (
            "kind".to_string(),
            DataValue::from(endpoint.kind.as_str().to_string()),
        ),
        ("name".to_string(), DataValue::from(endpoint.name.clone())),
    ]))
}

fn discoverable_workload_value(workload: &DiscoverableWorkload) -> DataValue {
    DataValue::Map(BTreeMap::from([
        (
            "workload".to_string(),
            DataValue::from(workload.workload.key()),
        ),
        (
            "endpoints".to_string(),
            DataValue::List(
                workload
                    .endpoints
                    .iter()
                    .map(public_endpoint_value)
                    .collect(),
            ),
        ),
    ]))
}

fn discoverable_endpoint_value(endpoint: &DiscoverableEndpoint) -> DataValue {
    DataValue::Map(BTreeMap::from([
        (
            "endpoint".to_string(),
            public_endpoint_value(&endpoint.endpoint),
        ),
        (
            "contract".to_string(),
            contract_ref_value(endpoint.contract.as_ref()),
        ),
    ]))
}

fn resolved_workload_value(workload: &ResolvedWorkload) -> DataValue {
    DataValue::Map(BTreeMap::from([
        ("kind".to_string(), DataValue::from("workload".to_string())),
        (
            "workload".to_string(),
            DataValue::from(workload.workload.key()),
        ),
        (
            "endpoints".to_string(),
            DataValue::List(
                workload
                    .endpoints
                    .iter()
                    .map(discoverable_endpoint_value)
                    .collect(),
            ),
        ),
    ]))
}

fn resolved_endpoint_value(endpoint: &ResolvedEndpoint) -> DataValue {
    DataValue::Map(BTreeMap::from([
        ("kind".to_string(), DataValue::from("endpoint".to_string())),
        (
            "endpoint".to_string(),
            public_endpoint_value(&endpoint.endpoint),
        ),
        (
            "contract".to_string(),
            contract_ref_value(endpoint.contract.as_ref()),
        ),
    ]))
}

fn operational_process_value(process: &OperationalProcessRecord) -> DataValue {
    DataValue::Map(BTreeMap::from([
        (
            "kind".to_string(),
            DataValue::from("running_process".to_string()),
        ),
        (
            "workload".to_string(),
            DataValue::from(process.workload.key()),
        ),
        (
            "replica_key".to_string(),
            DataValue::from(process.replica_key.clone()),
        ),
        ("node".to_string(), DataValue::from(process.node.clone())),
    ]))
}

fn discovery_state_value(discovery: &DiscoveryState) -> DataValue {
    DataValue::Map(BTreeMap::from([
        (
            "workloads".to_string(),
            DataValue::List(
                discovery
                    .workloads
                    .iter()
                    .map(discoverable_workload_value)
                    .collect(),
            ),
        ),
        (
            "endpoints".to_string(),
            DataValue::List(
                discovery
                    .endpoints
                    .iter()
                    .map(discoverable_endpoint_value)
                    .collect(),
            ),
        ),
    ]))
}

fn discovery_resolution_value(resolution: &DiscoveryResolution) -> DataValue {
    match resolution {
        DiscoveryResolution::Workload(workload) => resolved_workload_value(workload),
        DiscoveryResolution::Endpoint(endpoint) => resolved_endpoint_value(endpoint),
        DiscoveryResolution::RunningProcess(process) => operational_process_value(process),
    }
}

fn list_len(value: &DataValue, key: &str) -> usize {
    value
        .get(key)
        .and_then(DataValue::as_array)
        .map_or(0, |items| items.len())
}

fn qualified_pipeline_key(tenant: &str, namespace: &str, pipeline: &str) -> String {
    format!("{tenant}/{namespace}/{pipeline}")
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GuestLogAttachSelector {
    workload: WorkloadRef,
    stream_selector: GuestLogStreamSelector,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum GuestLogStreamSelector {
    Exact(String),
    Prefix(String),
}

fn parse_guest_log_attach_selector(raw: &str) -> Result<GuestLogAttachSelector> {
    let raw = raw.trim();
    let (workload_raw, endpoint_raw) = raw.split_once('#').ok_or_else(|| {
        anyhow!(
            "attach selector must be formatted as `tenant/namespace/workload#event:stdout`, `...#event:stderr`, or `...#event:std*`"
        )
    })?;
    let workload = parse_attach_workload_ref(workload_raw)?;
    let (kind_raw, stream_raw) = endpoint_raw.split_once(':').ok_or_else(|| {
        anyhow!(
            "attach selector must be formatted as `tenant/namespace/workload#event:stdout`, `...#event:stderr`, or `...#event:std*`"
        )
    })?;
    if kind_raw != ContractKind::Event.as_str() {
        return Err(anyhow!(
            "`selium attach` currently only supports guest log event selectors such as `tenant/namespace/workload#event:stdout`"
        ));
    }
    if stream_raw.is_empty() {
        return Err(anyhow!(
            "attach selector must include a guest log stream name"
        ));
    }

    let stream_selector = if stream_raw.contains('*') {
        let Some(prefix) = stream_raw.strip_suffix('*') else {
            return Err(anyhow!(
                "attach guest log selectors only support a trailing `*` prefix wildcard, for example `tenant/namespace/workload#event:std*`"
            ));
        };
        if prefix.contains('*') {
            return Err(anyhow!(
                "attach guest log selectors only support a trailing `*` prefix wildcard, for example `tenant/namespace/workload#event:std*`"
            ));
        }
        GuestLogStreamSelector::Prefix(prefix.to_string())
    } else {
        GuestLogStreamSelector::Exact(stream_raw.to_string())
    };

    Ok(GuestLogAttachSelector {
        workload,
        stream_selector,
    })
}

fn parse_attach_workload_ref(raw: &str) -> Result<WorkloadRef> {
    let mut parts = raw.split('/');
    let (Some(tenant), Some(namespace), Some(name), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(anyhow!(
            "attach selector workload must be `tenant/namespace/workload`"
        ));
    };

    let workload = WorkloadRef {
        tenant: tenant.to_string(),
        namespace: namespace.to_string(),
        name: name.to_string(),
    };
    workload
        .validate("attach selector")
        .map_err(|err| anyhow!(err.to_string()))?;
    Ok(workload)
}

impl GuestLogAttachSelector {
    fn stream_names(&self) -> Result<Vec<String>> {
        match &self.stream_selector {
            GuestLogStreamSelector::Exact(name) => {
                if GUEST_LOG_STREAMS.contains(&name.as_str()) {
                    Ok(vec![name.clone()])
                } else {
                    Err(anyhow!(
                        "`selium attach` only supports guest log streams `stdout`, `stderr`, or prefix wildcards like `std*`"
                    ))
                }
            }
            GuestLogStreamSelector::Prefix(prefix) => {
                let pattern = DiscoveryPattern::Prefix(prefix.clone());
                let streams = GUEST_LOG_STREAMS
                    .into_iter()
                    .filter(|stream| pattern.matches(stream))
                    .map(str::to_string)
                    .collect::<Vec<_>>();
                if streams.is_empty() {
                    Err(anyhow!(
                        "guest log selector `{}#event:{}*` matched no guest log streams; try `stdout`, `stderr`, or `std*`",
                        self.workload,
                        prefix
                    ))
                } else {
                    Ok(streams)
                }
            }
        }
    }
}

fn load_agent_state(path: &Path) -> Result<AgentState> {
    if !path.exists() {
        return Ok(AgentState::default());
    }

    let data = fs::read(path).with_context(|| format!("read agent state {}", path.display()))?;
    let state =
        decode_rkyv(&data).with_context(|| format!("decode agent state {}", path.display()))?;
    Ok(state)
}

fn save_agent_state(path: &Path, state: &AgentState) -> Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("create agent state dir {}", parent.display()))?;
    }

    let data = encode_rkyv(state).context("encode agent state")?;
    fs::write(path, data).with_context(|| format!("write agent state {}", path.display()))?;
    Ok(())
}

fn unix_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests;
