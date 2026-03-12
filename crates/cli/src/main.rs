mod config;
mod daemon_client;

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, anyhow};
use selium_abi::{
    DataValue, RuntimeUsageQuery, RuntimeUsageRecord, RuntimeUsageReplayStart, decode_rkyv,
    encode_rkyv,
};
use selium_control_plane_protocol::{
    Empty, EndpointBridgeSemantics, EventBridgeSemantics, EventDeliveryMode, ListRequest,
    ListResponse, ManagedEndpointBinding, ManagedEndpointBindingType, ManagedEndpointRole, Method,
    MetricsApiResponse, MutateApiRequest, MutateApiResponse, QueryApiRequest, QueryApiResponse,
    ReplayApiRequest, ReplayApiResponse, RuntimeUsageApiRequest, RuntimeUsageApiResponse,
    ServiceBridgeSemantics, ServiceCorrelationMode, StartRequest, StartResponse, StatusApiResponse,
    StopRequest, StopResponse, StreamBridgeSemantics, StreamLifecycleMode,
};
use selium_module_control_plane::{
    AgentState, ReconcileAction,
    api::{
        ContractKind, ContractRef, ControlPlaneState, DeploymentSpec, DiscoverableEndpoint,
        DiscoverableWorkload, DiscoveryCapabilityScope, DiscoveryOperation, DiscoveryPattern,
        DiscoveryRequest, DiscoveryResolution, DiscoveryState, DiscoveryTarget, EventEndpointRef,
        OperationalProcessRecord, OperationalProcessSelector, PipelineEdge, PipelineEndpoint,
        PipelineSpec, PublicEndpointRef, ResolvedEndpoint, ResolvedWorkload, WorkloadRef,
        collect_contracts_for_workload, ensure_pipeline_consistency, generate_rust_bindings,
        parse_contract_ref, parse_idl,
    },
    apply, build_module_spec as build_runtime_module_spec,
    default_capabilities as default_runtime_capabilities,
    deployment_module_spec as build_deployment_module_spec, reconcile,
    runtime::{AttributedInfrastructureFilter, Mutation, Query},
    scheduler::build_plan,
};
use tokio::{signal, time::sleep};

use crate::{
    config::{
        AdaptorArg, AgentArgs, Command, ConnectArgs, DaemonConnectionArgs, DeployArgs,
        DiscoverArgs, IdlArgs, IdlCommand, IdlCompileArgs, IdlPublishArgs, InventoryArgs,
        IsolationArg, ListArgs, NodesArgs, ObserveArgs, ReplayArgs, ScaleArgs, StartArgs, StopArgs,
        UsageArgs, load_cli,
    },
    daemon_client::{DaemonQuicClient, parse_daemon_addr},
};

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
                cpu_millis: 0,
                memory_mib: 0,
                ephemeral_storage_mib: 0,
                bandwidth_profile: selium_module_control_plane::api::BandwidthProfile::Standard,
                volume_mounts: Vec::new(),
                external_account_ref: None,
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

fn observe_value(
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

fn status_value(status: &StatusApiResponse) -> DataValue {
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

fn metrics_value(metrics: &MetricsApiResponse) -> DataValue {
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

fn optional_u64_value(value: Option<u64>) -> DataValue {
    value.map(DataValue::from).unwrap_or(DataValue::Null)
}

fn replay_response_value(replay: &ReplayApiResponse) -> DataValue {
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

fn optional_json_u64(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "null".to_string())
}

fn optional_json_str(value: Option<&str>) -> String {
    value
        .map(|value| format!("\"{}\"", escape_json(value)))
        .unwrap_or_else(|| "null".to_string())
}

fn string_map_json(map: &BTreeMap<String, String>) -> String {
    let pairs = map
        .iter()
        .map(|(key, value)| format!("\"{}\":\"{}\"", escape_json(key), escape_json(value)))
        .collect::<Vec<_>>()
        .join(",");
    format!("{{{pairs}}}")
}

fn runtime_usage_record_json(record: &RuntimeUsageRecord) -> String {
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

fn runtime_usage_response_json(response: &RuntimeUsageApiResponse) -> String {
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

fn data_value_to_json(value: &DataValue) -> String {
    let mut out = String::new();
    write_data_value_json(&mut out, value);
    out
}

fn write_data_value_json(out: &mut String, value: &DataValue) {
    use std::fmt::Write as _;

    match value {
        DataValue::Null => out.push_str("null"),
        DataValue::Bool(value) => out.push_str(if *value { "true" } else { "false" }),
        DataValue::U64(value) => {
            let _ = write!(out, "{value}");
        }
        DataValue::I64(value) => {
            let _ = write!(out, "{value}");
        }
        DataValue::String(value) => {
            out.push('"');
            write_json_string_content(out, value);
            out.push('"');
        }
        DataValue::Bytes(bytes) => {
            out.push('[');
            for (idx, byte) in bytes.iter().enumerate() {
                if idx > 0 {
                    out.push(',');
                }
                let _ = write!(out, "{byte}");
            }
            out.push(']');
        }
        DataValue::List(values) => {
            out.push('[');
            for (idx, item) in values.iter().enumerate() {
                if idx > 0 {
                    out.push(',');
                }
                write_data_value_json(out, item);
            }
            out.push(']');
        }
        DataValue::Map(values) => {
            out.push('{');
            for (idx, (key, item)) in values.iter().enumerate() {
                if idx > 0 {
                    out.push(',');
                }
                out.push('"');
                write_json_string_content(out, key);
                out.push_str("\":");
                write_data_value_json(out, item);
            }
            out.push('}');
        }
    }
}

fn write_json_string_content(out: &mut String, value: &str) {
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

fn escape_json(value: &str) -> String {
    let mut out = String::new();
    write_json_string_content(&mut out, value);
    out
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
            let discovery = cp_query_discovery_state(&daemon, scope, true).await?;
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
            let resolution = cp_query_discovery_resolution(&daemon, request, true).await?;
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
                        contract_ref_key(&endpoint.contract)
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
        let list: ListResponse = daemon
            .request(Method::ListInstances, &ListRequest { node_id: node })
            .await?;
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
        let list: ListResponse = client
            .request(
                Method::ListInstances,
                &ListRequest {
                    node_id: node.name.clone(),
                },
            )
            .await
            .unwrap_or(ListResponse {
                instances: BTreeMap::new(),
                active_bridges: Vec::new(),
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

fn print_instance_map(instances: &BTreeMap<String, usize>) {
    for (instance, pid) in instances {
        println!("{instance} {pid}");
    }
}

fn endpoint_bridge_semantics(kind: ContractKind) -> EndpointBridgeSemantics {
    match kind {
        ContractKind::Event => EndpointBridgeSemantics::Event(EventBridgeSemantics {
            delivery: EventDeliveryMode::Frame,
        }),
        ContractKind::Service => EndpointBridgeSemantics::Service(ServiceBridgeSemantics {
            correlation: ServiceCorrelationMode::RequestId,
        }),
        ContractKind::Stream => EndpointBridgeSemantics::Stream(StreamBridgeSemantics {
            lifecycle: StreamLifecycleMode::SessionFrames,
        }),
    }
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

async fn cp_query_discovery_state(
    daemon: &DaemonQuicClient,
    scope: DiscoveryCapabilityScope,
    allow_stale: bool,
) -> Result<DiscoveryState> {
    let value = cp_query_value(daemon, Query::DiscoveryState { scope }, allow_stale).await?;
    let bytes = match value {
        DataValue::Bytes(bytes) => bytes,
        other => {
            return Err(anyhow!(
                "invalid discovery state payload (expected bytes), got {other:?}"
            ));
        }
    };
    decode_rkyv(&bytes).context("decode discovery state")
}

async fn cp_query_discovery_resolution(
    daemon: &DaemonQuicClient,
    request: DiscoveryRequest,
    allow_stale: bool,
) -> Result<DiscoveryResolution> {
    let value = cp_query_value(daemon, Query::ResolveDiscovery { request }, allow_stale).await?;
    let bytes = match value {
        DataValue::Bytes(bytes) => bytes,
        other => {
            return Err(anyhow!(
                "invalid discovery resolution payload (expected bytes), got {other:?}"
            ));
        }
    };
    decode_rkyv(&bytes).context("decode discovery resolution")
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
            ReconcileAction::EnsureEndpointBridge {
                bridge_id,
                source_instance_id,
                source_endpoint,
                target_instance_id,
                target_node,
                target_endpoint,
            } => {
                let target_spec = state
                    .nodes
                    .get(target_node)
                    .ok_or_else(|| anyhow!("unknown target node `{target_node}`"))?;
                let _ = node_client
                    .request::<_, selium_control_plane_protocol::ActivateEndpointBridgeResponse>(
                        Method::ActivateEndpointBridge,
                        &selium_control_plane_protocol::ActivateEndpointBridgeRequest {
                            node_id: node.to_string(),
                            bridge_id: bridge_id.clone(),
                            source_instance_id: source_instance_id.clone(),
                            source_endpoint: source_endpoint.clone(),
                            target_instance_id: target_instance_id.clone(),
                            target_node: target_node.clone(),
                            target_daemon_addr: target_spec.daemon_addr.clone(),
                            target_daemon_server_name: target_spec.daemon_server_name.clone(),
                            target_endpoint: target_endpoint.clone(),
                            semantics: endpoint_bridge_semantics(source_endpoint.kind),
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

fn contract_ref_value(contract: &ContractRef) -> DataValue {
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

fn contract_ref_key(contract: &ContractRef) -> String {
    format!(
        "{}/{}:{}@{}",
        contract.namespace,
        contract.kind.as_str(),
        contract.name,
        contract.version
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
            contract_ref_value(&endpoint.contract),
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
            contract_ref_value(&endpoint.contract),
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

fn format_contract(contract: ContractRef) -> String {
    format!(
        "{}/{}:{}@{}",
        contract.namespace,
        contract.kind.as_str(),
        contract.name,
        contract.version
    )
}

fn unix_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
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
                contract: ContractRef {
                    namespace: "media.pipeline".to_string(),
                    kind: ContractKind::Service,
                    name: "camera.detect".to_string(),
                    version: "v1".to_string(),
                },
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
                contract: ContractRef {
                    namespace: "analytics.topology".to_string(),
                    kind: ContractKind::Event,
                    name: "ingest.frames".to_string(),
                    version: "v1".to_string(),
                },
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
                headers: BTreeMap::from([(
                    "external_account_ref".to_string(),
                    "acct-123".to_string(),
                )]),
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
                headers: BTreeMap::from([(
                    "external_account_ref".to_string(),
                    "acct-123".to_string(),
                )]),
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
}
