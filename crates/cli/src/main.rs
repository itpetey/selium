use std::{
    collections::BTreeMap,
    fs,
    net::{SocketAddr, ToSocketAddrs},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, anyhow};
use clap::{Args, Parser, Subcommand, ValueEnum};
use quinn::crypto::rustls::QuicClientConfig;
use quinn::{Connection, Endpoint};
use rkyv::{
    Archive,
    api::high::{HighDeserializer, HighValidator},
};
use rustls::{RootCertStore, pki_types::PrivateKeyDer};
use rustls_pemfile::{certs, private_key};
use selium_abi::{DataValue, RkyvEncode, decode_rkyv, encode_rkyv};
use selium_control_plane_protocol::{
    Empty, ListResponse, Method, MutateApiRequest, QueryApiRequest, ReplayApiRequest,
    ReplayApiResponse, StartRequest, StartResponse, StopRequest, StopResponse, decode_envelope,
    decode_error, decode_payload, encode_request, is_error, read_framed, write_framed,
};
use selium_module_control_plane::{
    AgentState, ReconcileAction,
    api::{
        ContractRef, ControlPlaneState, DeploymentSpec, IsolationProfile, PipelineEdge,
        PipelineEndpoint, PipelineSpec, collect_contracts_for_app, ensure_pipeline_consistency,
        generate_rust_bindings, parse_contract_ref, parse_idl,
    },
    apply, reconcile,
    runtime::{Mutation, Query},
    scheduler::build_plan,
};
use tokio::{signal, sync::Mutex, time::sleep};

#[derive(Debug, Parser)]
#[command(name = "selium", about = "Selium platform CLI")]
struct Cli {
    #[command(flatten)]
    daemon: DaemonConnectionArgs,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Args, Clone)]
struct DaemonConnectionArgs {
    #[arg(long, default_value = "127.0.0.1:7100")]
    daemon_addr: String,
    #[arg(long, default_value = "localhost")]
    daemon_server_name: String,
    #[arg(long, default_value = "certs/ca.crt")]
    ca_cert: PathBuf,
    #[arg(long, default_value = "certs/client.crt")]
    client_cert: PathBuf,
    #[arg(long, default_value = "certs/client.key")]
    client_key: PathBuf,
}

#[derive(Debug, Subcommand)]
enum Command {
    Deploy(DeployArgs),
    Connect(ConnectArgs),
    Scale(ScaleArgs),
    Observe(ObserveArgs),
    Replay(ReplayArgs),
    Nodes(NodesArgs),
    Start(StartArgs),
    Stop(StopArgs),
    List(ListArgs),
    Agent(AgentArgs),
    Idl(IdlArgs),
}

#[derive(Debug, Args)]
struct DeployArgs {
    #[arg(long)]
    app: String,
    #[arg(long)]
    module: String,
    #[arg(long, default_value_t = 1)]
    replicas: u32,
    #[arg(long, value_enum, default_value_t = IsolationArg::Standard)]
    isolation: IsolationArg,
    #[arg(long = "contract")]
    contracts: Vec<String>,
}

#[derive(Debug, Args)]
struct ConnectArgs {
    #[arg(long)]
    pipeline: String,
    #[arg(long)]
    namespace: String,
    #[arg(long)]
    from_app: String,
    #[arg(long)]
    to_app: String,
    #[arg(long)]
    contract: String,
}

#[derive(Debug, Args)]
struct ScaleArgs {
    #[arg(long)]
    app: String,
    #[arg(long)]
    replicas: u32,
}

#[derive(Debug, Args)]
struct ObserveArgs {
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
struct ReplayArgs {
    #[arg(long, default_value_t = 50)]
    limit: usize,
    #[arg(long)]
    app: Option<String>,
}

#[derive(Debug, Args)]
struct NodesArgs {
    #[arg(long, default_value_t = 5_000)]
    max_staleness_ms: u64,
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
struct StartArgs {
    #[arg(long)]
    node: String,
    #[arg(long)]
    instance_id: String,
    #[arg(long)]
    module_spec: Option<String>,
    #[arg(long)]
    module: Option<String>,
    #[arg(long, value_enum, default_value_t = AdapterArg::Wasmtime)]
    adapter: AdapterArg,
    #[arg(long, value_enum, default_value_t = IsolationArg::Standard)]
    isolation: IsolationArg,
    #[arg(long = "capability")]
    capabilities: Vec<String>,
}

#[derive(Debug, Args)]
struct StopArgs {
    #[arg(long)]
    node: String,
    #[arg(long)]
    instance_id: String,
}

#[derive(Debug, Args)]
struct ListArgs {
    #[arg(long)]
    node: Option<String>,
}

#[derive(Debug, Args)]
struct AgentArgs {
    #[arg(long, default_value = "local-node")]
    node: String,
    #[arg(long, default_value_t = 1000)]
    interval_ms: u64,
    #[arg(long)]
    once: bool,
    #[arg(long)]
    agent_state: Option<PathBuf>,
}

#[derive(Debug, Subcommand)]
enum IdlCommand {
    Compile(IdlCompileArgs),
    Publish(IdlPublishArgs),
}

#[derive(Debug, Args)]
struct IdlCompileArgs {
    #[arg(long)]
    input: PathBuf,
    #[arg(long)]
    output: PathBuf,
}

#[derive(Debug, Args)]
struct IdlPublishArgs {
    #[arg(long)]
    input: PathBuf,
}

#[derive(Debug, Args)]
struct IdlArgs {
    #[command(subcommand)]
    command: IdlCommand,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum IsolationArg {
    Standard,
    Hardened,
    Microvm,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum AdapterArg {
    Wasmtime,
    Microvm,
}

impl From<IsolationArg> for IsolationProfile {
    fn from(value: IsolationArg) -> Self {
        match value {
            IsolationArg::Standard => IsolationProfile::Standard,
            IsolationArg::Hardened => IsolationProfile::Hardened,
            IsolationArg::Microvm => IsolationProfile::Microvm,
        }
    }
}

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("error: {err:#}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let cli = Cli::parse();

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
                Command::Nodes(args) => cmd_nodes(daemon, args).await,
                Command::Start(args) => cmd_start(daemon, &cli.daemon, args).await,
                Command::Stop(args) => cmd_stop(daemon, &cli.daemon, args).await,
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
    let contracts = args
        .contracts
        .iter()
        .map(|raw| parse_contract_ref(raw))
        .collect::<std::result::Result<Vec<_>, _>>()?;

    cp_mutate(
        &daemon,
        Mutation::UpsertDeployment {
            spec: DeploymentSpec {
                app: args.app,
                module: args.module,
                replicas: args.replicas,
                contracts,
                isolation: args.isolation.into(),
            },
        },
    )
    .await?;

    println!("deployment upserted");
    Ok(())
}

async fn cmd_connect(daemon: Arc<DaemonQuicClient>, args: ConnectArgs) -> Result<()> {
    let mut state = cp_query_state(&daemon, false).await?;
    let contract = parse_contract_ref(&args.contract)?;

    let edge = PipelineEdge {
        from: PipelineEndpoint {
            app: args.from_app,
            contract: contract.clone(),
        },
        to: PipelineEndpoint {
            app: args.to_app,
            contract,
        },
    };

    let pipeline_spec = {
        let pipeline = state
            .pipelines
            .entry(args.pipeline.clone())
            .or_insert(PipelineSpec {
                name: args.pipeline,
                namespace: args.namespace,
                edges: Vec::new(),
            });
        pipeline.edges.push(edge);
        pipeline.clone()
    };

    ensure_pipeline_consistency(&state)
        .with_context(|| "pipeline references unknown deployment or contract")?;

    cp_mutate(
        &daemon,
        Mutation::UpsertPipeline {
            spec: pipeline_spec,
        },
    )
    .await?;

    println!("pipeline edge added");
    Ok(())
}

async fn cmd_scale(daemon: Arc<DaemonQuicClient>, args: ScaleArgs) -> Result<()> {
    cp_mutate(
        &daemon,
        Mutation::SetScale {
            app: args.app.clone(),
            replicas: args.replicas,
        },
    )
    .await?;
    println!("scaled {} to {} replicas", args.app, args.replicas.max(1));
    Ok(())
}

async fn cmd_observe(daemon: Arc<DaemonQuicClient>, args: ObserveArgs) -> Result<()> {
    let _ = args.json;
    let snapshot = cp_query_value(&daemon, Query::ControlPlaneSummary, false).await?;
    println!("{:#?}", snapshot);
    Ok(())
}

async fn cmd_replay(daemon: Arc<DaemonQuicClient>, args: ReplayArgs) -> Result<()> {
    let replay: ReplayApiResponse = daemon
        .request(
            Method::ControlReplay,
            &ReplayApiRequest { limit: args.limit },
        )
        .await?;

    println!("replay events: {}", replay.events.len());
    if let Some(app) = args.app {
        let state = cp_query_state(&daemon, true).await?;
        let contracts = collect_contracts_for_app(&state, &app)
            .into_iter()
            .map(format_contract)
            .collect::<Vec<_>>();
        println!("app contracts: {}", contracts.join(", "));
    }
    println!("{:#?}", replay.events);
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
        println!("{:#?}", nodes);
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

async fn cmd_start(
    daemon: Arc<DaemonQuicClient>,
    conn_args: &DaemonConnectionArgs,
    args: StartArgs,
) -> Result<()> {
    let node_client = node_client(&daemon, conn_args, &args.node).await?;
    let module_spec = if let Some(spec) = args.module_spec {
        spec
    } else {
        let module = args
            .module
            .ok_or_else(|| anyhow!("provide --module-spec or --module"))?;
        build_module_spec(
            &module,
            args.adapter,
            args.isolation,
            if args.capabilities.is_empty() {
                default_capabilities()
            } else {
                args.capabilities
            },
        )
    };

    let response: StartResponse = node_client
        .request(
            Method::StartInstance,
            &StartRequest {
                instance_id: args.instance_id,
                module_spec,
            },
        )
        .await?;

    println!(
        "start status={} instance={} process_id={} already_running={}",
        response.status, response.instance_id, response.process_id, response.already_running
    );
    Ok(())
}

async fn cmd_stop(
    daemon: Arc<DaemonQuicClient>,
    conn_args: &DaemonConnectionArgs,
    args: StopArgs,
) -> Result<()> {
    let node_client = node_client(&daemon, conn_args, &args.node).await?;
    let response: StopResponse = node_client
        .request(
            Method::StopInstance,
            &StopRequest {
                instance_id: args.instance_id,
            },
        )
        .await?;

    println!(
        "stop status={} instance={} process_id={}",
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
        let node_client = node_client(&daemon, conn_args, &node).await?;
        let list: ListResponse = node_client
            .request(Method::ListInstances, &Empty {})
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
            .request(Method::ListInstances, &Empty {})
            .await
            .unwrap_or(ListResponse {
                instances: BTreeMap::new(),
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
        let state = cp_query_state(&daemon, false).await?;
        ensure_pipeline_consistency(&state)?;
        let plan = build_plan(&state)?;

        let actions = reconcile(&args.node, &plan, &agent_state);
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

async fn cp_mutate(daemon: &DaemonQuicClient, mutation: Mutation) -> Result<DataValue> {
    static IDEMPOTENCY_COUNTER: AtomicU64 = AtomicU64::new(1);
    let key = format!(
        "cli-{}-{}",
        unix_ms(),
        IDEMPOTENCY_COUNTER.fetch_add(1, Ordering::Relaxed)
    );

    let response: selium_control_plane_protocol::MutateApiResponse = daemon
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
    let response: selium_control_plane_protocol::QueryApiResponse = daemon
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
            } => {
                let spec = state
                    .deployments
                    .get(deployment)
                    .ok_or_else(|| anyhow!("missing deployment `{deployment}`"))?;
                let module_spec = deployment_module_spec(spec);
                let _ = node_client
                    .request::<_, StartResponse>(
                        Method::StartInstance,
                        &StartRequest {
                            instance_id: instance_id.clone(),
                            module_spec,
                        },
                    )
                    .await?;
            }
            ReconcileAction::Stop { instance_id } => {
                let _ = node_client
                    .request::<_, StopResponse>(
                        Method::StopInstance,
                        &StopRequest {
                            instance_id: instance_id.clone(),
                        },
                    )
                    .await?;
            }
        }
    }

    Ok(())
}

struct DaemonQuicClient {
    endpoint: Endpoint,
    addr: SocketAddr,
    server_name: String,
    connection: Mutex<Option<Connection>>,
    request_id: AtomicU64,
}

impl DaemonQuicClient {
    fn from_args(args: &DaemonConnectionArgs) -> Result<Self> {
        Self::new_from_material(
            parse_daemon_addr(&args.daemon_addr)?,
            args.daemon_server_name.clone(),
            &args.ca_cert,
            &args.client_cert,
            &args.client_key,
        )
    }

    fn new_from_material(
        addr: SocketAddr,
        server_name: String,
        ca_cert: &Path,
        client_cert: &Path,
        client_key: &Path,
    ) -> Result<Self> {
        let bind = if cfg!(target_family = "unix") {
            "0.0.0.0:0"
        } else {
            "127.0.0.1:0"
        }
        .parse::<SocketAddr>()?;

        let mut endpoint = Endpoint::client(bind).context("create QUIC client endpoint")?;
        let roots = load_root_store(ca_cert)?;
        let cert_chain = load_cert_chain(client_cert)?;
        let key = load_private_key(client_key)?;

        let tls = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_client_auth_cert(cert_chain, key)
            .context("build QUIC TLS client config")?;
        let quic_crypto = QuicClientConfig::try_from(tls).context("build QUIC crypto config")?;
        endpoint.set_default_client_config(quinn::ClientConfig::new(Arc::new(quic_crypto)));

        Ok(Self {
            endpoint,
            addr,
            server_name,
            connection: Mutex::new(None),
            request_id: AtomicU64::new(1),
        })
    }

    async fn request<Req, Resp>(&self, method: Method, payload: &Req) -> Result<Resp>
    where
        Req: RkyvEncode,
        Resp: Archive + Sized,
        for<'a> Resp::Archived: rkyv::Deserialize<Resp, HighDeserializer<rkyv::rancor::Error>>
            + rkyv::bytecheck::CheckBytes<HighValidator<'a, rkyv::rancor::Error>>,
    {
        let connection = self.connection().await?;
        let (mut send, mut recv) = connection.open_bi().await.context("open QUIC stream")?;
        let request_id = self.request_id.fetch_add(1, Ordering::Relaxed);
        let frame = encode_request(method, request_id, payload).context("encode request")?;

        write_framed(&mut send, &frame)
            .await
            .context("write request")?;
        let _ = send.finish();

        let frame = read_framed(&mut recv).await.context("read response")?;
        let envelope = decode_envelope(&frame).context("decode response envelope")?;
        if envelope.method != method || envelope.request_id != request_id {
            return Err(anyhow!("daemon response mismatch"));
        }

        if is_error(&envelope) {
            let error = decode_error(&envelope).context("decode daemon error")?;
            return Err(anyhow!("daemon error {}: {}", error.code, error.message));
        }

        decode_payload::<Resp>(&envelope).context("decode daemon payload")
    }

    async fn connection(&self) -> Result<Connection> {
        {
            let guard = self.connection.lock().await;
            if let Some(connection) = guard.as_ref()
                && connection.close_reason().is_none()
            {
                return Ok(connection.clone());
            }
        }

        let connecting = self
            .endpoint
            .connect(self.addr, &self.server_name)
            .context("connect daemon")?;
        let connection = connecting.await.context("await daemon connect")?;
        let mut guard = self.connection.lock().await;
        *guard = Some(connection.clone());
        Ok(connection)
    }

    async fn reset_connection(&self) {
        let mut guard = self.connection.lock().await;
        if let Some(connection) = guard.take() {
            connection.close(0u32.into(), b"reset");
        }
    }
}

fn deployment_module_spec(deployment: &DeploymentSpec) -> String {
    let (adapter, profile) = match deployment.isolation {
        IsolationProfile::Standard => ("wasmtime", "standard"),
        IsolationProfile::Hardened => ("wasmtime", "hardened"),
        IsolationProfile::Microvm => ("microvm", "microvm"),
    };

    build_module_spec(
        &deployment.module,
        match deployment.isolation {
            IsolationProfile::Microvm => AdapterArg::Microvm,
            _ => AdapterArg::Wasmtime,
        },
        match deployment.isolation {
            IsolationProfile::Standard => IsolationArg::Standard,
            IsolationProfile::Hardened => IsolationArg::Hardened,
            IsolationProfile::Microvm => IsolationArg::Microvm,
        },
        default_capabilities(),
    )
    .replace(
        "adapter=wasmtime;profile=standard",
        &format!("adapter={adapter};profile={profile}"),
    )
}

fn build_module_spec(
    module: &str,
    adapter: AdapterArg,
    isolation: IsolationArg,
    capabilities: Vec<String>,
) -> String {
    let adapter = match adapter {
        AdapterArg::Wasmtime => "wasmtime",
        AdapterArg::Microvm => "microvm",
    };
    let profile = match isolation {
        IsolationArg::Standard => "standard",
        IsolationArg::Hardened => "hardened",
        IsolationArg::Microvm => "microvm",
    };

    format!(
        "path={};capabilities={};adapter={};profile={}",
        module,
        capabilities.join(","),
        adapter,
        profile
    )
}

fn default_capabilities() -> Vec<String> {
    vec![
        "session_lifecycle".to_string(),
        "process_lifecycle".to_string(),
        "time_read".to_string(),
        "shared_memory".to_string(),
        "queue_lifecycle".to_string(),
        "queue_writer".to_string(),
        "queue_reader".to_string(),
    ]
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
        "{}/{}@{}",
        contract.namespace, contract.name, contract.version
    )
}

fn parse_daemon_addr(raw: &str) -> Result<SocketAddr> {
    let raw = raw
        .trim()
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .trim_start_matches("quic://")
        .trim_end_matches('/');

    if let Ok(addr) = raw.parse::<SocketAddr>() {
        return Ok(addr);
    }

    raw.to_socket_addrs()?
        .next()
        .ok_or_else(|| anyhow!("no socket addresses for daemon `{raw}`"))
}

fn load_root_store(path: &Path) -> Result<RootCertStore> {
    let pem = fs::read(path).with_context(|| format!("read CA cert {}", path.display()))?;
    let mut reader = std::io::Cursor::new(pem);
    let mut store = RootCertStore::empty();
    for cert in certs(&mut reader) {
        let cert = cert.context("parse CA cert")?;
        store
            .add(cert)
            .map_err(|err| anyhow!("add CA cert to root store: {err}"))?;
    }
    Ok(store)
}

fn load_cert_chain(path: &Path) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>> {
    let pem = fs::read(path).with_context(|| format!("read cert {}", path.display()))?;
    let mut reader = std::io::Cursor::new(pem);
    let mut chain = Vec::new();
    for cert in certs(&mut reader) {
        chain.push(cert.context("parse cert")?);
    }
    if chain.is_empty() {
        return Err(anyhow!("no certs found in {}", path.display()));
    }
    Ok(chain)
}

fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>> {
    let pem = fs::read(path).with_context(|| format!("read key {}", path.display()))?;
    let mut reader = std::io::Cursor::new(pem);
    private_key(&mut reader)
        .context("parse private key")?
        .ok_or_else(|| anyhow!("no private key in {}", path.display()))
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
    fn parse_daemon_addr_accepts_scheme() {
        let addr = parse_daemon_addr("http://127.0.0.1:7100").expect("addr");
        assert_eq!(addr.port(), 7100);
    }

    #[test]
    fn parse_daemon_addr_accepts_raw_socket() {
        let addr = parse_daemon_addr("127.0.0.1:7100").expect("addr");
        assert_eq!(addr.to_string(), "127.0.0.1:7100");
    }

    #[test]
    fn module_spec_contains_expected_capabilities() {
        let spec = build_module_spec(
            "echo.wasm",
            AdapterArg::Wasmtime,
            IsolationArg::Standard,
            default_capabilities(),
        );
        assert!(spec.contains("path=echo.wasm"));
        assert!(spec.contains("adapter=wasmtime"));
        assert!(spec.contains("queue_writer"));
    }
}
