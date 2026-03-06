use std::{
    collections::BTreeMap,
    ffi::OsString,
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
use clap::{
    Args, CommandFactory, FromArgMatches, Parser, Subcommand, ValueEnum, parser::ValueSource,
};
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
use serde::Deserialize;
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

#[derive(Debug, Clone, Copy, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
enum IsolationArg {
    Standard,
    Hardened,
    Microvm,
}

#[derive(Debug, Clone, Copy, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
enum AdapterArg {
    Wasmtime,
    Microvm,
}

#[derive(Debug, Parser)]
#[command(name = "selium", about = "Selium platform CLI")]
struct RawCli {
    #[arg(short = 'c', long, global = true, value_name = "FILE")]
    config: Option<PathBuf>,
    #[command(flatten)]
    daemon: RawDaemonConnectionArgs,
    #[command(subcommand)]
    command: RawCommand,
}

#[derive(Debug, Args, Clone, Default, Deserialize)]
#[serde(rename_all = "kebab-case", default)]
struct RawDaemonConnectionArgs {
    #[arg(long)]
    daemon_addr: Option<String>,
    #[arg(long)]
    daemon_server_name: Option<String>,
    #[arg(long)]
    ca_cert: Option<PathBuf>,
    #[arg(long)]
    client_cert: Option<PathBuf>,
    #[arg(long)]
    client_key: Option<PathBuf>,
}

#[derive(Debug, Subcommand)]
enum RawCommand {
    Deploy(RawDeployArgs),
    Connect(RawConnectArgs),
    Scale(RawScaleArgs),
    Observe(RawObserveArgs),
    Replay(RawReplayArgs),
    Nodes(RawNodesArgs),
    Start(RawStartArgs),
    Stop(RawStopArgs),
    List(RawListArgs),
    Agent(RawAgentArgs),
    Idl(RawIdlArgs),
}

#[derive(Debug, Args, Default, Deserialize)]
#[serde(rename_all = "kebab-case", default)]
struct RawDeployArgs {
    #[arg(long)]
    app: Option<String>,
    #[arg(long)]
    module: Option<String>,
    #[arg(long)]
    replicas: Option<u32>,
    #[arg(long, value_enum)]
    isolation: Option<IsolationArg>,
    #[arg(long = "contract")]
    contracts: Vec<String>,
}

#[derive(Debug, Args, Default, Deserialize)]
#[serde(rename_all = "kebab-case", default)]
struct RawConnectArgs {
    #[arg(long)]
    pipeline: Option<String>,
    #[arg(long)]
    namespace: Option<String>,
    #[arg(long)]
    from_app: Option<String>,
    #[arg(long)]
    to_app: Option<String>,
    #[arg(long)]
    contract: Option<String>,
}

#[derive(Debug, Args, Default, Deserialize)]
#[serde(rename_all = "kebab-case", default)]
struct RawScaleArgs {
    #[arg(long)]
    app: Option<String>,
    #[arg(long)]
    replicas: Option<u32>,
}

#[derive(Debug, Args, Default, Deserialize)]
#[serde(rename_all = "kebab-case", default)]
struct RawObserveArgs {
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args, Default, Deserialize)]
#[serde(rename_all = "kebab-case", default)]
struct RawReplayArgs {
    #[arg(long)]
    limit: Option<usize>,
    #[arg(long)]
    app: Option<String>,
}

#[derive(Debug, Args, Default, Deserialize)]
#[serde(rename_all = "kebab-case", default)]
struct RawNodesArgs {
    #[arg(long)]
    max_staleness_ms: Option<u64>,
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args, Default, Deserialize)]
#[serde(rename_all = "kebab-case", default)]
struct RawStartArgs {
    #[arg(long)]
    node: Option<String>,
    #[arg(long)]
    instance_id: Option<String>,
    #[arg(long)]
    module_spec: Option<String>,
    #[arg(long)]
    module: Option<String>,
    #[arg(long, value_enum)]
    adapter: Option<AdapterArg>,
    #[arg(long, value_enum)]
    isolation: Option<IsolationArg>,
    #[arg(long = "capability")]
    capabilities: Vec<String>,
}

#[derive(Debug, Args, Default, Deserialize)]
#[serde(rename_all = "kebab-case", default)]
struct RawStopArgs {
    #[arg(long)]
    node: Option<String>,
    #[arg(long)]
    instance_id: Option<String>,
}

#[derive(Debug, Args, Default, Deserialize)]
#[serde(rename_all = "kebab-case", default)]
struct RawListArgs {
    #[arg(long)]
    node: Option<String>,
}

#[derive(Debug, Args, Default, Deserialize)]
#[serde(rename_all = "kebab-case", default)]
struct RawAgentArgs {
    #[arg(long)]
    node: Option<String>,
    #[arg(long)]
    interval_ms: Option<u64>,
    #[arg(long)]
    once: bool,
    #[arg(long)]
    agent_state: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct RawIdlArgs {
    #[command(subcommand)]
    command: RawIdlCommand,
}

#[derive(Debug, Subcommand)]
enum RawIdlCommand {
    Compile(RawIdlCompileArgs),
    Publish(RawIdlPublishArgs),
}

#[derive(Debug, Args, Default, Deserialize)]
#[serde(rename_all = "kebab-case", default)]
struct RawIdlCompileArgs {
    #[arg(long)]
    input: Option<PathBuf>,
    #[arg(long)]
    output: Option<PathBuf>,
}

#[derive(Debug, Args, Default, Deserialize)]
#[serde(rename_all = "kebab-case", default)]
struct RawIdlPublishArgs {
    #[arg(long)]
    input: Option<PathBuf>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "kebab-case", default)]
struct CliConfig {
    daemon: Option<RawDaemonConnectionArgs>,
    deploy: Option<RawDeployArgs>,
    connect: Option<RawConnectArgs>,
    scale: Option<RawScaleArgs>,
    observe: Option<RawObserveArgs>,
    replay: Option<RawReplayArgs>,
    nodes: Option<RawNodesArgs>,
    start: Option<RawStartArgs>,
    stop: Option<RawStopArgs>,
    list: Option<RawListArgs>,
    agent: Option<RawAgentArgs>,
    idl: Option<RawIdlConfig>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "kebab-case", default)]
struct RawIdlConfig {
    compile: Option<RawIdlCompileArgs>,
    publish: Option<RawIdlPublishArgs>,
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

fn load_cli() -> Result<Cli> {
    load_cli_from(std::env::args_os())
}

fn load_cli_from<I, T>(args: I) -> Result<Cli>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let matches = RawCli::command().get_matches_from(args);
    let mut raw =
        RawCli::from_arg_matches(&matches).map_err(|err| anyhow!("parse CLI arguments: {err}"))?;

    if let Some(path) = raw.config.clone() {
        let config: CliConfig = load_toml_config(&path)?;
        merge_cli_config(&mut raw, &matches, config);
    }

    raw.resolve()
}

impl RawCli {
    fn resolve(self) -> Result<Cli> {
        Ok(Cli {
            daemon: self.daemon.resolve(),
            command: self.command.resolve()?,
        })
    }
}

impl RawDaemonConnectionArgs {
    fn resolve(self) -> DaemonConnectionArgs {
        DaemonConnectionArgs {
            daemon_addr: self
                .daemon_addr
                .unwrap_or_else(|| "127.0.0.1:7100".to_string()),
            daemon_server_name: self
                .daemon_server_name
                .unwrap_or_else(|| "localhost".to_string()),
            ca_cert: self
                .ca_cert
                .unwrap_or_else(|| PathBuf::from("certs/ca.crt")),
            client_cert: self
                .client_cert
                .unwrap_or_else(|| PathBuf::from("certs/client.crt")),
            client_key: self
                .client_key
                .unwrap_or_else(|| PathBuf::from("certs/client.key")),
        }
    }
}

impl RawCommand {
    fn resolve(self) -> Result<Command> {
        Ok(match self {
            RawCommand::Deploy(args) => Command::Deploy(args.resolve()?),
            RawCommand::Connect(args) => Command::Connect(args.resolve()?),
            RawCommand::Scale(args) => Command::Scale(args.resolve()?),
            RawCommand::Observe(args) => Command::Observe(args.resolve()),
            RawCommand::Replay(args) => Command::Replay(args.resolve()),
            RawCommand::Nodes(args) => Command::Nodes(args.resolve()),
            RawCommand::Start(args) => Command::Start(args.resolve()?),
            RawCommand::Stop(args) => Command::Stop(args.resolve()?),
            RawCommand::List(args) => Command::List(args.resolve()),
            RawCommand::Agent(args) => Command::Agent(args.resolve()),
            RawCommand::Idl(args) => Command::Idl(args.resolve()?),
        })
    }
}

impl RawDeployArgs {
    fn resolve(self) -> Result<DeployArgs> {
        Ok(DeployArgs {
            app: required_arg("deploy.app", self.app)?,
            module: required_arg("deploy.module", self.module)?,
            replicas: self.replicas.unwrap_or(1),
            isolation: self.isolation.unwrap_or(IsolationArg::Standard),
            contracts: self.contracts,
        })
    }
}

impl RawConnectArgs {
    fn resolve(self) -> Result<ConnectArgs> {
        Ok(ConnectArgs {
            pipeline: required_arg("connect.pipeline", self.pipeline)?,
            namespace: required_arg("connect.namespace", self.namespace)?,
            from_app: required_arg("connect.from-app", self.from_app)?,
            to_app: required_arg("connect.to-app", self.to_app)?,
            contract: required_arg("connect.contract", self.contract)?,
        })
    }
}

impl RawScaleArgs {
    fn resolve(self) -> Result<ScaleArgs> {
        Ok(ScaleArgs {
            app: required_arg("scale.app", self.app)?,
            replicas: required_arg("scale.replicas", self.replicas)?,
        })
    }
}

impl RawObserveArgs {
    fn resolve(self) -> ObserveArgs {
        ObserveArgs { json: self.json }
    }
}

impl RawReplayArgs {
    fn resolve(self) -> ReplayArgs {
        ReplayArgs {
            limit: self.limit.unwrap_or(50),
            app: self.app,
        }
    }
}

impl RawNodesArgs {
    fn resolve(self) -> NodesArgs {
        NodesArgs {
            max_staleness_ms: self.max_staleness_ms.unwrap_or(5_000),
            json: self.json,
        }
    }
}

impl RawStartArgs {
    fn resolve(self) -> Result<StartArgs> {
        Ok(StartArgs {
            node: required_arg("start.node", self.node)?,
            instance_id: required_arg("start.instance-id", self.instance_id)?,
            module_spec: self.module_spec,
            module: self.module,
            adapter: self.adapter.unwrap_or(AdapterArg::Wasmtime),
            isolation: self.isolation.unwrap_or(IsolationArg::Standard),
            capabilities: self.capabilities,
        })
    }
}

impl RawStopArgs {
    fn resolve(self) -> Result<StopArgs> {
        Ok(StopArgs {
            node: required_arg("stop.node", self.node)?,
            instance_id: required_arg("stop.instance-id", self.instance_id)?,
        })
    }
}

impl RawListArgs {
    fn resolve(self) -> ListArgs {
        ListArgs { node: self.node }
    }
}

impl RawAgentArgs {
    fn resolve(self) -> AgentArgs {
        AgentArgs {
            node: self.node.unwrap_or_else(|| "local-node".to_string()),
            interval_ms: self.interval_ms.unwrap_or(1000),
            once: self.once,
            agent_state: self.agent_state,
        }
    }
}

impl RawIdlArgs {
    fn resolve(self) -> Result<IdlArgs> {
        Ok(IdlArgs {
            command: self.command.resolve()?,
        })
    }
}

impl RawIdlCommand {
    fn resolve(self) -> Result<IdlCommand> {
        Ok(match self {
            RawIdlCommand::Compile(args) => IdlCommand::Compile(args.resolve()?),
            RawIdlCommand::Publish(args) => IdlCommand::Publish(args.resolve()?),
        })
    }
}

impl RawIdlCompileArgs {
    fn resolve(self) -> Result<IdlCompileArgs> {
        Ok(IdlCompileArgs {
            input: required_arg("idl.compile.input", self.input)?,
            output: required_arg("idl.compile.output", self.output)?,
        })
    }
}

impl RawIdlPublishArgs {
    fn resolve(self) -> Result<IdlPublishArgs> {
        Ok(IdlPublishArgs {
            input: required_arg("idl.publish.input", self.input)?,
        })
    }
}

fn merge_cli_config(raw: &mut RawCli, matches: &clap::ArgMatches, config: CliConfig) {
    let CliConfig {
        daemon,
        deploy,
        connect,
        scale,
        observe,
        replay,
        nodes,
        start,
        stop,
        list,
        agent,
        idl,
    } = config;

    if let Some(daemon) = daemon {
        merge_daemon_connection_config(&mut raw.daemon, matches, daemon);
    }

    match (
        &mut raw.command,
        deploy,
        connect,
        scale,
        observe,
        replay,
        nodes,
        start,
        stop,
        list,
        agent,
        idl,
    ) {
        (RawCommand::Deploy(args), Some(cfg), _, _, _, _, _, _, _, _, _, _) => {
            merge_deploy_config(args, matches.subcommand_matches("deploy"), cfg);
        }
        (RawCommand::Connect(args), _, Some(cfg), _, _, _, _, _, _, _, _, _) => {
            merge_connect_config(args, matches.subcommand_matches("connect"), cfg);
        }
        (RawCommand::Scale(args), _, _, Some(cfg), _, _, _, _, _, _, _, _) => {
            merge_scale_config(args, matches.subcommand_matches("scale"), cfg);
        }
        (RawCommand::Observe(args), _, _, _, Some(cfg), _, _, _, _, _, _, _) => {
            merge_observe_config(args, matches.subcommand_matches("observe"), cfg);
        }
        (RawCommand::Replay(args), _, _, _, _, Some(cfg), _, _, _, _, _, _) => {
            merge_replay_config(args, matches.subcommand_matches("replay"), cfg);
        }
        (RawCommand::Nodes(args), _, _, _, _, _, Some(cfg), _, _, _, _, _) => {
            merge_nodes_config(args, matches.subcommand_matches("nodes"), cfg);
        }
        (RawCommand::Start(args), _, _, _, _, _, _, Some(cfg), _, _, _, _) => {
            merge_start_config(args, matches.subcommand_matches("start"), cfg);
        }
        (RawCommand::Stop(args), _, _, _, _, _, _, _, Some(cfg), _, _, _) => {
            merge_stop_config(args, matches.subcommand_matches("stop"), cfg);
        }
        (RawCommand::List(args), _, _, _, _, _, _, _, _, Some(cfg), _, _) => {
            merge_list_config(args, matches.subcommand_matches("list"), cfg);
        }
        (RawCommand::Agent(args), _, _, _, _, _, _, _, _, _, Some(cfg), _) => {
            merge_agent_config(args, matches.subcommand_matches("agent"), cfg);
        }
        (RawCommand::Idl(args), _, _, _, _, _, _, _, _, _, _, Some(cfg)) => {
            merge_idl_config(args, matches.subcommand_matches("idl"), cfg);
        }
        _ => {}
    }
}

fn merge_daemon_connection_config(
    args: &mut RawDaemonConnectionArgs,
    matches: &clap::ArgMatches,
    config: RawDaemonConnectionArgs,
) {
    merge_option(
        &mut args.daemon_addr,
        matches.value_source("daemon_addr"),
        config.daemon_addr,
    );
    merge_option(
        &mut args.daemon_server_name,
        matches.value_source("daemon_server_name"),
        config.daemon_server_name,
    );
    merge_option(
        &mut args.ca_cert,
        matches.value_source("ca_cert"),
        config.ca_cert,
    );
    merge_option(
        &mut args.client_cert,
        matches.value_source("client_cert"),
        config.client_cert,
    );
    merge_option(
        &mut args.client_key,
        matches.value_source("client_key"),
        config.client_key,
    );
}

fn merge_deploy_config(
    args: &mut RawDeployArgs,
    matches: Option<&clap::ArgMatches>,
    config: RawDeployArgs,
) {
    let value_source = |name| matches.and_then(|m| m.value_source(name));
    merge_option(&mut args.app, value_source("app"), config.app);
    merge_option(&mut args.module, value_source("module"), config.module);
    merge_option(
        &mut args.replicas,
        value_source("replicas"),
        config.replicas,
    );
    merge_option(
        &mut args.isolation,
        value_source("isolation"),
        config.isolation,
    );
    merge_vec(
        &mut args.contracts,
        value_source("contracts"),
        config.contracts,
    );
}

fn merge_connect_config(
    args: &mut RawConnectArgs,
    matches: Option<&clap::ArgMatches>,
    config: RawConnectArgs,
) {
    let value_source = |name| matches.and_then(|m| m.value_source(name));
    merge_option(
        &mut args.pipeline,
        value_source("pipeline"),
        config.pipeline,
    );
    merge_option(
        &mut args.namespace,
        value_source("namespace"),
        config.namespace,
    );
    merge_option(
        &mut args.from_app,
        value_source("from_app"),
        config.from_app,
    );
    merge_option(&mut args.to_app, value_source("to_app"), config.to_app);
    merge_option(
        &mut args.contract,
        value_source("contract"),
        config.contract,
    );
}

fn merge_scale_config(
    args: &mut RawScaleArgs,
    matches: Option<&clap::ArgMatches>,
    config: RawScaleArgs,
) {
    let value_source = |name| matches.and_then(|m| m.value_source(name));
    merge_option(&mut args.app, value_source("app"), config.app);
    merge_option(
        &mut args.replicas,
        value_source("replicas"),
        config.replicas,
    );
}

fn merge_observe_config(
    args: &mut RawObserveArgs,
    matches: Option<&clap::ArgMatches>,
    config: RawObserveArgs,
) {
    merge_bool(
        &mut args.json,
        matches.and_then(|m| m.value_source("json")),
        config.json,
    );
}

fn merge_replay_config(
    args: &mut RawReplayArgs,
    matches: Option<&clap::ArgMatches>,
    config: RawReplayArgs,
) {
    let value_source = |name| matches.and_then(|m| m.value_source(name));
    merge_option(&mut args.limit, value_source("limit"), config.limit);
    merge_option(&mut args.app, value_source("app"), config.app);
}

fn merge_nodes_config(
    args: &mut RawNodesArgs,
    matches: Option<&clap::ArgMatches>,
    config: RawNodesArgs,
) {
    let value_source = |name| matches.and_then(|m| m.value_source(name));
    merge_option(
        &mut args.max_staleness_ms,
        value_source("max_staleness_ms"),
        config.max_staleness_ms,
    );
    merge_bool(&mut args.json, value_source("json"), config.json);
}

fn merge_start_config(
    args: &mut RawStartArgs,
    matches: Option<&clap::ArgMatches>,
    config: RawStartArgs,
) {
    let value_source = |name| matches.and_then(|m| m.value_source(name));
    merge_option(&mut args.node, value_source("node"), config.node);
    merge_option(
        &mut args.instance_id,
        value_source("instance_id"),
        config.instance_id,
    );
    merge_option(
        &mut args.module_spec,
        value_source("module_spec"),
        config.module_spec,
    );
    merge_option(&mut args.module, value_source("module"), config.module);
    merge_option(&mut args.adapter, value_source("adapter"), config.adapter);
    merge_option(
        &mut args.isolation,
        value_source("isolation"),
        config.isolation,
    );
    merge_vec(
        &mut args.capabilities,
        value_source("capabilities"),
        config.capabilities,
    );
}

fn merge_stop_config(
    args: &mut RawStopArgs,
    matches: Option<&clap::ArgMatches>,
    config: RawStopArgs,
) {
    let value_source = |name| matches.and_then(|m| m.value_source(name));
    merge_option(&mut args.node, value_source("node"), config.node);
    merge_option(
        &mut args.instance_id,
        value_source("instance_id"),
        config.instance_id,
    );
}

fn merge_list_config(
    args: &mut RawListArgs,
    matches: Option<&clap::ArgMatches>,
    config: RawListArgs,
) {
    merge_option(
        &mut args.node,
        matches.and_then(|m| m.value_source("node")),
        config.node,
    );
}

fn merge_agent_config(
    args: &mut RawAgentArgs,
    matches: Option<&clap::ArgMatches>,
    config: RawAgentArgs,
) {
    let value_source = |name| matches.and_then(|m| m.value_source(name));
    merge_option(&mut args.node, value_source("node"), config.node);
    merge_option(
        &mut args.interval_ms,
        value_source("interval_ms"),
        config.interval_ms,
    );
    merge_bool(&mut args.once, value_source("once"), config.once);
    merge_option(
        &mut args.agent_state,
        value_source("agent_state"),
        config.agent_state,
    );
}

fn merge_idl_config(
    args: &mut RawIdlArgs,
    matches: Option<&clap::ArgMatches>,
    config: RawIdlConfig,
) {
    match (&mut args.command, config) {
        (
            RawIdlCommand::Compile(args),
            RawIdlConfig {
                compile: Some(cfg), ..
            },
        ) => {
            let compile_matches = matches.and_then(|m| m.subcommand_matches("compile"));
            merge_idl_compile_config(args, compile_matches, cfg);
        }
        (
            RawIdlCommand::Publish(args),
            RawIdlConfig {
                publish: Some(cfg), ..
            },
        ) => {
            let publish_matches = matches.and_then(|m| m.subcommand_matches("publish"));
            merge_idl_publish_config(args, publish_matches, cfg);
        }
        _ => {}
    }
}

fn merge_idl_compile_config(
    args: &mut RawIdlCompileArgs,
    matches: Option<&clap::ArgMatches>,
    config: RawIdlCompileArgs,
) {
    let value_source = |name| matches.and_then(|m| m.value_source(name));
    merge_option(&mut args.input, value_source("input"), config.input);
    merge_option(&mut args.output, value_source("output"), config.output);
}

fn merge_idl_publish_config(
    args: &mut RawIdlPublishArgs,
    matches: Option<&clap::ArgMatches>,
    config: RawIdlPublishArgs,
) {
    merge_option(
        &mut args.input,
        matches.and_then(|m| m.value_source("input")),
        config.input,
    );
}

fn merge_option<T>(slot: &mut Option<T>, source: Option<ValueSource>, config: Option<T>) {
    if should_apply_config(source)
        && let Some(config) = config
    {
        *slot = Some(config);
    }
}

fn merge_vec<T>(slot: &mut Vec<T>, source: Option<ValueSource>, config: Vec<T>) {
    if should_apply_config(source) && !config.is_empty() {
        *slot = config;
    }
}

fn merge_bool(slot: &mut bool, source: Option<ValueSource>, config: bool) {
    if should_apply_config(source) && config {
        *slot = true;
    }
}

fn should_apply_config(source: Option<ValueSource>) -> bool {
    !matches!(
        source,
        Some(ValueSource::CommandLine | ValueSource::EnvVariable)
    )
}

fn required_arg<T>(name: &str, value: Option<T>) -> Result<T> {
    value.ok_or_else(|| anyhow!("missing required configuration for `{name}`"))
}

fn load_toml_config<T>(path: &Path) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    let raw =
        fs::read_to_string(path).with_context(|| format!("read config file {}", path.display()))?;
    toml::from_str(&raw).with_context(|| format!("parse TOML config {}", path.display()))
}

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
    use std::time::{SystemTime, UNIX_EPOCH};

    fn write_test_config(contents: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "selium-cli-config-{}.toml",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        fs::write(&path, contents).expect("write config");
        path
    }

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

    #[test]
    fn cli_config_supplies_required_command_args() {
        let config = write_test_config(
            r#"
[daemon]
daemon-addr = "127.0.0.1:7999"

[deploy]
app = "from-config"
module = "module.wasm"
replicas = 3
"#,
        );

        let cli = load_cli_from([
            "selium",
            "--config",
            config.to_str().expect("config path"),
            "deploy",
        ])
        .expect("parse cli");

        assert_eq!(cli.daemon.daemon_addr, "127.0.0.1:7999");
        let Command::Deploy(args) = cli.command else {
            panic!("expected deploy command");
        };
        assert_eq!(args.app, "from-config");
        assert_eq!(args.module, "module.wasm");
        assert_eq!(args.replicas, 3);
    }

    #[test]
    fn cli_command_line_overrides_config() {
        let config = write_test_config(
            r#"
[deploy]
app = "from-config"
module = "module.wasm"
replicas = 3
"#,
        );

        let cli = load_cli_from([
            "selium",
            "--config",
            config.to_str().expect("config path"),
            "deploy",
            "--app",
            "from-cli",
        ])
        .expect("parse cli");

        let Command::Deploy(args) = cli.command else {
            panic!("expected deploy command");
        };
        assert_eq!(args.app, "from-cli");
        assert_eq!(args.module, "module.wasm");
        assert_eq!(args.replicas, 3);
    }
}
