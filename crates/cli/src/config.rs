use std::{
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, anyhow};
use clap::{
    Args, CommandFactory, FromArgMatches, Parser, Subcommand, ValueEnum, parser::ValueSource,
};
use selium_module_control_plane::api::IsolationProfile;
use serde::Deserialize;

#[derive(Debug, Parser)]
#[command(name = "selium", about = "Selium platform CLI")]
pub(crate) struct Cli {
    #[command(flatten)]
    pub(crate) daemon: DaemonConnectionArgs,
    #[command(subcommand)]
    pub(crate) command: Command,
}

#[derive(Debug, Args, Clone)]
pub(crate) struct DaemonConnectionArgs {
    #[arg(long, default_value = "127.0.0.1:7100")]
    pub(crate) daemon_addr: String,
    #[arg(long, default_value = "localhost")]
    pub(crate) daemon_server_name: String,
    #[arg(long, default_value = "certs/ca.crt")]
    pub(crate) ca_cert: PathBuf,
    #[arg(long, default_value = "certs/client.crt")]
    pub(crate) client_cert: PathBuf,
    #[arg(long, default_value = "certs/client.key")]
    pub(crate) client_key: PathBuf,
}

#[derive(Debug, Subcommand)]
pub(crate) enum Command {
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
pub(crate) struct DeployArgs {
    #[arg(long)]
    pub(crate) app: String,
    #[arg(long)]
    pub(crate) module: String,
    #[arg(long, default_value_t = 1)]
    pub(crate) replicas: u32,
    #[arg(long, value_enum, default_value_t = IsolationArg::Standard)]
    pub(crate) isolation: IsolationArg,
    #[arg(long = "contract")]
    pub(crate) contracts: Vec<String>,
}

#[derive(Debug, Args)]
pub(crate) struct ConnectArgs {
    #[arg(long)]
    pub(crate) pipeline: String,
    #[arg(long)]
    pub(crate) namespace: String,
    #[arg(long)]
    pub(crate) from_app: String,
    #[arg(long)]
    pub(crate) to_app: String,
    #[arg(long)]
    pub(crate) contract: String,
}

#[derive(Debug, Args)]
pub(crate) struct ScaleArgs {
    #[arg(long)]
    pub(crate) app: String,
    #[arg(long)]
    pub(crate) replicas: u32,
}

#[derive(Debug, Args)]
pub(crate) struct ObserveArgs {
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Debug, Args)]
pub(crate) struct ReplayArgs {
    #[arg(long, default_value_t = 50)]
    pub(crate) limit: usize,
    #[arg(long)]
    pub(crate) app: Option<String>,
}

#[derive(Debug, Args)]
pub(crate) struct NodesArgs {
    #[arg(long, default_value_t = 5_000)]
    pub(crate) max_staleness_ms: u64,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Debug, Args)]
pub(crate) struct StartArgs {
    #[arg(long)]
    pub(crate) node: String,
    #[arg(long)]
    pub(crate) instance_id: String,
    #[arg(long)]
    pub(crate) module_spec: Option<String>,
    #[arg(long)]
    pub(crate) module: Option<String>,
    #[arg(long = "adaptor", value_enum, default_value_t = AdaptorArg::Wasmtime)]
    pub(crate) adaptor: AdaptorArg,
    #[arg(long, value_enum, default_value_t = IsolationArg::Standard)]
    pub(crate) isolation: IsolationArg,
    #[arg(long = "capability")]
    pub(crate) capabilities: Vec<String>,
}

#[derive(Debug, Args)]
pub(crate) struct StopArgs {
    #[arg(long)]
    pub(crate) node: String,
    #[arg(long)]
    pub(crate) instance_id: String,
}

#[derive(Debug, Args)]
pub(crate) struct ListArgs {
    #[arg(long)]
    pub(crate) node: Option<String>,
}

#[derive(Debug, Args)]
pub(crate) struct AgentArgs {
    #[arg(long, default_value = "local-node")]
    pub(crate) node: String,
    #[arg(long, default_value_t = 1000)]
    pub(crate) interval_ms: u64,
    #[arg(long)]
    pub(crate) once: bool,
    #[arg(long)]
    pub(crate) agent_state: Option<PathBuf>,
}

#[derive(Debug, Subcommand)]
pub(crate) enum IdlCommand {
    Compile(IdlCompileArgs),
    Publish(IdlPublishArgs),
}

#[derive(Debug, Args)]
pub(crate) struct IdlCompileArgs {
    #[arg(long)]
    pub(crate) input: PathBuf,
    #[arg(long)]
    pub(crate) output: PathBuf,
}

#[derive(Debug, Args)]
pub(crate) struct IdlPublishArgs {
    #[arg(long)]
    pub(crate) input: PathBuf,
}

#[derive(Debug, Args)]
pub(crate) struct IdlArgs {
    #[command(subcommand)]
    pub(crate) command: IdlCommand,
}

#[derive(Debug, Clone, Copy, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub(crate) enum IsolationArg {
    Standard,
    Hardened,
    Microvm,
}

#[derive(Debug, Clone, Copy, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub(crate) enum AdaptorArg {
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
    #[arg(long = "adaptor", value_enum)]
    adaptor: Option<AdaptorArg>,
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

#[derive(Debug, Args, Default, Deserialize)]
#[serde(rename_all = "kebab-case", default)]
struct RawIdlArgs {
    #[command(subcommand)]
    command: RawIdlCommand,
}

#[derive(Debug, Clone, Deserialize, Subcommand)]
#[serde(rename_all = "kebab-case")]
enum RawIdlCommand {
    Compile(RawIdlCompileArgs),
    Publish(RawIdlPublishArgs),
}

impl Default for RawIdlCommand {
    fn default() -> Self {
        Self::Compile(RawIdlCompileArgs::default())
    }
}

#[derive(Debug, Args, Clone, Default, Deserialize)]
#[serde(rename_all = "kebab-case", default)]
struct RawIdlCompileArgs {
    #[arg(long)]
    input: Option<PathBuf>,
    #[arg(long)]
    output: Option<PathBuf>,
}

#[derive(Debug, Args, Clone, Default, Deserialize)]
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

pub(crate) fn load_cli() -> Result<Cli> {
    load_cli_from(std::env::args_os())
}

pub(crate) fn load_cli_from<I, T>(args: I) -> Result<Cli>
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
            adaptor: self.adaptor.unwrap_or(AdaptorArg::Wasmtime),
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
    merge_option(&mut args.adaptor, value_source("adaptor"), config.adaptor);
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
