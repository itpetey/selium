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
    Discover(DiscoverArgs),
    Inventory(InventoryArgs),
    Usage(UsageArgs),
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
    pub(crate) tenant: String,
    #[arg(long)]
    pub(crate) namespace: String,
    #[arg(long, alias = "app")]
    pub(crate) workload: String,
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
    pub(crate) tenant: String,
    #[arg(long)]
    pub(crate) namespace: String,
    #[arg(long, alias = "from-app")]
    pub(crate) from_workload: String,
    #[arg(long, alias = "to-app")]
    pub(crate) to_workload: String,
    #[arg(long)]
    pub(crate) endpoint: String,
    #[arg(long)]
    pub(crate) contract: String,
}

#[derive(Debug, Args)]
pub(crate) struct ScaleArgs {
    #[arg(long)]
    pub(crate) tenant: String,
    #[arg(long)]
    pub(crate) namespace: String,
    #[arg(long, alias = "app")]
    pub(crate) workload: String,
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
    pub(crate) since_sequence: Option<u64>,
    #[arg(long)]
    pub(crate) external_account_ref: Option<String>,
    #[arg(long)]
    pub(crate) workload_key: Option<String>,
    #[arg(long)]
    pub(crate) tenant: Option<String>,
    #[arg(long)]
    pub(crate) namespace: Option<String>,
    #[arg(long, alias = "app")]
    pub(crate) workload: Option<String>,
    #[arg(long)]
    pub(crate) module: Option<String>,
    #[arg(long)]
    pub(crate) pipeline: Option<String>,
    #[arg(long)]
    pub(crate) node: Option<String>,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Debug, Args)]
pub(crate) struct DiscoverArgs {
    #[arg(long = "workload")]
    pub(crate) workloads: Vec<String>,
    #[arg(long = "workload-prefix")]
    pub(crate) workload_prefixes: Vec<String>,
    #[arg(long)]
    pub(crate) resolve_workload: Option<String>,
    #[arg(long)]
    pub(crate) resolve_endpoint: Option<String>,
    #[arg(long)]
    pub(crate) resolve_running_workload: Option<String>,
    #[arg(long)]
    pub(crate) resolve_replica_key: Option<String>,
    #[arg(long)]
    pub(crate) allow_operational_processes: bool,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Debug, Args)]
pub(crate) struct InventoryArgs {
    #[arg(long)]
    pub(crate) external_account_ref: Option<String>,
    #[arg(long)]
    pub(crate) workload: Option<String>,
    #[arg(long)]
    pub(crate) module: Option<String>,
    #[arg(long)]
    pub(crate) pipeline: Option<String>,
    #[arg(long)]
    pub(crate) node: Option<String>,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Debug, Args)]
pub(crate) struct UsageArgs {
    #[arg(long)]
    pub(crate) node: String,
    #[arg(long, default_value_t = 50)]
    pub(crate) limit: usize,
    #[arg(long)]
    pub(crate) latest: bool,
    #[arg(long)]
    pub(crate) since_sequence: Option<u64>,
    #[arg(long)]
    pub(crate) since_timestamp_ms: Option<u64>,
    #[arg(long)]
    pub(crate) external_account_ref: Option<String>,
    #[arg(long)]
    pub(crate) workload: Option<String>,
    #[arg(long)]
    pub(crate) module: Option<String>,
    #[arg(long)]
    pub(crate) window_start_ms: Option<u64>,
    #[arg(long)]
    pub(crate) window_end_ms: Option<u64>,
    #[arg(long)]
    pub(crate) json: bool,
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
    #[arg(long = "replica-key")]
    pub(crate) replica_key: String,
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
    #[arg(long = "event-reader")]
    pub(crate) event_readers: Vec<String>,
    #[arg(long = "event-writer")]
    pub(crate) event_writers: Vec<String>,
}

#[derive(Debug, Args)]
pub(crate) struct StopArgs {
    #[arg(long)]
    pub(crate) node: String,
    #[arg(long = "replica-key")]
    pub(crate) replica_key: String,
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
    /// Use the default sandboxing profile.
    Standard,
    /// Use the hardened profile with stricter isolation.
    Hardened,
    /// Run inside a microVM-backed isolation boundary.
    Microvm,
}

#[derive(Debug, Clone, Copy, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub(crate) enum AdaptorArg {
    /// Run the module with the Wasmtime adaptor.
    Wasmtime,
    /// Run the module with the microVM adaptor.
    Microvm,
}

/// Manage Selium control-plane resources and node instances.
///
/// Most command options can be loaded from a TOML file with `--config` and then
/// overridden on the command line.
#[derive(Debug, Parser)]
#[command(
    name = "selium",
    about = "Manage Selium control-plane resources and node instances."
)]
struct RawCli {
    /// Load default option values from a TOML configuration file.
    #[arg(short = 'c', long, global = true, value_name = "FILE")]
    config: Option<PathBuf>,
    #[command(flatten)]
    daemon: RawDaemonConnectionArgs,
    #[command(subcommand)]
    command: RawCommand,
}

/// Connection settings used when talking to a Selium daemon over QUIC.
#[derive(Debug, Args, Clone, Default, Deserialize)]
#[serde(rename_all = "kebab-case", default)]
struct RawDaemonConnectionArgs {
    /// Daemon address in `HOST:PORT` form.
    #[arg(long, value_name = "HOST:PORT")]
    daemon_addr: Option<String>,
    /// TLS server name expected from the daemon certificate.
    #[arg(long, value_name = "NAME")]
    daemon_server_name: Option<String>,
    /// Path to the CA certificate used to verify the daemon.
    #[arg(long, value_name = "PATH")]
    ca_cert: Option<PathBuf>,
    /// Path to the client certificate presented to the daemon.
    #[arg(long, value_name = "PATH")]
    client_cert: Option<PathBuf>,
    /// Path to the client private key presented to the daemon.
    #[arg(long, value_name = "PATH")]
    client_key: Option<PathBuf>,
}

/// Top-level commands exposed by the Selium CLI.
#[derive(Debug, Subcommand)]
enum RawCommand {
    /// Create or update a workload deployment in the control plane.
    Deploy(RawDeployArgs),
    /// Connect two workloads through a named event pipeline edge.
    Connect(RawConnectArgs),
    /// Change the desired replica count for a workload.
    Scale(RawScaleArgs),
    /// Print a summary of the current control-plane state.
    Observe(RawObserveArgs),
    /// Show recent control-plane replay events.
    Replay(RawReplayArgs),
    /// Discover workloads, endpoints, and running processes from Selium-native state.
    Discover(RawDiscoverArgs),
    /// Query attributed infrastructure inventory for external consumers.
    Inventory(RawInventoryArgs),
    /// Export attributed runtime usage records from a specific node daemon.
    Usage(RawUsageArgs),
    /// List nodes that are considered live by the control plane.
    Nodes(RawNodesArgs),
    /// Start a specific instance directly on a node.
    Start(RawStartArgs),
    /// Stop a specific instance on a node.
    Stop(RawStopArgs),
    /// List running instances, either for one node or for every known node.
    List(RawListArgs),
    /// Run the reconciliation agent loop for a node.
    Agent(RawAgentArgs),
    /// Compile or publish interface definition language (IDL) files.
    Idl(RawIdlArgs),
}

/// Create or update a deployment for a workload.
#[derive(Debug, Args, Default, Deserialize)]
#[serde(rename_all = "kebab-case", default)]
struct RawDeployArgs {
    /// Tenant that owns the workload.
    #[arg(long, value_name = "TENANT")]
    tenant: Option<String>,
    /// Namespace that contains the workload.
    #[arg(long, value_name = "NAMESPACE")]
    namespace: Option<String>,
    /// Workload name within the tenant and namespace.
    #[arg(long, alias = "app", value_name = "WORKLOAD")]
    #[serde(alias = "app")]
    workload: Option<String>,
    /// Module identifier or artifact path for the deployment.
    #[arg(long, value_name = "MODULE")]
    module: Option<String>,
    /// Desired number of replicas for the workload.
    #[arg(long, value_name = "COUNT")]
    replicas: Option<u32>,
    /// Isolation profile to apply to new replicas.
    #[arg(long, value_enum, value_name = "PROFILE")]
    isolation: Option<IsolationArg>,
    /// Contract to attach, formatted as `namespace/kind:name@version`.
    #[arg(long = "contract", value_name = "CONTRACT")]
    contracts: Vec<String>,
}

/// Add an event pipeline edge between two workloads.
#[derive(Debug, Args, Default, Deserialize)]
#[serde(rename_all = "kebab-case", default)]
struct RawConnectArgs {
    /// Pipeline name to create or update.
    #[arg(long, value_name = "PIPELINE")]
    pipeline: Option<String>,
    /// Tenant that owns both workloads.
    #[arg(long, value_name = "TENANT")]
    tenant: Option<String>,
    /// Namespace that contains both workloads.
    #[arg(long, value_name = "NAMESPACE")]
    namespace: Option<String>,
    /// Source workload that emits the endpoint.
    #[arg(long, alias = "from-app", value_name = "WORKLOAD")]
    #[serde(alias = "from-app")]
    from_workload: Option<String>,
    /// Destination workload that consumes the endpoint.
    #[arg(long, alias = "to-app", value_name = "WORKLOAD")]
    #[serde(alias = "to-app")]
    to_workload: Option<String>,
    /// Shared event endpoint name on both workloads.
    #[arg(long, value_name = "ENDPOINT")]
    endpoint: Option<String>,
    /// Event contract for the edge, formatted as `namespace/kind:name@version`.
    #[arg(long, value_name = "CONTRACT")]
    contract: Option<String>,
}

/// Set the desired replica count for an existing workload.
#[derive(Debug, Args, Default, Deserialize)]
#[serde(rename_all = "kebab-case", default)]
struct RawScaleArgs {
    /// Tenant that owns the workload.
    #[arg(long, value_name = "TENANT")]
    tenant: Option<String>,
    /// Namespace that contains the workload.
    #[arg(long, value_name = "NAMESPACE")]
    namespace: Option<String>,
    /// Workload name within the tenant and namespace.
    #[arg(long, alias = "app", value_name = "WORKLOAD")]
    #[serde(alias = "app")]
    workload: Option<String>,
    /// Desired number of replicas.
    #[arg(long, value_name = "COUNT")]
    replicas: Option<u32>,
}

/// Print a control-plane summary.
#[derive(Debug, Args, Default, Deserialize)]
#[serde(rename_all = "kebab-case", default)]
struct RawObserveArgs {
    /// Request machine-readable output when supported.
    #[arg(long)]
    json: bool,
}

/// Inspect recently replayed control-plane events.
#[derive(Debug, Args, Default, Deserialize)]
#[serde(rename_all = "kebab-case", default)]
struct RawReplayArgs {
    /// Maximum number of replay events to fetch.
    #[arg(long, value_name = "COUNT")]
    limit: Option<usize>,
    /// Start replay from this durable sequence, inclusive.
    #[arg(long, value_name = "SEQUENCE")]
    since_sequence: Option<u64>,
    /// Filter output to one opaque external account reference.
    #[arg(long, value_name = "EXTERNAL_ACCOUNT_REF")]
    external_account_ref: Option<String>,
    /// Filter output to one fully-qualified workload key.
    #[arg(long, value_name = "TENANT/NAMESPACE/WORKLOAD")]
    workload_key: Option<String>,
    /// Filter output to a tenant when combined with namespace and workload.
    #[arg(long, value_name = "TENANT")]
    tenant: Option<String>,
    /// Filter output to a namespace when combined with tenant and workload.
    #[arg(long, value_name = "NAMESPACE")]
    namespace: Option<String>,
    /// Filter output to a workload when combined with tenant and namespace.
    #[arg(long, alias = "app", value_name = "WORKLOAD")]
    #[serde(alias = "app")]
    workload: Option<String>,
    /// Filter output to one Selium module identifier.
    #[arg(long, value_name = "MODULE")]
    module: Option<String>,
    /// Filter output to one fully-qualified pipeline key.
    #[arg(long, value_name = "TENANT/NAMESPACE/PIPELINE")]
    pipeline: Option<String>,
    /// Filter output to one node name.
    #[arg(long, value_name = "NODE")]
    node: Option<String>,
    /// Print the replay payload as machine-readable JSON.
    #[arg(long)]
    json: bool,
}

/// Discover workloads, endpoints, and running processes from Selium-native state.
#[derive(Debug, Args, Default, Deserialize)]
#[serde(rename_all = "kebab-case", default)]
struct RawDiscoverArgs {
    /// Restrict discovery to one exact workload key. Repeat to include multiple workloads.
    #[arg(long = "workload", value_name = "TENANT/NAMESPACE/WORKLOAD")]
    workloads: Vec<String>,
    /// Restrict discovery to workloads whose keys start with this prefix. Repeat to include multiple prefixes.
    #[arg(long = "workload-prefix", value_name = "PREFIX")]
    workload_prefixes: Vec<String>,
    /// Resolve one workload to its discoverable public endpoints.
    #[arg(long, value_name = "TENANT/NAMESPACE/WORKLOAD")]
    resolve_workload: Option<String>,
    /// Resolve one public endpoint to its bound contract.
    #[arg(long, value_name = "TENANT/NAMESPACE/WORKLOAD#KIND:ENDPOINT")]
    resolve_endpoint: Option<String>,
    /// Resolve one workload to a running process, preserving operational replica identity when unambiguous.
    #[arg(long, value_name = "TENANT/NAMESPACE/WORKLOAD")]
    resolve_running_workload: Option<String>,
    /// Resolve one exact running replica key.
    #[arg(long, value_name = "REPLICA_KEY")]
    resolve_replica_key: Option<String>,
    /// Permit running-process discovery output when resolving operational targets.
    #[arg(long)]
    allow_operational_processes: bool,
    /// Print the discovery payload as machine-readable JSON.
    #[arg(long)]
    json: bool,
}

/// Query attributed workload, module, pipeline, and node inventory.
#[derive(Debug, Args, Default, Deserialize)]
#[serde(rename_all = "kebab-case", default)]
struct RawInventoryArgs {
    /// Filter output to one opaque external account reference.
    #[arg(long, value_name = "EXTERNAL_ACCOUNT_REF")]
    external_account_ref: Option<String>,
    /// Filter output to one fully-qualified workload key.
    #[arg(long, value_name = "TENANT/NAMESPACE/WORKLOAD")]
    workload: Option<String>,
    /// Filter output to one Selium module identifier.
    #[arg(long, value_name = "MODULE")]
    module: Option<String>,
    /// Filter output to one fully-qualified pipeline key.
    #[arg(long, value_name = "TENANT/NAMESPACE/PIPELINE")]
    pipeline: Option<String>,
    /// Filter output to one node name.
    #[arg(long, value_name = "NODE")]
    node: Option<String>,
    /// Print the inventory payload as machine-readable JSON.
    #[arg(long)]
    json: bool,
}

/// Export attributed runtime usage records from a specific node daemon.
#[derive(Debug, Args, Default, Deserialize)]
#[serde(rename_all = "kebab-case", default)]
struct RawUsageArgs {
    /// Node expected to serve the runtime usage export.
    #[arg(long, value_name = "NODE")]
    node: Option<String>,
    /// Maximum number of runtime usage records to fetch.
    #[arg(long, value_name = "COUNT")]
    limit: Option<usize>,
    /// Start from the newest retained usage record.
    #[arg(long)]
    latest: bool,
    /// Start from this durable usage sequence, inclusive.
    #[arg(long, value_name = "SEQUENCE")]
    since_sequence: Option<u64>,
    /// Start from this durable usage timestamp, inclusive.
    #[arg(long, value_name = "TIMESTAMP_MS")]
    since_timestamp_ms: Option<u64>,
    /// Filter output to one opaque external account reference.
    #[arg(long, value_name = "EXTERNAL_ACCOUNT_REF")]
    external_account_ref: Option<String>,
    /// Filter output to one fully-qualified workload key.
    #[arg(long, value_name = "TENANT/NAMESPACE/WORKLOAD")]
    workload: Option<String>,
    /// Filter output to one Selium module identifier.
    #[arg(long, value_name = "MODULE")]
    module: Option<String>,
    /// Filter output to samples whose window overlaps this inclusive lower bound.
    #[arg(long, value_name = "TIMESTAMP_MS")]
    window_start_ms: Option<u64>,
    /// Filter output to samples whose window overlaps this exclusive upper bound.
    #[arg(long, value_name = "TIMESTAMP_MS")]
    window_end_ms: Option<u64>,
    /// Print the runtime usage payload as machine-readable JSON.
    #[arg(long)]
    json: bool,
}

/// List nodes that are currently live.
#[derive(Debug, Args, Default, Deserialize)]
#[serde(rename_all = "kebab-case", default)]
struct RawNodesArgs {
    /// Maximum node age, in milliseconds, before a node is treated as stale.
    #[arg(long, value_name = "MILLISECONDS")]
    max_staleness_ms: Option<u64>,
    /// Print the raw node payload instead of the compact table.
    #[arg(long)]
    json: bool,
}

/// Start a specific instance directly on a node.
///
/// Provide either `--module-spec` to send a prebuilt runtime module spec, or
/// `--module` to let the CLI build one from the remaining flags.
#[derive(Debug, Args, Default, Deserialize)]
#[serde(rename_all = "kebab-case", default, deny_unknown_fields)]
struct RawStartArgs {
    /// Node name that should host the instance.
    #[arg(long, value_name = "NODE")]
    node: Option<String>,
    /// Stable replica identifier for the instance.
    #[arg(long = "replica-key", value_name = "REPLICA")]
    replica_key: Option<String>,
    /// Prebuilt runtime module spec to send as-is.
    #[arg(long, value_name = "SPEC")]
    module_spec: Option<String>,
    /// Module path or identifier used to build a runtime spec.
    #[arg(long, value_name = "MODULE")]
    module: Option<String>,
    /// Runtime adaptor used when building a module spec from `--module`.
    #[arg(long = "adaptor", value_enum, value_name = "ADAPTOR")]
    adaptor: Option<AdaptorArg>,
    /// Isolation profile used when building a module spec from `--module`.
    #[arg(long, value_enum, value_name = "PROFILE")]
    isolation: Option<IsolationArg>,
    /// Capability to include when building a module spec from `--module`.
    #[arg(long = "capability", value_name = "CAPABILITY")]
    capabilities: Vec<String>,
    /// Managed event endpoint to expose as an ingress reader.
    #[arg(long = "event-reader", value_name = "ENDPOINT")]
    event_readers: Vec<String>,
    /// Managed event endpoint to expose as an egress writer.
    #[arg(long = "event-writer", value_name = "ENDPOINT")]
    event_writers: Vec<String>,
}

/// Stop a specific instance on a node.
#[derive(Debug, Args, Default, Deserialize)]
#[serde(rename_all = "kebab-case", default, deny_unknown_fields)]
struct RawStopArgs {
    /// Node name that currently hosts the instance.
    #[arg(long, value_name = "NODE")]
    node: Option<String>,
    /// Replica identifier to stop.
    #[arg(long = "replica-key", value_name = "REPLICA")]
    replica_key: Option<String>,
}

/// List running instances.
#[derive(Debug, Args, Default, Deserialize)]
#[serde(rename_all = "kebab-case", default)]
struct RawListArgs {
    /// Restrict the query to one node; omit to inspect every known node.
    #[arg(long, value_name = "NODE")]
    node: Option<String>,
}

/// Reconcile one node against the control-plane state.
#[derive(Debug, Args, Default, Deserialize)]
#[serde(rename_all = "kebab-case", default)]
struct RawAgentArgs {
    /// Node name to reconcile.
    #[arg(long, value_name = "NODE")]
    node: Option<String>,
    /// Delay between reconciliation loops in milliseconds.
    #[arg(long, value_name = "MILLISECONDS")]
    interval_ms: Option<u64>,
    /// Run one reconciliation pass and exit.
    #[arg(long)]
    once: bool,
    /// Path to the persisted agent state snapshot.
    #[arg(long, value_name = "PATH")]
    agent_state: Option<PathBuf>,
}

/// Compile or publish IDL files.
#[derive(Debug, Args, Default, Deserialize)]
#[serde(rename_all = "kebab-case", default)]
struct RawIdlArgs {
    #[command(subcommand)]
    command: RawIdlCommand,
}

/// Operations for Selium IDL files.
#[derive(Debug, Clone, Deserialize, Subcommand)]
#[serde(rename_all = "kebab-case")]
enum RawIdlCommand {
    /// Generate Rust bindings from an IDL file.
    Compile(RawIdlCompileArgs),
    /// Publish an IDL file to the control plane.
    Publish(RawIdlPublishArgs),
}

impl Default for RawIdlCommand {
    fn default() -> Self {
        Self::Compile(RawIdlCompileArgs::default())
    }
}

/// Generate Rust bindings from an IDL file.
#[derive(Debug, Args, Clone, Default, Deserialize)]
#[serde(rename_all = "kebab-case", default)]
struct RawIdlCompileArgs {
    /// Input IDL file to compile.
    #[arg(long, value_name = "PATH")]
    input: Option<PathBuf>,
    /// Output path for the generated Rust source.
    #[arg(long, value_name = "PATH")]
    output: Option<PathBuf>,
}

/// Publish an IDL file to the control plane.
#[derive(Debug, Args, Clone, Default, Deserialize)]
#[serde(rename_all = "kebab-case", default)]
struct RawIdlPublishArgs {
    /// Input IDL file to publish.
    #[arg(long, value_name = "PATH")]
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
    discover: Option<RawDiscoverArgs>,
    inventory: Option<RawInventoryArgs>,
    usage: Option<RawUsageArgs>,
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
    let matches = RawCli::command().get_matches_from(std::env::args_os());
    resolve_cli_from_matches(matches)
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn load_cli_from<I, T>(args: I) -> Result<Cli>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let matches = RawCli::command()
        .try_get_matches_from(args)
        .map_err(|err| anyhow!("parse CLI arguments: {err}"))?;

    resolve_cli_from_matches(matches)
}

fn resolve_cli_from_matches(matches: clap::ArgMatches) -> Result<Cli> {
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
            RawCommand::Discover(args) => Command::Discover(args.resolve()?),
            RawCommand::Inventory(args) => Command::Inventory(args.resolve()),
            RawCommand::Usage(args) => Command::Usage(args.resolve()?),
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
            tenant: required_arg("deploy.tenant", self.tenant)?,
            namespace: required_arg("deploy.namespace", self.namespace)?,
            workload: required_arg("deploy.workload", self.workload)?,
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
            tenant: required_arg("connect.tenant", self.tenant)?,
            namespace: required_arg("connect.namespace", self.namespace)?,
            from_workload: required_arg("connect.from-workload", self.from_workload)?,
            to_workload: required_arg("connect.to-workload", self.to_workload)?,
            endpoint: required_arg("connect.endpoint", self.endpoint)?,
            contract: required_arg("connect.contract", self.contract)?,
        })
    }
}

impl RawScaleArgs {
    fn resolve(self) -> Result<ScaleArgs> {
        Ok(ScaleArgs {
            tenant: required_arg("scale.tenant", self.tenant)?,
            namespace: required_arg("scale.namespace", self.namespace)?,
            workload: required_arg("scale.workload", self.workload)?,
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
            since_sequence: self.since_sequence,
            external_account_ref: self.external_account_ref,
            workload_key: self.workload_key,
            tenant: self.tenant,
            namespace: self.namespace,
            workload: self.workload,
            module: self.module,
            pipeline: self.pipeline,
            node: self.node,
            json: self.json,
        }
    }
}

impl RawDiscoverArgs {
    fn resolve(self) -> Result<DiscoverArgs> {
        let resolve_target_count = [
            self.resolve_workload.is_some(),
            self.resolve_endpoint.is_some(),
            self.resolve_running_workload.is_some(),
            self.resolve_replica_key.is_some(),
        ]
        .into_iter()
        .filter(|present| *present)
        .count();
        if resolve_target_count > 1 {
            return Err(anyhow!(
                "provide at most one of --resolve-workload, --resolve-endpoint, --resolve-running-workload, or --resolve-replica-key"
            ));
        }

        Ok(DiscoverArgs {
            workloads: self.workloads,
            workload_prefixes: self.workload_prefixes,
            resolve_workload: self.resolve_workload,
            resolve_endpoint: self.resolve_endpoint,
            resolve_running_workload: self.resolve_running_workload,
            resolve_replica_key: self.resolve_replica_key,
            allow_operational_processes: self.allow_operational_processes,
            json: self.json,
        })
    }
}

impl RawInventoryArgs {
    fn resolve(self) -> InventoryArgs {
        InventoryArgs {
            external_account_ref: self.external_account_ref,
            workload: self.workload,
            module: self.module,
            pipeline: self.pipeline,
            node: self.node,
            json: self.json,
        }
    }
}

impl RawUsageArgs {
    fn resolve(self) -> Result<UsageArgs> {
        Ok(UsageArgs {
            node: self.node.context("usage --node is required")?,
            limit: self.limit.unwrap_or(50),
            latest: self.latest,
            since_sequence: self.since_sequence,
            since_timestamp_ms: self.since_timestamp_ms,
            external_account_ref: self.external_account_ref,
            workload: self.workload,
            module: self.module,
            window_start_ms: self.window_start_ms,
            window_end_ms: self.window_end_ms,
            json: self.json,
        })
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
            replica_key: required_arg("start.replica-key", self.replica_key)?,
            module_spec: self.module_spec,
            module: self.module,
            adaptor: self.adaptor.unwrap_or(AdaptorArg::Wasmtime),
            isolation: self.isolation.unwrap_or(IsolationArg::Standard),
            capabilities: self.capabilities,
            event_readers: self.event_readers,
            event_writers: self.event_writers,
        })
    }
}

impl RawStopArgs {
    fn resolve(self) -> Result<StopArgs> {
        Ok(StopArgs {
            node: required_arg("stop.node", self.node)?,
            replica_key: required_arg("stop.replica-key", self.replica_key)?,
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
        discover,
        inventory,
        usage,
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
        discover,
        inventory,
        usage,
        nodes,
        start,
        stop,
        list,
        agent,
        idl,
    ) {
        (RawCommand::Deploy(args), Some(cfg), _, _, _, _, _, _, _, _, _, _, _, _, _) => {
            merge_deploy_config(args, matches.subcommand_matches("deploy"), cfg);
        }
        (RawCommand::Connect(args), _, Some(cfg), _, _, _, _, _, _, _, _, _, _, _, _) => {
            merge_connect_config(args, matches.subcommand_matches("connect"), cfg);
        }
        (RawCommand::Scale(args), _, _, Some(cfg), _, _, _, _, _, _, _, _, _, _, _) => {
            merge_scale_config(args, matches.subcommand_matches("scale"), cfg);
        }
        (RawCommand::Observe(args), _, _, _, Some(cfg), _, _, _, _, _, _, _, _, _, _) => {
            merge_observe_config(args, matches.subcommand_matches("observe"), cfg);
        }
        (RawCommand::Replay(args), _, _, _, _, Some(cfg), _, _, _, _, _, _, _, _, _) => {
            merge_replay_config(args, matches.subcommand_matches("replay"), cfg);
        }
        (RawCommand::Discover(args), _, _, _, _, _, Some(cfg), _, _, _, _, _, _, _, _) => {
            merge_discover_config(args, matches.subcommand_matches("discover"), cfg);
        }
        (RawCommand::Inventory(args), _, _, _, _, _, _, Some(cfg), _, _, _, _, _, _, _) => {
            merge_inventory_config(args, matches.subcommand_matches("inventory"), cfg);
        }
        (RawCommand::Usage(args), _, _, _, _, _, _, _, Some(cfg), _, _, _, _, _, _) => {
            merge_usage_config(args, matches.subcommand_matches("usage"), cfg);
        }
        (RawCommand::Nodes(args), _, _, _, _, _, _, _, _, Some(cfg), _, _, _, _, _) => {
            merge_nodes_config(args, matches.subcommand_matches("nodes"), cfg);
        }
        (RawCommand::Start(args), _, _, _, _, _, _, _, _, _, Some(cfg), _, _, _, _) => {
            merge_start_config(args, matches.subcommand_matches("start"), cfg);
        }
        (RawCommand::Stop(args), _, _, _, _, _, _, _, _, _, _, Some(cfg), _, _, _) => {
            merge_stop_config(args, matches.subcommand_matches("stop"), cfg);
        }
        (RawCommand::List(args), _, _, _, _, _, _, _, _, _, _, _, Some(cfg), _, _) => {
            merge_list_config(args, matches.subcommand_matches("list"), cfg);
        }
        (RawCommand::Agent(args), _, _, _, _, _, _, _, _, _, _, _, _, Some(cfg), _) => {
            merge_agent_config(args, matches.subcommand_matches("agent"), cfg);
        }
        (RawCommand::Idl(args), _, _, _, _, _, _, _, _, _, _, _, _, _, Some(cfg)) => {
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
    merge_option(&mut args.tenant, value_source("tenant"), config.tenant);
    merge_option(
        &mut args.namespace,
        value_source("namespace"),
        config.namespace,
    );
    merge_option(
        &mut args.workload,
        value_source("workload"),
        config.workload,
    );
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
    merge_option(&mut args.tenant, value_source("tenant"), config.tenant);
    merge_option(
        &mut args.namespace,
        value_source("namespace"),
        config.namespace,
    );
    merge_option(
        &mut args.from_workload,
        value_source("from_workload"),
        config.from_workload,
    );
    merge_option(
        &mut args.to_workload,
        value_source("to_workload"),
        config.to_workload,
    );
    merge_option(
        &mut args.endpoint,
        value_source("endpoint"),
        config.endpoint,
    );
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
    merge_option(&mut args.tenant, value_source("tenant"), config.tenant);
    merge_option(
        &mut args.namespace,
        value_source("namespace"),
        config.namespace,
    );
    merge_option(
        &mut args.workload,
        value_source("workload"),
        config.workload,
    );
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
    merge_option(
        &mut args.since_sequence,
        value_source("since_sequence"),
        config.since_sequence,
    );
    merge_option(
        &mut args.external_account_ref,
        value_source("external_account_ref"),
        config.external_account_ref,
    );
    merge_option(
        &mut args.workload_key,
        value_source("workload_key"),
        config.workload_key,
    );
    merge_option(&mut args.tenant, value_source("tenant"), config.tenant);
    merge_option(
        &mut args.namespace,
        value_source("namespace"),
        config.namespace,
    );
    merge_option(
        &mut args.workload,
        value_source("workload"),
        config.workload,
    );
    merge_option(&mut args.module, value_source("module"), config.module);
    merge_option(
        &mut args.pipeline,
        value_source("pipeline"),
        config.pipeline,
    );
    merge_option(&mut args.node, value_source("node"), config.node);
    merge_bool(&mut args.json, value_source("json"), config.json);
}

fn merge_discover_config(
    args: &mut RawDiscoverArgs,
    matches: Option<&clap::ArgMatches>,
    config: RawDiscoverArgs,
) {
    let value_source = |name| matches.and_then(|m| m.value_source(name));
    merge_vec(
        &mut args.workloads,
        value_source("workloads"),
        config.workloads,
    );
    merge_vec(
        &mut args.workload_prefixes,
        value_source("workload_prefixes"),
        config.workload_prefixes,
    );
    merge_option(
        &mut args.resolve_workload,
        value_source("resolve_workload"),
        config.resolve_workload,
    );
    merge_option(
        &mut args.resolve_endpoint,
        value_source("resolve_endpoint"),
        config.resolve_endpoint,
    );
    merge_option(
        &mut args.resolve_running_workload,
        value_source("resolve_running_workload"),
        config.resolve_running_workload,
    );
    merge_option(
        &mut args.resolve_replica_key,
        value_source("resolve_replica_key"),
        config.resolve_replica_key,
    );
    merge_bool(
        &mut args.allow_operational_processes,
        value_source("allow_operational_processes"),
        config.allow_operational_processes,
    );
    merge_bool(&mut args.json, value_source("json"), config.json);
}

fn merge_inventory_config(
    args: &mut RawInventoryArgs,
    matches: Option<&clap::ArgMatches>,
    config: RawInventoryArgs,
) {
    let value_source = |name| matches.and_then(|m| m.value_source(name));
    merge_option(
        &mut args.external_account_ref,
        value_source("external_account_ref"),
        config.external_account_ref,
    );
    merge_option(
        &mut args.workload,
        value_source("workload"),
        config.workload,
    );
    merge_option(&mut args.module, value_source("module"), config.module);
    merge_option(
        &mut args.pipeline,
        value_source("pipeline"),
        config.pipeline,
    );
    merge_option(&mut args.node, value_source("node"), config.node);
    merge_bool(&mut args.json, value_source("json"), config.json);
}

fn merge_usage_config(
    args: &mut RawUsageArgs,
    matches: Option<&clap::ArgMatches>,
    config: RawUsageArgs,
) {
    let value_source = |name| matches.and_then(|m| m.value_source(name));
    merge_option(&mut args.node, value_source("node"), config.node);
    merge_option(&mut args.limit, value_source("limit"), config.limit);
    merge_bool(&mut args.latest, value_source("latest"), config.latest);
    merge_option(
        &mut args.since_sequence,
        value_source("since_sequence"),
        config.since_sequence,
    );
    merge_option(
        &mut args.since_timestamp_ms,
        value_source("since_timestamp_ms"),
        config.since_timestamp_ms,
    );
    merge_option(
        &mut args.external_account_ref,
        value_source("external_account_ref"),
        config.external_account_ref,
    );
    merge_option(
        &mut args.workload,
        value_source("workload"),
        config.workload,
    );
    merge_option(&mut args.module, value_source("module"), config.module);
    merge_option(
        &mut args.window_start_ms,
        value_source("window_start_ms"),
        config.window_start_ms,
    );
    merge_option(
        &mut args.window_end_ms,
        value_source("window_end_ms"),
        config.window_end_ms,
    );
    merge_bool(&mut args.json, value_source("json"), config.json);
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
        &mut args.replica_key,
        value_source("replica_key"),
        config.replica_key,
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
        &mut args.replica_key,
        value_source("replica_key"),
        config.replica_key,
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
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static NEXT_CONFIG_ID: AtomicU64 = AtomicU64::new(0);

    fn write_test_config(contents: &str) -> PathBuf {
        let id = NEXT_CONFIG_ID.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "selium-cli-config-{}-{id}.toml",
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
tenant = "tenant-a"
namespace = "payments"
workload = "from-config"
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
        assert_eq!(args.tenant, "tenant-a");
        assert_eq!(args.namespace, "payments");
        assert_eq!(args.workload, "from-config");
        assert_eq!(args.module, "module.wasm");
        assert_eq!(args.replicas, 3);
    }

    #[test]
    fn cli_command_line_overrides_config() {
        let config = write_test_config(
            r#"
[deploy]
tenant = "tenant-a"
namespace = "payments"
workload = "from-config"
module = "module.wasm"
replicas = 3
"#,
        );

        let cli = load_cli_from([
            "selium",
            "--config",
            config.to_str().expect("config path"),
            "deploy",
            "--tenant",
            "tenant-b",
            "--namespace",
            "search",
            "--workload",
            "from-cli",
        ])
        .expect("parse cli");

        let Command::Deploy(args) = cli.command else {
            panic!("expected deploy command");
        };
        assert_eq!(args.tenant, "tenant-b");
        assert_eq!(args.namespace, "search");
        assert_eq!(args.workload, "from-cli");
        assert_eq!(args.module, "module.wasm");
        assert_eq!(args.replicas, 3);
    }

    #[test]
    fn start_command_rejects_legacy_instance_id_flag() {
        let err = load_cli_from([
            "selium",
            "start",
            "--node",
            "node-a",
            "--instance-id",
            "legacy-id",
            "--module",
            "module.wasm",
        ])
        .expect_err("legacy flag should be rejected");

        assert!(err.to_string().contains("unexpected argument"));
    }

    #[test]
    fn start_config_rejects_legacy_instance_id_key() {
        let config = write_test_config(
            r#"
[start]
node = "node-a"
instance-id = "legacy-id"
module = "module.wasm"
"#,
        );

        let err = load_cli_from([
            "selium",
            "--config",
            config.to_str().expect("config path"),
            "start",
        ])
        .expect_err("legacy config key should be rejected");

        assert!(format!("{err:#}").contains("unknown field `instance-id`"));
    }

    #[test]
    fn replay_command_preserves_export_filters_and_json_flag() {
        let cli = load_cli_from([
            "selium",
            "replay",
            "--limit",
            "25",
            "--since-sequence",
            "41",
            "--external-account-ref",
            "acct-123",
            "--workload-key",
            "tenant-a/media/ingest",
            "--module",
            "ingest.wasm",
            "--pipeline",
            "tenant-a/media/camera",
            "--node",
            "node-a",
            "--json",
        ])
        .expect("parse replay command");

        let Command::Replay(args) = cli.command else {
            panic!("expected replay command");
        };
        assert_eq!(args.limit, 25);
        assert_eq!(args.since_sequence, Some(41));
        assert_eq!(args.external_account_ref.as_deref(), Some("acct-123"));
        assert_eq!(args.workload_key.as_deref(), Some("tenant-a/media/ingest"));
        assert_eq!(args.module.as_deref(), Some("ingest.wasm"));
        assert_eq!(args.pipeline.as_deref(), Some("tenant-a/media/camera"));
        assert_eq!(args.node.as_deref(), Some("node-a"));
        assert!(args.json);
    }

    #[test]
    fn inventory_command_parses_external_consumer_filters() {
        let cli = load_cli_from([
            "selium",
            "inventory",
            "--external-account-ref",
            "acct-123",
            "--workload",
            "tenant-a/media/ingest",
            "--module",
            "ingest.wasm",
            "--pipeline",
            "tenant-a/media/camera",
            "--node",
            "node-a",
            "--json",
        ])
        .expect("parse inventory command");

        let Command::Inventory(args) = cli.command else {
            panic!("expected inventory command");
        };
        assert_eq!(args.external_account_ref.as_deref(), Some("acct-123"));
        assert_eq!(args.workload.as_deref(), Some("tenant-a/media/ingest"));
        assert_eq!(args.module.as_deref(), Some("ingest.wasm"));
        assert_eq!(args.pipeline.as_deref(), Some("tenant-a/media/camera"));
        assert_eq!(args.node.as_deref(), Some("node-a"));
        assert!(args.json);
    }

    #[test]
    fn usage_command_parses_export_cursor_and_filters() {
        let cli = load_cli_from([
            "selium",
            "usage",
            "--node",
            "node-a",
            "--limit",
            "25",
            "--since-sequence",
            "41",
            "--external-account-ref",
            "acct-123",
            "--workload",
            "tenant-a/media/ingest",
            "--module",
            "ingest.wasm",
            "--window-start-ms",
            "1000",
            "--window-end-ms",
            "2000",
            "--json",
        ])
        .expect("parse usage command");

        let Command::Usage(args) = cli.command else {
            panic!("expected usage command");
        };
        assert_eq!(args.node, "node-a");
        assert_eq!(args.limit, 25);
        assert_eq!(args.since_sequence, Some(41));
        assert_eq!(args.external_account_ref.as_deref(), Some("acct-123"));
        assert_eq!(args.workload.as_deref(), Some("tenant-a/media/ingest"));
        assert_eq!(args.module.as_deref(), Some("ingest.wasm"));
        assert_eq!(args.window_start_ms, Some(1_000));
        assert_eq!(args.window_end_ms, Some(2_000));
        assert!(args.json);
    }

    #[test]
    fn discover_command_parses_state_filters() {
        let cli = load_cli_from([
            "selium",
            "discover",
            "--workload",
            "tenant-a/media/router",
            "--workload-prefix",
            "tenant-a/",
            "--allow-operational-processes",
            "--json",
        ])
        .expect("parse discover command");

        let Command::Discover(args) = cli.command else {
            panic!("expected discover command");
        };
        assert_eq!(args.workloads, vec!["tenant-a/media/router".to_string()]);
        assert_eq!(args.workload_prefixes, vec!["tenant-a/".to_string()]);
        assert!(args.allow_operational_processes);
        assert!(args.json);
    }

    #[test]
    fn discover_command_rejects_multiple_resolve_targets() {
        let err = load_cli_from([
            "selium",
            "discover",
            "--resolve-workload",
            "tenant-a/media/router",
            "--resolve-endpoint",
            "tenant-a/media/router#service:camera.detect",
        ])
        .expect_err("multiple resolve targets should be rejected");

        assert!(
            err.to_string()
                .contains("provide at most one of --resolve-workload")
        );
    }

    #[test]
    fn root_help_describes_top_level_commands() {
        let mut command = RawCli::command();
        let help = command.render_long_help().to_string();

        assert!(help.contains("Manage Selium control-plane resources and node instances."));
        assert!(help.contains("Create or update a workload deployment in the control plane"));
        assert!(help.contains("Discover workloads, endpoints, and running processes"));
        assert!(help.contains("Query attributed infrastructure inventory for external consumers"));
        assert!(
            help.contains("Export attributed runtime usage records from a specific node daemon")
        );
        assert!(help.contains("Run the reconciliation agent loop for a node"));
    }

    #[test]
    fn deploy_help_describes_required_options() {
        let mut command = RawCli::command();
        let deploy = command
            .find_subcommand_mut("deploy")
            .expect("deploy subcommand");
        let help = deploy.render_long_help().to_string();

        assert!(help.contains("Create or update a workload deployment in the control plane"));
        assert!(help.contains("Tenant that owns the workload"));
        assert!(help.contains("Contract to attach, formatted as `namespace/kind:name@version`"));
    }
}
