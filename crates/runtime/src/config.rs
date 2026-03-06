use std::{
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, anyhow};
use clap::{
    Args, CommandFactory, FromArgMatches, Parser, Subcommand, ValueEnum, parser::ValueSource,
};
use serde::Deserialize;

#[derive(Copy, Clone, Debug, Deserialize, ValueEnum, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum LogFormat {
    /// Human-friendly text logs suitable for local development.
    Text,
    /// JSON logs for ingestion into systems such as Loki or OTLP collectors.
    Json,
}

#[derive(Parser, Debug)]
#[command(version, about = "Selium host runtime")]
pub(crate) struct ServerOptions {
    /// Optional TOML config file that provides defaults for omitted flags.
    #[arg(short = 'c', long, global = true, value_name = "FILE")]
    config: Option<PathBuf>,
    /// Log output format (text or JSON) for tracing events.
    #[arg(long, env = "SELIUM_LOG_FORMAT", default_value = "text")]
    pub(crate) log_format: LogFormat,
    #[command(subcommand)]
    pub(crate) command: Option<ServerCommand>,
    /// Base directory where certificates and WASM modules are stored.
    #[arg(short, long, env = "SELIUM_WORK_DIR", default_value_os = ".")]
    pub(crate) work_dir: PathBuf,
    /// Module specification to start (repeatable). Format:
    /// `path=...;capabilities=...;adapter=wasmtime;profile=standard;args=...`
    #[arg(long, value_name = "SPEC")]
    pub(crate) module: Option<Vec<String>>,
}

#[derive(Subcommand, Debug)]
pub(crate) enum ServerCommand {
    /// Generate a local CA plus server and client certificate pairs.
    GenerateCerts(GenerateCertsArgs),
    /// Run long-lived runtime daemon with lifecycle API.
    Daemon(DaemonArgs),
}

#[derive(Args, Debug)]
pub(crate) struct GenerateCertsArgs {
    /// Directory to write certificate and key files to.
    #[arg(long, default_value = "certs")]
    pub(crate) output_dir: PathBuf,
    /// Common Name to embed in the generated CA.
    #[arg(long, default_value = "Selium Local CA")]
    pub(crate) ca_common_name: String,
    /// DNS name to embed in the server certificate.
    #[arg(long, default_value = "localhost")]
    pub(crate) server_name: String,
    /// DNS name to embed in the client certificate.
    #[arg(long, default_value = "client.localhost")]
    pub(crate) client_name: String,
}

#[derive(Args, Debug)]
pub(crate) struct DaemonArgs {
    /// QUIC listener address for daemon lifecycle and control-plane API.
    #[arg(long, default_value = "127.0.0.1:7100")]
    pub(crate) listen: String,
    /// Logical node identifier for control-plane consensus.
    #[arg(long, default_value = "local-node")]
    pub(crate) cp_node_id: String,
    /// Peer node endpoint in node_id=host:port[@server_name] format (repeatable).
    #[arg(long = "cp-peer")]
    pub(crate) cp_peers: Vec<String>,
    /// Bootstrap this node as leader when no persisted term exists.
    #[arg(long)]
    pub(crate) cp_bootstrap_leader: bool,
    /// Directory used for control-plane raft state, snapshots, and durable events.
    #[arg(long, default_value = ".selium/control-plane")]
    pub(crate) cp_state_dir: PathBuf,
    /// Path to server cert PEM used for daemon QUIC endpoint.
    #[arg(long, default_value = "certs/server.crt")]
    pub(crate) quic_cert: PathBuf,
    /// Path to server key PEM used for daemon QUIC endpoint.
    #[arg(long, default_value = "certs/server.key")]
    pub(crate) quic_key: PathBuf,
    /// Path to client cert PEM used for outbound peer RPCs (defaults to --quic-cert).
    #[arg(long)]
    pub(crate) quic_peer_cert: Option<PathBuf>,
    /// Path to client key PEM used for outbound peer RPCs (defaults to --quic-key).
    #[arg(long)]
    pub(crate) quic_peer_key: Option<PathBuf>,
    /// Path to CA cert PEM used to validate mTLS clients and peers.
    #[arg(long, default_value = "certs/ca.crt")]
    pub(crate) quic_ca: PathBuf,
    /// Public daemon address advertised into control-plane node registry.
    #[arg(long)]
    pub(crate) cp_public_addr: Option<String>,
    /// TLS server name advertised for this node's daemon endpoint.
    #[arg(long, default_value = "localhost")]
    pub(crate) cp_server_name: String,
    /// Capacity slots advertised for this node.
    #[arg(long, default_value_t = 64)]
    pub(crate) cp_capacity_slots: u32,
    /// Heartbeat interval for node liveness updates (milliseconds).
    #[arg(long, default_value_t = 1000)]
    pub(crate) cp_heartbeat_interval_ms: u64,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "kebab-case", default)]
struct RuntimeConfig {
    log_format: Option<LogFormat>,
    work_dir: Option<PathBuf>,
    module: Option<Vec<String>>,
    generate_certs: Option<GenerateCertsConfig>,
    daemon: Option<DaemonConfig>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "kebab-case", default)]
struct GenerateCertsConfig {
    output_dir: Option<PathBuf>,
    ca_common_name: Option<String>,
    server_name: Option<String>,
    client_name: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "kebab-case", default)]
struct DaemonConfig {
    listen: Option<String>,
    cp_node_id: Option<String>,
    cp_peers: Option<Vec<String>>,
    cp_bootstrap_leader: Option<bool>,
    cp_state_dir: Option<PathBuf>,
    quic_cert: Option<PathBuf>,
    quic_key: Option<PathBuf>,
    quic_peer_cert: Option<PathBuf>,
    quic_peer_key: Option<PathBuf>,
    quic_ca: Option<PathBuf>,
    cp_public_addr: Option<String>,
    cp_server_name: Option<String>,
    cp_capacity_slots: Option<u32>,
    cp_heartbeat_interval_ms: Option<u64>,
}

pub(crate) fn load_server_options() -> Result<ServerOptions> {
    load_server_options_from(std::env::args_os())
}

pub(crate) fn load_server_options_from<I, T>(args: I) -> Result<ServerOptions>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let matches = ServerOptions::command().get_matches_from(args);
    let mut options = ServerOptions::from_arg_matches(&matches)
        .map_err(|err| anyhow!("parse runtime arguments: {err}"))?;

    if let Some(path) = options.config.clone() {
        let config: RuntimeConfig = load_toml_config(&path)?;
        merge_runtime_config(&mut options, &matches, config);
    }

    Ok(options)
}

fn merge_runtime_config(
    options: &mut ServerOptions,
    matches: &clap::ArgMatches,
    config: RuntimeConfig,
) {
    let RuntimeConfig {
        log_format,
        work_dir,
        module,
        generate_certs,
        daemon,
    } = config;

    merge_value(
        &mut options.log_format,
        matches.value_source("log_format"),
        log_format,
    );
    merge_value(
        &mut options.work_dir,
        matches.value_source("work_dir"),
        work_dir,
    );
    merge_optional(&mut options.module, matches.value_source("module"), module);

    match (&mut options.command, generate_certs, daemon) {
        (Some(ServerCommand::GenerateCerts(args)), Some(cfg), _) => {
            merge_generate_certs_config(args, matches.subcommand_matches("generate-certs"), cfg);
        }
        (Some(ServerCommand::Daemon(args)), _, Some(cfg)) => {
            merge_daemon_config(args, matches.subcommand_matches("daemon"), cfg);
        }
        _ => {}
    }
}

fn merge_generate_certs_config(
    args: &mut GenerateCertsArgs,
    matches: Option<&clap::ArgMatches>,
    config: GenerateCertsConfig,
) {
    let value_source = |name| matches.and_then(|m| m.value_source(name));
    merge_value(
        &mut args.output_dir,
        value_source("output_dir"),
        config.output_dir,
    );
    merge_value(
        &mut args.ca_common_name,
        value_source("ca_common_name"),
        config.ca_common_name,
    );
    merge_value(
        &mut args.server_name,
        value_source("server_name"),
        config.server_name,
    );
    merge_value(
        &mut args.client_name,
        value_source("client_name"),
        config.client_name,
    );
}

fn merge_daemon_config(
    args: &mut DaemonArgs,
    matches: Option<&clap::ArgMatches>,
    config: DaemonConfig,
) {
    let value_source = |name| matches.and_then(|m| m.value_source(name));

    merge_value(&mut args.listen, value_source("listen"), config.listen);
    merge_value(
        &mut args.cp_node_id,
        value_source("cp_node_id"),
        config.cp_node_id,
    );
    merge_vec(
        &mut args.cp_peers,
        value_source("cp_peers"),
        config.cp_peers,
    );
    merge_bool(
        &mut args.cp_bootstrap_leader,
        value_source("cp_bootstrap_leader"),
        config.cp_bootstrap_leader,
    );
    merge_value(
        &mut args.cp_state_dir,
        value_source("cp_state_dir"),
        config.cp_state_dir,
    );
    merge_value(
        &mut args.quic_cert,
        value_source("quic_cert"),
        config.quic_cert,
    );
    merge_value(
        &mut args.quic_key,
        value_source("quic_key"),
        config.quic_key,
    );
    merge_optional(
        &mut args.quic_peer_cert,
        value_source("quic_peer_cert"),
        config.quic_peer_cert,
    );
    merge_optional(
        &mut args.quic_peer_key,
        value_source("quic_peer_key"),
        config.quic_peer_key,
    );
    merge_value(&mut args.quic_ca, value_source("quic_ca"), config.quic_ca);
    merge_optional(
        &mut args.cp_public_addr,
        value_source("cp_public_addr"),
        config.cp_public_addr,
    );
    merge_value(
        &mut args.cp_server_name,
        value_source("cp_server_name"),
        config.cp_server_name,
    );
    merge_value(
        &mut args.cp_capacity_slots,
        value_source("cp_capacity_slots"),
        config.cp_capacity_slots,
    );
    merge_value(
        &mut args.cp_heartbeat_interval_ms,
        value_source("cp_heartbeat_interval_ms"),
        config.cp_heartbeat_interval_ms,
    );
}

fn merge_value<T>(slot: &mut T, source: Option<ValueSource>, config: Option<T>) {
    if should_apply_config(source)
        && let Some(config) = config
    {
        *slot = config;
    }
}

fn merge_optional<T>(slot: &mut Option<T>, source: Option<ValueSource>, config: Option<T>) {
    if should_apply_config(source)
        && let Some(config) = config
    {
        *slot = Some(config);
    }
}

fn merge_vec<T>(slot: &mut Vec<T>, source: Option<ValueSource>, config: Option<Vec<T>>) {
    if should_apply_config(source)
        && let Some(config) = config
        && !config.is_empty()
    {
        *slot = config;
    }
}

fn merge_bool(slot: &mut bool, source: Option<ValueSource>, config: Option<bool>) {
    if should_apply_config(source)
        && let Some(config) = config
    {
        *slot = config;
    }
}

fn should_apply_config(source: Option<ValueSource>) -> bool {
    !matches!(
        source,
        Some(ValueSource::CommandLine | ValueSource::EnvVariable)
    )
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
            "selium-runtime-config-{}.toml",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        fs::write(&path, contents).expect("write config");
        path
    }

    #[test]
    fn parses_default_options() {
        let opts = load_server_options_from(["selium-runtime"]).expect("parse opts");
        assert_eq!(opts.log_format, LogFormat::Text);
        assert!(opts.command.is_none());
        assert_eq!(opts.work_dir, PathBuf::from("."));
    }

    #[test]
    fn parses_generate_certs_command() {
        let opts = load_server_options_from([
            "selium-runtime",
            "generate-certs",
            "--output-dir",
            "certs-out",
            "--server-name",
            "example.local",
        ])
        .expect("parse opts");
        let Some(ServerCommand::GenerateCerts(args)) = opts.command else {
            panic!("expected generate-certs command");
        };
        assert_eq!(args.output_dir, PathBuf::from("certs-out"));
        assert_eq!(args.server_name, "example.local");
    }

    #[test]
    fn parses_daemon_command() {
        let opts =
            load_server_options_from(["selium-runtime", "daemon", "--listen", "127.0.0.1:7999"])
                .expect("parse opts");
        let Some(ServerCommand::Daemon(args)) = opts.command else {
            panic!("expected daemon command");
        };
        assert_eq!(args.listen, "127.0.0.1:7999");
    }

    #[test]
    fn applies_runtime_config_when_flag_missing() {
        let config = write_test_config(
            r#"
log-format = "json"
work-dir = "runtime-data"

[daemon]
listen = "127.0.0.1:7999"
cp-node-id = "cfg-node"
"#,
        );

        let opts = load_server_options_from([
            "selium-runtime",
            "--config",
            config.to_str().expect("config path"),
            "daemon",
        ])
        .expect("parse opts");

        assert_eq!(opts.log_format, LogFormat::Json);
        assert_eq!(opts.work_dir, PathBuf::from("runtime-data"));
        let Some(ServerCommand::Daemon(args)) = opts.command else {
            panic!("expected daemon command");
        };
        assert_eq!(args.listen, "127.0.0.1:7999");
        assert_eq!(args.cp_node_id, "cfg-node");
    }

    #[test]
    fn command_line_runtime_args_override_config() {
        let config = write_test_config(
            r#"
[daemon]
listen = "127.0.0.1:7999"
"#,
        );

        let opts = load_server_options_from([
            "selium-runtime",
            "--config",
            config.to_str().expect("config path"),
            "daemon",
            "--listen",
            "127.0.0.1:8001",
        ])
        .expect("parse opts");

        let Some(ServerCommand::Daemon(args)) = opts.command else {
            panic!("expected daemon command");
        };
        assert_eq!(args.listen, "127.0.0.1:8001");
    }
}
