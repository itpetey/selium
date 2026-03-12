#![allow(dead_code)]

use std::{
    collections::BTreeSet,
    fs,
    net::UdpSocket,
    path::{Path, PathBuf},
    process::{Child, Command, Output, Stdio},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow, bail};
use quinn::Endpoint;
use selium_abi::{DataValue, decode_rkyv};
use selium_control_plane_api::ControlPlaneState;
use selium_control_plane_core::{Mutation, Query};
use selium_control_plane_protocol::{
    Method, MutateApiRequest, MutateApiResponse, QueryApiRequest, QueryApiResponse,
    decode_envelope, decode_error, decode_payload, encode_request, is_error,
};
use selium_runtime_support::{
    ClientTlsPaths, build_quic_client_endpoint, read_framed, write_framed,
};
use tokio::time::{sleep, timeout};

const USER_ECHO_MANIFEST: &str = "examples/rpc-echo-service/Cargo.toml";
const USER_ECHO_ARTIFACT: &str = "rpc_echo_service.wasm";
const USER_IO_MANIFEST: &str = "examples/event-broadcast/Cargo.toml";
const USER_IO_ARTIFACT: &str = "event_broadcast.wasm";
const TOPOLOGY_INGRESS_MANIFEST: &str = "examples/control-plane-topology/apps/ingress/Cargo.toml";
const TOPOLOGY_INGRESS_ARTIFACT: &str = "cp_topology_ingress.wasm";
const TOPOLOGY_PROCESSOR_MANIFEST: &str =
    "examples/control-plane-topology/apps/processor/Cargo.toml";
const TOPOLOGY_PROCESSOR_ARTIFACT: &str = "cp_topology_processor.wasm";
const TOPOLOGY_SINK_MANIFEST: &str = "examples/control-plane-topology/apps/sink/Cargo.toml";
const TOPOLOGY_SINK_ARTIFACT: &str = "cp_topology_sink.wasm";
const CONTROL_PLANE_MANIFEST: &str = "modules/control-plane/Cargo.toml";
const CONTROL_PLANE_ARTIFACT: &str = "selium_module_control_plane.wasm";
const STAGED_CONTROL_PLANE_ARTIFACT: &str = "control-plane.wasm";
const DEFAULT_SERVER_NAME: &str = "localhost";
const CLI_CONNECT_TIMEOUT_RETRIES: usize = 5;
const CLI_CONNECT_TIMEOUT_BACKOFF: Duration = Duration::from_millis(400);
const DIRECT_QUERY_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Debug)]
pub struct ClusterHarnessConfig {
    pub name: String,
    pub consensus_timeout: Duration,
    pub poll_interval: Duration,
}

impl ClusterHarnessConfig {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            consensus_timeout: Duration::from_secs(60),
            poll_interval: Duration::from_millis(500),
        }
    }
}

pub struct ClusterHarness {
    root: PathBuf,
    base_dir: PathBuf,
    cert_dir: PathBuf,
    runtime_bin: PathBuf,
    cli_bin: PathBuf,
    control_plane_source: PathBuf,
    user_echo_source: PathBuf,
    user_io_source: PathBuf,
    topology_ingress_source: PathBuf,
    topology_processor_source: PathBuf,
    topology_sink_source: PathBuf,
    config: ClusterHarnessConfig,
    nodes: Vec<NodeProcess>,
}

struct NodeProcess {
    id: String,
    addr: String,
    work_dir: PathBuf,
    stdout_log: PathBuf,
    stderr_log: PathBuf,
    child: Child,
}

impl ClusterHarness {
    pub fn new(config: ClusterHarnessConfig) -> Result<Self> {
        let root = repo_root()?;
        let run_id = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let base_dir = root
            .join("target")
            .join("e2e")
            .join(&config.name)
            .join(run_id.to_string());
        let cert_dir = base_dir.join("certs");
        fs::create_dir_all(&cert_dir)
            .with_context(|| format!("create cert dir {}", cert_dir.display()))?;

        Ok(Self {
            runtime_bin: root.join("target/debug/selium-runtime"),
            cli_bin: root.join("target/debug/selium"),
            control_plane_source: root
                .join("target/wasm32-unknown-unknown/debug")
                .join(CONTROL_PLANE_ARTIFACT),
            user_echo_source: root
                .join("target/wasm32-unknown-unknown/debug")
                .join(USER_ECHO_ARTIFACT),
            user_io_source: root
                .join("target/wasm32-unknown-unknown/debug")
                .join(USER_IO_ARTIFACT),
            topology_ingress_source: root
                .join("target/wasm32-unknown-unknown/debug")
                .join(TOPOLOGY_INGRESS_ARTIFACT),
            topology_processor_source: root
                .join("target/wasm32-unknown-unknown/debug")
                .join(TOPOLOGY_PROCESSOR_ARTIFACT),
            topology_sink_source: root
                .join("target/wasm32-unknown-unknown/debug")
                .join(TOPOLOGY_SINK_ARTIFACT),
            root,
            base_dir,
            cert_dir,
            config,
            nodes: Vec::new(),
        })
    }

    pub fn prepare(&mut self) -> Result<()> {
        self.prepare_binaries()?;
        self.build_example_wasm(CONTROL_PLANE_MANIFEST, &self.control_plane_source)?;
        self.build_example_wasm(USER_ECHO_MANIFEST, &self.user_echo_source)?;
        self.build_example_wasm(USER_IO_MANIFEST, &self.user_io_source)?;
        self.generate_certs()?;
        self.spawn_nodes()?;
        Ok(())
    }

    pub fn prepare_control_plane_topology(&self) -> Result<()> {
        self.build_example_wasm(TOPOLOGY_INGRESS_MANIFEST, &self.topology_ingress_source)?;
        self.build_example_wasm(TOPOLOGY_PROCESSOR_MANIFEST, &self.topology_processor_source)?;
        self.build_example_wasm(TOPOLOGY_SINK_MANIFEST, &self.topology_sink_source)?;
        self.stage_module_for_all_nodes(&self.topology_ingress_source, TOPOLOGY_INGRESS_ARTIFACT)?;
        self.stage_module_for_all_nodes(
            &self.topology_processor_source,
            TOPOLOGY_PROCESSOR_ARTIFACT,
        )?;
        self.stage_module_for_all_nodes(&self.topology_sink_source, TOPOLOGY_SINK_ARTIFACT)?;
        Ok(())
    }

    pub fn daemon_addr_for(&self, node_id: &str) -> Result<String> {
        self.nodes
            .iter()
            .find(|node| node.id == node_id)
            .map(|node| node.addr.clone())
            .ok_or_else(|| anyhow!("unknown node id `{node_id}`"))
    }

    pub fn user_module_relative_path(&self) -> &'static str {
        "modules/rpc_echo_service.wasm"
    }

    pub fn user_io_module_relative_path(&self) -> &'static str {
        "modules/event_broadcast.wasm"
    }

    pub fn topology_contract_path(&self) -> &'static str {
        "examples/control-plane-topology/contracts/analytics.topology.v1.selium"
    }

    pub fn topology_ingress_module_relative_path(&self) -> &'static str {
        "modules/cp_topology_ingress.wasm"
    }

    pub fn topology_processor_module_relative_path(&self) -> &'static str {
        "modules/cp_topology_processor.wasm"
    }

    pub fn topology_sink_module_relative_path(&self) -> &'static str {
        "modules/cp_topology_sink.wasm"
    }

    pub fn agent_state_path(&self, node_id: &str) -> Result<PathBuf> {
        if !self.nodes.iter().any(|node| node.id == node_id) {
            bail!("unknown node id `{node_id}`");
        }
        let dir = self.base_dir.join("agent-state");
        fs::create_dir_all(&dir).with_context(|| format!("create {}", dir.display()))?;
        Ok(dir.join(format!("{node_id}.rkyv")))
    }

    pub fn runtime_pids(&self) -> Vec<u32> {
        self.nodes.iter().map(|node| node.child.id()).collect()
    }

    pub fn live_nodes(&self, daemon_addr: &str) -> Result<BTreeSet<String>> {
        self.fetch_live_nodes(daemon_addr)
    }

    pub fn node_logs(&self, node_id: &str) -> Result<String> {
        let node = self
            .nodes
            .iter()
            .find(|node| node.id == node_id)
            .ok_or_else(|| anyhow!("unknown node id `{node_id}`"))?;
        let stdout = fs::read_to_string(&node.stdout_log)
            .with_context(|| format!("read {}", node.stdout_log.display()))?;
        let stderr = fs::read_to_string(&node.stderr_log)
            .with_context(|| format!("read {}", node.stderr_log.display()))?;
        Ok(format!("{stdout}\n{stderr}"))
    }

    pub async fn wait_for_node_log_contains(
        &mut self,
        node_id: &str,
        needle: &str,
        timeout: Duration,
    ) -> Result<String> {
        let deadline = Instant::now() + timeout;
        let mut last_logs = String::new();
        while Instant::now() < deadline {
            self.ensure_nodes_alive()?;
            last_logs = self.node_logs(node_id)?;
            if last_logs.contains(needle) {
                return Ok(last_logs);
            }
            sleep(self.config.poll_interval).await;
        }

        bail!("timed out waiting for log text `{needle}` on {node_id}. last logs:\n{last_logs}")
    }

    pub async fn wait_for_control_plane_state<F>(
        &mut self,
        daemon_addr: &str,
        allow_stale: bool,
        timeout_window: Duration,
        predicate: F,
    ) -> Result<ControlPlaneState>
    where
        F: Fn(&ControlPlaneState) -> bool,
    {
        let deadline = Instant::now() + timeout_window;
        let mut last_state = None;
        while Instant::now() < deadline {
            self.ensure_nodes_alive()?;
            let state = self
                .query_control_plane_state(daemon_addr, allow_stale)
                .await?;
            if predicate(&state) {
                return Ok(state);
            }
            last_state = Some(format!("{state:#?}"));
            sleep(self.config.poll_interval).await;
        }

        bail!(
            "timed out waiting for control-plane state on {} to satisfy predicate. last state:\n{}",
            daemon_addr,
            last_state.unwrap_or_else(|| "<none>".to_string())
        )
    }

    pub async fn wait_for_consensus_ready(&mut self) -> Result<()> {
        if self.nodes.len() != 2 {
            bail!("expected exactly two nodes, found {}", self.nodes.len());
        }

        let deadline = Instant::now() + self.config.consensus_timeout;
        let mut last = String::new();

        while Instant::now() < deadline {
            self.ensure_nodes_alive()?;

            let state_a = self.fetch_live_nodes(&self.nodes[0].addr);
            let state_b = self.fetch_live_nodes(&self.nodes[1].addr);

            match (state_a, state_b) {
                (Ok(a), Ok(b)) => {
                    let a_live = a.contains(&self.nodes[0].id);
                    let b_live = b.contains(&self.nodes[1].id);
                    if a_live && b_live {
                        return Ok(());
                    }
                    last = format!("node-a live={a:?} node-b live={b:?}");
                }
                (Err(err_a), Err(err_b)) => {
                    last = format!("node-a error={err_a:#}; node-b error={err_b:#}");
                }
                (Err(err), Ok(state)) => {
                    last = format!("node-a error={err:#}; node-b live={state:?}");
                }
                (Ok(state), Err(err)) => {
                    last = format!("node-a live={state:?}; node-b error={err:#}");
                }
            }

            sleep(self.config.poll_interval).await;
        }

        bail!(
            "cluster did not become ready within {:?}: {}. logs: {}",
            self.config.consensus_timeout,
            last,
            self.log_paths()
        );
    }

    pub fn run_cli(&self, daemon_addr: &str, args: &[&str]) -> Result<String> {
        for attempt in 0..=CLI_CONNECT_TIMEOUT_RETRIES {
            let output = self
                .run_cli_output(daemon_addr, args)
                .with_context(|| format!("run cli command {:?}", args))?;

            if output.status.success() {
                return String::from_utf8(output.stdout).context("decode cli stdout");
            }

            let stderr = String::from_utf8_lossy(&output.stderr);
            if (stderr.contains("await daemon connect: timed out")
                || stderr.contains("read response: timed out"))
                && attempt < CLI_CONNECT_TIMEOUT_RETRIES
            {
                thread::sleep(CLI_CONNECT_TIMEOUT_BACKOFF);
                continue;
            }

            bail!(
                "cli command failed {:?}\nstatus={}\nstdout:\n{}\nstderr:\n{}\nlogs: {}",
                args,
                output.status,
                String::from_utf8_lossy(&output.stdout),
                stderr,
                self.log_paths(),
            );
        }

        unreachable!("retry loop should always return or bail")
    }

    pub async fn wait_for_cli_contains(
        &mut self,
        daemon_addr: &str,
        args: &[&str],
        needle: &str,
        timeout: Duration,
    ) -> Result<String> {
        let deadline = Instant::now() + timeout;
        let mut last_output = String::new();
        while Instant::now() < deadline {
            self.ensure_nodes_alive()?;
            last_output = self.run_cli(daemon_addr, args)?;
            if last_output.contains(needle) {
                return Ok(last_output);
            }
            sleep(self.config.poll_interval).await;
        }

        bail!(
            "timed out waiting for CLI output {:?} on {} to contain `{}`. last output:\n{}",
            args,
            daemon_addr,
            needle,
            last_output
        )
    }

    pub async fn wait_for_any_cli_contains(
        &mut self,
        daemon_addrs: &[&str],
        args: &[&str],
        needle: &str,
        timeout: Duration,
    ) -> Result<(String, String)> {
        let deadline = Instant::now() + timeout;
        let mut last_output = String::new();
        while Instant::now() < deadline {
            self.ensure_nodes_alive()?;
            for daemon_addr in daemon_addrs {
                let output = self.run_cli(daemon_addr, args)?;
                if output.contains(needle) {
                    return Ok(((*daemon_addr).to_string(), output));
                }
                last_output = output;
            }
            sleep(self.config.poll_interval).await;
        }

        bail!(
            "timed out waiting for CLI output {:?} to contain `{}` on any daemon. last output:\n{}",
            args,
            needle,
            last_output
        )
    }

    pub async fn mutate_control_plane(
        &self,
        daemon_addr: &str,
        idempotency_key: &str,
        mutation: Mutation,
    ) -> Result<MutateApiResponse> {
        let addr = daemon_addr.parse().context("parse daemon addr")?;
        let endpoint = self.build_query_endpoint()?;
        let connecting = endpoint
            .connect(addr, DEFAULT_SERVER_NAME)
            .context("connect daemon")?;
        let connection = timeout(DIRECT_QUERY_TIMEOUT, connecting)
            .await
            .map_err(|_| anyhow!("timed out"))
            .context("await daemon connect")??;
        let (mut send, mut recv) = timeout(DIRECT_QUERY_TIMEOUT, connection.open_bi())
            .await
            .map_err(|_| anyhow!("timed out"))
            .context("open QUIC stream")??;
        let frame = encode_request(
            Method::ControlMutate,
            1,
            &MutateApiRequest {
                idempotency_key: idempotency_key.to_string(),
                mutation,
            },
        )
        .context("encode control mutation")?;
        timeout(DIRECT_QUERY_TIMEOUT, write_framed(&mut send, &frame))
            .await
            .map_err(|_| anyhow!("timed out"))
            .context("write control mutation")??;
        let _ = send.finish();
        let frame = timeout(DIRECT_QUERY_TIMEOUT, read_framed(&mut recv))
            .await
            .map_err(|_| anyhow!("timed out"))
            .context("read control mutation response")??;
        connection.close(0u32.into(), b"done");
        endpoint.close(0u32.into(), b"done");

        let envelope = decode_envelope(&frame).context("decode response envelope")?;
        if is_error(&envelope) {
            let error = decode_error(&envelope).context("decode daemon error")?;
            bail!("daemon error {}: {}", error.code, error.message);
        }

        decode_payload(&envelope).context("decode mutate payload")
    }

    async fn query_control_plane_state(
        &self,
        daemon_addr: &str,
        allow_stale: bool,
    ) -> Result<ControlPlaneState> {
        let response = self
            .query_control_plane_value(daemon_addr, Query::ControlPlaneState, allow_stale)
            .await?;
        let DataValue::Bytes(bytes) = response else {
            bail!("control-plane state query returned non-bytes payload: {response:?}");
        };
        decode_rkyv(&bytes).context("decode control-plane state")
    }

    async fn query_control_plane_value(
        &self,
        daemon_addr: &str,
        query: Query,
        allow_stale: bool,
    ) -> Result<DataValue> {
        let addr = daemon_addr.parse().context("parse daemon addr")?;
        let endpoint = self.build_query_endpoint()?;
        let connecting = endpoint
            .connect(addr, DEFAULT_SERVER_NAME)
            .context("connect daemon")?;
        let connection = timeout(DIRECT_QUERY_TIMEOUT, connecting)
            .await
            .map_err(|_| anyhow!("timed out"))
            .context("await daemon connect")??;
        let (mut send, mut recv) = timeout(DIRECT_QUERY_TIMEOUT, connection.open_bi())
            .await
            .map_err(|_| anyhow!("timed out"))
            .context("open QUIC stream")??;
        let frame = encode_request(
            Method::ControlQuery,
            1,
            &QueryApiRequest { query, allow_stale },
        )
        .context("encode control query")?;
        timeout(DIRECT_QUERY_TIMEOUT, write_framed(&mut send, &frame))
            .await
            .map_err(|_| anyhow!("timed out"))
            .context("write control query")??;
        let _ = send.finish();
        let frame = timeout(DIRECT_QUERY_TIMEOUT, read_framed(&mut recv))
            .await
            .map_err(|_| anyhow!("timed out"))
            .context("read control query response")??;
        connection.close(0u32.into(), b"done");
        endpoint.close(0u32.into(), b"done");

        let envelope = decode_envelope(&frame).context("decode response envelope")?;
        if is_error(&envelope) {
            let error = decode_error(&envelope).context("decode daemon error")?;
            bail!("daemon error {}: {}", error.code, error.message);
        }
        let response: QueryApiResponse =
            decode_payload(&envelope).context("decode query payload")?;
        let Some(result) = response.result else {
            bail!(
                "query returned no result: {}",
                response
                    .error
                    .or(response.leader_hint)
                    .unwrap_or_else(|| "unknown error".to_string())
            );
        };
        Ok(result)
    }

    fn build_query_endpoint(&self) -> Result<Endpoint> {
        let bind = "127.0.0.1:0".parse().expect("client bind");
        let ca_cert = self.cert_dir.join("ca.crt");
        let client_cert = self.cert_dir.join("client.crt");
        let client_key = self.cert_dir.join("client.key");
        build_quic_client_endpoint(
            bind,
            ClientTlsPaths {
                ca_cert: &ca_cert,
                client_cert: Some(&client_cert),
                client_key: Some(&client_key),
            },
        )
    }

    fn fetch_live_nodes(&self, daemon_addr: &str) -> Result<BTreeSet<String>> {
        let output = self.run_cli_output(daemon_addr, &["nodes"])?;
        if !output.status.success() {
            bail!(
                "nodes failed on {}: {}",
                daemon_addr,
                String::from_utf8_lossy(&output.stderr)
            );
        }
        let stdout = String::from_utf8(output.stdout).context("decode nodes output")?;
        let mut live = BTreeSet::new();
        for line in stdout.lines() {
            let mut parts = line.split_whitespace();
            let Some(name) = parts.next() else {
                continue;
            };
            let is_live = parts.any(|part| part == "live=true");
            if is_live {
                live.insert(name.to_string());
            }
        }
        Ok(live)
    }

    fn run_cli_output(&self, daemon_addr: &str, args: &[&str]) -> Result<Output> {
        let mut command = Command::new(&self.cli_bin);
        command
            .arg("--ca-cert")
            .arg(self.cert_dir.join("ca.crt"))
            .arg("--client-cert")
            .arg(self.cert_dir.join("client.crt"))
            .arg("--client-key")
            .arg(self.cert_dir.join("client.key"))
            .arg("--daemon-addr")
            .arg(daemon_addr)
            .args(args)
            .current_dir(&self.root);

        command.output().context("spawn CLI")
    }

    fn ensure_nodes_alive(&mut self) -> Result<()> {
        for node in &mut self.nodes {
            if let Some(status) = node.child.try_wait().context("query node process status")? {
                bail!(
                    "node {} exited early with status {} (stdout={}, stderr={})",
                    node.id,
                    status,
                    node.stdout_log.display(),
                    node.stderr_log.display()
                );
            }
        }

        Ok(())
    }

    fn prepare_binaries(&self) -> Result<()> {
        let output = Command::new("cargo")
            .arg("build")
            .arg("-p")
            .arg("selium-runtime")
            .arg("-p")
            .arg("selium")
            .current_dir(&self.root)
            .output()
            .context("build runtime + cli binaries")?;
        ensure_success(&output, "cargo build -p selium-runtime -p selium")?;

        if !self.runtime_bin.exists() {
            bail!("runtime binary missing at {}", self.runtime_bin.display());
        }
        if !self.cli_bin.exists() {
            bail!("cli binary missing at {}", self.cli_bin.display());
        }

        Ok(())
    }

    fn build_example_wasm(&self, manifest_path: &str, expected_artifact: &Path) -> Result<()> {
        let manifest = self.root.join(manifest_path);
        let output = Command::new("cargo")
            .arg("build")
            .arg("--manifest-path")
            .arg(&manifest)
            .arg("--target")
            .arg("wasm32-unknown-unknown")
            .env("CARGO_TARGET_DIR", self.root.join("target"))
            .current_dir(&self.root)
            .output()
            .with_context(|| format!("build wasm module from {}", manifest.display()))?;

        ensure_success(&output, "cargo build example module wasm")?;

        if !expected_artifact.exists() {
            bail!(
                "expected wasm artifact at {} after build",
                expected_artifact.display()
            );
        }

        Ok(())
    }

    fn stage_module_for_all_nodes(&self, source: &Path, staged_name: &str) -> Result<()> {
        for node in &self.nodes {
            let target = node.work_dir.join("modules").join(staged_name);
            fs::copy(source, &target).with_context(|| {
                format!("copy module {} -> {}", source.display(), target.display())
            })?;
        }
        Ok(())
    }

    fn generate_certs(&self) -> Result<()> {
        let output = Command::new(&self.runtime_bin)
            .arg("generate-certs")
            .arg("--output-dir")
            .arg(&self.cert_dir)
            .current_dir(&self.root)
            .output()
            .context("generate runtime certs")?;
        ensure_success(&output, "generate certs")
    }

    fn spawn_nodes(&mut self) -> Result<()> {
        let ports = allocate_ports(4)?;
        let addr_a = format!("127.0.0.1:{}", ports[0]);
        let internal_addr_a = format!("127.0.0.1:{}", ports[1]);
        let addr_b = format!("127.0.0.1:{}", ports[2]);
        let internal_addr_b = format!("127.0.0.1:{}", ports[3]);
        let node_a = NodeStart {
            id: "node-a",
            port: ports[0],
            internal_addr: internal_addr_a,
            capacity_slots: 1,
            bootstrap_leader: true,
            peers: vec![format!("node-b={addr_b}@{DEFAULT_SERVER_NAME}")],
        };
        let node_b = NodeStart {
            id: "node-b",
            port: ports[2],
            internal_addr: internal_addr_b,
            capacity_slots: 2,
            bootstrap_leader: false,
            peers: vec![format!("node-a={addr_a}@{DEFAULT_SERVER_NAME}")],
        };

        self.nodes.push(self.spawn_node(node_a)?);
        self.nodes.push(self.spawn_node(node_b)?);
        Ok(())
    }

    fn spawn_node(&self, start: NodeStart<'_>) -> Result<NodeProcess> {
        let work_dir = self.base_dir.join(start.id);
        let modules_dir = work_dir.join("modules");
        let system_modules_dir = modules_dir.join("system");
        let logs_dir = self.base_dir.join("logs");
        fs::create_dir_all(&modules_dir)
            .with_context(|| format!("create module dir {}", modules_dir.display()))?;
        fs::create_dir_all(&system_modules_dir)
            .with_context(|| format!("create module dir {}", system_modules_dir.display()))?;
        fs::create_dir_all(&logs_dir)
            .with_context(|| format!("create logs dir {}", logs_dir.display()))?;

        let staged_control_plane = system_modules_dir.join(STAGED_CONTROL_PLANE_ARTIFACT);
        fs::copy(&self.control_plane_source, &staged_control_plane).with_context(|| {
            format!(
                "copy module {} -> {}",
                self.control_plane_source.display(),
                staged_control_plane.display()
            )
        })?;
        let staged_user_echo = modules_dir.join("rpc_echo_service.wasm");
        fs::copy(&self.user_echo_source, &staged_user_echo).with_context(|| {
            format!(
                "copy module {} -> {}",
                self.user_echo_source.display(),
                staged_user_echo.display()
            )
        })?;
        let staged_user_io = modules_dir.join("event_broadcast.wasm");
        fs::copy(&self.user_io_source, &staged_user_io).with_context(|| {
            format!(
                "copy module {} -> {}",
                self.user_io_source.display(),
                staged_user_io.display()
            )
        })?;

        let stdout_log = logs_dir.join(format!("{}.stdout.log", start.id));
        let stderr_log = logs_dir.join(format!("{}.stderr.log", start.id));
        let stdout = fs::File::create(&stdout_log)
            .with_context(|| format!("create {}", stdout_log.display()))?;
        let stderr = fs::File::create(&stderr_log)
            .with_context(|| format!("create {}", stderr_log.display()))?;

        let addr = format!("127.0.0.1:{}", start.port);
        let work_dir_arg = work_dir
            .strip_prefix(&self.root)
            .unwrap_or(work_dir.as_path());

        let mut cmd = Command::new(&self.runtime_bin);
        cmd.arg("--work-dir")
            .arg(work_dir_arg)
            .arg("daemon")
            .arg("--listen")
            .arg(&addr)
            .arg("--cp-node-id")
            .arg(start.id)
            .arg("--cp-public-addr")
            .arg(&addr)
            .arg("--cp-internal-addr")
            .arg(&start.internal_addr)
            .arg("--cp-state-dir")
            .arg("control-plane")
            .arg("--quic-ca")
            .arg(self.cert_dir.join("ca.crt"))
            .arg("--quic-cert")
            .arg(self.cert_dir.join("server.crt"))
            .arg("--quic-key")
            .arg(self.cert_dir.join("server.key"))
            .arg("--quic-peer-cert")
            .arg(self.cert_dir.join("client.crt"))
            .arg("--quic-peer-key")
            .arg(self.cert_dir.join("client.key"))
            .arg("--cp-heartbeat-interval-ms")
            .arg("500")
            .arg("--cp-capacity-slots")
            .arg(start.capacity_slots.to_string())
            .arg("--cp-server-name")
            .arg(DEFAULT_SERVER_NAME)
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .current_dir(&self.root);
        if start.bootstrap_leader {
            cmd.arg("--cp-bootstrap-leader");
        }
        for peer in &start.peers {
            cmd.arg("--cp-peer").arg(peer);
        }

        let child = cmd
            .spawn()
            .with_context(|| format!("spawn runtime daemon {}", start.id))?;

        Ok(NodeProcess {
            id: start.id.to_string(),
            addr,
            work_dir,
            stdout_log,
            stderr_log,
            child,
        })
    }

    fn log_paths(&self) -> String {
        self.nodes
            .iter()
            .map(|node| {
                format!(
                    "{} (work_dir={}, stdout={}, stderr={})",
                    node.id,
                    node.work_dir.display(),
                    node.stdout_log.display(),
                    node.stderr_log.display()
                )
            })
            .collect::<Vec<_>>()
            .join("; ")
    }
}

impl Drop for ClusterHarness {
    fn drop(&mut self) {
        for node in &mut self.nodes {
            let _ = node.child.kill();
            let _ = node.child.wait();
        }
    }
}

struct NodeStart<'a> {
    id: &'a str,
    port: u16,
    internal_addr: String,
    capacity_slots: u32,
    bootstrap_leader: bool,
    peers: Vec<String>,
}

fn repo_root() -> Result<PathBuf> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .ok_or_else(|| anyhow!("resolve repo root from {}", manifest_dir.display()))
}

fn allocate_ports(count: usize) -> Result<Vec<u16>> {
    let mut sockets = Vec::with_capacity(count);
    let mut ports = Vec::with_capacity(count);
    for _ in 0..count {
        let socket = UdpSocket::bind("127.0.0.1:0").context("allocate local port")?;
        let port = socket.local_addr().context("local addr")?.port();
        ports.push(port);
        sockets.push(socket);
    }
    drop(sockets);
    Ok(ports)
}

fn ensure_success(output: &Output, step: &str) -> Result<()> {
    if output.status.success() {
        return Ok(());
    }

    bail!(
        "{} failed\nstatus={}\nstdout:\n{}\nstderr:\n{}",
        step,
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}
