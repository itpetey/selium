use std::{
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use crate::wasmtime::runtime::{Error as WasmtimeError, WasmtimeProcessDriver};
use anyhow::{Context, Result, anyhow, bail};
use selium_abi::{
    AbiParam, AbiScalarType, AbiScalarValue, AbiSignature, AbiValue, Capability, EntrypointArg,
    EntrypointInvocation,
};
use selium_kernel::{
    Kernel, KernelError,
    registry::{Registry, ResourceHandle, ResourceId, ResourceType},
    spi::process::ProcessLifecycleCapability,
};
use selium_runtime_adaptor_microvm::MicroVmAdaptor;
use selium_runtime_adaptor_spi::{
    AdaptorKind, ExecutionProfile, ModuleSpec as AdaptorModuleSpec, RuntimeAdaptor,
};
use selium_runtime_adaptor_wasmtime::WasmtimeAdaptor;
use selium_runtime_network::NetworkService;
use selium_runtime_storage::StorageService;
use tokio::task::JoinHandle;
use tracing::info;

const DEFAULT_ENTRYPOINT: &str = "start";
const DEFAULT_ADAPTOR: AdaptorKind = AdaptorKind::Wasmtime;
const DEFAULT_PROFILE: ExecutionProfile = ExecutionProfile::Standard;

#[derive(Default)]
struct ModuleArgs {
    params: Vec<AbiParam>,
    args: Vec<EntrypointArg>,
}

#[derive(Debug)]
struct ModuleSpec {
    module_label: String,
    module_path: PathBuf,
    entrypoint: String,
    capabilities: Vec<Capability>,
    network_egress_profiles: Vec<String>,
    network_ingress_bindings: Vec<String>,
    storage_logs: Vec<String>,
    storage_blobs: Vec<String>,
    adaptor: AdaptorKind,
    profile: ExecutionProfile,
    params: Vec<AbiParam>,
    args: Vec<EntrypointArg>,
}

#[derive(Default)]
struct ModuleSpecBuilder {
    path: Option<String>,
    entrypoint: Option<String>,
    capabilities: Option<Vec<Capability>>,
    network_egress_profiles: Option<Vec<String>>,
    network_ingress_bindings: Option<Vec<String>>,
    storage_logs: Option<Vec<String>>,
    storage_blobs: Option<Vec<String>>,
    adaptor: Option<AdaptorKind>,
    profile: Option<ExecutionProfile>,
    params: Option<Vec<ParamKind>>,
    args: Option<Vec<Argument>>,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum ParamKind {
    I8,
    U8,
    I16,
    U16,
    I32,
    U32,
    I64,
    U64,
    F32,
    F64,
    Buffer,
    Utf8,
    Resource,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Argument {
    Typed { kind: ParamKind, value: String },
    Untyped(String),
}

impl ModuleSpecBuilder {
    fn is_empty(&self) -> bool {
        self.path.is_none()
            && self.entrypoint.is_none()
            && self.capabilities.is_none()
            && self.network_egress_profiles.is_none()
            && self.network_ingress_bindings.is_none()
            && self.storage_logs.is_none()
            && self.storage_blobs.is_none()
            && self.adaptor.is_none()
            && self.profile.is_none()
            && self.params.is_none()
            && self.args.is_none()
    }
}

impl ParamKind {
    fn from_label(label: &str) -> Option<Self> {
        match label.to_ascii_lowercase().as_str() {
            "i8" => Some(Self::I8),
            "u8" => Some(Self::U8),
            "i16" => Some(Self::I16),
            "u16" => Some(Self::U16),
            "i32" => Some(Self::I32),
            "u32" => Some(Self::U32),
            "i64" => Some(Self::I64),
            "u64" => Some(Self::U64),
            "f32" => Some(Self::F32),
            "f64" => Some(Self::F64),
            "buffer" | "bytes" | "byte" | "data" => Some(Self::Buffer),
            "utf8" | "utf-8" | "string" | "str" | "text" => Some(Self::Utf8),
            "resource" | "handle" => Some(Self::Resource),
            _ => None,
        }
    }
}

/// Read module specifications from CLI strings and start each module.
///
/// Input format per module: a `;`-delimited list of `key=value` entries. Required keys are
/// `path` and `capabilities`. Optional keys are `entrypoint` (defaults to `start`), `adaptor`
/// (`wasmtime`/`microvm`), `profile` (`standard`/`hardened`/`microvm`), `params`, and `args`.
/// The `args` value is a comma-separated list of values that may be prefixed with `TYPE:` to
/// infer parameter kinds. When `params` is omitted, every arg must be typed. The `path` must
/// be relative to `work_dir`.
///
/// Supported argument types: `i8`, `u8`, `i16`, `u16`, `i32`, `u32`, `i64`, `u64`, `f32`,
/// `f64`, `buffer`, `utf8`, `resource`. Buffer values support a `hex:` prefix to pass raw
/// bytes.
pub async fn spawn_from_cli(
    kernel: &Kernel,
    registry: &Arc<Registry>,
    work_dir: impl AsRef<Path>,
    specs: &[String],
) -> Result<Vec<ResourceId>> {
    let specs = parse_module_specs(specs, work_dir.as_ref())?;
    validate_runtime_grants(kernel, &specs).await?;
    let runtime = kernel.get::<WasmtimeProcessDriver>().ok_or_else(|| {
        WasmtimeError::Kernel(KernelError::Driver(
            "missing Wasmtime process driver in kernel".to_string(),
        ))
    })?;

    let mut processes = Vec::with_capacity(specs.len());
    for spec in specs {
        let process_id = spawn_module(runtime, registry, spec).await?;
        processes.push(process_id);
    }

    Ok(processes)
}

async fn validate_runtime_grants(kernel: &Kernel, specs: &[ModuleSpec]) -> Result<()> {
    let network = kernel
        .get::<NetworkService>()
        .ok_or_else(|| anyhow!("missing NetworkService in kernel"))?;
    let storage = kernel
        .get::<StorageService>()
        .ok_or_else(|| anyhow!("missing StorageService in kernel"))?;

    for spec in specs {
        network
            .validate_process_grants(
                &spec.network_egress_profiles,
                &spec.network_ingress_bindings,
            )
            .await
            .with_context(|| {
                format!("validate network grants for module `{}`", spec.module_label)
            })?;
        storage
            .validate_process_grants(&spec.storage_logs, &spec.storage_blobs)
            .await
            .with_context(|| {
                format!("validate storage grants for module `{}`", spec.module_label)
            })?;
    }

    Ok(())
}

pub async fn stop_process(
    kernel: &Kernel,
    registry: &Arc<Registry>,
    process_id: ResourceId,
) -> Result<()> {
    let runtime = kernel.get::<WasmtimeProcessDriver>().ok_or_else(|| {
        WasmtimeError::Kernel(KernelError::Driver(
            "missing Wasmtime process driver in kernel".to_string(),
        ))
    })?;

    let mut process = registry
        .remove(ResourceHandle::<
            JoinHandle<Result<Vec<AbiValue>, wasmtime::Error>>,
        >::new(process_id))
        .ok_or_else(|| anyhow!("process resource `{process_id}` not found"))?;

    runtime
        .stop(&mut process)
        .await
        .with_context(|| format!("stop process resource {process_id}"))?;
    Ok(())
}

fn parse_module_specs(specs: &[String], work_dir: &Path) -> Result<Vec<ModuleSpec>> {
    if specs.is_empty() {
        return Err(anyhow!("no module specifications provided"));
    }

    specs
        .iter()
        .enumerate()
        .map(|(index, spec)| {
            parse_module_spec(spec, work_dir)
                .with_context(|| format!("parse module specification {}", index + 1))
        })
        .collect()
}

fn parse_module_spec(raw: &str, work_dir: &Path) -> Result<ModuleSpec> {
    let mut builder = ModuleSpecBuilder::default();
    let normalized = raw.replace(';', "\n");

    for (index, raw_line) in normalized.lines().enumerate() {
        let line_no = index + 1;
        let line = raw_line.trim();

        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let (key, value) = line
            .split_once('=')
            .ok_or_else(|| anyhow!("entry {line_no}: expected key=value"))?;
        let key = key.trim();
        let value = value.trim();

        match key {
            "path" => {
                if builder.path.is_some() {
                    return Err(anyhow!("entry {line_no}: duplicate path"));
                }
                builder.path = Some(value.to_string());
            }
            "entrypoint" => {
                if builder.entrypoint.is_some() {
                    return Err(anyhow!("entry {line_no}: duplicate entrypoint"));
                }
                builder.entrypoint = Some(value.to_string());
            }
            "capabilities" => {
                if builder.capabilities.is_some() {
                    return Err(anyhow!("entry {line_no}: duplicate capabilities"));
                }
                builder.capabilities = Some(parse_capabilities(value)?);
            }
            "network_egress_profiles" | "network-egress-profiles" => {
                if builder.network_egress_profiles.is_some() {
                    return Err(anyhow!(
                        "entry {line_no}: duplicate network egress profiles"
                    ));
                }
                builder.network_egress_profiles = Some(parse_string_list(value)?);
            }
            "network_ingress_bindings" | "network-ingress-bindings" => {
                if builder.network_ingress_bindings.is_some() {
                    return Err(anyhow!(
                        "entry {line_no}: duplicate network ingress bindings"
                    ));
                }
                builder.network_ingress_bindings = Some(parse_string_list(value)?);
            }
            "storage_logs" | "storage-logs" => {
                if builder.storage_logs.is_some() {
                    return Err(anyhow!("entry {line_no}: duplicate storage logs"));
                }
                builder.storage_logs = Some(parse_string_list(value)?);
            }
            "storage_blobs" | "storage-blobs" => {
                if builder.storage_blobs.is_some() {
                    return Err(anyhow!("entry {line_no}: duplicate storage blobs"));
                }
                builder.storage_blobs = Some(parse_string_list(value)?);
            }
            "adaptor" => {
                if builder.adaptor.is_some() {
                    return Err(anyhow!("entry {line_no}: duplicate adaptor"));
                }
                builder.adaptor = Some(parse_adaptor_kind(value)?);
            }
            "profile" => {
                if builder.profile.is_some() {
                    return Err(anyhow!("entry {line_no}: duplicate profile"));
                }
                builder.profile = Some(parse_execution_profile(value)?);
            }
            "params" | "param" => {
                if builder.params.is_some() {
                    return Err(anyhow!("entry {line_no}: duplicate params"));
                }
                builder.params = Some(parse_params(value)?);
            }
            "args" => {
                if builder.args.is_some() {
                    return Err(anyhow!("entry {line_no}: duplicate args"));
                }
                builder.args = Some(parse_args(value)?);
            }
            _ => return Err(anyhow!("entry {line_no}: unknown key `{key}`")),
        }
    }

    if builder.is_empty() {
        return Err(anyhow!("module specification is empty"));
    }

    build_module_spec(builder, work_dir)
}

fn build_module_spec(builder: ModuleSpecBuilder, _work_dir: &Path) -> Result<ModuleSpec> {
    let path = builder
        .path
        .ok_or_else(|| anyhow!("module specification missing path"))?;
    let entrypoint = builder
        .entrypoint
        .unwrap_or_else(|| DEFAULT_ENTRYPOINT.to_string());
    let capabilities = builder.capabilities.unwrap_or_default();
    let network_egress_profiles = builder.network_egress_profiles.unwrap_or_default();
    let network_ingress_bindings = builder.network_ingress_bindings.unwrap_or_default();
    let storage_logs = builder.storage_logs.unwrap_or_default();
    let storage_blobs = builder.storage_blobs.unwrap_or_default();
    let adaptor = builder.adaptor.unwrap_or(DEFAULT_ADAPTOR);
    let profile = builder.profile.unwrap_or(DEFAULT_PROFILE);
    let args = builder.args.unwrap_or_default();
    let params = builder.params.unwrap_or_default();
    let (params, values) = resolve_arguments(params, args)?;
    let ModuleArgs { params, args } = build_module_args(params, values)?;

    if path.trim().is_empty() {
        return Err(anyhow!("module path must not be empty"));
    }
    if entrypoint.trim().is_empty() {
        return Err(anyhow!("entrypoint must not be empty"));
    }
    if capabilities.is_empty() {
        return Err(anyhow!("capabilities list must not be empty"));
    }

    let module_path = normalize_module_id(parse_relative_path(&path)?);
    if module_path.as_os_str().is_empty() {
        return Err(anyhow!("module path must reference a file"));
    }

    let module_spec = ModuleSpec {
        module_label: path,
        module_path,
        entrypoint,
        capabilities,
        network_egress_profiles,
        network_ingress_bindings,
        storage_logs,
        storage_blobs,
        adaptor,
        profile,
        params,
        args,
    };

    validate_module_spec(&module_spec)?;
    Ok(module_spec)
}

fn parse_relative_path(raw: &str) -> Result<PathBuf> {
    let path = Path::new(raw);
    if path.is_absolute() {
        return Err(anyhow!("module path must be relative"));
    }

    if path.components().any(|component| {
        matches!(
            component,
            Component::Prefix(_) | Component::RootDir | Component::ParentDir
        )
    }) {
        return Err(anyhow!("module path must not contain parent segments"));
    }

    Ok(path.to_path_buf())
}

fn normalize_module_id(path: PathBuf) -> PathBuf {
    use std::ffi::OsStr;

    let mut components = path.components();
    let stripped = match components.next() {
        Some(Component::Normal(first)) if first == OsStr::new("modules") => {
            components.as_path().to_path_buf()
        }
        _ => path,
    };
    stripped
}

fn parse_capabilities(raw: &str) -> Result<Vec<Capability>> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("capabilities list must not be empty"));
    }

    let mut caps = Vec::new();
    for item in trimmed.split(',') {
        let item = item.trim();
        if item.is_empty() {
            return Err(anyhow!("capability entry must not be empty"));
        }
        let capability = match item.to_ascii_lowercase().as_str() {
            "sessionlifecycle" | "session_lifecycle" | "session-lifecycle" => {
                Capability::SessionLifecycle
            }
            "processlifecycle" | "process_lifecycle" | "process-lifecycle" => {
                Capability::ProcessLifecycle
            }
            "timeread" | "time_read" | "time-read" => Capability::TimeRead,
            "sharedmemory" | "shared_memory" | "shared-memory" => Capability::SharedMemory,
            "queuelifecycle" | "queue_lifecycle" | "queue-lifecycle" => Capability::QueueLifecycle,
            "queuewriter" | "queue_writer" | "queue-writer" => Capability::QueueWriter,
            "queuereader" | "queue_reader" | "queue-reader" => Capability::QueueReader,
            "networklifecycle" | "network_lifecycle" | "network-lifecycle" => {
                Capability::NetworkLifecycle
            }
            "networkconnect" | "network_connect" | "network-connect" => Capability::NetworkConnect,
            "networkaccept" | "network_accept" | "network-accept" => Capability::NetworkAccept,
            "networkstreamread" | "network_stream_read" | "network-stream-read" => {
                Capability::NetworkStreamRead
            }
            "networkstreamwrite" | "network_stream_write" | "network-stream-write" => {
                Capability::NetworkStreamWrite
            }
            "networkrpcclient" | "network_rpc_client" | "network-rpc-client" => {
                Capability::NetworkRpcClient
            }
            "networkrpcserver" | "network_rpc_server" | "network-rpc-server" => {
                Capability::NetworkRpcServer
            }
            "storagelifecycle" | "storage_lifecycle" | "storage-lifecycle" => {
                Capability::StorageLifecycle
            }
            "storagelogread" | "storage_log_read" | "storage-log-read" => {
                Capability::StorageLogRead
            }
            "storagelogwrite" | "storage_log_write" | "storage-log-write" => {
                Capability::StorageLogWrite
            }
            "storageblobread" | "storage_blob_read" | "storage-blob-read" => {
                Capability::StorageBlobRead
            }
            "storageblobwrite" | "storage_blob_write" | "storage-blob-write" => {
                Capability::StorageBlobWrite
            }
            _ => return Err(anyhow!("unknown capability `{item}`")),
        };

        if !caps.contains(&capability) {
            caps.push(capability);
        }
    }

    Ok(caps)
}

fn parse_string_list(raw: &str) -> Result<Vec<String>> {
    if raw.trim().is_empty() {
        return Ok(Vec::new());
    }

    Ok(raw
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(ToString::to_string)
        .collect())
}

fn parse_adaptor_kind(raw: &str) -> Result<AdaptorKind> {
    raw.parse::<AdaptorKind>()
        .map_err(|_| anyhow!("unknown adaptor `{raw}`"))
}

fn parse_execution_profile(raw: &str) -> Result<ExecutionProfile> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "standard" => Ok(ExecutionProfile::Standard),
        "hardened" => Ok(ExecutionProfile::Hardened),
        "microvm" => Ok(ExecutionProfile::Microvm),
        _ => Err(anyhow!("unknown execution profile `{raw}`")),
    }
}

fn parse_params(raw: &str) -> Result<Vec<ParamKind>> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("params list must not be empty"));
    }

    let mut params = Vec::new();
    for (index, item) in trimmed.split(',').enumerate() {
        let item = item.trim();
        if item.is_empty() {
            return Err(anyhow!("param {} must not be empty", index + 1));
        }
        let kind =
            ParamKind::from_label(item).ok_or_else(|| anyhow!("unknown param kind `{item}`"))?;
        params.push(kind);
    }

    Ok(params)
}

fn parse_args(raw: &str) -> Result<Vec<Argument>> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }

    let mut args = Vec::new();
    for (index, item) in trimmed.split(',').enumerate() {
        let item = item.trim();
        let arg_index = index + 1;
        if item.is_empty() {
            return Err(anyhow!("arg {arg_index} must not be empty"));
        }
        args.push(parse_argument(item));
    }

    Ok(args)
}

fn parse_argument(raw: &str) -> Argument {
    if let Some((label, value)) = raw.split_once(':')
        && let Some(kind) = ParamKind::from_label(label)
    {
        return Argument::Typed {
            kind,
            value: value.to_owned(),
        };
    }

    Argument::Untyped(raw.to_owned())
}

fn resolve_arguments(
    params: Vec<ParamKind>,
    args: Vec<Argument>,
) -> Result<(Vec<ParamKind>, Vec<String>)> {
    if params.is_empty() {
        if args.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }
        let mut inferred_params = Vec::with_capacity(args.len());
        let mut values = Vec::with_capacity(args.len());
        for (index, arg) in args.into_iter().enumerate() {
            match arg {
                Argument::Typed { kind, value } => {
                    inferred_params.push(kind);
                    values.push(value);
                }
                Argument::Untyped(value) => {
                    bail!(
                        "argument {} is missing a type. Supply params or prefix the value with TYPE:, e.g. string:{value}",
                        index + 1
                    );
                }
            }
        }
        return Ok((inferred_params, values));
    }

    if args.len() != params.len() {
        bail!(
            "argument count mismatch: expected {} arguments, got {}",
            params.len(),
            args.len()
        );
    }

    let mut values = Vec::with_capacity(args.len());
    for (index, (expected, arg)) in params.iter().zip(args.into_iter()).enumerate() {
        match arg {
            Argument::Typed { kind, value } => {
                if *expected != kind {
                    bail!(
                        "argument {} declared as {:?} but params expects {:?}",
                        index + 1,
                        kind,
                        expected
                    );
                }
                values.push(value);
            }
            Argument::Untyped(value) => values.push(value),
        }
    }

    Ok((params, values))
}

fn build_module_args(params: Vec<ParamKind>, values: Vec<String>) -> Result<ModuleArgs> {
    let mut abi_params = Vec::with_capacity(params.len());
    let mut args = Vec::with_capacity(params.len());

    for (index, (kind, value)) in params.iter().zip(values.iter()).enumerate() {
        abi_params.push(map_param(kind));
        let arg = parse_entrypoint_arg(kind, value)
            .with_context(|| format!("parse argument {}", index + 1))?;
        args.push(arg);
    }

    Ok(ModuleArgs {
        params: abi_params,
        args,
    })
}

fn map_param(kind: &ParamKind) -> AbiParam {
    match kind {
        ParamKind::I8 => AbiParam::Scalar(AbiScalarType::I8),
        ParamKind::U8 => AbiParam::Scalar(AbiScalarType::U8),
        ParamKind::I16 => AbiParam::Scalar(AbiScalarType::I16),
        ParamKind::U16 => AbiParam::Scalar(AbiScalarType::U16),
        ParamKind::I32 => AbiParam::Scalar(AbiScalarType::I32),
        ParamKind::U32 => AbiParam::Scalar(AbiScalarType::U32),
        ParamKind::I64 => AbiParam::Scalar(AbiScalarType::I64),
        ParamKind::U64 | ParamKind::Resource => AbiParam::Scalar(AbiScalarType::U64),
        ParamKind::F32 => AbiParam::Scalar(AbiScalarType::F32),
        ParamKind::F64 => AbiParam::Scalar(AbiScalarType::F64),
        ParamKind::Buffer | ParamKind::Utf8 => AbiParam::Buffer,
    }
}

fn parse_entrypoint_arg(kind: &ParamKind, raw: &str) -> Result<EntrypointArg> {
    match kind {
        ParamKind::I8 => {
            let value = raw.parse::<i8>().context("parse i8 argument")?;
            Ok(EntrypointArg::Scalar(AbiScalarValue::I8(value)))
        }
        ParamKind::U8 => {
            let value = raw.parse::<u8>().context("parse u8 argument")?;
            Ok(EntrypointArg::Scalar(AbiScalarValue::U8(value)))
        }
        ParamKind::I16 => {
            let value = raw.parse::<i16>().context("parse i16 argument")?;
            Ok(EntrypointArg::Scalar(AbiScalarValue::I16(value)))
        }
        ParamKind::U16 => {
            let value = raw.parse::<u16>().context("parse u16 argument")?;
            Ok(EntrypointArg::Scalar(AbiScalarValue::U16(value)))
        }
        ParamKind::I32 => {
            let value = raw.parse::<i32>().context("parse i32 argument")?;
            Ok(EntrypointArg::Scalar(AbiScalarValue::I32(value)))
        }
        ParamKind::U32 => {
            let value = raw.parse::<u32>().context("parse u32 argument")?;
            Ok(EntrypointArg::Scalar(AbiScalarValue::U32(value)))
        }
        ParamKind::I64 => {
            let value = raw.parse::<i64>().context("parse i64 argument")?;
            Ok(EntrypointArg::Scalar(AbiScalarValue::I64(value)))
        }
        ParamKind::U64 => {
            let value = raw.parse::<u64>().context("parse u64 argument")?;
            Ok(EntrypointArg::Scalar(AbiScalarValue::U64(value)))
        }
        ParamKind::F32 => {
            let value = raw.parse::<f32>().context("parse f32 argument")?;
            Ok(EntrypointArg::Scalar(AbiScalarValue::F32(value)))
        }
        ParamKind::F64 => {
            let value = raw.parse::<f64>().context("parse f64 argument")?;
            Ok(EntrypointArg::Scalar(AbiScalarValue::F64(value)))
        }
        ParamKind::Buffer => {
            let bytes = parse_buffer_bytes(raw).context("parse buffer argument")?;
            Ok(EntrypointArg::Buffer(bytes))
        }
        ParamKind::Utf8 => Ok(EntrypointArg::Buffer(raw.as_bytes().to_vec())),
        ParamKind::Resource => {
            let value = raw.parse::<u64>().context("parse resource handle")?;
            Ok(EntrypointArg::Resource(value))
        }
    }
}

fn parse_buffer_bytes(raw: &str) -> Result<Vec<u8>> {
    let Some(hex) = raw.strip_prefix("hex:") else {
        return Ok(raw.as_bytes().to_vec());
    };

    decode_hex(hex)
}

fn decode_hex(mut raw: &str) -> Result<Vec<u8>> {
    if let Some(trimmed) = raw.strip_prefix("0x") {
        raw = trimmed;
    }

    let mut cleaned = Vec::with_capacity(raw.len());
    for ch in raw.bytes() {
        if ch.is_ascii_whitespace() || ch == b'_' {
            continue;
        }
        cleaned.push(ch);
    }

    if cleaned.len() % 2 != 0 {
        bail!("hex payload must have an even number of digits");
    }

    let mut out = Vec::with_capacity(cleaned.len() / 2);
    for pair in cleaned.chunks_exact(2) {
        let hi = hex_digit(pair[0])?;
        let lo = hex_digit(pair[1])?;
        out.push((hi << 4) | lo);
    }

    Ok(out)
}

fn hex_digit(byte: u8) -> Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => bail!("invalid hex digit {byte:?}"),
    }
}

async fn spawn_module(
    runtime: &WasmtimeProcessDriver,
    registry: &Arc<Registry>,
    spec: ModuleSpec,
) -> Result<ResourceId> {
    let process_id = registry
        .reserve(None, ResourceType::Process)
        .map_err(KernelError::from)
        .context("reserve process id")?;

    let ModuleSpec {
        module_label,
        module_path,
        entrypoint,
        capabilities,
        network_egress_profiles,
        network_ingress_bindings,
        storage_logs,
        storage_blobs,
        adaptor,
        profile,
        params,
        args,
    } = spec;

    info!(
        module = module_label,
        adaptor = %adaptor,
        profile = ?profile,
        "spawning module"
    );

    if adaptor != AdaptorKind::Wasmtime {
        bail!("adaptor `{adaptor}` is configured but not executable in this runtime build");
    }

    let entrypoint_invocation =
        EntrypointInvocation::new(AbiSignature::new(params, Vec::new()), args)
            .with_context(|| format!("build entrypoint invocation for {module_label}"))?;

    let module_id = module_path.to_str().ok_or_else(|| {
        WasmtimeError::Kernel(KernelError::Driver(format!(
            "module path for {module_label} is not valid UTF-8"
        )))
    })?;

    if let Err(err) = runtime
        .start(
            registry,
            process_id,
            module_id,
            &entrypoint,
            capabilities,
            network_egress_profiles,
            network_ingress_bindings,
            storage_logs,
            storage_blobs,
            entrypoint_invocation,
        )
        .await
    {
        registry.discard(process_id);
        return Err(err).with_context(|| format!("start module {module_label}"));
    }

    Ok(process_id)
}

fn validate_module_spec(spec: &ModuleSpec) -> Result<()> {
    let adaptor = adaptor_for_kind(spec.adaptor);
    let module_id = spec
        .module_path
        .to_str()
        .ok_or_else(|| anyhow!("module path for `{}` is not valid UTF-8", spec.module_label))?;

    let adaptor_spec = AdaptorModuleSpec {
        module_id: module_id.to_string(),
        entrypoint: spec.entrypoint.clone(),
        capabilities: spec.capabilities.clone(),
        profile: spec.profile,
    };
    adaptor.validate(&adaptor_spec).map_err(|err| {
        anyhow!(
            "module `{}` failed adaptor validation: {err}",
            spec.module_label
        )
    })?;
    if !adaptor.executable() {
        bail!(
            "adaptor `{}` is configured but marked non-executable on this node",
            adaptor.kind()
        );
    }
    Ok(())
}

fn adaptor_for_kind(kind: AdaptorKind) -> Box<dyn RuntimeAdaptor> {
    match kind {
        AdaptorKind::Wasmtime => Box::new(WasmtimeAdaptor),
        AdaptorKind::Microvm => Box::new(MicroVmAdaptor),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use selium_runtime_network::NetworkService;

    #[test]
    fn parse_relative_path_accepts_safe_paths() {
        let parsed = parse_relative_path("modules/echo.wasm").expect("parse");
        assert_eq!(parsed, PathBuf::from("modules/echo.wasm"));
    }

    #[test]
    fn parse_relative_path_rejects_absolute_and_parent_paths() {
        assert!(parse_relative_path("/tmp/module.wasm").is_err());
        assert!(parse_relative_path("../module.wasm").is_err());
    }

    #[test]
    fn parse_capabilities_accepts_aliases_and_deduplicates() {
        let caps = parse_capabilities("time_read, time-read, shared-memory").expect("caps");
        assert_eq!(caps, vec![Capability::TimeRead, Capability::SharedMemory]);
    }

    #[test]
    fn parse_params_and_args_detect_invalid_entries() {
        assert!(parse_params("i32,unknown").is_err());
        assert!(parse_args("i32:1,").is_err());
    }

    #[test]
    fn resolve_arguments_infers_types_when_params_are_omitted() {
        let args = vec![
            Argument::Typed {
                kind: ParamKind::I32,
                value: "1".to_string(),
            },
            Argument::Typed {
                kind: ParamKind::Utf8,
                value: "hello".to_string(),
            },
        ];

        let (params, values) = resolve_arguments(Vec::new(), args).expect("resolve");
        assert_eq!(params, vec![ParamKind::I32, ParamKind::Utf8]);
        assert_eq!(values, vec!["1".to_string(), "hello".to_string()]);
    }

    #[test]
    fn resolve_arguments_rejects_untyped_values_without_params() {
        let err = resolve_arguments(Vec::new(), vec![Argument::Untyped("x".to_string())])
            .expect_err("expected failure");
        assert!(err.to_string().contains("missing a type"));
    }

    #[test]
    fn parse_entrypoint_arg_supports_resource_and_hex_buffer() {
        let resource = parse_entrypoint_arg(&ParamKind::Resource, "9").expect("resource");
        assert_eq!(resource, EntrypointArg::Resource(9));

        let buffer = parse_entrypoint_arg(&ParamKind::Buffer, "hex:0x41_42").expect("buffer");
        assert_eq!(buffer, EntrypointArg::Buffer(vec![0x41, 0x42]));
    }

    #[test]
    fn parse_hex_rejects_odd_digit_count() {
        let err = decode_hex("abc").expect_err("odd hex should fail");
        assert!(err.to_string().contains("even number of digits"));
    }

    #[test]
    fn parse_module_spec_applies_defaults() {
        let work_dir = Path::new(".");
        let spec =
            parse_module_spec("path=mod.wasm;capabilities=time_read", work_dir).expect("spec");

        assert_eq!(spec.module_label, "mod.wasm");
        assert_eq!(spec.module_path, PathBuf::from("mod.wasm"));
        assert_eq!(spec.entrypoint, DEFAULT_ENTRYPOINT);
        assert_eq!(spec.adaptor, AdaptorKind::Wasmtime);
        assert_eq!(spec.profile, ExecutionProfile::Standard);
        assert_eq!(spec.capabilities, vec![Capability::TimeRead]);
        assert_eq!(spec.params, Vec::<AbiParam>::new());
        assert_eq!(spec.args, Vec::<EntrypointArg>::new());
    }

    #[test]
    fn parse_module_spec_strips_modules_prefix() {
        let work_dir = Path::new(".");
        let spec = parse_module_spec("path=modules/demo.wasm;capabilities=time_read", work_dir)
            .expect("spec");
        assert_eq!(spec.module_path, PathBuf::from("demo.wasm"));
    }

    #[test]
    fn parse_module_spec_rejects_non_executable_adaptor() {
        let work_dir = Path::new(".");
        let err = parse_module_spec(
            "path=image.oci;capabilities=time_read;adaptor=microvm;profile=microvm",
            work_dir,
        )
        .expect_err("microvm should not be executable");
        assert!(err.to_string().contains("non-executable"));
    }

    #[test]
    fn parse_module_specs_requires_input() {
        let specs = parse_module_specs(&[], Path::new("."));
        assert!(specs.is_err());
    }

    #[tokio::test]
    async fn validate_runtime_grants_rejects_unknown_profile() {
        let mut builder = Kernel::build();
        let network = builder.add_capability(NetworkService::new());
        let storage = builder.add_capability(StorageService::new());
        network
            .register_egress_profile(selium_runtime_network::NetworkEgressProfile {
                name: "known".to_string(),
                protocol: selium_abi::NetworkProtocol::Http,
                interactions: vec![selium_abi::InteractionKind::Rpc],
                allowed_authorities: vec!["api.example.com:443".to_string()],
                ca_cert_path: PathBuf::from("certs/ca.crt"),
                client_cert_path: None,
                client_key_path: None,
            })
            .await;
        storage
            .register_log(selium_runtime_storage::StorageLogDefinition {
                name: "known-log".to_string(),
                path: PathBuf::from("state/known-log.rkyv"),
                retention: selium_io_durability::RetentionPolicy::default(),
            })
            .await;
        let kernel = builder.build().expect("build kernel");

        let spec = parse_module_spec(
            "path=demo.wasm;capabilities=time_read;network-egress-profiles=missing",
            Path::new("."),
        )
        .expect("spec");

        let err = validate_runtime_grants(&kernel, &[spec])
            .await
            .expect_err("expected validation failure");
        let rendered = format!("{err:#}");
        assert!(rendered.contains("demo.wasm"));
        assert!(rendered.contains("not registered"));
    }
}
