use std::{
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use crate::wasmtime::runtime::{Error as WasmtimeError, WasmtimeProcessDriver};
use anyhow::{Context, Result, anyhow, bail};
use selium_abi::{
    AbiParam, AbiScalarType, AbiScalarValue, AbiSignature, Capability, EntrypointArg,
    EntrypointInvocation,
};
use selium_kernel::{
    Kernel, KernelError,
    registry::{Registry, ResourceId, ResourceType},
    spi::process::ProcessLifecycleCapability,
};
use tracing::info;

const DEFAULT_ENTRYPOINT: &str = "start";

#[derive(Default)]
struct ModuleArgs {
    params: Vec<AbiParam>,
    args: Vec<EntrypointArg>,
}

struct ModuleSpec {
    module_label: String,
    module_path: PathBuf,
    entrypoint: String,
    capabilities: Vec<Capability>,
    params: Vec<AbiParam>,
    args: Vec<EntrypointArg>,
}

#[derive(Default)]
struct ModuleSpecBuilder {
    path: Option<String>,
    entrypoint: Option<String>,
    capabilities: Option<Vec<Capability>>,
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
/// `path` and `capabilities`. Optional keys are `entrypoint` (defaults to `start`), `params`,
/// and `args`. The `args` value is a comma-separated list of values that may be prefixed with
/// `TYPE:` to infer parameter kinds. When `params` is omitted, every arg must be typed. The
/// `path` must be relative to `work_dir`.
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

fn build_module_spec(builder: ModuleSpecBuilder, work_dir: &Path) -> Result<ModuleSpec> {
    let path = builder
        .path
        .ok_or_else(|| anyhow!("module specification missing path"))?;
    let entrypoint = builder
        .entrypoint
        .unwrap_or_else(|| DEFAULT_ENTRYPOINT.to_string());
    let capabilities = builder.capabilities.unwrap_or_default();
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

    let module_path = work_dir.join(parse_relative_path(&path)?);

    Ok(ModuleSpec {
        module_label: path,
        module_path,
        entrypoint,
        capabilities,
        params,
        args,
    })
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
            "singletonregistry" | "singleton_registry" | "singleton-registry" => {
                Capability::SingletonRegistry
            }
            "singletonlookup" | "singleton_lookup" | "singleton-lookup" => {
                Capability::SingletonLookup
            }
            "timeread" | "time_read" | "time-read" => Capability::TimeRead,
            "sharedmemory" | "shared_memory" | "shared-memory" => Capability::SharedMemory,
            _ => return Err(anyhow!("unknown capability `{item}`")),
        };

        if !caps.contains(&capability) {
            caps.push(capability);
        }
    }

    Ok(caps)
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
        params,
        args,
    } = spec;

    info!(module = module_label, "spawning module");

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
            entrypoint_invocation,
        )
        .await
    {
        registry.discard(process_id);
        return Err(err).with_context(|| format!("start module {module_label}"));
    }

    Ok(process_id)
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(spec.entrypoint, DEFAULT_ENTRYPOINT);
        assert_eq!(spec.capabilities, vec![Capability::TimeRead]);
        assert_eq!(spec.params, Vec::<AbiParam>::new());
        assert_eq!(spec.args, Vec::<EntrypointArg>::new());
    }

    #[test]
    fn parse_module_specs_requires_input() {
        let specs = parse_module_specs(&[], Path::new("."));
        assert!(specs.is_err());
    }
}
