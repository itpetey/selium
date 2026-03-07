//! Control-plane contracts, IDL parsing, registry, and desired-state resources.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::Result;
use rkyv::{Archive, Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ContractRef {
    pub namespace: String,
    pub name: String,
    pub version: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum ContractKind {
    Event,
    Service,
    Stream,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum DeliveryGuarantee {
    AtLeastOnce,
    AtMostOnce,
    ExactlyOnce,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct SchemaField {
    pub name: String,
    pub ty: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct SchemaDef {
    pub name: String,
    pub fields: Vec<SchemaField>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct EventDef {
    pub name: String,
    pub payload_schema: String,
    pub partitions: u16,
    pub retention: String,
    pub delivery: DeliveryGuarantee,
    pub replay_enabled: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum ServiceBodyMode {
    None,
    Buffered,
    Stream,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ServiceBodyDef {
    pub mode: ServiceBodyMode,
    pub schema: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ServiceFieldBinding {
    pub field: String,
    pub target: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ServiceDef {
    pub name: String,
    pub request_schema: String,
    pub response_schema: String,
    pub protocol: Option<String>,
    pub method: Option<String>,
    pub path: Option<String>,
    pub request_headers: Vec<ServiceFieldBinding>,
    pub request_body: ServiceBodyDef,
    pub response_body: ServiceBodyDef,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct StreamDef {
    pub name: String,
    pub payload_schema: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ContractPackage {
    pub package: String,
    pub namespace: String,
    pub version: String,
    pub schemas: Vec<SchemaDef>,
    pub events: Vec<EventDef>,
    pub services: Vec<ServiceDef>,
    pub streams: Vec<StreamDef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ResolvedContract {
    pub kind: ContractKind,
    pub schema: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize, Default)]
#[rkyv(bytecheck())]
pub struct ContractRegistry {
    /// namespace -> version -> package
    pub packages: BTreeMap<String, BTreeMap<String, ContractPackage>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum IsolationProfile {
    Standard,
    Hardened,
    Microvm,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct DeploymentSpec {
    pub app: String,
    pub module: String,
    pub replicas: u32,
    pub contracts: Vec<ContractRef>,
    pub isolation: IsolationProfile,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct PipelineEndpoint {
    pub app: String,
    pub contract: ContractRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct PipelineEdge {
    pub from: PipelineEndpoint,
    pub to: PipelineEndpoint,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct PipelineSpec {
    pub name: String,
    pub namespace: String,
    pub edges: Vec<PipelineEdge>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct NodeSpec {
    pub name: String,
    pub capacity_slots: u32,
    pub supported_isolation: Vec<IsolationProfile>,
    pub daemon_addr: String,
    pub daemon_server_name: String,
    pub last_heartbeat_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize, Default)]
#[rkyv(bytecheck())]
pub struct ControlPlaneState {
    pub registry: ContractRegistry,
    pub deployments: BTreeMap<String, DeploymentSpec>,
    pub pipelines: BTreeMap<String, PipelineSpec>,
    pub nodes: BTreeMap<String, NodeSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompatibilityReport {
    pub previous_version: String,
    pub current_version: String,
    pub warnings: Vec<String>,
}

#[derive(Debug, Error)]
pub enum ApiError {
    #[error("invalid contract reference")]
    InvalidContract,
    #[error("parse error: {0}")]
    Parse(String),
    #[error("duplicate package version `{version}` for namespace `{namespace}`")]
    DuplicateVersion { namespace: String, version: String },
    #[error("incompatible package `{namespace}` {from} -> {to}: {reason}")]
    Incompatible {
        namespace: String,
        from: String,
        to: String,
        reason: String,
    },
    #[error("unknown deployment `{0}`")]
    UnknownDeployment(String),
    #[error("unknown node `{0}`")]
    UnknownNode(String),
    #[error("invalid deployment `{0}`")]
    InvalidDeployment(String),
    #[error("invalid node `{0}`")]
    InvalidNode(String),
}

impl ContractRegistry {
    pub fn register_package(
        &mut self,
        package: ContractPackage,
    ) -> std::result::Result<CompatibilityReport, ApiError> {
        let namespace = package.namespace.clone();
        let version = package.version.clone();

        let versions = self.packages.entry(namespace.clone()).or_default();
        if versions.contains_key(&version) {
            return Err(ApiError::DuplicateVersion { namespace, version });
        }

        let mut report = CompatibilityReport {
            previous_version: String::new(),
            current_version: package.version.clone(),
            warnings: Vec::new(),
        };

        if let Some((prev_version, prev_pkg)) = highest_version(versions) {
            ensure_compatible(prev_pkg, &package).map_err(|reason| ApiError::Incompatible {
                namespace: package.namespace.clone(),
                from: prev_version.to_string(),
                to: package.version.clone(),
                reason,
            })?;
            report.previous_version = prev_version.to_string();
            if prev_pkg.events.len() != package.events.len() {
                report
                    .warnings
                    .push("event set changed between versions".to_string());
            }
        }

        versions.insert(package.version.clone(), package);
        Ok(report)
    }

    pub fn resolve(&self, contract: &ContractRef) -> Option<ResolvedContract> {
        let package = self
            .packages
            .get(&contract.namespace)?
            .get(&contract.version)?;

        if let Some(event) = package
            .events
            .iter()
            .find(|event| event.name == contract.name)
        {
            return Some(ResolvedContract {
                kind: ContractKind::Event,
                schema: Some(event.payload_schema.clone()),
            });
        }

        if let Some(service) = package
            .services
            .iter()
            .find(|service| service.name == contract.name)
        {
            return Some(ResolvedContract {
                kind: ContractKind::Service,
                schema: Some(service.request_schema.clone()),
            });
        }

        if let Some(stream) = package
            .streams
            .iter()
            .find(|stream| stream.name == contract.name)
        {
            return Some(ResolvedContract {
                kind: ContractKind::Stream,
                schema: Some(stream.payload_schema.clone()),
            });
        }

        None
    }

    pub fn has_contract(&self, contract: &ContractRef) -> bool {
        self.resolve(contract).is_some()
    }
}

impl ControlPlaneState {
    pub fn new_local_default() -> Self {
        let mut nodes = BTreeMap::new();
        nodes.insert(
            "local-node".to_string(),
            NodeSpec {
                name: "local-node".to_string(),
                capacity_slots: 64,
                supported_isolation: vec![
                    IsolationProfile::Standard,
                    IsolationProfile::Hardened,
                    IsolationProfile::Microvm,
                ],
                daemon_addr: "127.0.0.1:7100".to_string(),
                daemon_server_name: "localhost".to_string(),
                last_heartbeat_ms: 0,
            },
        );

        Self {
            nodes,
            ..Self::default()
        }
    }

    pub fn upsert_deployment(
        &mut self,
        deployment: DeploymentSpec,
    ) -> std::result::Result<(), ApiError> {
        if deployment.app.trim().is_empty() {
            return Err(ApiError::InvalidDeployment(
                "app name must not be empty".to_string(),
            ));
        }
        if deployment.module.trim().is_empty() {
            return Err(ApiError::InvalidDeployment(
                "module must not be empty".to_string(),
            ));
        }
        if deployment.replicas == 0 {
            return Err(ApiError::InvalidDeployment(
                "replicas must be > 0".to_string(),
            ));
        }

        self.deployments.insert(deployment.app.clone(), deployment);
        Ok(())
    }

    pub fn set_scale(&mut self, app: &str, replicas: u32) -> std::result::Result<(), ApiError> {
        let deployment = self
            .deployments
            .get_mut(app)
            .ok_or_else(|| ApiError::UnknownDeployment(app.to_string()))?;
        deployment.replicas = replicas.max(1);
        Ok(())
    }

    pub fn upsert_pipeline(&mut self, pipeline: PipelineSpec) {
        self.pipelines.insert(pipeline.name.clone(), pipeline);
    }

    pub fn upsert_node(&mut self, node: NodeSpec) -> std::result::Result<(), ApiError> {
        if node.name.trim().is_empty() {
            return Err(ApiError::InvalidNode(
                "node name must not be empty".to_string(),
            ));
        }
        if node.daemon_addr.trim().is_empty() {
            return Err(ApiError::InvalidNode(
                "daemon address must not be empty".to_string(),
            ));
        }

        self.nodes.insert(node.name.clone(), node);
        Ok(())
    }
}

pub fn parse_idl(input: &str) -> std::result::Result<ContractPackage, ApiError> {
    let mut package = None;
    let mut schemas = Vec::new();
    let mut events = Vec::new();
    let mut services = Vec::new();
    let mut streams = Vec::new();

    let lines = sanitize_lines(input);
    let mut index = 0;

    while index < lines.len() {
        let line = lines[index].as_str();
        if line.is_empty() {
            index += 1;
            continue;
        }

        if line.starts_with("package ") {
            if package.is_some() {
                return Err(ApiError::Parse("duplicate package declaration".to_string()));
            }
            package = Some(parse_package_name(line)?);
            index += 1;
            continue;
        }

        if line.starts_with("schema ") {
            let (schema, consumed) = parse_schema_block(&lines[index..])?;
            schemas.push(schema);
            index += consumed;
            continue;
        }

        if line.starts_with("event ") {
            let (event, consumed) = parse_event_block(&lines[index..])?;
            events.push(event);
            index += consumed;
            continue;
        }

        if line.starts_with("service ") {
            let (service, consumed) = parse_service(&lines[index..])?;
            services.push(service);
            index += consumed;
            continue;
        }

        if line.starts_with("stream ") {
            streams.push(parse_stream(line)?);
            index += 1;
            continue;
        }

        return Err(ApiError::Parse(format!(
            "unexpected top-level statement: {line}"
        )));
    }

    let package = package.ok_or_else(|| ApiError::Parse("missing package declaration".into()))?;
    let (namespace, version) = split_namespace_version(&package);

    let package = ContractPackage {
        package,
        namespace,
        version,
        schemas,
        events,
        services,
        streams,
    };
    validate_package(&package)?;
    Ok(package)
}

pub fn generate_rust_bindings(package: &ContractPackage) -> String {
    let mut out = String::new();
    out.push_str("// Generated by Selium IDL compiler\n");
    out.push_str("use anyhow::Context as _;\n");
    out.push_str("use rkyv::{Archive, Serialize, Deserialize};\n\n");
    out.push_str("#[derive(Debug, Clone, Copy, PartialEq, Eq)]\n");
    out.push_str("pub enum ServiceBodyMode {\n");
    out.push_str("    None,\n");
    out.push_str("    Buffered,\n");
    out.push_str("    Stream,\n");
    out.push_str("}\n\n");
    out.push_str("#[derive(Debug, Clone, PartialEq, Eq)]\n");
    out.push_str("pub struct ServiceBodyBinding {\n");
    out.push_str("    pub mode: ServiceBodyMode,\n");
    out.push_str("    pub schema: Option<&'static str>,\n");
    out.push_str("}\n\n");
    out.push_str("#[derive(Debug, Clone, PartialEq, Eq)]\n");
    out.push_str("pub struct ServiceFieldBinding {\n");
    out.push_str("    pub field: &'static str,\n");
    out.push_str("    pub target: &'static str,\n");
    out.push_str("}\n\n");
    out.push_str("#[derive(Debug, Clone, PartialEq, Eq)]\n");
    out.push_str("pub struct ServiceBinding {\n");
    out.push_str("    pub name: &'static str,\n");
    out.push_str("    pub request_schema: &'static str,\n");
    out.push_str("    pub response_schema: &'static str,\n");
    out.push_str("    pub protocol: Option<&'static str>,\n");
    out.push_str("    pub method: Option<&'static str>,\n");
    out.push_str("    pub path: Option<&'static str>,\n");
    out.push_str("    pub request_headers: &'static [ServiceFieldBinding],\n");
    out.push_str("    pub request_body: ServiceBodyBinding,\n");
    out.push_str("    pub response_body: ServiceBodyBinding,\n");
    out.push_str("}\n\n");

    if !package.services.is_empty() || !package.streams.is_empty() {
        out.push_str("#[allow(dead_code)]\n");
        out.push_str("fn __selium_extract_path_params(\n");
        out.push_str("    template: &str,\n");
        out.push_str("    actual: &str,\n");
        out.push_str(") -> Option<std::collections::BTreeMap<String, String>> {\n");
        out.push_str("    let template_segments = if template.trim_matches('/').is_empty() {\n");
        out.push_str("        Vec::new()\n");
        out.push_str("    } else {\n");
        out.push_str("        template.trim_matches('/').split('/').collect::<Vec<_>>()\n");
        out.push_str("    };\n");
        out.push_str("    let actual_segments = if actual.trim_matches('/').is_empty() {\n");
        out.push_str("        Vec::new()\n");
        out.push_str("    } else {\n");
        out.push_str("        actual.trim_matches('/').split('/').collect::<Vec<_>>()\n");
        out.push_str("    };\n");
        out.push_str("    if template_segments.len() != actual_segments.len() {\n");
        out.push_str("        return None;\n");
        out.push_str("    }\n");
        out.push_str("    let mut params = std::collections::BTreeMap::new();\n");
        out.push_str("    for (template_segment, actual_segment) in template_segments.into_iter().zip(actual_segments) {\n");
        out.push_str("        if template_segment.starts_with('{') && template_segment.ends_with('}') && template_segment.len() > 2 {\n");
        out.push_str("            params.insert(\n");
        out.push_str(
            "                template_segment[1..template_segment.len() - 1].to_string(),\n",
        );
        out.push_str("                actual_segment.to_string(),\n");
        out.push_str("            );\n");
        out.push_str("        } else if template_segment != actual_segment {\n");
        out.push_str("            return None;\n");
        out.push_str("        }\n");
        out.push_str("    }\n");
        out.push_str("    Some(params)\n");
        out.push_str("}\n\n");

        out.push_str("#[allow(dead_code)]\n");
        out.push_str("async fn __selium_read_stream_message(\n");
        out.push_str("    stream: &selium_guest::network::StreamChannel,\n");
        out.push_str("    max_bytes: u32,\n");
        out.push_str("    timeout_ms: u32,\n");
        out.push_str(") -> anyhow::Result<Vec<u8>> {\n");
        out.push_str("    let mut payload = Vec::new();\n");
        out.push_str("    loop {\n");
        out.push_str(
            "        let Some(chunk) = stream.recv(max_bytes, timeout_ms).await? else {\n",
        );
        out.push_str("            continue;\n");
        out.push_str("        };\n");
        out.push_str("        payload.extend_from_slice(&chunk.bytes);\n");
        out.push_str("        if chunk.finish {\n");
        out.push_str("            return Ok(payload);\n");
        out.push_str("        }\n");
        out.push_str("    }\n");
        out.push_str("}\n\n");
    }

    for schema in &package.schemas {
        out.push_str("#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]\n");
        out.push_str("#[rkyv(bytecheck())]\n");
        out.push_str(&format!("pub struct {} {{\n", schema.name));
        for field in &schema.fields {
            out.push_str(&format!(
                "    pub {}: {},\n",
                field.name,
                map_field_type(&field.ty)
            ));
        }
        out.push_str("}\n\n");
    }

    for event in &package.events {
        out.push_str(&format!(
            "pub const EVENT_{}: &str = \"{}\";\n",
            const_name(&event.name),
            event.name
        ));
    }

    if !package.events.is_empty() {
        out.push('\n');
    }

    for service in &package.services {
        out.push_str(&format!(
            "pub const SERVICE_{}: ServiceBinding = ServiceBinding {{\n",
            const_name(&service.name)
        ));
        out.push_str(&format!("    name: {:?},\n", service.name));
        out.push_str(&format!(
            "    request_schema: {:?},\n",
            service.request_schema
        ));
        out.push_str(&format!(
            "    response_schema: {:?},\n",
            service.response_schema
        ));
        out.push_str(&format!(
            "    protocol: {},\n",
            option_literal(service.protocol.as_deref())
        ));
        out.push_str(&format!(
            "    method: {},\n",
            option_literal(service.method.as_deref())
        ));
        out.push_str(&format!(
            "    path: {},\n",
            option_literal(service.path.as_deref())
        ));
        out.push_str(&format!(
            "    request_headers: {},\n",
            generate_field_bindings(&service.request_headers)
        ));
        out.push_str(&format!(
            "    request_body: {},\n",
            generate_body_binding(&service.request_body)
        ));
        out.push_str(&format!(
            "    response_body: {},\n",
            generate_body_binding(&service.response_body)
        ));
        out.push_str("};\n");
    }

    if !package.services.is_empty() {
        out.push('\n');
    }

    for stream in &package.streams {
        out.push_str(&format!(
            "pub const STREAM_{}: &str = {:?};\n",
            const_name(&stream.name),
            stream.name
        ));
    }

    if !package.services.is_empty() {
        out.push('\n');
        for service in &package.services {
            match service.protocol.as_deref() {
                Some("http") => out.push_str(&generate_http_service_module(package, service)),
                Some("quic") => out.push_str(&generate_quic_service_module(service)),
                _ => {}
            }
        }
    }

    if !package.streams.is_empty() {
        out.push('\n');
        for stream in &package.streams {
            out.push_str(&generate_stream_module(stream));
        }
    }

    out
}

fn sanitize_lines(input: &str) -> Vec<String> {
    input
        .lines()
        .map(|line| line.split_once("//").map_or(line, |(prefix, _)| prefix))
        .map(str::trim)
        .map(ToString::to_string)
        .collect()
}

fn schema_fields<'a>(package: &'a ContractPackage, schema_name: &str) -> Option<&'a [SchemaField]> {
    package
        .schemas
        .iter()
        .find(|schema| schema.name == schema_name)
        .map(|schema| schema.fields.as_slice())
}

fn schema_field_type<'a>(
    package: &'a ContractPackage,
    schema_name: &str,
    field_name: &str,
) -> Option<&'a str> {
    schema_fields(package, schema_name)?
        .iter()
        .find(|field| field.name == field_name)
        .map(|field| field.ty.as_str())
}

fn rust_type_name(ty: &str) -> String {
    match ty {
        "string" => "String".to_string(),
        "bytes" => "Vec<u8>".to_string(),
        "bool" => "bool".to_string(),
        "u8" => "u8".to_string(),
        "u16" => "u16".to_string(),
        "u32" => "u32".to_string(),
        "u64" => "u64".to_string(),
        "i8" => "i8".to_string(),
        "i16" => "i16".to_string(),
        "i32" => "i32".to_string(),
        "i64" => "i64".to_string(),
        "f32" => "f32".to_string(),
        "f64" => "f64".to_string(),
        _ => ty.to_string(),
    }
}

fn module_name(raw: &str) -> String {
    let mut out = String::new();
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
        } else if !out.ends_with('_') {
            out.push('_');
        }
    }
    let out = out.trim_matches('_').to_string();
    if out.is_empty() {
        "_contract".to_string()
    } else if out.chars().next().is_some_and(|ch| ch.is_ascii_digit()) {
        format!("_{out}")
    } else {
        out
    }
}

fn extract_path_parameters(path: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = path;
    while let Some(start) = rest.find('{') {
        rest = &rest[start + 1..];
        let Some(end) = rest.find('}') else {
            break;
        };
        let name = rest[..end].trim();
        if !name.is_empty() {
            out.push(name.to_string());
        }
        rest = &rest[end + 1..];
    }
    out
}

fn field_to_string_expr(access: &str, ty: &str, context: &str) -> String {
    match ty {
        "string" => format!("{access}.clone()"),
        "bytes" => format!("String::from_utf8({access}.clone()).context({context:?})?"),
        "bool" | "u8" | "u16" | "u32" | "u64" | "i8" | "i16" | "i32" | "i64" | "f32" | "f64" => {
            format!("{access}.to_string()")
        }
        _ => format!("selium_abi::encode_rkyv(&{access}).context({context:?})?.len().to_string()"),
    }
}

fn field_from_string_expr(source: &str, ty: &str, context: &str) -> String {
    match ty {
        "string" => format!("{source}.to_string()"),
        "bytes" => format!("{source}.as_bytes().to_vec()"),
        "bool" | "u8" | "u16" | "u32" | "u64" | "i8" | "i16" | "i32" | "i64" | "f32" | "f64" => {
            format!(
                "{source}.parse::<{}>().with_context(|| {context:?}.to_string())?",
                rust_type_name(ty)
            )
        }
        _ => "unreachable!(\"unsupported HTTP field binding type\")".to_string(),
    }
}

fn generate_field_bindings(bindings: &[ServiceFieldBinding]) -> String {
    if bindings.is_empty() {
        return "&[]".to_string();
    }

    let mut out = String::from("&[");
    for binding in bindings {
        out.push_str(&format!(
            "ServiceFieldBinding {{ field: {:?}, target: {:?} }}, ",
            binding.field, binding.target
        ));
    }
    out.push(']');
    out
}

fn generate_http_service_module(package: &ContractPackage, service: &ServiceDef) -> String {
    let module = module_name(&service.name);
    let const_name = const_name(&service.name);
    let request_type = rust_type_name(&service.request_schema);
    let response_body_type = rust_type_name(
        service
            .response_body
            .schema
            .as_deref()
            .unwrap_or(&service.response_schema),
    );
    let method = service.method.as_deref().unwrap_or("POST");
    let path = service.path.as_deref().unwrap_or("/");
    let path_params = extract_path_parameters(path);
    let request_fields = schema_fields(package, &service.request_schema).unwrap_or(&[]);

    let mut out = String::new();
    out.push_str("#[allow(dead_code)]\n");
    out.push_str(&format!("pub mod {module} {{\n"));
    out.push_str("    use super::*;\n\n");
    out.push_str(&format!(
        "    pub const DEF: ServiceBinding = super::SERVICE_{const_name};\n\n"
    ));
    out.push_str("    pub struct Client<'a> {\n");
    out.push_str("        session: &'a selium_guest::network::Session,\n");
    out.push_str("    }\n\n");
    out.push_str("    pub struct PendingRequest {\n");
    out.push_str("        exchange: selium_guest::network::ClientExchange,\n");
    out.push_str("        request_body: selium_guest::network::BodyWriter,\n");
    out.push_str("    }\n\n");
    out.push_str("    pub struct Response {\n");
    out.push_str("        pub head: selium_abi::NetworkRpcResponseHead,\n");
    out.push_str("        pub body: selium_guest::network::BodyReader,\n");
    out.push_str("    }\n\n");
    out.push_str("    pub struct AcceptedRequest {\n");
    out.push_str("        exchange: selium_guest::network::ServerExchange,\n");
    out.push_str(&format!("        request: {request_type},\n"));
    out.push_str("    }\n\n");

    out.push_str("    impl<'a> Client<'a> {\n");
    out.push_str("        pub fn new(session: &'a selium_guest::network::Session) -> Self {\n");
    out.push_str("            Self { session }\n");
    out.push_str("        }\n\n");
    out.push_str("        pub async fn start(\n");
    out.push_str(&format!(
        "            &self,\n            request: &{request_type},\n"
    ));
    out.push_str("            timeout_ms: u32,\n");
    out.push_str("        ) -> anyhow::Result<Option<PendingRequest>> {\n");
    out.push_str("            let request_head = build_request_head(request)?;\n");
    out.push_str("            Ok(selium_guest::network::rpc::start(self.session, request_head, timeout_ms)\n");
    out.push_str("                .await?\n");
    out.push_str("                .map(|(exchange, request_body)| PendingRequest {\n");
    out.push_str("                    exchange,\n");
    out.push_str("                    request_body,\n");
    out.push_str("                }))\n");
    out.push_str("        }\n");
    out.push_str("    }\n\n");

    out.push_str("    impl PendingRequest {\n");
    out.push_str("        pub fn request_body(&self) -> &selium_guest::network::BodyWriter {\n");
    out.push_str("            &self.request_body\n");
    out.push_str("        }\n\n");
    out.push_str("        pub async fn await_response(self, timeout_ms: u32) -> anyhow::Result<Option<Response>> {\n");
    out.push_str("            Ok(self.exchange.await_response(timeout_ms).await?.map(|(head, body)| Response {\n");
    out.push_str("                head,\n");
    out.push_str("                body,\n");
    out.push_str("            }))\n");
    out.push_str("        }\n");
    if service.response_body.mode == ServiceBodyMode::Buffered {
        out.push('\n');
        out.push_str(&format!(
            "        pub async fn await_buffered_response(self, max_bytes: u32, timeout_ms: u32) -> anyhow::Result<Option<{response_body_type}>> {{\n"
        ));
        out.push_str("            match self.await_response(timeout_ms).await? {\n");
        out.push_str("                Some(response) => Ok(Some(response.decode_buffered(max_bytes, timeout_ms).await?)),\n");
        out.push_str("                None => Ok(None),\n");
        out.push_str("            }\n");
        out.push_str("        }\n");
    }
    out.push_str("    }\n\n");

    out.push_str("    impl Response {\n");
    if service.response_body.mode == ServiceBodyMode::Buffered {
        out.push_str(&format!(
            "        pub async fn decode_buffered(self, max_bytes: u32, timeout_ms: u32) -> anyhow::Result<{response_body_type}> {{\n"
        ));
        out.push_str("            let body = self.body.read_all(max_bytes, timeout_ms).await?;\n");
        if service
            .response_body
            .schema
            .as_deref()
            .unwrap_or(&service.response_schema)
            == "bytes"
        {
            out.push_str("            Ok(body)\n");
        } else {
            out.push_str(&format!(
                "            selium_abi::decode_rkyv::<{response_body_type}>(&body).context(\"decode buffered response body\")\n"
            ));
        }
        out.push_str("        }\n");
    }
    out.push_str("    }\n\n");

    out.push_str("    pub async fn accept(\n");
    out.push_str("        session: &selium_guest::network::Session,\n");
    out.push_str("        timeout_ms: u32,\n");
    out.push_str("    ) -> anyhow::Result<Option<AcceptedRequest>> {\n");
    out.push_str("        let Some(exchange) = selium_guest::network::rpc::accept(session, timeout_ms).await? else {\n");
    out.push_str("            return Ok(None);\n");
    out.push_str("        };\n");
    out.push_str("        let request = parse_request_head(exchange.request_head())?;\n");
    out.push_str("        Ok(Some(AcceptedRequest { exchange, request }))\n");
    out.push_str("    }\n\n");

    out.push_str("    impl AcceptedRequest {\n");
    out.push_str(&format!(
        "        pub fn request(&self) -> &{request_type} {{\n"
    ));
    out.push_str("            &self.request\n");
    out.push_str("        }\n\n");
    out.push_str("        pub fn request_body(&self) -> &selium_guest::network::BodyReader {\n");
    out.push_str("            self.exchange.request_body()\n");
    out.push_str("        }\n");
    if service.response_body.mode == ServiceBodyMode::Buffered {
        out.push('\n');
        out.push_str(&format!(
            "        pub async fn respond_buffered(self, response: &{response_body_type}, timeout_ms: u32) -> anyhow::Result<()> {{\n"
        ));
        out.push_str("            self.exchange\n");
        out.push_str("                .respond(selium_abi::NetworkRpcResponse {\n");
        out.push_str("                    head: selium_abi::NetworkRpcResponseHead {\n");
        out.push_str("                        status: 200,\n");
        out.push_str("                        metadata: std::collections::BTreeMap::new(),\n");
        out.push_str("                    },\n");
        if service
            .response_body
            .schema
            .as_deref()
            .unwrap_or(&service.response_schema)
            == "bytes"
        {
            out.push_str("                    body: response.clone(),\n");
        } else {
            out.push_str("                    body: selium_abi::encode_rkyv(response).context(\"encode buffered response body\")?,\n");
        }
        out.push_str("                }, timeout_ms)\n");
        out.push_str("                .await?;\n");
        out.push_str("            Ok(())\n");
        out.push_str("        }\n");
    }
    out.push_str("    }\n\n");

    out.push_str(&format!(
        "    fn build_request_head(request: &{request_type}) -> anyhow::Result<selium_abi::NetworkRpcRequestHead> {{\n"
    ));
    out.push_str(&format!("        let mut path = {:?}.to_string();\n", path));
    for param in &path_params {
        let ty = schema_field_type(package, &service.request_schema, param).unwrap_or("string");
        out.push_str(&format!(
            "        path = path.replace({:?}, &{});\n",
            format!("{{{param}}}"),
            field_to_string_expr(
                &format!("request.{param}"),
                ty,
                &format!("serialize path parameter `{param}`"),
            )
        ));
    }
    out.push_str("        let mut metadata = std::collections::BTreeMap::new();\n");
    for header in &service.request_headers {
        let ty =
            schema_field_type(package, &service.request_schema, &header.field).unwrap_or("string");
        out.push_str(&format!(
            "        metadata.insert({:?}.to_string(), {});\n",
            header.target,
            field_to_string_expr(
                &format!("request.{}", header.field),
                ty,
                &format!("serialize header `{}`", header.target),
            )
        ));
    }
    out.push_str("        Ok(selium_abi::NetworkRpcRequestHead {\n");
    out.push_str(&format!("            method: {:?}.to_string(),\n", method));
    out.push_str("            path,\n");
    out.push_str("            metadata,\n");
    out.push_str("        })\n");
    out.push_str("    }\n\n");

    out.push_str(&format!(
        "    fn parse_request_head(head: &selium_abi::NetworkRpcRequestHead) -> anyhow::Result<{request_type}> {{\n"
    ));
    out.push_str(&format!(
        "        if head.method != {:?} {{\n            return Err(anyhow::anyhow!(\"unexpected HTTP method for {}: {{}}\", head.method));\n        }}\n",
        method, service.name
    ));
    out.push_str(&format!(
        "        let params = __selium_extract_path_params({:?}, &head.path)\n            .ok_or_else(|| anyhow::anyhow!(\"unexpected HTTP path for {}: {{}}\", head.path))?;\n",
        path, service.name
    ));
    out.push_str(&format!("        Ok({request_type} {{\n"));
    for field in request_fields {
        if path_params.iter().any(|param| param == &field.name) {
            out.push_str(&format!(
                "            {}: {},\n",
                field.name,
                field_from_string_expr(
                    &format!(
                        "params.get({:?}).ok_or_else(|| anyhow::anyhow!(\"missing path parameter `{}`\"))?",
                        field.name, field.name
                    ),
                    &field.ty,
                    &format!("parse path parameter `{}`", field.name),
                )
            ));
        } else if let Some(binding) = service
            .request_headers
            .iter()
            .find(|binding| binding.field == field.name)
        {
            out.push_str(&format!(
                "            {}: {},\n",
                field.name,
                field_from_string_expr(
                    &format!(
                        "head.metadata.get({:?}).ok_or_else(|| anyhow::anyhow!(\"missing header `{}`\"))?",
                        binding.target, binding.target
                    ),
                    &field.ty,
                    &format!("parse header `{}`", binding.target),
                )
            ));
        }
    }
    out.push_str("        })\n");
    out.push_str("    }\n");
    out.push_str("}\n\n");
    out
}

fn generate_quic_service_module(service: &ServiceDef) -> String {
    let module = module_name(&service.name);
    let const_name = const_name(&service.name);
    let request_type = rust_type_name(&service.request_schema);
    let response_type = rust_type_name(&service.response_schema);
    let method = service.method.as_deref().unwrap_or(&service.name);
    let path = service.path.as_deref().unwrap_or("");

    let mut out = String::new();
    out.push_str("#[allow(dead_code)]\n");
    out.push_str(&format!("pub mod {module} {{\n"));
    out.push_str("    use super::*;\n\n");
    out.push_str(&format!(
        "    pub const DEF: ServiceBinding = super::SERVICE_{const_name};\n\n"
    ));
    out.push_str("    pub struct Client<'a> {\n");
    out.push_str("        session: &'a selium_guest::network::Session,\n");
    out.push_str("    }\n\n");
    out.push_str("    pub struct AcceptedRequest {\n");
    out.push_str("        exchange: selium_guest::network::ServerExchange,\n");
    out.push_str(&format!("        request: {request_type},\n"));
    out.push_str("    }\n\n");

    out.push_str("    impl<'a> Client<'a> {\n");
    out.push_str("        pub fn new(session: &'a selium_guest::network::Session) -> Self {\n");
    out.push_str("            Self { session }\n");
    out.push_str("        }\n\n");
    out.push_str(&format!(
        "        pub async fn invoke(&self, request: &{request_type}, timeout_ms: u32) -> anyhow::Result<Option<{response_type}>> {{\n"
    ));
    out.push_str("            let Some(response) = selium_guest::network::rpc::invoke(\n");
    out.push_str("                self.session,\n");
    out.push_str("                selium_abi::NetworkRpcRequest {\n");
    out.push_str("                    head: selium_abi::NetworkRpcRequestHead {\n");
    out.push_str(&format!(
        "                        method: {:?}.to_string(),\n",
        method
    ));
    out.push_str(&format!(
        "                        path: {:?}.to_string(),\n",
        path
    ));
    out.push_str("                        metadata: std::collections::BTreeMap::new(),\n");
    out.push_str("                    },\n");
    if service
        .request_body
        .schema
        .as_deref()
        .unwrap_or(&service.request_schema)
        == "bytes"
    {
        out.push_str("                    body: request.clone(),\n");
    } else {
        out.push_str("                    body: selium_abi::encode_rkyv(request).context(\"encode QUIC request\")?,\n");
    }
    out.push_str("                },\n");
    out.push_str("                timeout_ms,\n");
    out.push_str("            )\n");
    out.push_str("            .await? else {\n");
    out.push_str("                return Ok(None);\n");
    out.push_str("            };\n");
    if service
        .response_body
        .schema
        .as_deref()
        .unwrap_or(&service.response_schema)
        == "bytes"
    {
        out.push_str("            Ok(Some(response.body))\n");
    } else {
        out.push_str(&format!(
            "            Ok(Some(selium_abi::decode_rkyv::<{response_type}>(&response.body).context(\"decode QUIC response\")?))\n"
        ));
    }
    out.push_str("        }\n");
    out.push_str("    }\n\n");

    out.push_str("    pub async fn accept(\n");
    out.push_str("        session: &selium_guest::network::Session,\n");
    out.push_str("        timeout_ms: u32,\n");
    out.push_str("    ) -> anyhow::Result<Option<AcceptedRequest>> {\n");
    out.push_str("        let Some(exchange) = selium_guest::network::rpc::accept(session, timeout_ms).await? else {\n");
    out.push_str("            return Ok(None);\n");
    out.push_str("        };\n");
    out.push_str(&format!(
        "        if exchange.request_head().method != {:?} || exchange.request_head().path != {:?} {{\n",
        method, path
    ));
    out.push_str("            return Err(anyhow::anyhow!(\"unexpected QUIC RPC route\"));\n");
    out.push_str("        }\n");
    out.push_str("        let buffered = exchange.buffered_request(8192, timeout_ms).await?;\n");
    if service
        .request_body
        .schema
        .as_deref()
        .unwrap_or(&service.request_schema)
        == "bytes"
    {
        out.push_str("        let request = buffered.body;\n");
    } else {
        out.push_str(&format!(
            "        let request = selium_abi::decode_rkyv::<{request_type}>(&buffered.body).context(\"decode QUIC request\")?;\n"
        ));
    }
    out.push_str("        Ok(Some(AcceptedRequest { exchange, request }))\n");
    out.push_str("    }\n\n");

    out.push_str("    impl AcceptedRequest {\n");
    out.push_str(&format!(
        "        pub fn request(&self) -> &{request_type} {{\n"
    ));
    out.push_str("            &self.request\n");
    out.push_str("        }\n\n");
    out.push_str(&format!(
        "        pub async fn respond(self, response: &{response_type}, timeout_ms: u32) -> anyhow::Result<()> {{\n"
    ));
    out.push_str("            self.exchange\n");
    out.push_str("                .respond(selium_abi::NetworkRpcResponse {\n");
    out.push_str("                    head: selium_abi::NetworkRpcResponseHead {\n");
    out.push_str("                        status: 200,\n");
    out.push_str("                        metadata: std::collections::BTreeMap::new(),\n");
    out.push_str("                    },\n");
    if service
        .response_body
        .schema
        .as_deref()
        .unwrap_or(&service.response_schema)
        == "bytes"
    {
        out.push_str("                    body: response.clone(),\n");
    } else {
        out.push_str("                    body: selium_abi::encode_rkyv(response).context(\"encode QUIC response\")?,\n");
    }
    out.push_str("                }, timeout_ms)\n");
    out.push_str("                .await?;\n");
    out.push_str("            Ok(())\n");
    out.push_str("        }\n");
    out.push_str("    }\n");
    out.push_str("}\n\n");
    out
}

fn generate_stream_module(stream: &StreamDef) -> String {
    let module = module_name(&stream.name);
    let const_name = const_name(&stream.name);
    let payload_type = rust_type_name(&stream.payload_schema);

    let mut out = String::new();
    out.push_str("#[allow(dead_code)]\n");
    out.push_str(&format!("pub mod {module} {{\n"));
    out.push_str("    use super::*;\n\n");
    out.push_str(&format!(
        "    pub const DEF: &str = super::STREAM_{const_name};\n\n"
    ));
    out.push_str("    pub async fn open(\n");
    out.push_str("        session: &selium_guest::network::Session,\n");
    out.push_str("    ) -> anyhow::Result<Option<selium_guest::network::StreamChannel>> {\n");
    out.push_str("        Ok(selium_guest::network::stream::open(session).await?)\n");
    out.push_str("    }\n\n");
    out.push_str("    pub async fn accept(\n");
    out.push_str("        session: &selium_guest::network::Session,\n");
    out.push_str("        timeout_ms: u32,\n");
    out.push_str("    ) -> anyhow::Result<Option<selium_guest::network::StreamChannel>> {\n");
    out.push_str("        Ok(selium_guest::network::stream::accept(session, timeout_ms).await?)\n");
    out.push_str("    }\n\n");
    out.push_str(&format!(
        "    pub async fn send(stream: &selium_guest::network::StreamChannel, payload: &{payload_type}, timeout_ms: u32) -> anyhow::Result<()> {{\n"
    ));
    if stream.payload_schema == "bytes" {
        out.push_str("        stream.send(payload.clone(), true, timeout_ms).await?;\n");
    } else {
        out.push_str("        stream\n");
        out.push_str("            .send(selium_abi::encode_rkyv(payload).context(\"encode QUIC stream payload\")?, true, timeout_ms)\n");
        out.push_str("            .await?;\n");
    }
    out.push_str("        Ok(())\n");
    out.push_str("    }\n\n");
    out.push_str(&format!(
        "    pub async fn recv(stream: &selium_guest::network::StreamChannel, max_bytes: u32, timeout_ms: u32) -> anyhow::Result<{payload_type}> {{\n"
    ));
    out.push_str("        let payload = __selium_read_stream_message(stream, max_bytes, timeout_ms).await?;\n");
    if stream.payload_schema == "bytes" {
        out.push_str("        Ok(payload)\n");
    } else {
        out.push_str(&format!(
            "        selium_abi::decode_rkyv::<{payload_type}>(&payload).context(\"decode QUIC stream payload\")\n"
        ));
    }
    out.push_str("    }\n");
    out.push_str("}\n\n");
    out
}

fn parse_package_name(line: &str) -> std::result::Result<String, ApiError> {
    let value = line
        .strip_prefix("package ")
        .ok_or_else(|| ApiError::Parse("invalid package declaration".to_string()))?
        .trim()
        .trim_end_matches(';')
        .trim();

    if value.is_empty() {
        return Err(ApiError::Parse(
            "package name must not be empty".to_string(),
        ));
    }

    Ok(value.to_string())
}

fn parse_schema_block(lines: &[String]) -> std::result::Result<(SchemaDef, usize), ApiError> {
    let header = lines
        .first()
        .ok_or_else(|| ApiError::Parse("unexpected end of schema block".to_string()))?;
    let schema_decl = header
        .strip_prefix("schema ")
        .ok_or_else(|| ApiError::Parse("invalid schema header".to_string()))?;
    let (name, after_open) = schema_decl
        .split_once('{')
        .ok_or_else(|| ApiError::Parse(format!("schema header must contain '{{': {header}")))?;
    let name = name.trim();

    if name.is_empty() {
        return Err(ApiError::Parse("schema name must not be empty".to_string()));
    }

    let mut fields = Vec::new();
    let mut consumed = 1;

    let first_fragment = after_open.trim();
    if !first_fragment.is_empty() {
        if let Some((before_close, tail)) = first_fragment.split_once('}') {
            parse_schema_fields_fragment(before_close, &mut fields)?;
            if !tail.trim().is_empty() {
                return Err(ApiError::Parse(format!(
                    "unexpected tokens after schema block close: {tail}"
                )));
            }
            return Ok((
                SchemaDef {
                    name: name.to_string(),
                    fields,
                },
                consumed,
            ));
        }
        parse_schema_fields_fragment(first_fragment, &mut fields)?;
    }

    for line in &lines[1..] {
        consumed += 1;
        if line.is_empty() {
            continue;
        }

        if let Some((before_close, tail)) = line.split_once('}') {
            parse_schema_fields_fragment(before_close, &mut fields)?;
            if !tail.trim().is_empty() {
                return Err(ApiError::Parse(format!(
                    "unexpected tokens after schema block close: {tail}"
                )));
            }
            return Ok((
                SchemaDef {
                    name: name.to_string(),
                    fields,
                },
                consumed,
            ));
        }
        parse_schema_fields_fragment(line, &mut fields)?;
    }

    Err(ApiError::Parse(format!(
        "schema block `{name}` is missing closing brace"
    )))
}

fn parse_event_block(lines: &[String]) -> std::result::Result<(EventDef, usize), ApiError> {
    let header = lines
        .first()
        .ok_or_else(|| ApiError::Parse("unexpected end of event block".to_string()))?;
    let event_decl = header
        .strip_prefix("event ")
        .ok_or_else(|| ApiError::Parse("invalid event header".to_string()))?;
    let (signature, after_open) = event_decl
        .split_once('{')
        .ok_or_else(|| ApiError::Parse(format!("event header must contain '{{': {header}")))?;

    let (name, payload_schema) = signature
        .split_once('(')
        .ok_or_else(|| ApiError::Parse(format!("invalid event signature: {signature}")))?;
    let payload_schema = payload_schema.trim_end_matches(')').trim().to_string();

    let name = name.trim().to_string();
    if name.is_empty() || payload_schema.is_empty() {
        return Err(ApiError::Parse(format!(
            "invalid event signature: {signature}"
        )));
    }

    let mut partitions = 1;
    let mut retention = "24h".to_string();
    let mut delivery = DeliveryGuarantee::AtLeastOnce;
    let mut replay_enabled = true;
    let mut consumed = 1;

    let first_fragment = after_open.trim();
    if !first_fragment.is_empty() {
        if let Some((before_close, tail)) = first_fragment.split_once('}') {
            apply_event_properties_fragment(
                before_close,
                &mut partitions,
                &mut retention,
                &mut delivery,
                &mut replay_enabled,
            )?;
            if !tail.trim().is_empty() {
                return Err(ApiError::Parse(format!(
                    "unexpected tokens after event block close: {tail}"
                )));
            }
            return Ok((
                EventDef {
                    name,
                    payload_schema,
                    partitions,
                    retention,
                    delivery,
                    replay_enabled,
                },
                consumed,
            ));
        }
        apply_event_properties_fragment(
            first_fragment,
            &mut partitions,
            &mut retention,
            &mut delivery,
            &mut replay_enabled,
        )?;
    }

    for line in &lines[1..] {
        consumed += 1;
        if line.is_empty() {
            continue;
        }

        if let Some((before_close, tail)) = line.split_once('}') {
            apply_event_properties_fragment(
                before_close,
                &mut partitions,
                &mut retention,
                &mut delivery,
                &mut replay_enabled,
            )?;
            if !tail.trim().is_empty() {
                return Err(ApiError::Parse(format!(
                    "unexpected tokens after event block close: {tail}"
                )));
            }
            return Ok((
                EventDef {
                    name,
                    payload_schema,
                    partitions,
                    retention,
                    delivery,
                    replay_enabled,
                },
                consumed,
            ));
        }
        apply_event_properties_fragment(
            line,
            &mut partitions,
            &mut retention,
            &mut delivery,
            &mut replay_enabled,
        )?;
    }

    Err(ApiError::Parse(format!(
        "event block `{name}` is missing closing brace"
    )))
}

fn parse_schema_fields_fragment(
    fragment: &str,
    fields: &mut Vec<SchemaField>,
) -> std::result::Result<(), ApiError> {
    for entry in fragment.split(';') {
        let line = entry.trim();
        if line.is_empty() {
            continue;
        }

        let (field, ty) = line
            .split_once(':')
            .ok_or_else(|| ApiError::Parse(format!("invalid schema field: {line}")))?;
        let field = field.trim();
        let ty = ty.trim();
        if field.is_empty() || ty.is_empty() {
            return Err(ApiError::Parse(format!("invalid schema field: {line}")));
        }

        fields.push(SchemaField {
            name: field.to_string(),
            ty: ty.to_string(),
        });
    }
    Ok(())
}

fn apply_event_properties_fragment(
    fragment: &str,
    partitions: &mut u16,
    retention: &mut String,
    delivery: &mut DeliveryGuarantee,
    replay_enabled: &mut bool,
) -> std::result::Result<(), ApiError> {
    for entry in fragment.split(';') {
        let line = entry.trim();
        if line.is_empty() {
            continue;
        }

        let (key, value) = line
            .split_once(':')
            .ok_or_else(|| ApiError::Parse(format!("invalid event property: {line}")))?;
        let key = key.trim();
        let value = value.trim().trim_matches('"');
        match key {
            "partitions" => {
                *partitions = value.parse::<u16>().map_err(|_| {
                    ApiError::Parse(format!("invalid partitions value `{value}` in event"))
                })?;
            }
            "retention" => {
                *retention = value.to_string();
            }
            "delivery" => {
                *delivery = match value {
                    "at_least_once" => DeliveryGuarantee::AtLeastOnce,
                    "at_most_once" => DeliveryGuarantee::AtMostOnce,
                    "exactly_once" => DeliveryGuarantee::ExactlyOnce,
                    _ => {
                        return Err(ApiError::Parse(format!(
                            "unknown delivery guarantee `{value}`"
                        )));
                    }
                };
            }
            "replay" => {
                *replay_enabled = match value {
                    "enabled" | "true" => true,
                    "disabled" | "false" => false,
                    _ => {
                        return Err(ApiError::Parse(format!("unknown replay value `{value}`")));
                    }
                };
            }
            _ => return Err(ApiError::Parse(format!("unknown event property `{key}`"))),
        }
    }

    Ok(())
}

fn parse_service(lines: &[String]) -> std::result::Result<(ServiceDef, usize), ApiError> {
    let header = lines
        .first()
        .ok_or_else(|| ApiError::Parse("unexpected end of service block".to_string()))?;
    let line = header.as_str();
    let mut consumed = 1;
    let (signature, after_open) = match line.split_once('{') {
        Some((signature, after_open)) => (signature.trim(), Some(after_open.trim())),
        None => (line.trim(), None),
    };

    let signature = signature
        .trim_end_matches(';')
        .strip_prefix("service ")
        .ok_or_else(|| ApiError::Parse(format!("invalid service declaration: {line}")))?
        .trim();

    let (left, response_schema) = signature
        .split_once("->")
        .ok_or_else(|| ApiError::Parse(format!("invalid service declaration: {line}")))?;

    let (name, request_schema) = left
        .split_once('(')
        .ok_or_else(|| ApiError::Parse(format!("invalid service declaration: {line}")))?;

    let request_schema = request_schema.trim().trim_end_matches(')').trim();
    let name = name.trim();
    let response_schema = response_schema.trim();

    if name.is_empty() || request_schema.is_empty() || response_schema.is_empty() {
        return Err(ApiError::Parse(format!(
            "invalid service declaration: {line}"
        )));
    }

    let mut service = ServiceDef {
        name: name.to_string(),
        request_schema: request_schema.to_string(),
        response_schema: response_schema.to_string(),
        protocol: None,
        method: None,
        path: None,
        request_headers: Vec::new(),
        request_body: ServiceBodyDef {
            mode: ServiceBodyMode::Buffered,
            schema: Some(request_schema.to_string()),
        },
        response_body: ServiceBodyDef {
            mode: ServiceBodyMode::Buffered,
            schema: Some(response_schema.to_string()),
        },
    };

    let Some(first_fragment) = after_open else {
        return Ok((service, consumed));
    };

    if !first_fragment.is_empty() {
        if let Some((before_close, tail)) = first_fragment.split_once('}') {
            apply_service_properties_fragment(before_close, &mut service)?;
            let tail = tail.trim().trim_start_matches(';').trim();
            if !tail.is_empty() {
                return Err(ApiError::Parse(format!(
                    "unexpected tokens after service block close: {tail}"
                )));
            }
            return Ok((service, consumed));
        }
        apply_service_properties_fragment(first_fragment, &mut service)?;
    }

    for line in &lines[1..] {
        consumed += 1;
        if line.is_empty() {
            continue;
        }

        if let Some(tail) = line.trim_start().strip_prefix('}') {
            let tail = tail.trim().trim_start_matches(';').trim();
            if !tail.is_empty() {
                return Err(ApiError::Parse(format!(
                    "unexpected tokens after service block close: {tail}"
                )));
            }
            return Ok((service, consumed));
        }

        apply_service_properties_fragment(line, &mut service)?;
    }

    Err(ApiError::Parse(format!(
        "service block `{name}` is missing closing brace"
    )))
}

fn apply_service_properties_fragment(
    fragment: &str,
    service: &mut ServiceDef,
) -> std::result::Result<(), ApiError> {
    for entry in fragment.split(';') {
        let line = entry.trim();
        if line.is_empty() {
            continue;
        }

        let (key, value) = line
            .split_once(':')
            .ok_or_else(|| ApiError::Parse(format!("invalid service property: {line}")))?;
        let key = key.trim();
        let value = value.trim().trim_matches('"');

        match key {
            "protocol" => {
                service.protocol = Some(value.to_ascii_lowercase());
            }
            "method" => {
                service.method = Some(value.to_string());
            }
            "path" => {
                service.path = Some(value.to_string());
            }
            "request-header" => {
                service
                    .request_headers
                    .push(parse_service_field_binding(value)?);
            }
            "request-body" => {
                service.request_body = parse_service_body_def(value)?;
            }
            "response-body" => {
                service.response_body = parse_service_body_def(value)?;
            }
            _ => return Err(ApiError::Parse(format!("unknown service property `{key}`"))),
        }
    }

    Ok(())
}

fn parse_service_body_def(value: &str) -> std::result::Result<ServiceBodyDef, ApiError> {
    let trimmed = value.trim();
    if trimmed.eq_ignore_ascii_case("none") {
        return Ok(ServiceBodyDef {
            mode: ServiceBodyMode::None,
            schema: None,
        });
    }

    if let Some(inner) = trimmed
        .strip_prefix("buffered<")
        .and_then(|inner| inner.strip_suffix('>'))
    {
        let inner = inner.trim();
        if inner.is_empty() {
            return Err(ApiError::Parse(
                "service body schema must not be empty".to_string(),
            ));
        }
        return Ok(ServiceBodyDef {
            mode: ServiceBodyMode::Buffered,
            schema: Some(inner.to_string()),
        });
    }

    if let Some(inner) = trimmed
        .strip_prefix("stream<")
        .and_then(|inner| inner.strip_suffix('>'))
    {
        let inner = inner.trim();
        if inner.is_empty() {
            return Err(ApiError::Parse(
                "service body schema must not be empty".to_string(),
            ));
        }
        return Ok(ServiceBodyDef {
            mode: ServiceBodyMode::Stream,
            schema: Some(inner.to_string()),
        });
    }

    Ok(ServiceBodyDef {
        mode: ServiceBodyMode::Buffered,
        schema: Some(trimmed.to_string()),
    })
}

fn parse_service_field_binding(value: &str) -> std::result::Result<ServiceFieldBinding, ApiError> {
    let (field, target) = value
        .split_once('=')
        .ok_or_else(|| ApiError::Parse(format!("invalid service field binding: {value}")))?;
    let field = field.trim();
    let target = target.trim().trim_matches('"');

    if field.is_empty() || target.is_empty() {
        return Err(ApiError::Parse(format!(
            "invalid service field binding: {value}"
        )));
    }

    Ok(ServiceFieldBinding {
        field: field.to_string(),
        target: target.to_string(),
    })
}

fn parse_stream(line: &str) -> std::result::Result<StreamDef, ApiError> {
    let signature = line
        .trim_end_matches(';')
        .strip_prefix("stream ")
        .ok_or_else(|| ApiError::Parse(format!("invalid stream declaration: {line}")))?
        .trim();

    let (name, payload_schema) = signature
        .split_once('(')
        .ok_or_else(|| ApiError::Parse(format!("invalid stream declaration: {line}")))?;

    let name = name.trim();
    let payload_schema = payload_schema.trim_end_matches(')').trim();
    if name.is_empty() || payload_schema.is_empty() {
        return Err(ApiError::Parse(format!(
            "invalid stream declaration: {line}"
        )));
    }

    Ok(StreamDef {
        name: name.to_string(),
        payload_schema: payload_schema.to_string(),
    })
}

fn validate_package(package: &ContractPackage) -> std::result::Result<(), ApiError> {
    for service in &package.services {
        if service.protocol.as_deref() != Some("http") {
            continue;
        }

        let fields = schema_fields(package, &service.request_schema).ok_or_else(|| {
            ApiError::Parse(format!(
                "HTTP service `{}` references unknown request schema `{}`",
                service.name, service.request_schema
            ))
        })?;

        let path_params = service
            .path
            .as_deref()
            .map(extract_path_parameters)
            .unwrap_or_default();
        let mut bound_fields = BTreeSet::new();

        for path_param in path_params {
            if !fields.iter().any(|field| field.name == path_param) {
                return Err(ApiError::Parse(format!(
                    "HTTP service `{}` path binds unknown field `{}`",
                    service.name, path_param
                )));
            }
            bound_fields.insert(path_param);
        }

        for header in &service.request_headers {
            if !fields.iter().any(|field| field.name == header.field) {
                return Err(ApiError::Parse(format!(
                    "HTTP service `{}` header binding references unknown field `{}`",
                    service.name, header.field
                )));
            }
            bound_fields.insert(header.field.clone());
        }

        for field in fields {
            if !bound_fields.contains(&field.name) {
                return Err(ApiError::Parse(format!(
                    "HTTP service `{}` must bind request field `{}` to path or header",
                    service.name, field.name
                )));
            }
        }
    }

    Ok(())
}

fn split_namespace_version(package: &str) -> (String, String) {
    let mut parts = package.split('.').collect::<Vec<_>>();
    if let Some(last) = parts.last().copied()
        && last.starts_with('v')
        && last[1..].chars().all(|ch| ch.is_ascii_digit())
    {
        parts.pop();
        let namespace = parts.join(".");
        return (namespace, last.to_string());
    }

    (package.to_string(), "v1".to_string())
}

fn highest_version(
    versions: &BTreeMap<String, ContractPackage>,
) -> Option<(&String, &ContractPackage)> {
    versions
        .iter()
        .max_by_key(|(version, _)| version_number(version))
}

fn version_number(version: &str) -> u64 {
    version
        .strip_prefix('v')
        .and_then(|num| num.parse::<u64>().ok())
        .unwrap_or(0)
}

fn ensure_compatible(
    previous: &ContractPackage,
    next: &ContractPackage,
) -> std::result::Result<(), String> {
    let previous_schemas = previous
        .schemas
        .iter()
        .map(|schema| (schema.name.as_str(), schema))
        .collect::<BTreeMap<_, _>>();
    let next_schemas = next
        .schemas
        .iter()
        .map(|schema| (schema.name.as_str(), schema))
        .collect::<BTreeMap<_, _>>();

    for (name, old_schema) in previous_schemas {
        let new_schema = next_schemas
            .get(name)
            .ok_or_else(|| format!("schema `{name}` was removed"))?;

        let old_fields = old_schema
            .fields
            .iter()
            .map(|field| (field.name.as_str(), field.ty.as_str()))
            .collect::<BTreeMap<_, _>>();
        let new_fields = new_schema
            .fields
            .iter()
            .map(|field| (field.name.as_str(), field.ty.as_str()))
            .collect::<BTreeMap<_, _>>();

        for (field_name, field_ty) in old_fields {
            let Some(new_ty) = new_fields.get(field_name) else {
                return Err(format!(
                    "schema `{name}` removed field `{field_name}` from prior version"
                ));
            };

            if field_ty != *new_ty {
                return Err(format!(
                    "schema `{name}` changed field `{field_name}` type `{field_ty}` -> `{new_ty}`"
                ));
            }
        }
    }

    let previous_events = previous
        .events
        .iter()
        .map(|event| (event.name.as_str(), event))
        .collect::<BTreeMap<_, _>>();
    let next_events = next
        .events
        .iter()
        .map(|event| (event.name.as_str(), event))
        .collect::<BTreeMap<_, _>>();

    for (name, old_event) in previous_events {
        let Some(new_event) = next_events.get(name) else {
            return Err(format!("event `{name}` was removed"));
        };

        if old_event.payload_schema != new_event.payload_schema {
            return Err(format!(
                "event `{name}` changed payload schema `{}` -> `{}`",
                old_event.payload_schema, new_event.payload_schema
            ));
        }
    }

    let previous_services = previous
        .services
        .iter()
        .map(|service| (service.name.as_str(), service))
        .collect::<BTreeMap<_, _>>();
    let next_services = next
        .services
        .iter()
        .map(|service| (service.name.as_str(), service))
        .collect::<BTreeMap<_, _>>();

    for (name, old_service) in previous_services {
        let Some(new_service) = next_services.get(name) else {
            return Err(format!("service `{name}` was removed"));
        };

        if old_service.request_schema != new_service.request_schema {
            return Err(format!(
                "service `{name}` changed request schema `{}` -> `{}`",
                old_service.request_schema, new_service.request_schema
            ));
        }
        if old_service.response_schema != new_service.response_schema {
            return Err(format!(
                "service `{name}` changed response schema `{}` -> `{}`",
                old_service.response_schema, new_service.response_schema
            ));
        }
        if old_service.protocol != new_service.protocol
            || old_service.method != new_service.method
            || old_service.path != new_service.path
            || old_service.request_headers != new_service.request_headers
            || old_service.request_body != new_service.request_body
            || old_service.response_body != new_service.response_body
        {
            return Err(format!("service `{name}` changed transport binding"));
        }
    }

    Ok(())
}

fn map_field_type(ty: &str) -> &'static str {
    match ty {
        "string" => "String",
        "bytes" => "Vec<u8>",
        "bool" => "bool",
        "u8" => "u8",
        "u16" => "u16",
        "u32" => "u32",
        "u64" => "u64",
        "i8" => "i8",
        "i16" => "i16",
        "i32" => "i32",
        "i64" => "i64",
        "f32" => "f32",
        "f64" => "f64",
        _ => "String",
    }
}

fn const_name(raw: &str) -> String {
    raw.chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect()
}

fn option_literal(value: Option<&str>) -> String {
    match value {
        Some(value) => format!("Some({value:?})"),
        None => "None".to_string(),
    }
}

fn generate_body_binding(body: &ServiceBodyDef) -> String {
    format!(
        "ServiceBodyBinding {{ mode: ServiceBodyMode::{}, schema: {} }}",
        match body.mode {
            ServiceBodyMode::None => "None",
            ServiceBodyMode::Buffered => "Buffered",
            ServiceBodyMode::Stream => "Stream",
        },
        option_literal(body.schema.as_deref()),
    )
}

pub fn parse_contract_ref(raw: &str) -> Result<ContractRef> {
    let (lhs, version) = raw
        .split_once('@')
        .ok_or_else(|| anyhow::anyhow!("contract ref must be namespace/name@version"))?;
    let (namespace, name) = lhs
        .split_once('/')
        .ok_or_else(|| anyhow::anyhow!("contract ref must be namespace/name@version"))?;

    if namespace.trim().is_empty() || name.trim().is_empty() || version.trim().is_empty() {
        return Err(anyhow::anyhow!(
            "contract ref must have namespace/name@version"
        ));
    }

    Ok(ContractRef {
        namespace: namespace.to_string(),
        name: name.to_string(),
        version: version.to_string(),
    })
}

pub fn ensure_pipeline_consistency(state: &ControlPlaneState) -> std::result::Result<(), ApiError> {
    for pipeline in state.pipelines.values() {
        for edge in &pipeline.edges {
            if !state.deployments.contains_key(&edge.from.app) {
                return Err(ApiError::UnknownDeployment(edge.from.app.clone()));
            }
            if !state.deployments.contains_key(&edge.to.app) {
                return Err(ApiError::UnknownDeployment(edge.to.app.clone()));
            }
            if !state.registry.has_contract(&edge.from.contract) {
                return Err(ApiError::InvalidContract);
            }
            if !state.registry.has_contract(&edge.to.contract) {
                return Err(ApiError::InvalidContract);
            }
        }
    }

    Ok(())
}

pub fn collect_contracts_for_app(state: &ControlPlaneState, app: &str) -> BTreeSet<ContractRef> {
    let mut out = BTreeSet::new();
    for pipeline in state.pipelines.values() {
        for edge in &pipeline.edges {
            if edge.from.app == app {
                out.insert(edge.from.contract.clone());
            }
            if edge.to.app == app {
                out.insert(edge.to.contract.clone());
            }
        }
    }
    out
}

impl Ord for ContractRef {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (&self.namespace, &self.name, &self.version).cmp(&(
            &other.namespace,
            &other.name,
            &other.version,
        ))
    }
}

impl PartialOrd for ContractRef {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"
package media.pipeline.v1;

schema Frame {
  camera_id: string;
  ts_ms: u64;
  jpeg: bytes;
}

schema UploadHead {
  camera_id: string;
  ts_ms: u64;
}

event camera.frames(Frame) {
  partitions: 12;
  retention: "24h";
  delivery: at_least_once;
  replay: enabled;
}

service detect(Frame) -> Frame;
service mirror(Frame) -> Frame {
  protocol: quic;
  method: mirror;
  request-body: buffered<Frame>;
  response-body: buffered<Frame>;
}
service upload(UploadHead) -> Frame {
  protocol: http;
  method: POST;
  path: "/upload/{camera_id}";
  request-header: ts_ms = "x-ts-ms";
  request-body: stream<bytes>;
  response-body: buffered<Frame>;
}
stream camera.raw(Frame);
"#;

    #[test]
    fn parses_package_and_events() {
        let package = parse_idl(SAMPLE).expect("parse");
        assert_eq!(package.namespace, "media.pipeline");
        assert_eq!(package.version, "v1");
        assert_eq!(package.schemas.len(), 2);
        assert_eq!(package.events.len(), 1);
        assert_eq!(package.services.len(), 3);
        assert_eq!(package.services[0].name, "detect");
        assert_eq!(package.streams.len(), 1);
        assert_eq!(package.events[0].name, "camera.frames");
        assert!(package.events[0].replay_enabled);
    }

    #[test]
    fn parses_service_block_with_streaming_body() {
        let package = parse_idl(SAMPLE).expect("parse");
        assert_eq!(package.services.len(), 3);
        let upload = package
            .services
            .iter()
            .find(|service| service.name == "upload")
            .expect("upload service");
        assert_eq!(upload.protocol.as_deref(), Some("http"));
        assert_eq!(upload.method.as_deref(), Some("POST"));
        assert_eq!(upload.path.as_deref(), Some("/upload/{camera_id}"));
        assert_eq!(
            upload.request_headers,
            vec![ServiceFieldBinding {
                field: "ts_ms".to_string(),
                target: "x-ts-ms".to_string(),
            }]
        );
        assert_eq!(upload.request_body.mode, ServiceBodyMode::Stream);
        assert_eq!(upload.request_body.schema.as_deref(), Some("bytes"));
        assert_eq!(upload.response_body.mode, ServiceBodyMode::Buffered);
        assert_eq!(upload.response_body.schema.as_deref(), Some("Frame"));
    }

    #[test]
    fn registry_validates_backward_compatibility() {
        let mut registry = ContractRegistry::default();
        let v1 = parse_idl(SAMPLE).expect("parse v1");
        registry.register_package(v1).expect("register v1");

        let incompatible = parse_idl(
            "package media.pipeline.v2;\n\
             schema Frame { camera_id: string; }\n\
             event camera.frames(Frame) { replay: enabled; }",
        )
        .expect("parse v2");

        let err = registry
            .register_package(incompatible)
            .expect_err("must fail");
        assert!(err.to_string().contains("removed field"));
    }

    #[test]
    fn rust_bindings_generation_is_non_empty() {
        let package = parse_idl(SAMPLE).expect("parse");
        let generated = generate_rust_bindings(&package);
        assert!(generated.contains("pub struct Frame"));
        assert!(generated.contains("EVENT_CAMERA_FRAMES"));
        assert!(generated.contains("pub const SERVICE_UPLOAD"));
        assert!(generated.contains("ServiceBodyMode::Stream"));
        assert!(generated.contains("pub const STREAM_CAMERA_RAW"));
        assert!(generated.contains("pub mod upload"));
        assert!(generated.contains("pub mod mirror"));
        assert!(generated.contains("pub mod camera_raw"));
    }

    #[test]
    fn contract_ref_parser_rejects_invalid_shape() {
        assert!(parse_contract_ref("bad").is_err());
        let contract = parse_contract_ref("media.pipeline/camera.frames@v1").expect("parse");
        assert_eq!(contract.namespace, "media.pipeline");
    }

    #[test]
    fn pipeline_consistency_checks_registry_and_deployments() {
        let mut state = ControlPlaneState::new_local_default();
        let package = parse_idl(SAMPLE).expect("parse sample");
        state.registry.register_package(package).expect("register");

        state
            .upsert_deployment(DeploymentSpec {
                app: "ingest".to_string(),
                module: "ingest.wasm".to_string(),
                replicas: 1,
                contracts: Vec::new(),
                isolation: IsolationProfile::Standard,
            })
            .expect("deployment");
        state
            .upsert_deployment(DeploymentSpec {
                app: "detector".to_string(),
                module: "detector.wasm".to_string(),
                replicas: 1,
                contracts: Vec::new(),
                isolation: IsolationProfile::Standard,
            })
            .expect("deployment");

        state.upsert_pipeline(PipelineSpec {
            name: "p".to_string(),
            namespace: "media".to_string(),
            edges: vec![PipelineEdge {
                from: PipelineEndpoint {
                    app: "ingest".to_string(),
                    contract: ContractRef {
                        namespace: "media.pipeline".to_string(),
                        name: "camera.frames".to_string(),
                        version: "v1".to_string(),
                    },
                },
                to: PipelineEndpoint {
                    app: "detector".to_string(),
                    contract: ContractRef {
                        namespace: "media.pipeline".to_string(),
                        name: "camera.frames".to_string(),
                        version: "v1".to_string(),
                    },
                },
            }],
        });

        ensure_pipeline_consistency(&state).expect("consistent");
    }
}
