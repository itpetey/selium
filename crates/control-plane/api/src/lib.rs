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

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ServiceDef {
    pub name: String,
    pub request_schema: String,
    pub response_schema: String,
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
            services.push(parse_service(line)?);
            index += 1;
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

    Ok(ContractPackage {
        package,
        namespace,
        version,
        schemas,
        events,
        services,
        streams,
    })
}

pub fn generate_rust_bindings(package: &ContractPackage) -> String {
    let mut out = String::new();
    out.push_str("// Generated by Selium IDL compiler\n");
    out.push_str("use rkyv::{Archive, Serialize, Deserialize};\n\n");

    for schema in &package.schemas {
        out.push_str("#[derive(Debug, Clone, Archive, Serialize, Deserialize)]\n");
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
            event
                .name
                .chars()
                .map(|ch| if ch.is_ascii_alphanumeric() {
                    ch.to_ascii_uppercase()
                } else {
                    '_'
                })
                .collect::<String>(),
            event.name
        ));
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

fn parse_service(line: &str) -> std::result::Result<ServiceDef, ApiError> {
    let signature = line
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

    let request_schema = request_schema.trim_end_matches(')').trim();
    let name = name.trim();
    let response_schema = response_schema.trim();

    if name.is_empty() || request_schema.is_empty() || response_schema.is_empty() {
        return Err(ApiError::Parse(format!(
            "invalid service declaration: {line}"
        )));
    }

    Ok(ServiceDef {
        name: name.to_string(),
        request_schema: request_schema.to_string(),
        response_schema: response_schema.to_string(),
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

    Ok(())
}

fn map_field_type(ty: &str) -> &'static str {
    match ty {
        "string" => "String",
        "bytes" => "Vec<u8>",
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

event camera.frames(Frame) {
  partitions: 12;
  retention: "24h";
  delivery: at_least_once;
  replay: enabled;
}

service detect(Frame) -> Frame;
stream camera.raw(Frame);
"#;

    #[test]
    fn parses_package_and_events() {
        let package = parse_idl(SAMPLE).expect("parse");
        assert_eq!(package.namespace, "media.pipeline");
        assert_eq!(package.version, "v1");
        assert_eq!(package.schemas.len(), 1);
        assert_eq!(package.events.len(), 1);
        assert_eq!(package.services.len(), 1);
        assert_eq!(package.streams.len(), 1);
        assert_eq!(package.events[0].name, "camera.frames");
        assert!(package.events[0].replay_enabled);
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
