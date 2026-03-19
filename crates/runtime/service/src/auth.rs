use std::{collections::BTreeSet, str::FromStr};

use anyhow::{Context, Result, anyhow, bail};
use quinn::Connection;
use selium_abi::{PrincipalKind, PrincipalRef};
use selium_control_plane_api::{DiscoveryCapabilityScope, DiscoveryPattern, PublicEndpointRef};
use selium_control_plane_protocol::Method;
use x509_parser::{certificate::X509Certificate, extensions::GeneralName, prelude::FromDer};

const PRINCIPAL_URI_PREFIX: &str = "spiffe://selium/principal/";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum MethodGrant {
    ControlRead,
    ControlReadGlobal,
    ControlWrite,
    NodeManage,
    BridgeManage,
    Peer,
}

impl FromStr for MethodGrant {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "control_read" | "control-read" => Ok(Self::ControlRead),
            "control_read_global" | "control-read-global" => Ok(Self::ControlReadGlobal),
            "control_write" | "control-write" => Ok(Self::ControlWrite),
            "node_manage" | "node-manage" => Ok(Self::NodeManage),
            "bridge_manage" | "bridge-manage" => Ok(Self::BridgeManage),
            "peer" => Ok(Self::Peer),
            _ => Err("unknown method grant"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PrincipalSelector {
    Exact(PrincipalRef),
    Kind(PrincipalKind),
}

impl PrincipalSelector {
    fn matches(&self, principal: &PrincipalRef) -> bool {
        match self {
            Self::Exact(expected) => expected == principal,
            Self::Kind(kind) => principal.kind == *kind,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AccessGrant {
    principal: PrincipalSelector,
    pub methods: BTreeSet<MethodGrant>,
    pub discovery_scope: DiscoveryCapabilityScope,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct AccessPolicy {
    grants: Vec<AccessGrant>,
}

#[derive(Debug, Clone)]
pub(crate) struct AuthenticatedRequestContext {
    pub principal: PrincipalRef,
    pub peer_fingerprint: String,
    grants: Vec<AccessGrant>,
}

impl AccessPolicy {
    pub fn from_specs(specs: &[String]) -> Result<Self> {
        let mut grants = Vec::with_capacity(specs.len());
        for spec in specs {
            grants.push(parse_access_grant(spec)?);
        }
        Ok(Self { grants })
    }

    pub fn resolve(
        &self,
        principal: PrincipalRef,
        peer_fingerprint: String,
    ) -> AuthenticatedRequestContext {
        let grants = self
            .grants
            .iter()
            .filter(|grant| grant.principal.matches(&principal))
            .cloned()
            .collect();
        AuthenticatedRequestContext {
            principal,
            peer_fingerprint,
            grants,
        }
    }
}

impl AuthenticatedRequestContext {
    pub fn allows(&self, method: Method) -> bool {
        if matches!(method, Method::ControlStatus | Method::ControlMetrics) {
            self.allows_all_workloads_for(method)
        } else {
            self.grants_for_method(method).next().is_some()
        }
    }

    pub fn discovery_scope_for(&self, method: Method) -> DiscoveryCapabilityScope {
        let mut operations = Vec::new();
        let mut workloads = Vec::new();
        let mut endpoints = Vec::new();
        let mut allow_operational_processes = false;
        let mut allow_all_workloads = false;
        let mut allow_all_endpoints = false;

        for grant in self.grants_for_method(method) {
            for operation in &grant.discovery_scope.operations {
                if !operations.contains(operation) {
                    operations.push(*operation);
                }
            }
            if grant.discovery_scope.workloads.is_empty() {
                allow_all_workloads = true;
            } else {
                workloads.extend(grant.discovery_scope.workloads.iter().cloned());
            }
            if grant.discovery_scope.endpoints.is_empty() {
                allow_all_endpoints = true;
            } else {
                endpoints.extend(grant.discovery_scope.endpoints.iter().cloned());
            }
            allow_operational_processes |= grant.discovery_scope.allow_operational_processes;
        }

        if allow_all_workloads {
            workloads.push(DiscoveryPattern::Prefix(String::new()));
        }
        if allow_all_endpoints {
            endpoints.push(DiscoveryPattern::Prefix(String::new()));
        }

        DiscoveryCapabilityScope {
            operations,
            workloads,
            endpoints,
            allow_operational_processes,
        }
    }

    pub fn principal(&self) -> &PrincipalRef {
        &self.principal
    }

    pub fn allows_all_workloads_for(&self, method: Method) -> bool {
        self.grants_for_method(method).any(|grant| {
            grant.discovery_scope.workloads.is_empty()
                || grant.discovery_scope.workloads.iter().any(
                    |pattern| matches!(pattern, DiscoveryPattern::Prefix(prefix) if prefix.is_empty()),
                )
        })
    }

    pub fn allows_workload_for(&self, method: Method, workload_key: &str) -> bool {
        self.grants_for_method(method).any(|grant| {
            grant.discovery_scope.workloads.is_empty()
                || grant
                    .discovery_scope
                    .workloads
                    .iter()
                    .any(|pattern| pattern.matches(workload_key))
        })
    }

    pub fn allows_endpoint_for(&self, method: Method, endpoint: &PublicEndpointRef) -> bool {
        let endpoint_key = endpoint.key();
        let workload_key = endpoint.workload.key();

        self.grants_for_method(method).any(|grant| {
            let workload_allowed = grant.discovery_scope.workloads.is_empty()
                || grant
                    .discovery_scope
                    .workloads
                    .iter()
                    .any(|pattern| pattern.matches(&workload_key));
            let endpoint_allowed = grant.discovery_scope.endpoints.is_empty()
                || grant
                    .discovery_scope
                    .endpoints
                    .iter()
                    .any(|pattern| pattern.matches(&endpoint_key));
            workload_allowed && endpoint_allowed
        })
    }

    fn grants_for_method(&self, method: Method) -> impl Iterator<Item = &AccessGrant> {
        let required = grants_for_method(method);
        self.grants.iter().filter(move |grant| {
            required
                .iter()
                .any(|required| grant.methods.contains(required))
        })
    }
}

pub(crate) fn principal_uri(principal: &PrincipalRef) -> String {
    format!(
        "{PRINCIPAL_URI_PREFIX}{}/{}",
        principal.kind.as_str(),
        principal.external_subject_id
    )
}

pub(crate) fn principal_from_connection(connection: &Connection) -> Result<PrincipalRef> {
    let identity = connection
        .peer_identity()
        .ok_or_else(|| anyhow!("peer certificate chain missing"))?;
    let certs = identity
        .downcast::<Vec<rustls::pki_types::CertificateDer<'static>>>()
        .map_err(|_| anyhow!("unexpected QUIC peer identity type"))?;
    let cert = certs
        .first()
        .ok_or_else(|| anyhow!("peer certificate chain empty"))?;
    principal_from_cert(cert.as_ref())
}

pub(crate) fn principal_from_cert(cert_der: &[u8]) -> Result<PrincipalRef> {
    let (_, certificate) = X509Certificate::from_der(cert_der)
        .map_err(|err| anyhow!("parse X.509 certificate: {err}"))?;
    let san = certificate
        .subject_alternative_name()
        .map_err(|err| anyhow!("read certificate subjectAltName: {err}"))?
        .ok_or_else(|| {
            anyhow!(
                "certificate missing subjectAltName; Selium client and peer certificates must carry a principal URI SAN like `{PRINCIPAL_URI_PREFIX}<kind>/<external-subject-id>`"
            )
        })?;

    for name in &san.value.general_names {
        if let GeneralName::URI(uri) = name
            && let Some(principal) = parse_principal_uri(uri)
        {
            return Ok(principal);
        }
    }

    bail!(
        "certificate missing Selium principal URI; Selium client and peer certificates must carry a principal URI SAN like `{PRINCIPAL_URI_PREFIX}<kind>/<external-subject-id>`"
    )
}

pub(crate) fn parse_principal_uri(uri: &str) -> Option<PrincipalRef> {
    let suffix = uri.strip_prefix(PRINCIPAL_URI_PREFIX)?;
    let (kind, external_subject_id) = suffix.split_once('/')?;
    let kind = PrincipalKind::from_str(kind).ok()?;
    if external_subject_id.trim().is_empty() {
        return None;
    }
    Some(PrincipalRef::new(kind, external_subject_id))
}

pub(crate) fn peer_fingerprint(connection: &Connection) -> String {
    use std::fmt::Write as _;

    let mut out = String::new();
    let stable_id = connection.stable_id().to_ne_bytes();
    for byte in stable_id {
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

fn parse_access_grant(spec: &str) -> Result<AccessGrant> {
    let mut principal = None;
    let mut methods = BTreeSet::new();
    let mut workloads = Vec::new();
    let mut endpoints = Vec::new();
    let mut allow_operational_processes = false;

    for part in spec.split(';').filter(|part| !part.trim().is_empty()) {
        let (key, value) = part
            .split_once('=')
            .ok_or_else(|| anyhow!("invalid access grant segment `{part}`"))?;
        match key.trim() {
            "principal" => principal = Some(parse_principal_selector(value.trim())?),
            "methods" => {
                for method in value.split(',').filter(|item| !item.trim().is_empty()) {
                    let method = MethodGrant::from_str(method.trim())
                        .map_err(|err| anyhow!("invalid access grant method `{method}`: {err}"))?;
                    methods.insert(method);
                }
            }
            "workloads" => workloads = parse_patterns(value)?,
            "endpoints" => endpoints = parse_patterns(value)?,
            "allow-operational-processes" => {
                allow_operational_processes = value
                    .trim()
                    .parse::<bool>()
                    .with_context(|| format!("parse allow-operational-processes `{value}`"))?;
            }
            other => bail!("unknown access grant field `{other}`"),
        }
    }

    let principal = principal.ok_or_else(|| anyhow!("access grant missing principal"))?;
    if methods.is_empty() {
        bail!("access grant must declare at least one method");
    }

    let operations = if methods.contains(&MethodGrant::ControlWrite)
        || methods.contains(&MethodGrant::NodeManage)
    {
        vec![
            selium_control_plane_api::DiscoveryOperation::Discover,
            selium_control_plane_api::DiscoveryOperation::Bind,
        ]
    } else if methods.contains(&MethodGrant::ControlRead)
        || methods.contains(&MethodGrant::ControlReadGlobal)
    {
        vec![selium_control_plane_api::DiscoveryOperation::Discover]
    } else {
        Vec::new()
    };

    Ok(AccessGrant {
        principal,
        methods,
        discovery_scope: DiscoveryCapabilityScope {
            operations,
            workloads,
            endpoints,
            allow_operational_processes,
        },
    })
}

fn parse_principal_selector(value: &str) -> Result<PrincipalSelector> {
    let (kind, external_subject_id) = value
        .split_once(':')
        .ok_or_else(|| anyhow!("invalid principal `{value}`"))?;
    let kind = PrincipalKind::from_str(kind)
        .map_err(|err| anyhow!("invalid principal kind `{kind}`: {err}"))?;
    let external_subject_id = external_subject_id.trim();
    if external_subject_id == "*" {
        return Ok(PrincipalSelector::Kind(kind));
    }
    if external_subject_id.is_empty() {
        bail!("principal external subject id must not be empty");
    }
    Ok(PrincipalSelector::Exact(PrincipalRef::new(
        kind,
        external_subject_id,
    )))
}

fn parse_patterns(value: &str) -> Result<Vec<DiscoveryPattern>> {
    let mut patterns = Vec::new();
    for pattern in value.split(',').filter(|item| !item.trim().is_empty()) {
        let pattern = pattern.trim();
        if pattern == "*" {
            patterns.push(DiscoveryPattern::Prefix(String::new()));
        } else if let Some(prefix) = pattern.strip_suffix('*') {
            patterns.push(DiscoveryPattern::Prefix(prefix.to_string()));
        } else {
            patterns.push(DiscoveryPattern::Exact(pattern.to_string()));
        }
    }
    Ok(patterns)
}

fn grants_for_method(method: Method) -> &'static [MethodGrant] {
    match method {
        Method::ControlMutate | Method::StartInstance | Method::StopInstance => {
            &[MethodGrant::ControlWrite]
        }
        Method::ActivateEndpointBridge
        | Method::DeactivateEndpointBridge
        | Method::DeliverBridgeMessage => &[MethodGrant::BridgeManage, MethodGrant::Peer],
        Method::RaftRequestVote | Method::RaftAppendEntries => &[MethodGrant::Peer],
        Method::ListInstances | Method::RuntimeUsageQuery | Method::SubscribeGuestLogs => {
            &[MethodGrant::NodeManage]
        }
        Method::ControlQuery | Method::ControlReplay => {
            &[MethodGrant::ControlRead, MethodGrant::ControlReadGlobal]
        }
        Method::ControlStatus | Method::ControlMetrics => &[MethodGrant::ControlReadGlobal],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use selium_control_plane_api::{
        ContractKind, DiscoveryOperation, PublicEndpointRef, WorkloadRef,
    };

    #[test]
    fn principal_uri_round_trips() {
        let principal = PrincipalRef::new(PrincipalKind::Machine, "client.localhost");
        let uri = principal_uri(&principal);
        assert_eq!(parse_principal_uri(&uri), Some(principal));
    }

    #[test]
    fn parse_access_grant_expands_patterns() {
        let grant = parse_access_grant(
            "principal=machine:client.localhost;methods=control_read,control_write;workloads=tenant-a/*,*;endpoints=tenant-a/media/router#service:*;allow-operational-processes=true",
        )
        .expect("grant");

        assert!(grant.methods.contains(&MethodGrant::ControlRead));
        assert!(grant.methods.contains(&MethodGrant::ControlWrite));
        assert!(
            grant
                .discovery_scope
                .operations
                .contains(&DiscoveryOperation::Bind)
        );
        assert!(grant.discovery_scope.allow_operational_processes);
        assert_eq!(
            grant.discovery_scope.workloads,
            vec![
                DiscoveryPattern::Prefix("tenant-a/".to_string()),
                DiscoveryPattern::Prefix(String::new())
            ]
        );
        assert_eq!(
            grant.discovery_scope.endpoints,
            vec![DiscoveryPattern::Prefix(
                "tenant-a/media/router#service:".to_string()
            )]
        );
    }

    #[test]
    fn access_policy_matches_principal_exactly() {
        let policy = AccessPolicy::from_specs(&[
            "principal=machine:client.localhost;methods=control_read;workloads=*".to_string(),
        ])
        .expect("policy");

        let allowed = policy.resolve(
            PrincipalRef::new(PrincipalKind::Machine, "client.localhost"),
            "fp".to_string(),
        );
        let denied = policy.resolve(
            PrincipalRef::new(PrincipalKind::Machine, "other-client"),
            "fp".to_string(),
        );

        assert!(allowed.allows(Method::ControlQuery));
        assert!(!allowed.allows(Method::ControlStatus));
        assert!(!allowed.allows(Method::ControlMutate));
        assert!(!denied.allows(Method::ControlQuery));
    }

    #[test]
    fn access_policy_can_match_all_principals_of_a_kind() {
        let policy =
            AccessPolicy::from_specs(&["principal=runtime-peer:*;methods=peer".to_string()])
                .expect("policy");

        let allowed = policy.resolve(
            PrincipalRef::new(PrincipalKind::RuntimePeer, "node-a"),
            "fp".to_string(),
        );
        let denied = policy.resolve(
            PrincipalRef::new(PrincipalKind::Machine, "client.localhost"),
            "fp".to_string(),
        );

        assert!(allowed.allows(Method::RaftAppendEntries));
        assert!(!denied.allows(Method::RaftAppendEntries));
    }

    #[test]
    fn node_manage_grants_bind_discovery() {
        let grant = parse_access_grant(
            "principal=machine:client.localhost;methods=node_manage;workloads=tenant-a/*",
        )
        .expect("grant");

        assert_eq!(
            grant.discovery_scope.operations,
            vec![DiscoveryOperation::Discover, DiscoveryOperation::Bind]
        );
    }

    #[test]
    fn workload_checks_are_scoped_to_the_authorised_method() {
        let policy = AccessPolicy::from_specs(&[
            "principal=machine:client.localhost;methods=control_write;workloads=tenant-a/*"
                .to_string(),
            "principal=machine:client.localhost;methods=control_read;workloads=tenant-b/*"
                .to_string(),
        ])
        .expect("policy");
        let request_context = policy.resolve(
            PrincipalRef::new(PrincipalKind::Machine, "client.localhost"),
            "fp".to_string(),
        );

        assert!(request_context.allows_workload_for(Method::ControlMutate, "tenant-a/api"));
        assert!(!request_context.allows_workload_for(Method::ControlMutate, "tenant-b/api"));
        assert!(request_context.allows_workload_for(Method::ControlQuery, "tenant-b/api"));
    }

    #[test]
    fn endpoint_checks_require_matching_bridge_manage_scope() {
        let policy = AccessPolicy::from_specs(&[
            "principal=machine:client.localhost;methods=bridge_manage;endpoints=tenant-a/media/router#service:*".to_string(),
            "principal=machine:client.localhost;methods=control_read;endpoints=tenant-a/media/reader#service:*".to_string(),
        ])
        .expect("policy");
        let request_context = policy.resolve(
            PrincipalRef::new(PrincipalKind::Machine, "client.localhost"),
            "fp".to_string(),
        );
        let allowed_endpoint = PublicEndpointRef {
            workload: WorkloadRef {
                tenant: "tenant-a".to_string(),
                namespace: "media".to_string(),
                name: "router".to_string(),
            },
            kind: ContractKind::Service,
            name: "rpc".to_string(),
        };
        let denied_endpoint = PublicEndpointRef {
            workload: WorkloadRef {
                tenant: "tenant-a".to_string(),
                namespace: "media".to_string(),
                name: "reader".to_string(),
            },
            kind: ContractKind::Service,
            name: "rpc".to_string(),
        };

        assert!(
            request_context.allows_endpoint_for(Method::ActivateEndpointBridge, &allowed_endpoint)
        );
        assert!(
            !request_context.allows_endpoint_for(Method::ActivateEndpointBridge, &denied_endpoint)
        );
    }

    #[test]
    fn control_status_requires_global_read_with_full_workload_scope() {
        let scoped_policy = AccessPolicy::from_specs(&[
            "principal=machine:client.localhost;methods=control_read_global;workloads=tenant-a/*"
                .to_string(),
        ])
        .expect("scoped policy");
        let global_policy = AccessPolicy::from_specs(&[
            "principal=machine:client.localhost;methods=control_read_global;workloads=*"
                .to_string(),
        ])
        .expect("global policy");

        let scoped = scoped_policy.resolve(
            PrincipalRef::new(PrincipalKind::Machine, "client.localhost"),
            "fp".to_string(),
        );
        let global = global_policy.resolve(
            PrincipalRef::new(PrincipalKind::Machine, "client.localhost"),
            "fp".to_string(),
        );

        assert!(!scoped.allows(Method::ControlStatus));
        assert!(!scoped.allows(Method::ControlMetrics));
        assert!(global.allows(Method::ControlStatus));
        assert!(global.allows(Method::ControlMetrics));
        assert!(global.allows(Method::ControlQuery));
    }

    #[test]
    fn peer_grant_allows_internal_bridge_methods() {
        let policy =
            AccessPolicy::from_specs(&["principal=runtime-peer:*;methods=peer".to_string()])
                .expect("policy");
        let request_context = policy.resolve(
            PrincipalRef::new(PrincipalKind::RuntimePeer, "peer.localhost"),
            "fp".to_string(),
        );
        let endpoint = PublicEndpointRef {
            workload: WorkloadRef {
                tenant: "tenant-a".to_string(),
                namespace: "media".to_string(),
                name: "router".to_string(),
            },
            kind: ContractKind::Service,
            name: "rpc".to_string(),
        };

        assert!(request_context.allows(Method::ActivateEndpointBridge));
        assert!(request_context.allows(Method::DeactivateEndpointBridge));
        assert!(request_context.allows(Method::DeliverBridgeMessage));
        assert!(request_context.allows(Method::RaftAppendEntries));
        assert!(request_context.allows_endpoint_for(Method::DeliverBridgeMessage, &endpoint));
    }
}
