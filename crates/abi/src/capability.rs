//! Capability vocabulary, grant selectors, and scope matching.

use crate::uri;
use crate::{LocalResourceId, NetworkEndpoint, SharedResourceId};
use rkyv::{Archive, Deserialize, Serialize};

/// Capability required to perform a class of host operations.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum Capability {
    /// Permission to start, stop, and inspect processes.
    ProcessLifecycle,
    /// Permission to allocate, attach, read, write, and destroy shared memory.
    SharedMemory,
    /// Permission to create, attach, notify, wait on, and close signals.
    Signal,
    /// Permission to use network listeners, sessions, streams, and requests.
    Network,
    /// Permission to use durable volumes.
    Storage,
    /// Permission to manage session lifetime.
    SessionLifecycle,
    /// Permission to read activity log events.
    ActivityRead,
    /// Permission to read metering observations.
    MeteringRead,
    /// Permission to read guest log entries.
    GuestLogRead,
    /// Permission to write guest log entries.
    GuestLogWrite,
    /// Permission to create, attach, send, and receive from host-mediated connection queues.
    HostQueue,
    /// Permission to confer child grants the holder does not itself hold,
    /// subject to a tenant scope. Held only by system guests (e.g. the
    /// per-tenant bridge-server) that spawn children carrying an external
    /// client's grants.
    DelegateGrants,
    /// Permission to publish one exact reserved registry name (`sel:///…`)
    /// under a host-issued, non-delegable claim. The URI the claim covers is
    /// carried by a `ResourceSelector::Uri`; without a matching claim a guest
    /// cannot register that name. Bootstrap-provisioned only: a spawn that
    /// confers it on a child is denied.
    RouteClaim,
    /// Permission to sign tenant CAs and user certificates through the signing
    /// hostcalls (`SignTenantCa`, `SignUserCert`, `RevokeCa`). Bootstrap-
    /// provisioned only: like `DelegateGrants`, it can never be conferred on a
    /// child process. Held solely by the identity system guest.
    MintCertificate,
    /// Permission to author host-held quota counters via the `QuotaSet` and
    /// `QuotaClear` hostcalls. Bootstrap-provisioned only: like
    /// `DelegateGrants` and `MintCertificate`, it can never be conferred on a
    /// child process. Held solely by the accounting system guest.
    QuotaWrite,
    /// Permission to probe whether another process holds a capability via the
    /// `ProcessCapability` hostcall. Bootstrap-provisioned only: like
    /// `DelegateGrants`, `MintCertificate` and `QuotaWrite`, it can never be
    /// conferred on a child process. Held by a registry implementation (e.g.
    /// the discovery system guest) that must gate a request against the
    /// caller's grants.
    ProcessProbe,
    /// Permission to author the advisory domain-to-tenant mapping via the
    /// `SetDomain` discovery request. Bootstrap-provisioned only: like
    /// `DelegateGrants`, `MintCertificate` and `QuotaWrite`, it can never be
    /// conferred on a child process. Held solely by the identity system guest,
    /// which owns the mapping durably.
    DomainWrite,
}

impl Capability {
    /// Every capability, in declaration order.
    ///
    /// This is the single source for whole-vocabulary checks (coverage tests,
    /// bootstrap-provisioning audits, documentation). [`Self::name`] is
    /// exhaustive, so adding a variant fails to compile until it is named; the
    /// coverage test then fails until the variant is listed here too.
    pub const ALL: &'static [Capability] = &[
        Self::ProcessLifecycle,
        Self::SharedMemory,
        Self::Signal,
        Self::Network,
        Self::Storage,
        Self::SessionLifecycle,
        Self::ActivityRead,
        Self::MeteringRead,
        Self::GuestLogRead,
        Self::GuestLogWrite,
        Self::HostQueue,
        Self::DelegateGrants,
        Self::RouteClaim,
        Self::MintCertificate,
        Self::QuotaWrite,
        Self::ProcessProbe,
        Self::DomainWrite,
    ];

    /// The capability's stable identifier, used in diagnostics and error
    /// messages.
    pub const fn name(&self) -> &'static str {
        match self {
            Self::ProcessLifecycle => "ProcessLifecycle",
            Self::SharedMemory => "SharedMemory",
            Self::Signal => "Signal",
            Self::Network => "Network",
            Self::Storage => "Storage",
            Self::SessionLifecycle => "SessionLifecycle",
            Self::ActivityRead => "ActivityRead",
            Self::MeteringRead => "MeteringRead",
            Self::GuestLogRead => "GuestLogRead",
            Self::GuestLogWrite => "GuestLogWrite",
            Self::HostQueue => "HostQueue",
            Self::DelegateGrants => "DelegateGrants",
            Self::RouteClaim => "RouteClaim",
            Self::MintCertificate => "MintCertificate",
            Self::QuotaWrite => "QuotaWrite",
            Self::ProcessProbe => "ProcessProbe",
            Self::DomainWrite => "DomainWrite",
        }
    }

    /// True when only the host's bootstrap may provision this capability: a
    /// spawn that confers it on a child process is denied, even when the
    /// parent holds it.
    pub const fn is_bootstrap_provisioned(&self) -> bool {
        matches!(
            self,
            Self::DelegateGrants
                | Self::RouteClaim
                | Self::MintCertificate
                | Self::QuotaWrite
                | Self::ProcessProbe
                | Self::DomainWrite
        )
    }
}

/// Identity of a resource in either local-handle or shared-resource space.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Archive, Serialize, Deserialize,
)]
#[rkyv(bytecheck())]
pub enum ResourceIdentity {
    /// A local resource handle.
    Local(LocalResourceId),
    /// A shared resource identity.
    Shared(SharedResourceId),
}

/// Locality against which capability selectors can be matched.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum LocalityScope {
    /// Any locality is accepted.
    Any,
    /// Any process within the cluster is accepted.
    Cluster,
    /// A specific host is accepted.
    Host(String),
}

/// Class of resource protected by capability checks.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum ResourceClass {
    /// Shared memory region.
    SharedRegion,
    /// Local mapping of a shared memory region.
    SharedMapping,
    /// Signal resource.
    Signal,
    /// TCP listener resource.
    TcpListener,
    /// TCP stream resource.
    TcpStream,
    /// UDP socket resource.
    UdpSocket,
    /// Durable volume resource: an opaque byte container.
    Volume,
    /// Process resource.
    Process,
    /// Activity log resource.
    ActivityLog,
    /// Metering stream resource.
    MeteringStream,
    /// Guest log resource.
    GuestLog,
    /// Host-mediated connection queue resource.
    HostQueue,
    /// Per-tenant CPU instruction ceiling (a quota dimension, not an
    /// allocatable resource): the accountant authors a tenant's per-minute
    /// executed-instruction ceiling under this class and the runtime translates
    /// it into per-process engine execution budgets.
    Cpu,
}

/// Context used to evaluate a capability grant.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ScopeContext {
    /// Optional tenant name associated with the operation.
    pub tenant: Option<String>,
    /// Optional resource URI associated with the operation.
    pub uri: Option<String>,
    /// Locality where the operation is performed.
    pub locality: LocalityScope,
    /// Optional class of resource being accessed.
    pub resource_class: Option<ResourceClass>,
    /// Optional concrete resource identity being accessed.
    pub resource_id: Option<ResourceIdentity>,
}

/// A requestor namespace: either the platform root (no tenant) or a tenant.
///
/// This is the requestor-tenant vocabulary shared by capability selectors
/// (`ResourceSelector::Namespace`), delegation scopes, and the control-plane /
/// bridge-server requestor-derivation path. `Root` is the platform namespace
/// (`None` process tenant); `Tenant(t)` is a named tenant.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum Namespace {
    /// The platform root namespace: a scope context with no tenant.
    Root,
    /// A named tenant namespace.
    Tenant(String),
}

/// Selector that narrows where a capability grant applies.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum ResourceSelector {
    /// Match a tenant name exactly.
    Tenant(String),
    /// Match a requestor namespace hierarchically: `Namespace::Root` admits a
    /// root scope context, `Namespace::Tenant(t)` admits tenant `t` exactly.
    /// Unlike `Tenant`, this variant also carries the `Root` value, which is
    /// required for `DelegateGrants` to express root-wide delegation.
    Namespace(Namespace),
    /// Match resources whose URI starts with the prefix. For `sel://` URIs
    /// matching is path-segment aware; for `tcp://`/`udp://` endpoints it is
    /// component-aware; other URIs keep plain string-prefix semantics.
    UriPrefix(String),
    /// Match one exact canonical registry URI. Both sides are normalised to
    /// the canonical internal `sel://…` form before comparison, and the
    /// selector is never treated as a prefix.
    Uri(String),
    /// Match an operation locality.
    Locality(LocalityScope),
    /// Match a resource class.
    ResourceClass(ResourceClass),
    /// Match a concrete resource identity.
    ExplicitResource(ResourceIdentity),
    /// Match processes that are descendants of the grantee (children,
    /// grandchildren, etc.). Used by metering/activity/guest-log read
    /// grants so supervisors can read their descendants' telemetry.
    Children,
}

/// Scope an owner-declared attachable-by-resolution declaration admits.
///
/// The owner of a shared resource authors the declaration and the host
/// enforces it: a resolve-recording naming a client process is accepted as
/// that process's attach basis only when the declaration's scope admits the
/// process. A narrower scope keeps the owner's authority from being broadened
/// by a resolver.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum AttachDeclarationScope {
    /// Any client process is admitted by the declaration.
    Any,
    /// Only client processes whose tenant matches the named tenant are
    /// admitted by the declaration.
    Tenant(String),
}

/// Grant allowing one capability within the intersection of its selectors.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct CapabilityGrant {
    /// Capability being granted.
    pub capability: Capability,
    /// Selectors that must all match for the grant to apply.
    pub selectors: Vec<ResourceSelector>,
}

/// Authoritative ownership answer for `(class, id)`.
///
/// `owned` says whether the queried process owns the resource under the class
/// it asked about; when it does, `tenant` is the tenant the host minted the
/// resource under (an empty string for the root/platform tenant). The answer is
/// read from the host's mint-time records, never from a guest-projected store.
///
/// `other_classes` is populated only when `owned` is false: it lists the
/// classes under which the queried process *does* own a resource with the same
/// id. It lets an admission decision tell a class mismatch (a permission
/// failure) apart from an identity the caller does not own at all.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ResourceAuthority {
    /// Whether the queried process owns the resource under the queried class.
    pub owned: bool,
    /// Tenant the resource was minted under, when owned.
    pub tenant: Option<String>,
    /// Classes the process owns for the same id when it does not own the
    /// queried class. Empty when `owned` is true or nothing with that id is
    /// owned.
    pub other_classes: Vec<ResourceClass>,
}

impl LocalityScope {
    /// Returns whether this scope admits the actual locality.
    pub fn matches(&self, actual: &LocalityScope) -> bool {
        match self {
            Self::Any => true,
            Self::Cluster => matches!(actual, LocalityScope::Cluster | LocalityScope::Host(_)),
            Self::Host(expected) => {
                matches!(actual, LocalityScope::Host(actual) if actual == expected)
            }
        }
    }
}

impl ResourceClass {
    /// Returns the lowercased typed URI segment for this class, drawn from the
    /// closed segment vocabulary (`proc`, `region`, `queue`, …). This is the
    /// single mapping between a resource class and the `<type>` segment of a
    /// `sel://<tenant>/<type>/<id>` URI.
    pub fn uri_segment(&self) -> &'static str {
        match self {
            Self::SharedRegion => "region",
            Self::SharedMapping => "mapping",
            Self::Signal => "signal",
            Self::TcpListener => "listener",
            Self::TcpStream => "stream",
            Self::UdpSocket => "socket",
            Self::Volume => "volume",
            Self::Process => "proc",
            Self::ActivityLog => "activity",
            Self::MeteringStream => "metering",
            Self::GuestLog => "guest-log",
            Self::HostQueue => "queue",
            Self::Cpu => "cpu",
        }
    }

    /// Returns the class named by a typed URI segment, if the segment is part
    /// of the closed vocabulary. Inverse of [`Self::uri_segment`], used to
    /// reserve class nouns so a leaf alias cannot shadow a type segment.
    pub fn from_uri_segment(segment: &str) -> Option<Self> {
        match segment {
            "region" => Some(Self::SharedRegion),
            "mapping" => Some(Self::SharedMapping),
            "signal" => Some(Self::Signal),
            "listener" => Some(Self::TcpListener),
            "stream" => Some(Self::TcpStream),
            "socket" => Some(Self::UdpSocket),
            "volume" => Some(Self::Volume),
            "proc" => Some(Self::Process),
            "activity" => Some(Self::ActivityLog),
            "metering" => Some(Self::MeteringStream),
            "guest-log" => Some(Self::GuestLog),
            "queue" => Some(Self::HostQueue),
            "cpu" => Some(Self::Cpu),
            _ => None,
        }
    }
}

impl Default for ScopeContext {
    fn default() -> Self {
        Self {
            tenant: None,
            uri: None,
            locality: LocalityScope::Any,
            resource_class: None,
            resource_id: None,
        }
    }
}

impl Namespace {
    /// Returns whether this namespace admits a scope context whose tenant is
    /// `tenant`: `Root` admits `None` only; `Tenant(t)` admits `Some(t)` exactly.
    pub fn matches_tenant(&self, tenant: Option<&str>) -> bool {
        match self {
            Self::Root => tenant.is_none(),
            Self::Tenant(expected) => tenant == Some(expected.as_str()),
        }
    }
}

impl ResourceSelector {
    /// Returns whether the runtime can evaluate this selector against a `ScopeContext`.
    ///
    /// The enforcement matrix is:
    /// - `Tenant`, `Namespace`, `Locality`, `ResourceClass`, `ExplicitResource`,
    ///   `Uri`, `Children`: evaluatable.
    /// - `UriPrefix`: evaluatable only when the same grant also carries a
    ///   resource-class selector the prefix applies to: a network class
    ///   (`TcpListener`, `TcpStream`, or `UdpSocket`) for endpoint URIs, or
    ///   `Volume` for volume names (and `sel://` registry URIs).
    pub fn is_evaluatable(&self, grant_selectors: &[ResourceSelector]) -> bool {
        match self {
            Self::UriPrefix(_) => grant_selectors.iter().any(|s| {
                matches!(
                    s,
                    Self::ResourceClass(ResourceClass::TcpListener)
                        | Self::ResourceClass(ResourceClass::TcpStream)
                        | Self::ResourceClass(ResourceClass::UdpSocket)
                        | Self::ResourceClass(ResourceClass::Volume)
                )
            }),
            Self::Tenant(_)
            | Self::Namespace(_)
            | Self::Locality(_)
            | Self::ResourceClass(_)
            | Self::ExplicitResource(_)
            | Self::Uri(_)
            | Self::Children => true,
        }
    }

    /// Returns whether this selector matches the supplied scope context.
    ///
    /// `Children` is not evaluated here — it requires process-tree knowledge
    /// only available to the runtime, so it returns `false` by default and the
    /// runtime handles it specially in `Runtime::authorises`.
    ///
    /// For `UriPrefix` on `tcp://` / `udp://` URIs, matching is component-aware
    /// (scheme exact; host exact or `*.`-label-boundary wildcard; port exact,
    /// list, or `*`). For `sel://` registry URIs, matching is path-segment
    /// aware. Other non-network URIs keep plain `starts_with` semantics.
    ///
    /// `Uri` matches one exact canonical registry URI after normalising both
    /// sides; it never acts as a prefix.
    pub fn matches(&self, context: &ScopeContext) -> bool {
        match self {
            Self::Tenant(expected) => context.tenant.as_ref() == Some(expected),
            Self::Namespace(namespace) => namespace.matches_tenant(context.tenant.as_deref()),
            Self::Uri(expected) => context
                .uri
                .as_ref()
                .is_some_and(|context_uri| canonical_uri(expected) == canonical_uri(context_uri)),
            Self::UriPrefix(prefix) => {
                let context_uri = context.uri.as_ref();
                match context_uri {
                    Some(ctx_uri) => {
                        // Component-aware matching for network URIs.
                        if let (Some(grant_ep), Some(ctx_ep)) = (
                            NetworkEndpoint::parse(prefix),
                            NetworkEndpoint::parse(ctx_uri),
                        ) {
                            NetworkEndpoint::prefix_matches(&grant_ep, &ctx_ep)
                        } else if uri::parse_sel(prefix).is_some()
                            || uri::parse_sel(ctx_uri).is_some()
                        {
                            sel_prefix_matches(prefix, ctx_uri)
                        } else {
                            // Plain string prefix for other non-network URIs.
                            ctx_uri.starts_with(prefix)
                        }
                    }
                    None => false,
                }
            }
            Self::Locality(expected) => expected.matches(&context.locality),
            Self::ResourceClass(expected) => context.resource_class.as_ref() == Some(expected),
            Self::ExplicitResource(expected) => context.resource_id == Some(*expected),
            // Handled by the runtime with process-tree access.
            Self::Children => false,
        }
    }
}

impl CapabilityGrant {
    /// Creates a capability grant with the supplied selectors.
    pub fn new(capability: Capability, selectors: Vec<ResourceSelector>) -> Self {
        Self {
            capability,
            selectors,
        }
    }

    /// Returns whether all selectors admit the supplied context.
    pub fn allows(&self, context: &ScopeContext) -> bool {
        self.selectors
            .iter()
            .all(|selector| selector.matches(context))
    }
}

/// Normalises a registry URI to its canonical internal `sel://` spelling:
/// trailing separators removed, and wire names projected to their internal
/// path. Non-`sel` values (network endpoints, volume names) are returned
/// unchanged.
fn canonical_uri(raw: &str) -> String {
    if let Some((tenant, path)) = uri::parse_sel(raw) {
        let path = path.trim_matches('/');
        if path.is_empty() {
            return format!("{}{tenant}/", uri::SEL_PREFIX);
        }
        return format!("{}{tenant}/{path}", uri::SEL_PREFIX);
    }
    if raw.contains("://") {
        return raw.to_string();
    }
    match uri::resolve_wire_name(raw, &uri::DomainTable::new()) {
        Some((tenant, path)) if path.is_empty() => format!("{}{tenant}/", uri::SEL_PREFIX),
        Some((tenant, path)) => format!("{}{tenant}/{}", uri::SEL_PREFIX, path.join("/")),
        None => raw.to_string(),
    }
}

/// Path-segment-aware prefix matching for `sel://` registry URIs: a prefix
/// matches at a segment boundary, so `sel:///control` does not match
/// `sel:///control-plane`.
fn sel_prefix_matches(prefix: &str, context: &str) -> bool {
    let prefix = canonical_uri(prefix);
    let context = canonical_uri(context);
    if context == prefix {
        return true;
    }
    match context.strip_prefix(&prefix) {
        Some(rest) => prefix.ends_with('/') || rest.starts_with('/'),
        None => false,
    }
}
