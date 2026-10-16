//! Discovery system guest.
//!
//! The store is a fed index over one deterministic URI taxonomy:
//! `sel://<tenant>/<type>/<id>` (typed), `sel://<tenant>/<path…>` (named
//! service routes), and leaf aliases. External wire names are projections of
//! this space: `resolve_wire_name` strips the tenant domain (longest match
//! against the advisory domain table, defaulting to the synthetic tenant
//! label) and reverses the remaining labels into a path. Everything is
//! tenant-scoped; the empty tenant is the reserved root namespace.
//!
//! Owner-registered routes carry a lease (see [`LEASE_TTL_MS`]): an owner
//! renews it through `DiscoveryRequest::Renew`, and the lease sweep revokes a
//! lapsed registration as if its owner had revoked it. Host-authored (Tier-1)
//! registrations are not leased; the runtime revokes them when their owning
//! process exits.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::{Arc, Mutex},
};

use anyhow::Context as _;
use selium_abi::{Capability, ProcessId, ResourceClass, ResourceKind, uri};
use selium_guest::{AttachDeclarationScope, InterfaceMetadata, entrypoint, pattern_interface};
use selium_service::{DiscoveryRequest, DiscoveryResponse, DomainEntry, ResourceTarget};
use selium_shm::{Channel, ChannelBackpressure, transport::ShmTransport};
use selium_wire::{
    LiveTable,
    framed::{FramedRead, FramedWrite},
    pubsub::{Publisher, Subscriber},
};

/// The route-change live table: key = registered URI, value = target.
pub type RouteTable = LiveTable<String, ResourceTarget, ShmTransport>;

pub const DISCOVERY_EXCHANGE: &str = "selium.discovery.resolve";
pub const INTERFACE_METADATA_TABLE: &str = "selium.discovery.interfaces";
/// Lease lifetime: a guest registration lapses if its owner does not renew it
/// within this window. Shared with the SDK's renewal task, which renews at half
/// this interval.
pub const LEASE_TTL_MS: u64 = selium_service::REGISTRATION_LEASE_TTL_MS;
pub const REGISTRATION_LOG: &str = "selium.discovery.registrations";
/// The root-namespace route the discovery route-change table is served on.
pub const ROUTE_TABLE_ROUTE: &str = "sel:///discovery-routes";
pub const URI_LIVE_TABLE: &str = "selium.discovery.uri-table";

#[pattern_interface]
pub trait DiscoveryControl {
    fn register(target: ResourceTarget);
    fn remove(uri: String);
    fn resolve_exact(uri: String);
    fn resolve_prefix(prefix: String);
}

/// A guest registration's lease: the owning process and the wall-clock
/// deadline it must renew before.
#[derive(Debug, Clone, Copy)]
pub struct Lease {
    /// Process that owns the registration.
    pub owner: ProcessId,
    /// Wall-clock deadline in milliseconds since the Unix epoch.
    pub expires_at_ms: u64,
}

#[derive(Debug, Clone, Default)]
pub struct DiscoveryStore {
    /// Exact-key registrations: typed URIs, root well-known URIs, and named
    /// service routes (`sel://<tenant>/<path…>`).
    registrations: BTreeMap<String, ResourceTarget>,
    /// Ownership table: maps `(process_id, resource_id)` pairs to the
    /// resource's class, populated by Tier-1 (runtime) registrations. Used
    /// to validate Tier-2 (guest) registrations and revocations without
    /// parsing the URI — the recorded class pins which *kind* of resource
    /// the owner may alias or name externally.
    ownership: HashMap<(u64, u64), selium_abi::ResourceClass>,
    /// Lease per owner-registered route: liveness is observable registration.
    /// Host-authored (Tier-1) registrations are not leased; they are revoked
    /// on process exit by the runtime.
    leases: HashMap<String, Lease>,
    /// Leaf aliases: alias URI → canonical typed URI.
    aliases: HashMap<String, String>,
    /// Reverse alias index: canonical typed URI → alias URIs, for reverse
    /// revocation when a target is revoked.
    alias_backrefs: HashMap<String, Vec<String>>,
    /// Label index: `(key, value)` → canonical typed URIs.
    label_index: HashMap<(String, String), BTreeSet<String>>,
    /// Advisory domain→tenant table, provisioned out-of-band (Tier-1 seed).
    /// Routes and scopes names; never authenticates.
    domains: uri::DomainTable,
    /// Owning process per registered route key (typed URIs, named-service
    /// routes, aliases, and translated wire-name registrations). Used for
    /// owner-keyed revocation on process exit.
    route_owners: HashMap<String, ProcessId>,
    /// Root-service designation per tenant: the bare domain (apex) resolves
    /// to this internal URI.
    root_services: HashMap<String, String>,
}

impl DiscoveryStore {
    /// Seeds the advisory domain→tenant table (out-of-band provisioning).
    pub fn seed_domain(&mut self, domain: &str, tenant: &str) {
        self.domains.seed(domain, tenant);
    }

    /// Returns whether the domain table maps `domain`.
    pub fn domain_tenant(&self, domain: &str) -> Option<&str> {
        self.domains.get(domain)
    }

    /// Records or refreshes a guest registration's lease.
    pub fn set_lease(&mut self, uri: &str, owner: ProcessId, now_ms: u64) {
        self.leases.insert(
            uri.to_string(),
            Lease {
                owner,
                expires_at_ms: now_ms.saturating_add(LEASE_TTL_MS),
            },
        );
    }

    /// Renews a guest registration's lease if `owner` holds it. Returns
    /// whether the lease was renewed.
    pub fn renew_lease(&mut self, uri: &str, owner: ProcessId, now_ms: u64) -> bool {
        match self.leases.get_mut(uri) {
            Some(lease) if lease.owner == owner => {
                lease.expires_at_ms = now_ms.saturating_add(LEASE_TTL_MS);
                true
            }
            _ => false,
        }
    }

    /// Expires every lapsed lease, revoking the registration as if its owner
    /// had revoked it. Watchers observe the resulting revocation.
    pub fn expire_leases(&mut self, now_ms: u64) {
        let expired: Vec<String> = self
            .leases
            .iter()
            .filter(|(_, lease)| lease.expires_at_ms <= now_ms)
            .map(|(uri, _)| uri.clone())
            .collect();
        for uri in expired {
            if self.aliases.contains_key(&uri) {
                self.revoke_alias(&uri);
            } else {
                self.revoke_key(&uri);
            }
        }
    }

    /// Registers a host-authored protocol handler (Tier-1).
    ///
    /// Handler registration is host-authored so a serve-side pin cannot be
    /// forged by a guest; resolution is an ordinary discovery lookup.
    pub fn set_handler(&mut self, scheme: &str, process_id: ProcessId) {
        let uri = handler_uri(scheme);
        let target = ResourceTarget {
            uri: uri.clone(),
            host_id: String::new(),
            resource_id: process_id,
            interface: None,
            tenant: None,
            class: selium_abi::ResourceClass::Process,
            labels: Vec::new(),
        };
        self.registrations.insert(uri.clone(), target);
        self.route_owners.insert(uri.clone(), process_id);
    }

    /// Sets or clears a guest's health record for `name` under `tenant`.
    ///
    /// Health is published state, not a resource route: it is not lease-expired
    /// (a lapsing lease would mark every healthy long-running guest unhealthy
    /// until its owner renewed it again) and is not subject to the typed-resource
    /// ownership check. Liveness is carried by the route leases, and the record
    /// itself is revoked when its owner's process exits.
    pub fn set_health(
        &mut self,
        tenant: Option<&str>,
        caller: ProcessId,
        name: &str,
        healthy: bool,
    ) {
        let uri = health_uri(tenant, name);
        if healthy {
            let target = ResourceTarget {
                uri: uri.clone(),
                host_id: String::new(),
                resource_id: caller,
                interface: None,
                tenant: tenant.map(str::to_string),
                class: selium_abi::ResourceClass::Process,
                labels: Vec::new(),
            };
            self.registrations.insert(uri.clone(), target);
            self.route_owners.insert(uri.clone(), caller);
        } else {
            self.revoke_key(&uri);
        }
    }

    /// Stores a Tier-1 registration under its exact key, populates the
    /// ownership table from `owner`, and maintains the label index for typed
    /// targets.
    fn store(&mut self, target: ResourceTarget, owner: Option<ProcessId>) {
        if let Some(process_id) = owner {
            self.ownership
                .insert((process_id, target.resource_id), target.class.clone());
            self.route_owners.insert(target.uri.clone(), process_id);
        }
        if uri::parse_typed(&target.uri).is_some() {
            for label in &target.labels {
                self.label_index
                    .entry((label.key.clone(), label.value.clone()))
                    .or_default()
                    .insert(target.uri.clone());
            }
        }
        self.registrations.insert(target.uri.clone(), target);
    }

    /// Removes a registration by exact key, revoking any aliases that resolve
    /// to it and cleaning its label index entries.
    fn revoke_key(&mut self, key: &str) {
        let removed = self.registrations.remove(key);
        self.leases.remove(key);
        // Reverse alias revocation: revoking a target revokes its aliases
        // (and, for single-segment routes registered under those alias keys,
        // their exact-key entries too).
        if let Some(aliases) = self.alias_backrefs.remove(key) {
            for alias in aliases {
                self.revoke_alias(&alias);
            }
        }
        // Drop label index entries referencing the removed target.
        let mut empty_labels = Vec::new();
        for ((label_key, label_value), uris) in self.label_index.iter_mut() {
            uris.remove(key);
            if uris.is_empty() {
                empty_labels.push((label_key.clone(), label_value.clone()));
            }
        }
        for label in empty_labels {
            self.label_index.remove(&label);
        }
        // Clean up the root-service designation if it pointed here.
        self.root_services.retain(|_, uri| uri != key);
        // Clean up ownership entries for resources no longer referenced.
        if let Some(target) = removed {
            let still_referenced = self
                .registrations
                .values()
                .any(|t| t.resource_id == target.resource_id);
            if !still_referenced {
                self.ownership
                    .retain(|(_, rid), _| *rid != target.resource_id);
            }
        }
        self.route_owners.remove(key);
    }

    /// Removes an alias registration (guest/Tier-2 alias, or a Tier-1
    /// revoke-by-alias-key). Returns the alias's canonical key, if any.
    ///
    /// A single-segment named-service route is registered both as an exact
    /// key (the caller's serving target) and as an alias (the cascade
    /// backref), so revoking it must remove both.
    fn revoke_alias(&mut self, alias: &str) {
        if let Some(canonical) = self.aliases.remove(alias)
            && let Some(backrefs) = self.alias_backrefs.get_mut(&canonical)
        {
            backrefs.retain(|a| a != alias);
            if backrefs.is_empty() {
                self.alias_backrefs.remove(&canonical);
            }
        }
        self.registrations.remove(alias);
        self.leases.remove(alias);
        self.route_owners.remove(alias);
        self.root_services.retain(|_, uri| uri != alias);
    }

    /// Revokes every route owned by a process, keyed by the owner recorded at
    /// registration time. This is owner-keyed revocation living in discovery:
    /// there is no runtime side map of guest-registered routes.
    pub fn revoke_by_owner(&mut self, process_id: ProcessId) {
        let owned_keys: Vec<String> = self
            .route_owners
            .iter()
            .filter(|(_, owner)| **owner == process_id)
            .map(|(uri, _)| uri.clone())
            .collect();
        for key in owned_keys {
            if self.aliases.contains_key(&key) {
                self.revoke_alias(&key);
            } else {
                self.revoke_key(&key);
            }
        }
    }

    /// Revokes every registration whose target is the named resource identity
    /// (class + id). The registry recorded the resource's tenant at
    /// registration, so a resource minted under a serving tenant is revoked
    /// under the tenant it was minted for — the runtime needs no tenant side
    /// map.
    pub fn revoke_resource(&mut self, class: &str, id: u64) {
        let keys: Vec<String> = self
            .registrations
            .iter()
            .filter(|(_, target)| target.resource_id == id && target.class.uri_segment() == class)
            .map(|(uri, _)| uri.clone())
            .collect();
        for key in keys {
            if self.aliases.contains_key(&key) {
                self.revoke_alias(&key);
            } else {
                self.revoke_key(&key);
            }
        }
    }

    /// Applies a volatile Tier-1 event from the runtime feed.
    ///
    /// Tier-1 authority comes from the *transport* (the runtime feed), never
    /// from the URI string: everything arriving here is stored verbatim.
    fn apply_tier1_event(&mut self, request: DiscoveryRequest) {
        match request {
            DiscoveryRequest::Register { target, owner, .. } => {
                self.store(target, owner);
            }
            DiscoveryRequest::Revoke { uri } => {
                self.revoke_key(&uri);
            }
            DiscoveryRequest::RevokeByOwner { process_id } => {
                self.revoke_by_owner(process_id);
            }
            DiscoveryRequest::SeedDomain { domain, tenant } => {
                self.seed_domain(&domain, &tenant);
            }
            DiscoveryRequest::RevokeResource { class, id } => {
                self.revoke_resource(&class, id);
            }
            DiscoveryRequest::RegisterHandler { scheme, process_id } => {
                self.set_handler(&scheme, process_id);
            }
            // Query variants never arrive over the feed.
            DiscoveryRequest::Resolve(_)
            | DiscoveryRequest::ResolvePrefix(_)
            | DiscoveryRequest::ResolveLabels { .. }
            | DiscoveryRequest::Renew { .. }
            | DiscoveryRequest::SetHealth { .. }
            | DiscoveryRequest::SetDomain { .. }
            | DiscoveryRequest::ListDomains => {}
        }
    }

    /// Resolves an exact URI (typed, named-service route, leaf alias, or
    /// external wire name) with optional tenant scoping.
    ///
    /// An external wire name is translated through the unified resolver: the
    /// tenant domain is stripped (longest domain-table match, falling back to
    /// the synthetic tenant label), the remaining labels are reversed into a
    /// path, and that internal path is resolved. The bare domain (apex)
    /// resolves through the tenant's designated root service.
    pub fn resolve_exact(&self, uri: &str, caller_tenant: Option<&str>) -> Option<ResourceTarget> {
        match self.resolve_internal(uri, caller_tenant) {
            Some(target) => Some(target),
            None => {
                // External wire name: translate to an internal path.
                if uri::parse_sel(uri).is_some() {
                    return None;
                }
                let (tenant, path) = uri::resolve_wire_name(uri, &self.domains)?;
                if !tenant_admits(caller_tenant, Some(&tenant)) {
                    return None;
                }
                if path.is_empty() {
                    let root_service = self.root_services.get(&tenant)?;
                    return self.resolve_internal(root_service, caller_tenant);
                }
                let internal = format!("{}{}/{}", uri::SEL_PREFIX, tenant, path.join("/"));
                self.resolve_internal(&internal, caller_tenant)
            }
        }
    }

    /// Resolves an internal `sel://` URI: exact registration first, then leaf
    /// aliases, with tenant scoping.
    fn resolve_internal(&self, uri: &str, caller_tenant: Option<&str>) -> Option<ResourceTarget> {
        if let Some(target) = self.registrations.get(uri) {
            return tenant_admits(caller_tenant, target.tenant.as_deref()).then(|| target.clone());
        }
        if let Some(canonical) = self.aliases.get(uri)
            && let Some(target) = self.registrations.get(canonical)
        {
            return tenant_admits(caller_tenant, target.tenant.as_deref()).then(|| target.clone());
        }
        None
    }

    /// Resolves a prefix/`*` enumeration query (`sel://<tenant>/<type>/*` or
    /// `sel://<tenant>/*`) with tenant scoping.
    pub fn resolve_prefix(&self, prefix: &str, caller_tenant: Option<&str>) -> Vec<ResourceTarget> {
        let Some((tenant, path)) = uri::parse_sel(prefix) else {
            return Vec::new();
        };
        let Some(base) = uri::wildcard_prefix(path) else {
            return Vec::new();
        };
        let mut results = self
            .registrations
            .iter()
            .filter_map(|(key, target)| {
                let (target_tenant, target_path) = uri::parse_sel(key)?;
                if target_tenant != tenant {
                    return None;
                }
                if !tenant_admits(caller_tenant, target.tenant.as_deref()) {
                    return None;
                }
                if base.is_empty() {
                    // Enumerate every typed target in the tenant.
                    uri::parse_typed(key)?;
                } else {
                    let first_segment = target_path.split('/').next().unwrap_or("");
                    if first_segment != base {
                        return None;
                    }
                }
                Some(target.clone())
            })
            .collect::<Vec<_>>();
        // Deterministic order from the BTreeMap iteration.
        results.sort_by(|a, b| a.uri.cmp(&b.uri));
        results
    }

    /// Answers a label query: every typed target in the caller's tenant whose
    /// labels match `(key, value)`.
    pub fn resolve_labels(
        &self,
        key: &str,
        value: &str,
        caller_tenant: Option<&str>,
    ) -> Vec<ResourceTarget> {
        let mut results = Vec::new();
        if let Some(uris) = self.label_index.get(&(key.to_string(), value.to_string())) {
            for uri in uris {
                if let Some(target) = self.registrations.get(uri)
                    && tenant_admits(caller_tenant, target.tenant.as_deref())
                {
                    results.push(target.clone());
                }
            }
        }
        results
    }

    /// Translates and validates a registration's URI into its internal
    /// `sel://` form: wire-name projection, advisory domain scoping, root
    /// capability gating, tenant admission, typed-segment rejection, and
    /// ownership of the claimed target. Returns the internal URI to register
    /// under, or `None` when the registration is not admitted.
    pub fn registration_uri(
        &self,
        raw: &str,
        caller: ProcessId,
        caller_tenant: Option<&str>,
        root_allowed: bool,
        resource_id: u64,
        class: &selium_abi::ResourceClass,
    ) -> Option<String> {
        let uri_str = match uri::parse_sel(raw) {
            Some(_) => raw.to_string(),
            None => {
                let (domain_tenant, path) = uri::resolve_wire_name(raw, &self.domains)?;
                if domain_tenant.is_empty() && !root_allowed {
                    return None;
                }
                if !path.is_empty() && !tenant_admits(caller_tenant, Some(&domain_tenant)) {
                    // Foreign domain: the mapping does not own this tenant.
                    return None;
                }
                if path.is_empty() {
                    // A bare domain designates no service of its own.
                    return None;
                }
                format!("{}{}/{}", uri::SEL_PREFIX, domain_tenant, path.join("/"))
            }
        };
        let (tenant, path) = uri::parse_sel(&uri_str)?;
        if tenant.is_empty() && !root_allowed {
            return None;
        }
        if !tenant_admits(caller_tenant, Some(tenant)) {
            return None;
        }
        let first = path.split('/').next()?;
        if first.is_empty() || uri::is_class_segment(first) {
            // Typed URIs are minted by the runtime; guests may not register
            // them directly.
            return None;
        }
        if first == "handler" {
            // The handler namespace is host-authored (Tier-1): a guest may not
            // register a handler route, so the serve-side pin stays unforgeable.
            return None;
        }
        if first == "health" {
            // The health namespace is reserved: health records are published
            // through `SetHealth` under their owner's own name, so a guest
            // cannot shadow or forge another guest's health record by
            // registering a same-named route.
            return None;
        }
        if self.ownership.get(&(caller, resource_id)) != Some(class) {
            return None;
        }
        Some(uri_str)
    }

    /// The internal key a raw registration form resolves to, without admission
    /// checks.
    ///
    /// Renewal keys off this so a lease taken out under a projected internal
    /// URI is renewed by the same raw form the owner registered with; the
    /// owner check in [`renew_lease`](Self::renew_lease) remains the gate.
    fn registration_key(&self, raw: &str) -> Option<String> {
        match uri::parse_sel(raw) {
            Some(_) => Some(raw.to_string()),
            None => {
                let (domain_tenant, path) = uri::resolve_wire_name(raw, &self.domains)?;
                if path.is_empty() {
                    return None;
                }
                Some(format!(
                    "{}{}/{}",
                    uri::SEL_PREFIX,
                    domain_tenant,
                    path.join("/")
                ))
            }
        }
    }

    /// Renews a lease named by any registration form (an internal `sel://` URI
    /// or a wire name), keyed exactly as [`registration_uri`](Self::registration_uri)
    /// keyed the original registration.
    pub fn renew_lease_raw(&mut self, raw: &str, owner: ProcessId, now_ms: u64) -> bool {
        match self.registration_key(raw) {
            Some(key) => self.renew_lease(&key, owner, now_ms),
            None => false,
        }
    }

    /// Applies a guest (Tier-2) registration with the full validation chain:
    /// wire-name translation and domain scoping, root capability gating,
    /// leaf-alias/typed rules, then ownership (including the claimed resource
    /// class) and target existence for aliases.
    pub fn apply_register(
        &mut self,
        caller: ProcessId,
        caller_tenant: Option<&str>,
        target: ResourceTarget,
        root_allowed: bool,
        root_service: bool,
    ) -> DiscoveryResponse {
        let Some(uri_str) = self.registration_uri(
            &target.uri,
            caller,
            caller_tenant,
            root_allowed,
            target.resource_id,
            &target.class,
        ) else {
            return DiscoveryResponse::Forbidden;
        };
        let (tenant, path) = uri::parse_sel(&uri_str).expect("registration_uri is a sel uri");

        // A named-service route (single- or multi-segment): an exact-key
        // registration storing the caller's full target, so resolution
        // returns the serving target with its interface metadata intact.
        if !path.contains('/') {
            // A single-segment route shares the leaf-alias namespace and must
            // point at an owned typed target that currently exists; the alias
            // backref keeps the cascade, so revoking the typed target revokes
            // the route too.
            let canonical = uri::resource_uri(tenant, target.class.clone(), target.resource_id);
            if !self.registrations.contains_key(&canonical) {
                // The claimed target is not registered (e.g. it was already
                // revoked); a dangling route would resolve to nothing.
                return DiscoveryResponse::NotFound;
            }
            self.aliases.insert(uri_str.clone(), canonical.clone());
            let backrefs = self.alias_backrefs.entry(canonical).or_default();
            if !backrefs.contains(&uri_str) {
                backrefs.push(uri_str.clone());
            }
        }
        // Pin the stored target to the derived internal route URI, so the
        // registration is keyed identically however the caller spelled the
        // name (internal path or wire name projection).
        let mut target = target;
        target.uri = uri_str.clone();
        self.registrations.insert(uri_str.clone(), target);
        self.route_owners.insert(uri_str.clone(), caller);
        if root_service {
            self.root_services.insert(tenant.to_string(), uri_str);
        }
        DiscoveryResponse::Registered
    }

    /// Applies a guest (Tier-2) revocation. Guests may revoke only the
    /// registrations they own, within their own tenant; typed URIs are
    /// runtime-minted and revoked over the Tier-1 feed; unknown keys report
    /// `NotFound`.
    pub fn apply_revoke(
        &mut self,
        caller: ProcessId,
        caller_tenant: Option<&str>,
        uri: &str,
    ) -> DiscoveryResponse {
        // Translate an external wire name to the internal route it projects
        // to, so a guest can revoke by wire name.
        if !uri::parse_sel(uri).is_some() {
            return match uri::resolve_wire_name(uri, &self.domains) {
                Some((tenant, path)) if !path.is_empty() => {
                    let internal = format!("{}{}/{}", uri::SEL_PREFIX, tenant, path.join("/"));
                    self.apply_revoke(caller, caller_tenant, &internal)
                }
                _ => DiscoveryResponse::NotFound,
            };
        }

        // Guests may not revoke the root namespace.
        if uri::is_root_uri(uri) {
            return DiscoveryResponse::Forbidden;
        }
        if self.aliases.contains_key(uri) {
            // Leaf alias: tenant admission on the alias's own tenant, plus
            // ownership of the aliased resource.
            let Some((tenant, _name)) = uri::parse_alias(uri) else {
                return DiscoveryResponse::NotFound;
            };
            if !tenant_admits(caller_tenant, Some(tenant)) {
                return DiscoveryResponse::Forbidden;
            }
            let canonical = self.aliases.get(uri).cloned();
            let Some((_, class, id)) = canonical.as_deref().and_then(uri::parse_typed) else {
                return DiscoveryResponse::NotFound;
            };
            if self.ownership.get(&(caller, id)) != Some(&class) {
                return DiscoveryResponse::Forbidden;
            }
            self.revoke_alias(uri);
            DiscoveryResponse::Revoked
        } else if let Some((tenant, path)) = uri::parse_sel(uri) {
            if uri::is_class_segment(path.split('/').next().unwrap_or("")) {
                // Typed URIs are runtime-minted; only the Tier-1 feed revokes
                // them.
                return DiscoveryResponse::Forbidden;
            }
            // Named-service route: the caller must own the target resource.
            if !tenant_admits(caller_tenant, Some(tenant)) {
                return DiscoveryResponse::Forbidden;
            }
            let Some(target) = self.registrations.get(uri).cloned() else {
                return DiscoveryResponse::NotFound;
            };
            if self.ownership.get(&(caller, target.resource_id)) != Some(&target.class) {
                return DiscoveryResponse::Forbidden;
            }
            self.revoke_key(uri);
            DiscoveryResponse::Revoked
        } else {
            DiscoveryResponse::NotFound
        }
    }

    /// Returns the provisioned domain→tenant entries for a `ListDomains`
    /// request.
    pub fn domain_entries(&self) -> Vec<DomainEntry> {
        self.domains
            .entries()
            .map(|(domain, tenant)| DomainEntry {
                domain: domain.to_string(),
                tenant: tenant.to_string(),
            })
            .collect()
    }

    pub fn ingest_interface_metadata(&mut self, uri: &str, metadata: InterfaceMetadata) -> bool {
        let Some(target) = self.registrations.get_mut(uri) else {
            return false;
        };
        target.interface = Some(metadata);
        true
    }
}

pub fn interface_metadata() -> InterfaceMetadata {
    discoverycontrol_pattern_metadata()
}

fn attach_feed_subscriber(
    feed_region_id: u64,
) -> selium_guest::Result<Subscriber<DiscoveryRequest, ShmTransport>> {
    let channel = Channel::attach(feed_region_id)
        .map_err(|error| selium_guest::GuestError::Host(error.to_string()))?;
    let transport = ShmTransport::new(&channel, &channel)
        .map_err(|error| selium_guest::GuestError::Host(error.to_string()))?;
    let framed = FramedRead::new(transport);
    // Disable overwrite detection: the discovery feed is volatile and the guest
    // reads whatever is currently available, accepting that events may be lost.
    Ok(Subscriber::new(framed, None))
}

/// Builds the route-change live table over its own ring, returning the ring id
/// and the table.
fn create_route_table() -> selium_wire::Result<(u64, RouteTable)> {
    let channel = Channel::create_with_backpressure(
        256 * 1024,
        ChannelBackpressure::Drop,
        ResourceKind::LiveTable,
    )?;
    let region_id = channel.region_id();
    let write = ShmTransport::new(&channel, &channel)?;
    let read = ShmTransport::new(&channel, &channel)?;
    let publisher = Publisher::new(FramedWrite::new(write));
    let subscriber = Subscriber::new(FramedRead::new(read), None);
    let table = LiveTable::new(publisher, subscriber)?;
    Ok((region_id, table))
}

/// Response used when the caller's tenant scope could not be verified
/// (fail-closed): reads disclose nothing, writes are refused. A tenant
/// *absence* (verified `None`, i.e. a root/system principal) is legitimate
/// and scoped normally — only a failed lookup takes this path.
fn denied_response(request: &DiscoveryRequest) -> DiscoveryResponse {
    match request {
        DiscoveryRequest::Resolve(_) => DiscoveryResponse::NotFound,
        DiscoveryRequest::ResolvePrefix(_) | DiscoveryRequest::ResolveLabels { .. } => {
            DiscoveryResponse::Resolved(Vec::new())
        }
        DiscoveryRequest::ListDomains => DiscoveryResponse::Domains(Vec::new()),
        DiscoveryRequest::Register { .. }
        | DiscoveryRequest::Revoke { .. }
        | DiscoveryRequest::RevokeByOwner { .. }
        | DiscoveryRequest::SeedDomain { .. }
        | DiscoveryRequest::RevokeResource { .. }
        | DiscoveryRequest::Renew { .. }
        | DiscoveryRequest::SetHealth { .. }
        | DiscoveryRequest::RegisterHandler { .. }
        | DiscoveryRequest::SetDomain { .. } => DiscoveryResponse::Forbidden,
    }
}

#[entrypoint]
async fn discovery_main(feed_region_id: u64, listener_shared_id: u64) -> anyhow::Result<()> {
    drop(selium_guest::log::init());
    selium_guest::info!(guest = "selium-discovery", "system guest booting");

    let feed_subscriber = attach_feed_subscriber(feed_region_id)
        .with_context(|| "failed to attach discovery feed subscriber")?;

    let listener = selium_guest::ResourceListener::attach(listener_shared_id)
        .with_context(|| "failed to attach discovery listener")?;

    selium_guest::info!(
        feed_region_id,
        shared_id = listener.descriptor().shared_id,
        "discovery feed and listener attached"
    );

    let store = Arc::new(Mutex::new(DiscoveryStore::default()));

    // The route-change table: consumers watch it to converge on providers.
    let (route_region, route_table) =
        create_route_table().with_context(|| "failed to create discovery route table")?;
    let route_table = Arc::new(route_table);
    {
        let mut store = store
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        store.store(
            ResourceTarget {
                uri: ROUTE_TABLE_ROUTE.to_string(),
                host_id: String::new(),
                resource_id: route_region,
                interface: None,
                tenant: None,
                class: ResourceClass::SharedRegion,
                labels: Vec::new(),
            },
            Some(selium_guest::process_id()),
        );
    }
    // Declare the table's region attachable so consumers can resolve and
    // attach it.
    selium_guest::declare_attachable_by_resolution(
        ResourceClass::SharedRegion,
        route_region,
        AttachDeclarationScope::Any,
    )
    .with_context(|| "failed to declare the route table attachable")?;
    {
        let store = store
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Err(error) = sync_route_table(&store, &route_table) {
            selium_guest::warn!("discovery route table initial sync failed: {error}");
        }
    }

    // Spawn the feed processing loop.
    selium_guest::spawn(feed_loop(
        store.clone(),
        route_table.clone(),
        feed_subscriber,
    ));
    // Spawn the lease sweep loop.
    selium_guest::spawn(lease_sweep_loop(store.clone(), route_table.clone()));

    // Discovery publishes no health record of its own: it is the registry, its
    // entrypoint takes raw infrastructure handles rather than a `Context`, and
    // publishing would mean an RPC from this guest to its own listener — a
    // self-join the SDK does not model (it deadlocks the guest's reactor
    // against its own accept loop). Health is published state and an absent
    // record is valid: peers observe discovery by the routes it serves.

    // Accept incoming RPC connections forever.
    loop {
        let incoming = match listener.recv().await {
            Ok(connection) => connection,
            Err(error) => {
                selium_guest::warn!("discovery accept failed: {error}");
                continue;
            }
        };

        let connection =
            match selium_shm::rpc::accept::<DiscoveryRequest, DiscoveryResponse>(incoming.into()) {
                Ok(c) => c,
                Err(error) => {
                    selium_guest::warn!("discovery rpc accept failed: {error}");
                    continue;
                }
            };

        let store = store.clone();
        let route_table = route_table.clone();
        selium_guest::spawn(handler(store, route_table, connection));
    }
}

async fn feed_loop(
    store: Arc<Mutex<DiscoveryStore>>,
    route_table: Arc<RouteTable>,
    mut subscriber: Subscriber<DiscoveryRequest, ShmTransport>,
) {
    loop {
        match subscriber.read_with_tag() {
            Ok((request, _tag)) => {
                let mut store = store
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                store.apply_tier1_event(request);
                if let Err(error) = sync_route_table(&store, &route_table) {
                    selium_guest::warn!("discovery route table sync failed: {error}");
                }
            }
            Err(selium_wire::error::Error::BufferEmpty) => {
                selium_guest::yield_now().await;
            }
            Err(error) => {
                selium_guest::warn!("discovery feed read failed: {error}");
                break;
            }
        }
    }
}

async fn handler(
    store: Arc<Mutex<DiscoveryStore>>,
    route_table: Arc<RouteTable>,
    mut conn: selium_shm::rpc::RpcConnection<DiscoveryRequest, DiscoveryResponse>,
) {
    let client_process_id = conn.client_process_id();
    // The caller's tenant is read from the runtime's persisted process
    // authority, scoping every operation to the calling process's own
    // tenant. Fail-closed: if the lookup itself fails, requests are denied
    // rather than silently treated as unscoped.
    let caller_scope = selium_guest::process_tenant(client_process_id);
    let scope_verified = caller_scope.is_ok();
    let caller_tenant = caller_scope.unwrap_or_default();
    if !scope_verified {
        selium_guest::warn!(
            "failed to resolve caller tenant for process {client_process_id}; denying requests"
        );
    }
    loop {
        match conn.recv().await {
            Ok(request) => {
                let response = {
                    let mut store = store
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    let response = match request.payload() {
                        Ok(payload) if !scope_verified => denied_response(&payload),
                        Ok(payload) => match payload {
                            DiscoveryRequest::Resolve(uri) => {
                                match store.resolve_exact(&uri, caller_tenant.as_deref()) {
                                    Some(target) => {
                                        // A resolved shared resource (region or
                                        // queue) gains an authorisation basis
                                        // for the resolving client, but only
                                        // when its owner declared it attachable
                                        // by resolution: the runtime enforces
                                        // the declaration's scope and refuses an
                                        // undeclared resource. No guest identity
                                        // is privileged for recording.
                                        if matches!(
                                            target.class,
                                            selium_abi::ResourceClass::SharedRegion
                                                | selium_abi::ResourceClass::HostQueue
                                        ) && let Err(error) =
                                            selium_guest::record_resolved_resource_for(
                                                target.class.clone(),
                                                target.resource_id,
                                                client_process_id,
                                            )
                                        {
                                            selium_guest::warn!(
                                                "resolve authorisation record failed: {error}"
                                            );
                                        }
                                        DiscoveryResponse::Found(target)
                                    }
                                    None => DiscoveryResponse::NotFound,
                                }
                            }
                            DiscoveryRequest::ResolvePrefix(prefix) => {
                                let targets =
                                    store.resolve_prefix(&prefix, caller_tenant.as_deref());
                                DiscoveryResponse::Resolved(targets)
                            }
                            DiscoveryRequest::ResolveLabels { key, value } => {
                                let targets =
                                    store.resolve_labels(&key, &value, caller_tenant.as_deref());
                                DiscoveryResponse::Resolved(targets)
                            }
                            DiscoveryRequest::Register {
                                target,
                                root_service,
                                ..
                            } => {
                                // Root-namespace registrations (a `sel:///…`
                                // URI or a single-label wire name) require the
                                // system-registration capability.
                                let root_candidate = uri::is_root_uri(&target.uri)
                                    || !uri::parse_sel(&target.uri).is_some();
                                let root_allowed = root_candidate
                                    && selium_guest::process_capability(
                                        client_process_id,
                                        Capability::SystemRegistration,
                                    )
                                    .unwrap_or(false);
                                let registered_uri = store.registration_uri(
                                    &target.uri,
                                    client_process_id,
                                    caller_tenant.as_deref(),
                                    root_allowed,
                                    target.resource_id,
                                    &target.class,
                                );
                                let response = store.apply_register(
                                    client_process_id,
                                    caller_tenant.as_deref(),
                                    target,
                                    root_allowed,
                                    root_service,
                                );
                                if matches!(response, DiscoveryResponse::Registered)
                                    && let Some(uri) = registered_uri
                                {
                                    let now_ms = selium_guest::time::now()
                                        .map(|nanos| nanos / 1_000_000)
                                        .unwrap_or(0);
                                    store.set_lease(&uri, client_process_id, now_ms);
                                }
                                response
                            }
                            DiscoveryRequest::Renew { uri } => {
                                let now_ms = selium_guest::time::now()
                                    .map(|nanos| nanos / 1_000_000)
                                    .unwrap_or(0);
                                // Keyed exactly as the original registration was,
                                // so a wire-name registration renews the lease
                                // taken out under its projected internal URI.
                                if store.renew_lease_raw(&uri, client_process_id, now_ms) {
                                    DiscoveryResponse::Registered
                                } else {
                                    DiscoveryResponse::Forbidden
                                }
                            }
                            DiscoveryRequest::SetHealth { name, healthy } => {
                                store.set_health(
                                    caller_tenant.as_deref(),
                                    client_process_id,
                                    &name,
                                    healthy,
                                );
                                if healthy {
                                    DiscoveryResponse::Registered
                                } else {
                                    DiscoveryResponse::Revoked
                                }
                            }
                            DiscoveryRequest::Revoke { uri } => store.apply_revoke(
                                client_process_id,
                                caller_tenant.as_deref(),
                                &uri,
                            ),
                            DiscoveryRequest::ListDomains => {
                                DiscoveryResponse::Domains(store.domain_entries())
                            }
                            DiscoveryRequest::SetDomain { domain, tenant } => {
                                // The advisory domain mapping is authored by the
                                // bootstrap-provisioned `DomainWrite` authority
                                // (the identity guest), not by any guest identity.
                                let allowed = selium_guest::process_capability(
                                    client_process_id,
                                    Capability::DomainWrite,
                                )
                                .unwrap_or(false);
                                if allowed {
                                    store.seed_domain(&domain, &tenant);
                                    DiscoveryResponse::Registered
                                } else {
                                    DiscoveryResponse::Forbidden
                                }
                            }
                            // Tier-1-only operations never arrive over RPC.
                            DiscoveryRequest::SeedDomain { .. }
                            | DiscoveryRequest::RevokeByOwner { .. }
                            | DiscoveryRequest::RevokeResource { .. }
                            | DiscoveryRequest::RegisterHandler { .. } => {
                                DiscoveryResponse::Forbidden
                            }
                        },
                        Err(error) => {
                            selium_guest::warn!("discovery payload decode failed: {error}");
                            continue;
                        }
                    };
                    if let Err(error) = sync_route_table(&store, &route_table) {
                        selium_guest::warn!("discovery route table sync failed: {error}");
                    }
                    response
                };
                if let Err(error) = request.reply(response).await {
                    selium_guest::warn!("discovery reply failed: {error}");
                    break;
                }
            }
            Err(selium_shm::rpc::RpcError::ConnectionClosed) => break,
            Err(error) => {
                selium_guest::warn!("discovery recv failed: {error}");
                break;
            }
        }
    }
}

/// The root-namespace route a host-authored protocol handler resolves on.
fn handler_uri(scheme: &str) -> String {
    format!("{}/handler/{}", uri::SEL_PREFIX, scheme)
}

/// The tenant-scoped health route for `name`.
fn health_uri(tenant: Option<&str>, name: &str) -> String {
    format!(
        "{}{}/health/{}",
        uri::SEL_PREFIX,
        tenant.unwrap_or_default(),
        name
    )
}

/// Periodically expires lapsed guest-registration leases, revoking the routes
/// and publishing the resulting route changes.
async fn lease_sweep_loop(store: Arc<Mutex<DiscoveryStore>>, route_table: Arc<RouteTable>) {
    const SWEEP_INTERVAL_NANOS: u64 = 2_000_000_000;
    loop {
        let deadline = match selium_guest::time::now() {
            Ok(now) => selium_guest::Instant::from_nanos(now.saturating_add(SWEEP_INTERVAL_NANOS)),
            Err(error) => {
                selium_guest::warn!("discovery lease sweep clock failed: {error}");
                selium_guest::yield_now().await;
                continue;
            }
        };
        selium_guest::Timer::new(deadline).await;

        let now_ms = selium_guest::time::now()
            .map(|nanos| nanos / 1_000_000)
            .unwrap_or(0);
        let mut store = store
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        store.expire_leases(now_ms);
        if let Err(error) = sync_route_table(&store, &route_table) {
            selium_guest::warn!("discovery route table sync failed: {error}");
        }
    }
}

/// Projects the store's registrations onto the route table, publishing only
/// real changes: a set for every new or changed route, a delete for every
/// removed route. The table's per-entry version is the route's generation.
fn sync_route_table(store: &DiscoveryStore, table: &RouteTable) -> selium_wire::Result<()> {
    let current: std::collections::HashMap<&str, &ResourceTarget> = store
        .registrations
        .iter()
        .map(|(uri, target)| (uri.as_str(), target))
        .collect();
    for (uri, target) in &current {
        let uri = (*uri).to_string();
        if table.get(&uri)?.as_ref() != Some(*target) {
            table.set(uri, (*target).clone())?;
        }
    }
    for (uri, record) in table.scan(usize::MAX)? {
        if record.value.is_some() && !current.contains_key(uri.as_str()) {
            table.delete(uri)?;
        }
    }
    Ok(())
}

/// Returns whether a caller's tenant admits a target's tenant. The check is
/// skipped when either side is absent (backward-compatible with root/system
/// registrations and untracked tenants).
fn tenant_admits(caller: Option<&str>, target: Option<&str>) -> bool {
    match (caller, target) {
        (Some(caller), Some(target)) => caller == target,
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use selium_service::Label;

    fn target(
        uri: &str,
        resource_id: u64,
        tenant: Option<&str>,
        class: selium_abi::ResourceClass,
    ) -> ResourceTarget {
        ResourceTarget {
            uri: uri.to_string(),
            host_id: "host-a".to_string(),
            resource_id,
            interface: None,
            tenant: tenant.map(str::to_string),
            class,
            labels: Vec::new(),
        }
    }

    fn region(tenant: &str, id: u64) -> ResourceTarget {
        target(
            &uri::resource_uri(tenant, selium_abi::ResourceClass::SharedRegion, id),
            id,
            (!tenant.is_empty()).then_some(tenant),
            selium_abi::ResourceClass::SharedRegion,
        )
    }

    fn queue(tenant: &str, id: u64) -> ResourceTarget {
        target(
            &uri::resource_uri(tenant, selium_abi::ResourceClass::HostQueue, id),
            id,
            (!tenant.is_empty()).then_some(tenant),
            selium_abi::ResourceClass::HostQueue,
        )
    }

    fn process(tenant: &str, id: u64, labels: Vec<(&str, &str)>) -> ResourceTarget {
        let mut t = target(
            &uri::resource_uri(tenant, selium_abi::ResourceClass::Process, id),
            id,
            (!tenant.is_empty()).then_some(tenant),
            selium_abi::ResourceClass::Process,
        );
        t.labels = labels
            .into_iter()
            .map(|(k, v)| Label {
                key: k.to_string(),
                value: v.to_string(),
            })
            .collect();
        t
    }

    #[test]
    fn typed_uri_resolves_within_tenant() {
        let mut store = DiscoveryStore::default();
        store.store(region("acme", 7), Some(42));

        assert_eq!(
            store.resolve_exact("sel://acme/region/7", Some("acme")),
            Some(region("acme", 7))
        );
        assert_eq!(
            store.resolve_exact("sel://acme/region/7", Some("beta")),
            None
        );
        assert_eq!(
            store.resolve_exact("sel://acme/region/8", Some("acme")),
            None
        );
    }

    #[test]
    fn root_uri_resolves_for_system_callers() {
        let mut store = DiscoveryStore::default();
        let dnchecked = target(
            "sel:///dns/resolve",
            12,
            None,
            selium_abi::ResourceClass::HostQueue,
        );
        store.store(dnchecked, None);

        assert!(store.resolve_exact("sel:///dns/resolve", None).is_some());
        assert!(
            store
                .resolve_exact("sel:///dns/resolve", Some("acme"))
                .is_some()
        );
    }

    #[test]
    fn single_segment_route_resolves_to_the_serving_target() {
        let mut store = DiscoveryStore::default();
        store.store(process("acme", 123, vec![]), Some(123));
        let route = target(
            "sel://acme/proxy",
            123,
            Some("acme"),
            selium_abi::ResourceClass::Process,
        );
        store.apply_register(123, Some("acme"), route.clone(), false, false);

        // A single-segment route is an exact-key registration of the caller's
        // own target (same resource as the typed registration, carrying the
        // route URI and any interface metadata), not a bare pointer that
        // resolves to the typed target.
        assert_eq!(
            store.resolve_exact("sel://acme/proxy", Some("acme")),
            Some(route)
        );
    }

    #[test]
    fn single_segment_route_preserves_interface_metadata() {
        // A single-segment serve (e.g. `HttpServeStream::bind(ctx, "api")`)
        // carries an interface marker on its target; resolution must return
        // the caller's target so the marker survives (a bare alias pointer
        // to the typed registration would drop it).
        let mut store = DiscoveryStore::default();
        store.store(queue("acme", 7), Some(42));
        let mut route = target(
            "sel://acme/api",
            7,
            Some("acme"),
            selium_abi::ResourceClass::HostQueue,
        );
        route.interface = Some(InterfaceMetadata {
            name: "selium.http/stream".to_string(),
            methods: Vec::new(),
        });
        store.apply_register(42, Some("acme"), route.clone(), false, false);

        let resolved = store
            .resolve_exact("api.acme", Some("acme"))
            .expect("route resolves");
        assert_eq!(
            resolved.interface.expect("interface marker survives"),
            route.interface.unwrap()
        );
    }

    #[test]
    fn revoking_a_target_revokes_its_aliases() {
        let mut store = DiscoveryStore::default();
        store.store(region("acme", 456), Some(42));
        store.apply_register(
            42,
            Some("acme"),
            target(
                "sel://acme/proxy",
                456,
                Some("acme"),
                selium_abi::ResourceClass::SharedRegion,
            ),
            false,
            false,
        );

        store.revoke_key("sel://acme/region/456");
        assert!(
            store
                .resolve_exact("sel://acme/region/456", Some("acme"))
                .is_none()
        );
        assert!(
            store
                .resolve_exact("sel://acme/proxy", Some("acme"))
                .is_none()
        );
    }

    #[test]
    fn guest_cannot_register_in_root_namespace() {
        let mut store = DiscoveryStore::default();
        store.store(region("acme", 7), Some(42));

        let response = store.apply_register(
            42,
            Some("acme"),
            target(
                "sel:///discovery",
                7,
                None,
                selium_abi::ResourceClass::SharedRegion,
            ),
            false,
            false,
        );
        assert!(matches!(response, DiscoveryResponse::Forbidden));
        assert!(
            store
                .resolve_exact("sel:///discovery", Some("acme"))
                .is_none()
        );
    }

    #[test]
    fn guest_cannot_mint_typed_uris() {
        let mut store = DiscoveryStore::default();
        store.store(region("acme", 7), Some(42));

        let response = store.apply_register(
            42,
            Some("acme"),
            target(
                "sel://acme/region/999",
                7,
                Some("acme"),
                selium_abi::ResourceClass::SharedRegion,
            ),
            false,
            false,
        );
        assert!(matches!(response, DiscoveryResponse::Forbidden));
    }

    #[test]
    fn guest_registration_requires_ownership() {
        let mut store = DiscoveryStore::default();
        store.store(region("acme", 7), Some(42));

        // Process 99 does not own resource 7.
        let response = store.apply_register(
            99,
            Some("acme"),
            target(
                "sel://acme/proxy",
                7,
                Some("acme"),
                selium_abi::ResourceClass::SharedRegion,
            ),
            false,
            false,
        );
        assert!(matches!(response, DiscoveryResponse::Forbidden));
        assert!(
            store
                .resolve_exact("sel://acme/proxy", Some("acme"))
                .is_none()
        );
    }

    #[test]
    fn guest_alias_must_live_under_own_tenant() {
        let mut store = DiscoveryStore::default();
        store.store(region("acme", 7), Some(42));

        let response = store.apply_register(
            42,
            Some("beta"),
            target(
                "sel://acme/proxy",
                7,
                Some("acme"),
                selium_abi::ResourceClass::SharedRegion,
            ),
            false,
            false,
        );
        assert!(matches!(response, DiscoveryResponse::Forbidden));
    }

    #[test]
    fn alias_cannot_shadow_a_class_noun() {
        let mut store = DiscoveryStore::default();
        store.store(region("acme", 7), Some(42));

        // `region` is a reserved class noun; parse_alias rejects it, so it is
        // treated as a typed URI attempt and refused.
        let response = store.apply_register(
            42,
            Some("acme"),
            target(
                "sel://acme/region",
                7,
                Some("acme"),
                selium_abi::ResourceClass::SharedRegion,
            ),
            false,
            false,
        );
        assert!(matches!(response, DiscoveryResponse::Forbidden));
    }

    #[test]
    fn label_query_returns_matching_targets_only() {
        let mut store = DiscoveryStore::default();
        store.store(process("acme", 123, vec![("app", "web")]), Some(123));
        store.store(process("acme", 124, vec![("app", "web")]), Some(124));
        store.store(process("acme", 125, vec![("app", "worker")]), Some(125));

        let matches = store.resolve_labels("app", "web", Some("acme"));
        assert_eq!(matches.len(), 2);
        assert!(matches.iter().all(|t| {
            t.labels.contains(&Label {
                key: "app".to_string(),
                value: "web".to_string(),
            })
        }));
    }

    #[test]
    fn label_query_is_tenant_scoped() {
        let mut store = DiscoveryStore::default();
        store.store(process("acme", 123, vec![("app", "web")]), Some(123));
        store.store(process("beta", 124, vec![("app", "web")]), Some(124));

        assert_eq!(store.resolve_labels("app", "web", Some("acme")).len(), 1);
        assert_eq!(store.resolve_labels("app", "web", Some("beta")).len(), 1);
    }

    #[test]
    fn prefix_enumeration_lists_a_tenants_resources() {
        let mut store = DiscoveryStore::default();
        store.store(region("acme", 1), Some(42));
        store.store(region("acme", 2), Some(42));
        store.store(region("beta", 3), Some(43));

        let results = store.resolve_prefix("sel://acme/region/*", Some("acme"));
        assert_eq!(results.len(), 2);

        // Cross-tenant enumeration is denied.
        assert!(
            store
                .resolve_prefix("sel://acme/region/*", Some("beta"))
                .is_empty()
        );
    }

    #[test]
    fn prefix_enumeration_with_bare_star() {
        let mut store = DiscoveryStore::default();
        store.store(region("acme", 1), Some(42));
        store.store(process("acme", 123, vec![]), Some(123));

        let results = store.resolve_prefix("sel://acme/*", Some("acme"));
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn wire_name_resolves_through_addressing() {
        let mut store = DiscoveryStore::default();
        store.store(queue("acme", 7), Some(42));
        // A named-service route: the tenant's queue under the path `bridge`.
        let bridge = target(
            "sel://acme/bridge",
            7,
            Some("acme"),
            selium_abi::ResourceClass::HostQueue,
        );
        assert!(matches!(
            store.apply_register(42, Some("acme"), bridge.clone(), false, false),
            DiscoveryResponse::Registered
        ));

        // The synthetic label resolves without any table entry, mapping
        // `bridge.acme` onto the internal `sel://acme/bridge` route. The
        // resolved target is the route's own registration (same queue
        // resource, carrying the route URI and interface metadata).
        assert_eq!(
            store.resolve_exact("bridge.acme", Some("acme")),
            store.resolve_exact("sel://acme/bridge", Some("acme"))
        );
        assert_eq!(
            store.resolve_exact("bridge.acme", Some("acme")),
            Some(bridge)
        );
        // Unknown traffic does not contact the synthetic namespace.
        assert_eq!(store.resolve_exact("bridge.beta", Some("acme")), None);
    }

    #[test]
    fn registered_domain_wire_name_resolves() {
        let mut store = DiscoveryStore::default();
        store.seed_domain("example.com", "acme");
        store.store(queue("acme", 7), Some(42));
        let bridge = target(
            "sel://acme/bridge",
            7,
            Some("acme"),
            selium_abi::ResourceClass::HostQueue,
        );
        store.apply_register(42, Some("acme"), bridge.clone(), false, false);

        // `example.com -> acme` is provisioned, so `bridge.example.com`
        // reverses into path `["bridge"]` within tenant `acme`.
        assert_eq!(
            store.resolve_exact("bridge.example.com", Some("acme")),
            Some(bridge)
        );
    }

    #[test]
    fn domain_table_covers_unknown_and_provisioned_cases() {
        let mut store = DiscoveryStore::default();
        assert_eq!(store.domain_tenant("example.com"), None);

        store.seed_domain("example.com", "acme");
        assert_eq!(store.domain_tenant("example.com"), Some("acme"));
        assert_eq!(store.domain_tenant("example.org"), None);
    }

    #[test]
    fn tier1_seed_domain_populates_the_table() {
        // The runtime publishes SeedDomain over the Tier-1 feed; the store
        // applies it as out-of-band provisioning.
        let mut store = DiscoveryStore::default();
        store.apply_tier1_event(DiscoveryRequest::SeedDomain {
            domain: "example.com".to_string(),
            tenant: "acme".to_string(),
        });
        assert_eq!(store.domain_tenant("example.com"), Some("acme"));
    }

    #[test]
    fn tier1_revoke_by_owner_revokes_guest_routes() {
        // A process exit arrives as a Tier-1 RevokeByOwner event; the store
        // revokes every route the owner registered, with no runtime side map.
        let mut store = DiscoveryStore::default();
        store.store(queue("acme", 7), Some(42));
        store.apply_register(
            42,
            Some("acme"),
            target(
                "sel://acme/bridge",
                7,
                Some("acme"),
                selium_abi::ResourceClass::HostQueue,
            ),
            false,
            false,
        );
        assert!(store.resolve_exact("bridge.acme", Some("acme")).is_some());

        store.apply_tier1_event(DiscoveryRequest::RevokeByOwner { process_id: 42 });
        assert_eq!(store.resolve_exact("bridge.acme", Some("acme")), None);
    }

    #[test]
    fn tier1_revoke_resource_revokes_by_identity() {
        // A resource free arrives as a Tier-1 RevokeResource event; the store
        // revokes the registration recorded for that resource identity under
        // the tenant it recorded — the runtime needs no tenant side map.
        let mut store = DiscoveryStore::default();
        store.store(queue("acme", 7), Some(42));
        store.store(queue("beta", 9), Some(99));
        assert!(
            store
                .resolve_exact("sel://acme/queue/7", Some("acme"))
                .is_some()
        );
        assert!(
            store
                .resolve_exact("sel://beta/queue/9", Some("beta"))
                .is_some()
        );

        store.apply_tier1_event(DiscoveryRequest::RevokeResource {
            class: "queue".to_string(),
            id: 7,
        });
        assert_eq!(
            store.resolve_exact("sel://acme/queue/7", Some("acme")),
            None
        );
        assert!(
            store
                .resolve_exact("sel://beta/queue/9", Some("beta"))
                .is_some(),
            "only the named resource identity is revoked"
        );
    }

    #[test]
    fn foreign_domain_registration_is_refused() {
        let mut store = DiscoveryStore::default();
        store.seed_domain("example.com", "acme");
        store.store(queue("beta", 7), Some(99));

        // Tenant `beta` attempts to register a name under `example.com`, a
        // domain owned by `acme`: the derived tenant does not admit the
        // caller, so the registration is refused.
        let response = store.apply_register(
            99,
            Some("beta"),
            target(
                "bridge.example.com",
                7,
                Some("beta"),
                selium_abi::ResourceClass::HostQueue,
            ),
            false,
            false,
        );
        assert!(matches!(response, DiscoveryResponse::Forbidden));
        assert_eq!(
            store.resolve_exact("bridge.example.com", Some("acme")),
            None
        );
    }

    #[test]
    fn tier1_register_populates_ownership() {
        let mut store = DiscoveryStore::default();
        store.store(region("acme", 7), Some(42));
        assert_eq!(
            store.ownership.get(&(42, 7)),
            Some(&selium_abi::ResourceClass::SharedRegion)
        );
        assert!(!store.ownership.contains_key(&(99, 7)));
    }

    #[test]
    fn guest_revokes_their_own_custom_uri() {
        let mut store = DiscoveryStore::default();
        store.store(queue("acme", 7), Some(42));
        let route = target(
            "sel://acme/bridge",
            7,
            Some("acme"),
            selium_abi::ResourceClass::HostQueue,
        );
        store.apply_register(42, Some("acme"), route, false, false);

        // Revocation accepts the wire name and translates it to the internal
        // route it projects onto.
        let response = store.apply_revoke(42, Some("acme"), "bridge.acme");
        assert!(matches!(response, DiscoveryResponse::Revoked));
        assert_eq!(store.resolve_exact("bridge.acme", Some("acme")), None);
    }

    #[test]
    fn guest_cannot_revoke_root_namespace() {
        let mut store = DiscoveryStore::default();
        let response = store.apply_revoke(42, Some("acme"), "sel:///dns/resolve");
        assert!(matches!(response, DiscoveryResponse::Forbidden));
    }

    #[test]
    fn guest_cannot_revoke_typed_uris() {
        let mut store = DiscoveryStore::default();
        store.store(region("acme", 7), Some(42));

        // Typed URIs are runtime-minted; only the Tier-1 feed revokes them.
        let response = store.apply_revoke(42, Some("acme"), "sel://acme/region/7");
        assert!(matches!(response, DiscoveryResponse::Forbidden));
        assert!(
            store
                .resolve_exact("sel://acme/region/7", Some("acme"))
                .is_some()
        );
    }

    #[test]
    fn guest_cannot_revoke_another_tenants_alias() {
        let mut store = DiscoveryStore::default();
        store.store(region("acme", 7), Some(42));
        let registered = store.apply_register(
            42,
            Some("acme"),
            target(
                "sel://acme/proxy",
                7,
                Some("acme"),
                selium_abi::ResourceClass::SharedRegion,
            ),
            false,
            false,
        );
        assert!(matches!(registered, DiscoveryResponse::Registered));

        // A caller whose verified tenant is not the alias's tenant is denied.
        let response = store.apply_revoke(42, Some("beta"), "sel://acme/proxy");
        assert!(matches!(response, DiscoveryResponse::Forbidden));
        assert!(
            store
                .resolve_exact("sel://acme/proxy", Some("acme"))
                .is_some()
        );
    }

    #[test]
    fn guest_cannot_revoke_an_alias_it_does_not_own() {
        let mut store = DiscoveryStore::default();
        store.store(region("acme", 7), Some(42));
        store.apply_register(
            42,
            Some("acme"),
            target(
                "sel://acme/proxy",
                7,
                Some("acme"),
                selium_abi::ResourceClass::SharedRegion,
            ),
            false,
            false,
        );

        // Process 99 does not own resource 7, so it may not revoke the alias.
        let response = store.apply_revoke(99, Some("acme"), "sel://acme/proxy");
        assert!(matches!(response, DiscoveryResponse::Forbidden));
        assert!(
            store
                .resolve_exact("sel://acme/proxy", Some("acme"))
                .is_some()
        );
    }

    #[test]
    fn guest_cannot_revoke_a_route_it_does_not_own() {
        let mut store = DiscoveryStore::default();
        store.store(queue("acme", 7), Some(42));
        let bridge = target(
            "sel://acme/bridge",
            7,
            Some("acme"),
            selium_abi::ResourceClass::HostQueue,
        );
        store.apply_register(42, Some("acme"), bridge.clone(), false, false);

        let response = store.apply_revoke(99, Some("acme"), "bridge.acme");
        assert!(matches!(response, DiscoveryResponse::Forbidden));
        assert_eq!(
            store.resolve_exact("bridge.acme", Some("acme")),
            Some(bridge)
        );
    }

    #[test]
    fn revoking_an_unknown_uri_returns_not_found() {
        let mut store = DiscoveryStore::default();
        store.store(region("acme", 7), Some(42));

        let response = store.apply_revoke(42, Some("acme"), "sel://acme/nowhere");
        assert!(matches!(response, DiscoveryResponse::NotFound));
    }

    #[test]
    fn guest_alias_class_must_match_the_owned_resource() {
        let mut store = DiscoveryStore::default();
        store.store(region("acme", 7), Some(42));

        // Process 42 owns resource 7 as a SharedRegion; an alias claiming it
        // is a Process node must not pass, even for the owner.
        let response = store.apply_register(
            42,
            Some("acme"),
            target(
                "sel://acme/proxy",
                7,
                Some("acme"),
                selium_abi::ResourceClass::Process,
            ),
            false,
            false,
        );
        assert!(matches!(response, DiscoveryResponse::Forbidden));
    }

    #[test]
    fn guest_alias_must_point_at_a_registered_target() {
        let mut store = DiscoveryStore::default();
        // A delegated allocation: the runtime minted region 7 for tenant
        // "beta" on behalf of process 42, which owns it.
        store.store(region("beta", 7), Some(42));

        // An alias under the caller's own tenant claims the canonical
        // `sel://acme/region/7`, which does not exist — the registered
        // target lives under tenant "beta".
        let response = store.apply_register(
            42,
            Some("acme"),
            target(
                "sel://acme/proxy",
                7,
                Some("acme"),
                selium_abi::ResourceClass::SharedRegion,
            ),
            false,
            false,
        );
        assert!(matches!(response, DiscoveryResponse::NotFound));
    }

    #[test]
    fn denied_response_fails_closed_per_variant() {
        use selium_service::ResourceTarget;

        let resolve = DiscoveryRequest::Resolve("sel://acme/region/7".to_string());
        assert!(matches!(
            denied_response(&resolve),
            DiscoveryResponse::NotFound
        ));

        let prefix = DiscoveryRequest::ResolvePrefix("sel://acme/region/*".to_string());
        assert!(matches!(
            denied_response(&prefix),
            DiscoveryResponse::Resolved(ref targets) if targets.is_empty()
        ));

        let labels = DiscoveryRequest::ResolveLabels {
            key: "app".to_string(),
            value: "web".to_string(),
        };
        assert!(matches!(
            denied_response(&labels),
            DiscoveryResponse::Resolved(ref targets) if targets.is_empty()
        ));

        let register = DiscoveryRequest::Register {
            uri: "sel://acme/proxy".to_string(),
            target: ResourceTarget {
                uri: "sel://acme/proxy".to_string(),
                host_id: String::new(),
                resource_id: 7,
                interface: None,
                tenant: Some("acme".to_string()),
                class: selium_abi::ResourceClass::SharedRegion,
                labels: Vec::new(),
            },
            owner: None,
            root_service: false,
        };
        assert!(matches!(
            denied_response(&register),
            DiscoveryResponse::Forbidden
        ));

        let revoke = DiscoveryRequest::Revoke {
            uri: "https://acme.com/path".to_string(),
        };
        assert!(matches!(
            denied_response(&revoke),
            DiscoveryResponse::Forbidden
        ));
    }

    #[test]
    fn end_to_end_discovery_lifecycle() {
        // Task 5.2 store-level golden path: spawn-node → allocate-region →
        // alias → label-query → teardown-revoke.
        let mut store = DiscoveryStore::default();

        // Spawn node: process 123 of tenant "acme" (registered by the runtime).
        store.store(process("acme", 123, vec![("app", "web")]), Some(123));
        assert!(
            store
                .resolve_exact("sel://acme/proc/123", Some("acme"))
                .is_some()
        );

        // Allocate region: runtime mints `sel://acme/region/7`.
        store.store(region("acme", 7), Some(123));
        assert!(
            store
                .resolve_exact("sel://acme/region/7", Some("acme"))
                .is_some()
        );

        // Route: the owning process registers a single-segment route
        // (`sel://acme/cache`) for the region. Resolution returns the
        // route's own registration — same resource, route URI — and
        // revoking the typed registration cascades to the route below.
        let route = target(
            "sel://acme/cache",
            7,
            Some("acme"),
            selium_abi::ResourceClass::SharedRegion,
        );
        assert!(matches!(
            store.apply_register(123, Some("acme"), route.clone(), false, false),
            DiscoveryResponse::Registered
        ));
        assert_eq!(
            store.resolve_exact("sel://acme/cache", Some("acme")),
            Some(route)
        );

        // Label query: the process node carries `app=web`.
        assert_eq!(store.resolve_labels("app", "web", Some("acme")).len(), 1);

        // Teardown: revoking the region also revokes its alias.
        store.revoke_key("sel://acme/region/7");
        assert!(
            store
                .resolve_exact("sel://acme/region/7", Some("acme"))
                .is_none()
        );
        assert!(
            store
                .resolve_exact("sel://acme/cache", Some("acme"))
                .is_none()
        );

        // Revoking the process node makes it unresolvable.
        store.revoke_key("sel://acme/proc/123");
        assert!(
            store
                .resolve_exact("sel://acme/proc/123", Some("acme"))
                .is_none()
        );
    }

    #[test]
    fn owner_exit_revokes_owned_routes() {
        let mut store = DiscoveryStore::default();
        store.store(queue("acme", 7), Some(42));
        store.apply_register(
            42,
            Some("acme"),
            target(
                "sel://acme/bridge",
                7,
                Some("acme"),
                selium_abi::ResourceClass::HostQueue,
            ),
            false,
            false,
        );
        assert!(store.resolve_exact("bridge.acme", Some("acme")).is_some());

        // Owner-keyed revocation: the owning process exits and discovery
        // revokes its routes without a runtime-maintained side map.
        store.revoke_by_owner(42);
        assert_eq!(store.resolve_exact("bridge.acme", Some("acme")), None);
        assert_eq!(store.resolve_exact("sel://acme/bridge", Some("acme")), None);
    }

    #[test]
    fn root_registration_requires_capability() {
        let mut store = DiscoveryStore::default();
        store.store(queue("", 7), Some(42));

        // Without the system-registration capability, a root-namespace route
        // is refused.
        let denied = store.apply_register(
            42,
            None,
            target(
                "sel:///dns/resolve",
                7,
                None,
                selium_abi::ResourceClass::HostQueue,
            ),
            false,
            false,
        );
        assert!(matches!(denied, DiscoveryResponse::Forbidden));

        // With the capability, the same registration is accepted.
        let allowed = store.apply_register(
            42,
            None,
            target(
                "sel:///dns/resolve",
                7,
                None,
                selium_abi::ResourceClass::HostQueue,
            ),
            true,
            false,
        );
        assert!(matches!(allowed, DiscoveryResponse::Registered));
        assert!(store.resolve_exact("sel:///dns/resolve", None).is_some());
    }

    #[test]
    fn root_service_designation_resolves_the_apex() {
        let mut store = DiscoveryStore::default();
        store.seed_domain("example.com", "acme");
        store.store(queue("acme", 7), Some(42));
        // The tenant designates `["http", "prod"]` as its root service.
        store.apply_register(
            42,
            Some("acme"),
            target(
                "sel://acme/http/prod",
                7,
                Some("acme"),
                selium_abi::ResourceClass::HostQueue,
            ),
            false,
            true,
        );

        // The bare domain resolves to the designated root service.
        assert_eq!(
            store.resolve_exact("example.com", Some("acme")),
            Some(target(
                "sel://acme/http/prod",
                7,
                Some("acme"),
                selium_abi::ResourceClass::HostQueue,
            ))
        );
    }

    #[test]
    fn wire_name_registration_produces_the_same_route_as_serve() {
        let mut store = DiscoveryStore::default();
        store.store(queue("acme", 7), Some(42));
        // A guest registers a bare wire name; it translates to the internal
        // path and is resolvable both ways.
        let route = target(
            "bridge.acme",
            7,
            Some("acme"),
            selium_abi::ResourceClass::HostQueue,
        );
        let response = store.apply_register(42, Some("acme"), route.clone(), false, false);
        assert!(matches!(response, DiscoveryResponse::Registered));
        // Both spellings resolve to the same route registration, whose target
        // is keyed by the derived internal path (the wire name is a
        // projection, not a second key).
        assert_eq!(
            store.resolve_exact("bridge.acme", Some("acme")),
            Some(target(
                "sel://acme/bridge",
                7,
                Some("acme"),
                selium_abi::ResourceClass::HostQueue,
            ))
        );
        assert_eq!(
            store.resolve_exact("sel://acme/bridge", Some("acme")),
            Some(target(
                "sel://acme/bridge",
                7,
                Some("acme"),
                selium_abi::ResourceClass::HostQueue,
            ))
        );
    }

    #[test]
    fn route_table_publishes_appear_replace_and_revoke() {
        drop(selium_memory::set_region_provider(Box::new(
            selium_memory::HeapRegionProvider::new(),
        )));
        let (region_id, table) = create_route_table().expect("create route table");

        let mut store = DiscoveryStore::default();
        let uri = "sel://acme/queue/7".to_string();
        store.store(queue("acme", 7), Some(42));
        sync_route_table(&store, &table).expect("sync register");

        let channel = Channel::attach(region_id).expect("attach route table");
        let transport = ShmTransport::new_replay(&channel, &channel).expect("transport");
        let subscriber = Subscriber::new(FramedRead::new(transport), None);
        let view: selium_wire::LiveTableView<String, ResourceTarget, ShmTransport> =
            selium_wire::LiveTableView::new(subscriber).expect("view");

        let appeared = view.get_record(&uri).expect("get").expect("appeared");
        assert_eq!(
            appeared.value.as_ref().map(|target| target.resource_id),
            Some(7)
        );
        let first_version = appeared.version;

        // Replace the same URI with a new target: the generation advances.
        store.store(
            target(&uri, 99, Some("acme"), selium_abi::ResourceClass::HostQueue),
            Some(42),
        );
        sync_route_table(&store, &table).expect("sync replace");
        view.sync().expect("view sync");
        let replaced = view.get_record(&uri).expect("get").expect("replaced");
        assert_eq!(
            replaced.value.as_ref().map(|target| target.resource_id),
            Some(99)
        );
        assert!(
            replaced.version > first_version,
            "replacement advances the generation"
        );

        // Revoke: the table publishes a tombstone.
        store.revoke_key(&uri);
        sync_route_table(&store, &table).expect("sync revoke");
        view.sync().expect("view sync");
        let revoked = view.get_record(&uri).expect("get").expect("tombstone");
        assert!(revoked.value.is_none(), "revocation is a tombstone");
    }

    #[test]
    fn lease_renewal_keeps_a_registration_live() {
        let mut store = DiscoveryStore::default();
        store.store(queue("acme", 7), Some(42));
        store.set_lease("sel://acme/queue/7", 42, 1_000);
        assert!(store.renew_lease("sel://acme/queue/7", 42, 1_000 + LEASE_TTL_MS - 1));
        store.expire_leases(1_000 + LEASE_TTL_MS + 5);
        assert!(
            store
                .resolve_exact("sel://acme/queue/7", Some("acme"))
                .is_some(),
            "a renewed lease stays live"
        );
    }

    #[test]
    fn lapsed_lease_expires_the_registration() {
        let mut store = DiscoveryStore::default();
        store.store(queue("acme", 7), Some(42));
        store.set_lease("sel://acme/queue/7", 42, 1_000);
        store.expire_leases(1_000 + LEASE_TTL_MS + 1);
        assert_eq!(
            store.resolve_exact("sel://acme/queue/7", Some("acme")),
            None,
            "a lapsed lease revokes the registration"
        );
    }

    #[test]
    fn lease_renewal_requires_the_owner() {
        let mut store = DiscoveryStore::default();
        store.store(queue("acme", 7), Some(42));
        store.set_lease("sel://acme/queue/7", 42, 1_000);
        assert!(!store.renew_lease("sel://acme/queue/7", 99, 1_000));
    }

    /// A wire-name registration is leased under its projected internal URI, so
    /// renewal has to key the same way registration did: renewing the raw wire
    /// name must find the lease, not miss it and lapse the route.
    #[test]
    fn a_wire_name_registration_renews_under_the_key_it_was_registered_with() {
        let mut store = DiscoveryStore::default();
        // `bridge.acme` projects to `sel://acme/bridge`.
        store.set_lease("sel://acme/bridge", 42, 1_000);
        assert!(
            store.renew_lease_raw("bridge.acme", 42, 1_000),
            "renewing by the raw wire name finds the projected lease"
        );
        store.expire_leases(1_000 + LEASE_TTL_MS + 1);
        assert!(
            !store.renew_lease_raw("bridge.acme", 42, 1_000 + LEASE_TTL_MS + 2),
            "a lease nobody renewed has lapsed"
        );
    }

    #[test]
    fn a_health_record_outlives_the_lease_window_and_dies_with_its_owner() {
        // Health is advisory published state, not a leased route: it must not
        // lapse while its guest is alive and healthy (that would report every
        // long-running guest as unhealthy), but it disappears with the process
        // that wrote it.
        let mut store = DiscoveryStore::default();
        store.set_health(Some("acme"), 7, "api", true);
        store.expire_leases(LEASE_TTL_MS * 3);
        assert!(
            store
                .resolve_exact("sel://acme/health/api", Some("acme"))
                .is_some(),
            "a health record is not lease-expired"
        );

        store.revoke_by_owner(7);
        assert_eq!(
            store.resolve_exact("sel://acme/health/api", Some("acme")),
            None,
            "a health record is revoked when its owner exits"
        );
    }

    /// The health namespace is reserved: a guest cannot shadow another guest's
    /// health record by registering a same-named route of its own.
    #[test]
    fn the_health_namespace_is_reserved_against_guest_registration() {
        let mut store = DiscoveryStore::default();
        store.store(queue("acme", 7), Some(42));
        assert_eq!(
            store.registration_uri(
                "sel://acme/health/api",
                42,
                Some("acme"),
                false,
                7,
                &selium_abi::ResourceClass::HostQueue,
            ),
            None,
            "a guest may not register inside the health namespace"
        );
    }

    #[test]
    fn health_record_sets_resolves_and_clears() {
        // Health is published state: a tenant-scoped record a peer resolves.
        let mut store = DiscoveryStore::default();
        store.set_health(Some("acme"), 7, "api", true);
        assert!(
            store
                .resolve_exact("sel://acme/health/api", Some("acme"))
                .is_some()
        );

        store.set_health(Some("acme"), 7, "api", false);
        assert!(
            store
                .resolve_exact("sel://acme/health/api", Some("acme"))
                .is_none(),
            "an absent health record is not observable"
        );
    }

    #[test]
    fn handler_registration_resolves_and_is_revoked_by_owner() {
        let mut store = DiscoveryStore::default();
        store.apply_tier1_event(DiscoveryRequest::RegisterHandler {
            scheme: "sel-quic".to_string(),
            process_id: 42,
        });
        let target = store
            .resolve_exact("sel:///handler/sel-quic", None)
            .expect("handler resolves");
        assert_eq!(target.resource_id, 42);

        store.revoke_by_owner(42);
        assert_eq!(store.resolve_exact("sel:///handler/sel-quic", None), None);
    }

    #[test]
    fn guests_cannot_register_a_handler_route() {
        // The handler namespace is host-authored: even a root-authorised guest
        // registration of a handler route is refused, so the pin is unforgeable.
        let mut store = DiscoveryStore::default();
        let response = store.apply_register(
            7,
            None,
            target(
                "sel:///handler/sel-quic",
                7,
                None,
                selium_abi::ResourceClass::Process,
            ),
            true,
            false,
        );
        assert!(matches!(response, DiscoveryResponse::Forbidden));
    }
}
