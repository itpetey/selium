//! Capability authority: delegation, child-grant admission, and ownership.
//!
//! These helpers answer the questions the dispatcher's handlers share: may
//! this process mint a resource for a serving tenant, may it confer these
//! grants on a child, and does it own a shared resource. Grant evaluation is
//! per-grant and conjunctive: several grants never merge into broader
//! authority, and every tenant-scoping selector must be in scope.

use selium_abi::{
    AbiError, AbiErrorCode, Capability, CapabilityGrant, ModuleSource, Namespace, ProcessId,
    ResourceClass, ResourceSelector, ScopeContext,
};

use crate::Runtime;

/// Whether one `DelegateGrants` grant admits delegation in `tenant`.
///
/// Every tenant-scoping selector in the grant intersects: `Namespace::Root` is
/// the wildcard and a `Tenant`/`Namespace::Tenant` narrows it. A grant carrying
/// both `Root` and `Tenant("acme")` admits only `acme`.
fn grant_delegates_tenant(grant: &CapabilityGrant, tenant: &str) -> bool {
    if grant.capability != Capability::DelegateGrants {
        return false;
    }
    let mut scoped = false;
    for selector in &grant.selectors {
        match selector {
            ResourceSelector::Tenant(name)
            | ResourceSelector::Namespace(Namespace::Tenant(name)) => {
                scoped = true;
                if name != tenant {
                    return false;
                }
            }
            ResourceSelector::Namespace(Namespace::Root) => scoped = true,
            _ => {}
        }
    }
    scoped
}

/// Whether any single grant (never a synthesis of several grants) admits
/// delegation in `tenant`.
fn grants_delegate_tenant(grants: &[CapabilityGrant], tenant: &str) -> bool {
    grants
        .iter()
        .any(|grant| grant_delegates_tenant(grant, tenant))
}

/// Whether a child grant is wholly within the parent's delegation authority.
fn child_grant_within_delegation(
    parent_grants: &[CapabilityGrant],
    child: &CapabilityGrant,
) -> bool {
    let mut child_tenants = child
        .selectors
        .iter()
        .filter_map(|selector| match selector {
            ResourceSelector::Tenant(name)
            | ResourceSelector::Namespace(Namespace::Tenant(name)) => Some(name.as_str()),
            _ => None,
        });
    let Some(first) = child_tenants.next() else {
        return false;
    };
    if child_tenants.any(|tenant| tenant != first) {
        return false;
    }
    parent_grants
        .iter()
        .any(|parent| grant_delegates_tenant(parent, first))
}

impl Runtime {
    /// Authorises a principal-provenance allocation for the supplied
    /// serving tenant and returns the principal tenant the resource is
    /// minted under.
    ///
    /// A serving tenant equal to the allocating process's own tenant (or
    /// absent) needs no extra authority. A root principal (no tenant) may
    /// mint for any tenant — connectors and other trusted edge
    /// infrastructure run as root. A tenant-scoped process may mint for
    /// another tenant only with a `DelegateGrants` grant scoped to that
    /// tenant.
    pub(super) fn authorise_serving_tenant(
        &self,
        process_id: ProcessId,
        serving_tenant: Option<&str>,
    ) -> std::result::Result<String, AbiError> {
        let own_tenant = self.process_tenant(process_id);
        let Some(requested) = serving_tenant else {
            return Ok(own_tenant.unwrap_or_default());
        };
        if own_tenant.as_deref() == Some(requested) || own_tenant.is_none() {
            return Ok(requested.to_string());
        }
        if self.authorises(
            process_id,
            Capability::DelegateGrants,
            &ScopeContext {
                tenant: Some(requested.to_string()),
                ..ScopeContext::default()
            },
        ) {
            return Ok(requested.to_string());
        }
        Err(AbiError::new(
            AbiErrorCode::PermissionDenied,
            format!(
                "cross-tenant allocation for tenant {requested:?} requires a root principal or tenant-scoped delegation"
            ),
        ))
    }

    pub(super) fn ensure_shared_resource_owner(
        &self,
        process_id: ProcessId,
        capability: Capability,
        resource_class: ResourceClass,
        shared_id: u64,
    ) -> std::result::Result<(), AbiError> {
        if self
            .shared_resource_owners
            .lock()
            .get(&(resource_class, shared_id))
            .is_some_and(|owners| owners.contains(&process_id))
        {
            Ok(())
        } else {
            Err(AbiError::new(
                AbiErrorCode::PermissionDenied,
                format!("permission denied for capability {capability:?}"),
            ))
        }
    }

    /// Resolves a process's module source to bytes.
    ///
    /// Inline bytes are used as-is. A durable slice is read through the
    /// runtime's own volume layer under the caller's storage authority — the
    /// same rule as opening the volume — so a spawner cannot read a volume its
    /// storage authority does not cover, and the module bytes never round-trip
    /// through a guest. The volume name is passed as the scope URI so a
    /// prefix-scoped volume grant can be evaluated.
    pub(super) fn resolve_module_source(
        &self,
        process_id: ProcessId,
        source: ModuleSource,
    ) -> std::result::Result<Vec<u8>, AbiError> {
        match source {
            ModuleSource::Inline(bytes) => Ok(bytes),
            ModuleSource::Slice {
                volume,
                offset,
                len,
            } => {
                self.require_with_uri(
                    process_id,
                    Capability::Storage,
                    ResourceClass::Volume,
                    None,
                    volume.clone(),
                )?;
                let tenant = self.process_tenant(process_id);
                let volumes = self.kernel.volumes();
                let descriptor = volumes.open(tenant.as_deref(), &volume).map_err(|error| {
                    AbiError::new(AbiErrorCode::PermissionDenied, error.to_string())
                })?;
                let bytes = volumes
                    .read_at(descriptor.local_id, offset, len as usize)
                    .map_err(|error| AbiError::new(AbiErrorCode::Internal, error.to_string()))?;
                drop(volumes.close(descriptor.local_id));
                Ok(bytes)
            }
        }
    }

    pub(super) fn validate_child_grants(
        &self,
        process_id: ProcessId,
        grants: &[CapabilityGrant],
    ) -> std::result::Result<(), AbiError> {
        // Well-formedness admission always runs, even under delegation.
        self.validate_grants(grants)
            .map_err(|error| AbiError::new(AbiErrorCode::MalformedPayload, error.to_string()))?;
        let authority = self.restore_process_authority(process_id).ok_or_else(|| {
            AbiError::new(
                AbiErrorCode::InvalidHandle,
                format!("unknown process authority {process_id}"),
            )
        })?;

        // Some capabilities are bootstrap-provisioned only: the host mints
        // them for the system guest that owns the resource (the bridge, the
        // identity guest, the accountant, the registry), and a spawn can never
        // confer them on a child. One vocabulary drives the check so a new
        // bootstrap-provisioned capability cannot be forgotten here.
        if let Some(grant) = grants
            .iter()
            .find(|grant| grant.capability.is_bootstrap_provisioned())
        {
            return Err(AbiError::new(
                AbiErrorCode::PermissionDenied,
                format!(
                    "{} cannot be conferred on child processes",
                    grant.capability.name()
                ),
            ));
        }

        // Tenant-scoped grant delegation: a parent holding a
        // `DelegateGrants` grant with a tenant or root namespace selector may
        // confer child grants it does not itself hold, provided **every**
        // child grant is itself tenant-scoped within the parent's delegation
        // scope. A grant without an in-scope `Tenant` selector is unrestricted
        // within its capability and must therefore fall through to the subset
        // check: admitting it under delegation would escape the tenant fence.
        // This is the deliberate, tenant-fenced exception to authority
        // monotonicity (see the `rebuild-guest-bridge` design, D7).
        let delegating = grants
            .iter()
            .all(|grant| child_grant_within_delegation(&authority.grants, grant));

        if delegating {
            return Ok(());
        }

        let parent_grants = authority.grants;
        for grant in grants {
            if !parent_grants
                .iter()
                .any(|parent| parent_grant_covers_child(parent, grant))
            {
                return Err(AbiError::new(
                    AbiErrorCode::PermissionDenied,
                    format!(
                        "child grant exceeds parent authority for {:?}",
                        grant.capability
                    ),
                ));
            }
        }
        Ok(())
    }

    /// Resolves the tenant a spawned child runs under.
    ///
    /// `None` (or a value equal to the parent's own tenant) inherits the
    /// parent's tenant with no extra authority. Spawning a child under any
    /// other tenant — for a root parent as much as a tenant-scoped one —
    /// requires a `DelegateGrants` grant whose scope (root or tenant-scoped)
    /// admits the requested tenant: cross-tenant authority is a grant, not an
    /// accident of the parent's bootstrap tenant.
    pub(super) fn resolve_spawn_tenant(
        &self,
        process_id: ProcessId,
        requested: Option<&str>,
    ) -> std::result::Result<Option<String>, AbiError> {
        let own_tenant = self.process_tenant(process_id);
        let Some(requested) = requested else {
            return Ok(own_tenant);
        };
        if own_tenant.as_deref() == Some(requested) {
            return Ok(Some(requested.to_string()));
        }
        let authority = self.restore_process_authority(process_id).ok_or_else(|| {
            AbiError::new(
                AbiErrorCode::InvalidHandle,
                format!("unknown process authority {process_id}"),
            )
        })?;
        if grants_delegate_tenant(&authority.grants, requested) {
            return Ok(Some(requested.to_string()));
        }
        Err(AbiError::new(
            AbiErrorCode::PermissionDenied,
            format!("spawn tenant {requested:?} requires an in-scope DelegateGrants grant"),
        ))
    }
}

/// Returns whether a parent `Namespace` selector covers (is at least as broad
/// as) a child namespace: root is greater than any tenant, and a tenant
/// covers only itself.
fn namespace_covers(parent: &Namespace, child: &Namespace) -> bool {
    match parent {
        Namespace::Root => true,
        Namespace::Tenant(parent_tenant) => child == &Namespace::Tenant(parent_tenant.clone()),
    }
}

fn parent_grant_covers_child(parent: &CapabilityGrant, child: &CapabilityGrant) -> bool {
    if parent.capability != child.capability {
        return false;
    }

    parent.selectors.iter().all(|parent_selector| match parent_selector {
        ResourceSelector::Tenant(parent_tenant) => child.selectors.iter().any(|selector| {
            matches!(selector, ResourceSelector::Tenant(child_tenant) if child_tenant == parent_tenant)
        }),
        ResourceSelector::Namespace(parent_scope) => {
            child.selectors.iter().any(|selector| match selector {
                ResourceSelector::Namespace(child_scope) => {
                    namespace_covers(parent_scope, child_scope)
                }
                ResourceSelector::Tenant(child_tenant) => {
                    namespace_covers(parent_scope, &Namespace::Tenant(child_tenant.clone()))
                }
                _ => false,
            })
        }
        ResourceSelector::UriPrefix(parent_prefix) => child.selectors.iter().any(|selector| {
            match selector {
                ResourceSelector::UriPrefix(child_prefix) | ResourceSelector::Uri(child_prefix) => {
                    // Evaluate the parent with the same canonical, segment-aware
                    // matcher used at enforcement time, so delegation coverage
                    // cannot admit a child grant the parent would not match.
                    ResourceSelector::UriPrefix(parent_prefix.clone()).matches(&ScopeContext {
                        uri: Some(child_prefix.clone()),
                        ..ScopeContext::default()
                    })
                }
                _ => false,
            }
        }),
        ResourceSelector::Uri(parent_uri) => child.selectors.iter().any(|selector| {
            match selector {
                ResourceSelector::Uri(child_uri) => ResourceSelector::Uri(parent_uri.clone())
                    .matches(&ScopeContext {
                        uri: Some(child_uri.clone()),
                        ..ScopeContext::default()
                    }),
                _ => false,
            }
        }),
        ResourceSelector::Locality(parent_locality) => child.selectors.iter().any(|selector| {
            matches!(selector, ResourceSelector::Locality(child_locality) if parent_locality.matches(child_locality))
        }),
        ResourceSelector::ResourceClass(parent_class) => child.selectors.iter().any(|selector| {
            matches!(selector, ResourceSelector::ResourceClass(child_class) if child_class == parent_class)
        }),
        ResourceSelector::ExplicitResource(parent_identity) => {
            child.selectors.iter().any(|selector| {
                matches!(selector, ResourceSelector::ExplicitResource(child_identity) if child_identity == parent_identity)
            })
        }
        ResourceSelector::Children => child.selectors.iter().any(|selector| {
            matches!(selector, ResourceSelector::Children)
        }),
    })
}
