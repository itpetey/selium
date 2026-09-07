//! Selium URI grammar and matching.
//!
//! Selium uses two addressing surfaces:
//!
//! - `sel://<tenant>/<type>/<id>` — the internal typed schema. `<tenant>` is
//!   the URI authority (empty for the root/system tenant), `<type>` is a
//!   lowercased [`ResourceClass`] segment (`proc`, `region`, `queue`, …), and
//!   `<id>` is the resource's numeric identity. A single non-class segment
//!   (`sel://<tenant>/<name>`) names a leaf alias.
//! - External names — always a distinct, opaque key such as
//!   `https://acme.com/path/` or a bare hostname for server-name-only
//!   transport. Discovery stores and matches these exactly without
//!   interpreting their scheme or path.
//!
//! The root tenant (`sel:///…`) is **reserved**: only the runtime and system
//! guests (Tier-1) may register inside it. Guests registering over RPC
//! (Tier-2) are rejected.
//!
//! This module is the single source of truth for these rules, shared by the
//! runtime (URI generation), the discovery guest (validation), and the
//! connectors (external-name normalisation).

use super::ResourceClass;

/// The scheme of internal `sel` URIs.
pub const SEL_PREFIX: &str = "sel://";

/// Builds the canonical external key for a server-name-only protocol (a bare
/// normalised hostname).
pub fn bare_external_name(name: &str) -> String {
    normalize_host(name)
}

/// Builds the canonical external key for an HTTP route: `https://<host>/<path>`.
///
/// `host` and `path` are normalised so the connector's lookup and the app
/// guest's registration agree on one key.
pub fn https_external_name(host: &str, path: &str) -> String {
    normalize_external_name(&format!("https://{host}/{path}"))
}

/// Returns whether `segment` names a resource class (a reserved type segment).
pub fn is_class_segment(segment: &str) -> bool {
    ResourceClass::from_uri_segment(segment).is_some()
}

/// Returns whether `uri` addresses the root/system tenant (`sel:///…`).
pub fn is_root_uri(uri: &str) -> bool {
    matches!(parse_sel(uri), Some((tenant, _)) if tenant.is_empty())
}

/// Normalises an external address to its canonical opaque key.
///
/// Scheme and authority are lowercased, the authority's trailing dot and
/// numeric port are stripped, and trailing path slashes are removed. A value
/// without a `://` is treated as a bare hostname (server-name-only transport)
/// and host-normalised.
pub fn normalize_external_name(name: &str) -> String {
    let trimmed = name.trim_end_matches('/');
    let Some((scheme, rest)) = trimmed.split_once("://") else {
        return normalize_host(trimmed);
    };
    let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
    let scheme = scheme.to_ascii_lowercase();
    let authority = normalize_host(authority);
    let path = path.trim_matches('/');
    if path.is_empty() {
        format!("{scheme}://{authority}")
    } else {
        format!("{scheme}://{authority}/{path}")
    }
}

/// Normalises a host/authority value: lowercased, trailing dot stripped, and
/// a numeric `:port` suffix removed.
pub fn normalize_host(host: &str) -> String {
    let host = host.trim().to_ascii_lowercase();
    let host = host.strip_suffix('.').unwrap_or(&host);
    if let Some((name, port)) = host.rsplit_once(':')
        && port.chars().all(|c| c.is_ascii_digit())
        && !name.is_empty()
    {
        return name.to_string();
    }
    host.to_string()
}

/// Parses a leaf alias `sel://<tenant>/<name>` into `(tenant, name)`.
/// A class noun is reserved, so a name shadowing a type segment is rejected.
pub fn parse_alias(uri: &str) -> Option<(&str, &str)> {
    let (tenant, path) = parse_sel(uri)?;
    if path.is_empty() || path.contains('/') {
        return None;
    }
    if is_class_segment(path) {
        return None;
    }
    Some((tenant, path))
}

/// Parses a `sel://` URI into its `(tenant, path)` components.
///
/// `tenant` is the authority (empty for the root/system tenant); `path` is
/// the remainder with leading and trailing `/` stripped (it may be empty or
/// contain `/`-separated segments). Returns `None` for external names and
/// other non-`sel` URIs.
pub fn parse_sel(uri: &str) -> Option<(&str, &str)> {
    let rest = uri.strip_prefix(SEL_PREFIX)?;
    let (tenant, path) = rest.split_once('/').unwrap_or((rest, ""));
    Some((tenant, path.trim_matches('/')))
}

/// Parses a typed internal URI `sel://<tenant>/<type>/<id>` into
/// `(tenant, class, id)`. Returns `None` for aliases, root well-known paths,
/// and external names.
pub fn parse_typed(uri: &str) -> Option<(&str, ResourceClass, u64)> {
    let (tenant, path) = parse_sel(uri)?;
    let (class_seg, id_seg) = path.split_once('/')?;
    if id_seg.is_empty() || id_seg.contains('/') {
        return None;
    }
    let class = ResourceClass::from_uri_segment(class_seg)?;
    let id = id_seg.parse::<u64>().ok()?;
    Some((tenant, class, id))
}

/// Builds a typed resource URI: `sel://<tenant>/<type>/<id>`.
pub fn resource_uri(tenant: &str, class: ResourceClass, id: u64) -> String {
    format!("{SEL_PREFIX}{tenant}/{}/{id}", class.uri_segment())
}

/// Returns whether a `sel` path is a wildcard enumeration (`…/*`), and the
/// non-wildcard prefix of the path (everything before the `/*`).
pub fn wildcard_prefix(path: &str) -> Option<&str> {
    let stripped = path.trim_end_matches('/');
    let prefix = stripped.strip_suffix("*")?;
    let prefix = prefix.trim_end_matches('/');
    Some(prefix)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_sel_extracts_tenant_and_path() {
        assert_eq!(parse_sel("sel://tenant/app"), Some(("tenant", "app")));
        assert_eq!(parse_sel("sel://acme/region/7"), Some(("acme", "region/7")));
        assert_eq!(parse_sel("sel:///dns/resolve"), Some(("", "dns/resolve")));
        assert_eq!(parse_sel("sel:///proc/1"), Some(("", "proc/1")));
        assert_eq!(parse_sel("https://example.com/"), None);
        assert_eq!(parse_sel("example.com"), None);
    }

    #[test]
    fn root_uri_detection() {
        assert!(is_root_uri("sel:///dns/resolve"));
        assert!(is_root_uri("sel:///proc/1"));
        assert!(!is_root_uri("sel://acme/proc/1"));
        assert!(!is_root_uri("https://example.com/"));
    }

    #[test]
    fn resource_uri_uses_typed_segment() {
        assert_eq!(
            resource_uri("acme", ResourceClass::SharedRegion, 7),
            "sel://acme/region/7"
        );
        assert_eq!(
            resource_uri("acme", ResourceClass::Process, 42),
            "sel://acme/proc/42"
        );
        assert_eq!(
            resource_uri("acme", ResourceClass::HostQueue, 9),
            "sel://acme/queue/9"
        );
        assert_eq!(
            resource_uri("", ResourceClass::HostQueue, 9),
            "sel:///queue/9"
        );
    }

    #[test]
    fn parse_typed_recognises_typed_uris() {
        assert_eq!(
            parse_typed("sel://acme/region/7"),
            Some(("acme", ResourceClass::SharedRegion, 7))
        );
        assert_eq!(
            parse_typed("sel://acme/proc/42"),
            Some(("acme", ResourceClass::Process, 42))
        );
        assert_eq!(
            parse_typed("sel:///queue/9"),
            Some(("", ResourceClass::HostQueue, 9))
        );
        assert_eq!(parse_typed("sel://acme/proxy"), None);
        assert_eq!(parse_typed("sel:///dns/resolve"), None);
        assert_eq!(parse_typed("https://example.com/"), None);
    }

    #[test]
    fn parse_alias_recognises_leaf_names() {
        assert_eq!(parse_alias("sel://acme/proxy"), Some(("acme", "proxy")));
        assert_eq!(parse_alias("sel:///discovery"), Some(("", "discovery")));
        // Class nouns are reserved: an alias cannot shadow a type segment.
        assert_eq!(parse_alias("sel://acme/region"), None);
        assert_eq!(parse_alias("sel://acme/proc"), None);
        assert_eq!(parse_alias("sel://acme/region/7"), None);
        assert_eq!(parse_alias("https://example.com/"), None);
    }

    #[test]
    fn class_segment_detection() {
        assert!(is_class_segment("proc"));
        assert!(is_class_segment("region"));
        assert!(is_class_segment("queue"));
        assert!(!is_class_segment("proxy"));
        assert!(!is_class_segment(""));
    }

    #[test]
    fn wildcard_prefix_strips_star() {
        assert_eq!(wildcard_prefix("region/*"), Some("region"));
        assert_eq!(wildcard_prefix("region/*/"), Some("region"));
        assert_eq!(wildcard_prefix("*"), Some(""));
        assert_eq!(wildcard_prefix("region/7"), None);
    }

    #[test]
    fn normalize_external_name_is_canonical() {
        assert_eq!(
            normalize_external_name("https://Acme.com/path/"),
            "https://acme.com/path"
        );
        assert_eq!(
            normalize_external_name("https://acme.com"),
            "https://acme.com"
        );
        assert_eq!(
            normalize_external_name("https://acme.com:443/"),
            "https://acme.com"
        );
        assert_eq!(normalize_external_name("Example.COM."), "example.com");
        assert_eq!(
            normalize_external_name("QUIC://Example.com/"),
            "quic://example.com"
        );
    }

    #[test]
    fn external_name_builders_match_normalization() {
        assert_eq!(
            https_external_name("example.com", "api"),
            "https://example.com/api"
        );
        assert_eq!(
            https_external_name("example.com", ""),
            "https://example.com"
        );
        assert_eq!(bare_external_name("Example.COM."), "example.com");
    }
}
