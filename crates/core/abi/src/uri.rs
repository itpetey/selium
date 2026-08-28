//! Fabric URI classification and matching.
//!
//! Selium uses a single `sel` URI family with two shapes:
//!
//! - `sel://<path>` — a generic namespace (plain name registration).
//! - `sel-<protocol>://<authority>/<path>` — a protocol-aware route whose
//!   scheme declares the handler that serves it (e.g. `sel-http://`).
//!
//! The `sel://_sys/` subtree is **reserved**: only the runtime may register
//! inside it (Tier 1). Guests registering over RPC (Tier 2) are rejected.
//!
//! This module is the single source of truth for these rules, shared by the
//! runtime (URI generation), the discovery guest (validation), and the
//! connectors (route construction and prefix matching).

/// Resolved/tier-1 namespace prefix. Anything under it is runtime-owned.
pub const RESERVED_URI_PREFIX: &str = "sel://_sys/";
/// Process-scoped tier-1 prefix: `sel://_sys/proc/<process-id>/...`.
pub const PROC_URI_PREFIX: &str = "sel://_sys/proc/";
/// Prefix under which protocol handlers register: `sel://_sys/handlers/<scheme>`.
pub const HANDLER_URI_PREFIX: &str = "sel://_sys/handlers/";

/// Returns the scheme portion of `uri` (`sel`, `sel-http`, …), if well-formed.
pub fn scheme_of(uri: &str) -> Option<&str> {
    let scheme = uri.split_once("://")?.0;
    (!scheme.is_empty()
        && scheme
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-'))
    .then_some(scheme)
}

/// Returns whether the URI falls inside the reserved tier-1 namespace.
pub fn is_reserved(uri: &str) -> bool {
    uri.starts_with(RESERVED_URI_PREFIX)
}

/// Returns whether `scheme` is a protocol-aware fanric scheme (`sel-<proto>`).
pub fn is_protocol_scheme(scheme: &str) -> bool {
    scheme.starts_with("sel-")
}

/// Returns the protocol scheme of a protocol-aware URI (`sel-http` for
/// `sel-http://…`), or `None` for generic `sel://` URIs.
pub fn protocol_scheme(uri: &str) -> Option<&str> {
    let scheme = scheme_of(uri)?;
    is_protocol_scheme(scheme).then_some(scheme)
}

/// Extracts the process id from a `sel://_sys/proc/<id>/...` URI, if present.
pub fn extract_process_id(uri: &str) -> Option<u64> {
    let rest = uri.strip_prefix(PROC_URI_PREFIX)?;
    let id_str = rest.split('/').next()?;
    id_str.parse().ok()
}

/// Returns the tier-1 registration URI for a protocol handler.
pub fn handler_uri(scheme: &str) -> String {
    format!("{HANDLER_URI_PREFIX}{scheme}")
}

/// Builds a protocol-aware URI from its parts, e.g.
/// `protocol_uri("sel-http", "example.com", "/api")` → `sel-http://example.com/api`.
pub fn protocol_uri(scheme: &str, authority: &str, path: &str) -> String {
    format!("{scheme}://{authority}{path}")
}

/// Normalises a `Host` header value: lowercased, trailing dot stripped, and
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

/// Returns whether `prefix` is a component-aware prefix of `uri`.
///
/// Component boundaries are honoured so that `sel-http://example.com/foo`
/// never matches `sel-http://example.com/foobar` (path segments) or
/// `sel-http://example.com` never matches `sel-http://example.com.evil`
/// (host label boundaries).
pub fn prefix_matches(prefix: &str, uri: &str) -> bool {
    if prefix == uri {
        return true;
    }
    let Some((prefix_scheme, prefix_rest)) = prefix.split_once("://") else {
        return false;
    };
    let Some((uri_scheme, uri_rest)) = uri.split_once("://") else {
        return false;
    };
    if prefix_scheme != uri_scheme {
        return false;
    }

    if is_protocol_scheme(prefix_scheme) {
        let (prefix_host, prefix_path) = split_host_path(prefix_rest);
        let (uri_host, uri_path) = split_host_path(uri_rest);
        host_matches(prefix_host, uri_host) && segment_prefix(prefix_path, uri_path)
    } else {
        segment_prefix(prefix_rest, uri_rest)
    }
}

/// Returns whether `prefix_host` matches `host` (exact or a `*.` wildcard at a
/// label boundary).
fn host_matches(prefix_host: &str, host: &str) -> bool {
    if prefix_host == host {
        return true;
    }
    let Some(suffix) = prefix_host.strip_prefix("*.") else {
        return false;
    };
    host.len() > suffix.len()
        && host.ends_with(suffix)
        && host.as_bytes().get(host.len() - suffix.len() - 1) == Some(&b'.')
}

/// Returns whether `prefix_path` is a prefix of `path` at a segment boundary.
/// Trailing slashes on the prefix are ignored so `/foo/` matches `/foo/bar`.
fn segment_prefix(prefix_path: &str, path: &str) -> bool {
    let prefix = prefix_path.trim_end_matches('/');
    if prefix.is_empty() {
        return true;
    }
    if prefix == path.trim_end_matches('/') {
        return true;
    }
    path.starts_with(prefix) && path.as_bytes().get(prefix.len()) == Some(&b'/')
}

/// Splits the authority+path portion of a URI into `(host, path)` where `path`
/// retains its leading `/` (or is empty when absent).
fn split_host_path(rest: &str) -> (&str, &str) {
    rest.split_once('/').unwrap_or((rest, ""))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scheme_of_parses_known_forms() {
        assert_eq!(scheme_of("sel://tenant/app"), Some("sel"));
        assert_eq!(scheme_of("sel-http://example.com/api"), Some("sel-http"));
        assert_eq!(scheme_of("not-a-uri"), None);
        assert_eq!(scheme_of("sel://"), Some("sel"));
    }

    #[test]
    fn reserved_namespace_detection() {
        assert!(is_reserved("sel://_sys/proc/42/regions/7"));
        assert!(is_reserved("sel://_sys/handlers/sel-http"));
        assert!(!is_reserved("sel://tenant/app"));
        assert!(!is_reserved("sel-http://example.com/"));
    }

    #[test]
    fn protocol_scheme_detection() {
        assert_eq!(
            protocol_scheme("sel-http://example.com/api"),
            Some("sel-http")
        );
        assert_eq!(protocol_scheme("sel://tenant/app"), None);
        assert_eq!(protocol_scheme("not-a-uri"), None);
    }

    #[test]
    fn extract_process_id_from_proc_uris() {
        assert_eq!(extract_process_id("sel://_sys/proc/42/regions/7"), Some(42));
        assert_eq!(extract_process_id("sel://_sys/proc/99/queues/3"), Some(99));
        assert_eq!(extract_process_id("sel://my-app/logs"), None);
        assert_eq!(extract_process_id("sel://_sys/handlers/sel-http"), None);
    }

    #[test]
    fn handler_uri_places_scheme_under_reserved_prefix() {
        assert_eq!(handler_uri("sel-http"), "sel://_sys/handlers/sel-http");
        assert!(is_reserved(&handler_uri("sel-http")));
    }

    #[test]
    fn normalize_host_lowercases_and_strips_port() {
        assert_eq!(normalize_host("Example.COM:443"), "example.com");
        assert_eq!(normalize_host("example.com"), "example.com");
        assert_eq!(normalize_host("example.com."), "example.com");
        // Non-numeric suffixes are kept (e.g. an odd header value).
        assert_eq!(normalize_host("example.com:https"), "example.com:https");
    }

    #[test]
    fn protocol_prefix_matching_honours_segment_boundaries() {
        assert!(prefix_matches(
            "sel-http://example.com/foo",
            "sel-http://example.com/foo/bar"
        ));
        assert!(!prefix_matches(
            "sel-http://example.com/foo",
            "sel-http://example.com/foobar"
        ));
        // Trailing slash on the prefix is equivalent.
        assert!(prefix_matches(
            "sel-http://example.com/foo/",
            "sel-http://example.com/foo/bar"
        ));
        // Scheme must match.
        assert!(!prefix_matches(
            "sel-http://example.com/",
            "sel-dns://example.com/"
        ));
    }

    #[test]
    fn protocol_prefix_matching_honours_host_label_boundaries() {
        assert!(prefix_matches(
            "sel-http://example.com",
            "sel-http://example.com/api"
        ));
        assert!(!prefix_matches(
            "sel-http://example.com",
            "sel-http://example.com.evil/api"
        ));
    }

    #[test]
    fn generic_prefix_matching_honours_segment_boundaries() {
        assert!(prefix_matches("sel://tenant/app/", "sel://tenant/app/api"));
        assert!(!prefix_matches("sel://tenant/app/", "sel://tenant/apple"));
        assert!(prefix_matches(
            "sel://tenant/app/",
            "sel://tenant/app/worker"
        ));
    }
}
