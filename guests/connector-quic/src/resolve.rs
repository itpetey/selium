//! SNI-based discovery route resolution with caching.
//!
//! The connector holds no routing table: routes live in discovery as
//! `sel-quic://<name>` entries registered by app guests via
//! [`QuicServe::bind`](selium_guest::net::quic::QuicServe::bind). A connection
//! is routed once, from the QUIC handshake's server name (SNI); every stream
//! on the connection then goes to that resolved guest. The resolver caches the
//! lookup and evicts on attach failure, mirroring the HTTP connector's
//! `RouteResolver`.

use std::{collections::HashMap, sync::Arc};

use selium_abi::uri;
use selium_guest::{Context, net::quic::QUIC_SCHEME};

/// Shared handle to the SNI route resolver, cloned into each connection task.
pub type ResolverHandle = Arc<tokio::sync::Mutex<RouteResolver>>;

/// Resolves a QUIC server name (SNI) to a serving channel via discovery.
pub struct RouteResolver {
    ctx: Option<Context>,
    cache: HashMap<String, CachedRoute>,
}

#[derive(Clone)]
struct CachedRoute {
    target: selium_abi::ResourceTarget,
    _created_at_ms: u64,
}

/// Route resolution failures.
#[derive(Debug)]
pub enum ResolveError {
    /// No registration matches the presented server name.
    NotFound,
}

impl RouteResolver {
    /// Creates a resolver backed by the connector's discovery context.
    pub fn new(ctx: Context) -> Self {
        Self {
            ctx: Some(ctx),
            cache: HashMap::new(),
        }
    }

    /// Evicts a cached route entry, forcing re-resolution on the next
    /// connection for the same name. Called on channel-attach failure.
    pub fn evict(&mut self, name: &str) {
        self.cache.remove(name);
    }

    /// Returns whether a route is cached for the given name.
    /// Test utility — not for production use.
    pub fn is_cached(&self, name: &str) -> bool {
        self.cache.contains_key(name)
    }

    /// Creates an empty resolver with no context and no routes.
    /// Test utility.
    pub fn empty() -> Self {
        Self {
            ctx: None,
            cache: HashMap::new(),
        }
    }

    /// Creates a resolver with a pre-populated cache entry — bypasses
    /// discovery lookup so tests can exercise cache semantics without a
    /// running discovery service.
    pub fn with_cached_route(name: &str, target: selium_abi::ResourceTarget) -> Self {
        let mut cache = HashMap::new();
        cache.insert(
            name.to_string(),
            CachedRoute {
                target,
                _created_at_ms: 0,
            },
        );
        Self { ctx: None, cache }
    }

    /// Resolves the serving guest for a server name.
    ///
    /// The name is normalised (lowercased, trailing dot stripped) and matched
    /// against the registered `sel-quic://<name>` discovery URI. Resolution
    /// happens once per connection; the connector caches the result.
    pub async fn resolve(
        &mut self,
        server_name: &str,
    ) -> Result<selium_abi::ResourceTarget, ResolveError> {
        let name = normalize_sni(server_name);
        if let Some(route) = self.cache.get(&name) {
            return Ok(route.target.clone());
        }

        let Some(ref mut ctx) = self.ctx else {
            return Err(ResolveError::NotFound);
        };

        let discovery_uri = route_uri(&name);
        match ctx.lookup(&discovery_uri).await {
            Ok(Some(target)) => {
                self.cache.insert(
                    name,
                    CachedRoute {
                        target: target.clone(),
                        _created_at_ms: 0,
                    },
                );
                Ok(target)
            }
            Ok(None) => Err(ResolveError::NotFound),
            Err(e) => {
                tracing::warn!("quic-connector: discovery lookup failed for {discovery_uri}: {e}");
                Err(ResolveError::NotFound)
            }
        }
    }
}

/// Normalises a raw SNI server name: lowercased, trailing dot stripped.
fn normalize_sni(server_name: &str) -> String {
    uri::normalize_host(server_name)
}

/// Builds the `sel-quic://` discovery URI for a normalised server name.
fn route_uri(name: &str) -> String {
    uri::protocol_uri(QUIC_SCHEME, name, "")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_uri_builds_protocol_aware_uris() {
        assert_eq!(route_uri("example.com"), "sel-quic://example.com");
    }

    #[test]
    fn normalize_sni_lowercases_and_strips_trailing_dot() {
        assert_eq!(normalize_sni("Example.COM."), "example.com");
        assert_eq!(normalize_sni("example.com"), "example.com");
    }

    fn make_target(id: u64) -> selium_abi::ResourceTarget {
        selium_abi::ResourceTarget {
            uri: "sel-quic://example.com".to_string(),
            host_id: String::new(),
            resource_id: id,
            interface: None,
            tenant: None,
        }
    }

    #[test]
    fn resolver_evict_removes_cached_entry() {
        let mut resolver = RouteResolver::with_cached_route("example.com", make_target(42));
        assert!(resolver.is_cached("example.com"));
        resolver.evict("example.com");
        assert!(!resolver.is_cached("example.com"));
    }

    #[test]
    fn resolver_evict_of_nonexistent_entry_is_noop() {
        let mut resolver = RouteResolver::with_cached_route("example.com", make_target(42));
        assert!(resolver.is_cached("example.com"));
        resolver.evict("other.example");
        assert!(resolver.is_cached("example.com"));
    }

    #[test]
    fn resolver_cache_hit_returns_cached_target() {
        let target = make_target(42);
        let mut resolver = RouteResolver::with_cached_route("example.com", target);
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        let result = rt.block_on(resolver.resolve("Example.COM."));
        assert_eq!(result.expect("resolve").resource_id, 42);
    }

    #[test]
    fn resolver_cache_miss_without_context_returns_not_found() {
        let mut resolver = RouteResolver::empty();
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        let result = rt.block_on(resolver.resolve("example.com"));
        assert!(matches!(result, Err(ResolveError::NotFound)));
    }
}
