//! Discovery-based route resolution with caching.
//!
//! The connector holds no routing table: routes live in discovery as
//! live-table entries registered by app guests. The resolver caches
//! lookups per connection-worker and evicts on attach failure, so a stale
//! entry costs one failed request and forces a fresh lookup.

use std::{collections::HashMap, sync::Arc};

use selium_guest::Context;

/// Test support: re-exports helpers for integration tests in `tests/`.
/// Test utilities — not for production use.
pub mod test_support {
    pub use super::RouteResolver;
}

/// Shared handle to the route resolver, cloned into each connection task.
///
/// The cache is shared across all connections on the listener; lookups
/// are serialised briefly on the mutex, and cache hits avoid discovery
/// round-trips entirely.
pub type ResolverHandle = Arc<tokio::sync::Mutex<RouteResolver>>;

/// Resolves Host + path to a serving channel via discovery lookups.
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
    /// No registration matches the request's Host/path.
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
    /// request for the same host+path. Called on session-attach failure.
    pub fn evict(&mut self, host: &str, path: &str) {
        let cache_key = format!("{}:{}", host, path);
        self.cache.remove(&cache_key);
    }

    /// Returns whether a route is cached for the given host+path.
    /// Test utility — not for production use.
    pub fn is_cached(&self, host: &str, path: &str) -> bool {
        let cache_key = format!("{}:{}", host, path);
        self.cache.contains_key(&cache_key)
    }

    /// Creates a RouteResolver with a pre-populated cache entry.
    /// Test utility — bypasses discovery lookup so tests can exercise cache
    /// semantics without a running discovery service.
    pub fn with_cached_route(host: &str, path: &str, target: selium_abi::ResourceTarget) -> Self {
        let mut cache = HashMap::new();
        let cache_key = format!("{}:{}", host, path);
        cache.insert(
            cache_key,
            CachedRoute {
                target,
                _created_at_ms: 0,
            },
        );
        Self { ctx: None, cache }
    }

    /// Creates an empty resolver with no context and no routes.
    /// Test utility.
    pub fn empty() -> Self {
        Self {
            ctx: None,
            cache: HashMap::new(),
        }
    }

    /// Creates a resolver with several pre-populated cache entries,
    /// keyed by path for one host. Test utility.
    pub fn with_routes(host: &str, routes: HashMap<String, selium_abi::ResourceTarget>) -> Self {
        let mut cache = HashMap::new();
        for (path, target) in routes {
            let cache_key = format!("{}:{}", host, path);
            cache.insert(
                cache_key,
                CachedRoute {
                    target,
                    _created_at_ms: 0,
                },
            );
        }
        Self { ctx: None, cache }
    }

    /// Resolves the serving target for a Host + path pair.
    ///
    /// Tries the exact discovery URI first, then each parent subtree
    /// (longest prefix first), then the host root — matching app guests'
    /// registered URI subtrees.
    pub async fn resolve(
        &mut self,
        host: &str,
        path: &str,
    ) -> Result<selium_abi::ResourceTarget, ResolveError> {
        let cache_key = format!("{}:{}", host, path);
        if let Some(route) = self.cache.get(&cache_key) {
            return Ok(route.target.clone());
        }

        let Some(ref mut ctx) = self.ctx else {
            return Err(ResolveError::NotFound);
        };

        let clean_path = path.trim_start_matches('/').trim_end_matches('/');
        let discovery_uri = if clean_path.is_empty() {
            format!("sel://{}", host)
        } else {
            format!("sel://{}/{}", host, clean_path)
        };

        match ctx.lookup(&discovery_uri).await {
            Ok(Some(target)) => {
                self.cache.insert(
                    cache_key,
                    CachedRoute {
                        target: target.clone(),
                        _created_at_ms: 0,
                    },
                );
                Ok(target)
            }
            Ok(None) => self.resolve_parent(host, path).await,
            Err(e) => {
                tracing::warn!("discovery lookup failed for {discovery_uri}: {e}");
                Err(ResolveError::NotFound)
            }
        }
    }

    async fn resolve_parent(
        &mut self,
        host: &str,
        path: &str,
    ) -> Result<selium_abi::ResourceTarget, ResolveError> {
        let Some(ref mut ctx) = self.ctx else {
            return Err(ResolveError::NotFound);
        };

        let segments: Vec<&str> = path
            .trim_matches('/')
            .split('/')
            .filter(|s| !s.is_empty())
            .collect();

        for len in (1..=segments.len()).rev() {
            let prefix = segments
                .iter()
                .take(len)
                .copied()
                .collect::<Vec<_>>()
                .join("/");
            let uri = format!("sel://{}/{}", host, prefix);

            match ctx.lookup(&uri).await {
                Ok(Some(target)) => {
                    let orig_key = format!("{}:{}", host, path);
                    self.cache.insert(
                        orig_key,
                        CachedRoute {
                            target: target.clone(),
                            _created_at_ms: 0,
                        },
                    );
                    return Ok(target);
                }
                Ok(None) => continue,
                Err(e) => {
                    tracing::warn!("discovery lookup failed for {uri}: {e}");
                    continue;
                }
            }
        }

        let root_uri = format!("sel://{}", host);
        match ctx.lookup(&root_uri).await {
            Ok(Some(target)) => {
                let orig_key = format!("{}:{}", host, path);
                self.cache.insert(
                    orig_key,
                    CachedRoute {
                        target: target.clone(),
                        _created_at_ms: 0,
                    },
                );
                Ok(target)
            }
            _ => Err(ResolveError::NotFound),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_target(id: u64) -> selium_abi::ResourceTarget {
        selium_abi::ResourceTarget {
            uri: "sel://example.com/test".to_string(),
            host_id: String::new(),
            resource_id: id,
            interface: None,
            tenant: None,
        }
    }

    #[test]
    fn route_resolver_evict_removes_cached_entry() {
        let target = make_target(42);
        let mut resolver = RouteResolver::with_cached_route("example.com", "/test", target);

        assert!(resolver.is_cached("example.com", "/test"));
        resolver.evict("example.com", "/test");
        assert!(!resolver.is_cached("example.com", "/test"));
    }

    #[test]
    fn route_resolver_evict_of_nonexistent_entry_is_noop() {
        let target = make_target(42);
        let mut resolver = RouteResolver::with_cached_route("example.com", "/api", target);

        assert!(resolver.is_cached("example.com", "/api"));
        assert!(!resolver.is_cached("example.com", "/other"));

        resolver.evict("example.com", "/other");
        assert!(resolver.is_cached("example.com", "/api"));

        resolver.evict("example.com", "/api");
        assert!(!resolver.is_cached("example.com", "/api"));
    }

    #[test]
    fn route_resolver_cache_hit_returns_cached_target() {
        let target = make_target(42);
        let mut resolver = RouteResolver::with_cached_route("example.com", "/test", target);

        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        let result = rt.block_on(resolver.resolve("example.com", "/test"));
        assert!(result.is_ok());
        assert_eq!(result.unwrap().resource_id, 42);
    }

    #[test]
    fn route_resolver_cache_miss_without_context_returns_not_found() {
        let mut resolver =
            RouteResolver::with_cached_route("example.com", "/cached-only", make_target(7));

        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        let result = rt.block_on(resolver.resolve("example.com", "/not-cached"));
        assert!(matches!(result, Err(ResolveError::NotFound)));
    }

    #[test]
    fn route_resolver_stale_entry_not_reused_after_eviction() {
        let target = make_target(42);
        let mut resolver = RouteResolver::with_cached_route("example.com", "/test", target);

        resolver.evict("example.com", "/test");
        assert!(!resolver.is_cached("example.com", "/test"));

        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        let result = rt.block_on(resolver.resolve("example.com", "/test"));
        assert!(matches!(result, Err(ResolveError::NotFound)));
    }
}
