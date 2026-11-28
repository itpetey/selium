//! Network endpoint parsing and prefix matching.

/// Parsed components of a network endpoint URI (`tcp://` or `udp://`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkEndpoint {
    /// Scheme: `"tcp"` or `"udp"`.
    pub scheme: String,
    /// Host: lowercase, trailing dot stripped, IPv6 bracketed.
    pub host: String,
    /// Port: explicit numeric string.
    pub port: String,
}

impl NetworkEndpoint {
    /// Parse a network endpoint URI like `tcp://127.0.0.1:8080` or `udp://[::1]:53`.
    pub fn parse(uri: &str) -> Option<Self> {
        let (scheme, rest) = uri.split_once("://")?;
        if scheme != "tcp" && scheme != "udp" {
            return None;
        }
        // Port is after the last colon.
        let (host_part, port) = rest.rsplit_once(':')?;

        // Validate port: empty rejected; comma-separated digits accepted; bare * accepted.
        if port.is_empty() {
            return None;
        }
        if port == "*" {
            // ok
        } else if !port
            .chars()
            .all(|c| c.is_ascii_digit() || c == ',' || c == ' ')
        {
            return None;
        }

        let host = host_part.to_lowercase();
        let host = host.strip_suffix('.').unwrap_or(&host).to_string();

        // Host must be non-empty.
        if host.is_empty() {
            return None;
        }

        Some(Self {
            scheme: scheme.to_string(),
            host,
            port: port.to_string(),
        })
    }

    /// Returns whether `grant` (the grant-side endpoint) matches `context`
    /// (the runtime-side endpoint) using component-aware semantics.
    pub fn prefix_matches(grant: &NetworkEndpoint, context: &NetworkEndpoint) -> bool {
        // Scheme must match exactly.
        if grant.scheme != context.scheme {
            return false;
        }
        // Host: exact or wildcard label boundary.
        if !Self::host_matches(&grant.host, &context.host) {
            return false;
        }
        // Port: exact, *, or comma-separated list.
        Self::port_matches(&grant.port, &context.port)
    }

    fn host_matches(grant_host: &str, context_host: &str) -> bool {
        if grant_host == context_host {
            return true;
        }
        // Wildcard: `*.example.com` matches `foo.example.com`, `bar.foo.example.com`, etc.
        if let Some(suffix) = grant_host.strip_prefix("*.") {
            // Must match at a label boundary and the context host must be longer.
            if context_host.ends_with(suffix) && context_host.len() > suffix.len() {
                // Ensure the character before the suffix is a dot (label boundary).
                let dot_pos = context_host.len() - suffix.len() - 1;
                return context_host.as_bytes().get(dot_pos) == Some(&b'.');
            }
        }
        false
    }

    fn port_matches(grant_port: &str, context_port: &str) -> bool {
        if grant_port == "*" {
            return true;
        }
        // Comma-separated list.
        grant_port.split(',').any(|p| p.trim() == context_port)
    }
}
