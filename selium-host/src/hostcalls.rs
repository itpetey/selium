//! Hostcall dispatch mechanism.
//!
//! Hostcalls are the interface between guests and the host kernel.
//! Each hostcall has a version and deprecation status.

use parking_lot::RwLock;
use std::collections::HashMap;

use crate::kernel::Capability;

/// Current host version for deprecation checks.
pub const HOST_VERSION: u32 = 1;

/// A registered hostcall with version and deprecation info.
pub struct HostcallVersion {
    pub name: String,
    pub version: u32,
    pub deprecated_in: Option<u32>,
    pub removed_in: Option<u32>,
}

/// Information about a deprecated hostcall.
#[derive(Debug, Clone)]
pub struct DeprecatedHostcall {
    pub name: String,
    pub deprecated_in: u32,
    pub removed_in: u32,
}

/// Dispatcher for hostcalls with versioning support.
pub struct HostcallDispatcher {
    hostcalls: RwLock<HashMap<String, HostcallVersion>>,
}

impl HostcallDispatcher {
    /// Create a new hostcall dispatcher.
    pub fn new() -> Self {
        Self {
            hostcalls: RwLock::new(HashMap::new()),
        }
    }

    /// Register a hostcall.
    pub fn register(
        &self,
        name: &str,
        version: u32,
        deprecated_in: Option<u32>,
        removed_in: Option<u32>,
    ) {
        let mut hc = self.hostcalls.write();
        hc.insert(
            name.to_string(),
            HostcallVersion {
                name: name.to_string(),
                version,
                deprecated_in,
                removed_in,
            },
        );
    }

    /// Check if a hostcall is deprecated and return deprecation info if so.
    pub fn check_deprecation(&self, name: &str) -> Option<DeprecatedHostcall> {
        let hc = self.hostcalls.read();
        let version = hc.get(name)?;

        match version.deprecated_in {
            Some(deprecated_in) if deprecated_in <= HOST_VERSION => Some(DeprecatedHostcall {
                name: version.name.clone(),
                deprecated_in,
                removed_in: version.removed_in.unwrap_or(deprecated_in + 2),
            }),
            _ => None,
        }
    }

    /// Check if a hostcall is removed.
    pub fn is_removed(&self, name: &str) -> bool {
        let hc = self.hostcalls.read();
        if let Some(version) = hc.get(name) {
            if let Some(removed_in) = version.removed_in {
                return removed_in <= HOST_VERSION;
            }
        }
        false
    }

    /// Get deprecation warning message if applicable.
    pub fn deprecation_warning(&self, name: &str) -> Option<String> {
        let deprecated = self.check_deprecation(name)?;
        Some(format!(
            "DEPRECATED: {} will be removed in version {}",
            name, deprecated.removed_in
        ))
    }
}

impl Default for HostcallDispatcher {
    fn default() -> Self {
        Self::new()
    }
}

impl Capability for HostcallDispatcher {
    fn name(&self) -> &'static str {
        "selium::hostcalls"
    }
}
