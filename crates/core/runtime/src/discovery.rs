//! Tier-1 discovery registration for the runtime.
//!
//! The runtime automatically registers every allocated resource in the
//! discovery service. This module provides the URI generation and
//! tracking logic.

use selium_abi::{ProcessId, ResourceKind};

/// Returns the purpose-specific URI alias suffix for a `ResourceKind`, if any.
///
/// Initially:
/// - `LogChannel` → `logs`
/// - `LiveTable` → `tables` (name suffix deferred)
/// - `RpcRing` → `rpc` (name suffix deferred)
/// - Others → `None` (no alias)
pub fn purpose_alias(purpose: ResourceKind) -> Option<&'static str> {
    match purpose {
        ResourceKind::LogChannel => Some("logs"),
        ResourceKind::LiveTable => Some("tables"),
        ResourceKind::RpcRing => Some("rpc"),
        ResourceKind::PubSubTopic => Some("pubsub"),
        ResourceKind::NetworkBuffer => None,
        ResourceKind::DurableLog => None,
        ResourceKind::BlobStore => None,
        ResourceKind::SharedMemory => None,
    }
}

/// Generates the URIs to register for a given allocation.
///
/// Always returns `sel://process/<process_id>/regions/<region_id>`.
/// If the purpose maps to a known alias, also returns
/// `sel://process/<process_id>/<alias>`.
pub fn registration_uris(process_id: ProcessId, region_id: u64, purpose: ResourceKind) -> Vec<String> {
    let mut uris = vec![format!("sel://process/{process_id}/regions/{region_id}")];
    if let Some(alias) = purpose_alias(purpose) {
        uris.push(format!("sel://process/{process_id}/{alias}"));
    }
    uris
}

/// Records URIs registered for a process.
pub fn record_uris(
    process_discovery_uris: &parking_lot::Mutex<std::collections::HashMap<ProcessId, Vec<String>>>,
    process_id: ProcessId,
    uris: Vec<String>,
) {
    process_discovery_uris
        .lock()
        .entry(process_id)
        .or_default()
        .extend(uris);
}

/// Returns and removes all URIs registered for a process.
pub fn take_uris(
    process_discovery_uris: &parking_lot::Mutex<std::collections::HashMap<ProcessId, Vec<String>>>,
    process_id: ProcessId,
) -> Vec<String> {
    process_discovery_uris
        .lock()
        .remove(&process_id)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registration_uris_always_includes_region() {
        let uris = registration_uris(42, 7, ResourceKind::SharedMemory);
        assert_eq!(uris, vec!["sel://process/42/regions/7"]);
    }

    #[test]
    fn registration_uris_includes_log_alias() {
        let uris = registration_uris(42, 7, ResourceKind::LogChannel);
        assert_eq!(
            uris,
            vec![
                "sel://process/42/regions/7",
                "sel://process/42/logs",
            ]
        );
    }

    #[test]
    fn registration_uris_includes_table_alias() {
        let uris = registration_uris(99, 3, ResourceKind::LiveTable);
        assert_eq!(
            uris,
            vec![
                "sel://process/99/regions/3",
                "sel://process/99/tables",
            ]
        );
    }

    #[test]
    fn purpose_alias_returns_expected_values() {
        assert_eq!(purpose_alias(ResourceKind::LogChannel), Some("logs"));
        assert_eq!(purpose_alias(ResourceKind::LiveTable), Some("tables"));
        assert_eq!(purpose_alias(ResourceKind::RpcRing), Some("rpc"));
        assert_eq!(purpose_alias(ResourceKind::PubSubTopic), Some("pubsub"));
        assert_eq!(purpose_alias(ResourceKind::SharedMemory), None);
        assert_eq!(purpose_alias(ResourceKind::NetworkBuffer), None);
    }

    #[test]
    fn record_and_take_uris() {
        let uris_map = parking_lot::Mutex::new(std::collections::HashMap::new());
        record_uris(&uris_map, 42, vec!["sel://process/42/regions/7".to_string()]);
        record_uris(&uris_map, 42, vec!["sel://process/42/logs".to_string()]);

        let taken = take_uris(&uris_map, 42);
        assert_eq!(taken.len(), 2);
        assert!(taken.contains(&"sel://process/42/regions/7".to_string()));
        assert!(taken.contains(&"sel://process/42/logs".to_string()));

        // After take, the process should have no URIs.
        let taken_again = take_uris(&uris_map, 42);
        assert!(taken_again.is_empty());
    }
}
