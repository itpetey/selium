//! Tier-1 discovery registration for the runtime.
//!
//! The runtime publishes every allocated resource as a volatile event on the
//! runtime→discovery pub/sub feed. This module provides the URI generation
//! logic; durable registration state lives in the discovery guest, not here.

use selium_abi::{ProcessId, ResourceKind};

/// Generates the URIs to register for a given allocation.
///
/// Always returns `sel://process/<process_id>/regions/<region_id>`.
/// If the purpose maps to a known alias, also returns
/// `sel://process/<process_id>/<alias>`.
pub fn registration_uris(
    process_id: ProcessId,
    region_id: u64,
    purpose: ResourceKind,
) -> Vec<String> {
    vec![
        format!("sel://process/{process_id}/regions/{region_id}"),
        format!("sel://process/{process_id}/{}", purpose_alias(purpose)),
    ]
}

/// Returns the purpose-specific URI alias suffix for a `ResourceKind`, if any.
///
/// Initially:
/// - `LogChannel` → `logs`
/// - `LiveTable` → `tables` (name suffix deferred)
/// - `RpcRing` → `rpc` (name suffix deferred)
/// - `PubSubTopic` → `pubsub`
/// - Others → `None` (no alias)
fn purpose_alias(purpose: ResourceKind) -> &'static str {
    match purpose {
        ResourceKind::LogChannel => "logs",
        ResourceKind::LiveTable => "tables",
        ResourceKind::RpcRing => "rpc",
        ResourceKind::PubSubTopic => "pubsub",
        ResourceKind::NetworkBuffer => "net",
        ResourceKind::DurableLog => "retained",
        ResourceKind::BlobStore => "blobs",
        ResourceKind::SharedMemory => "shm",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registration_uris_always_includes_region() {
        let uris = registration_uris(42, 7, ResourceKind::SharedMemory);
        assert_eq!(
            uris,
            vec!["sel://process/42/regions/7", "sel://process/42/shm"]
        );
    }

    #[test]
    fn registration_uris_includes_log_alias() {
        let uris = registration_uris(42, 7, ResourceKind::LogChannel);
        assert_eq!(
            uris,
            vec!["sel://process/42/regions/7", "sel://process/42/logs",]
        );
    }

    #[test]
    fn registration_uris_includes_table_alias() {
        let uris = registration_uris(99, 3, ResourceKind::LiveTable);
        assert_eq!(
            uris,
            vec!["sel://process/99/regions/3", "sel://process/99/tables",]
        );
    }
}
