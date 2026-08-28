//! Tier-1 discovery registration for the runtime.
//!
//! The runtime publishes every allocated resource as a volatile event on the
//! runtime→discovery pub/sub feed. This module provides the URI generation
//! logic; durable registration state lives in the discovery guest, not here.

use selium_abi::uri::PROC_URI_PREFIX;
use selium_abi::{ProcessId, ResourceKind, uri};

/// Generates the URIs to register for a given allocation.
///
/// Always returns `sel://_sys/proc/<process_id>/regions/<region_id>`.
/// If the purpose maps to a known alias, also returns
/// `sel://_sys/proc/<process_id>/<alias>`.
pub fn registration_uris(
    process_id: ProcessId,
    region_id: u64,
    purpose: ResourceKind,
) -> Vec<String> {
    vec![
        format!("{PROC_URI_PREFIX}{process_id}/regions/{region_id}"),
        format!("{PROC_URI_PREFIX}{process_id}/{}", purpose_alias(purpose)),
    ]
}

/// Generates the tier-1 registration URI for a host connection queue created
/// by `HostQueueCreate`. Queues are first-class resources so guests can
/// register routes (e.g. HTTP routes) whose target is a listener queue and
/// still pass discovery's ownership validation.
pub fn queue_registration_uri(process_id: ProcessId, queue_id: u64) -> String {
    format!("{PROC_URI_PREFIX}{process_id}/queues/{queue_id}")
}

/// Returns the protocol handler registration URI for a scheme.
pub fn handler_registration_uri(scheme: &str) -> String {
    uri::handler_uri(scheme)
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
            vec!["sel://_sys/proc/42/regions/7", "sel://_sys/proc/42/shm"]
        );
    }

    #[test]
    fn registration_uris_includes_log_alias() {
        let uris = registration_uris(42, 7, ResourceKind::LogChannel);
        assert_eq!(
            uris,
            vec!["sel://_sys/proc/42/regions/7", "sel://_sys/proc/42/logs"]
        );
    }

    #[test]
    fn registration_uris_includes_table_alias() {
        let uris = registration_uris(99, 3, ResourceKind::LiveTable);
        assert_eq!(
            uris,
            vec!["sel://_sys/proc/99/regions/3", "sel://_sys/proc/99/tables"]
        );
    }

    #[test]
    fn queue_registration_uri_is_under_proc_namespace() {
        assert_eq!(queue_registration_uri(42, 7), "sel://_sys/proc/42/queues/7");
    }

    #[test]
    fn handler_registration_uri_is_under_reserved_namespace() {
        assert_eq!(
            handler_registration_uri("sel-http"),
            "sel://_sys/handlers/sel-http"
        );
    }
}
