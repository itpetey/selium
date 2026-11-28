//! Shared-page fast-path detection and wake kick accounting.
//!
//! A region takes the fast path only when every attacher's module advertises
//! the engine wait registry and emits atomic notifies. The host records one
//! vote per attacher; a region with any ineligible attacher keeps its kick.

use selium_abi::ProcessId;

use crate::Runtime;

impl Runtime {
    /// True when the shared-page fast path is active for `shared_id`: every
    /// attaching process's guest module is fast-path capable (shared memory
    /// declaration + atomic notify opcodes; see `module_probe`) and the
    /// engine advertises its per-region wait registry. Detection, not
    /// configuration — see the `shared-page-fastpath` capability spec.
    pub fn fast_path_region_active(&self, shared_id: u64) -> bool {
        let attachments = self.fast_path_attachments.lock().clone();
        region_fast_path_active(&attachments, shared_id)
    }

    /// Shared ids of the regions with active network outbound proxies (the
    /// kick targets), for observability and tests.
    pub fn network_wait_regions(&self) -> Vec<u64> {
        self.network_wait_keys
            .lock()
            .iter()
            .map(|(shared_id, _)| *shared_id)
            .collect()
    }

    /// Total guest→host transition kicks **delivered** since runtime
    /// creation (suppressed regions are not counted).
    pub fn kick_count(&self) -> u64 {
        self.kick_counts.lock().values().sum()
    }

    /// Guest→host transition kicks delivered to `shared_id`. Zero for a
    /// fast-path region means every wake was carried by the guest's atomic
    /// notify — the end-to-end fast-path assertion.
    pub fn region_kick_count(&self, shared_id: u64) -> u64 {
        self.kick_counts
            .lock()
            .get(&shared_id)
            .copied()
            .unwrap_or(0)
    }

    /// Records a guest module's fast-path capability, probed from its module
    /// bytes at spawn (see `module_probe`). Consumed at region attach.
    pub(crate) fn record_process_fastpath(&self, process_id: ProcessId, capable: bool) {
        self.process_fastpath.lock().insert(process_id, capable);
    }

    /// True when the process's guest module is fast-path capable. Unknown
    /// processes (never spawned, or already cleaned up) are not — the
    /// portable kick path is the safe default.
    pub(crate) fn process_fastpath_capable(&self, process_id: ProcessId) -> bool {
        self.process_fastpath
            .lock()
            .get(&process_id)
            .copied()
            .unwrap_or(false)
    }

    /// Records the per-attachment fast-path eligibility vote for
    /// `(region_id, process_id)` at attach time. `capable` combines the
    /// engine's registry support with the attaching process's module probe;
    /// an incapable attacher votes `false` so the region's fast path stays
    /// off until every attacher is capable.
    pub(crate) fn record_fast_path_attachment(
        &self,
        region_id: u64,
        process_id: ProcessId,
        capable: bool,
    ) {
        self.fast_path_attachments
            .lock()
            .entry(region_id)
            .or_default()
            .insert(process_id, capable);
    }

    /// Drops all fast-path eligibility for a destroyed region. Called when
    /// the region is freed: every attachment is force-detached, so every
    /// vote is stale.
    pub(crate) fn clear_fast_path_attachments(&self, region_id: u64) {
        self.fast_path_attachments.lock().remove(&region_id);
    }
}

/// True when the per-attachment eligibility map marks every attacher of
/// `shared_id` as fast-path capable (and at least one attacher exists). See
/// [`Runtime::fast_path_region_active`].
pub(super) fn region_fast_path_active(
    attachments: &std::collections::HashMap<u64, std::collections::HashMap<ProcessId, bool>>,
    shared_id: u64,
) -> bool {
    attachments
        .get(&shared_id)
        .is_some_and(|voters| !voters.is_empty() && voters.values().all(|capable| *capable))
}
