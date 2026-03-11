//! Usage accounting callbacks exposed by the runtime host.

use std::fmt::Debug;

/// Runtime hook used to attribute raw usage counters to one guest process.
pub trait UsageRecorder: Debug + Send + Sync {
    /// Record bytes received from the network into the guest boundary.
    fn record_network_ingress(&self, bytes: u64);

    /// Record bytes sent from the guest over the network boundary.
    fn record_network_egress(&self, bytes: u64);

    /// Record bytes read from runtime-managed storage.
    fn record_storage_read(&self, bytes: u64);

    /// Record bytes written to runtime-managed storage.
    fn record_storage_write(&self, bytes: u64);
}
