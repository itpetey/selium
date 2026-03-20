//! Scheduler guest - Workload placement and scheduling.
//!
//! Provides:
//! - Placement decision logic
//! - Capacity tracking
//! - Consensus coordination

use crate::error::GuestResult;
use crate::rpc::{Attribution, RpcCall, RpcServer};

#[derive(Debug, Clone)]
pub struct NodeCapacity {
    pub node_id: u64,
    pub cpu_units: u64,
    pub memory_bytes: u64,
    pub available: bool,
}

impl Default for NodeCapacity {
    fn default() -> Self {
        Self {
            node_id: 0,
            cpu_units: 100,
            memory_bytes: 1024 * 1024 * 1024,
            available: true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PlacementDecision {
    pub workload_id: String,
    pub node_id: u64,
    pub reason: String,
}

pub struct SchedulerGuest {
    capacities: Vec<NodeCapacity>,
    #[allow(dead_code)]
    server: RpcServer,
}

#[allow(dead_code)]
impl SchedulerGuest {
    pub fn new() -> Self {
        let server = RpcServer::new();
        Self {
            capacities: vec![NodeCapacity::default()],
            server,
        }
    }

    pub fn capacities(&self) -> &[NodeCapacity] {
        &self.capacities
    }

    pub fn find_best_node(&self, required_cpu: u64, required_mem: u64) -> Option<u64> {
        self.capacities
            .iter()
            .find(|c| c.available && c.cpu_units >= required_cpu && c.memory_bytes >= required_mem)
            .map(|c| c.node_id)
    }

    #[allow(unused_variables)]
    fn handle_place_workload(call: RpcCall, _attr: Attribution) -> GuestResult<Vec<u8>> {
        Ok(vec![])
    }

    #[allow(unused_variables)]
    fn handle_update_capacity(call: RpcCall, _attr: Attribution) -> GuestResult<Vec<u8>> {
        Ok(vec![])
    }
}

impl Default for SchedulerGuest {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scheduler_guest_new() {
        let guest = SchedulerGuest::new();
        assert_eq!(guest.capacities().len(), 1);
    }

    #[test]
    fn test_find_best_node() {
        let guest = SchedulerGuest::new();
        let node_id = guest.find_best_node(10, 1024);
        assert!(node_id.is_some());
    }

    #[test]
    fn test_find_best_node_unavailable() {
        let mut guest = SchedulerGuest::new();
        guest.capacities[0].available = false;
        let node_id = guest.find_best_node(10, 1024);
        assert!(node_id.is_none());
    }
}
