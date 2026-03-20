//! Supervisor guest - Health monitoring and restart management.
//!
//! Provides:
//! - Health monitoring
//! - Restart policies (immediate, backoff)
//! - Scheduler coordination

use selium_guest_runtime::{Attribution, GuestResult, RpcCall, RpcServer};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RestartPolicy {
    Always,
    #[default]
    OnFailure,
    Never,
}

#[derive(Debug, Clone)]
pub struct HealthStatus {
    pub healthy: bool,
    pub restart_count: u32,
    pub last_restart: Option<u64>,
}

impl Default for HealthStatus {
    fn default() -> Self {
        Self {
            healthy: true,
            restart_count: 0,
            last_restart: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ManagedProcess {
    pub id: String,
    pub policy: RestartPolicy,
    pub health: HealthStatus,
}

pub struct SupervisorGuest {
    processes: HashMap<String, ManagedProcess>,
    #[allow(dead_code)]
    server: RpcServer,
}

#[allow(dead_code)]
impl SupervisorGuest {
    pub fn new() -> Self {
        let server = RpcServer::new();
        Self {
            processes: HashMap::new(),
            server,
        }
    }

    pub fn register_process(&mut self, id: String, policy: RestartPolicy) {
        self.processes.insert(
            id.clone(),
            ManagedProcess {
                id,
                policy,
                health: HealthStatus::default(),
            },
        );
    }

    pub fn report_health(&mut self, id: &str, healthy: bool) {
        if let Some(process) = self.processes.get_mut(id) {
            process.health.healthy = healthy;
        }
    }

    pub fn get_status(&self, id: &str) -> Option<&HealthStatus> {
        self.processes.get(id).map(|p| &p.health)
    }

    pub fn should_restart(&self, id: &str) -> bool {
        self.processes
            .get(id)
            .is_some_and(|p| !p.health.healthy && p.policy != RestartPolicy::Never)
    }

    #[allow(unused_variables)]
    fn handle_report_health(call: RpcCall, _attr: Attribution) -> GuestResult<Vec<u8>> {
        Ok(vec![])
    }

    #[allow(unused_variables)]
    fn handle_get_status(call: RpcCall, _attr: Attribution) -> GuestResult<Vec<u8>> {
        Ok(vec![])
    }
}

impl Default for SupervisorGuest {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_supervisor_guest_new() {
        let guest = SupervisorGuest::new();
        assert!(guest.processes.is_empty());
    }

    #[test]
    fn test_register_process() {
        let mut guest = SupervisorGuest::new();
        guest.register_process("worker-1".to_string(), RestartPolicy::Always);
        assert!(guest.processes.contains_key("worker-1"));
    }

    #[test]
    fn test_report_health() {
        let mut guest = SupervisorGuest::new();
        guest.register_process("worker-1".to_string(), RestartPolicy::Always);
        guest.report_health("worker-1", false);
        assert!(!guest.get_status("worker-1").unwrap().healthy);
    }

    #[test]
    fn test_should_restart() {
        let mut guest = SupervisorGuest::new();
        guest.register_process("worker-1".to_string(), RestartPolicy::Always);
        guest.report_health("worker-1", false);
        assert!(guest.should_restart("worker-1"));
    }

    #[test]
    fn test_should_not_restart_never_policy() {
        let mut guest = SupervisorGuest::new();
        guest.register_process("worker-1".to_string(), RestartPolicy::Never);
        guest.report_health("worker-1", false);
        assert!(!guest.should_restart("worker-1"));
    }
}
