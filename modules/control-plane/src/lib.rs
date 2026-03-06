//! System control-plane module bootstrap helpers.

use std::collections::{BTreeMap, BTreeSet};

use rkyv::{Archive, Deserialize, Serialize};
use selium_control_plane_scheduler::{SchedulePlan, ScheduledInstance};

pub mod api {
    pub use selium_control_plane_api::*;
}

pub mod runtime {
    pub use selium_control_plane_runtime::*;
}

pub mod scheduler {
    pub use selium_control_plane_scheduler::*;
}

/// Well-known module identifier used by the host when wiring system workloads.
pub const MODULE_ID: &str = "system/control-plane";

/// Default entrypoint function expected by the runtime.
pub const ENTRYPOINT: &str = "start";

/// Minimal bootstrap output for host-side diagnostics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapReport {
    pub module_id: &'static str,
    pub entrypoint: &'static str,
    pub managed_capabilities: &'static [&'static str],
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct AgentState {
    pub running_instances: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum ReconcileAction {
    Start {
        instance_id: String,
        deployment: String,
    },
    Stop {
        instance_id: String,
    },
}

pub fn bootstrap_report() -> BootstrapReport {
    BootstrapReport {
        module_id: MODULE_ID,
        entrypoint: ENTRYPOINT,
        managed_capabilities: &[
            "process.lifecycle",
            "queue.lifecycle",
            "queue.reader",
            "queue.writer",
        ],
    }
}

pub fn reconcile(
    node_name: &str,
    desired: &SchedulePlan,
    current: &AgentState,
) -> Vec<ReconcileAction> {
    let desired_on_node = desired
        .instances
        .iter()
        .filter(|instance| instance.node == node_name)
        .collect::<Vec<&ScheduledInstance>>();

    let desired_ids = desired_on_node
        .iter()
        .map(|instance| instance.instance_id.clone())
        .collect::<BTreeSet<_>>();

    let mut actions = Vec::new();

    for instance in desired_on_node {
        if !current
            .running_instances
            .contains_key(&instance.instance_id)
        {
            actions.push(ReconcileAction::Start {
                instance_id: instance.instance_id.clone(),
                deployment: instance.deployment.clone(),
            });
        }
    }

    for instance_id in current.running_instances.keys() {
        if !desired_ids.contains(instance_id) {
            actions.push(ReconcileAction::Stop {
                instance_id: instance_id.clone(),
            });
        }
    }

    actions.sort_by(|lhs, rhs| format!("{lhs:?}").cmp(&format!("{rhs:?}")));
    actions
}

pub fn apply(current: &mut AgentState, actions: &[ReconcileAction]) {
    for action in actions {
        match action {
            ReconcileAction::Start {
                instance_id,
                deployment,
            } => {
                current
                    .running_instances
                    .insert(instance_id.clone(), deployment.clone());
            }
            ReconcileAction::Stop { instance_id } => {
                current.running_instances.remove(instance_id);
            }
        }
    }
}

#[selium_guest::entrypoint]
pub async fn start() -> anyhow::Result<()> {
    let _report = bootstrap_report();
    Ok(())
}

#[cfg(test)]
mod tests {
    use selium_control_plane_api::{ControlPlaneState, DeploymentSpec, IsolationProfile};
    use selium_control_plane_scheduler::build_plan;

    use super::*;

    #[test]
    fn report_contains_expected_entrypoint() {
        let report = bootstrap_report();
        assert_eq!(report.module_id, MODULE_ID);
        assert_eq!(report.entrypoint, ENTRYPOINT);
        assert!(!report.managed_capabilities.is_empty());
    }

    #[test]
    fn reconcile_generates_start_and_stop_actions() {
        let mut state = ControlPlaneState::new_local_default();
        state
            .upsert_deployment(DeploymentSpec {
                app: "echo".to_string(),
                module: "echo.wasm".to_string(),
                replicas: 2,
                contracts: Vec::new(),
                isolation: IsolationProfile::Standard,
            })
            .expect("deployment");

        let desired = build_plan(&state).expect("schedule");
        let mut current = AgentState {
            running_instances: BTreeMap::from([("old-0".to_string(), "old".to_string())]),
        };

        let actions = reconcile("local-node", &desired, &current);
        assert_eq!(actions.len(), 3);

        apply(&mut current, &actions);
        assert_eq!(current.running_instances.len(), 2);
        assert!(current.running_instances.contains_key("echo-0"));
        assert!(current.running_instances.contains_key("echo-1"));
    }
}
