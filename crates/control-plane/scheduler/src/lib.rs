//! Placement and scheduling primitives for the control plane.

use std::collections::{BTreeMap, BTreeSet};

use rkyv::{Archive, Deserialize, Serialize};
use selium_control_plane_api::{
    ContractRef, ControlPlaneState, DeploymentSpec, EventEndpointRef, IsolationProfile, NodeSpec,
    PlacementMode, PublicEndpointRef, WorkloadRef, collect_contracts_for_workload,
    ensure_pipeline_consistency,
};
use thiserror::Error;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
struct NodeUsage {
    slots: u32,
    cpu_millis: u32,
    memory_mib: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct NodeScore {
    band: PlacementBand,
    active_node: bool,
    same_workload_instances: u32,
    observed_same_workload_instances: u32,
    observed_running_instances: u32,
    max_utilisation: u32,
    total_utilisation: u32,
    total_reserve_slack_ppm: u32,
    total_reserve_overage_ppm: u32,
    active_bridges: u32,
    remaining_cpu_millis: u32,
    remaining_memory_mib: u32,
    remaining_slots: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum PlacementBand {
    BelowReserveActive,
    BelowReserveInactive,
    BurstingActive,
    BurstingInactive,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ScheduledInstance {
    pub deployment: String,
    pub instance_id: String,
    pub node: String,
    pub isolation: IsolationProfile,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct SchedulePlan {
    pub instances: Vec<ScheduledInstance>,
    pub node_slots: BTreeMap<String, u32>,
    pub node_cpu_millis: BTreeMap<String, u32>,
    pub node_memory_mib: BTreeMap<String, u32>,
    pub idle_candidate_nodes: Vec<String>,
    pub bursting_nodes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ScheduledEndpointBridgeIntent {
    pub bridge_id: String,
    pub source_instance_id: String,
    pub source_node: String,
    pub source_endpoint: PublicEndpointRef,
    pub target_instance_id: String,
    pub target_node: String,
    pub target_endpoint: PublicEndpointRef,
    pub contract: ContractRef,
}

#[derive(Debug, PartialEq, Eq, Error)]
pub enum ScheduleError {
    #[error("pipeline is inconsistent: {0}")]
    InconsistentPipeline(String),
    #[error("no nodes are registered in control-plane state")]
    NoNodes,
    #[error("deployment `{0}` has no eligible nodes for its isolation profile")]
    NoEligibleNodes(String),
    #[error(
        "insufficient capacity to place deployment `{deployment}`: required={required}, available={available}"
    )]
    InsufficientCapacity {
        deployment: String,
        required: u32,
        available: u32,
    },
    #[error("insufficient allocatable resources to place deployment `{0}`")]
    InsufficientResources(String),
}

pub fn build_plan(state: &ControlPlaneState) -> Result<SchedulePlan, ScheduleError> {
    ensure_pipeline_consistency(state)
        .map_err(|err| ScheduleError::InconsistentPipeline(err.to_string()))?;

    let schedulable_nodes = schedulable_node_names(state);
    if schedulable_nodes.is_empty() {
        return Err(ScheduleError::NoNodes);
    }

    let mut node_order = schedulable_nodes.iter().cloned().collect::<Vec<_>>();
    node_order.sort_unstable();

    let mut usage: BTreeMap<String, NodeUsage> = node_order
        .iter()
        .map(|node| (node.clone(), NodeUsage::default()))
        .collect();
    let mut workload_placements: BTreeMap<(String, String), u32> = BTreeMap::new();
    let mut planned_workloads = BTreeSet::new();
    let mut instances = Vec::new();

    for deployment in state.deployments.values() {
        let eligible = eligible_nodes(state, deployment.isolation.clone(), &schedulable_nodes);
        if eligible.is_empty() {
            return Err(ScheduleError::NoEligibleNodes(
                deployment.workload.to_string(),
            ));
        }
        for ordinal in 0..deployment.replicas {
            let Some(node) = select_node_for_replica(
                state,
                deployment,
                &eligible,
                &usage,
                &workload_placements,
                &planned_workloads,
                ordinal,
            ) else {
                return Err(schedule_failure(
                    state,
                    deployment,
                    &eligible,
                    &workload_placements,
                    &planned_workloads,
                ));
            };

            instances.push(ScheduledInstance {
                deployment: deployment.workload.key(),
                instance_id: internal_instance_id(&deployment.workload, ordinal),
                node: node.clone(),
                isolation: deployment.isolation.clone(),
            });
            record_usage(
                state.nodes.get(&node).expect("tracked node"),
                usage.get_mut(&node).expect("tracked node usage"),
                deployment,
            );
            record_workload_placement(&mut workload_placements, &deployment.workload.key(), &node);
        }
        planned_workloads.insert(deployment.workload.key());
    }

    Ok(SchedulePlan {
        instances,
        node_slots: usage
            .iter()
            .map(|(node, used)| (node.clone(), used.slots))
            .collect(),
        node_cpu_millis: usage
            .iter()
            .map(|(node, used)| (node.clone(), used.cpu_millis))
            .collect(),
        node_memory_mib: usage
            .iter()
            .map(|(node, used)| (node.clone(), used.memory_mib))
            .collect(),
        idle_candidate_nodes: idle_candidate_nodes(state, &usage),
        bursting_nodes: bursting_nodes(state, &usage, &workload_placements, &planned_workloads),
    })
}

fn select_node_for_replica(
    state: &ControlPlaneState,
    deployment: &DeploymentSpec,
    eligible: &[String],
    usage: &BTreeMap<String, NodeUsage>,
    workload_placements: &BTreeMap<(String, String), u32>,
    planned_workloads: &BTreeSet<String>,
    _ordinal: u32,
) -> Option<String> {
    let workload_key = deployment.workload.key();
    eligible
        .iter()
        .filter_map(|node_name| {
            let node = state.nodes.get(node_name)?;
            let used = usage.get(node_name).copied().unwrap_or_default();
            if !node_can_fit(
                state,
                node,
                deployment,
                used,
                workload_placements,
                planned_workloads,
            ) {
                return None;
            }
            let same_workload_instances = workload_placements
                .get(&(workload_key.clone(), node_name.clone()))
                .copied()
                .unwrap_or(0);
            Some((
                node_name,
                score_node(
                    state,
                    node,
                    deployment,
                    used,
                    workload_placements,
                    planned_workloads,
                    same_workload_instances,
                ),
            ))
        })
        .min_by(|(lhs_name, lhs_score), (rhs_name, rhs_score)| {
            compare_node_score(deployment.placement_mode, lhs_score, rhs_score)
                .then_with(|| lhs_name.cmp(rhs_name))
        })
        .map(|(node_name, _)| node_name.clone())
}

fn node_can_fit(
    state: &ControlPlaneState,
    node: &NodeSpec,
    deployment: &DeploymentSpec,
    _used: NodeUsage,
    workload_placements: &BTreeMap<(String, String), u32>,
    planned_workloads: &BTreeSet<String>,
) -> bool {
    let projected_slots = projected_slot_occupancy(
        state,
        node,
        workload_placements,
        planned_workloads,
        &node.name,
        Some((&deployment.workload.key(), 1)),
    );
    let slots_fit = node.capacity_slots == 0 || projected_slots <= node.capacity_slots;
    let projected_cpu_millis = projected_cpu_millis(
        state,
        node,
        workload_placements,
        planned_workloads,
        &node.name,
        Some((&deployment.workload.key(), 1)),
    );
    let cpu_fit = node
        .allocatable_cpu_millis
        .is_none_or(|capacity| projected_cpu_millis <= capacity);
    let projected_memory_mib = projected_memory_mib(
        state,
        node,
        workload_placements,
        planned_workloads,
        &node.name,
        Some((&deployment.workload.key(), 1)),
    );
    let memory_fit = node
        .allocatable_memory_mib
        .is_none_or(|capacity| projected_memory_mib <= capacity);
    slots_fit && cpu_fit && memory_fit
}

fn record_usage(_node: &NodeSpec, usage: &mut NodeUsage, deployment: &DeploymentSpec) {
    usage.slots += 1;
    usage.cpu_millis += deployment.cpu_millis;
    usage.memory_mib += deployment.memory_mib;
}

fn record_workload_placement(
    placements: &mut BTreeMap<(String, String), u32>,
    workload_key: &str,
    node_name: &str,
) {
    *placements
        .entry((workload_key.to_string(), node_name.to_string()))
        .or_default() += 1;
}

fn score_node(
    state: &ControlPlaneState,
    node: &NodeSpec,
    deployment: &DeploymentSpec,
    used: NodeUsage,
    workload_placements: &BTreeMap<(String, String), u32>,
    planned_workloads: &BTreeSet<String>,
    same_workload_instances: u32,
) -> NodeScore {
    let next_slots = projected_slot_occupancy(
        state,
        node,
        workload_placements,
        planned_workloads,
        &node.name,
        Some((&deployment.workload.key(), 1)),
    );
    let observed_running_instances = node.observed_running_instances.unwrap_or(0);
    let observed_same_workload_instances =
        observed_workload_instances(node, &deployment.workload.key());
    let next_cpu_millis = projected_cpu_millis(
        state,
        node,
        workload_placements,
        planned_workloads,
        &node.name,
        Some((&deployment.workload.key(), 1)),
    );
    let projected_memory_mib = projected_memory_mib(
        state,
        node,
        workload_placements,
        planned_workloads,
        &node.name,
        Some((&deployment.workload.key(), 1)),
    );

    let slot_utilisation = utilisation(next_slots, Some(node.capacity_slots));
    let cpu_utilisation = utilisation(next_cpu_millis, node.allocatable_cpu_millis);
    let memory_utilisation = utilisation(projected_memory_mib, node.allocatable_memory_mib);
    let slot_reserve =
        slot_reserve_threshold_ppm(node.capacity_slots, node.reserve_slots_utilisation_ppm);
    let cpu_reserve = reserve_threshold_ppm(
        node.allocatable_cpu_millis,
        node.reserve_cpu_utilisation_ppm,
    );
    let memory_reserve = reserve_threshold_ppm(
        node.allocatable_memory_mib,
        node.reserve_memory_utilisation_ppm,
    );
    let (slot_slack, slot_overage) = reserve_slack_and_overage(slot_utilisation, slot_reserve);
    let (cpu_slack, cpu_overage) = reserve_slack_and_overage(cpu_utilisation, cpu_reserve);
    let (memory_slack, memory_overage) =
        reserve_slack_and_overage(memory_utilisation, memory_reserve);

    let max_utilisation = slot_utilisation
        .max(cpu_utilisation)
        .max(memory_utilisation);
    let total_utilisation = slot_utilisation
        .saturating_add(cpu_utilisation)
        .saturating_add(memory_utilisation);
    let active_node = observed_running_instances > 0 || used.slots > 0;
    let total_reserve_slack_ppm = slot_slack
        .saturating_add(cpu_slack)
        .saturating_add(memory_slack);
    let total_reserve_overage_ppm = slot_overage
        .saturating_add(cpu_overage)
        .saturating_add(memory_overage);

    NodeScore {
        band: placement_band(active_node, total_reserve_overage_ppm > 0),
        active_node,
        same_workload_instances,
        observed_same_workload_instances,
        observed_running_instances,
        max_utilisation,
        total_utilisation,
        total_reserve_slack_ppm,
        total_reserve_overage_ppm,
        active_bridges: node.observed_active_bridges.unwrap_or(0),
        remaining_cpu_millis: remaining_capacity(next_cpu_millis, node.allocatable_cpu_millis),
        remaining_memory_mib: remaining_capacity(projected_memory_mib, node.allocatable_memory_mib),
        remaining_slots: remaining_slots(next_slots, node.capacity_slots),
    }
}

fn placement_band(active_node: bool, bursting: bool) -> PlacementBand {
    match (bursting, active_node) {
        (false, true) => PlacementBand::BelowReserveActive,
        (false, false) => PlacementBand::BelowReserveInactive,
        (true, true) => PlacementBand::BurstingActive,
        (true, false) => PlacementBand::BurstingInactive,
    }
}

fn compare_node_score(
    placement_mode: PlacementMode,
    lhs: &NodeScore,
    rhs: &NodeScore,
) -> std::cmp::Ordering {
    match placement_mode {
        PlacementMode::ElasticPack => lhs
            .band
            .cmp(&rhs.band)
            .then_with(|| {
                lhs.total_reserve_slack_ppm
                    .cmp(&rhs.total_reserve_slack_ppm)
            })
            .then_with(|| {
                lhs.total_reserve_overage_ppm
                    .cmp(&rhs.total_reserve_overage_ppm)
            })
            .then_with(|| {
                lhs.same_workload_instances
                    .cmp(&rhs.same_workload_instances)
            })
            .then_with(|| {
                rhs.observed_same_workload_instances
                    .cmp(&lhs.observed_same_workload_instances)
            })
            .then_with(|| {
                lhs.observed_running_instances
                    .cmp(&rhs.observed_running_instances)
            })
            .then_with(|| lhs.active_bridges.cmp(&rhs.active_bridges))
            .then_with(|| lhs.remaining_cpu_millis.cmp(&rhs.remaining_cpu_millis))
            .then_with(|| lhs.remaining_memory_mib.cmp(&rhs.remaining_memory_mib))
            .then_with(|| lhs.remaining_slots.cmp(&rhs.remaining_slots)),
        PlacementMode::Balanced => lhs
            .max_utilisation
            .cmp(&rhs.max_utilisation)
            .then_with(|| lhs.total_utilisation.cmp(&rhs.total_utilisation))
            .then_with(|| {
                lhs.same_workload_instances
                    .cmp(&rhs.same_workload_instances)
            })
            .then_with(|| {
                rhs.observed_same_workload_instances
                    .cmp(&lhs.observed_same_workload_instances)
            })
            .then_with(|| rhs.active_node.cmp(&lhs.active_node))
            .then_with(|| {
                lhs.observed_running_instances
                    .cmp(&rhs.observed_running_instances)
            })
            .then_with(|| lhs.active_bridges.cmp(&rhs.active_bridges))
            .then_with(|| rhs.remaining_cpu_millis.cmp(&lhs.remaining_cpu_millis))
            .then_with(|| rhs.remaining_memory_mib.cmp(&lhs.remaining_memory_mib))
            .then_with(|| rhs.remaining_slots.cmp(&lhs.remaining_slots)),
        PlacementMode::Spread => lhs
            .same_workload_instances
            .cmp(&rhs.same_workload_instances)
            .then_with(|| {
                rhs.observed_same_workload_instances
                    .cmp(&lhs.observed_same_workload_instances)
            })
            .then_with(|| rhs.active_node.cmp(&lhs.active_node))
            .then_with(|| {
                lhs.observed_running_instances
                    .cmp(&rhs.observed_running_instances)
            })
            .then_with(|| lhs.max_utilisation.cmp(&rhs.max_utilisation))
            .then_with(|| lhs.total_utilisation.cmp(&rhs.total_utilisation))
            .then_with(|| lhs.active_bridges.cmp(&rhs.active_bridges))
            .then_with(|| rhs.remaining_cpu_millis.cmp(&lhs.remaining_cpu_millis))
            .then_with(|| rhs.remaining_memory_mib.cmp(&lhs.remaining_memory_mib))
            .then_with(|| rhs.remaining_slots.cmp(&lhs.remaining_slots)),
    }
}

fn slot_reserve_threshold_ppm(capacity_slots: u32, reserve_utilisation_ppm: u32) -> u32 {
    if capacity_slots <= 1 {
        0
    } else {
        reserve_utilisation_ppm
    }
}

fn reserve_threshold_ppm(capacity: Option<u32>, reserve_utilisation_ppm: u32) -> u32 {
    match capacity {
        Some(0) | None => 0,
        Some(_) => reserve_utilisation_ppm,
    }
}

fn reserve_slack_and_overage(utilisation_ppm: u32, reserve_threshold_ppm: u32) -> (u32, u32) {
    if reserve_threshold_ppm == 0 {
        return (0, 0);
    }
    if utilisation_ppm <= reserve_threshold_ppm {
        (reserve_threshold_ppm.saturating_sub(utilisation_ppm), 0)
    } else {
        (0, utilisation_ppm.saturating_sub(reserve_threshold_ppm))
    }
}

fn projected_node_usage(
    state: &ControlPlaneState,
    node: &NodeSpec,
    used: NodeUsage,
    workload_placements: &BTreeMap<(String, String), u32>,
    planned_workloads: &BTreeSet<String>,
) -> NodeScore {
    let observed_running_instances = node.observed_running_instances.unwrap_or(0);
    let projected_slots = projected_slot_occupancy(
        state,
        node,
        workload_placements,
        planned_workloads,
        &node.name,
        None,
    );
    let projected_memory_mib = projected_memory_mib(
        state,
        node,
        workload_placements,
        planned_workloads,
        &node.name,
        None,
    );
    let slot_utilisation = utilisation(projected_slots, Some(node.capacity_slots));
    let projected_cpu_millis = projected_cpu_millis(
        state,
        node,
        workload_placements,
        planned_workloads,
        &node.name,
        None,
    );
    let cpu_utilisation = utilisation(projected_cpu_millis, node.allocatable_cpu_millis);
    let memory_utilisation = utilisation(projected_memory_mib, node.allocatable_memory_mib);
    let (_, slot_overage) = reserve_slack_and_overage(
        slot_utilisation,
        slot_reserve_threshold_ppm(node.capacity_slots, node.reserve_slots_utilisation_ppm),
    );
    let (_, cpu_overage) = reserve_slack_and_overage(
        cpu_utilisation,
        reserve_threshold_ppm(
            node.allocatable_cpu_millis,
            node.reserve_cpu_utilisation_ppm,
        ),
    );
    let (_, memory_overage) = reserve_slack_and_overage(
        memory_utilisation,
        reserve_threshold_ppm(
            node.allocatable_memory_mib,
            node.reserve_memory_utilisation_ppm,
        ),
    );
    let total_reserve_overage_ppm = slot_overage
        .saturating_add(cpu_overage)
        .saturating_add(memory_overage);
    let active_node = observed_running_instances > 0 || used.slots > 0;
    NodeScore {
        band: placement_band(active_node, total_reserve_overage_ppm > 0),
        active_node,
        same_workload_instances: 0,
        observed_same_workload_instances: 0,
        observed_running_instances,
        max_utilisation: slot_utilisation
            .max(cpu_utilisation)
            .max(memory_utilisation),
        total_utilisation: slot_utilisation
            .saturating_add(cpu_utilisation)
            .saturating_add(memory_utilisation),
        total_reserve_slack_ppm: 0,
        total_reserve_overage_ppm,
        active_bridges: node.observed_active_bridges.unwrap_or(0),
        remaining_cpu_millis: remaining_capacity(projected_cpu_millis, node.allocatable_cpu_millis),
        remaining_memory_mib: remaining_capacity(projected_memory_mib, node.allocatable_memory_mib),
        remaining_slots: remaining_slots(projected_slots, node.capacity_slots),
    }
}

fn observed_workload_instances(node: &NodeSpec, workload_key: &str) -> u32 {
    node.observed_workloads
        .get(workload_key)
        .copied()
        .unwrap_or(0)
}

fn total_observed_workload_instances(node: &NodeSpec) -> u32 {
    node.observed_workloads.values().copied().sum()
}

fn observed_workload_memory_mib(
    state: &ControlPlaneState,
    node: &NodeSpec,
    workload_key: &str,
) -> u32 {
    if let Some(memory_mib) = node.observed_workload_memory_mib.get(workload_key).copied() {
        return memory_mib;
    }

    let observed_attributed_memory_mib = node
        .observed_memory_mib
        .unwrap_or(0)
        .saturating_sub(observed_unattributed_memory_mib(node));
    if observed_attributed_memory_mib == 0 {
        return 0;
    }

    let mut workload_weight = 0u128;
    let mut total_weight = 0u128;
    for (observed_workload_key, observed_instances) in &node.observed_workloads {
        let Some(deployment) = state.deployments.get(observed_workload_key) else {
            continue;
        };
        let weight =
            u128::from(*observed_instances).saturating_mul(u128::from(deployment.memory_mib));
        if observed_workload_key == workload_key {
            workload_weight = weight;
        }
        total_weight = total_weight.saturating_add(weight);
    }

    if workload_weight == 0 || total_weight == 0 {
        return 0;
    }

    ((u128::from(observed_attributed_memory_mib) * workload_weight).div_ceil(total_weight)) as u32
}

fn observed_unattributed_slots(node: &NodeSpec) -> u32 {
    node.observed_running_instances
        .unwrap_or(0)
        .saturating_sub(total_observed_workload_instances(node))
}

fn total_observed_workload_memory_mib(node: &NodeSpec) -> u32 {
    node.observed_workload_memory_mib
        .values()
        .copied()
        .fold(0, u32::saturating_add)
}

fn observed_unattributed_memory_mib(node: &NodeSpec) -> u32 {
    if !node.observed_workload_memory_mib.is_empty() {
        return node
            .observed_memory_mib
            .unwrap_or(0)
            .saturating_sub(total_observed_workload_memory_mib(node));
    }

    let observed_slots = node.observed_running_instances.unwrap_or(0);
    let unattributed_slots = observed_unattributed_slots(node);
    if observed_slots == 0 || unattributed_slots == 0 {
        return 0;
    }

    let observed_memory_mib = node.observed_memory_mib.unwrap_or(0);
    ((u128::from(observed_memory_mib) * u128::from(unattributed_slots))
        .div_ceil(u128::from(observed_slots))) as u32
}

fn projected_workload_instances(
    node: &NodeSpec,
    workload_placements: &BTreeMap<(String, String), u32>,
    planned_workloads: &BTreeSet<String>,
    node_name: &str,
    workload_key: &str,
    desired_replicas: u32,
    pending: Option<(&str, u32)>,
) -> u32 {
    let mut desired_instances = workload_placements
        .get(&(workload_key.to_string(), node_name.to_string()))
        .copied()
        .unwrap_or(0);
    let pending_matches = pending.is_some_and(|(pending_workload_key, additional_instances)| {
        if workload_key == pending_workload_key {
            desired_instances = desired_instances.saturating_add(additional_instances);
            true
        } else {
            false
        }
    });

    if planned_workloads.contains(workload_key) || pending_matches || desired_instances > 0 {
        desired_instances
    } else {
        observed_workload_instances(node, workload_key).min(desired_replicas)
    }
}

fn projected_extra_replicas(
    state: &ControlPlaneState,
    node: &NodeSpec,
    workload_placements: &BTreeMap<(String, String), u32>,
    planned_workloads: &BTreeSet<String>,
    node_name: &str,
    pending: Option<(&str, u32)>,
) -> (u32, u32) {
    let mut projected_slots = observed_unattributed_slots(node);
    let mut projected_cpu_millis = 0u32;

    for (workload_key, deployment) in &state.deployments {
        let instances = projected_workload_instances(
            node,
            workload_placements,
            planned_workloads,
            node_name,
            workload_key,
            deployment.replicas,
            pending,
        );
        projected_slots = projected_slots.saturating_add(instances);
        projected_cpu_millis =
            projected_cpu_millis.saturating_add(instances.saturating_mul(deployment.cpu_millis));
    }

    (projected_slots, projected_cpu_millis)
}

fn projected_slot_occupancy(
    state: &ControlPlaneState,
    node: &NodeSpec,
    workload_placements: &BTreeMap<(String, String), u32>,
    planned_workloads: &BTreeSet<String>,
    node_name: &str,
    pending: Option<(&str, u32)>,
) -> u32 {
    let (projected_slots, _) = projected_extra_replicas(
        state,
        node,
        workload_placements,
        planned_workloads,
        node_name,
        pending,
    );
    projected_slots
}

fn projected_cpu_millis(
    state: &ControlPlaneState,
    node: &NodeSpec,
    workload_placements: &BTreeMap<(String, String), u32>,
    planned_workloads: &BTreeSet<String>,
    node_name: &str,
    pending: Option<(&str, u32)>,
) -> u32 {
    let (_, projected_cpu_millis) = projected_extra_replicas(
        state,
        node,
        workload_placements,
        planned_workloads,
        node_name,
        pending,
    );
    projected_cpu_millis
}

fn projected_memory_mib(
    state: &ControlPlaneState,
    node: &NodeSpec,
    workload_placements: &BTreeMap<(String, String), u32>,
    planned_workloads: &BTreeSet<String>,
    node_name: &str,
    pending: Option<(&str, u32)>,
) -> u32 {
    let observed_workload_slots = total_observed_workload_instances(node);
    let observed_unattributed_memory_mib = observed_unattributed_memory_mib(node);
    let mut retained_observed_memory_mib = 0u32;
    let mut extra_memory_mib = observed_unattributed_memory_mib;

    for (workload_key, deployment) in &state.deployments {
        let projected_instances = projected_workload_instances(
            node,
            workload_placements,
            planned_workloads,
            node_name,
            workload_key,
            deployment.replicas,
            pending,
        );
        let retained_instances =
            projected_instances.min(observed_workload_instances(node, workload_key));
        let observed_workload_memory_mib = observed_workload_memory_mib(state, node, workload_key);
        retained_observed_memory_mib = retained_observed_memory_mib.saturating_add(
            if retained_instances == 0 || observed_workload_slots == 0 {
                0
            } else {
                ((u128::from(observed_workload_memory_mib) * u128::from(retained_instances))
                    .div_ceil(u128::from(observed_workload_instances(node, workload_key))))
                    as u32
            },
        );
        let extra_instances = projected_instances.saturating_sub(retained_instances);
        extra_memory_mib =
            extra_memory_mib.saturating_add(extra_instances.saturating_mul(deployment.memory_mib));
    }

    retained_observed_memory_mib.saturating_add(extra_memory_mib)
}

fn idle_candidate_nodes(
    state: &ControlPlaneState,
    usage: &BTreeMap<String, NodeUsage>,
) -> Vec<String> {
    let mut nodes = state
        .nodes
        .iter()
        .filter(|(node_name, node)| {
            node.observed_running_instances.unwrap_or(0) > 0
                && usage.get(*node_name).copied().unwrap_or_default().slots == 0
        })
        .map(|(node_name, _)| node_name.clone())
        .collect::<Vec<_>>();
    nodes.sort_unstable();
    nodes
}

fn bursting_nodes(
    state: &ControlPlaneState,
    usage: &BTreeMap<String, NodeUsage>,
    workload_placements: &BTreeMap<(String, String), u32>,
    planned_workloads: &BTreeSet<String>,
) -> Vec<String> {
    let mut nodes = state
        .nodes
        .iter()
        .filter(|(node_name, node)| {
            projected_node_usage(
                state,
                node,
                usage.get(*node_name).copied().unwrap_or_default(),
                workload_placements,
                planned_workloads,
            )
            .total_reserve_overage_ppm
                > 0
        })
        .map(|(node_name, _)| node_name.clone())
        .collect::<Vec<_>>();
    nodes.sort_unstable();
    nodes
}

fn utilisation(used: u32, capacity: Option<u32>) -> u32 {
    match capacity {
        Some(0) | None => 0,
        Some(capacity) => ((u128::from(used) * 1_000_000) / u128::from(capacity)) as u32,
    }
}

fn remaining_capacity(used: u32, capacity: Option<u32>) -> u32 {
    capacity.map_or(u32::MAX, |capacity| capacity.saturating_sub(used))
}

fn remaining_slots(used: u32, capacity: u32) -> u32 {
    if capacity == 0 {
        u32::MAX
    } else {
        capacity.saturating_sub(used)
    }
}

fn schedule_failure(
    state: &ControlPlaneState,
    deployment: &DeploymentSpec,
    eligible: &[String],
    workload_placements: &BTreeMap<(String, String), u32>,
    planned_workloads: &BTreeSet<String>,
) -> ScheduleError {
    if deployment.cpu_millis > 0 || deployment.memory_mib > 0 {
        return ScheduleError::InsufficientResources(deployment.workload.to_string());
    }

    let available = eligible
        .iter()
        .map(|node| {
            let Some(node_spec) = state.nodes.get(node) else {
                return 0;
            };
            let projected_slots = projected_slot_occupancy(
                state,
                node_spec,
                workload_placements,
                planned_workloads,
                node,
                None,
            );
            node_spec.capacity_slots.saturating_sub(projected_slots)
        })
        .sum();
    ScheduleError::InsufficientCapacity {
        deployment: deployment.workload.to_string(),
        required: deployment.replicas,
        available,
    }
}

fn schedulable_node_names(state: &ControlPlaneState) -> BTreeSet<String> {
    let live = state
        .nodes
        .values()
        .filter(|node| node.last_heartbeat_ms > 0)
        .map(|node| node.name.clone())
        .collect::<BTreeSet<_>>();
    if live.is_empty() {
        state.nodes.keys().cloned().collect()
    } else {
        live
    }
}

pub fn deployment_contract_usage(state: &ControlPlaneState) -> BTreeMap<String, BTreeSet<String>> {
    state
        .deployments
        .values()
        .map(|deployment| {
            let contracts = collect_contracts_for_workload(state, &deployment.workload)
                .into_iter()
                .map(|contract| {
                    format!(
                        "{}/{}:{}@{}",
                        contract.namespace,
                        contract.kind.as_str(),
                        contract.name,
                        contract.version
                    )
                })
                .collect::<BTreeSet<_>>();
            (deployment.workload.key(), contracts)
        })
        .collect()
}

pub fn build_endpoint_bridge_intents(
    state: &ControlPlaneState,
    plan: &SchedulePlan,
) -> Vec<ScheduledEndpointBridgeIntent> {
    let mut instances_by_workload: BTreeMap<&str, Vec<&ScheduledInstance>> = BTreeMap::new();
    for instance in &plan.instances {
        instances_by_workload
            .entry(instance.deployment.as_str())
            .or_default()
            .push(instance);
    }

    let mut intents = Vec::new();
    for pipeline in state.pipelines.values() {
        for edge in &pipeline.edges {
            let Some(source_instances) =
                instances_by_workload.get(edge.from.endpoint.workload.key().as_str())
            else {
                continue;
            };
            let Some(target_instances) =
                instances_by_workload.get(edge.to.endpoint.workload.key().as_str())
            else {
                continue;
            };
            for instance in source_instances {
                let Some(target) = select_target_instance(instance.node.as_str(), target_instances)
                else {
                    continue;
                };
                intents.push(ScheduledEndpointBridgeIntent {
                    bridge_id: endpoint_bridge_id(
                        &instance.instance_id,
                        &target.instance_id,
                        &public_endpoint_ref(&edge.from.endpoint, &edge.from.contract),
                        &public_endpoint_ref(&edge.to.endpoint, &edge.to.contract),
                    ),
                    source_instance_id: instance.instance_id.clone(),
                    source_node: instance.node.clone(),
                    source_endpoint: public_endpoint_ref(&edge.from.endpoint, &edge.from.contract),
                    target_instance_id: target.instance_id.clone(),
                    target_node: target.node.clone(),
                    target_endpoint: public_endpoint_ref(&edge.to.endpoint, &edge.to.contract),
                    contract: edge.to.contract.clone(),
                });
            }
        }
    }
    intents.sort_by(|lhs, rhs| lhs.bridge_id.cmp(&rhs.bridge_id));
    intents
}

fn internal_instance_id(workload: &WorkloadRef, ordinal: u32) -> String {
    format!(
        "tenant={};namespace={};workload={};replica={ordinal}",
        workload.tenant, workload.namespace, workload.name
    )
}

fn endpoint_bridge_id(
    source_instance_id: &str,
    target_instance_id: &str,
    source_endpoint: &PublicEndpointRef,
    target_endpoint: &PublicEndpointRef,
) -> String {
    format!(
        "{source_instance_id}->{target_instance_id}::{source}->{target}",
        source = source_endpoint.key(),
        target = target_endpoint.key()
    )
}

fn public_endpoint_ref(endpoint: &EventEndpointRef, contract: &ContractRef) -> PublicEndpointRef {
    PublicEndpointRef {
        workload: endpoint.workload.clone(),
        kind: contract.kind,
        name: endpoint.name.clone(),
    }
}

fn select_target_instance<'a>(
    source_node: &str,
    targets: &'a [&'a ScheduledInstance],
) -> Option<&'a ScheduledInstance> {
    targets
        .iter()
        .copied()
        .filter(|candidate| candidate.node == source_node)
        .min_by(|lhs, rhs| lhs.instance_id.cmp(&rhs.instance_id))
        .or_else(|| {
            targets.iter().copied().min_by(|lhs, rhs| {
                lhs.node
                    .cmp(&rhs.node)
                    .then(lhs.instance_id.cmp(&rhs.instance_id))
            })
        })
}

fn eligible_nodes(
    state: &ControlPlaneState,
    isolation: IsolationProfile,
    schedulable_nodes: &BTreeSet<String>,
) -> Vec<String> {
    let mut nodes = state
        .nodes
        .values()
        .filter(|node| {
            schedulable_nodes.contains(&node.name) && node.supported_isolation.contains(&isolation)
        })
        .map(|node| node.name.clone())
        .collect::<Vec<_>>();
    nodes.sort_unstable();
    nodes
}

#[cfg(test)]
mod tests {
    use selium_control_plane_api::{
        BandwidthProfile, ContractRef, DeploymentSpec, PipelineEdge, PipelineEndpoint,
        PipelineSpec, PlacementMode, parse_idl,
    };

    use super::*;

    fn sample_state() -> ControlPlaneState {
        let mut state = ControlPlaneState::new_local_default();

        state
            .registry
            .register_package(
                parse_idl(
                    "package media.pipeline.v1;\n\
                     schema Frame { camera_id: string; ts_ms: u64; }\n\
                     event camera.frames(Frame) { replay: enabled; }",
                )
                .expect("parse"),
            )
            .expect("register");

        state
            .upsert_deployment(DeploymentSpec {
                workload: WorkloadRef {
                    tenant: "tenant-a".to_string(),
                    namespace: "media".to_string(),
                    name: "ingest".to_string(),
                },
                module: "ingest.wasm".to_string(),
                replicas: 2,
                contracts: vec![ContractRef {
                    namespace: "media.pipeline".to_string(),
                    kind: selium_control_plane_api::ContractKind::Event,
                    name: "camera.frames".to_string(),
                    version: "v1".to_string(),
                }],
                isolation: IsolationProfile::Standard,
                placement_mode: selium_control_plane_api::PlacementMode::ElasticPack,
                cpu_millis: 0,
                memory_mib: 0,
                ephemeral_storage_mib: 0,
                bandwidth_profile: BandwidthProfile::Standard,
                volume_mounts: Vec::new(),
                external_account_ref: None,
            })
            .expect("deployment");
        state
            .upsert_deployment(DeploymentSpec {
                workload: WorkloadRef {
                    tenant: "tenant-a".to_string(),
                    namespace: "media".to_string(),
                    name: "detector".to_string(),
                },
                module: "detector.wasm".to_string(),
                replicas: 1,
                contracts: vec![ContractRef {
                    namespace: "media.pipeline".to_string(),
                    kind: selium_control_plane_api::ContractKind::Event,
                    name: "camera.frames".to_string(),
                    version: "v1".to_string(),
                }],
                isolation: IsolationProfile::Standard,
                placement_mode: selium_control_plane_api::PlacementMode::ElasticPack,
                cpu_millis: 0,
                memory_mib: 0,
                ephemeral_storage_mib: 0,
                bandwidth_profile: BandwidthProfile::Standard,
                volume_mounts: Vec::new(),
                external_account_ref: None,
            })
            .expect("deployment");

        state.upsert_pipeline(PipelineSpec {
            name: "pipeline".to_string(),
            tenant: "tenant-a".to_string(),
            namespace: "media".to_string(),
            edges: vec![PipelineEdge {
                from: PipelineEndpoint {
                    endpoint: selium_control_plane_api::EventEndpointRef {
                        workload: WorkloadRef {
                            tenant: "tenant-a".to_string(),
                            namespace: "media".to_string(),
                            name: "ingest".to_string(),
                        },
                        name: "camera.frames".to_string(),
                    },
                    contract: ContractRef {
                        namespace: "media.pipeline".to_string(),
                        kind: selium_control_plane_api::ContractKind::Event,
                        name: "camera.frames".to_string(),
                        version: "v1".to_string(),
                    },
                },
                to: PipelineEndpoint {
                    endpoint: selium_control_plane_api::EventEndpointRef {
                        workload: WorkloadRef {
                            tenant: "tenant-a".to_string(),
                            namespace: "media".to_string(),
                            name: "detector".to_string(),
                        },
                        name: "camera.frames".to_string(),
                    },
                    contract: ContractRef {
                        namespace: "media.pipeline".to_string(),
                        kind: selium_control_plane_api::ContractKind::Event,
                        name: "camera.frames".to_string(),
                        version: "v1".to_string(),
                    },
                },
            }],
            external_account_ref: None,
        });

        state
    }

    fn test_node(name: &str, cpu_millis: u32, memory_mib: u32) -> NodeSpec {
        NodeSpec {
            name: name.to_string(),
            capacity_slots: 8,
            allocatable_cpu_millis: Some(cpu_millis),
            allocatable_memory_mib: Some(memory_mib),
            reserve_cpu_utilisation_ppm: 800_000,
            reserve_memory_utilisation_ppm: 800_000,
            reserve_slots_utilisation_ppm: 800_000,
            observed_running_instances: None,
            observed_active_bridges: None,
            observed_memory_mib: None,
            observed_workloads: BTreeMap::new(),
            observed_workload_memory_mib: BTreeMap::new(),
            supported_isolation: vec![IsolationProfile::Standard],
            daemon_addr: format!("127.0.0.1:{}", if name == "node-a" { 7101 } else { 7102 }),
            daemon_server_name: name.to_string(),
            last_heartbeat_ms: 1,
        }
    }

    fn test_workload_key(name: &str) -> String {
        format!("tenant-a/media/{name}")
    }

    fn test_deployment(
        name: &str,
        placement_mode: PlacementMode,
        replicas: u32,
        cpu_millis: u32,
        memory_mib: u32,
    ) -> DeploymentSpec {
        DeploymentSpec {
            workload: WorkloadRef {
                tenant: "tenant-a".to_string(),
                namespace: "media".to_string(),
                name: name.to_string(),
            },
            module: format!("{name}.wasm"),
            replicas,
            contracts: Vec::new(),
            isolation: IsolationProfile::Standard,
            placement_mode,
            cpu_millis,
            memory_mib,
            ephemeral_storage_mib: 0,
            bandwidth_profile: BandwidthProfile::Standard,
            volume_mounts: Vec::new(),
            external_account_ref: None,
        }
    }

    #[test]
    fn builds_plan_for_registered_nodes() {
        let state = sample_state();
        let plan = build_plan(&state).expect("plan");
        assert_eq!(plan.instances.len(), 3);
        assert_eq!(plan.node_cpu_millis.get("local-node"), Some(&0));
        assert_eq!(plan.node_memory_mib.get("local-node"), Some(&0));
        assert!(
            plan.instances
                .iter()
                .all(|instance| instance.node == "local-node")
        );
    }

    #[test]
    fn builds_plan_using_allocatable_resources_without_slots() {
        let mut state = sample_state();
        let node = state.nodes.get_mut("local-node").expect("local node");
        node.capacity_slots = 0;
        node.allocatable_cpu_millis = Some(2_000);
        node.allocatable_memory_mib = Some(4_096);
        for deployment in state.deployments.values_mut() {
            deployment.cpu_millis = 500;
            deployment.memory_mib = 512;
        }

        let plan = build_plan(&state).expect("plan");

        assert_eq!(plan.instances.len(), 3);
        assert_eq!(plan.node_cpu_millis.get("local-node"), Some(&1_500));
        assert_eq!(plan.node_memory_mib.get("local-node"), Some(&1_536));
    }

    #[test]
    fn rejects_when_allocatable_cpu_is_exceeded() {
        let mut state = ControlPlaneState::new_local_default();
        let node = state.nodes.get_mut("local-node").expect("local node");
        node.capacity_slots = 0;
        node.allocatable_cpu_millis = Some(1_000);
        node.allocatable_memory_mib = Some(4_096);

        state
            .upsert_deployment(DeploymentSpec {
                workload: WorkloadRef {
                    tenant: "tenant-a".to_string(),
                    namespace: "media".to_string(),
                    name: "ingest".to_string(),
                },
                module: "ingest.wasm".to_string(),
                replicas: 2,
                contracts: Vec::new(),
                isolation: IsolationProfile::Standard,
                placement_mode: selium_control_plane_api::PlacementMode::ElasticPack,
                cpu_millis: 600,
                memory_mib: 256,
                ephemeral_storage_mib: 0,
                bandwidth_profile: BandwidthProfile::Standard,
                volume_mounts: Vec::new(),
                external_account_ref: None,
            })
            .expect("deployment");

        let err = build_plan(&state).expect_err("insufficient cpu");

        assert_eq!(
            err,
            ScheduleError::InsufficientResources("tenant-a/media/ingest".to_string())
        );
    }

    #[test]
    fn prefers_node_with_lower_projected_utilisation() {
        let mut state = ControlPlaneState::default();
        state
            .upsert_node(NodeSpec {
                name: "node-a".to_string(),
                capacity_slots: 0,
                allocatable_cpu_millis: Some(1_000),
                allocatable_memory_mib: Some(4_096),
                reserve_cpu_utilisation_ppm: 800_000,
                reserve_memory_utilisation_ppm: 800_000,
                reserve_slots_utilisation_ppm: 800_000,
                observed_running_instances: None,
                observed_active_bridges: None,
                observed_memory_mib: None,
                observed_workloads: BTreeMap::new(),
                observed_workload_memory_mib: BTreeMap::new(),
                supported_isolation: vec![IsolationProfile::Standard],
                daemon_addr: "127.0.0.1:7101".to_string(),
                daemon_server_name: "node-a".to_string(),
                last_heartbeat_ms: 1,
            })
            .expect("node-a");
        state
            .upsert_node(NodeSpec {
                name: "node-b".to_string(),
                capacity_slots: 0,
                allocatable_cpu_millis: Some(4_000),
                allocatable_memory_mib: Some(4_096),
                reserve_cpu_utilisation_ppm: 800_000,
                reserve_memory_utilisation_ppm: 800_000,
                reserve_slots_utilisation_ppm: 800_000,
                observed_running_instances: None,
                observed_active_bridges: None,
                observed_memory_mib: None,
                observed_workloads: BTreeMap::new(),
                observed_workload_memory_mib: BTreeMap::new(),
                supported_isolation: vec![IsolationProfile::Standard],
                daemon_addr: "127.0.0.1:7102".to_string(),
                daemon_server_name: "node-b".to_string(),
                last_heartbeat_ms: 1,
            })
            .expect("node-b");

        state
            .upsert_deployment(DeploymentSpec {
                workload: WorkloadRef {
                    tenant: "tenant-a".to_string(),
                    namespace: "media".to_string(),
                    name: "ingest".to_string(),
                },
                module: "ingest.wasm".to_string(),
                replicas: 1,
                contracts: Vec::new(),
                isolation: IsolationProfile::Standard,
                placement_mode: selium_control_plane_api::PlacementMode::Balanced,
                cpu_millis: 750,
                memory_mib: 256,
                ephemeral_storage_mib: 0,
                bandwidth_profile: BandwidthProfile::Standard,
                volume_mounts: Vec::new(),
                external_account_ref: None,
            })
            .expect("deployment");

        let plan = build_plan(&state).expect("plan");

        assert_eq!(plan.instances.len(), 1);
        assert_eq!(plan.instances[0].node, "node-b");
    }

    #[test]
    fn balances_equal_nodes_by_projected_utilisation() {
        let mut state = ControlPlaneState::default();
        for node_name in ["node-a", "node-b"] {
            state
                .upsert_node(NodeSpec {
                    name: node_name.to_string(),
                    capacity_slots: 0,
                    allocatable_cpu_millis: Some(2_000),
                    allocatable_memory_mib: Some(4_096),
                    reserve_cpu_utilisation_ppm: 800_000,
                    reserve_memory_utilisation_ppm: 800_000,
                    reserve_slots_utilisation_ppm: 800_000,
                    observed_running_instances: None,
                    observed_active_bridges: None,
                    observed_memory_mib: None,
                    observed_workloads: BTreeMap::new(),
                    observed_workload_memory_mib: BTreeMap::new(),
                    supported_isolation: vec![IsolationProfile::Standard],
                    daemon_addr: format!(
                        "127.0.0.1:{}",
                        if node_name == "node-a" { 7101 } else { 7102 }
                    ),
                    daemon_server_name: node_name.to_string(),
                    last_heartbeat_ms: 1,
                })
                .expect("node");
        }

        state
            .upsert_deployment(DeploymentSpec {
                workload: WorkloadRef {
                    tenant: "tenant-a".to_string(),
                    namespace: "media".to_string(),
                    name: "ingest".to_string(),
                },
                module: "ingest.wasm".to_string(),
                replicas: 2,
                contracts: Vec::new(),
                isolation: IsolationProfile::Standard,
                placement_mode: selium_control_plane_api::PlacementMode::Balanced,
                cpu_millis: 500,
                memory_mib: 256,
                ephemeral_storage_mib: 0,
                bandwidth_profile: BandwidthProfile::Standard,
                volume_mounts: Vec::new(),
                external_account_ref: None,
            })
            .expect("deployment");

        let plan = build_plan(&state).expect("plan");

        assert_eq!(
            plan.instances
                .iter()
                .filter(|instance| instance.node == "node-a")
                .count(),
            1
        );
        assert_eq!(
            plan.instances
                .iter()
                .filter(|instance| instance.node == "node-b")
                .count(),
            1
        );
    }

    #[test]
    fn prefers_node_with_lower_observed_instance_load() {
        let mut state = ControlPlaneState::default();
        state
            .upsert_node(NodeSpec {
                name: "node-a".to_string(),
                capacity_slots: 8,
                allocatable_cpu_millis: Some(4_000),
                allocatable_memory_mib: Some(8_192),
                reserve_cpu_utilisation_ppm: 800_000,
                reserve_memory_utilisation_ppm: 800_000,
                reserve_slots_utilisation_ppm: 800_000,
                observed_running_instances: Some(4),
                observed_active_bridges: Some(2),
                observed_memory_mib: Some(3_072),
                observed_workloads: BTreeMap::new(),
                observed_workload_memory_mib: BTreeMap::new(),
                supported_isolation: vec![IsolationProfile::Standard],
                daemon_addr: "127.0.0.1:7101".to_string(),
                daemon_server_name: "node-a".to_string(),
                last_heartbeat_ms: 1,
            })
            .expect("node-a");
        state
            .upsert_node(NodeSpec {
                name: "node-b".to_string(),
                capacity_slots: 8,
                allocatable_cpu_millis: Some(4_000),
                allocatable_memory_mib: Some(8_192),
                reserve_cpu_utilisation_ppm: 800_000,
                reserve_memory_utilisation_ppm: 800_000,
                reserve_slots_utilisation_ppm: 800_000,
                observed_running_instances: Some(1),
                observed_active_bridges: Some(0),
                observed_memory_mib: Some(512),
                observed_workloads: BTreeMap::new(),
                observed_workload_memory_mib: BTreeMap::new(),
                supported_isolation: vec![IsolationProfile::Standard],
                daemon_addr: "127.0.0.1:7102".to_string(),
                daemon_server_name: "node-b".to_string(),
                last_heartbeat_ms: 1,
            })
            .expect("node-b");

        state
            .upsert_deployment(DeploymentSpec {
                workload: WorkloadRef {
                    tenant: "tenant-a".to_string(),
                    namespace: "media".to_string(),
                    name: "ingest".to_string(),
                },
                module: "ingest.wasm".to_string(),
                replicas: 1,
                contracts: Vec::new(),
                isolation: IsolationProfile::Standard,
                placement_mode: selium_control_plane_api::PlacementMode::Balanced,
                cpu_millis: 500,
                memory_mib: 256,
                ephemeral_storage_mib: 0,
                bandwidth_profile: BandwidthProfile::Standard,
                volume_mounts: Vec::new(),
                external_account_ref: None,
            })
            .expect("deployment");

        let plan = build_plan(&state).expect("plan");

        assert_eq!(plan.instances.len(), 1);
        assert_eq!(plan.instances[0].node, "node-b");
    }

    #[test]
    fn prefers_node_with_lower_observed_memory_pressure() {
        let mut state = ControlPlaneState::default();
        state
            .upsert_node(NodeSpec {
                name: "node-a".to_string(),
                capacity_slots: 8,
                allocatable_cpu_millis: Some(4_000),
                allocatable_memory_mib: Some(4_096),
                reserve_cpu_utilisation_ppm: 800_000,
                reserve_memory_utilisation_ppm: 800_000,
                reserve_slots_utilisation_ppm: 800_000,
                observed_running_instances: Some(1),
                observed_active_bridges: Some(0),
                observed_memory_mib: Some(3_584),
                observed_workloads: BTreeMap::new(),
                observed_workload_memory_mib: BTreeMap::new(),
                supported_isolation: vec![IsolationProfile::Standard],
                daemon_addr: "127.0.0.1:7101".to_string(),
                daemon_server_name: "node-a".to_string(),
                last_heartbeat_ms: 1,
            })
            .expect("node-a");
        state
            .upsert_node(NodeSpec {
                name: "node-b".to_string(),
                capacity_slots: 8,
                allocatable_cpu_millis: Some(4_000),
                allocatable_memory_mib: Some(4_096),
                reserve_cpu_utilisation_ppm: 800_000,
                reserve_memory_utilisation_ppm: 800_000,
                reserve_slots_utilisation_ppm: 800_000,
                observed_running_instances: Some(1),
                observed_active_bridges: Some(0),
                observed_memory_mib: Some(512),
                observed_workloads: BTreeMap::new(),
                observed_workload_memory_mib: BTreeMap::new(),
                supported_isolation: vec![IsolationProfile::Standard],
                daemon_addr: "127.0.0.1:7102".to_string(),
                daemon_server_name: "node-b".to_string(),
                last_heartbeat_ms: 1,
            })
            .expect("node-b");

        state
            .upsert_deployment(DeploymentSpec {
                workload: WorkloadRef {
                    tenant: "tenant-a".to_string(),
                    namespace: "media".to_string(),
                    name: "ingest".to_string(),
                },
                module: "ingest.wasm".to_string(),
                replicas: 1,
                contracts: Vec::new(),
                isolation: IsolationProfile::Standard,
                placement_mode: selium_control_plane_api::PlacementMode::Balanced,
                cpu_millis: 250,
                memory_mib: 256,
                ephemeral_storage_mib: 0,
                bandwidth_profile: BandwidthProfile::Standard,
                volume_mounts: Vec::new(),
                external_account_ref: None,
            })
            .expect("deployment");

        let plan = build_plan(&state).expect("plan");

        assert_eq!(plan.instances.len(), 1);
        assert_eq!(plan.instances[0].node, "node-b");
    }

    #[test]
    fn balanced_keeps_singleton_on_already_active_node() {
        let mut state = ControlPlaneState::default();
        state
            .upsert_node(test_node("node-a", 2_000, 4_096))
            .expect("node-a");
        let mut node_b = test_node("node-b", 2_000, 4_096);
        node_b.observed_running_instances = Some(1);
        node_b.observed_memory_mib = Some(256);
        node_b
            .observed_workloads
            .insert(test_workload_key("ingest"), 1);
        state.upsert_node(node_b).expect("node-b");

        state
            .upsert_deployment(test_deployment(
                "ingest",
                PlacementMode::Balanced,
                1,
                250,
                256,
            ))
            .expect("deployment");

        let plan = build_plan(&state).expect("plan");

        assert_eq!(plan.instances.len(), 1);
        assert_eq!(plan.instances[0].node, "node-b");
    }

    #[test]
    fn spread_keeps_singleton_on_already_active_node() {
        let mut state = ControlPlaneState::default();
        state
            .upsert_node(test_node("node-a", 2_000, 4_096))
            .expect("node-a");
        let mut node_b = test_node("node-b", 2_000, 4_096);
        node_b.observed_running_instances = Some(1);
        node_b.observed_memory_mib = Some(256);
        node_b
            .observed_workloads
            .insert(test_workload_key("ingest"), 1);
        state.upsert_node(node_b).expect("node-b");

        state
            .upsert_deployment(test_deployment(
                "ingest",
                PlacementMode::Spread,
                1,
                250,
                256,
            ))
            .expect("deployment");

        let plan = build_plan(&state).expect("plan");

        assert_eq!(plan.instances.len(), 1);
        assert_eq!(plan.instances[0].node, "node-b");
    }

    #[test]
    fn elastic_pack_prefers_active_nodes_while_below_reserve() {
        let mut state = ControlPlaneState::default();
        let mut node_a = test_node("node-a", 1_000, 1_024);
        node_a.observed_running_instances = Some(1);
        node_a
            .observed_workloads
            .insert(test_workload_key("ingest"), 1);
        state.upsert_node(node_a).expect("node-a");
        state
            .upsert_node(test_node("node-b", 1_000, 1_024))
            .expect("node-b");

        state
            .upsert_deployment(test_deployment(
                "ingest",
                PlacementMode::ElasticPack,
                1,
                100,
                128,
            ))
            .expect("deployment");

        let plan = build_plan(&state).expect("plan");

        assert_eq!(plan.instances.len(), 1);
        assert_eq!(plan.instances[0].node, "node-a");
        assert!(plan.idle_candidate_nodes.is_empty());
        assert!(plan.bursting_nodes.is_empty());
    }

    #[test]
    fn elastic_pack_adds_memory_for_replicas_beyond_observed_capacity() {
        let mut state = ControlPlaneState::default();
        let mut node_a = test_node("node-a", 1_000, 1_000);
        node_a.observed_running_instances = Some(1);
        node_a.observed_memory_mib = Some(700);
        node_a
            .observed_workloads
            .insert(test_workload_key("ingest"), 1);
        state.upsert_node(node_a).expect("node-a");
        state
            .upsert_node(test_node("node-b", 1_000, 1_000))
            .expect("node-b");

        state
            .upsert_deployment(test_deployment(
                "ingest",
                PlacementMode::ElasticPack,
                2,
                100,
                200,
            ))
            .expect("deployment");

        let plan = build_plan(&state).expect("plan");

        assert_eq!(
            plan.instances
                .iter()
                .filter(|instance| instance.node == "node-a")
                .count(),
            1
        );
        assert_eq!(
            plan.instances
                .iter()
                .filter(|instance| instance.node == "node-b")
                .count(),
            1
        );
        assert!(plan.bursting_nodes.is_empty());
    }

    #[test]
    fn elastic_pack_does_not_double_count_existing_replica_memory() {
        let mut state = ControlPlaneState::default();
        let mut node_a = test_node("node-a", 1_000, 1_000);
        node_a.observed_running_instances = Some(1);
        node_a.observed_memory_mib = Some(700);
        node_a
            .observed_workloads
            .insert(test_workload_key("ingest"), 1);
        state.upsert_node(node_a).expect("node-a");
        state
            .upsert_node(test_node("node-b", 1_000, 1_000))
            .expect("node-b");

        state
            .upsert_deployment(test_deployment(
                "ingest",
                PlacementMode::ElasticPack,
                1,
                100,
                200,
            ))
            .expect("deployment");

        let plan = build_plan(&state).expect("plan");

        assert_eq!(plan.instances.len(), 1);
        assert_eq!(plan.instances[0].node, "node-a");
        assert!(plan.idle_candidate_nodes.is_empty());
        assert!(plan.bursting_nodes.is_empty());
    }

    #[test]
    fn elastic_pack_marks_nodes_that_are_already_bursting() {
        let mut state = ControlPlaneState::default();
        let mut node_a = test_node("node-a", 1_000, 1_000);
        node_a.observed_running_instances = Some(1);
        node_a.observed_memory_mib = Some(900);
        node_a
            .observed_workloads
            .insert(test_workload_key("ingest"), 1);
        state.upsert_node(node_a).expect("node-a");

        state
            .upsert_deployment(test_deployment(
                "ingest",
                PlacementMode::ElasticPack,
                1,
                100,
                200,
            ))
            .expect("deployment");

        let plan = build_plan(&state).expect("plan");

        assert_eq!(plan.instances.len(), 1);
        assert_eq!(plan.instances[0].node, "node-a");
        assert_eq!(plan.bursting_nodes, vec!["node-a".to_string()]);
    }

    #[test]
    fn elastic_pack_does_not_penalize_single_slot_nodes_for_missing_burst_headroom() {
        let mut state = ControlPlaneState::default();
        let mut node_a = test_node("node-a", 1_000, 1_000);
        node_a.capacity_slots = 1;
        node_a.observed_running_instances = Some(1);
        node_a
            .observed_workloads
            .insert(test_workload_key("ingest"), 1);
        state.upsert_node(node_a).expect("node-a");

        let mut node_b = test_node("node-b", 1_000, 1_000);
        node_b.capacity_slots = 2;
        node_b.observed_running_instances = Some(1);
        state.upsert_node(node_b).expect("node-b");

        state
            .upsert_deployment(test_deployment(
                "ingest",
                PlacementMode::ElasticPack,
                1,
                0,
                0,
            ))
            .expect("deployment");

        let plan = build_plan(&state).expect("plan");

        assert_eq!(plan.instances.len(), 1);
        assert_eq!(plan.instances[0].node, "node-a");
    }

    #[test]
    fn rejects_new_replica_when_observed_slots_are_full() {
        let mut state = ControlPlaneState::default();
        let mut node_a = test_node("node-a", 1_000, 1_000);
        node_a.capacity_slots = 1;
        node_a.observed_running_instances = Some(1);
        node_a.observed_memory_mib = Some(128);
        state.upsert_node(node_a).expect("node-a");

        state
            .upsert_deployment(test_deployment("ingest", PlacementMode::Balanced, 1, 0, 0))
            .expect("deployment");

        let err = build_plan(&state).expect_err("insufficient capacity");

        assert_eq!(
            err,
            ScheduleError::InsufficientCapacity {
                deployment: test_workload_key("ingest"),
                required: 1,
                available: 0,
            }
        );
    }

    #[test]
    fn rejects_new_replica_when_observed_memory_is_full() {
        let mut state = ControlPlaneState::default();
        let mut node_a = test_node("node-a", 1_000, 1_000);
        node_a.observed_running_instances = Some(1);
        node_a.observed_memory_mib = Some(900);
        state.upsert_node(node_a).expect("node-a");

        state
            .upsert_deployment(test_deployment(
                "ingest",
                PlacementMode::Balanced,
                1,
                100,
                200,
            ))
            .expect("deployment");

        let err = build_plan(&state).expect_err("insufficient resources");

        assert_eq!(
            err,
            ScheduleError::InsufficientResources(test_workload_key("ingest"))
        );
    }

    #[test]
    fn elastic_pack_matches_observed_memory_by_workload_not_iteration_order() {
        let mut state = ControlPlaneState::default();
        let mut node_a = test_node("node-a", 1_000, 1_000);
        node_a.observed_running_instances = Some(1);
        node_a.observed_memory_mib = Some(700);
        node_a
            .observed_workloads
            .insert(test_workload_key("zeta"), 1);
        state.upsert_node(node_a).expect("node-a");
        state
            .upsert_node(test_node("node-b", 1_000, 1_000))
            .expect("node-b");

        state
            .upsert_deployment(test_deployment(
                "alpha",
                PlacementMode::ElasticPack,
                1,
                100,
                100,
            ))
            .expect("alpha deployment");
        state
            .upsert_deployment(test_deployment(
                "zeta",
                PlacementMode::ElasticPack,
                1,
                100,
                700,
            ))
            .expect("zeta deployment");

        let plan = build_plan(&state).expect("plan");

        assert_eq!(plan.instances.len(), 2);
        assert!(
            plan.instances
                .iter()
                .all(|instance| instance.node == "node-a")
        );
        assert!(plan.bursting_nodes.is_empty());
    }

    #[test]
    fn elastic_pack_uses_observed_workload_memory_for_mixed_nodes() {
        let mut state = ControlPlaneState::default();
        let mut node_a = test_node("node-a", 1_000, 1_000);
        node_a.observed_running_instances = Some(2);
        node_a.observed_memory_mib = Some(800);
        node_a
            .observed_workloads
            .insert(test_workload_key("big"), 1);
        node_a
            .observed_workloads
            .insert(test_workload_key("small"), 1);
        node_a
            .observed_workload_memory_mib
            .insert(test_workload_key("big"), 700);
        node_a
            .observed_workload_memory_mib
            .insert(test_workload_key("small"), 100);
        state.upsert_node(node_a).expect("node-a");
        state
            .upsert_node(test_node("node-b", 1_000, 1_000))
            .expect("node-b");

        state
            .upsert_deployment(test_deployment(
                "alpha",
                PlacementMode::ElasticPack,
                1,
                100,
                250,
            ))
            .expect("alpha deployment");
        state
            .upsert_deployment(test_deployment(
                "big",
                PlacementMode::ElasticPack,
                1,
                100,
                400,
            ))
            .expect("big deployment");
        state
            .upsert_deployment(test_deployment(
                "small",
                PlacementMode::ElasticPack,
                1,
                100,
                100,
            ))
            .expect("small deployment");

        let plan = build_plan(&state).expect("plan");

        assert_eq!(plan.instances.len(), 3);
        assert_eq!(
            plan.instances
                .iter()
                .find(|instance| instance.deployment == test_workload_key("big"))
                .expect("big instance")
                .node,
            "node-a"
        );
        assert_eq!(
            plan.instances
                .iter()
                .find(|instance| instance.deployment == test_workload_key("alpha"))
                .expect("alpha instance")
                .node,
            "node-b"
        );
        assert_eq!(
            plan.instances
                .iter()
                .find(|instance| instance.deployment == test_workload_key("small"))
                .expect("small instance")
                .node,
            "node-a"
        );
    }

    #[test]
    fn elastic_pack_matches_observed_cpu_by_workload_not_iteration_order() {
        let mut state = ControlPlaneState::default();
        let mut node_a = test_node("node-a", 1_000, 1_000);
        node_a.observed_running_instances = Some(1);
        node_a
            .observed_workloads
            .insert(test_workload_key("zeta"), 1);
        state.upsert_node(node_a).expect("node-a");
        state
            .upsert_node(test_node("node-b", 1_000, 1_000))
            .expect("node-b");

        state
            .upsert_deployment(test_deployment(
                "alpha",
                PlacementMode::ElasticPack,
                1,
                300,
                100,
            ))
            .expect("alpha deployment");
        state
            .upsert_deployment(test_deployment(
                "zeta",
                PlacementMode::ElasticPack,
                1,
                800,
                100,
            ))
            .expect("zeta deployment");

        let plan = build_plan(&state).expect("plan");

        let placements = plan
            .instances
            .iter()
            .map(|instance| (instance.deployment.as_str(), instance.node.as_str()))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(
            placements.get("tenant-a/media/alpha").copied(),
            Some("node-b")
        );
        assert_eq!(
            placements.get("tenant-a/media/zeta").copied(),
            Some("node-a")
        );
    }

    #[test]
    fn elastic_pack_can_replace_observed_workload_on_full_node() {
        let mut state = ControlPlaneState::default();
        let mut node_a = test_node("node-a", 1_000, 1_000);
        node_a.capacity_slots = 1;
        node_a.observed_running_instances = Some(1);
        node_a.observed_memory_mib = Some(700);
        node_a
            .observed_workloads
            .insert(test_workload_key("alpha"), 1);
        state.upsert_node(node_a).expect("node-a");

        state
            .upsert_deployment(test_deployment(
                "beta",
                PlacementMode::ElasticPack,
                1,
                800,
                700,
            ))
            .expect("beta deployment");

        let plan = build_plan(&state).expect("plan");

        assert_eq!(plan.instances.len(), 1);
        assert_eq!(plan.instances[0].deployment, "tenant-a/media/beta");
        assert_eq!(plan.instances[0].node, "node-a");
    }

    #[test]
    fn elastic_pack_caps_unplanned_scale_down_to_desired_replicas() {
        let mut state = ControlPlaneState::default();
        let mut node_a = test_node("node-a", 1_000, 1_000);
        node_a.capacity_slots = 2;
        node_a.observed_running_instances = Some(2);
        node_a.observed_memory_mib = Some(200);
        node_a
            .observed_workloads
            .insert(test_workload_key("zeta"), 2);
        state.upsert_node(node_a).expect("node-a");

        state
            .upsert_deployment(test_deployment(
                "alpha",
                PlacementMode::ElasticPack,
                1,
                100,
                100,
            ))
            .expect("alpha deployment");
        state
            .upsert_deployment(test_deployment(
                "zeta",
                PlacementMode::ElasticPack,
                1,
                100,
                100,
            ))
            .expect("zeta deployment");

        let plan = build_plan(&state).expect("plan");

        assert_eq!(plan.instances.len(), 2);
        assert!(
            plan.instances
                .iter()
                .all(|instance| instance.node == "node-a")
        );
    }

    #[test]
    fn usage_contains_contract_refs() {
        let state = sample_state();
        let usage = deployment_contract_usage(&state);
        let ingest = usage.get("tenant-a/media/ingest").expect("ingest usage");
        assert!(ingest.contains("media.pipeline/event:camera.frames@v1"));
    }

    #[test]
    fn builds_endpoint_bridge_intents_per_source_instance() {
        let state = sample_state();
        let plan = build_plan(&state).expect("plan");

        let intents = build_endpoint_bridge_intents(&state, &plan);
        assert_eq!(intents.len(), 2);
        assert_eq!(
            intents[0].source_endpoint.key(),
            "tenant-a/media/ingest#event:camera.frames"
        );
        assert_eq!(
            intents[0].target_endpoint.key(),
            "tenant-a/media/detector#event:camera.frames"
        );
        assert_eq!(intents[0].source_node, "local-node");
        assert_eq!(intents[0].target_node, "local-node");
        assert_eq!(
            intents[0].target_instance_id,
            "tenant=tenant-a;namespace=media;workload=detector;replica=0"
        );
        assert!(
            intents[0]
                .bridge_id
                .starts_with(
                    "tenant=tenant-a;namespace=media;workload=ingest;replica=0->tenant=tenant-a;namespace=media;workload=detector;replica=0::"
                )
        );
    }
}
