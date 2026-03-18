use std::collections::{BTreeMap, BTreeSet};

use selium_abi::DataValue;
use selium_control_plane_api::{ControlPlaneState, DeploymentSpec, NodeSpec, PipelineSpec};
use selium_control_plane_core::{
    AttributedInfrastructureFilter, serialize_deployment_resources, serialize_external_account_ref,
    serialize_optional_u32,
};
use selium_control_plane_scheduler::SchedulePlan;

use crate::values::serialize_isolation_profiles;

#[derive(Debug, Clone)]
struct InventoryInstance {
    deployment: String,
    workload: String,
    module: String,
    external_account_ref: Option<String>,
    node: String,
    instance_id: String,
    isolation: String,
}

#[derive(Debug, Default)]
struct ModuleInventory {
    deployments: usize,
    workloads: BTreeSet<String>,
    external_account_refs: BTreeSet<String>,
    nodes: BTreeSet<String>,
}

pub(super) fn serialize_attributed_infrastructure_inventory(
    state: &ControlPlaneState,
    plan: &SchedulePlan,
    filter: &AttributedInfrastructureFilter,
    last_applied: u64,
) -> DataValue {
    let instances = build_inventory_instances(state, plan);
    let mut instances_by_workload = BTreeMap::<String, Vec<InventoryInstance>>::new();
    let mut instances_by_node = BTreeMap::<String, Vec<InventoryInstance>>::new();
    for instance in instances {
        instances_by_workload
            .entry(instance.workload.clone())
            .or_default()
            .push(instance.clone());
        instances_by_node
            .entry(instance.node.clone())
            .or_default()
            .push(instance);
    }

    let pipeline_scope = filter.pipeline.as_deref().map(|pipeline_key| {
        state
            .pipelines
            .get(pipeline_key)
            .map(pipeline_workload_keys)
            .unwrap_or_default()
    });

    let workloads = state
        .deployments
        .values()
        .filter(|deployment| {
            let scheduled = instances_by_workload
                .get(&deployment.workload.key())
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            workload_matches_filter(deployment, scheduled, filter, pipeline_scope.as_ref())
        })
        .collect::<Vec<_>>();

    let workload_values = workloads
        .iter()
        .map(|deployment| {
            let scheduled = instances_by_workload
                .get(&deployment.workload.key())
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            serialize_inventory_workload(deployment, scheduled)
        })
        .collect::<Vec<_>>();

    let mut modules = BTreeMap::<String, ModuleInventory>::new();
    for deployment in &workloads {
        let scheduled = instances_by_workload
            .get(&deployment.workload.key())
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let module = modules.entry(deployment.module.clone()).or_default();
        module.deployments += 1;
        module.workloads.insert(deployment.workload.key());
        if let Some(reference) = &deployment.external_account_ref {
            module.external_account_refs.insert(reference.key.clone());
        }
        for instance in scheduled {
            module.nodes.insert(instance.node.clone());
        }
    }

    let pipeline_values = state
        .pipelines
        .values()
        .filter(|pipeline| pipeline_matches_filter(pipeline, state, &instances_by_workload, filter))
        .map(serialize_inventory_pipeline)
        .collect::<Vec<_>>();

    let module_values = modules
        .into_iter()
        .map(|(module_name, module)| {
            DataValue::Map(BTreeMap::from([
                ("module".to_string(), DataValue::from(module_name)),
                (
                    "deployment_count".to_string(),
                    DataValue::from(module.deployments as u64),
                ),
                (
                    "workloads".to_string(),
                    DataValue::List(module.workloads.into_iter().map(DataValue::from).collect()),
                ),
                (
                    "external_account_refs".to_string(),
                    DataValue::List(
                        module
                            .external_account_refs
                            .into_iter()
                            .map(DataValue::from)
                            .collect(),
                    ),
                ),
                (
                    "nodes".to_string(),
                    DataValue::List(module.nodes.into_iter().map(DataValue::from).collect()),
                ),
            ]))
        })
        .collect::<Vec<_>>();

    let node_values = state
        .nodes
        .values()
        .filter_map(|node| {
            let scheduled = instances_by_node
                .get(&node.name)
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            let filtered_instances = scheduled
                .iter()
                .filter(|instance| {
                    instance_matches_filter(instance, filter, pipeline_scope.as_ref())
                })
                .cloned()
                .collect::<Vec<_>>();
            if node_matches_filter(node, &filtered_instances, filter) {
                Some(serialize_inventory_node(node, &filtered_instances))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    DataValue::Map(BTreeMap::from([
        (
            "snapshot_marker".to_string(),
            DataValue::Map(BTreeMap::from([(
                "last_applied".to_string(),
                DataValue::from(last_applied),
            )])),
        ),
        ("filters".to_string(), serialize_inventory_filter(filter)),
        ("workloads".to_string(), DataValue::List(workload_values)),
        ("pipelines".to_string(), DataValue::List(pipeline_values)),
        ("modules".to_string(), DataValue::List(module_values)),
        ("nodes".to_string(), DataValue::List(node_values)),
    ]))
}

fn build_inventory_instances(
    state: &ControlPlaneState,
    plan: &SchedulePlan,
) -> Vec<InventoryInstance> {
    plan.instances
        .iter()
        .filter_map(|instance| {
            let deployment = state.deployments.get(&instance.deployment)?;
            Some(InventoryInstance {
                deployment: instance.deployment.clone(),
                workload: deployment.workload.key(),
                module: deployment.module.clone(),
                external_account_ref: deployment
                    .external_account_ref
                    .as_ref()
                    .map(|reference| reference.key.clone()),
                node: instance.node.clone(),
                instance_id: instance.instance_id.clone(),
                isolation: format!("{:?}", instance.isolation),
            })
        })
        .collect()
}

fn serialize_inventory_filter(filter: &AttributedInfrastructureFilter) -> DataValue {
    DataValue::Map(BTreeMap::from([
        (
            "external_account_ref".to_string(),
            filter
                .external_account_ref
                .clone()
                .map_or(DataValue::Null, DataValue::from),
        ),
        (
            "workload".to_string(),
            filter
                .workload
                .clone()
                .map_or(DataValue::Null, DataValue::from),
        ),
        (
            "module".to_string(),
            filter
                .module
                .clone()
                .map_or(DataValue::Null, DataValue::from),
        ),
        (
            "pipeline".to_string(),
            filter
                .pipeline
                .clone()
                .map_or(DataValue::Null, DataValue::from),
        ),
        (
            "node".to_string(),
            filter.node.clone().map_or(DataValue::Null, DataValue::from),
        ),
    ]))
}

fn serialize_inventory_workload(
    spec: &DeploymentSpec,
    instances: &[InventoryInstance],
) -> DataValue {
    let mut node_names = BTreeSet::new();
    for instance in instances {
        node_names.insert(instance.node.clone());
    }
    DataValue::Map(BTreeMap::from([
        ("workload".to_string(), DataValue::from(spec.workload.key())),
        ("module".to_string(), DataValue::from(spec.module.clone())),
        (
            "external_account_ref".to_string(),
            serialize_external_account_ref(&spec.external_account_ref),
        ),
        ("replicas".to_string(), DataValue::from(spec.replicas)),
        (
            "isolation".to_string(),
            DataValue::from(format!("{:?}", spec.isolation)),
        ),
        (
            "resources".to_string(),
            serialize_deployment_resources(spec),
        ),
        (
            "contracts".to_string(),
            DataValue::List(
                spec.contracts
                    .iter()
                    .map(|contract| {
                        DataValue::Map(BTreeMap::from([
                            (
                                "namespace".to_string(),
                                DataValue::from(contract.namespace.clone()),
                            ),
                            ("kind".to_string(), DataValue::from(contract.kind.as_str())),
                            ("name".to_string(), DataValue::from(contract.name.clone())),
                            (
                                "version".to_string(),
                                DataValue::from(contract.version.clone()),
                            ),
                        ]))
                    })
                    .collect(),
            ),
        ),
        (
            "nodes".to_string(),
            DataValue::List(node_names.into_iter().map(DataValue::from).collect()),
        ),
        (
            "scheduled_instances".to_string(),
            DataValue::List(instances.iter().map(serialize_inventory_instance).collect()),
        ),
    ]))
}

fn serialize_inventory_pipeline(spec: &PipelineSpec) -> DataValue {
    let workloads = pipeline_workload_keys(spec);
    DataValue::Map(BTreeMap::from([
        ("pipeline".to_string(), DataValue::from(spec.key())),
        (
            "external_account_ref".to_string(),
            serialize_external_account_ref(&spec.external_account_ref),
        ),
        (
            "edge_count".to_string(),
            DataValue::from(spec.edges.len() as u64),
        ),
        (
            "workloads".to_string(),
            DataValue::List(workloads.into_iter().map(DataValue::from).collect()),
        ),
        (
            "edges".to_string(),
            DataValue::List(
                spec.edges
                    .iter()
                    .map(|edge| {
                        DataValue::Map(BTreeMap::from([
                            (
                                "from".to_string(),
                                DataValue::from(edge.from.endpoint.key()),
                            ),
                            ("to".to_string(), DataValue::from(edge.to.endpoint.key())),
                            (
                                "contract".to_string(),
                                DataValue::Map(BTreeMap::from([
                                    (
                                        "namespace".to_string(),
                                        DataValue::from(edge.from.contract.namespace.clone()),
                                    ),
                                    (
                                        "kind".to_string(),
                                        DataValue::from(edge.from.contract.kind.as_str()),
                                    ),
                                    (
                                        "name".to_string(),
                                        DataValue::from(edge.from.contract.name.clone()),
                                    ),
                                    (
                                        "version".to_string(),
                                        DataValue::from(edge.from.contract.version.clone()),
                                    ),
                                ])),
                            ),
                        ]))
                    })
                    .collect(),
            ),
        ),
    ]))
}

fn serialize_inventory_node(node: &NodeSpec, instances: &[InventoryInstance]) -> DataValue {
    let mut external_account_refs = BTreeSet::new();
    for instance in instances {
        if let Some(reference) = &instance.external_account_ref {
            external_account_refs.insert(reference.clone());
        }
    }
    DataValue::Map(BTreeMap::from([
        ("name".to_string(), DataValue::from(node.name.clone())),
        (
            "capacity_slots".to_string(),
            DataValue::from(node.capacity_slots),
        ),
        (
            "allocatable_cpu_millis".to_string(),
            serialize_optional_u32(node.allocatable_cpu_millis),
        ),
        (
            "allocatable_memory_mib".to_string(),
            serialize_optional_u32(node.allocatable_memory_mib),
        ),
        (
            "reserve_cpu_utilisation_ppm".to_string(),
            DataValue::from(node.reserve_cpu_utilisation_ppm),
        ),
        (
            "reserve_memory_utilisation_ppm".to_string(),
            DataValue::from(node.reserve_memory_utilisation_ppm),
        ),
        (
            "reserve_slots_utilisation_ppm".to_string(),
            DataValue::from(node.reserve_slots_utilisation_ppm),
        ),
        (
            "observed_running_instances".to_string(),
            serialize_optional_u32(node.observed_running_instances),
        ),
        (
            "observed_active_bridges".to_string(),
            serialize_optional_u32(node.observed_active_bridges),
        ),
        (
            "observed_memory_mib".to_string(),
            serialize_optional_u32(node.observed_memory_mib),
        ),
        (
            "observed_workloads".to_string(),
            DataValue::Map(
                node.observed_workloads
                    .iter()
                    .map(|(workload, count)| (workload.clone(), DataValue::from(*count)))
                    .collect(),
            ),
        ),
        (
            "observed_workload_memory_mib".to_string(),
            DataValue::Map(
                node.observed_workload_memory_mib
                    .iter()
                    .map(|(workload, memory_mib)| (workload.clone(), DataValue::from(*memory_mib)))
                    .collect(),
            ),
        ),
        (
            "supported_isolation".to_string(),
            serialize_isolation_profiles(&node.supported_isolation),
        ),
        (
            "daemon_addr".to_string(),
            DataValue::from(node.daemon_addr.clone()),
        ),
        (
            "daemon_server_name".to_string(),
            DataValue::from(node.daemon_server_name.clone()),
        ),
        (
            "last_heartbeat_ms".to_string(),
            DataValue::from(node.last_heartbeat_ms),
        ),
        (
            "external_account_refs".to_string(),
            DataValue::List(
                external_account_refs
                    .into_iter()
                    .map(DataValue::from)
                    .collect(),
            ),
        ),
        (
            "scheduled_instances".to_string(),
            DataValue::List(instances.iter().map(serialize_inventory_instance).collect()),
        ),
    ]))
}

fn serialize_inventory_instance(instance: &InventoryInstance) -> DataValue {
    DataValue::Map(BTreeMap::from([
        (
            "deployment".to_string(),
            DataValue::from(instance.deployment.clone()),
        ),
        (
            "workload".to_string(),
            DataValue::from(instance.workload.clone()),
        ),
        (
            "module".to_string(),
            DataValue::from(instance.module.clone()),
        ),
        (
            "external_account_ref".to_string(),
            instance
                .external_account_ref
                .clone()
                .map_or(DataValue::Null, DataValue::from),
        ),
        ("node".to_string(), DataValue::from(instance.node.clone())),
        (
            "instance_id".to_string(),
            DataValue::from(instance.instance_id.clone()),
        ),
        (
            "isolation".to_string(),
            DataValue::from(instance.isolation.clone()),
        ),
    ]))
}

fn pipeline_workload_keys(spec: &PipelineSpec) -> BTreeSet<String> {
    let mut workloads = BTreeSet::new();
    for edge in &spec.edges {
        workloads.insert(edge.from.endpoint.workload.key());
        workloads.insert(edge.to.endpoint.workload.key());
    }
    workloads
}

fn workload_matches_filter(
    spec: &DeploymentSpec,
    instances: &[InventoryInstance],
    filter: &AttributedInfrastructureFilter,
    pipeline_scope: Option<&BTreeSet<String>>,
) -> bool {
    filter.matches_external_account_ref(
        spec.external_account_ref
            .as_ref()
            .map(|value| value.key.as_str()),
    ) && filter.matches_workload(&spec.workload.key())
        && filter.matches_module(&spec.module)
        && filter
            .node
            .as_deref()
            .is_none_or(|node| instances.iter().any(|instance| instance.node == node))
        && filter.matches_pipeline_scope(&spec.workload.key(), pipeline_scope)
}

fn pipeline_matches_filter(
    spec: &PipelineSpec,
    state: &ControlPlaneState,
    instances_by_workload: &BTreeMap<String, Vec<InventoryInstance>>,
    filter: &AttributedInfrastructureFilter,
) -> bool {
    filter.matches_pipeline(&spec.key())
        && filter.matches_external_account_ref(
            spec.external_account_ref
                .as_ref()
                .map(|value| value.key.as_str()),
        )
        && filter.workload.as_deref().is_none_or(|workload| {
            spec.edges.iter().any(|edge| {
                edge.from.endpoint.workload.key() == workload
                    || edge.to.endpoint.workload.key() == workload
            })
        })
        && filter.module.as_deref().is_none_or(|module| {
            spec.edges.iter().any(|edge| {
                [
                    edge.from.endpoint.workload.key(),
                    edge.to.endpoint.workload.key(),
                ]
                .into_iter()
                .any(|workload_key| {
                    state
                        .deployments
                        .get(&workload_key)
                        .is_some_and(|deployment| deployment.module == module)
                })
            })
        })
        && filter.node.as_deref().is_none_or(|node| {
            spec.edges.iter().any(|edge| {
                [
                    edge.from.endpoint.workload.key(),
                    edge.to.endpoint.workload.key(),
                ]
                .into_iter()
                .any(|workload_key| {
                    instances_by_workload
                        .get(&workload_key)
                        .is_some_and(|instances| {
                            instances.iter().any(|instance| instance.node == node)
                        })
                })
            })
        })
}

fn instance_matches_filter(
    instance: &InventoryInstance,
    filter: &AttributedInfrastructureFilter,
    pipeline_scope: Option<&BTreeSet<String>>,
) -> bool {
    filter.matches_external_account_ref(instance.external_account_ref.as_deref())
        && filter.matches_workload(&instance.workload)
        && filter.matches_module(&instance.module)
        && filter.matches_node(&instance.node)
        && filter.matches_pipeline_scope(&instance.workload, pipeline_scope)
}

fn node_matches_filter(
    node: &NodeSpec,
    filtered_instances: &[InventoryInstance],
    filter: &AttributedInfrastructureFilter,
) -> bool {
    if !filter.matches_node(&node.name) {
        return false;
    }
    if filter.external_account_ref.is_some()
        || filter.workload.is_some()
        || filter.module.is_some()
        || filter.pipeline.is_some()
    {
        return !filtered_instances.is_empty();
    }
    true
}
