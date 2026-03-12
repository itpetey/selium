use std::collections::{BTreeMap, BTreeSet};

use selium_abi::DataValue;
use selium_control_plane_api::{DeploymentSpec, IsolationProfile, NodeSpec, PipelineSpec};
use selium_control_plane_core::{
    serialize_deployment_resources, serialize_external_account_ref, serialize_optional_u32,
};
use selium_control_plane_scheduler::SchedulePlan;
use selium_io_tables::{TableApplyResult, TableRecord};

pub(super) fn ok_status() -> DataValue {
    DataValue::Map(BTreeMap::from([(
        "status".to_string(),
        DataValue::from("ok"),
    )]))
}

pub(super) fn serialize_deployment_spec(spec: &DeploymentSpec) -> DataValue {
    DataValue::Map(BTreeMap::from([
        ("workload".to_string(), DataValue::from(spec.workload.key())),
        ("module".to_string(), DataValue::from(spec.module.clone())),
        ("replicas".to_string(), DataValue::from(spec.replicas)),
        (
            "external_account_ref".to_string(),
            serialize_external_account_ref(&spec.external_account_ref),
        ),
        (
            "isolation".to_string(),
            DataValue::from(format!("{:?}", spec.isolation)),
        ),
        (
            "resources".to_string(),
            serialize_deployment_resources(spec),
        ),
    ]))
}

pub(super) fn serialize_pipeline_spec(spec: &PipelineSpec) -> DataValue {
    DataValue::Map(BTreeMap::from([
        ("pipeline".to_string(), DataValue::from(spec.key())),
        (
            "external_account_ref".to_string(),
            serialize_external_account_ref(&spec.external_account_ref),
        ),
        ("edge_count".to_string(), DataValue::from(spec.edges.len())),
    ]))
}

pub(super) fn serialize_node_spec(spec: &NodeSpec) -> DataValue {
    DataValue::Map(BTreeMap::from([
        ("name".to_string(), DataValue::from(spec.name.clone())),
        (
            "capacity_slots".to_string(),
            DataValue::from(spec.capacity_slots),
        ),
        (
            "allocatable_cpu_millis".to_string(),
            serialize_optional_u32(spec.allocatable_cpu_millis),
        ),
        (
            "allocatable_memory_mib".to_string(),
            serialize_optional_u32(spec.allocatable_memory_mib),
        ),
        (
            "supported_isolation".to_string(),
            serialize_isolation_profiles(&spec.supported_isolation),
        ),
        (
            "daemon_addr".to_string(),
            DataValue::from(spec.daemon_addr.clone()),
        ),
        (
            "daemon_server_name".to_string(),
            DataValue::from(spec.daemon_server_name.clone()),
        ),
        (
            "last_heartbeat_ms".to_string(),
            DataValue::from(spec.last_heartbeat_ms),
        ),
    ]))
}

pub(super) fn serialize_schedule_plan(plan: &SchedulePlan) -> DataValue {
    DataValue::Map(BTreeMap::from([
        (
            "instances".to_string(),
            DataValue::List(
                plan.instances
                    .iter()
                    .map(|instance| {
                        DataValue::Map(BTreeMap::from([
                            (
                                "deployment".to_string(),
                                DataValue::from(instance.deployment.clone()),
                            ),
                            (
                                "instance_id".to_string(),
                                DataValue::from(instance.instance_id.clone()),
                            ),
                            ("node".to_string(), DataValue::from(instance.node.clone())),
                            (
                                "isolation".to_string(),
                                DataValue::from(format!("{:?}", instance.isolation)),
                            ),
                        ]))
                    })
                    .collect(),
            ),
        ),
        (
            "node_slots".to_string(),
            serialize_string_u32_map(&plan.node_slots),
        ),
        (
            "node_cpu_millis".to_string(),
            serialize_string_u32_map(&plan.node_cpu_millis),
        ),
        (
            "node_memory_mib".to_string(),
            serialize_string_u32_map(&plan.node_memory_mib),
        ),
    ]))
}

pub(super) fn serialize_string_u32_map(values: &BTreeMap<String, u32>) -> DataValue {
    DataValue::Map(
        values
            .iter()
            .map(|(key, value)| (key.clone(), DataValue::from(*value)))
            .collect(),
    )
}

pub(super) fn serialize_string_set_map(values: &BTreeMap<String, BTreeSet<String>>) -> DataValue {
    DataValue::Map(
        values
            .iter()
            .map(|(key, value)| {
                (
                    key.clone(),
                    DataValue::List(value.iter().cloned().map(DataValue::from).collect()),
                )
            })
            .collect(),
    )
}

pub(super) fn serialize_isolation_profiles(profiles: &[IsolationProfile]) -> DataValue {
    DataValue::List(
        profiles
            .iter()
            .map(|profile| DataValue::from(format!("{:?}", profile)))
            .collect(),
    )
}

pub(super) fn serialize_table_record(record: &TableRecord) -> DataValue {
    DataValue::Map(BTreeMap::from([
        ("value".to_string(), record.value.clone()),
        ("version".to_string(), DataValue::from(record.version)),
        (
            "updated_index".to_string(),
            DataValue::from(record.updated_index),
        ),
    ]))
}

pub(super) fn serialize_table_result(result: TableApplyResult) -> DataValue {
    match result {
        TableApplyResult::Updated {
            table,
            key,
            version,
        } => DataValue::Map(BTreeMap::from([
            ("status".to_string(), DataValue::from("updated")),
            ("table".to_string(), DataValue::from(table)),
            ("key".to_string(), DataValue::from(key)),
            ("version".to_string(), DataValue::from(version)),
        ])),
        TableApplyResult::Deleted { table, key } => DataValue::Map(BTreeMap::from([
            ("status".to_string(), DataValue::from("deleted")),
            ("table".to_string(), DataValue::from(table)),
            ("key".to_string(), DataValue::from(key)),
        ])),
        TableApplyResult::ViewCreated { name } => DataValue::Map(BTreeMap::from([(
            "status".to_string(),
            DataValue::from(format!("view_created:{name}")),
        )])),
    }
}
