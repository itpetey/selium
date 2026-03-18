use std::collections::BTreeMap;

use selium_abi::DataValue;
use selium_control_plane_api::{DeploymentSpec, ExternalAccountRef};

/// Serialize deployment resource requirements into a stable `DataValue` projection.
pub fn serialize_deployment_resources(spec: &DeploymentSpec) -> DataValue {
    DataValue::Map(BTreeMap::from([
        (
            "placement_mode".to_string(),
            DataValue::from(spec.placement_mode.as_str()),
        ),
        ("cpu_millis".to_string(), DataValue::from(spec.cpu_millis)),
        ("memory_mib".to_string(), DataValue::from(spec.memory_mib)),
        (
            "ephemeral_storage_mib".to_string(),
            DataValue::from(spec.ephemeral_storage_mib),
        ),
        (
            "bandwidth_profile".to_string(),
            DataValue::from(spec.bandwidth_profile.as_str()),
        ),
        (
            "volume_mounts".to_string(),
            DataValue::List(
                spec.volume_mounts
                    .iter()
                    .map(|mount| {
                        DataValue::Map(BTreeMap::from([
                            ("name".to_string(), DataValue::from(mount.name.clone())),
                            (
                                "mount_path".to_string(),
                                DataValue::from(mount.mount_path.clone()),
                            ),
                            ("read_only".to_string(), DataValue::from(mount.read_only)),
                        ]))
                    })
                    .collect(),
            ),
        ),
    ]))
}

/// Serialize an optional integer as either `Null` or the integer value.
pub fn serialize_optional_u32(value: Option<u32>) -> DataValue {
    value.map_or(DataValue::Null, DataValue::from)
}

/// Serialize an optional external account reference as either `Null` or its key.
pub fn serialize_external_account_ref(
    external_account_ref: &Option<ExternalAccountRef>,
) -> DataValue {
    external_account_ref
        .as_ref()
        .map_or(DataValue::Null, |reference| {
            DataValue::from(reference.key.clone())
        })
}
