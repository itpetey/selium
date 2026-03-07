use selium_control_plane_api::{DeploymentSpec, IsolationProfile};

const DEFAULT_CAPABILITIES: &[&str] = &[
    "session_lifecycle",
    "process_lifecycle",
    "time_read",
    "shared_memory",
    "queue_lifecycle",
    "queue_writer",
    "queue_reader",
];

pub fn deployment_module_spec(deployment: &DeploymentSpec) -> String {
    let (adaptor, profile) = match deployment.isolation {
        IsolationProfile::Standard => ("wasmtime", "standard"),
        IsolationProfile::Hardened => ("wasmtime", "hardened"),
        IsolationProfile::Microvm => ("microvm", "microvm"),
    };

    build_module_spec(&deployment.module, adaptor, profile, default_capabilities())
}

pub fn build_module_spec(
    module: &str,
    adaptor: &str,
    profile: &str,
    capabilities: Vec<String>,
) -> String {
    format!(
        "path={};capabilities={};adaptor={};profile={}",
        module,
        capabilities.join(","),
        adaptor,
        profile
    )
}

pub fn default_capabilities() -> Vec<String> {
    DEFAULT_CAPABILITIES
        .iter()
        .map(|capability| (*capability).to_string())
        .collect()
}
