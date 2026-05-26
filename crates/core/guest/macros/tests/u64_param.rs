use selium_guest::{Capability, CapabilityGrant, LocalityScope, ResourceSelector, entrypoint};
use selium_runtime::{Runtime, RuntimeConfig, SystemGuestDescriptor};

fn encode_u64_argument(value: u64) -> Vec<u8> {
    let mut bytes = vec![1u8]; // WasmValue::I64 tag
    bytes.extend_from_slice(&value.to_le_bytes());
    bytes
}

#[test]
fn entrypoint_with_u64_param_receives_argument() {
    let runtime = Runtime::default();
    let mut descriptor = SystemGuestDescriptor::from_entrypoint_metadata(
        "param-guest",
        "param-module",
        module_with_entrypoint_and_i64_param("param_entrypoint"),
        param_entrypoint_entrypoint_metadata(),
        vec![CapabilityGrant::new(
            Capability::ProcessLifecycle,
            vec![ResourceSelector::Locality(LocalityScope::Cluster)],
        )],
    );
    descriptor.arguments = vec![encode_u64_argument(42)];

    let config = RuntimeConfig {
        system_guests: vec![descriptor],
    };

    let report = runtime
        .bootstrap_system_guests(config)
        .expect("bootstrap runtime");
    assert_eq!(report.guests.len(), 1);
}

fn module_with_entrypoint_and_i64_param(entrypoint: &str) -> Vec<u8> {
    wat::parse_str(format!(
        "(module
            (import \"selium\" \"process_id\" (func $process_id (result i64)))
            (import \"selium\" \"mark_ready\" (func $mark_ready))
            (func (export \"{entrypoint}\") (param i64)
                call $mark_ready
                call $process_id
                drop))"
    ))
    .expect("compile wat")
}

#[entrypoint]
async fn param_entrypoint(_handle: u64) {
    tracing::info!("param entrypoint invoked");
}
