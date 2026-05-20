use selium_abi::{
    Capability, CapabilityGrant, CompletionState, HostcallOutput, HostcallRequest, LocalityScope,
    ResourceClass, ResourceSelector,
};
use selium_runtime::{ReadinessCondition, Runtime, RuntimeConfig, SystemGuestDescriptor};

#[test]
fn foundation_crates_work_together_through_hostcalls() {
    let runtime = Runtime::default();
    let report = runtime
        .bootstrap_system_guests(RuntimeConfig {
            system_guests: vec![SystemGuestDescriptor {
                name: "cluster".to_string(),
                module_id: "cluster-module".to_string(),
                module_bytes: module_with_entrypoint("boot"),
                entrypoint: "boot".to_string(),
                arguments: Vec::new(),
                grants: vec![CapabilityGrant::new(
                    Capability::Signal,
                    vec![ResourceSelector::ResourceClass(ResourceClass::Signal)],
                )],
                dependencies: Vec::new(),
                readiness: ReadinessCondition::Immediate,
            }],
        })
        .expect("bootstrap runtime");
    assert_eq!(report.guests.len(), 1);

    let (status, create_id) =
        runtime.begin_hostcall(report.guests[0].process_id, HostcallRequest::SignalCreate);
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_READY);
    let CompletionState::Ready(HostcallOutput::Signal(signal)) =
        runtime.poll_hostcall(report.guests[0].process_id, create_id)
    else {
        panic!("expected signal descriptor");
    };

    let (status, notify_id) = runtime.begin_hostcall(
        report.guests[0].process_id,
        HostcallRequest::SignalNotify {
            local_id: signal.local_id,
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_READY);
    assert_eq!(
        runtime.poll_hostcall(report.guests[0].process_id, notify_id),
        CompletionState::Ready(HostcallOutput::SignalGeneration(1))
    );
}

#[test]
fn hostcalls_enforce_session_grants() {
    let runtime = Runtime::default();
    let report = runtime
        .bootstrap_system_guests(RuntimeConfig {
            system_guests: vec![SystemGuestDescriptor {
                name: "restricted".to_string(),
                module_id: "restricted-module".to_string(),
                module_bytes: module_with_entrypoint("boot"),
                entrypoint: "boot".to_string(),
                arguments: Vec::new(),
                grants: vec![CapabilityGrant::new(
                    Capability::ProcessLifecycle,
                    vec![ResourceSelector::Locality(LocalityScope::Cluster)],
                )],
                dependencies: Vec::new(),
                readiness: ReadinessCondition::Immediate,
            }],
        })
        .expect("bootstrap runtime");

    let (status, operation_id) =
        runtime.begin_hostcall(report.guests[0].process_id, HostcallRequest::SignalCreate);

    assert_eq!(status, selium_abi::HOSTCALL_STATUS_FAILED);
    assert!(matches!(
        runtime.poll_hostcall(report.guests[0].process_id, operation_id),
        CompletionState::Failed(_)
    ));
}

fn module_with_entrypoint(entrypoint: &str) -> Vec<u8> {
    wat::parse_str(format!("(module (func (export \"{entrypoint}\")))")).expect("compile wat")
}
