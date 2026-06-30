use selium_abi::{
    Capability, CapabilityGrant, CompletionState, HostcallOutput, HostcallRequest, LocalityScope,
    RegionProt, ResourceClass, ResourceSelector,
};
use selium_runtime::{ReadinessCondition, Runtime, RuntimeConfig, SystemGuestDescriptor};

#[test]
fn foundation_crates_work_together_through_hostcalls() {
    let runtime = Runtime::default();
    let report = runtime
        .bootstrap_system_guests(RuntimeConfig {
            start_discovery: false,
            system_guests: vec![SystemGuestDescriptor {
                name: "cluster".to_string(),
                module_id: "cluster-module".to_string(),
                module_bytes: module_with_entrypoint("boot"),
                entrypoint: "boot".to_string(),
                arguments: Vec::new(),
                grants: vec![CapabilityGrant::new(
                    Capability::SharedMemory,
                    vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
                )],
                dependencies: Vec::new(),
                readiness: ReadinessCondition::Immediate,
            }],
        })
        .expect("bootstrap runtime");
    assert_eq!(report.guests.len(), 1);

    // Allocate a shared region using the new ABI.
    let (status, alloc_id) = runtime.begin_hostcall(
        report.guests[0].process_id,
        HostcallRequest::AllocRegion {
            pages: 1,
            prot: RegionProt::ReadWrite,
            purpose: selium_abi::ResourceKind::SharedMemory,
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_READY);
    let CompletionState::Ready(HostcallOutput::RegionAlloc(allocation)) =
        runtime.poll_hostcall(report.guests[0].process_id, alloc_id)
    else {
        panic!("expected region allocation");
    };
    assert!(allocation.region_id > 0);
}

#[test]
fn hostcalls_enforce_session_grants() {
    let runtime = Runtime::default();
    let report = runtime
        .bootstrap_system_guests(RuntimeConfig {
            start_discovery: false,
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

    // Attempt to allocate a region without SharedMemory capability.
    let (status, operation_id) = runtime.begin_hostcall(
        report.guests[0].process_id,
        HostcallRequest::AllocRegion {
            pages: 1,
            prot: RegionProt::ReadWrite,
            purpose: selium_abi::ResourceKind::SharedMemory,
        },
    );

    assert_eq!(status, selium_abi::HOSTCALL_STATUS_FAILED);
    assert!(matches!(
        runtime.poll_hostcall(report.guests[0].process_id, operation_id),
        CompletionState::Failed(_)
    ));
}

fn module_with_entrypoint(entrypoint: &str) -> Vec<u8> {
    wat::parse_str(format!("(module (func (export \"{entrypoint}\")))")).expect("compile wat")
}
