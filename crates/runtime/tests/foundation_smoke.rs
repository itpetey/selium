use selium_abi::{Capability, CapabilityGrant, LocalityScope, ResourceClass, ResourceSelector};
use selium_guest::{
    ActivityLogHandle, GuestContext, GuestHost, PatternFabric, ProcessHandle, SharedMemoryHandle,
};
use selium_runtime::{Runtime, RuntimeConfig, SystemGuestDescriptor};

fn module_with_entrypoint(entrypoint: &str) -> Vec<u8> {
    wat::parse_str(format!("(module (func (export \"{entrypoint}\")))")).expect("compile wat")
}

#[tokio::test(flavor = "current_thread")]
async fn foundation_crates_work_together() {
    let grants = vec![
        CapabilityGrant::new(
            Capability::ProcessLifecycle,
            vec![ResourceSelector::Locality(LocalityScope::Cluster)],
        ),
        CapabilityGrant::new(
            Capability::SharedMemory,
            vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
        ),
        CapabilityGrant::new(
            Capability::SharedMemory,
            vec![ResourceSelector::ResourceClass(
                ResourceClass::SharedMapping,
            )],
        ),
        CapabilityGrant::new(
            Capability::MeteringRead,
            vec![ResourceSelector::ResourceClass(
                ResourceClass::MeteringStream,
            )],
        ),
        CapabilityGrant::new(
            Capability::ActivityRead,
            vec![ResourceSelector::ResourceClass(ResourceClass::ActivityLog)],
        ),
    ];

    let runtime = Runtime::default();
    runtime
        .register_module_bytes("user-module".to_string(), module_with_entrypoint("main"))
        .expect("register module bytes");
    let report = runtime
        .bootstrap_system_guests(RuntimeConfig {
            system_guests: vec![SystemGuestDescriptor {
                name: "cluster".to_string(),
                module_id: "cluster-module".to_string(),
                module_bytes: module_with_entrypoint("boot"),
                entrypoint: "boot".to_string(),
                arguments: Vec::new(),
                grants: grants.clone(),
                dependencies: Vec::new(),
                readiness: selium_runtime::ReadinessCondition::Immediate,
            }],
        })
        .expect("bootstrap runtime");
    assert_eq!(report.guests.len(), 1);

    let context = GuestContext::new(
        runtime
            .guest_host(report.guests[0].session_id)
            .expect("create runtime guest host"),
    );
    let memory = SharedMemoryHandle::allocate(context.clone(), 64, 8).expect("allocate memory");
    memory.write(0, b"foundation").expect("write memory");
    assert_eq!(memory.read(0, 10).expect("read memory"), b"foundation");

    let process = ProcessHandle::start(
        context.clone(),
        "user-module",
        "main",
        Vec::new(),
        vec![CapabilityGrant::new(
            Capability::ProcessLifecycle,
            vec![ResourceSelector::Locality(LocalityScope::Cluster)],
        )],
    )
    .expect("start process");
    assert_eq!(process.descriptor().entrypoint, "main");
    assert!(process.metering().expect("read metering").is_none());

    let activity_log = ActivityLogHandle::open(context.clone()).expect("open activity log");
    assert!(
        activity_log
            .read_from(0)
            .expect("read activity log")
            .iter()
            .any(|event| event.message.contains("bootstrapped"))
    );

    let fabric = PatternFabric::new();
    fabric
        .register_request_reply::<StringPayload, StringPayload, _, _>(
            "echo",
            |payload| async move { Ok(payload) },
        )
        .expect("register request reply");
    let echoed = fabric
        .request_reply::<StringPayload, StringPayload>("echo", &StringPayload("ready".to_string()))
        .await
        .expect("echo request");
    assert_eq!(echoed.0, "ready");
}

#[tokio::test(flavor = "current_thread")]
async fn runtime_guest_host_enforces_session_grants() {
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
                readiness: selium_runtime::ReadinessCondition::Immediate,
            }],
        })
        .expect("bootstrap runtime");

    let context = GuestContext::new(
        runtime
            .guest_host(report.guests[0].session_id)
            .expect("create runtime guest host"),
    );

    let result = SharedMemoryHandle::allocate(context, 64, 8);
    assert!(matches!(
        result,
        Err(selium_guest::GuestError::PermissionDenied(
            Capability::SharedMemory
        ))
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn runtime_guest_host_honours_scoped_tenant_context() {
    let runtime = Runtime::default();
    let report = runtime
        .bootstrap_system_guests(RuntimeConfig {
            system_guests: vec![SystemGuestDescriptor {
                name: "tenant".to_string(),
                module_id: "tenant-module".to_string(),
                module_bytes: module_with_entrypoint("boot"),
                entrypoint: "boot".to_string(),
                arguments: Vec::new(),
                grants: vec![
                    CapabilityGrant::new(
                        Capability::SharedMemory,
                        vec![
                            ResourceSelector::Tenant("acme".to_string()),
                            ResourceSelector::ResourceClass(ResourceClass::SharedRegion),
                        ],
                    ),
                    CapabilityGrant::new(
                        Capability::SharedMemory,
                        vec![
                            ResourceSelector::Tenant("acme".to_string()),
                            ResourceSelector::ResourceClass(ResourceClass::SharedMapping),
                        ],
                    ),
                ],
                dependencies: Vec::new(),
                readiness: selium_runtime::ReadinessCondition::Immediate,
            }],
        })
        .expect("bootstrap runtime");

    let context = GuestContext::new(
        runtime
            .guest_host(report.guests[0].session_id)
            .expect("create runtime guest host"),
    )
    .with_scope_context(selium_guest::ScopeContext {
        tenant: Some("other".to_string()),
        locality: LocalityScope::Cluster,
        ..selium_guest::ScopeContext::default()
    });

    let result = SharedMemoryHandle::allocate(context, 64, 8);
    assert!(matches!(
        result,
        Err(selium_guest::GuestError::PermissionDenied(
            Capability::SharedMemory
        ))
    ));

    let allowed = GuestContext::new(
        runtime
            .guest_host(report.guests[0].session_id)
            .expect("create runtime guest host"),
    )
    .with_scope_context(selium_guest::ScopeContext {
        tenant: Some("acme".to_string()),
        locality: LocalityScope::Cluster,
        ..selium_guest::ScopeContext::default()
    });

    assert!(SharedMemoryHandle::allocate(allowed, 64, 8).is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn runtime_guest_host_rejects_cross_session_local_handle_use() {
    let runtime = Runtime::default();
    let grants = vec![
        CapabilityGrant::new(
            Capability::SharedMemory,
            vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
        ),
        CapabilityGrant::new(
            Capability::SharedMemory,
            vec![ResourceSelector::ResourceClass(
                ResourceClass::SharedMapping,
            )],
        ),
    ];

    let report_a = runtime
        .bootstrap_system_guests(RuntimeConfig {
            system_guests: vec![SystemGuestDescriptor {
                name: "tenant-a".to_string(),
                module_id: "tenant-a-module".to_string(),
                module_bytes: module_with_entrypoint("boot"),
                entrypoint: "boot".to_string(),
                arguments: Vec::new(),
                grants: grants.clone(),
                dependencies: Vec::new(),
                readiness: selium_runtime::ReadinessCondition::Immediate,
            }],
        })
        .expect("bootstrap runtime a");
    let report_b = runtime
        .bootstrap_system_guests(RuntimeConfig {
            system_guests: vec![SystemGuestDescriptor {
                name: "tenant-b".to_string(),
                module_id: "tenant-b-module".to_string(),
                module_bytes: module_with_entrypoint("boot"),
                entrypoint: "boot".to_string(),
                arguments: Vec::new(),
                grants: grants.clone(),
                dependencies: Vec::new(),
                readiness: selium_runtime::ReadinessCondition::Immediate,
            }],
        })
        .expect("bootstrap runtime b");

    let context_a = GuestContext::new(
        runtime
            .guest_host(report_a.guests[0].session_id)
            .expect("guest host a"),
    );
    let host_b = runtime
        .guest_host(report_b.guests[0].session_id)
        .expect("guest host b");

    let memory = SharedMemoryHandle::allocate(context_a, 64, 8).expect("allocate memory");
    let local_id = memory.descriptor().local_id;

    let result = host_b.read_shared_memory(local_id, 0, 1);
    assert!(matches!(
        result,
        Err(selium_abi::HostError::PermissionDenied(
            Capability::SharedMemory
        ))
    ));
}

#[derive(Debug, Clone, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
#[rkyv(bytecheck())]
struct StringPayload(String);
