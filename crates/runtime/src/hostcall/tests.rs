//! Hostcall dispatcher test suite.

use super::*;
use crate::{Runtime, SystemGuestDescriptor};
use selium_abi::{
    Capability, CapabilityGrant, CompletionState, GuestLogEntry, HostcallOutput, HostcallRequest,
    MeteringObservation, Namespace, OperationId, ProcessId, ResourceClass, ResourceIdentity,
    ResourceSelector, SharedResourceId,
};

fn module_with_entrypoint(entrypoint: &str, body: &str) -> Vec<u8> {
    wat::parse_str(format!("(module (func (export \"{entrypoint}\") {body}))"))
        .expect("compile wat")
}

/// Allocates a shared region for `process_id`, returning its region id.
fn allocate_region(runtime: &Runtime, process_id: ProcessId) -> SharedResourceId {
    let (status, op) = runtime.begin_hostcall(
        process_id,
        HostcallRequest::AllocRegion {
            pages: 1,
            prot: selium_abi::RegionProt::ReadWrite,
            purpose: selium_abi::ResourceKind::SharedMemory,
            serving_tenant: None,
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_READY);
    match runtime.poll_hostcall(process_id, op) {
        CompletionState::Ready(HostcallOutput::RegionAlloc(alloc)) => alloc.region_id,
        other => panic!("expected RegionAlloc, got {other:?}"),
    }
}

/// A create-time rejection still names an operation, so the guest can poll
/// the structured reason (and, for a version mismatch, both versions)
/// instead of seeing only a failed status code.
#[test]
fn a_rejected_create_carries_its_structured_error() {
    let runtime = Runtime::default();
    let guest = spawn_with_grants(&runtime, Vec::new());
    let guest_version = selium_abi::SELIUM_ABI_VERSION + 1;
    let error = selium_abi::HostcallPreambleError::VersionMismatch {
        host: selium_abi::SELIUM_ABI_VERSION,
        guest: guest_version,
    }
    .to_abi_error();

    let (status, operation) = runtime.reject_hostcall(guest.process_id, error);
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_FAILED);
    match runtime.poll_hostcall(guest.process_id, operation) {
        CompletionState::Failed(error) => {
            assert_eq!(error.code, AbiErrorCode::MalformedPayload);
            assert!(
                error
                    .message
                    .contains(&selium_abi::SELIUM_ABI_VERSION.to_string()),
                "the host version is named: {}",
                error.message
            );
            assert!(
                error.message.contains(&guest_version.to_string()),
                "the guest version is named: {}",
                error.message
            );
        }
        other => panic!("expected a structured failure, got {other:?}"),
    }
    assert!(
        runtime.drop_hostcall(guest.process_id, operation),
        "the failed operation is reclaimed on drop"
    );
}

#[test]
fn non_cooperative_rejections_cannot_exceed_the_process_operation_budget() {
    let runtime = Runtime::default();
    let guest = spawn_with_grants(&runtime, Vec::new());
    let error = AbiError::new(AbiErrorCode::MalformedPayload, "hostile request");

    for _ in 0..MAX_OPERATIONS_PER_PROCESS {
        let (status, operation_id) = runtime.reject_hostcall(guest.process_id, error.clone());
        assert_eq!(status, selium_abi::HOSTCALL_STATUS_FAILED);
        assert_ne!(operation_id, 0);
    }
    assert_eq!(runtime.operations.lock().len(), MAX_OPERATIONS_PER_PROCESS);

    // The guest never polls or drops any completion. Further malformed
    // requests fail immediately without allocating another host record.
    for _ in 0..32 {
        assert_eq!(
            runtime.reject_hostcall(guest.process_id, error.clone()),
            (selium_abi::HOSTCALL_STATUS_FAILED, 0)
        );
    }
    assert_eq!(runtime.operations.lock().len(), MAX_OPERATIONS_PER_PROCESS);
}

fn spawn_with_grants(runtime: &Runtime, grants: Vec<CapabilityGrant>) -> crate::BootstrappedGuest {
    runtime
        .spawn_system_guest(SystemGuestDescriptor {
            name: "hostcall-test".to_string(),
            module_id: "hostcall-test-module".to_string(),
            module_bytes: module_with_entrypoint("boot", ""),
            entrypoint: "boot".to_string(),
            arguments: Vec::new(),
            grants,
            tenant: None,
            discovery_service: false,
            handlers: Vec::new(),
        })
        .expect("spawn hostcall test guest")
}

fn spawn_with_grants_and_tenant(
    runtime: &Runtime,
    name: &str,
    grants: Vec<CapabilityGrant>,
    tenant: Option<&str>,
) -> crate::BootstrappedGuest {
    runtime
        .spawn_system_guest(SystemGuestDescriptor {
            name: name.to_string(),
            module_id: format!("{name}-module"),
            module_bytes: module_with_entrypoint("boot", ""),
            entrypoint: "boot".to_string(),
            arguments: Vec::new(),
            grants,
            tenant: tenant.map(str::to_string),
            discovery_service: false,
            handlers: Vec::new(),
        })
        .expect("spawn hostcall test guest")
}

fn ready(runtime: &Runtime, process_id: ProcessId, operation_id: OperationId) -> HostcallOutput {
    match runtime.poll_hostcall(process_id, operation_id) {
        CompletionState::Ready(output) => output,
        other => panic!("expected ready hostcall, got {other:?}"),
    }
}

#[test]
fn operation_ids_roll_over_without_saturating() {
    let runtime = Runtime::default();
    let bootstrapped = runtime
        .spawn_system_guest(SystemGuestDescriptor {
            name: "rollover".to_string(),
            module_id: "rollover-module".to_string(),
            module_bytes: module_with_entrypoint("boot", ""),
            entrypoint: "boot".to_string(),
            arguments: Vec::new(),
            grants: vec![CapabilityGrant::new(
                Capability::SharedMemory,
                vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
            )],
            tenant: None,
            discovery_service: false,
            handlers: Vec::new(),
        })
        .expect("spawn rollover guest");
    *runtime.next_operation_id.lock() = OperationId::MAX;

    let (first_status, first_id) = runtime.begin_hostcall(
        bootstrapped.process_id,
        HostcallRequest::AllocRegion {
            pages: 1,
            prot: selium_abi::RegionProt::ReadWrite,
            purpose: selium_abi::ResourceKind::SharedMemory,
            serving_tenant: None,
        },
    );
    let (second_status, second_id) = runtime.begin_hostcall(
        bootstrapped.process_id,
        HostcallRequest::AllocRegion {
            pages: 1,
            prot: selium_abi::RegionProt::ReadWrite,
            purpose: selium_abi::ResourceKind::SharedMemory,
            serving_tenant: None,
        },
    );

    assert_eq!(first_status, selium_abi::HOSTCALL_STATUS_READY);
    assert_eq!(second_status, selium_abi::HOSTCALL_STATUS_READY);
    assert_eq!(first_id, OperationId::MAX);
    assert_eq!(second_id, 1);
}
// scenario: selium-abi/volume-hostcall-variants/bytes-round-trip
// scenario: selium-abi/volume-hostcall-variants/append-never-rewrites-earlier-bytes

#[test]
fn volume_hostcalls_cover_the_byte_container_lifecycle() {
    let runtime = Runtime::default();
    let bootstrapped = spawn_with_grants(
        &runtime,
        vec![CapabilityGrant::new(
            Capability::Storage,
            vec![ResourceSelector::ResourceClass(ResourceClass::Volume)],
        )],
    );

    let (_, open_op) = runtime.begin_hostcall(
        bootstrapped.process_id,
        HostcallRequest::VolumeOpen {
            name: "orders".to_string(),
            serving_tenant: None,
        },
    );
    let HostcallOutput::Volume(volume) = ready(&runtime, bootstrapped.process_id, open_op) else {
        panic!("expected volume descriptor");
    };
    assert_eq!(volume.len, 0);

    let (_, append_op) = runtime.begin_hostcall(
        bootstrapped.process_id,
        HostcallRequest::VolumeAppend {
            local_id: volume.local_id,
            bytes: b"hello".to_vec(),
        },
    );
    assert_eq!(
        ready(&runtime, bootstrapped.process_id, append_op),
        HostcallOutput::U64(0)
    );
    let (_, second_append_op) = runtime.begin_hostcall(
        bootstrapped.process_id,
        HostcallRequest::VolumeAppend {
            local_id: volume.local_id,
            bytes: b" world".to_vec(),
        },
    );
    assert_eq!(
        ready(&runtime, bootstrapped.process_id, second_append_op),
        HostcallOutput::U64(5)
    );

    let (_, read_op) = runtime.begin_hostcall(
        bootstrapped.process_id,
        HostcallRequest::VolumeReadAt {
            local_id: volume.local_id,
            offset: 6,
            len: 64,
        },
    );
    assert_eq!(
        ready(&runtime, bootstrapped.process_id, read_op),
        HostcallOutput::Bytes(b"world".to_vec())
    );

    let (_, len_op) = runtime.begin_hostcall(
        bootstrapped.process_id,
        HostcallRequest::VolumeLen {
            local_id: volume.local_id,
        },
    );
    assert_eq!(
        ready(&runtime, bootstrapped.process_id, len_op),
        HostcallOutput::U64(11)
    );

    let (_, list_op) = runtime.begin_hostcall(
        bootstrapped.process_id,
        HostcallRequest::VolumeList {
            serving_tenant: None,
        },
    );
    assert_eq!(
        ready(&runtime, bootstrapped.process_id, list_op),
        HostcallOutput::VolumeNames(vec!["orders".to_string()])
    );

    let (_, sync_op) = runtime.begin_hostcall(
        bootstrapped.process_id,
        HostcallRequest::VolumeSync {
            local_id: volume.local_id,
        },
    );
    assert_eq!(
        ready(&runtime, bootstrapped.process_id, sync_op),
        HostcallOutput::Empty
    );

    let (_, close_op) = runtime.begin_hostcall(
        bootstrapped.process_id,
        HostcallRequest::VolumeClose {
            local_id: volume.local_id,
        },
    );
    assert_eq!(
        ready(&runtime, bootstrapped.process_id, close_op),
        HostcallOutput::Empty
    );

    let (_, remove_op) = runtime.begin_hostcall(
        bootstrapped.process_id,
        HostcallRequest::VolumeRemove {
            name: "orders".to_string(),
            serving_tenant: None,
        },
    );
    assert_eq!(
        ready(&runtime, bootstrapped.process_id, remove_op),
        HostcallOutput::U64(11)
    );
}

// scenario: capability-enforcement/volume-hostcall-authorisation/capability-required
#[test]
fn volume_open_requires_the_storage_capability() {
    let runtime = Runtime::default();
    let bootstrapped = spawn_with_grants(&runtime, Vec::new());
    let (status, op) = runtime.begin_hostcall(
        bootstrapped.process_id,
        HostcallRequest::VolumeOpen {
            name: "orders".to_string(),
            serving_tenant: None,
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_FAILED);
    let CompletionState::Failed(error) = runtime.poll_hostcall(bootstrapped.process_id, op) else {
        panic!("expected permission denial");
    };
    assert_eq!(error.code, AbiErrorCode::PermissionDenied);
}

// scenario: capability-enforcement/volume-hostcall-authorisation/cross-tenant-open-denied
#[test]
fn volume_open_for_a_foreign_tenant_requires_delegation() {
    let runtime = Runtime::default();
    let bootstrapped = spawn_with_grants_and_tenant(
        &runtime,
        "acme-worker",
        vec![CapabilityGrant::new(
            Capability::Storage,
            vec![ResourceSelector::ResourceClass(ResourceClass::Volume)],
        )],
        Some("acme"),
    );
    let (status, op) = runtime.begin_hostcall(
        bootstrapped.process_id,
        HostcallRequest::VolumeOpen {
            name: "orders".to_string(),
            serving_tenant: Some("beta".to_string()),
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_FAILED);
    let CompletionState::Failed(error) = runtime.poll_hostcall(bootstrapped.process_id, op) else {
        panic!("expected cross-tenant denial");
    };
    assert_eq!(error.code, AbiErrorCode::PermissionDenied);
}

// scenario: selium-runtime/principal-provenance-allocation/volume-minted-under-the-serving-tenant
// scenario: capability-enforcement/volume-hostcall-authorisation/root-serves-a-tenant
#[test]
fn root_principal_opens_a_volume_under_a_named_tenant() {
    let runtime = Runtime::default();
    let bootstrapped = spawn_with_grants(
        &runtime,
        vec![CapabilityGrant::new(
            Capability::Storage,
            vec![ResourceSelector::ResourceClass(ResourceClass::Volume)],
        )],
    );
    let (_, open_op) = runtime.begin_hostcall(
        bootstrapped.process_id,
        HostcallRequest::VolumeOpen {
            name: "orders".to_string(),
            serving_tenant: Some("acme".to_string()),
        },
    );
    let HostcallOutput::Volume(volume) = ready(&runtime, bootstrapped.process_id, open_op) else {
        panic!("expected volume descriptor");
    };
    let (_, append_op) = runtime.begin_hostcall(
        bootstrapped.process_id,
        HostcallRequest::VolumeAppend {
            local_id: volume.local_id,
            bytes: b"acme-bytes".to_vec(),
        },
    );
    assert_eq!(
        ready(&runtime, bootstrapped.process_id, append_op),
        HostcallOutput::U64(0)
    );
    // The bytes are recorded under `acme`, not the root namespace.
    assert_eq!(
        runtime
            .kernel
            .volumes()
            .tenant_usage(Some("acme"))
            .expect("acme usage"),
        10
    );
    assert_eq!(
        runtime
            .kernel
            .volumes()
            .tenant_usage(None)
            .expect("root usage"),
        0
    );
}

/// `UriPrefix` scopes a `Storage` grant over volume *names*: a prefix-scoped
/// grant admits the volumes under it and refuses the rest, both when opening a
/// volume and when spawning from a durable slice on one.
// scenario: capability-enforcement/grant-time-evaluatable-honesty-for-uriprefix/volume-name-prefix-admitted
#[test]
fn a_volume_name_prefix_scopes_the_storage_grant_at_use_time() {
    let runtime = Runtime::default();
    let bootstrapped = spawn_with_grants_and_tenant(
        &runtime,
        "prefix-store",
        vec![CapabilityGrant::new(
            Capability::Storage,
            vec![
                ResourceSelector::ResourceClass(ResourceClass::Volume),
                ResourceSelector::UriPrefix("selium.control-plane".to_string()),
            ],
        )],
        Some("acme"),
    );
    let process_id = bootstrapped.process_id;

    // A volume under the prefix is admitted.
    let (status, open_op) = runtime.begin_hostcall(
        process_id,
        HostcallRequest::VolumeOpen {
            name: "selium.control-plane.modules".to_string(),
            serving_tenant: None,
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_READY);
    ready(&runtime, process_id, open_op);

    // A volume outside it is refused.
    let (status, _op) = runtime.begin_hostcall(
        process_id,
        HostcallRequest::VolumeOpen {
            name: "acme.private".to_string(),
            serving_tenant: None,
        },
    );
    assert_eq!(
        status,
        selium_abi::HOSTCALL_STATUS_FAILED,
        "a volume outside the granted prefix is denied"
    );

    // The same prefix bounds a module slice read at spawn.
    let spawner = spawn_with_grants_and_tenant(
        &runtime,
        "prefix-spawner",
        vec![
            CapabilityGrant::new(
                Capability::Storage,
                vec![
                    ResourceSelector::ResourceClass(ResourceClass::Volume),
                    ResourceSelector::UriPrefix("selium.control-plane".to_string()),
                ],
            ),
            CapabilityGrant::new(
                Capability::ProcessLifecycle,
                vec![ResourceSelector::ResourceClass(ResourceClass::Process)],
            ),
        ],
        Some("acme"),
    );
    let (status, _op) = runtime.begin_hostcall(
        spawner.process_id,
        HostcallRequest::ProcessStart {
            source: selium_abi::ModuleSource::Slice {
                volume: "acme.private".to_string(),
                offset: 0,
                len: 16,
            },
            entrypoint: "main".to_string(),
            arguments: Vec::new(),
            grants: Vec::new(),
            tenant: None,
        },
    );
    assert_eq!(
        status,
        selium_abi::HOSTCALL_STATUS_FAILED,
        "a slice on a volume outside the granted prefix is denied"
    );
}

// scenario: selium-runtime/volume-hostcall-enforcement/append-beyond-the-ceiling-denied
// scenario: selium-accountant/quota-enforcement-at-allocation/append-over-volume-quota-denied
#[test]
fn append_beyond_the_volume_ceiling_writes_no_bytes() {
    let runtime = Runtime::default();
    runtime.kernel.quota().set("acme", ResourceClass::Volume, 4);

    let bootstrapped = spawn_with_grants_and_tenant(
        &runtime,
        "acme-store",
        vec![CapabilityGrant::new(
            Capability::Storage,
            vec![ResourceSelector::ResourceClass(ResourceClass::Volume)],
        )],
        Some("acme"),
    );
    let (_, open_op) = runtime.begin_hostcall(
        bootstrapped.process_id,
        HostcallRequest::VolumeOpen {
            name: "orders".to_string(),
            serving_tenant: None,
        },
    );
    let HostcallOutput::Volume(volume) = ready(&runtime, bootstrapped.process_id, open_op) else {
        panic!("expected volume descriptor");
    };

    let (status, op) = runtime.begin_hostcall(
        bootstrapped.process_id,
        HostcallRequest::VolumeAppend {
            local_id: volume.local_id,
            bytes: b"oversized".to_vec(),
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_FAILED);
    let CompletionState::Failed(error) = runtime.poll_hostcall(bootstrapped.process_id, op) else {
        panic!("expected quota denial");
    };
    assert_eq!(error.code, AbiErrorCode::QuotaExceeded);
    assert!(
        error.message.contains("acme"),
        "names the tenant: {}",
        error.message
    );
    assert!(
        error.message.contains("Volume"),
        "names the dimension: {}",
        error.message
    );

    // No bytes were written: the volume stays empty and no usage is booked.
    let (_, len_op) = runtime.begin_hostcall(
        bootstrapped.process_id,
        HostcallRequest::VolumeLen {
            local_id: volume.local_id,
        },
    );
    assert_eq!(
        ready(&runtime, bootstrapped.process_id, len_op),
        HostcallOutput::U64(0)
    );
    assert_eq!(
        runtime.kernel.quota().used("acme", ResourceClass::Volume),
        0
    );
}
// scenario: selium-abi/volume-resource-class/volume-is-the-storage-quota-dimension
// scenario: selium-abi/tenant-volume-usage-hostcall/removal-releases-usage

// scenario: selium-runtime/volume-hostcall-enforcement/removal-releases-usage
#[test]
fn removing_a_volume_releases_its_usage() {
    let runtime = Runtime::default();
    let bootstrapped = spawn_with_grants_and_tenant(
        &runtime,
        "acme-store",
        vec![CapabilityGrant::new(
            Capability::Storage,
            vec![ResourceSelector::ResourceClass(ResourceClass::Volume)],
        )],
        Some("acme"),
    );
    let (_, open_op) = runtime.begin_hostcall(
        bootstrapped.process_id,
        HostcallRequest::VolumeOpen {
            name: "orders".to_string(),
            serving_tenant: None,
        },
    );
    let HostcallOutput::Volume(volume) = ready(&runtime, bootstrapped.process_id, open_op) else {
        panic!("expected volume descriptor");
    };
    let (_, append_op) = runtime.begin_hostcall(
        bootstrapped.process_id,
        HostcallRequest::VolumeAppend {
            local_id: volume.local_id,
            bytes: b"bytes".to_vec(),
        },
    );
    assert_eq!(
        ready(&runtime, bootstrapped.process_id, append_op),
        HostcallOutput::U64(0)
    );
    assert_eq!(
        runtime.kernel.quota().used("acme", ResourceClass::Volume),
        5
    );

    let (_, remove_op) = runtime.begin_hostcall(
        bootstrapped.process_id,
        HostcallRequest::VolumeRemove {
            name: "orders".to_string(),
            serving_tenant: None,
        },
    );
    assert_eq!(
        ready(&runtime, bootstrapped.process_id, remove_op),
        HostcallOutput::U64(5)
    );
    assert_eq!(
        runtime.kernel.quota().used("acme", ResourceClass::Volume),
        0
    );
}

#[test]
fn process_activity_metering_and_guest_log_hostcalls_work() {
    let runtime = Runtime::default();
    let bootstrapped = spawn_with_grants(
        &runtime,
        vec![
            CapabilityGrant::new(
                Capability::ProcessLifecycle,
                vec![ResourceSelector::Locality(
                    selium_abi::LocalityScope::Cluster,
                )],
            ),
            CapabilityGrant::new(
                Capability::ActivityRead,
                vec![ResourceSelector::ResourceClass(ResourceClass::ActivityLog)],
            ),
            CapabilityGrant::new(
                Capability::MeteringRead,
                vec![ResourceSelector::ResourceClass(
                    ResourceClass::MeteringStream,
                )],
            ),
            CapabilityGrant::new(
                Capability::GuestLogWrite,
                vec![ResourceSelector::ResourceClass(ResourceClass::GuestLog)],
            ),
            CapabilityGrant::new(
                Capability::GuestLogRead,
                vec![ResourceSelector::ResourceClass(ResourceClass::GuestLog)],
            ),
        ],
    );

    let child_grants = vec![CapabilityGrant::new(
        Capability::ProcessLifecycle,
        vec![ResourceSelector::Locality(
            selium_abi::LocalityScope::Cluster,
        )],
    )];
    let (_, start_op) = runtime.begin_hostcall(
        bootstrapped.process_id,
        HostcallRequest::ProcessStart {
            source: selium_abi::ModuleSource::Inline(module_with_entrypoint("main", "")),
            entrypoint: "main".to_string(),
            arguments: Vec::new(),
            grants: child_grants,
            tenant: None,
        },
    );
    let HostcallOutput::Process(child) = ready(&runtime, bootstrapped.process_id, start_op) else {
        panic!("expected child process");
    };
    runtime.project_metering(
        child.local_id,
        MeteringObservation {
            cpu_instructions: 1,
            memory_bytes: 2,
            bandwidth_bytes: 4,
        },
    );
    let (_, meter_op) = runtime.begin_hostcall(
        bootstrapped.process_id,
        HostcallRequest::MeteringRead {
            process_id: child.local_id,
        },
    );
    assert!(matches!(
        ready(&runtime, bootstrapped.process_id, meter_op),
        HostcallOutput::Metering(_)
    ));
    let entry = GuestLogEntry {
        process_id: Some(child.local_id),
        level: "INFO".to_string(),
        target: "test".to_string(),
        message: "hello".to_string(),
    };
    let (_, write_log_op) = runtime.begin_hostcall(
        bootstrapped.process_id,
        HostcallRequest::GuestLogWrite { entry },
    );
    assert_eq!(
        ready(&runtime, bootstrapped.process_id, write_log_op),
        HostcallOutput::Empty
    );
    let (_, read_log_op) = runtime.begin_hostcall(
        bootstrapped.process_id,
        HostcallRequest::GuestLogRead {
            cursor: 0,
            process_id: Some(child.local_id),
        },
    );
    let HostcallOutput::GuestLogEntries(entries) =
        ready(&runtime, bootstrapped.process_id, read_log_op)
    else {
        panic!("expected guest log entries");
    };
    assert_eq!(entries[0].message, "hello");
    let (_, activity_op) = runtime.begin_hostcall(
        bootstrapped.process_id,
        HostcallRequest::ActivityRead { cursor: 0 },
    );
    assert!(matches!(
        ready(&runtime, bootstrapped.process_id, activity_op),
        HostcallOutput::ActivityEvents(_)
    ));
    let (_, stop_op) = runtime.begin_hostcall(
        bootstrapped.process_id,
        HostcallRequest::ProcessStop {
            process_id: child.local_id,
        },
    );
    assert_eq!(
        ready(&runtime, bootstrapped.process_id, stop_op),
        HostcallOutput::Empty
    );
}

// scenario: selium-runtime/guestlogregister-hostcall-validation/guestlogregister-accepted-for-own-region
// scenario: selium-runtime/guestlogregister-hostcall-validation/guestlogregister-rejected-for-foreign-region
#[test]
fn guest_log_register_valid_and_foreign() {
    let runtime = Runtime::default();
    let bootstrapped = spawn_with_grants(
        &runtime,
        vec![CapabilityGrant::new(
            Capability::SharedMemory,
            vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
        )],
    );

    // Allocate a shared region owned by this process.
    let (_, alloc_op) = runtime.begin_hostcall(
        bootstrapped.process_id,
        HostcallRequest::AllocRegion {
            pages: 1,
            prot: selium_abi::RegionProt::ReadWrite,
            purpose: selium_abi::ResourceKind::LogChannel,
            serving_tenant: None,
        },
    );
    let HostcallOutput::RegionAlloc(alloc) = ready(&runtime, bootstrapped.process_id, alloc_op)
    else {
        panic!("expected RegionAlloc");
    };

    // GuestLogRegister with own shared_id should succeed.
    let (_, reg_op) = runtime.begin_hostcall(
        bootstrapped.process_id,
        HostcallRequest::GuestLogRegister {
            shared_id: alloc.region_id,
        },
    );
    assert_eq!(
        ready(&runtime, bootstrapped.process_id, reg_op),
        HostcallOutput::Empty
    );

    // Verify the kernel recorded the log channel.
    assert_eq!(
        runtime
            .kernel()
            .processes()
            .log_channel_shared_id(bootstrapped.process_id),
        Some(alloc.region_id)
    );

    // GuestLogRegister with a non-existent shared_id should fail.
    let (status, foreign_op) = runtime.begin_hostcall(
        bootstrapped.process_id,
        HostcallRequest::GuestLogRegister { shared_id: 99999 },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_FAILED);
    assert!(matches!(
        runtime.poll_hostcall(bootstrapped.process_id, foreign_op),
        CompletionState::Failed(_)
    ));
}

// scenario: selium-runtime/grant-admission-and-evaluation/errors-attribute-correctly
// scenario: capability-enforcement/evaluatable-scope-contexts/tenant-isolation-enforces
#[test]
fn tenant_scoped_grant_enforces_isolation() {
    let runtime = Runtime::default();

    // Process A: tenant "acme", grant scoped to Tenant("acme") + ResourceClass.
    let guest_a = runtime
        .spawn_system_guest(SystemGuestDescriptor {
            name: "tenant-a".to_string(),
            module_id: "tenant-a-module".to_string(),
            module_bytes: module_with_entrypoint("boot", ""),
            entrypoint: "boot".to_string(),
            arguments: Vec::new(),
            grants: vec![CapabilityGrant::new(
                Capability::SharedMemory,
                vec![
                    ResourceSelector::Tenant("acme".to_string()),
                    ResourceSelector::ResourceClass(ResourceClass::SharedRegion),
                ],
            )],
            tenant: Some("acme".to_string()),
            discovery_service: false,
            handlers: Vec::new(),
        })
        .expect("spawn tenant-a guest");

    // Process B: tenant "beta", same grant scoped to Tenant("acme").
    let guest_b = runtime
        .spawn_system_guest(SystemGuestDescriptor {
            name: "tenant-b".to_string(),
            module_id: "tenant-b-module".to_string(),
            module_bytes: module_with_entrypoint("boot", ""),
            entrypoint: "boot".to_string(),
            arguments: Vec::new(),
            grants: vec![CapabilityGrant::new(
                Capability::SharedMemory,
                vec![
                    ResourceSelector::Tenant("acme".to_string()),
                    ResourceSelector::ResourceClass(ResourceClass::SharedRegion),
                ],
            )],
            tenant: Some("beta".to_string()),
            discovery_service: false,
            handlers: Vec::new(),
        })
        .expect("spawn tenant-b guest");

    // Tenant "acme" process A: AllocRegion should succeed.
    let (status_a, _op_a) = runtime.begin_hostcall(
        guest_a.process_id,
        HostcallRequest::AllocRegion {
            pages: 1,
            prot: selium_abi::RegionProt::ReadWrite,
            purpose: selium_abi::ResourceKind::SharedMemory,
            serving_tenant: None,
        },
    );
    assert_eq!(status_a, selium_abi::HOSTCALL_STATUS_READY);

    // Tenant "beta" process B: AllocRegion should be denied.
    let (status_b, op_b) = runtime.begin_hostcall(
        guest_b.process_id,
        HostcallRequest::AllocRegion {
            pages: 1,
            prot: selium_abi::RegionProt::ReadWrite,
            purpose: selium_abi::ResourceKind::SharedMemory,
            serving_tenant: None,
        },
    );
    assert_eq!(status_b, selium_abi::HOSTCALL_STATUS_FAILED);

    // Verify error attribution names the capability and tenant.
    match runtime.poll_hostcall(guest_b.process_id, op_b) {
        CompletionState::Failed(error) => {
            assert!(
                error.message.contains("SharedMemory"),
                "error should name the denied capability: {error:?}"
            );
            assert!(
                error.message.contains("\"beta\""),
                "error should name the tenant: {error:?}"
            );
        }
        other => panic!("expected failed hostcall for tenant-b, got {other:?}"),
    }
}

// scenario: capability-enforcement/evaluatable-scope-contexts/selector-admission-at-grant-time
// scenario: capability-enforcement/grant-time-evaluatable-honesty-for-uriprefix/uriprefix-on-non-network-class-rejected
#[test]
fn unevaluatable_selector_rejected_at_spawn() {
    let runtime = Runtime::default();

    // A grant with UriPrefix should be rejected at spawn time — never
    // accepted and then always denied at hostcall time.
    let result = runtime.spawn_system_guest(SystemGuestDescriptor {
        name: "uri-prefix-rejected".to_string(),
        module_id: "uri-prefix-module".to_string(),
        module_bytes: module_with_entrypoint("boot", ""),
        entrypoint: "boot".to_string(),
        arguments: Vec::new(),
        grants: vec![CapabilityGrant::new(
            Capability::SharedMemory,
            vec![ResourceSelector::UriPrefix("sel://acme/".to_string())],
        )],
        tenant: None,
        discovery_service: false,
        handlers: Vec::new(),
    });

    let error = result.expect_err("should reject UriPrefix grant at spawn");
    assert!(
        error.to_string().contains("UriPrefix"),
        "error should name the rejected selector: {error}"
    );
}

// scenario: selium-runtime/grant-admission-and-evaluation/accept-then-deny-is-impossible
#[test]
fn accept_then_deny_trap_is_impossible() {
    let runtime = Runtime::default();
    let bootstrapped = spawn_with_grants(
        &runtime,
        vec![
            CapabilityGrant::new(
                Capability::ProcessLifecycle,
                vec![ResourceSelector::ResourceClass(ResourceClass::Process)],
            ),
            CapabilityGrant::new(
                Capability::SharedMemory,
                vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
            ),
        ],
    );

    // ProcessStart with a UriPrefix grant must fail immediately at spawn,
    // not pass validation and deny every subsequent hostcall.
    let (status, op) = runtime.begin_hostcall(
        bootstrapped.process_id,
        HostcallRequest::ProcessStart {
            source: selium_abi::ModuleSource::Inline(module_with_entrypoint("main", "")),
            entrypoint: "main".to_string(),
            arguments: Vec::new(),
            grants: vec![CapabilityGrant::new(
                Capability::SharedMemory,
                vec![ResourceSelector::UriPrefix("sel://acme/".to_string())],
            )],
            tenant: None,
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_FAILED);

    match runtime.poll_hostcall(bootstrapped.process_id, op) {
        CompletionState::Failed(error) => {
            assert!(
                error.message.contains("UriPrefix"),
                "error should name the rejected selector: {error:?}"
            );
            assert_eq!(error.code, AbiErrorCode::MalformedPayload);
        }
        other => panic!("expected failed hostcall for accept-then-deny trap, got {other:?}"),
    }
}

// scenario: capability-enforcement/scoped-grant-delegation/delegator-spawns-within-tenant
#[test]
fn delegator_within_tenant_spawns_child_with_extra_grants() {
    let runtime = Runtime::default();

    // A per-tenant bridge-server holding DelegateGrants(Tenant "acme")
    // but no Network grant of its own can still confer a tenant-scoped
    // Network grant on a child within the same tenant.
    let parent = spawn_with_grants_and_tenant(
        &runtime,
        "delegating-bridge",
        vec![
            CapabilityGrant::new(
                Capability::ProcessLifecycle,
                vec![ResourceSelector::ResourceClass(ResourceClass::Process)],
            ),
            CapabilityGrant::new(
                Capability::DelegateGrants,
                vec![ResourceSelector::Tenant("acme".to_string())],
            ),
        ],
        Some("acme"),
    );

    let (status, op) = runtime.begin_hostcall(
        parent.process_id,
        HostcallRequest::ProcessStart {
            source: selium_abi::ModuleSource::Inline(module_with_entrypoint("main", "")),
            entrypoint: "main".to_string(),
            arguments: Vec::new(),
            grants: vec![CapabilityGrant::new(
                Capability::Network,
                vec![
                    ResourceSelector::Tenant("acme".to_string()),
                    ResourceSelector::ResourceClass(ResourceClass::TcpStream),
                ],
            )],
            tenant: None,
        },
    );
    assert_eq!(
        status,
        selium_abi::HOSTCALL_STATUS_READY,
        "delegation within the tenant must succeed"
    );
    assert!(matches!(
        runtime.poll_hostcall(parent.process_id, op),
        CompletionState::Ready(HostcallOutput::Process(_))
    ));
}

// scenario: capability-enforcement/scoped-grant-delegation/non-delegator-out-of-scope-child-denied
#[test]
fn non_delegator_out_of_scope_child_denied() {
    let runtime = Runtime::default();

    let parent = spawn_with_grants_and_tenant(
        &runtime,
        "plain-parent",
        vec![CapabilityGrant::new(
            Capability::ProcessLifecycle,
            vec![ResourceSelector::ResourceClass(ResourceClass::Process)],
        )],
        Some("acme"),
    );

    let (status, op) = runtime.begin_hostcall(
        parent.process_id,
        HostcallRequest::ProcessStart {
            source: selium_abi::ModuleSource::Inline(module_with_entrypoint("main", "")),
            entrypoint: "main".to_string(),
            arguments: Vec::new(),
            grants: vec![CapabilityGrant::new(
                Capability::Network,
                vec![
                    ResourceSelector::Tenant("acme".to_string()),
                    ResourceSelector::ResourceClass(ResourceClass::TcpStream),
                ],
            )],
            tenant: None,
        },
    );
    assert_eq!(
        status,
        selium_abi::HOSTCALL_STATUS_FAILED,
        "a non-delegator child grant exceeding its own must be denied"
    );
    assert!(matches!(
        runtime.poll_hostcall(parent.process_id, op),
        CompletionState::Failed(error) if error.code == AbiErrorCode::PermissionDenied
    ));
}

// scenario: capability-enforcement/scoped-grant-delegation/delegation-outside-tenant-denied
#[test]
fn delegation_outside_tenant_denied() {
    let runtime = Runtime::default();

    let parent = spawn_with_grants_and_tenant(
        &runtime,
        "acme-bridge",
        vec![
            CapabilityGrant::new(
                Capability::ProcessLifecycle,
                vec![ResourceSelector::ResourceClass(ResourceClass::Process)],
            ),
            CapabilityGrant::new(
                Capability::DelegateGrants,
                vec![ResourceSelector::Tenant("acme".to_string())],
            ),
        ],
        Some("acme"),
    );

    let (status, op) = runtime.begin_hostcall(
        parent.process_id,
        HostcallRequest::ProcessStart {
            source: selium_abi::ModuleSource::Inline(module_with_entrypoint("main", "")),
            entrypoint: "main".to_string(),
            arguments: Vec::new(),
            grants: vec![CapabilityGrant::new(
                Capability::Network,
                vec![
                    ResourceSelector::Tenant("beta".to_string()),
                    ResourceSelector::ResourceClass(ResourceClass::TcpStream),
                ],
            )],
            tenant: None,
        },
    );
    assert_eq!(
        status,
        selium_abi::HOSTCALL_STATUS_FAILED,
        "delegation outside the tenant must be denied"
    );
    assert!(matches!(
        runtime.poll_hostcall(parent.process_id, op),
        CompletionState::Failed(error) if error.code == AbiErrorCode::PermissionDenied
    ));
}

// scenario: capability-enforcement/scoped-grant-delegation/unscoped-grant-not-conferable-under-delegation
#[test]
fn delegation_denies_unscoped_child_grant() {
    let runtime = Runtime::default();

    let parent = spawn_with_grants_and_tenant(
        &runtime,
        "acme-bridge",
        vec![
            CapabilityGrant::new(
                Capability::ProcessLifecycle,
                vec![ResourceSelector::ResourceClass(ResourceClass::Process)],
            ),
            CapabilityGrant::new(
                Capability::DelegateGrants,
                vec![ResourceSelector::Tenant("acme".to_string())],
            ),
        ],
        Some("acme"),
    );

    // The mixed set contains one tenant-scoped grant and one
    // unrestricted (empty-selector) grant. The unrestricted grant must
    // not be conferable under delegation: it escapes the tenant fence,
    // so the spawn falls through to the subset check and is denied.
    let (status, op) = runtime.begin_hostcall(
        parent.process_id,
        HostcallRequest::ProcessStart {
            source: selium_abi::ModuleSource::Inline(module_with_entrypoint("main", "")),
            entrypoint: "main".to_string(),
            arguments: Vec::new(),
            grants: vec![
                CapabilityGrant::new(
                    Capability::Network,
                    vec![
                        ResourceSelector::Tenant("acme".to_string()),
                        ResourceSelector::ResourceClass(ResourceClass::TcpStream),
                    ],
                ),
                CapabilityGrant::new(Capability::Storage, Vec::new()),
            ],
            tenant: None,
        },
    );
    assert_eq!(
        status,
        selium_abi::HOSTCALL_STATUS_FAILED,
        "an unscoped child grant must not ride the delegation path"
    );
    assert!(matches!(
        runtime.poll_hostcall(parent.process_id, op),
        CompletionState::Failed(error) if error.code == AbiErrorCode::PermissionDenied
    ));
}

/// A `Namespace::Root`-scoped delegator spawns a tenant-scoped child
/// whose grants the root delegator does not itself hold: the root
/// namespace is greater than any tenant, so any tenant-scoped child grant
/// is admitted.
// scenario: capability-enforcement/scoped-grant-delegation/root-delegator-spawns-for-any-tenant
#[test]
fn root_delegator_spawns_tenant_scoped_child() {
    let runtime = Runtime::default();

    let parent = spawn_with_grants_and_tenant(
        &runtime,
        "root-bridge",
        vec![
            CapabilityGrant::new(
                Capability::ProcessLifecycle,
                vec![ResourceSelector::ResourceClass(ResourceClass::Process)],
            ),
            CapabilityGrant::new(
                Capability::DelegateGrants,
                vec![ResourceSelector::Namespace(Namespace::Root)],
            ),
        ],
        None,
    );

    let (status, op) = runtime.begin_hostcall(
        parent.process_id,
        HostcallRequest::ProcessStart {
            source: selium_abi::ModuleSource::Inline(module_with_entrypoint("main", "")),
            entrypoint: "main".to_string(),
            arguments: Vec::new(),
            grants: vec![CapabilityGrant::new(
                Capability::Network,
                vec![
                    ResourceSelector::Tenant("acme".to_string()),
                    ResourceSelector::ResourceClass(ResourceClass::TcpStream),
                ],
            )],
            tenant: Some("acme".to_string()),
        },
    );
    assert_eq!(
        status,
        selium_abi::HOSTCALL_STATUS_READY,
        "a root delegator must admit any tenant-scoped child grant"
    );
    let child = match runtime.poll_hostcall(parent.process_id, op) {
        CompletionState::Ready(HostcallOutput::Process(child)) => child,
        other => panic!("expected child process descriptor, got {other:?}"),
    };
    assert_eq!(
        runtime.process_tenant(child.local_id).as_deref(),
        Some("acme"),
        "the root delegator spawns the child under the requested tenant"
    );
}

/// A root principal without a `DelegateGrants` grant cannot spawn a child
/// under a tenant: cross-tenant spawn authority is a grant, not an
/// accident of the parent's bootstrap tenant (the rejected
/// "root guest implicitly delegates" shortcut).
#[test]
fn root_principal_without_delegate_grants_cannot_spawn_tenant_child() {
    let runtime = Runtime::default();

    let parent = spawn_with_grants_and_tenant(
        &runtime,
        "root-spawner",
        vec![CapabilityGrant::new(
            Capability::ProcessLifecycle,
            vec![ResourceSelector::ResourceClass(ResourceClass::Process)],
        )],
        None,
    );

    let (status, op) = runtime.begin_hostcall(
        parent.process_id,
        HostcallRequest::ProcessStart {
            source: selium_abi::ModuleSource::Inline(module_with_entrypoint("main", "")),
            entrypoint: "main".to_string(),
            arguments: Vec::new(),
            grants: Vec::new(),
            tenant: Some("acme".to_string()),
        },
    );
    assert_eq!(
        status,
        selium_abi::HOSTCALL_STATUS_FAILED,
        "a root parent without DelegateGrants must not tenant-scope a spawn"
    );
    assert!(matches!(
        runtime.poll_hostcall(parent.process_id, op),
        CompletionState::Failed(error) if error.code == AbiErrorCode::PermissionDenied
    ));
}

/// A `Namespace::Root`-scoped delegator must still refuse an unscoped
/// (selector-less) child grant: such a grant is unrestricted within its
/// capability and not conferable under delegation — it falls through to
/// the subset check and is denied.
#[test]
fn root_delegator_denies_unscoped_child_grant() {
    let runtime = Runtime::default();

    let parent = spawn_with_grants_and_tenant(
        &runtime,
        "root-bridge",
        vec![
            CapabilityGrant::new(
                Capability::ProcessLifecycle,
                vec![ResourceSelector::ResourceClass(ResourceClass::Process)],
            ),
            CapabilityGrant::new(
                Capability::DelegateGrants,
                vec![ResourceSelector::Namespace(Namespace::Root)],
            ),
        ],
        None,
    );

    let (status, op) = runtime.begin_hostcall(
        parent.process_id,
        HostcallRequest::ProcessStart {
            source: selium_abi::ModuleSource::Inline(module_with_entrypoint("main", "")),
            entrypoint: "main".to_string(),
            arguments: Vec::new(),
            grants: vec![CapabilityGrant::new(Capability::Storage, Vec::new())],
            tenant: None,
        },
    );
    assert_eq!(
        status,
        selium_abi::HOSTCALL_STATUS_FAILED,
        "an unscoped child grant must not ride the root delegation path"
    );
    assert!(matches!(
        runtime.poll_hostcall(parent.process_id, op),
        CompletionState::Failed(error) if error.code == AbiErrorCode::PermissionDenied
    ));
}

/// A tenant-scoped delegator cannot spawn a child under a foreign tenant,
/// even with grants scoped to that foreign tenant: the requested child
/// tenant is outside its `DelegateGrants` scope (`resolve_spawn_tenant`).
#[test]
fn tenant_delegator_cannot_spawn_child_in_foreign_tenant() {
    let runtime = Runtime::default();

    let parent = spawn_with_grants_and_tenant(
        &runtime,
        "acme-bridge",
        vec![
            CapabilityGrant::new(
                Capability::ProcessLifecycle,
                vec![ResourceSelector::ResourceClass(ResourceClass::Process)],
            ),
            CapabilityGrant::new(
                Capability::DelegateGrants,
                vec![ResourceSelector::Tenant("acme".to_string())],
            ),
        ],
        Some("acme"),
    );

    let (status, op) = runtime.begin_hostcall(
        parent.process_id,
        HostcallRequest::ProcessStart {
            source: selium_abi::ModuleSource::Inline(module_with_entrypoint("main", "")),
            entrypoint: "main".to_string(),
            arguments: Vec::new(),
            grants: vec![
                CapabilityGrant::new(
                    Capability::Network,
                    vec![
                        ResourceSelector::Tenant("beta".to_string()),
                        ResourceSelector::ResourceClass(ResourceClass::TcpStream),
                    ],
                ),
                CapabilityGrant::new(
                    Capability::HostQueue,
                    vec![ResourceSelector::Tenant("beta".to_string())],
                ),
            ],
            tenant: Some("beta".to_string()),
        },
    );
    assert_eq!(
        status,
        selium_abi::HOSTCALL_STATUS_FAILED,
        "a tenant-scoped delegator must not spawn a child in a foreign tenant"
    );
    assert!(matches!(
        runtime.poll_hostcall(parent.process_id, op),
        CompletionState::Failed(error) if error.code == AbiErrorCode::PermissionDenied
    ));
}

// scenario: capability-enforcement/delegategrants-is-not-conferable/re-delegation-denied
#[test]
fn delegate_grants_cannot_be_conferred_on_children() {
    let runtime = Runtime::default();

    let parent = spawn_with_grants_and_tenant(
        &runtime,
        "acme-bridge",
        vec![
            CapabilityGrant::new(
                Capability::ProcessLifecycle,
                vec![ResourceSelector::ResourceClass(ResourceClass::Process)],
            ),
            CapabilityGrant::new(
                Capability::DelegateGrants,
                vec![ResourceSelector::Tenant("acme".to_string())],
            ),
        ],
        Some("acme"),
    );

    // Attempt to re-delegate `DelegateGrants` itself (tenant-scoped, so
    // the tenant fence alone would admit it): conferment must be denied
    // outright.
    let (status, op) = runtime.begin_hostcall(
        parent.process_id,
        HostcallRequest::ProcessStart {
            source: selium_abi::ModuleSource::Inline(module_with_entrypoint("main", "")),
            entrypoint: "main".to_string(),
            arguments: Vec::new(),
            grants: vec![CapabilityGrant::new(
                Capability::DelegateGrants,
                vec![ResourceSelector::Tenant("acme".to_string())],
            )],
            tenant: None,
        },
    );
    assert_eq!(
        status,
        selium_abi::HOSTCALL_STATUS_FAILED,
        "DelegateGrants must never be conferred on a child process"
    );
    assert!(matches!(
        runtime.poll_hostcall(parent.process_id, op),
        CompletionState::Failed(error) if error.code == AbiErrorCode::PermissionDenied
    ));
}

// scenario: route-claims/host-issued-route-claims/claim-is-not-delegable
// scenario: capability-enforcement/routeclaim-is-not-conferable/child-cannot-receive-a-route-claim
#[test]
fn route_claim_cannot_be_conferred_on_children() {
    let runtime = Runtime::default();
    let parent = spawn_with_grants_and_tenant(
        &runtime,
        "claim-holder",
        vec![
            CapabilityGrant::new(
                Capability::ProcessLifecycle,
                vec![ResourceSelector::ResourceClass(ResourceClass::Process)],
            ),
            CapabilityGrant::new(
                Capability::RouteClaim,
                vec![ResourceSelector::Uri("sel:///control".to_string())],
            ),
        ],
        None,
    );

    let (status, op) = runtime.begin_hostcall(
        parent.process_id,
        HostcallRequest::ProcessStart {
            source: selium_abi::ModuleSource::Inline(module_with_entrypoint("main", "")),
            entrypoint: "main".to_string(),
            arguments: Vec::new(),
            grants: vec![CapabilityGrant::new(
                Capability::RouteClaim,
                vec![ResourceSelector::Uri("sel:///control".to_string())],
            )],
            tenant: None,
        },
    );
    assert_eq!(
        status,
        selium_abi::HOSTCALL_STATUS_FAILED,
        "RouteClaim is non-delegable"
    );
    assert!(matches!(
        runtime.poll_hostcall(parent.process_id, op),
        CompletionState::Failed(error) if error.code == AbiErrorCode::PermissionDenied
    ));
}

// scenario: route-claims/host-issued-route-claims/exact-match-only
// scenario: capability-enforcement/routeclaim-is-not-conferable/bootstrap-claim-admitted
#[test]
fn route_claim_requires_one_exact_root_uri() {
    let runtime = Runtime::default();
    let descriptor = |capability: CapabilityGrant| SystemGuestDescriptor {
        name: "claim-guest".to_string(),
        module_id: "claim-guest-module".to_string(),
        module_bytes: module_with_entrypoint("boot", ""),
        entrypoint: "boot".to_string(),
        arguments: Vec::new(),
        grants: vec![capability],
        tenant: None,
        discovery_service: false,
        handlers: Vec::new(),
    };

    // Selector-less: vacuously claims every name.
    let selectorless = runtime.spawn_system_guest(descriptor(CapabilityGrant::new(
        Capability::RouteClaim,
        Vec::new(),
    )));
    assert!(selectorless.is_err(), "a selector-less claim is invalid");

    // Tenant-scoped: claims are root well-known names only.
    let tenant_scoped = runtime.spawn_system_guest(descriptor(CapabilityGrant::new(
        Capability::RouteClaim,
        vec![ResourceSelector::Uri("sel://acme/control".to_string())],
    )));
    assert!(
        tenant_scoped.is_err(),
        "a claim outside the root namespace is invalid"
    );

    // One exact root URI: admitted.
    let admitted = runtime.spawn_system_guest(descriptor(CapabilityGrant::new(
        Capability::RouteClaim,
        vec![ResourceSelector::Uri("sel:///control".to_string())],
    )));
    admitted.expect("bootstrap claim admitted");
}

#[test]
fn mint_certificate_is_bootstrap_provisionable() {
    let runtime = Runtime::default();

    // Bootstrap provisioning admits the capability: a system guest holding
    // `MintCertificate` starts normally and carries the grant in its
    // persisted authority.
    let bootstrapped = spawn_with_grants(
        &runtime,
        vec![CapabilityGrant::new(
            Capability::MintCertificate,
            Vec::new(),
        )],
    );

    assert!(
        runtime
            .restore_process_authority(bootstrapped.process_id)
            .is_some_and(|authority| authority
                .grants
                .iter()
                .any(|grant| grant.capability == Capability::MintCertificate))
    );
}

// scenario: capability-enforcement/mintcertificate-is-not-conferable/providing-mintcertificate-at-spawn-is-denied
#[test]
fn mint_certificate_cannot_be_conferred_on_children() {
    let runtime = Runtime::default();

    // A parent holding `MintCertificate` (bootstrap-provisioned) and
    // `ProcessLifecycle` cannot confer mint authority on a child: the
    // spawn is denied with a capability error, mirroring `DelegateGrants`.
    let parent = spawn_with_grants(
        &runtime,
        vec![
            CapabilityGrant::new(
                Capability::ProcessLifecycle,
                vec![ResourceSelector::ResourceClass(ResourceClass::Process)],
            ),
            CapabilityGrant::new(Capability::MintCertificate, Vec::new()),
        ],
    );

    let (status, op) = runtime.begin_hostcall(
        parent.process_id,
        HostcallRequest::ProcessStart {
            source: selium_abi::ModuleSource::Inline(module_with_entrypoint("main", "")),
            entrypoint: "main".to_string(),
            arguments: Vec::new(),
            grants: vec![CapabilityGrant::new(
                Capability::MintCertificate,
                Vec::new(),
            )],
            tenant: None,
        },
    );
    assert_eq!(
        status,
        selium_abi::HOSTCALL_STATUS_FAILED,
        "MintCertificate must never be conferred on a child process"
    );
    assert!(matches!(
        runtime.poll_hostcall(parent.process_id, op),
        CompletionState::Failed(error) if error.code == AbiErrorCode::PermissionDenied
    ));
}

#[test]
fn quota_write_is_bootstrap_provisionable() {
    let runtime = Runtime::default();

    // Bootstrap provisioning admits the capability: a system guest holding
    // `QuotaWrite` starts normally and carries the grant in its persisted
    // authority.
    let bootstrapped = spawn_with_grants(
        &runtime,
        vec![CapabilityGrant::new(Capability::QuotaWrite, Vec::new())],
    );

    assert!(
        runtime
            .restore_process_authority(bootstrapped.process_id)
            .is_some_and(|authority| authority
                .grants
                .iter()
                .any(|grant| grant.capability == Capability::QuotaWrite))
    );
}

// scenario: capability-enforcement/quotawrite-is-not-conferable/conferring-quotawrite-at-spawn-is-denied
#[test]
fn quota_write_cannot_be_conferred_on_children() {
    let runtime = Runtime::default();

    // A parent holding `QuotaWrite` (bootstrap-provisioned) and
    // `ProcessLifecycle` cannot confer quota authorship on a child: the
    // spawn is denied with a capability error, mirroring `MintCertificate`.
    let parent = spawn_with_grants(
        &runtime,
        vec![
            CapabilityGrant::new(
                Capability::ProcessLifecycle,
                vec![ResourceSelector::ResourceClass(ResourceClass::Process)],
            ),
            CapabilityGrant::new(Capability::QuotaWrite, Vec::new()),
        ],
    );

    let (status, op) = runtime.begin_hostcall(
        parent.process_id,
        HostcallRequest::ProcessStart {
            source: selium_abi::ModuleSource::Inline(module_with_entrypoint("main", "")),
            entrypoint: "main".to_string(),
            arguments: Vec::new(),
            grants: vec![CapabilityGrant::new(Capability::QuotaWrite, Vec::new())],
            tenant: None,
        },
    );
    assert_eq!(
        status,
        selium_abi::HOSTCALL_STATUS_FAILED,
        "QuotaWrite must never be conferred on a child process"
    );
    assert!(matches!(
        runtime.poll_hostcall(parent.process_id, op),
        CompletionState::Failed(error) if error.code == AbiErrorCode::PermissionDenied
    ));
}

// scenario: selium-abi/quotawrite-capability-variant/quota-hostcall-requires-the-capability
// scenario: selium-accountant/sole-quota-authorship/non-accountant-quota-write-denied
#[test]
fn quota_hostcalls_deny_without_quota_write() {
    let runtime = Runtime::default();
    // A guest without `QuotaWrite` is denied the quota hostcalls: quota
    // authorship is bootstrap-provisioned to the accounting guest alone.
    let guest = spawn_with_grants(
        &runtime,
        vec![CapabilityGrant::new(
            Capability::SharedMemory,
            vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
        )],
    );

    for request in [
        HostcallRequest::QuotaSet {
            tenant: "acme".to_string(),
            class: ResourceClass::SharedRegion,
            limit: 1024,
        },
        HostcallRequest::QuotaClear {
            tenant: "acme".to_string(),
            class: ResourceClass::SharedRegion,
        },
    ] {
        let (status, op) = runtime.begin_hostcall(guest.process_id, request);
        assert_eq!(status, selium_abi::HOSTCALL_STATUS_FAILED);
        assert!(matches!(
            runtime.poll_hostcall(guest.process_id, op),
            CompletionState::Failed(error) if error.code == AbiErrorCode::PermissionDenied
        ));
    }
}

// scenario: selium-accountant/quota-enforcement-at-allocation/allocation-over-quota-denied
// scenario: selium-accountant/quota-enforcement-at-allocation/quota-distinct-from-grant
#[test]
fn over_ceiling_shared_memory_allocation_is_denied() {
    let runtime = Runtime::default();
    // Author a 64 KiB shared-memory ceiling for `acme` (the runtime gates
    // `QuotaSet` behind `QuotaWrite`, covered by the test above; writing
    // the table directly keeps this test focused on enforcement).
    runtime
        .kernel
        .quota()
        .set("acme", ResourceClass::SharedRegion, 65_536);

    let tenant_guest = spawn_with_grants_and_tenant(
        &runtime,
        "acme-worker",
        vec![CapabilityGrant::new(
            Capability::SharedMemory,
            vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
        )],
        Some("acme"),
    );

    // One page fits the ceiling exactly; a second page exceeds it.
    let (first_status, _first_op) = runtime.begin_hostcall(
        tenant_guest.process_id,
        HostcallRequest::AllocRegion {
            pages: 1,
            prot: selium_abi::RegionProt::ReadWrite,
            purpose: selium_abi::ResourceKind::SharedMemory,
            serving_tenant: None,
        },
    );
    assert_eq!(first_status, selium_abi::HOSTCALL_STATUS_READY);

    let (second_status, second_op) = runtime.begin_hostcall(
        tenant_guest.process_id,
        HostcallRequest::AllocRegion {
            pages: 1,
            prot: selium_abi::RegionProt::ReadWrite,
            purpose: selium_abi::ResourceKind::SharedMemory,
            serving_tenant: None,
        },
    );
    assert_eq!(second_status, selium_abi::HOSTCALL_STATUS_FAILED);
    match runtime.poll_hostcall(tenant_guest.process_id, second_op) {
        CompletionState::Failed(error) => {
            assert_eq!(error.code, AbiErrorCode::QuotaExceeded);
            assert!(
                error.message.contains("acme"),
                "error names the tenant: {}",
                error.message
            );
            assert!(
                error.message.contains("SharedRegion"),
                "error names the dimension: {}",
                error.message
            );
        }
        other => panic!("expected quota denial, got {other:?}"),
    }
}

/// Pipe quotas meter queued *items*, not queue count: creating a queue
/// costs nothing, each queued item reserves one slot against the queue
/// owner's tenant, an over-ceiling send is denied with `QuotaExceeded`,
/// and receiving releases the slot.
// scenario: selium-accountant/pipe-quotas-meter-queued-items/over-ceiling-send-denied
// scenario: selium-accountant/pipe-quotas-meter-queued-items/receiving-frees-the-slot
#[test]
fn pipe_quota_meters_queued_items_not_queue_count() {
    let runtime = Runtime::default();

    // The queue owner: tenant acme, holding the pipe capability.
    let owner = spawn_with_grants_and_tenant(
        &runtime,
        "queue-owner",
        vec![CapabilityGrant::new(
            Capability::HostQueue,
            vec![ResourceSelector::ResourceClass(ResourceClass::HostQueue)],
        )],
        Some("acme"),
    )
    .process_id;

    // Creating a queue consumes no slot.
    let (create_status, create_op) = runtime.begin_hostcall(
        owner,
        HostcallRequest::HostQueueCreate {
            serving_tenant: None,
        },
    );
    assert_eq!(create_status, selium_abi::HOSTCALL_STATUS_READY);
    let CompletionState::Ready(HostcallOutput::HostQueue(queue)) =
        runtime.poll_hostcall(owner, create_op)
    else {
        panic!("owner should create its queue");
    };
    assert_eq!(
        runtime
            .kernel()
            .quota()
            .used("acme", ResourceClass::HostQueue),
        0
    );

    // Author a two-item pipe ceiling for acme.
    runtime
        .kernel()
        .quota()
        .set("acme", ResourceClass::HostQueue, 2);

    // A sender with an explicit grant for the owner's queue: it attaches
    // the queue to obtain its own local handle, then sends.
    let sender = spawn_with_grants_and_tenant(
        &runtime,
        "queue-sender",
        vec![CapabilityGrant::new(
            Capability::HostQueue,
            vec![ResourceSelector::ExplicitResource(
                ResourceIdentity::Shared(queue.shared_id),
            )],
        )],
        None,
    )
    .process_id;

    let (attach_status, attach_op) = runtime.begin_hostcall(
        sender,
        HostcallRequest::HostQueueAttach {
            shared_id: queue.shared_id,
        },
    );
    assert_eq!(attach_status, selium_abi::HOSTCALL_STATUS_READY);
    let CompletionState::Ready(HostcallOutput::HostQueue(sender_queue)) =
        runtime.poll_hostcall(sender, attach_op)
    else {
        panic!("sender should attach the owner's queue");
    };

    let send = |value: u64| {
        runtime.begin_hostcall(
            sender,
            HostcallRequest::HostQueueSend {
                local_id: sender_queue.local_id,
                value,
                metadata: Vec::new(),
            },
        )
    };

    // Two items fit the ceiling; the third is denied.
    let (first, _) = send(1);
    assert_eq!(first, selium_abi::HOSTCALL_STATUS_READY);
    let (second, _) = send(2);
    assert_eq!(second, selium_abi::HOSTCALL_STATUS_READY);
    assert_eq!(
        runtime
            .kernel()
            .quota()
            .used("acme", ResourceClass::HostQueue),
        2
    );
    let (third_status, third_op) = send(3);
    assert_eq!(third_status, selium_abi::HOSTCALL_STATUS_FAILED);
    assert!(matches!(
        runtime.poll_hostcall(sender, third_op),
        CompletionState::Failed(error) if error.code == AbiErrorCode::QuotaExceeded
    ));

    // The owner receives one item: its slot is released and a further
    // send fits again.
    let (recv_status, recv_op) = runtime.begin_hostcall(
        owner,
        HostcallRequest::HostQueueRecv {
            local_id: queue.local_id,
        },
    );
    assert_eq!(recv_status, selium_abi::HOSTCALL_STATUS_READY);
    assert!(matches!(
        runtime.poll_hostcall(owner, recv_op),
        CompletionState::Ready(HostcallOutput::ConnectionInfo { value: 1, .. })
    ));
    assert_eq!(
        runtime
            .kernel()
            .quota()
            .used("acme", ResourceClass::HostQueue),
        1
    );
    let (fourth, _) = send(4);
    assert_eq!(fourth, selium_abi::HOSTCALL_STATUS_READY);
}

/// A dying process releases its live reservations: allocated regions'
/// bytes return to the tenant's quota, and pipe slots held by items
/// still queued in its queues are returned. Storage stays sticky.
// scenario: guest-shared-memory/mapping-release-on-process-teardown/guest-side-attachment-is-released-at-teardown
// scenario: selium-accountant/quota-enforcement-at-allocation/a-dying-process-releases-its-reservations
#[test]
fn process_teardown_releases_region_and_pipe_quota() {
    let runtime = Runtime::default();
    let guest = spawn_with_grants_and_tenant(
        &runtime,
        "acme-worker",
        vec![
            CapabilityGrant::new(
                Capability::SharedMemory,
                vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
            ),
            CapabilityGrant::new(
                Capability::HostQueue,
                vec![ResourceSelector::ResourceClass(ResourceClass::HostQueue)],
            ),
        ],
        Some("acme"),
    )
    .process_id;

    // Allocate a one-page region and leave one item queued.
    let (alloc_status, alloc_op) = runtime.begin_hostcall(
        guest,
        HostcallRequest::AllocRegion {
            pages: 1,
            prot: selium_abi::RegionProt::ReadWrite,
            purpose: selium_abi::ResourceKind::SharedMemory,
            serving_tenant: None,
        },
    );
    assert_eq!(alloc_status, selium_abi::HOSTCALL_STATUS_READY);
    assert!(matches!(
        runtime.poll_hostcall(guest, alloc_op),
        CompletionState::Ready(HostcallOutput::RegionAlloc(_))
    ));

    let (queue_status, queue_op) = runtime.begin_hostcall(
        guest,
        HostcallRequest::HostQueueCreate {
            serving_tenant: None,
        },
    );
    assert_eq!(queue_status, selium_abi::HOSTCALL_STATUS_READY);
    let CompletionState::Ready(HostcallOutput::HostQueue(queue)) =
        runtime.poll_hostcall(guest, queue_op)
    else {
        panic!("guest should create its queue");
    };
    let (send_status, _) = runtime.begin_hostcall(
        guest,
        HostcallRequest::HostQueueSend {
            local_id: queue.local_id,
            value: 1,
            metadata: Vec::new(),
        },
    );
    assert_eq!(send_status, selium_abi::HOSTCALL_STATUS_READY);

    assert_eq!(
        runtime
            .kernel()
            .quota()
            .used("acme", ResourceClass::SharedRegion),
        65_536
    );
    assert_eq!(
        runtime
            .kernel()
            .quota()
            .used("acme", ResourceClass::HostQueue),
        1
    );

    // The worker dies: its region and queued item return to the quota.
    runtime.stop_process(guest).expect("stop worker");
    assert_eq!(
        runtime
            .kernel()
            .quota()
            .used("acme", ResourceClass::SharedRegion),
        0,
        "destroyed region's bytes must return to the tenant's quota"
    );
    assert_eq!(
        runtime
            .kernel()
            .quota()
            .used("acme", ResourceClass::HostQueue),
        0,
        "queued items of a dead owner must release their pipe slots"
    );
}

/// A handed-off region transfers: the sender's entry leaves its resource
/// table (it can no longer free the region) and the receiver's gains it,
/// with the quota reservation following the resource across tenants.
// scenario: capability-enforcement/ownership-checked-attach/queue-handoff-shares-ownership
// scenario: selium-accountant/handed-off-resources-transfer-ownership/cross-tenant-handoff-moves-the-reservation
#[test]
fn handoff_transfers_region_ownership_and_quota() {
    let runtime = Runtime::default();

    // The receiving side: tenant beta, holding memory and pipe
    // capabilities.
    let receiver = spawn_with_grants_and_tenant(
        &runtime,
        "beta-receiver",
        vec![
            CapabilityGrant::new(
                Capability::SharedMemory,
                vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
            ),
            CapabilityGrant::new(
                Capability::HostQueue,
                vec![ResourceSelector::ResourceClass(ResourceClass::HostQueue)],
            ),
        ],
        Some("beta"),
    )
    .process_id;

    // The receiver creates a queue under its own tenant first: the
    // sender (a different tenant) then spawns with an explicit grant for
    // that queue so it may attach and send into it.
    let (queue_status, queue_op) = runtime.begin_hostcall(
        receiver,
        HostcallRequest::HostQueueCreate {
            serving_tenant: None,
        },
    );
    assert_eq!(queue_status, selium_abi::HOSTCALL_STATUS_READY);
    let CompletionState::Ready(HostcallOutput::HostQueue(queue)) =
        runtime.poll_hostcall(receiver, queue_op)
    else {
        panic!("receiver should create its queue");
    };

    // The sending side: tenant acme, holding memory and an explicit
    // grant for the receiver's queue.
    let sender = spawn_with_grants_and_tenant(
        &runtime,
        "acme-sender",
        vec![
            CapabilityGrant::new(
                Capability::SharedMemory,
                vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
            ),
            CapabilityGrant::new(
                Capability::HostQueue,
                vec![ResourceSelector::ExplicitResource(
                    ResourceIdentity::Shared(queue.shared_id),
                )],
            ),
        ],
        Some("acme"),
    )
    .process_id;

    // The sender allocates a one-page region (charged to acme).
    let (alloc_status, alloc_op) = runtime.begin_hostcall(
        sender,
        HostcallRequest::AllocRegion {
            pages: 1,
            prot: selium_abi::RegionProt::ReadWrite,
            purpose: selium_abi::ResourceKind::SharedMemory,
            serving_tenant: None,
        },
    );
    assert_eq!(alloc_status, selium_abi::HOSTCALL_STATUS_READY);
    let CompletionState::Ready(HostcallOutput::RegionAlloc(region)) =
        runtime.poll_hostcall(sender, alloc_op)
    else {
        panic!("sender should allocate its region");
    };
    assert_eq!(
        runtime
            .kernel()
            .quota()
            .used("acme", ResourceClass::SharedRegion),
        65_536
    );

    let (attach_status, attach_op) = runtime.begin_hostcall(
        sender,
        HostcallRequest::HostQueueAttach {
            shared_id: queue.shared_id,
        },
    );
    assert_eq!(attach_status, selium_abi::HOSTCALL_STATUS_READY);
    let CompletionState::Ready(HostcallOutput::HostQueue(sender_queue)) =
        runtime.poll_hostcall(sender, attach_op)
    else {
        panic!("sender should attach the receiver's queue");
    };

    let (send_status, _) = runtime.begin_hostcall(
        sender,
        HostcallRequest::HostQueueSend {
            local_id: sender_queue.local_id,
            value: region.region_id,
            metadata: Vec::new(),
        },
    );
    assert_eq!(send_status, selium_abi::HOSTCALL_STATUS_READY);

    let (recv_status, recv_op) = runtime.begin_hostcall(
        receiver,
        HostcallRequest::HostQueueRecv {
            local_id: queue.local_id,
        },
    );
    assert_eq!(recv_status, selium_abi::HOSTCALL_STATUS_READY);
    assert!(matches!(
        runtime.poll_hostcall(receiver, recv_op),
        CompletionState::Ready(HostcallOutput::ConnectionInfo { value, .. }) if value == region.region_id
    ));

    // The reservation followed the resource: acme's bytes moved to beta.
    assert_eq!(
        runtime
            .kernel()
            .quota()
            .used("acme", ResourceClass::SharedRegion),
        0,
        "the sender's reservation must move with the handed-off region"
    );
    assert_eq!(
        runtime
            .kernel()
            .quota()
            .used("beta", ResourceClass::SharedRegion),
        65_536,
        "the receiver's tenant inherits the region's reservation"
    );

    // Ownership transferred: the receiver frees the region (releasing
    // beta's reservation), and the sender can no longer free it.
    let (free_status, free_op) = runtime.begin_hostcall(
        receiver,
        HostcallRequest::FreeRegion {
            region_id: region.region_id,
        },
    );
    assert_eq!(free_status, selium_abi::HOSTCALL_STATUS_READY);
    assert!(matches!(
        runtime.poll_hostcall(receiver, free_op),
        CompletionState::Ready(_)
    ));
    assert_eq!(
        runtime
            .kernel()
            .quota()
            .used("beta", ResourceClass::SharedRegion),
        0
    );

    let (sender_free_status, _sender_free_op) = runtime.begin_hostcall(
        sender,
        HostcallRequest::FreeRegion {
            region_id: region.region_id,
        },
    );
    assert_eq!(
        sender_free_status,
        selium_abi::HOSTCALL_STATUS_FAILED,
        "the sender must no longer own the handed-off region"
    );
}

// scenario: capability-enforcement/operation-target-certificate-authorisation/tenant-scoped-mint-cannot-cross-tenants
// scenario: capability-enforcement/operation-target-certificate-authorisation/authorised-target-succeeds
#[test]
fn tenant_scoped_mint_cannot_cross_tenants() {
    use rcgen::{KeyPair, PKCS_ECDSA_P256_SHA256, PublicKeyData};

    let runtime = Runtime::default();
    runtime.generate_keyring().expect("generate keyring");
    let root = spawn_with_grants(
        &runtime,
        vec![CapabilityGrant::new(
            Capability::MintCertificate,
            Vec::new(),
        )],
    );
    let (_, op) = runtime.begin_hostcall(
        root.process_id,
        HostcallRequest::SignTenantCa {
            tenant: "beta".to_string(),
        },
    );
    assert!(matches!(
        ready(&runtime, root.process_id, op),
        HostcallOutput::Certificate(_)
    ));

    let acme = spawn_with_grants_and_tenant(
        &runtime,
        "acme-mint",
        vec![CapabilityGrant::new(
            Capability::MintCertificate,
            vec![ResourceSelector::Tenant("acme".to_string())],
        )],
        Some("acme"),
    );
    let client_key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).expect("client key");
    let spki = client_key.subject_public_key_info();
    for request in [
        HostcallRequest::SignTenantCa {
            tenant: "beta".to_string(),
        },
        HostcallRequest::SignUserCert {
            tenant: "beta".to_string(),
            spki_der: spki.clone(),
        },
        HostcallRequest::RevokeCa {
            tenant: "beta".to_string(),
        },
    ] {
        let (status, op) = runtime.begin_hostcall(acme.process_id, request);
        assert_eq!(status, selium_abi::HOSTCALL_STATUS_FAILED);
        assert!(matches!(
            runtime.poll_hostcall(acme.process_id, op),
            CompletionState::Failed(error) if error.code == AbiErrorCode::PermissionDenied
        ));
    }
    let (_, op) = runtime.begin_hostcall(
        root.process_id,
        HostcallRequest::SignUserCert {
            tenant: "beta".to_string(),
            spki_der: spki,
        },
    );
    assert!(matches!(
        ready(&runtime, root.process_id, op),
        HostcallOutput::Certificate(_)
    ));
    let (_, op) = runtime.begin_hostcall(
        acme.process_id,
        HostcallRequest::SignTenantCa {
            tenant: "acme".to_string(),
        },
    );
    assert!(matches!(
        ready(&runtime, acme.process_id, op),
        HostcallOutput::Certificate(_)
    ));
}

// scenario: capability-enforcement/intersection-preserving-delegation/root-and-tenant-selectors-remain-conjunctive
#[test]
fn root_plus_tenant_selectors_stay_conjunctive() {
    let runtime = Runtime::default();
    let parent = spawn_with_grants_and_tenant(
        &runtime,
        "conjunctive-bridge",
        vec![
            CapabilityGrant::new(
                Capability::ProcessLifecycle,
                vec![ResourceSelector::ResourceClass(ResourceClass::Process)],
            ),
            CapabilityGrant::new(
                Capability::DelegateGrants,
                vec![
                    ResourceSelector::Namespace(Namespace::Root),
                    ResourceSelector::Tenant("acme".to_string()),
                ],
            ),
        ],
        None,
    );
    let spawn = |tenant: &str| {
        runtime.begin_hostcall(
            parent.process_id,
            HostcallRequest::ProcessStart {
                source: selium_abi::ModuleSource::Inline(module_with_entrypoint("main", "")),
                entrypoint: "main".to_string(),
                arguments: Vec::new(),
                grants: Vec::new(),
                tenant: Some(tenant.to_string()),
            },
        )
    };
    let (status, op) = spawn("acme");
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_READY);
    drop(runtime.poll_hostcall(parent.process_id, op));
    let (status, op) = spawn("beta");
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_FAILED);
    assert!(matches!(
        runtime.poll_hostcall(parent.process_id, op),
        CompletionState::Failed(error) if error.code == AbiErrorCode::PermissionDenied
    ));
}

// scenario: capability-enforcement/intersection-preserving-delegation/separate-grants-do-not-merge-authority
#[test]
fn separate_delegation_grants_do_not_merge_scope() {
    let runtime = Runtime::default();
    let parent = spawn_with_grants_and_tenant(
        &runtime,
        "split-bridge",
        vec![
            CapabilityGrant::new(
                Capability::ProcessLifecycle,
                vec![ResourceSelector::ResourceClass(ResourceClass::Process)],
            ),
            CapabilityGrant::new(
                Capability::DelegateGrants,
                vec![
                    ResourceSelector::Namespace(Namespace::Root),
                    ResourceSelector::Tenant("acme".to_string()),
                ],
            ),
            CapabilityGrant::new(
                Capability::DelegateGrants,
                vec![ResourceSelector::Tenant("beta".to_string())],
            ),
        ],
        None,
    );
    let spawn = |tenant: &str| {
        runtime.begin_hostcall(
            parent.process_id,
            HostcallRequest::ProcessStart {
                source: selium_abi::ModuleSource::Inline(module_with_entrypoint("main", "")),
                entrypoint: "main".to_string(),
                arguments: Vec::new(),
                grants: Vec::new(),
                tenant: Some(tenant.to_string()),
            },
        )
    };
    for tenant in ["acme", "beta"] {
        let (status, op) = spawn(tenant);
        assert_eq!(status, selium_abi::HOSTCALL_STATUS_READY);
        drop(runtime.poll_hostcall(parent.process_id, op));
    }
    let (status, op) = spawn("gamma");
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_FAILED);
    assert!(matches!(
        runtime.poll_hostcall(parent.process_id, op),
        CompletionState::Failed(error) if error.code == AbiErrorCode::PermissionDenied
    ));
}

#[test]
fn child_grant_must_fit_one_parent_delegation_grant() {
    let runtime = Runtime::default();
    let parent = spawn_with_grants_and_tenant(
        &runtime,
        "fenced-bridge",
        vec![
            CapabilityGrant::new(
                Capability::ProcessLifecycle,
                vec![ResourceSelector::ResourceClass(ResourceClass::Process)],
            ),
            CapabilityGrant::new(
                Capability::DelegateGrants,
                vec![
                    ResourceSelector::Namespace(Namespace::Root),
                    ResourceSelector::Tenant("acme".to_string()),
                ],
            ),
        ],
        None,
    );
    let spawn = |tenant: &str| {
        runtime.begin_hostcall(
            parent.process_id,
            HostcallRequest::ProcessStart {
                source: selium_abi::ModuleSource::Inline(module_with_entrypoint("main", "")),
                entrypoint: "main".to_string(),
                arguments: Vec::new(),
                grants: vec![CapabilityGrant::new(
                    Capability::Network,
                    vec![
                        ResourceSelector::Tenant(tenant.to_string()),
                        ResourceSelector::ResourceClass(ResourceClass::TcpStream),
                    ],
                )],
                tenant: None,
            },
        )
    };
    let (status, op) = spawn("beta");
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_FAILED);
    assert!(matches!(
        runtime.poll_hostcall(parent.process_id, op),
        CompletionState::Failed(error) if error.code == AbiErrorCode::PermissionDenied
    ));
    let (status, op) = spawn("acme");
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_READY);
    drop(runtime.poll_hostcall(parent.process_id, op));
}

#[test]
fn repeated_denied_hostcalls_are_reclaimed_after_poll_and_drop() {
    let runtime = Runtime::default();
    let guest = spawn_with_grants(
        &runtime,
        vec![CapabilityGrant::new(
            Capability::SharedMemory,
            vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
        )],
    );
    let baseline = runtime.operations.lock().len();
    let cases = [
        (
            HostcallRequest::VolumeOpen {
                name: "orders".to_string(),
                serving_tenant: None,
            },
            AbiErrorCode::PermissionDenied,
        ),
        (
            HostcallRequest::RandomBytes { len: u32::MAX },
            AbiErrorCode::MalformedPayload,
        ),
    ];
    for _ in 0..32 {
        for (request, expected) in &cases {
            let (status, operation) = runtime.begin_hostcall(guest.process_id, request.clone());
            assert_eq!(status, selium_abi::HOSTCALL_STATUS_FAILED);
            assert!(matches!(
                runtime.poll_hostcall(guest.process_id, operation),
                CompletionState::Failed(error) if error.code == *expected
            ));
            assert!(runtime.drop_hostcall(guest.process_id, operation));
        }
    }
    assert_eq!(runtime.operations.lock().len(), baseline);
}

// scenario: selium-runtime/failed-resource-operations-roll-back-reservations/region-allocation-fails-after-reservation
#[test]
fn injected_region_failure_rolls_back_quota_and_ownership() {
    let runtime = Runtime::default();
    let guest = spawn_with_grants_and_tenant(
        &runtime,
        "rollback-region",
        vec![CapabilityGrant::new(
            Capability::SharedMemory,
            vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
        )],
        Some("acme"),
    );
    let quota = runtime.kernel().quota();
    quota.set("acme", ResourceClass::SharedRegion, 1_000_000);
    let baseline_used = quota.used("acme", ResourceClass::SharedRegion);
    let baseline_regions = runtime.kernel().memory().live_shared_region_count();

    runtime.inject_region_allocation_failure(true);
    let (status, operation) = runtime.begin_hostcall(
        guest.process_id,
        HostcallRequest::AllocRegion {
            pages: 1,
            prot: selium_abi::RegionProt::ReadWrite,
            purpose: selium_abi::ResourceKind::SharedMemory,
            serving_tenant: None,
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_FAILED);
    assert!(matches!(
        runtime.poll_hostcall(guest.process_id, operation),
        CompletionState::Failed(error) if error.code == AbiErrorCode::Internal
    ));
    assert_eq!(
        quota.used("acme", ResourceClass::SharedRegion),
        baseline_used
    );
    assert_eq!(
        runtime.kernel().memory().live_shared_region_count(),
        baseline_regions
    );
    assert!(
        !runtime
            .shared_resource_owners
            .lock()
            .keys()
            .any(|(class, _)| class == &ResourceClass::SharedRegion)
    );
    assert!(
        !runtime
            .resource_tenants
            .lock()
            .keys()
            .any(|(class, _)| class == &ResourceClass::SharedRegion)
    );

    runtime.inject_region_allocation_failure(false);
    let (status, operation) = runtime.begin_hostcall(
        guest.process_id,
        HostcallRequest::AllocRegion {
            pages: 1,
            prot: selium_abi::RegionProt::ReadWrite,
            purpose: selium_abi::ResourceKind::SharedMemory,
            serving_tenant: None,
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_READY);
    assert!(matches!(
        runtime.poll_hostcall(guest.process_id, operation),
        CompletionState::Ready(HostcallOutput::RegionAlloc(_))
    ));
    assert_eq!(
        quota.used("acme", ResourceClass::SharedRegion),
        baseline_used + 65_536
    );
}

// scenario: selium-runtime/failed-resource-operations-roll-back-reservations/append-or-queue-send-fails
#[test]
fn injected_append_failure_rolls_back_volume_quota_without_writing() {
    let runtime = Runtime::default();
    let guest = spawn_with_grants_and_tenant(
        &runtime,
        "rollback-append",
        vec![CapabilityGrant::new(
            Capability::Storage,
            vec![ResourceSelector::ResourceClass(ResourceClass::Volume)],
        )],
        Some("acme"),
    );
    let quota = runtime.kernel().quota();
    quota.set("acme", ResourceClass::Volume, 1_000_000);
    let (status, open) = runtime.begin_hostcall(
        guest.process_id,
        HostcallRequest::VolumeOpen {
            name: "orders".to_string(),
            serving_tenant: None,
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_READY);
    let HostcallOutput::Volume(volume) = ready(&runtime, guest.process_id, open) else {
        panic!("expected a volume descriptor");
    };
    let baseline_used = quota.used("acme", ResourceClass::Volume);
    let baseline_len = runtime
        .kernel()
        .volumes()
        .len(volume.local_id)
        .expect("len");

    runtime.inject_volume_append_failure(true);
    let (status, operation) = runtime.begin_hostcall(
        guest.process_id,
        HostcallRequest::VolumeAppend {
            local_id: volume.local_id,
            bytes: b"data".to_vec(),
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_FAILED);
    assert!(matches!(
        runtime.poll_hostcall(guest.process_id, operation),
        CompletionState::Failed(error) if error.code == AbiErrorCode::Internal
    ));
    assert_eq!(quota.used("acme", ResourceClass::Volume), baseline_used);
    assert_eq!(
        runtime
            .kernel()
            .volumes()
            .len(volume.local_id)
            .expect("len"),
        baseline_len
    );

    runtime.inject_volume_append_failure(false);
    let (status, operation) = runtime.begin_hostcall(
        guest.process_id,
        HostcallRequest::VolumeAppend {
            local_id: volume.local_id,
            bytes: b"data".to_vec(),
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_READY);
    assert!(matches!(
        ready(&runtime, guest.process_id, operation),
        HostcallOutput::U64(_)
    ));
    assert_eq!(quota.used("acme", ResourceClass::Volume), baseline_used + 4);
}

#[test]
fn injected_queue_send_failure_rolls_back_the_pipe_slot() {
    let runtime = Runtime::default();
    let owner = spawn_with_grants_and_tenant(
        &runtime,
        "rollback-queue",
        vec![CapabilityGrant::new(
            Capability::HostQueue,
            vec![ResourceSelector::ResourceClass(ResourceClass::HostQueue)],
        )],
        Some("acme"),
    );
    let quota = runtime.kernel().quota();
    quota.set("acme", ResourceClass::HostQueue, 10);
    let (status, create) = runtime.begin_hostcall(
        owner.process_id,
        HostcallRequest::HostQueueCreate {
            serving_tenant: None,
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_READY);
    let HostcallOutput::HostQueue(queue) = ready(&runtime, owner.process_id, create) else {
        panic!("expected a queue descriptor");
    };
    let baseline_used = quota.used("acme", ResourceClass::HostQueue);
    runtime.inject_queue_send_failure(true);
    let (status, operation) = runtime.begin_hostcall(
        owner.process_id,
        HostcallRequest::HostQueueSend {
            local_id: queue.local_id,
            value: 1,
            metadata: Vec::new(),
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_FAILED);
    assert!(matches!(
        runtime.poll_hostcall(owner.process_id, operation),
        CompletionState::Failed(error) if error.code == AbiErrorCode::Internal
    ));
    assert_eq!(quota.used("acme", ResourceClass::HostQueue), baseline_used);
    assert_eq!(
        runtime
            .kernel()
            .queues()
            .host_queue_pending(queue.local_id)
            .expect("pending"),
        0
    );

    runtime.inject_queue_send_failure(false);
    let (status, _operation) = runtime.begin_hostcall(
        owner.process_id,
        HostcallRequest::HostQueueSend {
            local_id: queue.local_id,
            value: 1,
            metadata: Vec::new(),
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_READY);
    assert_eq!(
        quota.used("acme", ResourceClass::HostQueue),
        baseline_used + 1
    );
}

// scenario: selium-runtime/dedicated-timer-driver/a-cancelled-sleep-is-not-woken
#[test]
fn dropping_a_pending_sleep_releases_its_timer() {
    let runtime = Runtime::default();
    let guest = spawn_with_grants(
        &runtime,
        vec![CapabilityGrant::new(
            Capability::SharedMemory,
            vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
        )],
    );
    let baseline = runtime.operations.lock().len();
    let (status, operation) = runtime.begin_hostcall_with_task(
        guest.process_id,
        HostcallRequest::Sleep { millis: 3_600_000 },
        Some(1),
        None,
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_PENDING);
    assert_eq!(runtime.pending_timer_count(), 1);
    assert!(runtime.drop_hostcall(guest.process_id, operation));
    assert_eq!(runtime.pending_timer_count(), 0);
    assert_eq!(runtime.operations.lock().len(), baseline);
}

#[test]
fn stopping_a_process_releases_its_pending_sleep() {
    let runtime = Runtime::default();
    let guest = spawn_with_grants(
        &runtime,
        vec![CapabilityGrant::new(
            Capability::SharedMemory,
            vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
        )],
    );
    let (status, _operation) = runtime.begin_hostcall_with_task(
        guest.process_id,
        HostcallRequest::Sleep { millis: 3_600_000 },
        Some(1),
        None,
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_PENDING);
    assert_eq!(runtime.pending_timer_count(), 1);
    runtime
        .stop_process(guest.process_id)
        .expect("process teardown");
    assert_eq!(runtime.pending_timer_count(), 0);
}

#[test]
fn failed_guest_attachment_leaves_no_mapping_and_teardown_reclaims_it() {
    let runtime = Runtime::default();
    let owner = spawn_with_grants(
        &runtime,
        vec![CapabilityGrant::new(
            Capability::SharedMemory,
            vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
        )],
    );
    let consumer = runtime
        .spawn_system_guest(SystemGuestDescriptor {
            name: "attachment-consumer".to_string(),
            module_id: "attachment-consumer-module".to_string(),
            module_bytes: wat::parse_str(r#"(module (memory 1) (func (export "boot")))"#)
                .expect("compile consumer module"),
            entrypoint: "boot".to_string(),
            arguments: Vec::new(),
            grants: vec![CapabilityGrant::new(
                Capability::SharedMemory,
                vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
            )],
            tenant: None,
            discovery_service: false,
            handlers: Vec::new(),
        })
        .expect("spawn consumer");
    let (_, alloc_op) = runtime.begin_hostcall(
        owner.process_id,
        HostcallRequest::AllocRegion {
            pages: 1,
            prot: selium_abi::RegionProt::ReadWrite,
            purpose: selium_abi::ResourceKind::SharedMemory,
            serving_tenant: None,
        },
    );
    let HostcallOutput::RegionAlloc(alloc) = ready(&runtime, owner.process_id, alloc_op) else {
        panic!("expected region allocation");
    };
    let baseline_mappings = runtime.kernel().memory().live_shared_mapping_count();
    let (status, _) = runtime.begin_hostcall(
        consumer.process_id,
        HostcallRequest::AttachRegion {
            region_id: alloc.region_id,
            reader_slot: None,
            prot: selium_abi::RegionProt::ReadWrite,
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_FAILED);
    assert_eq!(
        runtime.kernel().memory().live_shared_mapping_count(),
        baseline_mappings
    );
    assert!(
        !runtime
            .guest_region_memories
            .lock()
            .contains_key(&(consumer.process_id, alloc.region_id))
    );

    let (status, _) = runtime.begin_hostcall(
        owner.process_id,
        HostcallRequest::DeclareAttachableByResolution {
            resource_class: ResourceClass::SharedRegion,
            resource_id: alloc.region_id,
            scope: selium_abi::AttachDeclarationScope::Any,
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_READY);
    let (status, _) = runtime.begin_hostcall(
        consumer.process_id,
        HostcallRequest::RecordResolvedResourceFor {
            resource_class: ResourceClass::SharedRegion,
            resource_id: alloc.region_id,
            client_process_id: consumer.process_id,
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_READY);
    let (status, attach_op) = runtime.begin_hostcall(
        consumer.process_id,
        HostcallRequest::AttachRegion {
            region_id: alloc.region_id,
            reader_slot: None,
            prot: selium_abi::RegionProt::ReadWrite,
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_READY);
    assert!(matches!(
        runtime.poll_hostcall(consumer.process_id, attach_op),
        CompletionState::Ready(HostcallOutput::RegionAttach(_))
    ));
    runtime
        .stop_process(consumer.process_id)
        .expect("consumer teardown");
    assert!(!runtime.resident_guest_ids().contains(&consumer.process_id));
}

// scenario: selium-abi/mintcertificate-capability-variant/signing-hostcall-requires-the-capability
#[test]
fn signing_hostcalls_deny_without_mint_certificate() {
    let runtime = Runtime::default();
    runtime.generate_keyring().expect("generate keyring");

    // A guest holding no `MintCertificate` grant is denied every signing
    // hostcall with a capability error.
    let guest = spawn_with_grants(
        &runtime,
        vec![CapabilityGrant::new(
            Capability::SharedMemory,
            vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
        )],
    );

    for request in [
        HostcallRequest::SignTenantCa {
            tenant: "acme".to_string(),
        },
        HostcallRequest::SignUserCert {
            tenant: "acme".to_string(),
            spki_der: vec![0x30, 0x01],
        },
        HostcallRequest::RevokeCa {
            tenant: "acme".to_string(),
        },
    ] {
        let (status, op) = runtime.begin_hostcall(guest.process_id, request);
        assert_eq!(status, selium_abi::HOSTCALL_STATUS_FAILED);
        assert!(matches!(
            runtime.poll_hostcall(guest.process_id, op),
            CompletionState::Failed(error) if error.code == AbiErrorCode::PermissionDenied
        ));
    }
}
// scenario: selium-identity/host-held-signing-oracle/signing-returns-certificates-never-keys

#[test]
fn signing_hostcalls_return_certificates_only() {
    use rcgen::{KeyPair, PKCS_ECDSA_P256_SHA256, PublicKeyData};

    let runtime = Runtime::default();
    runtime.generate_keyring().expect("generate keyring");

    let guest = spawn_with_grants(
        &runtime,
        vec![CapabilityGrant::new(
            Capability::MintCertificate,
            Vec::new(),
        )],
    );

    // SignTenantCa returns a DER tenant CA certificate (no key material).
    let (_, op) = runtime.begin_hostcall(
        guest.process_id,
        HostcallRequest::SignTenantCa {
            tenant: "acme".to_string(),
        },
    );
    let HostcallOutput::Certificate(tl_ca_der) = ready(&runtime, guest.process_id, op) else {
        panic!("SignTenantCa must return a certificate");
    };
    assert!(!tl_ca_der.is_empty());

    // SignUserCert signs a client SPKI, returning a DER leaf.
    let client_key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).expect("client key");
    let spki = client_key.subject_public_key_info();
    let (_, op) = runtime.begin_hostcall(
        guest.process_id,
        HostcallRequest::SignUserCert {
            tenant: "acme".to_string(),
            spki_der: spki,
        },
    );
    let HostcallOutput::Certificate(leaf_der) = ready(&runtime, guest.process_id, op) else {
        panic!("SignUserCert must return a certificate");
    };
    assert!(!leaf_der.is_empty());

    // RevokeCa returns empty, and a second revoke fails (key already gone).
    let (_, op) = runtime.begin_hostcall(
        guest.process_id,
        HostcallRequest::RevokeCa {
            tenant: "acme".to_string(),
        },
    );
    assert_eq!(ready(&runtime, guest.process_id, op), HostcallOutput::Empty);
    let (status, op) = runtime.begin_hostcall(
        guest.process_id,
        HostcallRequest::RevokeCa {
            tenant: "acme".to_string(),
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_FAILED);
    assert!(matches!(
        runtime.poll_hostcall(guest.process_id, op),
        CompletionState::Failed(error) if error.code == AbiErrorCode::Internal
    ));
}

#[test]
fn signing_hostcalls_fail_without_keyring() {
    let runtime = Runtime::default();
    // No keyring installed: the signing hostcall fails with an internal
    // error instead of silently returning a fabricated certificate.
    let guest = spawn_with_grants(
        &runtime,
        vec![CapabilityGrant::new(
            Capability::MintCertificate,
            Vec::new(),
        )],
    );
    let (status, op) = runtime.begin_hostcall(
        guest.process_id,
        HostcallRequest::SignTenantCa {
            tenant: "acme".to_string(),
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_FAILED);
    assert!(matches!(
        runtime.poll_hostcall(guest.process_id, op),
        CompletionState::Failed(error) if error.code == AbiErrorCode::Internal
    ));
}
// scenario: selium-abi/owner-scoped-attach-authorisation/recorded-id-authorises-the-attach
// scenario: selium-abi/owner-declared-resolvable-attach/declared-resource-admits-a-resolver
// scenario: selium-abi/owner-declared-resolvable-attach/undeclared-resource-refuses-a-record

#[test]
fn record_resolved_region_grants_attach_authority() {
    let runtime = Runtime::default();

    let owner = spawn_with_grants(
        &runtime,
        vec![CapabilityGrant::new(
            Capability::SharedMemory,
            vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
        )],
    );
    // The consumer needs a memory-declaring module so the final
    // `AttachRegion` maps the region into its linear memory.
    let consumer = runtime
        .spawn_system_guest(SystemGuestDescriptor {
            name: "consumer".to_string(),
            module_id: "consumer-module".to_string(),
            module_bytes: wat::parse_str(r#"(module (memory 1) (func (export "boot")))"#)
                .expect("compile consumer module"),
            entrypoint: "boot".to_string(),
            arguments: Vec::new(),
            grants: vec![CapabilityGrant::new(
                Capability::SharedMemory,
                vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
            )],
            tenant: None,
            discovery_service: false,
            handlers: Vec::new(),
        })
        .expect("spawn consumer");

    let (_, alloc_op) = runtime.begin_hostcall(
        owner.process_id,
        HostcallRequest::AllocRegion {
            pages: 1,
            prot: selium_abi::RegionProt::ReadWrite,
            purpose: selium_abi::ResourceKind::SharedMemory,
            serving_tenant: None,
        },
    );
    let HostcallOutput::RegionAlloc(alloc) = ready(&runtime, owner.process_id, alloc_op) else {
        panic!("expected region allocation");
    };

    // A peer that neither owns nor resolved the region is denied at attach.
    let (status, _) = runtime.begin_hostcall(
        consumer.process_id,
        HostcallRequest::AttachRegion {
            region_id: alloc.region_id,
            reader_slot: None,
            prot: selium_abi::RegionProt::ReadWrite,
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_FAILED);

    // An undeclared resource refuses a resolve recording even from the
    // would-be client itself.
    let (status, _) = runtime.begin_hostcall(
        consumer.process_id,
        HostcallRequest::RecordResolvedResourceFor {
            resource_class: ResourceClass::SharedRegion,
            resource_id: alloc.region_id,
            client_process_id: consumer.process_id,
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_FAILED);

    // The owner declares the region attachable by resolution; the
    // consumer's resolve then records an attach basis, after which the
    // attach is authorised.
    let (status, _) = runtime.begin_hostcall(
        owner.process_id,
        HostcallRequest::DeclareAttachableByResolution {
            resource_class: ResourceClass::SharedRegion,
            resource_id: alloc.region_id,
            scope: selium_abi::AttachDeclarationScope::Any,
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_READY);
    let (status, _) = runtime.begin_hostcall(
        consumer.process_id,
        HostcallRequest::RecordResolvedResourceFor {
            resource_class: ResourceClass::SharedRegion,
            resource_id: alloc.region_id,
            client_process_id: consumer.process_id,
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_READY);

    let (status, op) = runtime.begin_hostcall(
        consumer.process_id,
        HostcallRequest::AttachRegion {
            region_id: alloc.region_id,
            reader_slot: None,
            prot: selium_abi::RegionProt::ReadWrite,
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_READY);
    assert!(matches!(
        runtime.poll_hostcall(consumer.process_id, op),
        CompletionState::Ready(HostcallOutput::RegionAttach(_))
    ));
}
// scenario: resource-handshake/guest-self-identity/self-info

#[test]
fn self_info_returns_process_id_and_tenant() {
    let runtime = Runtime::default();
    let guest =
        spawn_with_grants_and_tenant(&runtime, "tenant-guest", vec![], Some("acme")).process_id;

    let (status, op) = runtime.begin_hostcall(guest, HostcallRequest::SelfInfo);
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_READY);
    match runtime.poll_hostcall(guest, op) {
        CompletionState::Ready(HostcallOutput::SelfInfo { process_id, tenant }) => {
            assert_eq!(process_id, guest);
            assert_eq!(tenant.as_deref(), Some("acme"));
        }
        other => panic!("expected SelfInfo output, got {other:?}"),
    }
}
// scenario: resource-handshake/handoff-metadata/oversized-metadata-rejected

#[test]
fn host_queue_send_rejects_oversized_metadata() {
    let runtime = Runtime::default();
    let sender = spawn_with_grants_and_tenant(
        &runtime,
        "meta-sender",
        vec![CapabilityGrant::new(
            Capability::HostQueue,
            vec![ResourceSelector::ResourceClass(ResourceClass::HostQueue)],
        )],
        None,
    )
    .process_id;

    let (_, op_id) = runtime.begin_hostcall(
        sender,
        HostcallRequest::HostQueueCreate {
            serving_tenant: None,
        },
    );
    let CompletionState::Ready(HostcallOutput::HostQueue(queue)) =
        runtime.poll_hostcall(sender, op_id)
    else {
        panic!("sender should create its listener queue");
    };

    let oversized = vec![0u8; selium_abi::METADATA_MAX_BYTES + 1];
    let (status, op) = runtime.begin_hostcall(
        sender,
        HostcallRequest::HostQueueSend {
            local_id: queue.local_id,
            value: 1,
            metadata: oversized,
        },
    );
    assert_eq!(
        status,
        selium_abi::HOSTCALL_STATUS_FAILED,
        "oversized handoff metadata must be rejected"
    );
    assert!(matches!(
        runtime.poll_hostcall(sender, op),
        CompletionState::Failed(error) if error.code == AbiErrorCode::MalformedPayload
    ));

    // The bound itself is admitted.
    let (status, op) = runtime.begin_hostcall(
        sender,
        HostcallRequest::HostQueueSend {
            local_id: queue.local_id,
            value: 2,
            metadata: vec![0u8; selium_abi::METADATA_MAX_BYTES],
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_READY);
    drop(runtime.poll_hostcall(sender, op));
}

#[test]
fn spawned_child_inherits_parent_tenant() {
    let runtime = Runtime::default();

    let parent = spawn_with_grants_and_tenant(
        &runtime,
        "bridge-server-tenant",
        vec![
            CapabilityGrant::new(
                Capability::ProcessLifecycle,
                vec![ResourceSelector::ResourceClass(ResourceClass::Process)],
            ),
            CapabilityGrant::new(
                Capability::DelegateGrants,
                vec![ResourceSelector::Tenant("acme".to_string())],
            ),
        ],
        Some("acme"),
    );

    let child_grants = vec![
        CapabilityGrant::new(
            Capability::Network,
            vec![
                ResourceSelector::Tenant("acme".to_string()),
                ResourceSelector::ResourceClass(ResourceClass::TcpStream),
            ],
        ),
        CapabilityGrant::new(
            Capability::SharedMemory,
            vec![
                ResourceSelector::Tenant("acme".to_string()),
                ResourceSelector::ResourceClass(ResourceClass::SharedRegion),
            ],
        ),
    ];
    let (status, op) = runtime.begin_hostcall(
        parent.process_id,
        HostcallRequest::ProcessStart {
            source: selium_abi::ModuleSource::Inline(module_with_entrypoint("main", "")),
            entrypoint: "main".to_string(),
            arguments: Vec::new(),
            grants: child_grants,
            tenant: None,
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_READY);

    let child_pid = match runtime.poll_hostcall(parent.process_id, op) {
        CompletionState::Ready(HostcallOutput::Process(process)) => process.local_id,
        other => panic!("expected child process descriptor, got {other:?}"),
    };

    assert_eq!(
        runtime.process_tenant(child_pid).as_deref(),
        Some("acme"),
        "a spawned bridge-channel must inherit the bridge-server's tenant"
    );
}

/// Task 3.1: a spawn for a tenant at its process ceiling is denied with
/// `QuotaExceeded` before the child is created, while a tenant-less (root)
/// spawn is not metered.
// scenario: selium-runtime/process-spawn-quota/quota-distinct-from-lifecycle-grant
// scenario: selium-accountant/quota-enforcement-at-allocation/spawn-over-process-quota-denied
#[test]
fn process_start_enforces_tenant_process_quota_and_skips_root() {
    let runtime = Runtime::default();
    runtime
        .kernel()
        .quota()
        .set("acme", ResourceClass::Process, 1);

    let parent = spawn_with_grants_and_tenant(
        &runtime,
        "root-spawner",
        vec![
            CapabilityGrant::new(
                Capability::ProcessLifecycle,
                vec![ResourceSelector::ResourceClass(ResourceClass::Process)],
            ),
            CapabilityGrant::new(
                Capability::DelegateGrants,
                vec![ResourceSelector::Namespace(Namespace::Root)],
            ),
        ],
        None,
    );

    // A tenant-less (root) spawn is not metered: it succeeds without
    // touching the ceiling.
    let (status, op) = runtime.begin_hostcall(
        parent.process_id,
        HostcallRequest::ProcessStart {
            source: selium_abi::ModuleSource::Inline(module_with_entrypoint("main", "")),
            entrypoint: "main".to_string(),
            arguments: Vec::new(),
            grants: Vec::new(),
            tenant: None,
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_READY);
    let root_child = match runtime.poll_hostcall(parent.process_id, op) {
        CompletionState::Ready(HostcallOutput::Process(process)) => process.local_id,
        other => panic!("expected root child descriptor, got {other:?}"),
    };

    // The first tenant-scoped spawn is within the ceiling.
    let (status, op) = runtime.begin_hostcall(
        parent.process_id,
        HostcallRequest::ProcessStart {
            source: selium_abi::ModuleSource::Inline(module_with_entrypoint("main", "")),
            entrypoint: "main".to_string(),
            arguments: Vec::new(),
            grants: Vec::new(),
            tenant: Some("acme".to_string()),
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_READY);
    let tenant_child = match runtime.poll_hostcall(parent.process_id, op) {
        CompletionState::Ready(HostcallOutput::Process(process)) => process.local_id,
        other => panic!("expected tenant child descriptor, got {other:?}"),
    };

    // The second tenant-scoped spawn is over the ceiling: denied before
    // any child is created.
    let (status, op) = runtime.begin_hostcall(
        parent.process_id,
        HostcallRequest::ProcessStart {
            source: selium_abi::ModuleSource::Inline(module_with_entrypoint("main", "")),
            entrypoint: "main".to_string(),
            arguments: Vec::new(),
            grants: Vec::new(),
            tenant: Some("acme".to_string()),
        },
    );
    assert_eq!(
        status,
        selium_abi::HOSTCALL_STATUS_FAILED,
        "an over-ceiling tenant spawn must be denied"
    );
    assert!(
        matches!(
            runtime.poll_hostcall(parent.process_id, op),
            CompletionState::Failed(error) if error.code == AbiErrorCode::QuotaExceeded
        ),
        "the over-ceiling spawn must fail with QuotaExceeded"
    );

    let _ = (root_child, tenant_child);
}

/// Task 3.2: the process-quota slot returns to the tenant when the child
/// exits (is torn down), letting the tenant spawn again up to its ceiling.
// scenario: selium-runtime/process-spawn-quota/exited-process-returns-its-slot
#[test]
fn process_quota_slot_returns_to_tenant_on_exit() {
    let runtime = Runtime::default();
    runtime
        .kernel()
        .quota()
        .set("acme", ResourceClass::Process, 1);

    let parent = spawn_with_grants_and_tenant(
        &runtime,
        "root-spawner",
        vec![
            CapabilityGrant::new(
                Capability::ProcessLifecycle,
                vec![ResourceSelector::ResourceClass(ResourceClass::Process)],
            ),
            CapabilityGrant::new(
                Capability::DelegateGrants,
                vec![ResourceSelector::Namespace(Namespace::Root)],
            ),
        ],
        None,
    );

    // The only slot is consumed by the first child.
    let (status, op) = runtime.begin_hostcall(
        parent.process_id,
        HostcallRequest::ProcessStart {
            source: selium_abi::ModuleSource::Inline(module_with_entrypoint("main", "")),
            entrypoint: "main".to_string(),
            arguments: Vec::new(),
            grants: Vec::new(),
            tenant: Some("acme".to_string()),
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_READY);
    let child = match runtime.poll_hostcall(parent.process_id, op) {
        CompletionState::Ready(HostcallOutput::Process(process)) => process.local_id,
        other => panic!("expected child descriptor, got {other:?}"),
    };
    assert_eq!(
        runtime
            .kernel()
            .quota()
            .used("acme", ResourceClass::Process),
        1,
        "the spawned child must hold the process slot"
    );

    // Stop the child (the existing teardown path): the slot returns.
    runtime.stop_process(child).expect("stop child");
    assert_eq!(
        runtime
            .kernel()
            .quota()
            .used("acme", ResourceClass::Process),
        0,
        "a torn-down child must return its process slot"
    );

    // The tenant can spawn again up to the ceiling.
    let (status, _op) = runtime.begin_hostcall(
        parent.process_id,
        HostcallRequest::ProcessStart {
            source: selium_abi::ModuleSource::Inline(module_with_entrypoint("main", "")),
            entrypoint: "main".to_string(),
            arguments: Vec::new(),
            grants: Vec::new(),
            tenant: Some("acme".to_string()),
        },
    );
    assert_eq!(
        status,
        selium_abi::HOSTCALL_STATUS_READY,
        "the released slot lets the tenant spawn again"
    );
}

#[test]
fn empty_selector_grant_is_unrestricted_within_capability() {
    let runtime = Runtime::default();
    let bootstrapped = runtime
        .spawn_system_guest(SystemGuestDescriptor {
            name: "unrestricted".to_string(),
            module_id: "unrestricted-module".to_string(),
            module_bytes: module_with_entrypoint("boot", ""),
            entrypoint: "boot".to_string(),
            arguments: Vec::new(),
            grants: vec![CapabilityGrant::new(Capability::SharedMemory, vec![])],
            tenant: None,
            discovery_service: false,
            handlers: Vec::new(),
        })
        .expect("empty selector grant should be accepted");

    let (status, _) = runtime.begin_hostcall(
        bootstrapped.process_id,
        HostcallRequest::AllocRegion {
            pages: 1,
            prot: selium_abi::RegionProt::ReadWrite,
            purpose: selium_abi::ResourceKind::SharedMemory,
            serving_tenant: None,
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_READY);
}

// scenario: capability-enforcement/unguessable-resource-identities/guessing-is-not-viable
// scenario: capability-enforcement/ownership-checked-attach/no-implicit-sharing
#[test]
fn attach_isolation_prevents_guessing() {
    let runtime = Runtime::default();

    // Process A: can allocate shared memory.
    let guest_a = spawn_with_grants(
        &runtime,
        vec![CapabilityGrant::new(
            Capability::SharedMemory,
            vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
        )],
    );

    // Process B: has SharedMemory capability but does not own A's regions.
    let guest_b = runtime
        .spawn_system_guest(SystemGuestDescriptor {
            name: "guesser".to_string(),
            module_id: "guesser-module".to_string(),
            module_bytes: module_with_entrypoint("boot", ""),
            entrypoint: "boot".to_string(),
            arguments: Vec::new(),
            grants: vec![CapabilityGrant::new(
                Capability::SharedMemory,
                vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
            )],
            tenant: None,
            discovery_service: false,
            handlers: Vec::new(),
        })
        .expect("spawn guesser");

    // A allocates a region.
    let (_, alloc_op) = runtime.begin_hostcall(
        guest_a.process_id,
        HostcallRequest::AllocRegion {
            pages: 1,
            prot: selium_abi::RegionProt::ReadWrite,
            purpose: selium_abi::ResourceKind::SharedMemory,
            serving_tenant: None,
        },
    );
    let HostcallOutput::RegionAlloc(alloc) = ready(&runtime, guest_a.process_id, alloc_op) else {
        panic!("expected RegionAlloc");
    };
    let region_id = alloc.region_id;

    // B tries to attach to A's region → denied (no ownership, no explicit grant).
    let (status, attach_op) = runtime.begin_hostcall(
        guest_b.process_id,
        HostcallRequest::AttachRegion {
            region_id,
            reader_slot: None,
            prot: selium_abi::RegionProt::ReadWrite,
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_FAILED);
    match runtime.poll_hostcall(guest_b.process_id, attach_op) {
        CompletionState::Failed(error) => {
            assert_eq!(error.code, AbiErrorCode::PermissionDenied);
            assert!(
                error.message.contains("does not own"),
                "error should mention ownership: {error:?}"
            );
        }
        other => panic!("expected failed, got {other:?}"),
    }
}

#[test]
fn attach_succeeds_with_explicit_resource_grant() {
    let runtime = Runtime::default();

    // Process A: can allocate shared memory.
    let guest_a = spawn_with_grants(
        &runtime,
        vec![CapabilityGrant::new(
            Capability::SharedMemory,
            vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
        )],
    );

    // A allocates a region.
    let (_, alloc_op) = runtime.begin_hostcall(
        guest_a.process_id,
        HostcallRequest::AllocRegion {
            pages: 1,
            prot: selium_abi::RegionProt::ReadWrite,
            purpose: selium_abi::ResourceKind::SharedMemory,
            serving_tenant: None,
        },
    );
    let HostcallOutput::RegionAlloc(alloc) = ready(&runtime, guest_a.process_id, alloc_op) else {
        panic!("expected RegionAlloc");
    };
    let region_id = alloc.region_id;

    // Process B: has ExplicitResource grant for the specific region.
    let guest_b = runtime
        .spawn_system_guest(SystemGuestDescriptor {
            name: "explicit-b".to_string(),
            module_id: "explicit-b-module".to_string(),
            module_bytes: wat::parse_str("(module (memory 1) (func (export \"boot\")))")
                .expect("compile wat"),
            entrypoint: "boot".to_string(),
            arguments: Vec::new(),
            grants: vec![CapabilityGrant::new(
                Capability::SharedMemory,
                vec![ResourceSelector::ExplicitResource(
                    ResourceIdentity::Shared(region_id),
                )],
            )],
            tenant: None,
            discovery_service: false,
            handlers: Vec::new(),
        })
        .expect("spawn explicit-b");

    // B attaches to A's region with the ExplicitResource grant → succeeds.
    let (status, attach_op) = runtime.begin_hostcall(
        guest_b.process_id,
        HostcallRequest::AttachRegion {
            region_id,
            reader_slot: None,
            prot: selium_abi::RegionProt::ReadWrite,
        },
    );
    if status != selium_abi::HOSTCALL_STATUS_READY {
        match runtime.poll_hostcall(guest_b.process_id, attach_op) {
            CompletionState::Failed(error) => {
                panic!("attach with ExplicitResource grant should succeed: {error:?}");
            }
            other => panic!("expected ready, got {other:?}"),
        }
    }
}

// scenario: capability-enforcement/descendant-telemetry-reads/supervisor-reads-child-telemetry
#[test]
fn children_selector_allows_descendant_metering_read() {
    let runtime = Runtime::default();

    let parent = spawn_with_grants(
        &runtime,
        vec![
            CapabilityGrant::new(
                Capability::ProcessLifecycle,
                vec![ResourceSelector::ResourceClass(ResourceClass::Process)],
            ),
            CapabilityGrant::new(
                Capability::MeteringRead,
                vec![ResourceSelector::ResourceClass(
                    ResourceClass::MeteringStream,
                )],
            ),
            CapabilityGrant::new(Capability::MeteringRead, vec![ResourceSelector::Children]),
        ],
    );

    let (_, start_op) = runtime.begin_hostcall(
        parent.process_id,
        HostcallRequest::ProcessStart {
            source: selium_abi::ModuleSource::Inline(module_with_entrypoint("main", "")),
            entrypoint: "main".to_string(),
            arguments: Vec::new(),
            grants: vec![],
            tenant: None,
        },
    );
    let HostcallOutput::Process(child) = ready(&runtime, parent.process_id, start_op) else {
        panic!("expected child process");
    };
    runtime.project_metering(
        child.local_id,
        MeteringObservation {
            cpu_instructions: 42,
            memory_bytes: 0,
            bandwidth_bytes: 0,
        },
    );

    // Parent reads descendant metering with Children selector → succeeds.
    let (status, meter_op) = runtime.begin_hostcall(
        parent.process_id,
        HostcallRequest::MeteringRead {
            process_id: child.local_id,
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_READY);
    assert!(matches!(
        ready(&runtime, parent.process_id, meter_op),
        HostcallOutput::Metering(_)
    ));

    // Parent reads an UNrelated process's metering → Children grant does
    // NOT match, but the ResourceClass grant DOES (so this succeeds).
    // To test denial, we need a grant with ONLY Children (no class-level).
}

#[test]
fn children_selector_denies_unrelated_process() {
    let runtime = Runtime::default();

    // Parent has ONLY Children-scoped MeteringRead (no class-level).
    let parent = spawn_with_grants(
        &runtime,
        vec![CapabilityGrant::new(
            Capability::MeteringRead,
            vec![ResourceSelector::Children],
        )],
    );

    // An unrelated process.
    let unrelated = spawn_with_grants(
        &runtime,
        vec![CapabilityGrant::new(
            Capability::SharedMemory,
            vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
        )],
    );
    runtime.project_metering(
        unrelated.process_id,
        MeteringObservation {
            cpu_instructions: 7,
            memory_bytes: 0,
            bandwidth_bytes: 0,
        },
    );

    // Parent reads unrelated process metering with Only Children → denied.
    let (status, _) = runtime.begin_hostcall(
        parent.process_id,
        HostcallRequest::MeteringRead {
            process_id: unrelated.process_id,
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_FAILED);
}

#[test]
fn guest_log_write_accepts_own_pid() {
    let runtime = Runtime::default();
    let guest = spawn_with_grants(
        &runtime,
        vec![CapabilityGrant::new(
            Capability::GuestLogWrite,
            vec![ResourceSelector::ResourceClass(ResourceClass::GuestLog)],
        )],
    );

    let entry = GuestLogEntry {
        process_id: Some(guest.process_id),
        level: "INFO".to_string(),
        target: "test".to_string(),
        message: "self log".to_string(),
    };
    let (status, _) =
        runtime.begin_hostcall(guest.process_id, HostcallRequest::GuestLogWrite { entry });
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_READY);
}

#[test]
fn child_grant_cannot_exceed_parent_authority() {
    let runtime = Runtime::default();

    // Parent has tenant-scoped ProcessLifecycle.
    let parent = runtime
        .spawn_system_guest(SystemGuestDescriptor {
            name: "contain-parent".to_string(),
            module_id: "contain-parent-module".to_string(),
            module_bytes: module_with_entrypoint("boot", ""),
            entrypoint: "boot".to_string(),
            arguments: Vec::new(),
            grants: vec![
                CapabilityGrant::new(
                    Capability::ProcessLifecycle,
                    vec![ResourceSelector::ResourceClass(ResourceClass::Process)],
                ),
                CapabilityGrant::new(
                    Capability::SharedMemory,
                    vec![
                        ResourceSelector::Tenant("acme".to_string()),
                        ResourceSelector::ResourceClass(ResourceClass::SharedRegion),
                    ],
                ),
            ],
            tenant: Some("acme".to_string()),
            discovery_service: false,
            handlers: Vec::new(),
        })
        .expect("spawn contain-parent");

    // Try to spawn a child with a Tenant("beta") grant that exceeds
    // the parent's Tenant("acme") authority.
    let (status, op) = runtime.begin_hostcall(
        parent.process_id,
        HostcallRequest::ProcessStart {
            source: selium_abi::ModuleSource::Inline(module_with_entrypoint("main", "")),
            entrypoint: "main".to_string(),
            arguments: Vec::new(),
            grants: vec![CapabilityGrant::new(
                Capability::SharedMemory,
                vec![
                    ResourceSelector::Tenant("beta".to_string()),
                    ResourceSelector::ResourceClass(ResourceClass::SharedRegion),
                ],
            )],
            tenant: None,
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_FAILED);

    match runtime.poll_hostcall(parent.process_id, op) {
        CompletionState::Failed(error) => {
            assert_eq!(error.code, AbiErrorCode::PermissionDenied);
            assert!(
                error.message.contains("exceeds parent authority")
                    || error.message.contains("child grant"),
                "error should mention parent authority: {error:?}"
            );
        }
        other => panic!("expected failed, got {other:?}"),
    }
}

#[test]
fn tcp_connect_rejects_hostname_with_malformed_payload() {
    let runtime = Runtime::default();
    let guest = spawn_with_grants(
        &runtime,
        vec![CapabilityGrant::new(
            Capability::Network,
            vec![ResourceSelector::ResourceClass(ResourceClass::TcpStream)],
        )],
    );

    // "localhost:80" is a hostname, not an IP literal.
    let (status, op) = runtime.begin_hostcall(
        guest.process_id,
        HostcallRequest::TcpConnect {
            address: "localhost:80".to_string(),
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_FAILED);

    match runtime.poll_hostcall(guest.process_id, op) {
        CompletionState::Failed(error) => {
            assert_eq!(error.code, AbiErrorCode::MalformedPayload);
            assert!(
                error.message.contains("IP literal"),
                "error should mention IP literal: {error:?}"
            );
        }
        other => panic!("expected failed hostcall, got {other:?}"),
    }
}

#[test]
fn tcp_bind_rejects_hostname_with_malformed_payload() {
    let runtime = Runtime::default();
    let guest = spawn_with_grants(
        &runtime,
        vec![CapabilityGrant::new(
            Capability::Network,
            vec![ResourceSelector::ResourceClass(ResourceClass::TcpListener)],
        )],
    );

    let (status, op) = runtime.begin_hostcall(
        guest.process_id,
        HostcallRequest::TcpBind {
            address: "example.com:0".to_string(),
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_FAILED);

    match runtime.poll_hostcall(guest.process_id, op) {
        CompletionState::Failed(error) => {
            assert_eq!(error.code, AbiErrorCode::MalformedPayload);
        }
        other => panic!("expected failed hostcall, got {other:?}"),
    }
}

#[test]
fn udp_bind_rejects_hostname_with_malformed_payload() {
    let runtime = Runtime::default();
    let guest = spawn_with_grants(
        &runtime,
        vec![CapabilityGrant::new(
            Capability::Network,
            vec![ResourceSelector::ResourceClass(ResourceClass::UdpSocket)],
        )],
    );

    let (status, op) = runtime.begin_hostcall(
        guest.process_id,
        HostcallRequest::UdpBind {
            address: "myhost.local:8080".to_string(),
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_FAILED);

    match runtime.poll_hostcall(guest.process_id, op) {
        CompletionState::Failed(error) => {
            assert_eq!(error.code, AbiErrorCode::MalformedPayload);
        }
        other => panic!("expected failed hostcall, got {other:?}"),
    }
}

#[test]
fn tcp_connect_accepts_ip_literal() {
    let runtime = Runtime::default();
    let guest = spawn_with_grants(
        &runtime,
        vec![CapabilityGrant::new(
            Capability::Network,
            vec![ResourceSelector::ResourceClass(ResourceClass::TcpStream)],
        )],
    );

    // "127.0.0.1:1" is a valid IP literal. The OS connect will fail
    // (connection refused), but it must NOT fail with MalformedPayload
    // (which would indicate our address check rejected it).
    let (status, op) = runtime.begin_hostcall(
        guest.process_id,
        HostcallRequest::TcpConnect {
            address: "127.0.0.1:1".to_string(),
        },
    );
    // Accept either ready or failed — the important thing is it wasn't
    // rejected at the validation step.
    if status == selium_abi::HOSTCALL_STATUS_FAILED {
        match runtime.poll_hostcall(guest.process_id, op) {
            CompletionState::Failed(error) => {
                assert_ne!(
                    error.code,
                    AbiErrorCode::MalformedPayload,
                    "IP literal must not be rejected as MalformedPayload"
                );
            }
            other => panic!("expected failed, got {other:?}"),
        }
    }
}

// scenario: selium-runtime/network-hostcall-dispatch/guest-binds-tcp-listener-via-runtime
#[tokio::test]
async fn tcp_bind_succeeds_with_ip_literal() {
    let runtime = Runtime::default();
    let guest = spawn_with_grants(
        &runtime,
        vec![CapabilityGrant::new(
            Capability::Network,
            vec![ResourceSelector::ResourceClass(ResourceClass::TcpListener)],
        )],
    );

    // "127.0.0.1:0" is a valid IP literal and should bind successfully.
    let (status, op) = runtime.begin_hostcall(
        guest.process_id,
        HostcallRequest::TcpBind {
            address: "127.0.0.1:0".to_string(),
        },
    );
    assert!(
        status == selium_abi::HOSTCALL_STATUS_READY
            || status == selium_abi::HOSTCALL_STATUS_PENDING,
        "expected ready or pending for valid TcpBind, got status {status}"
    );

    let output = ready(&runtime, guest.process_id, op);
    match output {
        HostcallOutput::HostQueue(descriptor) => {
            assert!(descriptor.shared_id > 0);
            assert!(descriptor.local_id > 0);
        }
        other => panic!("expected HostQueue output, got {other:?}"),
    }
}
// scenario: selium-abi/capability-probe-gated-by-authority/authorised-probe
// scenario: selium-abi/capability-probe-gated-by-authority/unauthorised-probe-denied
// scenario: selium-abi/capability-probe-scope/probe-requires-the-probe-authority

#[test]
fn process_capability_is_denied_without_probe_authority() {
    // Probing another process's grants requires the bootstrap-provisioned
    // `ProcessProbe` authority rather than any guest identity: exposing it
    // broadly would leak grant state.
    let runtime = Runtime::default();
    let guest = spawn_with_grants(&runtime, Vec::new());

    let (status, op) = runtime.begin_hostcall(
        guest.process_id,
        HostcallRequest::ProcessCapability {
            process_id: guest.process_id,
            capability: Capability::ActivityRead,
            uri: None,
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_FAILED);
    match runtime.poll_hostcall(guest.process_id, op) {
        CompletionState::Failed(error) => {
            assert_eq!(error.code, AbiErrorCode::PermissionDenied);
        }
        other => panic!("expected PermissionDenied, got {other:?}"),
    }

    // A holder of the probe authority gets a truthful answer.
    let prober = spawn_with_grants(
        &runtime,
        vec![CapabilityGrant::new(Capability::ProcessProbe, Vec::new())],
    );
    let (status, op) = runtime.begin_hostcall(
        prober.process_id,
        HostcallRequest::ProcessCapability {
            process_id: guest.process_id,
            capability: Capability::ActivityRead,
            uri: None,
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_READY);
    assert!(matches!(
        runtime.poll_hostcall(prober.process_id, op),
        CompletionState::Ready(HostcallOutput::U64(0))
    ));
}
// scenario: selium-abi/live-process-list/process-list-requires-the-probe-authority
// scenario: selium-abi/live-process-list/process-list-includes-a-silent-process

/// Process membership is registry state and is gated on the
/// bootstrap-provisioned probe authority, like capability probing.
#[test]
fn process_list_requires_the_probe_authority() {
    let runtime = Runtime::default();
    let guest = spawn_with_grants(&runtime, Vec::new());

    let (status, op) = runtime.begin_hostcall(guest.process_id, HostcallRequest::ProcessList);
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_FAILED);
    match runtime.poll_hostcall(guest.process_id, op) {
        CompletionState::Failed(error) => {
            assert_eq!(error.code, AbiErrorCode::PermissionDenied);
        }
        other => panic!("expected PermissionDenied, got {other:?}"),
    }

    let prober = spawn_with_grants(
        &runtime,
        vec![CapabilityGrant::new(Capability::ProcessProbe, Vec::new())],
    );
    let (status, op) = runtime.begin_hostcall(prober.process_id, HostcallRequest::ProcessList);
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_READY);
    let CompletionState::Ready(HostcallOutput::Processes(processes)) =
        runtime.poll_hostcall(prober.process_id, op)
    else {
        panic!("expected Processes output");
    };
    assert!(processes.contains(&guest.process_id));
    assert!(processes.contains(&prober.process_id));
    assert!(processes.windows(2).all(|pair| pair[0] <= pair[1]));
}

/// A probe answers truthfully for a tenant-scoped grant: the target holds it
/// within its own tenant, and the probe must not report "not held" because it
/// evaluated against an empty scope.
#[test]
fn process_capability_reports_a_tenant_scoped_grant_as_held() {
    let runtime = Runtime::default();
    let tenant_guest = spawn_with_grants_and_tenant(
        &runtime,
        "tenant-scoped",
        vec![CapabilityGrant::new(
            Capability::ActivityRead,
            vec![ResourceSelector::Tenant("acme".to_string())],
        )],
        Some("acme"),
    );
    let prober = spawn_with_grants(
        &runtime,
        vec![CapabilityGrant::new(Capability::ProcessProbe, Vec::new())],
    );

    let (status, op_id) = runtime.begin_hostcall(
        prober.process_id,
        HostcallRequest::ProcessCapability {
            process_id: tenant_guest.process_id,
            capability: Capability::ActivityRead,
            uri: None,
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_READY);
    assert!(
        matches!(
            runtime.poll_hostcall(prober.process_id, op_id),
            CompletionState::Ready(HostcallOutput::U64(1))
        ),
        "a tenant-scoped grant is held within that tenant"
    );

    // A capability the tenant does not hold is still reported as absent, so the
    // probe discloses nothing beyond the capability asked for.
    let (status, op_id) = runtime.begin_hostcall(
        prober.process_id,
        HostcallRequest::ProcessCapability {
            process_id: tenant_guest.process_id,
            capability: Capability::MintCertificate,
            uri: None,
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_READY);
    assert!(matches!(
        runtime.poll_hostcall(prober.process_id, op_id),
        CompletionState::Ready(HostcallOutput::U64(0))
    ));
}
// scenario: selium-abi/process-start-module-source/spawn-from-inline-bytes
// scenario: selium-abi/process-start-module-source/no-host-side-catalogue

/// A process starts from inline bytes with no host-side module catalogue:
/// the caller supplies the module source for every spawn.
#[test]
fn process_start_uses_inline_module_bytes_without_a_registry() {
    let runtime = Runtime::default();
    let spawner = spawn_with_grants(
        &runtime,
        vec![CapabilityGrant::new(
            Capability::ProcessLifecycle,
            vec![ResourceSelector::ResourceClass(ResourceClass::Process)],
        )],
    );
    let (_, op) = runtime.begin_hostcall(
        spawner.process_id,
        HostcallRequest::ProcessStart {
            source: selium_abi::ModuleSource::Inline(module_with_entrypoint("main", "")),
            entrypoint: "main".to_string(),
            arguments: Vec::new(),
            grants: Vec::new(),
            tenant: None,
        },
    );
    assert!(matches!(
        ready(&runtime, spawner.process_id, op),
        HostcallOutput::Process(_)
    ));
}

// scenario: selium-abi/process-start-module-source/spawn-from-a-durable-slice
/// A durable slice is read through the runtime's own volume layer: a
/// caller with storage authority starts from a slice it wrote.
#[test]
fn process_start_reads_a_module_from_a_durable_slice() {
    let runtime = Runtime::default();
    let spawner = spawn_with_grants(
        &runtime,
        vec![
            CapabilityGrant::new(
                Capability::ProcessLifecycle,
                vec![ResourceSelector::ResourceClass(ResourceClass::Process)],
            ),
            CapabilityGrant::new(
                Capability::Storage,
                vec![ResourceSelector::ResourceClass(ResourceClass::Volume)],
            ),
        ],
    );
    let module = module_with_entrypoint("main", "");
    let (_, open_op) = runtime.begin_hostcall(
        spawner.process_id,
        HostcallRequest::VolumeOpen {
            name: "module-vol".to_string(),
            serving_tenant: None,
        },
    );
    let HostcallOutput::Volume(volume) = ready(&runtime, spawner.process_id, open_op) else {
        panic!("expected volume descriptor");
    };
    let (_, append_op) = runtime.begin_hostcall(
        spawner.process_id,
        HostcallRequest::VolumeAppend {
            local_id: volume.local_id,
            bytes: module.clone(),
        },
    );
    drop(ready(&runtime, spawner.process_id, append_op));

    let (_, op) = runtime.begin_hostcall(
        spawner.process_id,
        HostcallRequest::ProcessStart {
            source: selium_abi::ModuleSource::Slice {
                volume: "module-vol".to_string(),
                offset: 0,
                len: module.len() as u64,
            },
            entrypoint: "main".to_string(),
            arguments: Vec::new(),
            grants: Vec::new(),
            tenant: None,
        },
    );
    assert!(matches!(
        ready(&runtime, spawner.process_id, op),
        HostcallOutput::Process(_)
    ));
}

// scenario: selium-abi/process-start-module-source/slice-outside-the-caller-s-authority
/// A slice on a volume the caller's storage authority does not cover is
/// denied with a permission error, and no process is created.
#[test]
fn process_start_denies_a_slice_outside_the_callers_authority() {
    let runtime = Runtime::default();
    let spawner = spawn_with_grants(
        &runtime,
        vec![CapabilityGrant::new(
            Capability::ProcessLifecycle,
            vec![ResourceSelector::ResourceClass(ResourceClass::Process)],
        )],
    );
    let (status, op) = runtime.begin_hostcall(
        spawner.process_id,
        HostcallRequest::ProcessStart {
            source: selium_abi::ModuleSource::Slice {
                volume: "someone-elses-vol".to_string(),
                offset: 0,
                len: 64,
            },
            entrypoint: "main".to_string(),
            arguments: Vec::new(),
            grants: Vec::new(),
            tenant: None,
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_FAILED);
    assert!(matches!(
        runtime.poll_hostcall(spawner.process_id, op),
        CompletionState::Failed(error) if error.code == AbiErrorCode::PermissionDenied
    ));
}

// scenario: selium-abi/waitregister-hostcall/wake-routed-via-envelope-task
/// Task 1.1: a guest registering interest in a region through
/// `WaitRegister` is recorded in the wait registry under
/// `(process_id, region_id)`, so a later generation advance — from this
/// process or any other — can route a wake back to its task.
#[test]
fn wait_register_records_the_waiting_process_keyed_by_region() {
    let runtime = Runtime::default();
    let guest = spawn_with_grants(
        &runtime,
        vec![CapabilityGrant::new(
            Capability::SharedMemory,
            vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
        )],
    );
    let region_id = allocate_region(&runtime, guest.process_id);

    let (status, _op) = runtime.begin_hostcall_with_task(
        guest.process_id,
        HostcallRequest::WaitRegister {
            region_id,
            generation: 7,
        },
        Some(1),
        None,
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_READY);

    let registry = runtime.wait_registry.lock();
    let entries = registry
        .get(&(guest.process_id, region_id))
        .expect("wait registered under (process, region)");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries.first().map(|entry| entry.generation), Some(7));
    assert_eq!(entries.first().map(|entry| entry.task_id), Some(1));
}

/// Task 1.3: a generation advance that targets a stopped process is a
/// no-op — it neither panics nor leaves a stale registration behind, and
/// a repeat advance stays a no-op (delivery is idempotent).
#[test]
fn generation_advance_for_a_stopped_process_is_a_no_op() {
    let runtime = Runtime::default();
    let guest = spawn_with_grants(
        &runtime,
        vec![CapabilityGrant::new(
            Capability::SharedMemory,
            vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
        )],
    );
    let region_id = allocate_region(&runtime, guest.process_id);
    let (status, _op) = runtime.begin_hostcall_with_task(
        guest.process_id,
        HostcallRequest::WaitRegister {
            region_id,
            generation: 1,
        },
        Some(1),
        None,
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_READY);

    runtime
        .stop_process(guest.process_id)
        .expect("stop process with an outstanding wait registration");

    // Deliver twice: the mailbox for a stopped process is gone, so each
    // advance is dropped without error and without leaving the registry
    // populated.
    runtime.note_generation_advance(region_id, 2);
    runtime.note_generation_advance(region_id, 3);
    assert!(
        runtime.wait_registry.lock().is_empty(),
        "a delivered wake must not leave the registration behind"
    );
}

// scenario: selium-abi/waitregister-hostcall/unattached-region-rejected
// scenario: channel-wake-wait/cross-process-wait-registration/registration-for-unattached-region
#[test]
fn wait_register_for_an_unattached_region_is_denied() {
    let runtime = Runtime::default();
    let shared_memory = || {
        CapabilityGrant::new(
            Capability::SharedMemory,
            vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
        )
    };
    let owner = spawn_with_grants(&runtime, vec![shared_memory()]);
    let stranger = spawn_with_grants(&runtime, vec![shared_memory()]);
    let region_id = allocate_region(&runtime, owner.process_id);

    // Holding the class grant is not enough: a wake registration requires a
    // live mapping, otherwise a guest could probe for the existence of
    // regions it never attached.
    let (status, op) = runtime.begin_hostcall_with_task(
        stranger.process_id,
        HostcallRequest::WaitRegister {
            region_id,
            generation: 1,
        },
        Some(1),
        None,
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_FAILED);
    assert!(matches!(
        runtime.poll_hostcall(stranger.process_id, op),
        CompletionState::Failed(error) if error.code == AbiErrorCode::PermissionDenied
    ));
    assert!(
        runtime.wait_registry.lock().is_empty(),
        "a denied registration must leave no entry behind"
    );
}

// scenario: selium-abi/expected-claims-and-process-list/another-process-s-expected-claims-require-the-probe-authority
#[test]
fn expected_claims_of_another_process_require_the_probe_authority() {
    let runtime = Runtime::default();
    let guest = spawn_with_grants(&runtime, Vec::new());
    let other = spawn_with_grants(&runtime, Vec::new());

    // A process always sees its own expectation.
    let (status, op) = runtime.begin_hostcall(
        guest.process_id,
        HostcallRequest::ExpectedClaims {
            process_id: guest.process_id,
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_READY);
    assert!(matches!(
        runtime.poll_hostcall(guest.process_id, op),
        CompletionState::Ready(HostcallOutput::Claims(_))
    ));

    // Another process's expectation is registry state.
    let (status, op) = runtime.begin_hostcall(
        guest.process_id,
        HostcallRequest::ExpectedClaims {
            process_id: other.process_id,
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_FAILED);
    assert!(matches!(
        runtime.poll_hostcall(guest.process_id, op),
        CompletionState::Failed(error) if error.code == AbiErrorCode::PermissionDenied
    ));

    let prober = spawn_with_grants(
        &runtime,
        vec![CapabilityGrant::new(Capability::ProcessProbe, Vec::new())],
    );
    let (status, op) = runtime.begin_hostcall(
        prober.process_id,
        HostcallRequest::ExpectedClaims {
            process_id: other.process_id,
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_READY);
    assert!(matches!(
        runtime.poll_hostcall(prober.process_id, op),
        CompletionState::Ready(HostcallOutput::Claims(_))
    ));
}

// scenario: selium-abi/capability-probe-scope/route-claim-is-probed-in-the-uri-s-scope
#[test]
fn capability_probe_evaluates_a_route_claim_in_the_uri_scope() {
    let runtime = Runtime::default();
    let claimant = spawn_with_grants(
        &runtime,
        vec![CapabilityGrant::new(
            Capability::RouteClaim,
            vec![ResourceSelector::Uri("sel:///control".to_string())],
        )],
    );
    let prober = spawn_with_grants(
        &runtime,
        vec![CapabilityGrant::new(Capability::ProcessProbe, Vec::new())],
    );

    let probe = |uri: &str| {
        let (status, op) = runtime.begin_hostcall(
            prober.process_id,
            HostcallRequest::ProcessCapability {
                process_id: claimant.process_id,
                capability: Capability::RouteClaim,
                uri: Some(uri.to_string()),
            },
        );
        assert_eq!(status, selium_abi::HOSTCALL_STATUS_READY);
        match runtime.poll_hostcall(prober.process_id, op) {
            CompletionState::Ready(HostcallOutput::U64(held)) => held,
            other => panic!("expected U64 probe answer, got {other:?}"),
        }
    };

    assert_eq!(probe("sel:///control"), 1, "claim holds in its URI scope");
    assert_eq!(probe("sel:///other"), 0, "claim does not extend past it");
}

// scenario: selium-abi/tenant-volume-usage-hostcall/unauthorised-caller-denied
#[test]
fn volume_usage_requires_the_quota_write_authority() {
    let runtime = Runtime::default();
    let guest = spawn_with_grants(&runtime, Vec::new());
    let (status, op) = runtime.begin_hostcall(
        guest.process_id,
        HostcallRequest::VolumeUsage {
            tenant: "acme".to_string(),
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_FAILED);
    assert!(matches!(
        runtime.poll_hostcall(guest.process_id, op),
        CompletionState::Failed(error) if error.code == AbiErrorCode::PermissionDenied
    ));
}

// scenario: selium-abi/cpu-resource-class/cpu-class-allocates-no-resource
// scenario: process-metering/cpu-ceiling-delivery-under-a-resource-class/cpu-class-allocates-nothing
#[test]
fn allocating_a_shared_region_leaves_the_cpu_class_untouched() {
    let runtime = Runtime::default();
    let guest = spawn_with_grants_and_tenant(
        &runtime,
        "cpu-class-guest",
        vec![CapabilityGrant::new(
            Capability::SharedMemory,
            vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
        )],
        Some("acme"),
    );
    let _region = allocate_region(&runtime, guest.process_id);

    assert_eq!(
        runtime.kernel.quota().used("acme", ResourceClass::Cpu),
        0,
        "allocating bytes must not consume the CPU class"
    );
    assert!(
        runtime
            .kernel
            .quota()
            .used("acme", ResourceClass::SharedRegion)
            > 0,
        "the allocation is charged to its own class"
    );
}

// --- Spec traceability (scripts/check-traceability.sh) -------------------
// These tests are the evidence for the `capability-enforcement` scenarios. One scenario may
// be claimed by several tests; the annotation lives next to the evidence.
