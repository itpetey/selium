//! Bridge handoff substrate test.
//!
//! Drives the golden "external client joins the fabric" flow at the hostcall
//! substrate level: a connector delivers a per-stream region handoff carrying
//! an authenticated client identity as `HostQueueSend` metadata; the
//! bridge-server decodes it, maps it to grants, and spawns a bridge-channel
//! under `DelegateGrants` carrying the client's grants plus an
//! `ExplicitResource` for the handed-off region; the child inherits the
//! tenant and can attach the region. The QUIC/TLS wire behaviour is exercised
//! separately (connector native tests and the wasm spine test).
//!
//! ```sh
//! cargo test -p selium-runtime --test bridge_handoff
//! ```

use selium_abi::{
    Capability, CapabilityGrant, CompletionState, HostcallOutput, HostcallRequest, ProcessId,
    RegionProt, ResourceClass, ResourceIdentity, ResourceSelector, client_identity::ClientIdentity,
};
use selium_runtime::{ReadinessCondition, Runtime, RuntimeConfig, SystemGuestDescriptor};

#[test]
fn bridge_server_delegates_and_child_attaches_stream_region() {
    let runtime = Runtime::default();

    // Bootstrap the connector (registered Tier-1 handler for `sel-quic`) and
    // the bridge-server (provisioned with its well-known route,
    // `sel://acme/bridge`) together: the runtime provisions the
    // bridge-server's listener host queue, injects its shared id as the
    // leading entrypoint argument, and registers the URI with discovery
    // (4.2: bind registers with discovery).
    let report = runtime
        .bootstrap_system_guests(RuntimeConfig {
            start_discovery: false,
            system_guests: vec![
                SystemGuestDescriptor {
                    name: "bridge-connector".to_string(),
                    module_id: "bridge-connector-module".to_string(),
                    module_bytes: module_with_entrypoint("boot"),
                    entrypoint: "boot".to_string(),
                    arguments: Vec::new(),
                    grants: vec![
                        CapabilityGrant::new(
                            Capability::SharedMemory,
                            vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
                        ),
                        CapabilityGrant::new(
                            Capability::HostQueue,
                            vec![ResourceSelector::ResourceClass(ResourceClass::HostQueue)],
                        ),
                    ],
                    dependencies: Vec::new(),
                    readiness: ReadinessCondition::Immediate,
                    tenant: None,
                    well_known_uri: None,
                    handlers: vec!["sel-quic".to_string()],
                },
                SystemGuestDescriptor {
                    name: "bridge-server".to_string(),
                    module_id: "bridge-server-module".to_string(),
                    module_bytes: module_with_entrypoint_args("boot", 1),
                    entrypoint: "boot".to_string(),
                    arguments: Vec::new(),
                    grants: vec![
                        CapabilityGrant::new(
                            Capability::ProcessLifecycle,
                            vec![ResourceSelector::ResourceClass(ResourceClass::Process)],
                        ),
                        CapabilityGrant::new(
                            Capability::DelegateGrants,
                            vec![ResourceSelector::Tenant("acme".to_string())],
                        ),
                        CapabilityGrant::new(
                            Capability::HostQueue,
                            vec![ResourceSelector::ResourceClass(ResourceClass::HostQueue)],
                        ),
                    ],
                    dependencies: Vec::new(),
                    readiness: ReadinessCondition::Immediate,
                    tenant: Some("acme".to_string()),
                    well_known_uri: Some("sel://acme/bridge".to_string()),
                    handlers: Vec::new(),
                },
            ],
        })
        .expect("bootstrap connector and bridge-server");
    let find = |name: &str| {
        report
            .guests
            .iter()
            .find(|guest| guest.name == name)
            .unwrap_or_else(|| panic!("bootstrap report contains guest {name}"))
    };
    let connector = find("bridge-connector").process_id;
    let bridge_server_guest = find("bridge-server");
    let bridge_server = bridge_server_guest.process_id;
    let listener_shared_id = bridge_server_guest
        .well_known_listener
        .expect("well-known route provisions a listener queue");
    let (route_uri, route_shared_id) = runtime
        .well_known_uri(bridge_server)
        .expect("route registered with the runtime");
    assert_eq!(route_uri, "sel://acme/bridge");
    assert_eq!(route_shared_id, listener_shared_id);

    // The bridge-server pins its listener to the registered `sel-quic`
    // handler: handoffs from any other process must be refused. The handler
    // resolution is bootstrap-authoritative.
    let (_, handler_op) = runtime.begin_hostcall(
        bridge_server,
        HostcallRequest::ResolveProtocolHandler {
            scheme: "sel-quic".to_string(),
        },
    );
    match runtime.poll_hostcall(bridge_server, handler_op) {
        CompletionState::Ready(HostcallOutput::U64(handler)) => {
            assert_eq!(handler, connector, "handler pin must be the connector")
        }
        other => panic!("expected handler pid, got {other:?}"),
    }

    runtime
        .register_module_bytes(
            "bridge-channel-module".to_string(),
            module_with_entrypoint("bridge_channel"),
        )
        .expect("register bridge-channel module");

    // 1. The connector resolves the bridge route via discovery and attaches
    //    the provisioned listener queue (gaining its own local handle).
    discovery_records_resolve(&runtime, connector, listener_shared_id);
    let (_, attach_op) = runtime.begin_hostcall(
        connector,
        HostcallRequest::HostQueueAttach {
            shared_id: listener_shared_id,
        },
    );
    let CompletionState::Ready(HostcallOutput::HostQueue(connector_queue)) =
        runtime.poll_hostcall(connector, attach_op)
    else {
        panic!("connector should attach to the bridge queue");
    };

    // The bridge-server attaches its own provisioned listener queue (the
    // guest-side equivalent: `ResourceListener::attach(listener_arg)`).
    let (_, server_attach_op) = runtime.begin_hostcall(
        bridge_server,
        HostcallRequest::HostQueueAttach {
            shared_id: listener_shared_id,
        },
    );
    let CompletionState::Ready(HostcallOutput::HostQueue(server_listener)) =
        runtime.poll_hostcall(bridge_server, server_attach_op)
    else {
        panic!("bridge-server should attach its provisioned listener queue");
    };

    let (_, alloc_op) = runtime.begin_hostcall(
        connector,
        HostcallRequest::AllocRegion {
            pages: 1,
            prot: RegionProt::ReadWrite,
            purpose: selium_abi::ResourceKind::SharedMemory,
            serving_tenant: None,
        },
    );
    let CompletionState::Ready(HostcallOutput::RegionAlloc(stream_region)) =
        runtime.poll_hostcall(connector, alloc_op)
    else {
        panic!("expected stream region allocation");
    };

    // 2. The connector delivers the handoff with the authenticated identity.
    let identity = ClientIdentity {
        tenant: "acme".to_string(),
        fingerprint: [0x7A; 32],
    };
    let (send_status, _) = runtime.begin_hostcall(
        connector,
        HostcallRequest::HostQueueSend {
            local_id: connector_queue.local_id,
            value: stream_region.region_id,
            metadata: identity.encode(),
        },
    );
    assert_eq!(send_status, selium_abi::HOSTCALL_STATUS_READY);

    // 3. The bridge-server receives it on the provisioned listener and
    //    decodes the identity.
    let (_, recv_op) = runtime.begin_hostcall(
        bridge_server,
        HostcallRequest::HostQueueRecv {
            local_id: server_listener.local_id,
        },
    );
    let CompletionState::Ready(HostcallOutput::ConnectionInfo {
        client_process_id,
        value,
        metadata,
    }) = runtime.poll_hostcall(bridge_server, recv_op)
    else {
        panic!("bridge-server should receive the delivered handoff");
    };
    assert_eq!(client_process_id, connector);
    assert_eq!(value, stream_region.region_id);
    let decoded = ClientIdentity::decode(&metadata).expect("decode handoff identity");
    assert_eq!(decoded.tenant, "acme");

    // 4. The bridge-server spawns the bridge-channel with the client's grants
    //    plus a tenant-scoped ExplicitResource grant for the handed-off
    //    region, under the DelegateGrants path (the server does not itself
    //    hold these grants). Delegation admits the grants only because every
    //    child grant carries an in-scope Tenant selector.
    let child_grants = vec![
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
                ResourceSelector::ExplicitResource(ResourceIdentity::Shared(
                    stream_region.region_id,
                )),
            ],
        ),
    ];
    let (status, spawn_op) = runtime.begin_hostcall(
        bridge_server,
        HostcallRequest::ProcessStart {
            module_id: "bridge-channel-module".to_string(),
            entrypoint: "bridge_channel".to_string(),
            arguments: Vec::new(),
            grants: child_grants,
        },
    );
    assert_eq!(
        status,
        selium_abi::HOSTCALL_STATUS_READY,
        "delegated spawn must succeed"
    );
    let child_pid = match runtime.poll_hostcall(bridge_server, spawn_op) {
        CompletionState::Ready(HostcallOutput::Process(child)) => child.local_id,
        other => panic!("expected child process descriptor, got {other:?}"),
    };

    // 5. The bridge-channel inherits its parent's tenant.
    assert_eq!(
        runtime.process_tenant(child_pid).as_deref(),
        Some("acme"),
        "bridge-channel must inherit the bridge-server's tenant"
    );

    // 6. The child attaches the delivered region via its ExplicitResource
    //    grant, matching both the Tenant and ExplicitResource selectors.
    let (_, child_attach) = runtime.begin_hostcall(
        child_pid,
        HostcallRequest::AttachRegion {
            region_id: stream_region.region_id,
            reader_slot: None,
            prot: RegionProt::ReadWrite,
        },
    );
    assert!(matches!(
        runtime.poll_hostcall(child_pid, child_attach),
        CompletionState::Ready(HostcallOutput::RegionAttach(_))
    ));
}

fn discovery_records_resolve(runtime: &Runtime, client: ProcessId, shared_id: u64) {
    let discovery = spawn_guest(runtime, "discovery", Vec::new(), None);
    let (status, _op) = runtime.begin_hostcall(
        discovery,
        HostcallRequest::RecordResolvedQueueFor {
            client_process_id: client,
            shared_id,
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_READY);
}

fn module_with_entrypoint(entrypoint: &str) -> Vec<u8> {
    module_with_entrypoint_args(entrypoint, 0)
}

fn module_with_entrypoint_args(entrypoint: &str, args: usize) -> Vec<u8> {
    let params: String = "i64 ".repeat(args).trim_end().to_string();
    let params = if params.is_empty() {
        String::new()
    } else {
        format!("(param {params})")
    };
    wat::parse_str(format!(
        "(module (memory 1) (func (export \"{entrypoint}\") {params}))"
    ))
    .expect("compile wat")
}

fn spawn_guest(
    runtime: &Runtime,
    name: &str,
    grants: Vec<CapabilityGrant>,
    tenant: Option<&str>,
) -> ProcessId {
    let report = runtime
        .bootstrap_system_guests(RuntimeConfig {
            start_discovery: false,
            system_guests: vec![SystemGuestDescriptor {
                name: name.to_string(),
                module_id: format!("{name}-module"),
                module_bytes: module_with_entrypoint("boot"),
                entrypoint: "boot".to_string(),
                arguments: Vec::new(),
                grants,
                dependencies: Vec::new(),
                readiness: ReadinessCondition::Immediate,
                tenant: tenant.map(str::to_string),
                well_known_uri: None,
                handlers: Vec::new(),
            }],
        })
        .expect("bootstrap guest");
    report
        .guests
        .first()
        .expect("bootstrap report contains the requested guest")
        .process_id
}
