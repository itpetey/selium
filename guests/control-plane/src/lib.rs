//! Control-plane system guest.
//!
//! Serves a typed, capability-gated control RPC surface to externally
//! authenticated clients. The guest creates its own listener (a host-mediated
//! connection queue), registers it as a named service (`sel://<tenant>/control`,
//! external wire name `control.<tenant>`) through discovery, and runs the
//! existing shared-memory RPC path over it: each client session is a two-ring
//! shared-memory (request/reply) region the client allocates with
//! `rpc::connect` and rendezvous to the control plane's queue, which the
//! control plane `rpc::accept`s — the same path the discovery guest serves.
//! Data flows over shared memory; only control flows over hostcalls.
//!
//! The control plane owns **user-facing desired state** (deployments and
//! pipeline bindings) in a durable log, projects it into an in-memory read
//! model rebuilt from replay, and delegates platform policy:
//!
//! - **placement/scale/stop** → the scheduler (typed `SchedulerRequest`),
//! - **resolve** → discovery (`Context::lookup`),
//! - **module upload** → storage hostcalls (`StorageBlobPut` +
//!   `StorageBlobSetManifest`).
//!
//! Access is enforced by the capability system at attach time; the guest does
//! not re-derive client identity (see `admit_control_client`).

use std::{cell::RefCell, collections::BTreeMap, rc::Rc};

use anyhow::{Context as _, bail};
use selium_abi::{
    Capability, CapabilityGrant, ControlRequest, ControlResponse, DelegationStatus, Deployment,
    DesiredStateRecord, PipelineBinding, ResolvedTarget, ResourceClass, ResourceSelector,
    ResourceTarget, SchedulerRequest, SchedulerResponse, ScopeContext, decode_rkyv, encode_rkyv,
};
use selium_guest::{
    BlobStore, Context, DurableLog, ResourceListener, Serve, debug, entrypoint, info, mark_ready,
    spawn, warn,
};
use selium_shm::rpc;

/// The internal route segment the control plane serves under.
pub const CONTROL_PATH: &str = "control";
/// Durable log name holding the control plane's desired-state records.
pub const CONTROL_LOG: &str = "selium.control-plane.desired-state";
/// Blob store name holding uploaded module bytes.
pub const CONTROL_BLOB_STORE: &str = "selium.control-plane.modules";

/// The desired-state read model: deployments and pipeline bindings projected
/// from the control plane's durable log.
///
/// The durable log is the store of record; this structure is rebuilt from
/// replay on boot and updated in lock-step with each append.
#[derive(Debug, Clone, Default)]
pub struct ControlPlaneState {
    deployments: BTreeMap<String, Deployment>,
    pipelines: BTreeMap<String, PipelineBinding>,
}

impl ControlPlaneState {
    /// Applies one desired-state record (a log entry, in replay order).
    pub fn apply_record(&mut self, record: DesiredStateRecord) {
        match record {
            DesiredStateRecord::Deployment(deployment) => {
                self.deployments
                    .insert(deployment.workload_id.clone(), deployment);
            }
            DesiredStateRecord::PipelineBinding(binding) => {
                self.pipelines.insert(binding.name.clone(), binding);
            }
            DesiredStateRecord::Stop { workload_id } => {
                self.deployments.remove(&workload_id);
            }
        }
    }

    /// Returns the last accepted desired state for a workload.
    pub fn deployment(&self, workload_id: &str) -> Option<&Deployment> {
        self.deployments.get(workload_id)
    }

    /// Returns the last accepted binding for a named pipeline.
    pub fn pipeline(&self, name: &str) -> Option<&PipelineBinding> {
        self.pipelines.get(name)
    }

    /// Materialises the projection from previously appended log records.
    fn rebuild(&mut self, log: &DurableLog) -> selium_guest::Result<()> {
        let records = log.replay(None, u32::MAX)?;
        for record in records {
            match decode_rkyv::<DesiredStateRecord>(&record.payload) {
                Ok(desired) => self.apply_record(desired),
                Err(error) => {
                    warn!("control-plane: skipping undecodable desired-state record: {error}");
                }
            }
        }
        Ok(())
    }
}

/// Appends a desired-state record to the durable log and applies it to the
/// projection, keeping the store of record and the read model in step.
fn record(
    log: &DurableLog,
    state: &RefCell<ControlPlaneState>,
    record: DesiredStateRecord,
) -> selium_guest::Result<()> {
    let timestamp_ms = selium_guest::time::now().map(|nanos| nanos / 1_000_000)?;
    let payload = encode_rkyv(&record)?;
    log.append(timestamp_ms, Vec::new(), payload)?;
    state.borrow_mut().apply_record(record);
    Ok(())
}

/// Scheduler delegation seam.
///
/// Day-1 boundary: the scheduler guest's RPC service is not yet online
/// (`implement-system-guests` §5), so delegation returns a typed deferred
/// status — recording intent without pretending it was applied. When the
/// scheduler service lands, this becomes a
/// `selium_shm::rpc::RpcClient<SchedulerRequest, SchedulerResponse>` resolved
/// through discovery; this method is the seam that client replaces.
#[derive(Debug, Clone, Copy, Default)]
pub struct SchedulerClient;

impl SchedulerClient {
    /// Delegates one scheduler interaction, returning its typed outcome.
    pub fn delegate(&self, request: SchedulerRequest) -> selium_guest::Result<SchedulerResponse> {
        debug!(
            ?request,
            "control-plane: scheduler delegation deferred until the scheduler guest lands its RPC service"
        );
        Ok(SchedulerResponse::Deferred {
            reason: "scheduler service not yet online".to_string(),
        })
    }
}

/// Maps a scheduler response to a typed delegation status.
fn delegation_status(response: SchedulerResponse) -> DelegationStatus {
    match response {
        SchedulerResponse::Applied => DelegationStatus {
            step: "scheduler".to_string(),
            applied: true,
            context: "applied".to_string(),
        },
        SchedulerResponse::Deferred { reason } => DelegationStatus {
            step: "scheduler".to_string(),
            applied: false,
            context: reason,
        },
        SchedulerResponse::Rejected { reason } => DelegationStatus {
            step: "scheduler".to_string(),
            applied: false,
            context: reason,
        },
    }
}

/// Maps a discovery lookup outcome to a typed resolve response.
pub fn resolve_response(target: Option<ResourceTarget>) -> ControlResponse {
    match target {
        Some(target) => ControlResponse::Resolved {
            target: Some(ResolvedTarget {
                uri: target.uri,
                host_id: target.host_id,
                resource_id: target.resource_id,
            }),
        },
        None => ControlResponse::Resolved { target: None },
    }
}

/// Delegates one scheduler interaction and returns the accepted response
/// carrying the typed delegated status.
fn accept_delegated(
    workload_id: &str,
    replicas: u32,
    module: String,
    request: SchedulerRequest,
) -> ControlResponse {
    match SchedulerClient.delegate(request) {
        Ok(response) => {
            let delegated = delegation_status(response);
            ControlResponse::Accepted {
                workload_id: workload_id.to_string(),
                replicas,
                module,
                delegated,
            }
        }
        Err(error) => ControlResponse::Error {
            step: "scheduler".to_string(),
            context: format!("{error}"),
        },
    }
}

/// Handles one decoded control request: `Resolve` routes through the async
/// discovery client, every other verb records desired state and/or delegates.
async fn handle_request(
    ctx: &mut Context,
    log: &DurableLog,
    blobs: &BlobStore,
    state: &RefCell<ControlPlaneState>,
    request: ControlRequest,
) -> ControlResponse {
    match request {
        ControlRequest::Resolve { uri } => match ctx.lookup(&uri).await {
            Ok(target) => resolve_response(target),
            Err(error) => ControlResponse::Error {
                step: "discovery".to_string(),
                context: format!("{error}"),
            },
        },
        ControlRequest::Upload { manifest, bytes } => {
            match blobs
                .put(bytes)
                .and_then(|blob_id| blobs.set_manifest(&manifest, blob_id))
            {
                Ok(()) => ControlResponse::Uploaded { manifest },
                Err(error) => ControlResponse::Error {
                    step: "storage".to_string(),
                    context: format!("{error}"),
                },
            }
        }
        ControlRequest::Deploy {
            workload_id,
            replicas,
            module,
        } => {
            let deployment = Deployment {
                workload_id: workload_id.clone(),
                replicas,
                module: module.clone(),
            };
            if let Err(error) = record(log, state, DesiredStateRecord::Deployment(deployment)) {
                return ControlResponse::Error {
                    step: "storage".to_string(),
                    context: format!("{error}"),
                };
            }
            let request = SchedulerRequest::Place {
                workload_id: workload_id.clone(),
                replicas,
            };
            accept_delegated(&workload_id, replicas, module, request)
        }
        ControlRequest::Scale {
            workload_id,
            replicas,
        } => {
            let module = state
                .borrow()
                .deployment(&workload_id)
                .map(|deployment| deployment.module.clone())
                .unwrap_or_default();
            let deployment = Deployment {
                workload_id: workload_id.clone(),
                replicas,
                module: module.clone(),
            };
            if let Err(error) = record(log, state, DesiredStateRecord::Deployment(deployment)) {
                return ControlResponse::Error {
                    step: "storage".to_string(),
                    context: format!("{error}"),
                };
            }
            let request = SchedulerRequest::Scale {
                workload_id: workload_id.clone(),
                replicas,
            };
            accept_delegated(&workload_id, replicas, module, request)
        }
        ControlRequest::Stop { workload_id } => {
            if let Err(error) = record(
                log,
                state,
                DesiredStateRecord::Stop {
                    workload_id: workload_id.clone(),
                },
            ) {
                return ControlResponse::Error {
                    step: "storage".to_string(),
                    context: format!("{error}"),
                };
            }
            let request = SchedulerRequest::Stop {
                workload_id: workload_id.clone(),
            };
            accept_delegated(&workload_id, 0, String::new(), request)
        }
        ControlRequest::Status { workload_id } => ControlResponse::Status {
            deployment: state.borrow().deployment(&workload_id).cloned(),
        },
    }
}

/// The grant set assigned to the control-plane guest: storage (durable log and
/// module blob store), shared memory (the RPC session rings), and host queue
/// (the serving listener plus the pre-connected discovery RPC client). All
/// scoped to the guest's own tenant.
pub fn control_plane_grants(tenant: &str) -> Vec<CapabilityGrant> {
    vec![
        CapabilityGrant::new(
            Capability::Storage,
            vec![
                ResourceSelector::Tenant(tenant.to_string()),
                ResourceSelector::ResourceClass(ResourceClass::DurableLog),
            ],
        ),
        CapabilityGrant::new(
            Capability::Storage,
            vec![
                ResourceSelector::Tenant(tenant.to_string()),
                ResourceSelector::ResourceClass(ResourceClass::BlobStore),
            ],
        ),
        CapabilityGrant::new(
            Capability::SharedMemory,
            vec![
                ResourceSelector::Tenant(tenant.to_string()),
                ResourceSelector::ResourceClass(ResourceClass::SharedRegion),
            ],
        ),
        CapabilityGrant::new(
            Capability::HostQueue,
            vec![
                ResourceSelector::Tenant(tenant.to_string()),
                ResourceSelector::ResourceClass(ResourceClass::HostQueue),
            ],
        ),
    ]
}

/// Returns whether a client's grant set admits an attach to the control
/// surface: a tenant-scoped host-queue admission (the serving listener's
/// resource class).
///
/// This mirrors the grant matrix the runtime enforces at attach time. It is a
/// grant-matrix evaluation, not an ad-hoc identity check — the control plane
/// never parses client identity to admit a session.
pub fn admit_control_client(grants: &[CapabilityGrant], tenant: &str) -> bool {
    let scope = ScopeContext {
        tenant: Some(tenant.to_string()),
        resource_class: Some(ResourceClass::HostQueue),
        ..ScopeContext::default()
    };
    grants
        .iter()
        .any(|grant| grant.capability == Capability::HostQueue && grant.allows(&scope))
}

/// Serves one accepted RPC session: each request is decoded (`request.payload`
/// is the typed decode; a failure becomes a typed serialization error rather
/// than any text-grammar interpretation), handled, and replied to over the
/// session's reply ring.
async fn handle_connection(
    mut connection: rpc::RpcConnection<ControlRequest, ControlResponse>,
    discovery_handle: u64,
    state: Rc<RefCell<ControlPlaneState>>,
    log: DurableLog,
    blobs: BlobStore,
) {
    // Each connection builds its own discovery client for `Resolve`; the
    // bootstrap context cannot be shared across concurrent handlers.
    let mut ctx = match Context::from_raw(discovery_handle).await {
        Ok(ctx) => ctx,
        Err(error) => {
            warn!("control-plane: discovery client failed: {error}");
            return;
        }
    };

    loop {
        match connection.recv().await {
            Ok(request) => {
                let response = match request.payload() {
                    Ok(payload) => handle_request(&mut ctx, &log, &blobs, &state, payload).await,
                    Err(error) => {
                        warn!("control-plane: request decode failed: {error}");
                        ControlResponse::Error {
                            step: "decode".to_string(),
                            context: format!("{error}"),
                        }
                    }
                };
                if request.reply(response).await.is_err() {
                    warn!("control-plane: reply failed");
                    break;
                }
            }
            Err(rpc::RpcError::ConnectionClosed) => break,
            Err(error) => {
                warn!("control-plane: recv failed: {error}");
                break;
            }
        }
    }
}

/// Control-plane entrypoint.
///
/// Creates its own listener, registers the `control` serving route with
/// discovery, and reports readiness only after the route is registered —
/// mirroring `bridge-server`'s self-registration — then accepts typed
/// shared-memory RPC sessions (mirroring `discovery`).
#[entrypoint]
async fn control_plane_main(mut ctx: Context) -> anyhow::Result<()> {
    drop(selium_guest::log::init());
    info!("control-plane: started");

    // The served surface is `control.<tenant>`; without a tenant scope there
    // is no wire name to serve under, so refuse to serve.
    let (_, own_tenant) =
        selium_guest::self_info().with_context(|| "control-plane: self info failed")?;
    let Some(own_tenant) = own_tenant else {
        bail!("control-plane: no tenant scope provisioned; refusing to serve");
    };

    let log = DurableLog::open(CONTROL_LOG).with_context(|| "control-plane: log open failed")?;
    let blobs = BlobStore::open(CONTROL_BLOB_STORE)
        .with_context(|| "control-plane: blob store open failed")?;
    let mut desired = ControlPlaneState::default();
    desired
        .rebuild(&log)
        .with_context(|| "control-plane: projection rebuild failed")?;
    let state = Rc::new(RefCell::new(desired));

    // The server creates its own listener: self-registration replaces the
    // runtime's well-known-URI queue minting.
    let listener =
        ResourceListener::create().with_context(|| "control-plane: create listener failed")?;

    // Register the serving route (`sel://<tenant>/control`, wire name
    // `control.<tenant>`) from one declaration.
    let target = ResourceTarget {
        uri: String::new(), // pinned by `serve` to the derived internal path
        host_id: String::new(),
        resource_id: listener.descriptor().shared_id,
        interface: None,
        tenant: Some(own_tenant.clone()),
        class: ResourceClass::HostQueue,
        labels: Vec::new(),
    };
    ctx.serve(Serve {
        path: vec![CONTROL_PATH.to_string()],
        target,
        default: false,
    })
    .await
    .with_context(|| "control-plane: serve failed")?;

    // Ready only after the route is registered.
    mark_ready();

    let discovery_handle = ctx.raw_handle();

    loop {
        let incoming = match listener.recv().await {
            Ok(incoming) => incoming,
            Err(error) => {
                warn!("control-plane: accept failed: {error}");
                continue;
            }
        };
        let connection = match rpc::accept::<ControlRequest, ControlResponse>(incoming.into()) {
            Ok(connection) => connection,
            Err(error) => {
                warn!("control-plane: rpc accept failed: {error}");
                continue;
            }
        };
        spawn(handle_connection(
            connection,
            discovery_handle,
            state.clone(),
            log.clone(),
            blobs.clone(),
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use selium_wire::Rendezvous;

    fn deployment(workload_id: &str, replicas: u32, module: &str) -> Deployment {
        Deployment {
            workload_id: workload_id.to_string(),
            replicas,
            module: module.to_string(),
        }
    }

    fn binding(name: &str, from: &str, to: &str) -> PipelineBinding {
        PipelineBinding {
            name: name.to_string(),
            from: from.to_string(),
            to: to.to_string(),
        }
    }

    /// 4.2: the projection reconstructs from replay of appended records.
    #[test]
    fn projection_reconstructs_from_replayed_records() {
        let mut state = ControlPlaneState::default();
        let records = vec![
            DesiredStateRecord::Deployment(deployment("api", 2, "api/v1")),
            DesiredStateRecord::Deployment(deployment("api", 4, "api/v2")),
            DesiredStateRecord::PipelineBinding(binding("api-to-db", "api", "db")),
            DesiredStateRecord::Stop {
                workload_id: "gone".to_string(),
            },
            DesiredStateRecord::Deployment(deployment("gone", 1, "gone/v1")),
            DesiredStateRecord::Stop {
                workload_id: "gone".to_string(),
            },
        ];
        for record in records {
            state.apply_record(record);
        }

        assert_eq!(
            state.deployment("api"),
            Some(&deployment("api", 4, "api/v2")),
            "last accepted desired state wins"
        );
        assert_eq!(state.deployment("gone"), None, "stop tombstones remove");
        assert_eq!(
            state.pipeline("api-to-db"),
            Some(&binding("api-to-db", "api", "db"))
        );
    }

    /// 4.1 + 4.2: an accepted deployment intent round-trips through the log
    /// encoding and is recorded in the projection.
    #[test]
    fn deployment_intent_is_recorded_in_projection() {
        let record = DesiredStateRecord::Deployment(deployment("api", 3, "api/v1"));
        let payload = encode_rkyv(&record).expect("encode record");
        let decoded: DesiredStateRecord = decode_rkyv(&payload).expect("decode record");

        let mut state = ControlPlaneState::default();
        state.apply_record(decoded);

        assert_eq!(
            state.deployment("api"),
            Some(&deployment("api", 3, "api/v1"))
        );
    }

    /// 4.3: status reads return the last accepted desired state.
    #[test]
    fn status_reads_return_last_accepted_desired_state() {
        let mut state = ControlPlaneState::default();
        state.apply_record(DesiredStateRecord::Deployment(deployment(
            "api", 2, "api/v1",
        )));
        state.apply_record(DesiredStateRecord::Deployment(deployment(
            "api", 5, "api/v2",
        )));

        assert_eq!(
            state.deployment("api"),
            Some(&deployment("api", 5, "api/v2"))
        );
        assert_eq!(state.deployment("missing"), None);
    }

    /// 5.1: a discovery lookup maps to a typed resolve response.
    #[test]
    fn resolve_response_maps_found_and_missing_targets() {
        let found = resolve_response(Some(ResourceTarget {
            uri: "sel://acme/bridge".to_string(),
            host_id: "host-a".to_string(),
            resource_id: 42,
            interface: None,
            tenant: Some("acme".to_string()),
            class: ResourceClass::HostQueue,
            labels: Vec::new(),
        }));
        assert_eq!(
            found,
            ControlResponse::Resolved {
                target: Some(ResolvedTarget {
                    uri: "sel://acme/bridge".to_string(),
                    host_id: "host-a".to_string(),
                    resource_id: 42,
                }),
            }
        );

        assert_eq!(
            resolve_response(None),
            ControlResponse::Resolved { target: None }
        );
    }

    /// 5.2: the scheduler seam returns a typed deferred status (mapped to a
    /// delegation status by the dispatcher).
    #[test]
    fn scheduler_seam_returns_typed_deferred_status() {
        let client = SchedulerClient;
        let response = client
            .delegate(SchedulerRequest::Place {
                workload_id: "api".to_string(),
                replicas: 3,
            })
            .expect("delegate");

        let status = delegation_status(response);
        assert_eq!(status.step, "scheduler");
        assert!(!status.applied, "day-1 delegation is not applied");
        assert!(status.context.contains("not yet online"));
    }

    /// 6.2: an accepted deployment carries its module reference in the
    /// recorded desired state.
    #[test]
    fn deployment_records_module_reference() {
        let record = DesiredStateRecord::Deployment(deployment("api", 3, "api/v1"));
        let mut state = ControlPlaneState::default();
        state.apply_record(record);
        assert_eq!(
            state
                .deployment("api")
                .map(|deployment| deployment.module.as_str()),
            Some("api/v1")
        );
    }

    /// 7.3: the grant set covers storage, shared memory, and host queue, each
    /// scoped to the guest's tenant.
    #[test]
    fn grant_set_is_tenant_scoped() {
        let grants = control_plane_grants("acme");
        let capabilities: Vec<Capability> = grants
            .iter()
            .map(|grant| grant.capability.clone())
            .collect();
        assert!(capabilities.contains(&Capability::Storage));
        assert!(capabilities.contains(&Capability::SharedMemory));
        assert!(capabilities.contains(&Capability::HostQueue));

        // Every grant carries a tenant selector scoping it to "acme".
        assert!(grants.iter().all(|grant| {
            grant
                .selectors
                .iter()
                .any(|selector| matches!(selector, ResourceSelector::Tenant(t) if t == "acme"))
        }));
    }

    /// 7.3: a client without a control-plane grant is refused; a client with
    /// the tenant-scoped host-queue admission is admitted.
    #[test]
    fn authority_boundary_refuses_unprivileged_clients() {
        // A data-plane-only client: host queue in another tenant.
        let data_plane = vec![CapabilityGrant::new(
            Capability::HostQueue,
            vec![ResourceSelector::Tenant("other".to_string())],
        )];
        assert!(!admit_control_client(&data_plane, "acme"));

        // A control-plane client: host queue scoped to the control tenant.
        let control_plane = control_plane_grants("acme");
        assert!(admit_control_client(&control_plane, "acme"));
        assert!(!admit_control_client(&control_plane, "other"));
    }

    /// 3.2: a client half sends a typed request and receives a correlated
    /// typed reply over the shared-memory RPC path (two-ring session).
    #[tokio::test]
    async fn shared_memory_rpc_round_trips_typed_request_and_reply() {
        drop(selium_memory::set_region_provider(Box::new(
            selium_memory::HeapRegionProvider::new(),
        )));
        let rendezvous = selium_shm::ShmRendezvous::new();

        let server = {
            let rendezvous = rendezvous.clone();
            tokio::spawn(async move {
                let incoming = loop {
                    match rendezvous.recv().await {
                        Ok(connection) => break connection,
                        Err(_) => tokio::task::yield_now().await,
                    }
                };
                let mut connection: rpc::RpcConnection<ControlRequest, ControlResponse> =
                    rpc::accept(incoming).expect("accept");
                let request = connection.recv().await.expect("recv request");
                let ControlRequest::Deploy {
                    workload_id,
                    replicas,
                    module,
                } = request.payload().expect("decode request")
                else {
                    panic!("expected a deploy request");
                };
                request
                    .reply(ControlResponse::Accepted {
                        workload_id,
                        replicas,
                        module,
                        delegated: DelegationStatus {
                            step: "scheduler".to_string(),
                            applied: false,
                            context: "deferred".to_string(),
                        },
                    })
                    .await
                    .expect("reply");
            })
        };

        let mut client = rpc::connect::<ControlRequest, ControlResponse, _>(rendezvous, 0, 0)
            .await
            .expect("connect");
        let reply = client
            .request(ControlRequest::Deploy {
                workload_id: "api".to_string(),
                replicas: 3,
                module: "api/v1".to_string(),
            })
            .await
            .expect("request");

        assert_eq!(
            reply,
            ControlResponse::Accepted {
                workload_id: "api".to_string(),
                replicas: 3,
                module: "api/v1".to_string(),
                delegated: DelegationStatus {
                    step: "scheduler".to_string(),
                    applied: false,
                    context: "deferred".to_string(),
                },
            }
        );

        server.await.expect("server task");
    }
}
