//! Hostcall dispatch: the host side of the guest/host ABI.
//!
//! Every entry point reserves a bounded operation record, dispatches by
//! [`HostcallRequest`] variant to the handler family that owns it, and
//! records the resulting state for [`Runtime::poll_hostcall`]. Handler
//! families live in sibling modules; this module owns only the shared
//! operation types and the routing match.

use std::time::Instant;

use selium_abi::{AbiError, AbiErrorCode, HostcallOutput, HostcallRequest, ProcessId, TaskId};
use wasmtiny::runtime::SharedMemory;

use crate::Runtime;

mod attach;
mod authority;
mod guest_log;
mod network;
mod operation;
mod process;
mod queue;
mod quota;
mod random;
mod region;
mod signing;
mod time;
mod volume;

/// Maximum operation records one process may retain before back-pressuring new
/// hostcalls. A guest that never polls or drops completions cannot grow host
/// memory without bound; other processes retain their own independent budget.
pub(crate) const MAX_OPERATIONS_PER_PROCESS: usize = 1024;

#[derive(Debug, Clone)]
pub(crate) enum HostOperationState {
    Dispatching,
    Ready(HostcallOutput),
    Failed(AbiError),
    HostQueueRecvWait { local_id: u64, deadline: Instant },
    SleepWait { deadline: Instant },
}

#[derive(Debug, Clone)]
pub(crate) struct HostOperation {
    pub(crate) process_id: ProcessId,
    pub(crate) task_id: Option<TaskId>,
    pub(crate) state: HostOperationState,
}

impl Runtime {
    fn dispatch_hostcall(
        &self,
        process_id: ProcessId,
        request: HostcallRequest,
        guest_memory: Option<&SharedMemory>,
    ) -> std::result::Result<HostOperationState, AbiError> {
        if !self.process_authorities.lock().contains_key(&process_id) {
            return Err(AbiError::new(
                AbiErrorCode::InvalidHandle,
                format!("unknown process authority {process_id}"),
            ));
        }

        match request {
            HostcallRequest::AllocRegion {
                pages,
                prot,
                purpose,
                serving_tenant,
            } => self.hostcall_alloc_region(process_id, pages, prot, purpose, serving_tenant),
            HostcallRequest::FreeRegion { region_id } => {
                self.hostcall_free_region(process_id, region_id)
            }
            HostcallRequest::AttachRegion {
                region_id,
                reader_slot,
                prot,
            } => {
                self.hostcall_attach_region(process_id, region_id, reader_slot, prot, guest_memory)
            }
            HostcallRequest::TcpBind { address } => self.hostcall_tcp_bind(process_id, address),
            HostcallRequest::TcpConnect { address } => {
                self.hostcall_tcp_connect(process_id, address)
            }
            HostcallRequest::UdpBind { address } => self.hostcall_udp_bind(process_id, address),
            HostcallRequest::VolumeOpen {
                name,
                serving_tenant,
            } => self.hostcall_volume_open(process_id, name, serving_tenant),
            HostcallRequest::VolumeAppend { local_id, bytes } => {
                self.hostcall_volume_append(process_id, local_id, bytes)
            }
            HostcallRequest::VolumeReadAt {
                local_id,
                offset,
                len,
            } => self.hostcall_volume_read_at(process_id, local_id, offset, len),
            HostcallRequest::VolumeLen { local_id } => {
                self.hostcall_volume_len(process_id, local_id)
            }
            HostcallRequest::VolumeList { serving_tenant } => {
                self.hostcall_volume_list(process_id, serving_tenant)
            }
            HostcallRequest::VolumeRemove {
                name,
                serving_tenant,
            } => self.hostcall_volume_remove(process_id, name, serving_tenant),
            HostcallRequest::VolumeSync { local_id } => {
                self.hostcall_volume_sync(process_id, local_id)
            }
            HostcallRequest::VolumeClose { local_id } => {
                self.hostcall_volume_close(process_id, local_id)
            }
            HostcallRequest::VolumeUsage { tenant } => {
                self.hostcall_volume_usage(process_id, tenant)
            }
            HostcallRequest::ProcessStart {
                source,
                entrypoint,
                arguments,
                grants,
                tenant,
            } => self
                .hostcall_process_start(process_id, source, entrypoint, arguments, grants, tenant),
            HostcallRequest::ProcessStop {
                process_id: target_process_id,
            } => self.hostcall_process_stop(process_id, target_process_id),
            HostcallRequest::ActivityRead { cursor } => {
                self.hostcall_activity_read(process_id, cursor)
            }
            HostcallRequest::MeteringRead {
                process_id: target_process_id,
            } => self.hostcall_metering_read(process_id, target_process_id),
            HostcallRequest::QuotaSet {
                tenant,
                class,
                limit,
            } => self.hostcall_quota_set(process_id, tenant, class, limit),
            HostcallRequest::QuotaClear { tenant, class } => {
                self.hostcall_quota_clear(process_id, tenant, class)
            }
            HostcallRequest::GuestLogWrite { entry } => {
                self.hostcall_guest_log_write(process_id, entry)
            }
            HostcallRequest::GuestLogRead {
                cursor,
                process_id: target_process_id,
            } => self.hostcall_guest_log_read(process_id, cursor, target_process_id),
            HostcallRequest::GuestLogRegister { shared_id } => {
                self.hostcall_guest_log_register(process_id, shared_id)
            }
            HostcallRequest::HostQueueCreate { serving_tenant } => {
                self.hostcall_host_queue_create(process_id, serving_tenant)
            }
            HostcallRequest::HostQueueAttach { shared_id } => {
                self.hostcall_host_queue_attach(process_id, shared_id)
            }
            HostcallRequest::HostQueueSend {
                local_id,
                value,
                metadata,
            } => self.hostcall_host_queue_send(process_id, local_id, value, metadata),
            HostcallRequest::HostQueueRecv { local_id } => {
                self.hostcall_host_queue_recv(process_id, local_id)
            }
            HostcallRequest::RandomBytes { len } => self.hostcall_random_bytes(len),
            HostcallRequest::AuthoriseAttach {
                resource_class,
                resource_id,
                client_process_id,
            } => self.hostcall_authorise_attach(
                process_id,
                resource_class,
                resource_id,
                client_process_id,
            ),
            HostcallRequest::DeclareAttachableByResolution {
                resource_class,
                resource_id,
                scope,
            } => self.hostcall_declare_attachable(process_id, resource_class, resource_id, scope),
            HostcallRequest::RecordResolvedResourceFor {
                resource_class,
                resource_id,
                client_process_id,
            } => self.hostcall_record_resolved(
                process_id,
                resource_class,
                resource_id,
                client_process_id,
            ),
            HostcallRequest::DiscoveryListener => self.hostcall_discovery_listener(process_id),
            HostcallRequest::SignTenantCa { tenant } => {
                self.hostcall_sign_tenant_ca(process_id, tenant)
            }
            HostcallRequest::CommitTenantCa {
                tenant,
                ca_cert_der,
            } => self.hostcall_commit_tenant_ca(process_id, tenant, ca_cert_der),
            HostcallRequest::RollbackTenantCa {
                tenant,
                ca_cert_der,
            } => self.hostcall_rollback_tenant_ca(process_id, tenant, ca_cert_der),
            HostcallRequest::SignUserCert { tenant, spki_der } => {
                self.hostcall_sign_user_cert(process_id, tenant, spki_der)
            }
            HostcallRequest::RevokeCa { tenant } => self.hostcall_revoke_ca(process_id, tenant),
            HostcallRequest::SelfInfo => self.hostcall_self_info(process_id),
            HostcallRequest::ProcessTenant { process_id } => {
                self.hostcall_process_tenant(process_id)
            }
            HostcallRequest::ProcessList => self.hostcall_process_list(process_id),
            HostcallRequest::ProcessOwnsResource {
                process_id: owner,
                resource_class,
                resource_id,
            } => {
                self.hostcall_process_owns_resource(process_id, owner, resource_class, resource_id)
            }
            HostcallRequest::ProcessCapability {
                process_id: target_process_id,
                capability,
                uri,
            } => self.hostcall_process_capability(process_id, target_process_id, capability, uri),
            HostcallRequest::ExpectedClaims { process_id: target } => {
                self.hostcall_expected_claims(process_id, target)
            }
            HostcallRequest::TimeNow => self.hostcall_time_now(process_id),
            HostcallRequest::TimeMonotonic => self.hostcall_time_monotonic(process_id),
            HostcallRequest::Sleep { millis } => self.hostcall_sleep(process_id, millis),
            HostcallRequest::WaitRegister {
                region_id,
                generation,
            } => self.hostcall_wait_register(process_id, region_id, generation),
            HostcallRequest::GenerationAdvance {
                region_id,
                generation,
            } => self.hostcall_generation_advance(process_id, region_id, generation),
        }
    }
}

#[cfg(test)]
mod tests;
