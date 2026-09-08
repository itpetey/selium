use std::{future::Future, pin::Pin, task::Poll};

use selium_abi::{
    CompletionState, HostcallEnvelope, HostcallOutput, HostcallRequest, OperationId, TaskId,
    decode_rkyv, encode_rkyv, unpack_hostcall_status,
};

use crate::{
    GuestError, Result,
    async_runtime::current_task_id,
    error::abi_error_to_guest_error,
    platform::{selium_hostcall_create, selium_hostcall_drop, selium_hostcall_poll},
};

pub(crate) struct HostcallFuture {
    request: Option<HostcallRequest>,
    operation_id: Option<OperationId>,
}

impl Future for HostcallFuture {
    type Output = Result<HostcallOutput>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        if self.operation_id.is_none() {
            let Some(task_id) = current_task_id() else {
                return Poll::Ready(Err(GuestError::Host(
                    "async hostcall polled outside Selium guest reactor".to_string(),
                )));
            };
            let request = self
                .request
                .take()
                .ok_or_else(|| GuestError::Host("hostcall request already consumed".to_string()))?;
            let envelope = HostcallEnvelope {
                request,
                task_id: Some(task_id),
            };
            let encoded = match encode_rkyv(&envelope) {
                Ok(encoded) => encoded,
                Err(error) => return Poll::Ready(Err(error.into())),
            };
            // SAFETY: `encoded` is a valid byte buffer; the host validates the request.
            let create_status = unsafe { selium_hostcall_create(encoded.as_ptr(), encoded.len()) };
            let (status, operation_id) = unpack_hostcall_status(create_status);
            if status == selium_abi::HOSTCALL_STATUS_FAILED {
                return Poll::Ready(Err(GuestError::Host("hostcall create failed".to_string())));
            }
            self.operation_id = Some(operation_id as OperationId);
        }

        let operation_id = self.operation_id.expect("operation id set above");
        match poll_operation(operation_id) {
            Ok(Some(output)) => {
                // SAFETY: `operation_id` is valid and was returned by `selium_hostcall_create`.
                unsafe { selium_hostcall_drop(operation_id) };
                self.operation_id = None;
                Poll::Ready(Ok(output))
            }
            Ok(None) => Poll::Pending,
            Err(error) => {
                // SAFETY: `operation_id` is valid and was returned by `selium_hostcall_create`.
                unsafe { selium_hostcall_drop(operation_id) };
                self.operation_id = None;
                Poll::Ready(Err(error))
            }
        }
    }
}

impl Drop for HostcallFuture {
    fn drop(&mut self) {
        if let Some(operation_id) = self.operation_id {
            // SAFETY: `operation_id` was returned by `selium_hostcall_create` and is still valid.
            unsafe { selium_hostcall_drop(operation_id) };
        }
    }
}

/// Returns whether `process_id` holds `capability`.
///
/// Restricted to the discovery system guest: the runtime accepts this
/// hostcall only from the process booted under the `"discovery"` name, so the
/// discovery service can gate root-registration requests against the caller's
/// grants without any other guest probing grant state.
pub fn process_capability(
    process_id: selium_abi::ProcessId,
    capability: selium_abi::Capability,
) -> Result<bool> {
    match hostcall_ready(HostcallRequest::ProcessCapability {
        process_id,
        capability,
    })? {
        HostcallOutput::U64(value) => Ok(value != 0),
        other => Err(GuestError::Host(format!(
            "unexpected hostcall output for ProcessCapability: {other:?}"
        ))),
    }
}

/// The tenant identity assigned to another process, if any.
///
/// The discovery service uses this to scope resolution to the calling
/// process's own tenant.
pub fn process_tenant(process_id: selium_abi::ProcessId) -> Result<Option<String>> {
    match hostcall_ready(HostcallRequest::ProcessTenant { process_id })? {
        HostcallOutput::Tenant(tenant) => Ok(tenant),
        other => Err(GuestError::Host(format!(
            "unexpected hostcall output for ProcessTenant: {other:?}"
        ))),
    }
}

/// Fills a buffer with cryptographically secure random bytes from the host.
///
/// Used by TLS-terminating guests on wasm32 where no OS entropy source is
/// available. The host enforces a maximum length (currently 4096 bytes);
/// legitimate TLS operations request well under that limit.
pub fn random_bytes(len: u32) -> Result<Vec<u8>> {
    match hostcall_ready(HostcallRequest::RandomBytes { len })? {
        HostcallOutput::RandomBytes(bytes) => Ok(bytes),
        other => Err(GuestError::Host(format!(
            "unexpected hostcall output for RandomBytes: {other:?}"
        ))),
    }
}

/// Records, on behalf of the discovery service, that `process_id` registered
/// the route `uri`. The runtime accepts this hostcall only from the discovery
/// system guest and uses the record to gate the readiness of role-declared
/// system guests on discoverable self-registration.
pub fn record_registration(process_id: selium_abi::ProcessId, uri: &str) -> Result<()> {
    hostcall_ready(HostcallRequest::RecordRegistration {
        process_id,
        uri: uri.to_string(),
    })
    .map(|_| ())
}

/// Records, on behalf of the discovery service, that a discovery resolve
/// performed by `client_process_id` returned `shared_id`. The runtime
/// accepts this hostcall only from the discovery system guest; the recorded
/// id gives the resolving client an authorisation basis for cross-process
/// `HostQueueAttach`.
pub fn record_resolved_queue_for(
    client_process_id: selium_abi::ProcessId,
    shared_id: selium_abi::SharedResourceId,
) -> Result<()> {
    hostcall_ready(HostcallRequest::RecordResolvedQueueFor {
        client_process_id,
        shared_id,
    })
    .map(|_| ())
}

/// Resolves the bootstrap-registered protocol handler for `scheme`
/// (e.g. `sel-quic`) to its process id.
///
/// Handler registrations are Tier-1 (runtime-published at bootstrap), so the
/// result cannot be forged by guests. Serve-side guests use it to pin the
/// process legitimately allowed to deliver handoffs (see
/// [`ResourceListener::expect_sender`](crate::ResourceListener::expect_sender)).
pub fn resolve_protocol_handler(scheme: &str) -> Result<Option<selium_abi::ProcessId>> {
    match hostcall_ready(HostcallRequest::ResolveProtocolHandler {
        scheme: scheme.to_string(),
    })? {
        HostcallOutput::U64(process_id) => Ok(Some(process_id)),
        HostcallOutput::Empty => Ok(None),
        other => Err(GuestError::Host(format!(
            "unexpected hostcall output for ResolveProtocolHandler: {other:?}"
        ))),
    }
}

/// The calling process's own identity: process id and tenant scope.
///
/// System guests use this to verify handoff identities against their own
/// tenant (e.g. the bridge-server refuses clients whose authenticated
/// tenant scope differs from its own).
pub fn self_info() -> Result<(selium_abi::ProcessId, Option<String>)> {
    match hostcall_ready(HostcallRequest::SelfInfo)? {
        HostcallOutput::SelfInfo { process_id, tenant } => Ok((process_id, tenant)),
        other => Err(GuestError::Host(format!(
            "unexpected hostcall output for SelfInfo: {other:?}"
        ))),
    }
}

pub(crate) fn hostcall_async(request: HostcallRequest) -> HostcallFuture {
    HostcallFuture {
        request: Some(request),
        operation_id: None,
    }
}

pub(crate) fn hostcall_ready(request: HostcallRequest) -> Result<HostcallOutput> {
    let envelope = HostcallEnvelope {
        request,
        task_id: None,
    };
    let request = encode_rkyv(&envelope)?;
    // SAFETY: `request` is a valid byte buffer; the host validates the contents.
    let create_status = unsafe { selium_hostcall_create(request.as_ptr(), request.len()) };
    let (status, operation_id) = unpack_hostcall_status(create_status);
    if status == selium_abi::HOSTCALL_STATUS_FAILED {
        return Err(GuestError::Host("hostcall create failed".to_string()));
    }

    match poll_operation(operation_id as OperationId) {
        Ok(Some(output)) => {
            // SAFETY: `operation_id` was returned by `selium_hostcall_create` and is still valid.
            unsafe { selium_hostcall_drop(operation_id as OperationId) };
            Ok(output)
        }
        Ok(None) => {
            // SAFETY: `operation_id` was returned by `selium_hostcall_create` and is still valid.
            unsafe { selium_hostcall_drop(operation_id as OperationId) };
            Err(GuestError::Host(
                "hostcall returned pending; await the async API instead".to_string(),
            ))
        }
        Err(error) => {
            // SAFETY: `operation_id` was returned by `selium_hostcall_create` and is still valid.
            unsafe { selium_hostcall_drop(operation_id as OperationId) };
            Err(error)
        }
    }
}

/// Synchronous hostcall that includes a task_id in the envelope.
/// Used for WaitRegister where the host needs to know which guest
/// task to wake on a generation advance.
pub(crate) fn hostcall_ready_with_task(
    request: HostcallRequest,
    task_id: TaskId,
) -> Result<HostcallOutput> {
    let envelope = HostcallEnvelope {
        request,
        task_id: Some(task_id),
    };
    let encoded = encode_rkyv(&envelope)?;
    // SAFETY: `encoded` is a valid byte buffer; the host validates the contents.
    let create_status = unsafe { selium_hostcall_create(encoded.as_ptr(), encoded.len()) };
    let (status, operation_id) = unpack_hostcall_status(create_status);
    if status == selium_abi::HOSTCALL_STATUS_FAILED {
        return Err(GuestError::Host("hostcall create failed".to_string()));
    }

    match poll_operation(operation_id as OperationId) {
        Ok(Some(output)) => {
            // SAFETY: `operation_id` was returned by `selium_hostcall_create` and is still valid.
            unsafe { selium_hostcall_drop(operation_id as OperationId) };
            Ok(output)
        }
        Ok(None) => {
            // SAFETY: `operation_id` was returned by `selium_hostcall_create` and is still valid.
            unsafe { selium_hostcall_drop(operation_id as OperationId) };
            Err(GuestError::Host(
                "hostcall returned pending; await the async API instead".to_string(),
            ))
        }
        Err(error) => {
            // SAFETY: `operation_id` was returned by `selium_hostcall_create` and is still valid.
            unsafe { selium_hostcall_drop(operation_id as OperationId) };
            Err(error)
        }
    }
}

pub(crate) fn poll_operation(operation_id: OperationId) -> Result<Option<HostcallOutput>> {
    let mut output = vec![0_u8; 4096];
    loop {
        // SAFETY: `output` is a valid mutable buffer; `operation_id` was returned by
        // `selium_hostcall_create`.
        let poll_status =
            unsafe { selium_hostcall_poll(operation_id, output.as_mut_ptr(), output.len()) };
        let (status, len) = unpack_hostcall_status(poll_status);
        if status == selium_abi::HOSTCALL_STATUS_OUTPUT_TOO_SMALL {
            output.resize(len as usize, 0);
            continue;
        }
        let state: CompletionState = decode_rkyv(
            output
                .get(..len as usize)
                .ok_or_else(|| GuestError::Host("invalid hostcall output length".to_string()))?,
        )?;
        return match state {
            CompletionState::Ready(output) => Ok(Some(output)),
            CompletionState::Pending { .. } => Ok(None),
            CompletionState::Failed(error) => Err(abi_error_to_guest_error(error)),
        };
    }
}
