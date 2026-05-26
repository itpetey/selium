use std::{future::Future, pin::Pin, task::Poll};

use selium_abi::{
    decode_rkyv, encode_rkyv, unpack_hostcall_status, CompletionState, HostcallEnvelope,
    HostcallOutput, HostcallRequest, OperationId,
};

use crate::{
    async_runtime::current_task_id,
    error::abi_error_to_guest_error,
    platform::{selium_hostcall_create, selium_hostcall_drop, selium_hostcall_poll},
    GuestError, Result,
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

fn poll_operation(operation_id: OperationId) -> Result<Option<HostcallOutput>> {
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
