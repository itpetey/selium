use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

use selium_service::{ControlRequest, ControlResponse};
use selium_shm::rpc::{self, OwnedRpcClient};

use crate::{GuestError, Result, async_runtime::yield_now};

/// Reply ring capacity for a control-plane session; sized for catalogue reads.
const RPC_REP_CAPACITY: u64 = 64 * 1024;
/// Request ring capacity for a control-plane session.
const RPC_REQ_CAPACITY: u64 = 64 * 1024;
/// The root-namespace route the control-plane service serves on.
pub const CONTROL_ROUTE: &str = "sel:///control";

/// A typed client of the `selium-control-plane` service.
///
/// The client owns one shared-memory RPC session and is cheap to clone. The
/// resource catalogue lives behind this surface: registration and lookup are
/// RPCs scoped to the session's tenant, so a guest never sees host-side naming
/// metadata.
#[derive(Clone)]
pub struct ControlClient {
    inner: Arc<ControlClientInner>,
}

struct ControlClientInner {
    /// Cooperative gate: the session is taken out for the duration of a
    /// request, so concurrent callers cannot interleave frames on one session.
    gate: AtomicBool,
    session: Mutex<Option<OwnedRpcClient<ControlRequest, ControlResponse>>>,
}

impl ControlClient {
    /// Wraps an established RPC session to the control-plane service.
    pub fn from_session(session: OwnedRpcClient<ControlRequest, ControlResponse>) -> Self {
        Self {
            inner: Arc::new(ControlClientInner {
                gate: AtomicBool::new(false),
                session: Mutex::new(Some(session)),
            }),
        }
    }

    /// Sends one request and awaits its reply.
    ///
    /// A control-plane-reported failure is returned as a [`GuestError`]; the
    /// session survives a `ControlResponse::Error`.
    pub async fn request(&self, request: ControlRequest) -> Result<ControlResponse> {
        self.acquire().await;
        let session = self
            .inner
            .session
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        let Some(mut session) = session else {
            self.inner.gate.store(false, Ordering::Release);
            return Err(GuestError::Host(
                "control-plane session unavailable".to_string(),
            ));
        };
        let reply = session.request(request).await;
        // Restore the session before opening the gate again.
        *self
            .inner
            .session
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(session);
        self.inner.gate.store(false, Ordering::Release);

        match reply.map_err(|error| GuestError::Host(format!("control request: {error}")))? {
            ControlResponse::Error { step, context } => {
                Err(GuestError::Host(format!("{step}: {context}")))
            }
            other => Ok(other),
        }
    }

    async fn acquire(&self) {
        while self.inner.gate.swap(true, Ordering::AcqRel) {
            yield_now().await;
        }
    }
}

/// Establishes a control-plane session over an attached rendezvous.
///
/// Used by [`crate::Context`], which resolves the control route itself.
pub async fn connect<R>(rendezvous: R) -> Result<ControlClient>
where
    R: selium_wire::rpc::Rendezvous,
{
    let session = rpc::connect(rendezvous, RPC_REQ_CAPACITY, RPC_REP_CAPACITY)
        .await
        .map_err(|error| GuestError::Host(format!("control connect: {error}")))?;
    Ok(ControlClient::from_session(session))
}
