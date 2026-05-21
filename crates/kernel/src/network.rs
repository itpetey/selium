use std::sync::{Arc, atomic::Ordering};

use selium_abi::{
    NetworkListenerDescriptor, NetworkSessionDescriptor, NetworkStreamDescriptor, SharedResourceId,
};
use tokio::{
    sync::Notify,
    time::{Duration, timeout},
};

use crate::{
    Error, Result,
    state::{
        Kernel, ListenerState, RequestExchangeData, RequestExchangeState, SessionState, StreamState,
    },
};

impl Kernel {
    /// Opens a network listener descriptor for an address.
    pub fn listen(&self, address: impl Into<String>) -> NetworkListenerDescriptor {
        let local_id = self.next_local_id();
        let shared_id = self.next_shared_id();
        let address = address.into();
        let mut listeners_by_shared = self.inner.listeners_by_shared.lock();
        let mut local_listeners = self.inner.local_listeners.lock();
        listeners_by_shared.insert(shared_id, ListenerState);
        local_listeners.insert(local_id, shared_id);
        NetworkListenerDescriptor {
            local_id,
            shared_id,
            address,
        }
    }

    /// Opens a network session descriptor for an authority.
    pub fn connect(&self, authority: impl Into<String>) -> NetworkSessionDescriptor {
        let local_id = self.next_local_id();
        let shared_id = self.next_shared_id();
        let authority = authority.into();
        let mut sessions_by_shared = self.inner.sessions_by_shared.lock();
        let mut local_sessions = self.inner.local_sessions.lock();
        sessions_by_shared.insert(
            shared_id,
            SessionState {
                authority: authority.clone(),
            },
        );
        local_sessions.insert(local_id, shared_id);
        NetworkSessionDescriptor {
            local_id,
            shared_id,
            authority,
        }
    }

    /// Opens a stream on a local network session.
    pub fn open_stream(&self, network_session_id: u64) -> Result<NetworkStreamDescriptor> {
        self.network_session_shared_id(network_session_id)?;
        let local_id = self.next_local_id();
        self.inner.streams.lock().insert(
            local_id,
            StreamState {
                network_session_id,
                chunks: std::collections::VecDeque::new(),
            },
        );
        Ok(NetworkStreamDescriptor {
            local_id,
            network_session_id,
        })
    }

    /// Queues a stream chunk for a local stream.
    pub fn send_stream_chunk(&self, stream_id: u64, bytes: Vec<u8>) -> Result<()> {
        let mut streams = self.inner.streams.lock();
        let stream = streams
            .get_mut(&stream_id)
            .ok_or_else(|| Error::NotFound(format!("stream {stream_id}")))?;
        stream.chunks.push_back(bytes);
        Ok(())
    }

    /// Receives the next queued stream chunk, if one exists.
    pub fn recv_stream_chunk(&self, stream_id: u64) -> Result<Option<Vec<u8>>> {
        let mut streams = self.inner.streams.lock();
        let stream = streams
            .get_mut(&stream_id)
            .ok_or_else(|| Error::NotFound(format!("stream {stream_id}")))?;
        Ok(stream.chunks.pop_front())
    }

    /// Creates a request exchange on a network session.
    pub fn send_request(
        &self,
        network_session_id: u64,
        method: impl Into<String>,
        path: impl Into<String>,
        request_body: Vec<u8>,
    ) -> Result<u64> {
        self.network_session_shared_id(network_session_id)?;
        let exchange_id = self.inner.next_exchange_id.fetch_add(1, Ordering::SeqCst) + 1;
        self.inner.request_exchanges.lock().insert(
            exchange_id,
            Arc::new(RequestExchangeState {
                data: parking_lot::Mutex::new(RequestExchangeData {
                    network_session_id,
                    method: method.into(),
                    path: path.into(),
                    request_body,
                    response_status: None,
                    response_body: None,
                }),
                notify: Notify::new(),
            }),
        );
        Ok(exchange_id)
    }

    /// Waits asynchronously for a request exchange response.
    pub async fn wait_request_response(
        &self,
        exchange_id: u64,
        timeout_ms: u64,
    ) -> Result<(u16, Vec<u8>)> {
        let exchange = self.request_exchange(exchange_id)?;
        loop {
            let notified = exchange.notify.notified();
            if let Some(response) = Self::response_from_exchange(&exchange) {
                self.inner.request_exchanges.lock().remove(&exchange_id);
                return Ok(response);
            }
            if timeout(Duration::from_millis(timeout_ms), notified)
                .await
                .is_err()
            {
                self.inner.request_exchanges.lock().remove(&exchange_id);
                return Err(Error::Timeout);
            }
            if let Some(response) = Self::response_from_exchange(&exchange) {
                self.inner.request_exchanges.lock().remove(&exchange_id);
                return Ok(response);
            }
        }
    }

    /// Completes a request exchange with a response.
    pub fn respond_request(&self, exchange_id: u64, status: u16, body: Vec<u8>) -> Result<()> {
        let exchange = self.request_exchange(exchange_id)?;
        let mut data = exchange.data.lock();
        if data.response_status.is_some() || data.response_body.is_some() {
            return Err(Error::AlreadyCompleted);
        }
        data.response_status = Some(status);
        data.response_body = Some(body);
        drop(data);
        exchange.notify.notify_waiters();
        Ok(())
    }

    /// Reads a completed request response without waiting.
    pub fn read_request_response(&self, exchange_id: u64) -> Result<Option<(u16, Vec<u8>)>> {
        let exchange = self.request_exchange(exchange_id)?;
        let response = Self::response_from_exchange(&exchange);
        if response.is_some() {
            self.inner.request_exchanges.lock().remove(&exchange_id);
        }
        Ok(response)
    }

    /// Returns the session id, method, path, and body for a request exchange.
    pub fn request_summary(&self, exchange_id: u64) -> Result<(u64, String, String, Vec<u8>)> {
        let exchange = self.request_exchange(exchange_id)?;
        let data = exchange.data.lock();
        Ok((
            data.network_session_id,
            data.method.clone(),
            data.path.clone(),
            data.request_body.clone(),
        ))
    }

    /// Closes a local network listener handle.
    pub fn close_listener(&self, local_id: u64) -> Result<()> {
        let mut listeners_by_shared = self.inner.listeners_by_shared.lock();
        let mut local_listeners = self.inner.local_listeners.lock();
        let shared_id = local_listeners
            .remove(&local_id)
            .ok_or_else(|| Error::NotFound(format!("listener {local_id}")))?;
        if !local_listeners.values().any(|id| *id == shared_id) {
            listeners_by_shared.remove(&shared_id);
        }
        Ok(())
    }

    /// Closes a local network session handle.
    pub fn close_session(&self, local_id: u64) -> Result<()> {
        let mut sessions_by_shared = self.inner.sessions_by_shared.lock();
        let mut local_sessions = self.inner.local_sessions.lock();
        let shared_id = local_sessions
            .remove(&local_id)
            .ok_or_else(|| Error::NotFound(format!("session {local_id}")))?;
        if !local_sessions.values().any(|id| *id == shared_id) {
            sessions_by_shared.remove(&shared_id);
        }
        Ok(())
    }

    /// Closes a local network stream handle.
    pub fn close_stream(&self, local_id: u64) -> Result<()> {
        self.inner
            .streams
            .lock()
            .remove(&local_id)
            .map(|_| ())
            .ok_or_else(|| Error::NotFound(format!("stream {local_id}")))
    }

    /// Returns the local network session id associated with a stream.
    pub fn stream_network_session_id(&self, stream_id: u64) -> Result<u64> {
        self.inner
            .streams
            .lock()
            .get(&stream_id)
            .map(|stream| stream.network_session_id)
            .ok_or_else(|| Error::NotFound(format!("stream {stream_id}")))
    }

    /// Returns the shared session id associated with a local session handle.
    pub fn network_session_shared_id_public(&self, local_id: u64) -> Result<SharedResourceId> {
        self.network_session_shared_id(local_id)
    }

    /// Returns the shared listener id associated with a local listener handle.
    pub fn listener_shared_id(&self, local_id: u64) -> Result<SharedResourceId> {
        self.inner
            .local_listeners
            .lock()
            .get(&local_id)
            .copied()
            .ok_or_else(|| Error::NotFound(format!("listener {local_id}")))
    }

    pub(crate) fn network_session_shared_id(&self, local_id: u64) -> Result<SharedResourceId> {
        let shared_id = self
            .inner
            .local_sessions
            .lock()
            .get(&local_id)
            .copied()
            .ok_or_else(|| Error::NotFound(format!("session {local_id}")))?;
        let sessions = self.inner.sessions_by_shared.lock();
        let session = sessions
            .get(&shared_id)
            .ok_or_else(|| Error::NotFound(format!("session {shared_id}")))?;
        let _ = &session.authority;
        Ok(shared_id)
    }

    pub(crate) fn request_exchange(&self, exchange_id: u64) -> Result<Arc<RequestExchangeState>> {
        self.inner
            .request_exchanges
            .lock()
            .get(&exchange_id)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("exchange {exchange_id}")))
    }

    fn response_from_exchange(exchange: &RequestExchangeState) -> Option<(u16, Vec<u8>)> {
        let data = exchange.data.lock();
        data.response_status.zip(data.response_body.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn request_response_waits_for_reply() {
        let kernel = Kernel::default();
        let session = kernel.connect("selium.test");
        let exchange = kernel
            .send_request(session.local_id, "GET", "/health", b"ping".to_vec())
            .expect("send request");

        let responder = {
            let kernel = kernel.clone();
            tokio::spawn(async move {
                kernel
                    .respond_request(exchange, 200, b"pong".to_vec())
                    .expect("respond request");
            })
        };

        let response = kernel
            .wait_request_response(exchange, 1_000)
            .await
            .expect("wait response");
        responder.await.expect("join responder");
        assert_eq!(response, (200, b"pong".to_vec()));
    }

    #[test]
    fn request_response_is_single_assignment() {
        let kernel = Kernel::default();
        let session = kernel.connect("selium.test");
        let exchange = kernel
            .send_request(session.local_id, "GET", "/health", b"ping".to_vec())
            .expect("send request");

        kernel
            .respond_request(exchange, 200, b"pong".to_vec())
            .expect("first response");
        assert!(matches!(
            kernel.respond_request(exchange, 201, b"other".to_vec()),
            Err(Error::AlreadyCompleted)
        ));
    }
}
