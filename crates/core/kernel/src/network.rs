use std::collections::HashMap;
use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use parking_lot::Mutex;
use selium_abi::SharedResourceId;

#[derive(Clone)]
pub struct NetworkState {
    pub(crate) inner: Arc<NetworkStateInner>,
}

pub(crate) struct NetworkStateInner {
    pub(crate) tcp_listeners: Mutex<HashMap<u64, TcpListenerState>>,
    pub(crate) tcp_streams: Mutex<HashMap<SharedResourceId, TcpStreamState>>,
    pub(crate) udp_sockets: Mutex<HashMap<SharedResourceId, UdpSocketState>>,
}

impl NetworkState {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(NetworkStateInner {
                tcp_listeners: Mutex::new(HashMap::new()),
                tcp_streams: Mutex::new(HashMap::new()),
                udp_sockets: Mutex::new(HashMap::new()),
            }),
        }
    }

    pub fn insert_tcp_listener(&self, local_id: u64, state: TcpListenerState) {
        self.inner.tcp_listeners.lock().insert(local_id, state);
    }

    pub fn insert_tcp_stream(&self, shared_id: SharedResourceId, state: TcpStreamState) {
        self.inner.tcp_streams.lock().insert(shared_id, state);
    }

    pub fn insert_udp_socket(&self, shared_id: SharedResourceId, state: UdpSocketState) {
        self.inner.udp_sockets.lock().insert(shared_id, state);
    }

    pub fn remove_tcp_listener(&self, local_id: u64) -> Option<TcpListenerState> {
        self.inner.tcp_listeners.lock().remove(&local_id)
    }

    pub fn remove_tcp_stream(&self, shared_id: u64) -> Option<TcpStreamState> {
        self.inner.tcp_streams.lock().remove(&shared_id)
    }

    pub fn remove_udp_socket(&self, shared_id: u64) -> Option<UdpSocketState> {
        self.inner.udp_sockets.lock().remove(&shared_id)
    }

    /// Returns the local addresses of all active TCP listeners.
    pub fn tcp_listener_addrs(&self) -> Vec<std::net::SocketAddr> {
        self.inner
            .tcp_listeners
            .lock()
            .values()
            .filter_map(|state| state._listener.local_addr().ok())
            .collect()
    }
}

pub struct TcpListenerState {
    pub shared_id: SharedResourceId,
    pub running: Arc<AtomicBool>,
    pub _listener: TcpListener,
}

pub struct TcpStreamState {
    pub running: Arc<AtomicBool>,
}

pub struct UdpSocketState {
    pub running: Arc<AtomicBool>,
}
