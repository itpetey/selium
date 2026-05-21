use selium_abi::{
    HostcallOutput, HostcallRequest, NetworkListenerDescriptor, NetworkSessionDescriptor,
    NetworkStreamDescriptor,
};

use crate::{
    GuestError, Result,
    hostcall::{hostcall_async, hostcall_ready},
};

/// Guest handle for a network listener.
#[derive(Clone, Debug)]
pub struct NetworkListener {
    descriptor: NetworkListenerDescriptor,
}

/// Guest handle for a network session.
#[derive(Clone, Debug)]
pub struct NetworkSession {
    descriptor: NetworkSessionDescriptor,
}

/// Guest handle for a stream opened on a network session.
#[derive(Clone, Copy, Debug)]
pub struct NetworkStream {
    descriptor: NetworkStreamDescriptor,
}

/// Guest handle for an in-flight request exchange.
#[derive(Clone, Copy, Debug)]
pub struct RequestExchange {
    local_id: u64,
}

impl NetworkListener {
    /// Opens a network listener bound to the supplied address.
    pub fn listen(address: impl Into<String>) -> Result<Self> {
        match hostcall_ready(HostcallRequest::NetworkListen {
            address: address.into(),
        })? {
            HostcallOutput::Listener(descriptor) => Ok(Self { descriptor }),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }

    /// Returns the listener descriptor.
    pub fn descriptor(&self) -> &NetworkListenerDescriptor {
        &self.descriptor
    }

    /// Closes the listener handle.
    pub fn close(self) -> Result<()> {
        match hostcall_ready(HostcallRequest::NetworkListenerClose {
            local_id: self.descriptor.local_id,
        })? {
            HostcallOutput::Empty => Ok(()),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }
}

impl NetworkSession {
    /// Opens a network session to an authority or endpoint.
    pub fn connect(authority: impl Into<String>) -> Result<Self> {
        match hostcall_ready(HostcallRequest::NetworkConnect {
            authority: authority.into(),
        })? {
            HostcallOutput::Session(descriptor) => Ok(Self { descriptor }),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }

    /// Returns the session descriptor.
    pub fn descriptor(&self) -> &NetworkSessionDescriptor {
        &self.descriptor
    }

    /// Opens a stream on this session.
    pub fn open_stream(&self) -> Result<NetworkStream> {
        match hostcall_ready(HostcallRequest::NetworkOpenStream {
            network_session_id: self.descriptor.local_id,
        })? {
            HostcallOutput::Stream(descriptor) => Ok(NetworkStream { descriptor }),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }

    /// Sends a request on this session and returns its exchange handle.
    pub fn send_request(
        &self,
        method: impl Into<String>,
        path: impl Into<String>,
        body: Vec<u8>,
    ) -> Result<RequestExchange> {
        match hostcall_ready(HostcallRequest::NetworkSendRequest {
            network_session_id: self.descriptor.local_id,
            method: method.into(),
            path: path.into(),
            body,
        })? {
            HostcallOutput::LocalId(local_id) => Ok(RequestExchange { local_id }),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }

    /// Closes the session handle.
    pub fn close(self) -> Result<()> {
        match hostcall_ready(HostcallRequest::NetworkSessionClose {
            local_id: self.descriptor.local_id,
        })? {
            HostcallOutput::Empty => Ok(()),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }
}

impl NetworkStream {
    /// Returns the stream descriptor.
    pub fn descriptor(&self) -> NetworkStreamDescriptor {
        self.descriptor
    }

    /// Sends bytes on the stream.
    pub fn send(&self, bytes: Vec<u8>) -> Result<()> {
        match hostcall_ready(HostcallRequest::NetworkStreamSend {
            local_id: self.descriptor.local_id,
            bytes,
        })? {
            HostcallOutput::Empty => Ok(()),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }

    /// Receives the next available stream chunk, if any.
    pub fn recv(&self) -> Result<Option<Vec<u8>>> {
        match hostcall_ready(HostcallRequest::NetworkStreamRecv {
            local_id: self.descriptor.local_id,
        })? {
            HostcallOutput::Bytes(bytes) => Ok(Some(bytes)),
            HostcallOutput::Empty => Ok(None),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }

    /// Closes the stream handle.
    pub fn close(self) -> Result<()> {
        match hostcall_ready(HostcallRequest::NetworkStreamClose {
            local_id: self.descriptor.local_id,
        })? {
            HostcallOutput::Empty => Ok(()),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }
}

impl RequestExchange {
    /// Returns the local request exchange id.
    pub fn local_id(&self) -> u64 {
        self.local_id
    }

    /// Waits for a response or timeout for this request exchange.
    pub async fn wait_response(self, timeout_ms: u64) -> Result<(u16, Vec<u8>)> {
        match hostcall_async(HostcallRequest::NetworkWaitRequestResponse {
            exchange_id: self.local_id,
            timeout_ms,
        })
        .await?
        {
            HostcallOutput::Response { status, body } => Ok((status, body)),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }
}
