use std::{
    net::SocketAddr,
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
};

use anyhow::{Context, Result, anyhow};
use quinn::{Connection, Endpoint};
use rkyv::{
    Archive,
    api::high::{HighDeserializer, HighValidator},
};
use selium_abi::{RkyvEncode, decode_rkyv};
use selium_control_plane_protocol::{
    GuestLogEvent, Method, SubscribeGuestLogsRequest, SubscribeGuestLogsResponse, decode_envelope,
    decode_error, decode_payload, encode_request, is_error,
};
use selium_runtime_support::{
    ClientTlsPaths, build_quic_client_endpoint, parse_socket_addr, read_framed, write_framed,
};
use tokio::sync::Mutex;
use tokio::time::{Duration, timeout};

use crate::config::DaemonConnectionArgs;

const DAEMON_CONNECT_TIMEOUT: Duration = Duration::from_millis(500);
const DAEMON_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const DAEMON_CONTROL_QUERY_TIMEOUT: Duration = Duration::from_secs(30);
const DAEMON_START_TIMEOUT: Duration = Duration::from_secs(60);

pub(crate) struct DaemonQuicClient {
    endpoint: Endpoint,
    addr: SocketAddr,
    server_name: String,
    connection: Mutex<Option<Connection>>,
    request_id: AtomicU64,
}

#[allow(dead_code)]
pub(crate) struct GuestLogSubscription {
    _connection: Connection,
    recv: quinn::RecvStream,
    pub(crate) response: SubscribeGuestLogsResponse,
}

impl DaemonQuicClient {
    pub(crate) fn from_args(args: &DaemonConnectionArgs) -> Result<Self> {
        Self::new_from_material(
            parse_daemon_addr(&args.daemon_addr)?,
            args.daemon_server_name.clone(),
            &args.ca_cert,
            &args.client_cert,
            &args.client_key,
        )
    }

    pub(crate) fn new_from_material(
        addr: SocketAddr,
        server_name: String,
        ca_cert: &Path,
        client_cert: &Path,
        client_key: &Path,
    ) -> Result<Self> {
        let bind = if cfg!(target_family = "unix") {
            "0.0.0.0:0"
        } else {
            "127.0.0.1:0"
        }
        .parse::<SocketAddr>()?;
        let endpoint = build_quic_client_endpoint(
            bind,
            ClientTlsPaths {
                ca_cert,
                client_cert: Some(client_cert),
                client_key: Some(client_key),
            },
        )?;

        Ok(Self {
            endpoint,
            addr,
            server_name,
            connection: Mutex::new(None),
            request_id: AtomicU64::new(1),
        })
    }

    pub(crate) async fn request<Req, Resp>(&self, method: Method, payload: &Req) -> Result<Resp>
    where
        Req: RkyvEncode,
        Resp: Archive + Sized,
        for<'a> Resp::Archived: rkyv::Deserialize<Resp, HighDeserializer<rkyv::rancor::Error>>
            + rkyv::bytecheck::CheckBytes<HighValidator<'a, rkyv::rancor::Error>>,
    {
        let connection = self.connection().await?;
        let request_timeout = request_timeout(method);
        let (mut send, mut recv) = timeout(request_timeout, connection.open_bi())
            .await
            .map_err(|_| anyhow!("timed out"))
            .context("open QUIC stream")??;
        let request_id = self.request_id.fetch_add(1, Ordering::Relaxed);
        let frame = encode_request(method, request_id, payload).context("encode request")?;

        timeout(request_timeout, write_framed(&mut send, &frame))
            .await
            .map_err(|_| anyhow!("timed out"))
            .context("write request")??;
        let _ = send.finish();

        let frame = timeout(request_timeout, read_framed(&mut recv))
            .await
            .map_err(|_| anyhow!("timed out"))
            .context("read response")??;
        let envelope = decode_envelope(&frame).context("decode response envelope")?;
        if envelope.method != method || envelope.request_id != request_id {
            return Err(anyhow!("daemon response mismatch"));
        }

        if is_error(&envelope) {
            let error = decode_error(&envelope).context("decode daemon error")?;
            return Err(anyhow!("daemon error {}: {}", error.code, error.message));
        }

        decode_payload::<Resp>(&envelope).context("decode daemon payload")
    }

    #[allow(dead_code)]
    pub(crate) async fn subscribe_guest_logs(
        &self,
        payload: &SubscribeGuestLogsRequest,
    ) -> Result<GuestLogSubscription> {
        let connection = self.connection().await?;
        let request_id = self.request_id.fetch_add(1, Ordering::Relaxed);
        let frame = encode_request(Method::SubscribeGuestLogs, request_id, payload)
            .context("encode guest log subscription request")?;
        let (mut send, mut recv) = timeout(DAEMON_REQUEST_TIMEOUT, connection.open_bi())
            .await
            .map_err(|_| anyhow!("timed out"))
            .context("open guest log subscription stream")??;

        timeout(DAEMON_REQUEST_TIMEOUT, write_framed(&mut send, &frame))
            .await
            .map_err(|_| anyhow!("timed out"))
            .context("write guest log subscription request")??;
        let _ = send.finish();

        let response = timeout(DAEMON_REQUEST_TIMEOUT, read_framed(&mut recv))
            .await
            .map_err(|_| anyhow!("timed out"))
            .context("read guest log subscription response")??;
        let envelope = decode_envelope(&response).context("decode subscription response")?;
        if envelope.method != Method::SubscribeGuestLogs || envelope.request_id != request_id {
            return Err(anyhow!("daemon response mismatch"));
        }
        if is_error(&envelope) {
            let error = decode_error(&envelope).context("decode daemon error")?;
            return Err(anyhow!("daemon error {}: {}", error.code, error.message));
        }

        Ok(GuestLogSubscription {
            _connection: connection,
            recv,
            response: decode_payload(&envelope).context("decode subscription payload")?,
        })
    }

    pub(crate) async fn reset_connection(&self) {
        let mut guard = self.connection.lock().await;
        if let Some(connection) = guard.take() {
            connection.close(0u32.into(), b"reset");
        }
    }

    async fn connection(&self) -> Result<Connection> {
        {
            let guard = self.connection.lock().await;
            if let Some(connection) = guard.as_ref()
                && connection.close_reason().is_none()
            {
                return Ok(connection.clone());
            }
        }

        let connecting = self
            .endpoint
            .connect(self.addr, &self.server_name)
            .context("connect daemon")?;
        let connection = timeout(DAEMON_CONNECT_TIMEOUT, connecting)
            .await
            .map_err(|_| anyhow!("timed out"))
            .context("await daemon connect")??;
        let mut guard = self.connection.lock().await;
        *guard = Some(connection.clone());
        Ok(connection)
    }
}

impl GuestLogSubscription {
    #[allow(dead_code)]
    pub(crate) async fn next_event(&mut self) -> Result<Option<GuestLogEvent>> {
        match read_framed(&mut self.recv).await {
            Ok(frame) => decode_rkyv(&frame)
                .context("decode guest log event")
                .map(Some),
            Err(err) => {
                if err.chain().any(|cause| {
                    cause
                        .downcast_ref::<std::io::Error>()
                        .is_some_and(|io| io.kind() == std::io::ErrorKind::UnexpectedEof)
                }) {
                    Ok(None)
                } else {
                    Err(err).context("read guest log event")
                }
            }
        }
    }
}

fn request_timeout(method: Method) -> Duration {
    match method {
        Method::StartInstance => DAEMON_START_TIMEOUT,
        Method::ControlQuery => DAEMON_CONTROL_QUERY_TIMEOUT,
        _ => DAEMON_REQUEST_TIMEOUT,
    }
}

pub(crate) fn parse_daemon_addr(raw: &str) -> Result<SocketAddr> {
    parse_socket_addr(raw).map_err(|err| anyhow!("parse daemon addr: {err:#}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_daemon_addr_accepts_scheme() {
        let addr = parse_daemon_addr("http://127.0.0.1:7100").expect("addr");
        assert_eq!(addr.port(), 7100);
    }

    #[test]
    fn parse_daemon_addr_accepts_raw_socket() {
        let addr = parse_daemon_addr("127.0.0.1:7100").expect("addr");
        assert_eq!(addr.to_string(), "127.0.0.1:7100");
    }
}
