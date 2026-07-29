//! Reference QUIC-to-shared-memory bridge guest.
//!
//! This guest terminates a single QUIC connection from an external client and
//! transparently relays `selium-wire` frames between QUIC streams and a
//! shared-memory ring channel.
//!
//! # Entrypoint arguments
//!
//! - `udp_shared_id`: shared id of the UDP socket shared region granted to
//!   this bridge.
//! - `channel_shared_id`: shared id of the ring channel to bridge.
//!
//! The local QUIC address is hard-coded to `0.0.0.0:4433` for this reference
//! implementation; the acceptor guest/runtime is expected to bind the UDP
//! socket to that address.
//!
//! # Security warning
//!
//! The embedded certificate is a hard-coded, self-signed reference certificate.
//! Real deployments must provision a proper certificate/key pair via the
//! acceptor guest or runtime.

#[cfg(target_arch = "wasm32")]
mod wasm {
    use std::{net::SocketAddr, sync::Arc};

    use selium_guest::{QuinnUdpSocket, SeliumQuinnRuntime, entrypoint, info, warn};
    use selium_quic::QuicTransport;
    use selium_shm::{Channel, transport::ShmTransport};
    use selium_wire::{
        error::Error,
        framed::{FramedRead, FramedWrite},
    };

    /// Embedded self-signed certificate for the reference bridge.
    ///
    /// DO NOT USE IN PRODUCTION. Generate or inject a real certificate for
    /// deployed bridges.
    const BRIDGE_CERT_DER: &[u8] = include_bytes!("bridge_cert.der");

    /// Embedded private key for the reference bridge certificate.
    ///
    /// DO NOT USE IN PRODUCTION.
    const BRIDGE_KEY_DER: &[u8] = include_bytes!("bridge_key.der");

    /// Hard-coded local address for the reference bridge.
    const BRIDGE_LOCAL_ADDR: &str = "0.0.0.0:4433";

    #[entrypoint]
    async fn bridge_main(udp_shared_id: u64, channel_shared_id: u64) {
        drop(selium_guest::log::init());
        info!(guest = "selium-bridge-guest", "bridge booting");

        let local_addr: SocketAddr = BRIDGE_LOCAL_ADDR
            .parse()
            .expect("hard-coded bridge address is valid");

        let udp_socket = match selium_guest::UdpSocket::attach(udp_shared_id, local_addr) {
            Ok(socket) => socket,
            Err(error) => {
                warn!(%error, "failed to attach UDP socket region");
                return;
            }
        };

        let quinn_socket = QuinnUdpSocket::from(udp_socket);
        let server_config = match make_server_config() {
            Ok(config) => config,
            Err(error) => {
                warn!(%error, "failed to create QUIC server config");
                return;
            }
        };

        let endpoint = match quinn::Endpoint::new_with_abstract_socket(
            quinn::EndpointConfig::default(),
            Some(server_config),
            Box::new(quinn_socket),
            Arc::new(SeliumQuinnRuntime),
        ) {
            Ok(endpoint) => endpoint,
            Err(error) => {
                warn!(%error, "failed to create QUIC endpoint");
                return;
            }
        };

        info!(addr = %BRIDGE_LOCAL_ADDR, "bridge listening for QUIC connections");
        selium_guest::mark_ready();

        // Accept incoming QUIC connections forever. Each connection is handled in
        // a separate guest task; the bridge is scoped to one external user per
        // process, so we only expect one connection in practice.
        while let Some(incoming) = endpoint.accept().await {
            let connection = match incoming.await {
                Ok(connection) => connection,
                Err(error) => {
                    warn!(%error, "incoming QUIC connection failed");
                    continue;
                }
            };
            selium_guest::spawn(handle_connection(connection, channel_shared_id));
        }
    }

    /// Handles a single QUIC connection, accepting bidirectional streams and
    /// relaying each stream to the shared-memory channel.
    async fn handle_connection(connection: quinn::Connection, channel_shared_id: u64) {
        loop {
            let (send_stream, recv_stream) = match connection.accept_bi().await {
                Ok(streams) => streams,
                Err(error) => {
                    warn!(%error, "failed to accept QUIC bidirectional stream");
                    break;
                }
            };

            let channel = match Channel::attach(channel_shared_id) {
                Ok(channel) => channel,
                Err(error) => {
                    warn!(%error, shared_id = channel_shared_id, "failed to attach channel");
                    break;
                }
            };

            // Two transports on the same channel: one for reading, one for
            // writing. This lets us build separate FramedRead/FramedWrite handles
            // for each relay direction.
            let shm_read_transport = match ShmTransport::new(&channel, &channel) {
                Ok(transport) => transport,
                Err(error) => {
                    warn!(%error, "failed to create shm read transport");
                    break;
                }
            };
            let shm_write_transport = match ShmTransport::new(&channel, &channel) {
                Ok(transport) => transport,
                Err(error) => {
                    warn!(%error, "failed to create shm write transport");
                    break;
                }
            };

            let (quic_write, quic_read) = QuicTransport::new(send_stream, recv_stream).split();

            let quic_to_shm_read = FramedRead::new(quic_read);
            let quic_to_shm_write = FramedWrite::new(shm_write_transport);
            let shm_to_quic_read = FramedRead::new(shm_read_transport);
            let shm_to_quic_write = FramedWrite::new(quic_write);

            selium_guest::spawn(relay_quic_to_shm(quic_to_shm_read, quic_to_shm_write));
            selium_guest::spawn(relay_shm_to_quic(shm_to_quic_read, shm_to_quic_write));
        }
    }

    /// Relays frames from the QUIC stream to the shared-memory channel.
    ///
    /// Each frame is forwarded verbatim so that correlation tags, flags, and
    /// payload bytes are preserved end-to-end.
    async fn relay_quic_to_shm(
        mut quic_read: FramedRead<QuicTransport>,
        mut shm_write: FramedWrite<ShmTransport>,
    ) {
        loop {
            match quic_read.read_frame() {
                Ok((payload, tag)) => {
                    if let Err(error) = shm_write.write_frame(&payload, tag) {
                        warn!(%error, "failed to write frame to shm");
                        break;
                    }
                }
                Err(Error::BufferEmpty) => {
                    selium_guest::yield_now().await;
                }
                Err(error) => {
                    warn!(%error, "quic read failed");
                    break;
                }
            }
        }
    }

    /// Relays frames from the shared-memory channel to the QUIC stream.
    async fn relay_shm_to_quic(
        mut shm_read: FramedRead<ShmTransport>,
        mut quic_write: FramedWrite<QuicTransport>,
    ) {
        loop {
            match shm_read.read_frame() {
                Ok((payload, tag)) => {
                    if let Err(error) = quic_write.write_frame(&payload, tag) {
                        warn!(%error, "failed to write frame to quic");
                        break;
                    }
                }
                Err(Error::BufferEmpty) => {
                    selium_guest::yield_now().await;
                }
                Err(error) => {
                    warn!(%error, "shm read failed");
                    break;
                }
            }
        }
    }

    /// Builds a Quinn server config from the embedded reference certificate.
    fn make_server_config() -> selium_guest::Result<quinn::ServerConfig> {
        use quinn::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

        let cert = CertificateDer::from(BRIDGE_CERT_DER.to_vec());
        let key: PrivateKeyDer<'static> = PrivatePkcs8KeyDer::from(BRIDGE_KEY_DER.to_vec()).into();

        quinn::ServerConfig::with_single_cert(vec![cert], key).map_err(|error| {
            selium_guest::GuestError::Host(format!("quinn server config: {error}"))
        })
    }
}
