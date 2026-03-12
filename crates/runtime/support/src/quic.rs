use std::{
    fs,
    net::{SocketAddr, ToSocketAddrs},
    path::Path,
    sync::Arc,
};

use anyhow::{Context, Result, anyhow};
use quinn::{Endpoint, crypto::rustls::QuicServerConfig};
use rustls::{RootCertStore, pki_types::PrivateKeyDer};
use rustls_pemfile::{certs, private_key};
use tokio_rustls::{TlsAcceptor, TlsConnector};

/// TLS inputs for a QUIC or TLS client.
#[derive(Debug, Clone, Copy)]
pub struct ClientTlsPaths<'a> {
    /// Root CA bundle.
    pub ca_cert: &'a Path,
    /// Optional client certificate.
    pub client_cert: Option<&'a Path>,
    /// Optional client private key.
    pub client_key: Option<&'a Path>,
}

/// TLS inputs for a QUIC or TLS server.
#[derive(Debug, Clone, Copy)]
pub struct ServerTlsPaths<'a> {
    /// Server certificate chain.
    pub cert: &'a Path,
    /// Server private key.
    pub key: &'a Path,
}

/// Build a QUIC client endpoint using the provided TLS material.
pub fn build_quic_client_endpoint(bind: SocketAddr, tls: ClientTlsPaths<'_>) -> Result<Endpoint> {
    let mut endpoint = Endpoint::client(bind).context("create QUIC client endpoint")?;
    endpoint.set_default_client_config(build_quic_client_config(tls)?);
    Ok(endpoint)
}

/// Build a QUIC client config using the provided TLS material.
pub fn build_quic_client_config(tls: ClientTlsPaths<'_>) -> Result<quinn::ClientConfig> {
    let tls = build_tls_client_config(tls)?;
    let quic_crypto =
        quinn::crypto::rustls::QuicClientConfig::try_from(tls).context("build QUIC crypto")?;
    Ok(quinn::ClientConfig::new(Arc::new(quic_crypto)))
}

/// Build a QUIC server endpoint using optional client certificate verification.
pub fn build_quic_server_endpoint(
    addr: SocketAddr,
    tls: ServerTlsPaths<'_>,
    client_ca: Option<&Path>,
) -> Result<Endpoint> {
    let cert_chain = load_cert_chain(tls.cert)?;
    let key = load_private_key(tls.key)?;
    let builder = rustls::ServerConfig::builder();
    let server_tls = if let Some(client_ca) = client_ca {
        let verifier =
            rustls::server::WebPkiClientVerifier::builder(Arc::new(load_root_store(client_ca)?))
                .build()
                .context("build QUIC client verifier")?;
        builder
            .with_client_cert_verifier(verifier)
            .with_single_cert(cert_chain, key)
            .context("build QUIC server TLS config")?
    } else {
        builder
            .with_no_client_auth()
            .with_single_cert(cert_chain, key)
            .context("build QUIC server TLS config")?
    };
    let quic_server = QuicServerConfig::try_from(server_tls).context("build QUIC server config")?;
    let server_config = quinn::ServerConfig::with_crypto(Arc::new(quic_server));
    Endpoint::server(server_config, addr).context("bind QUIC server endpoint")
}

/// Build a TLS connector from the provided client TLS material.
pub fn build_tls_connector(tls: ClientTlsPaths<'_>) -> Result<TlsConnector> {
    let config = build_tls_client_config(tls)?;
    Ok(TlsConnector::from(Arc::new(config)))
}

/// Build a TLS acceptor from the provided server TLS material.
pub fn build_tls_acceptor(tls: ServerTlsPaths<'_>) -> Result<TlsAcceptor> {
    let cert_chain = load_cert_chain(tls.cert)?;
    let key = load_private_key(tls.key)?;
    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert_chain, key)
        .context("build TLS server config")?;
    Ok(TlsAcceptor::from(Arc::new(config)))
}

fn build_tls_client_config(tls: ClientTlsPaths<'_>) -> Result<rustls::ClientConfig> {
    let roots = load_root_store(tls.ca_cert)?;
    let builder = rustls::ClientConfig::builder().with_root_certificates(roots);
    match (tls.client_cert, tls.client_key) {
        (Some(cert), Some(key)) => builder
            .with_client_auth_cert(load_cert_chain(cert)?, load_private_key(key)?)
            .context("build TLS client config"),
        _ => Ok(builder.with_no_client_auth()),
    }
}

/// Parse an address that may include a scheme.
pub fn parse_socket_addr(raw: &str) -> Result<SocketAddr> {
    let raw = raw
        .trim()
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .trim_start_matches("quic://")
        .trim_end_matches('/');

    if let Ok(addr) = raw.parse::<SocketAddr>() {
        return Ok(addr);
    }

    raw.to_socket_addrs()?
        .next()
        .ok_or_else(|| anyhow!("no socket addresses for `{raw}`"))
}

/// Parse a network authority into a socket address.
pub fn parse_authority_addr(raw: &str) -> Result<SocketAddr> {
    let (raw, _) = split_authority(raw);
    parse_socket_addr(raw)
}

/// Derive a TLS server name from an authority, preserving explicit overrides.
pub fn derive_server_name(authority: &str) -> String {
    let (authority, explicit_server_name) = split_authority(authority);
    explicit_server_name.unwrap_or_else(|| {
        authority
            .trim_start_matches("https://")
            .trim_start_matches("http://")
            .trim_start_matches("quic://")
            .trim_end_matches('/')
            .split(':')
            .next()
            .unwrap_or("localhost")
            .to_string()
    })
}

fn split_authority(raw: &str) -> (&str, Option<String>) {
    let trimmed = raw.trim();
    match trimmed.rsplit_once('@') {
        Some((authority, server_name)) if !server_name.trim().is_empty() => {
            (authority.trim(), Some(server_name.trim().to_string()))
        }
        _ => (trimmed, None),
    }
}

/// Load a PEM root store from disk.
pub fn load_root_store(path: &Path) -> Result<RootCertStore> {
    let pem = fs::read(path).with_context(|| format!("read CA cert {}", path.display()))?;
    let mut reader = std::io::Cursor::new(pem);
    let mut store = RootCertStore::empty();
    for cert in certs(&mut reader) {
        let cert = cert.context("parse CA cert")?;
        store
            .add(cert)
            .map_err(|err| anyhow!("add CA cert to root store: {err}"))?;
    }
    Ok(store)
}

/// Load a PEM certificate chain from disk.
pub fn load_cert_chain(path: &Path) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>> {
    let pem = fs::read(path).with_context(|| format!("read cert {}", path.display()))?;
    let mut reader = std::io::Cursor::new(pem);
    let mut chain = Vec::new();
    for cert in certs(&mut reader) {
        chain.push(cert.context("parse cert")?);
    }
    if chain.is_empty() {
        return Err(anyhow!("no certs found in {}", path.display()));
    }
    Ok(chain)
}

/// Load a PEM private key from disk.
pub fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>> {
    let pem = fs::read(path).with_context(|| format!("read key {}", path.display()))?;
    let mut reader = std::io::Cursor::new(pem);
    private_key(&mut reader)
        .context("parse private key")?
        .ok_or_else(|| anyhow!("no private key in {}", path.display()))
}
