//! Per-connection client identity derivation for the mTLS connector.
//!
//! QUIC handshakes enforce client authentication at the endpoint via a union
//! verifier over all configured tenant trust anchors. This module derives the
//! *authenticated identity* of an accepted connection for handoff metadata:
//!
//! - **tenant scope** — the tenant whose trust anchor verifies the presented
//!   client certificate (each tenant's anchor is tried in turn, mirroring the
//!   endpoint-global union verifier);
//! - **key fingerprint** — the SHA-256 of the leaf certificate's
//!   SubjectPublicKeyInfo (SPKI), so "same key = same client" survives
//!   certificate renewal while remaining stable across rotation.
//!
//! The identity is encoded as an opaque, self-describing byte payload and
//! attached to every stream handoff as `HostQueueSend` metadata.

use std::sync::Arc;

use quinn::{
    ServerConfig,
    crypto::rustls::QuicServerConfig,
    rustls::{
        RootCertStore,
        crypto::ring::default_provider,
        pki_types::{CertificateDer, PrivateKeyDer, UnixTime},
        server::{WebPkiClientVerifier, danger::ClientCertVerifier},
        version,
    },
};
use selium_abi::client_identity::{ClientIdentity, FINGERPRINT_LEN};
use sha2::{Digest, Sha256};

use crate::TlsError;

/// A single tenant's client-certification trust anchor + its verifier.
pub struct ClientAnchor {
    tenant: String,
    verifier: Arc<dyn ClientCertVerifier>,
}

/// The set of configured per-tenant client trust anchors.
///
/// Building [`ClientAnchorSet`] with no anchors is a hard error: mTLS is
/// endpoint-global, so a connector with no client trust anchors must refuse to
/// serve rather than silently accept unauthenticated connections.
#[derive(Clone)]
pub struct ClientAnchorSet {
    anchors: Arc<Vec<ClientAnchor>>,
    union: Arc<dyn ClientCertVerifier>,
}

impl ClientAnchor {
    /// Builds a tenant anchor from a single CA certificate (the trust root
    /// for that tenant's client certificates), given its verifier.
    fn with_verifier(tenant: String, verifier: Arc<dyn ClientCertVerifier>) -> Self {
        Self { tenant, verifier }
    }

    /// Returns this anchor's tenant scope.
    pub fn tenant(&self) -> &str {
        &self.tenant
    }

    /// Returns whether this anchor verifies the presented client certificate.
    pub fn verifies(
        &self,
        leaf: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
    ) -> bool {
        let now = UnixTime::now();
        self.verifier
            .verify_client_cert(leaf, intermediates, now)
            .is_ok()
    }
}

impl ClientAnchorSet {
    /// Builds the anchor set from `(tenant, CA certificate)` pairs.
    ///
    /// Fails loudly when no anchors are provided or a certificate is invalid.
    pub fn new(anchors: Vec<(String, CertificateDer<'static>)>) -> Result<Self, TlsError> {
        if anchors.is_empty() {
            return Err(TlsError::MissingClientAnchors);
        }

        let mut union_roots = RootCertStore::empty();
        let mut built = Vec::with_capacity(anchors.len());
        for (tenant, cert) in anchors {
            union_roots.add(cert.clone()).map_err(|e| {
                tracing::error!("quic-connector: invalid client anchor for {tenant}: {e}");
                TlsError::InvalidClientAnchor
            })?;
            let roots = RootCertStore::empty();
            let verifier = per_tenant_verifier(roots, &cert, &tenant)?;
            built.push(ClientAnchor::with_verifier(tenant, verifier));
        }

        let union = WebPkiClientVerifier::builder(Arc::new(union_roots))
            .build()
            .map_err(|e| {
                tracing::error!("quic-connector: client union verifier build failed: {e}");
                TlsError::InvalidClientAnchor
            })?;

        Ok(Self {
            anchors: Arc::new(built),
            union,
        })
    }

    /// Returns the endpoint-global union client verifier (mandatory).
    pub fn union_verifier(&self) -> Arc<dyn ClientCertVerifier> {
        self.union.clone()
    }

    /// Derives the authenticated identity for a `quinn::Connection`.
    pub fn identity_for(&self, connection: &quinn::Connection) -> Option<ClientIdentity> {
        let chain = connection
            .peer_identity()?
            .downcast::<Vec<CertificateDer<'static>>>()
            .ok()?;
        self.identity_from_chain(&chain)
    }

    /// Derives the authenticated identity from a presented certificate chain.
    pub fn identity_from_chain(&self, chain: &[CertificateDer<'static>]) -> Option<ClientIdentity> {
        let leaf = chain.first()?;
        let fingerprint = spki_fingerprint(leaf)?;
        let intermediates = chain.get(1..).unwrap_or_default();
        let tenant = self
            .anchors
            .iter()
            .find(|anchor| anchor.verifies(leaf, intermediates))
            .map(|anchor| anchor.tenant().to_string())?;
        Some(ClientIdentity {
            tenant,
            fingerprint,
        })
    }
}

/// Builds the QUIC server configuration: TLS 1.3, with **opt-in** mandatory
/// client authentication.
///
/// - `Some(anchors)`: every connection must present a client certificate
///   verifying against the union of the configured tenant anchors (mTLS).
/// - `None`: no client authentication (mTLS opt-out; routes that require it,
///   like the bridge, must not be served without anchors).
///
/// TLS 1.3 0-RTT early data stays **disabled** (the rustls default): early
/// data is replayable across connections, and the connector relays stream
/// bytes into the fabric under the authenticated client identity.
pub fn build_server_config(
    certs: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
    anchors: Option<&ClientAnchorSet>,
) -> Result<ServerConfig, TlsError> {
    let provider = default_provider();
    let builder = quinn::rustls::ServerConfig::builder_with_provider(Arc::new(provider))
        .with_protocol_versions(&[&version::TLS13])
        .map_err(|e| {
            tracing::error!("quic-connector: TLS provider missing TLS 1.3: {e}");
            TlsError::ConfigError
        })?;
    let rustls_config = match anchors {
        Some(anchors) => builder
            .with_client_cert_verifier(anchors.union_verifier())
            .with_single_cert(certs, key)
            .map_err(|e| {
                tracing::error!("quic-connector: failed to build TLS config: {e}");
                TlsError::ConfigError
            })?,
        None => builder
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| {
                tracing::error!("quic-connector: failed to build TLS config: {e}");
                TlsError::ConfigError
            })?,
    };

    let quic_crypto = QuicServerConfig::try_from(rustls_config).map_err(|e| {
        tracing::error!("quic-connector: QUIC crypto config rejected: {e}");
        TlsError::ConfigError
    })?;
    Ok(ServerConfig::with_crypto(Arc::new(quic_crypto)))
}

/// Builds a per-tenant client verifier from a single trust anchor.
fn per_tenant_verifier(
    mut roots: RootCertStore,
    cert: &CertificateDer<'static>,
    tenant: &str,
) -> Result<Arc<dyn ClientCertVerifier>, TlsError> {
    roots.add(cert.clone()).map_err(|e| {
        tracing::error!("quic-connector: invalid client anchor for {tenant}: {e}");
        TlsError::InvalidClientAnchor
    })?;
    WebPkiClientVerifier::builder(Arc::new(roots))
        .build()
        .map_err(|e| {
            tracing::error!("quic-connector: client verifier build failed for {tenant}: {e}");
            TlsError::InvalidClientAnchor
        })
}

/// SHA-256 of a certificate's SPKI.
fn spki_fingerprint(cert: &CertificateDer<'_>) -> Option<[u8; FINGERPRINT_LEN]> {
    let parsed = quinn::rustls::server::ParsedCertificate::try_from(cert).ok()?;
    let spki = parsed.subject_public_key_info();
    let digest = Sha256::digest(spki.as_ref());
    let mut out = [0u8; FINGERPRINT_LEN];
    out.copy_from_slice(&digest);
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    const CLIENT_CERT_DER: &[u8] = include_bytes!("../tests/fixtures/client_cert.der");
    const SERVER_CERT_DER: &[u8] = include_bytes!("../tests/fixtures/cert.der");

    fn client_anchor() -> ClientAnchorSet {
        let cert = CertificateDer::from(CLIENT_CERT_DER.to_vec());
        ClientAnchorSet::new(vec![("acme".to_string(), cert)]).expect("anchor set")
    }

    #[test]
    fn absent_anchors_fails_loudly() {
        assert!(matches!(
            ClientAnchorSet::new(Vec::new()),
            Err(TlsError::MissingClientAnchors)
        ));
    }

    #[test]
    fn identity_is_derived_from_fixture_chain() {
        let anchors = client_anchor();
        let cert = CertificateDer::from(CLIENT_CERT_DER.to_vec());

        let identity = anchors
            .identity_from_chain(std::slice::from_ref(&cert))
            .expect("self-signed anchor verifies its own chain");

        assert_eq!(identity.tenant, "acme");
        let parsed = quinn::rustls::server::ParsedCertificate::try_from(&cert).expect("parse");
        let expected = Sha256::digest(parsed.subject_public_key_info().as_ref());
        assert_eq!(identity.fingerprint.as_slice(), expected.as_slice());
    }

    #[test]
    fn identity_is_rejected_for_unknown_chain() {
        // A chain issued outside the configured anchors (here: the server's
        // own self-signed certificate) must not derive an identity.
        let anchors = client_anchor();
        let server_cert = CertificateDer::from(SERVER_CERT_DER.to_vec());
        assert!(anchors.identity_from_chain(&[server_cert]).is_none());
    }
}
