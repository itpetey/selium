//! Host-held PKI keyring backing the certificate-signing hostcalls.
//!
//! The keyring is the host side of the dumb-host/smart-guest crypto split:
//! the identity guest owns *policy* (who mints what, when to rotate or
//! revoke) while the host owns the *keys*. A depth-3 hierarchy is held here:
//!
//! - an **offline root** (`root`), retained for operator bootstrap only —
//!   no guest-facing hostcall references it;
//! - an **online intermediate** (`intermediate`), which signs tenant CAs;
//! - **per-tenant CA keys** (`tenant_cas`), which sign user leaf certificates.
//!
//! No read path exposes private-key material: the only private-key accessors
//! are the signing methods themselves, which reconstruct a [`rcgen::KeyPair`]
//! transiently from the stored PKCS#8 DER and discard it after signing.
//!
//! Tenant CA keys live behind a pluggable [`CaStore`] backing. The in-process
//! implementation is the day-1 default; a file or HSM backing replaces it
//! without changing the hostcall surface.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use rcgen::{
    BasicConstraints, Certificate, CertificateParams, DistinguishedName, DnType,
    ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair, KeyUsagePurpose, PKCS_ECDSA_P256_SHA256,
    SubjectPublicKeyInfo,
};
use rustls_pki_types::CertificateDer;
use thiserror::Error;
use time::{Duration, OffsetDateTime};

/// Default lifetime of a user leaf certificate, honouring the design's
/// "short-TTL leaf" requirement. Tokens degrade to expiry-driven revocation
/// alone after this window unless grants are removed first.
const DEFAULT_LEAF_TTL: Duration = Duration::days(1);

/// Pluggable backing for tenant CA keys.
pub trait CaStore: Send + Sync {
    /// Stores a tenant CA key.
    fn insert(&mut self, tenant: &str, ca: TenantCa) -> Result<(), KeyringError>;
    /// Returns a tenant CA key, if present.
    fn get(&self, tenant: &str) -> Option<TenantCa>;
    /// Removes and returns a tenant CA key, if present.
    fn remove(&mut self, tenant: &str) -> Option<TenantCa>;
}

#[derive(Debug, Error)]
pub enum KeyringError {
    #[error("no intermediate key material configured")]
    IntermediateMissing,
    #[error("invalid certificate material: {0}")]
    InvalidMaterial(String),
    #[error("signing failed: {0}")]
    Signing(String),
    #[error("tenant CA not found: {tenant}")]
    TenantCaNotFound { tenant: String },
    #[error("unsupported subject key algorithm")]
    UnsupportedSubjectKey,
    #[error("keyring state error: {0}")]
    Io(#[from] std::io::Error),
}

/// Host-side PKI key material: a DER certificate plus its PKCS#8 private key.
#[derive(Debug, Clone)]
pub struct KeyMaterial {
    /// DER-encoded X.509 certificate (public).
    pub cert_der: Vec<u8>,
    /// DER-encoded PKCS#8 private key (never leaves the host).
    pub key_der: Vec<u8>,
}

/// Material needed to initialise the keyring at startup.
#[derive(Debug, Clone, Default)]
pub struct KeyringBootstrap {
    /// Optional offline root, retained for operator bootstrap only.
    pub root: Option<KeyMaterial>,
    /// The online intermediate. `None` means the keyring cannot be
    /// initialised (the host would be unable to sign tenant CAs).
    pub intermediate: Option<KeyMaterial>,
}

/// Retained material for a tenant CA: public anchor certificate + private key.
/// `cert_der` is the only observable half; `key_der` is accessed only for
/// signing.
#[derive(Debug, Clone)]
pub struct TenantCa {
    pub(crate) cert_der: Vec<u8>,
    pub(crate) key_der: Vec<u8>,
}

/// The in-process backing implementation.
#[derive(Debug, Default)]
pub struct InMemoryCaStore {
    by_tenant: HashMap<String, TenantCa>,
}

/// The host-held keyring.
pub struct Keyring {
    intermediate: KeyMaterial,
    root: Option<KeyMaterial>,
    tenant_cas: Box<dyn CaStore>,
}

/// File-backed tenant CA store: one certificate/key pair per tenant.
pub struct FileCaStore {
    dir: PathBuf,
}

/// The host keyring persisted under a state directory.
///
/// Covers the whole hierarchy — the offline root, the online intermediate and
/// the per-tenant CAs — so a restart reuses the material it already had rather
/// than silently replacing it: certificates issued before a restart keep
/// validating against the tenant CA they chain to. An empty state directory
/// generates and persists a fresh hierarchy on first boot.
///
/// Layout: `<state>/keys/{root,intermediate}.{cert,key}.der` and
/// `<state>/keys/tenants/<enc tenant>.{cert,key}.der`.
pub struct KeyringStore {
    dir: PathBuf,
}

impl CaStore for InMemoryCaStore {
    fn insert(&mut self, tenant: &str, ca: TenantCa) -> Result<(), KeyringError> {
        self.by_tenant.insert(tenant.to_string(), ca);
        Ok(())
    }

    fn get(&self, tenant: &str) -> Option<TenantCa> {
        self.by_tenant.get(tenant).cloned()
    }

    fn remove(&mut self, tenant: &str) -> Option<TenantCa> {
        self.by_tenant.remove(tenant)
    }
}

impl Keyring {
    /// Initialises a keyring from bootstrap material, failing loudly when the
    /// intermediate is missing or invalid.
    pub fn initialize(
        tenant_cas: Box<dyn CaStore>,
        bootstrap: KeyringBootstrap,
    ) -> Result<Self, KeyringError> {
        let intermediate = bootstrap
            .intermediate
            .ok_or(KeyringError::IntermediateMissing)?;
        validate_material(&intermediate)?;
        if let Some(root) = &bootstrap.root {
            validate_material(root)?;
        }
        Ok(Self {
            intermediate,
            root: bootstrap.root,
            tenant_cas,
        })
    }

    /// Generates a fresh root + intermediate hierarchy. The offline root is
    /// retained for operator bootstrap and never referenced by a hostcall.
    pub fn generate() -> Result<Self, KeyringError> {
        let root_key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256)
            .map_err(|error| KeyringError::Signing(error.to_string()))?;
        let root_params = ca_params("Selium Root CA");
        let root_cert = root_params
            .self_signed(&root_key)
            .map_err(|error| KeyringError::Signing(error.to_string()))?;
        let root_issuer = Issuer::from_params(&root_params, &root_key);

        let intermediate_key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256)
            .map_err(|error| KeyringError::Signing(error.to_string()))?;
        let intermediate_params = ca_params("Selium Intermediate CA");
        let intermediate_cert = intermediate_params
            .signed_by(&intermediate_key, &root_issuer)
            .map_err(|error| KeyringError::Signing(error.to_string()))?;

        Ok(Self {
            intermediate: material_from(&intermediate_cert, &intermediate_key),
            root: Some(material_from(&root_cert, &root_key)),
            tenant_cas: Box::new(InMemoryCaStore::default()),
        })
    }

    /// Swaps the tenant CA backing for a custom store (e.g. a file or HSM
    /// backing).
    pub fn with_tenant_store(mut self, store: Box<dyn CaStore>) -> Self {
        self.tenant_cas = store;
        self
    }

    /// Returns the intermediate certificate (public DER only).
    pub fn intermediate_cert(&self) -> &[u8] {
        &self.intermediate.cert_der
    }

    /// Returns the root certificate (public DER only), if retained.
    pub fn root_cert(&self) -> Option<&[u8]> {
        self.root
            .as_ref()
            .map(|material| material.cert_der.as_slice())
    }

    /// Returns the host-private material a later boot needs to reconstruct this
    /// hierarchy.
    ///
    /// Host-only, and the counterpart of [`KeyringBootstrap`]: the private keys
    /// it carries are written to the host's state directory and never cross to
    /// a guest.
    pub fn bootstrap_material(&self) -> KeyringBootstrap {
        KeyringBootstrap {
            root: self.root.clone(),
            intermediate: Some(self.intermediate.clone()),
        }
    }

    /// Mints a tenant CA: generates a host-held tenant key pair, signs it via
    /// the online intermediate, retains the key, and returns the DER-encoded
    /// tenant CA certificate.
    pub fn sign_tenant_ca(&mut self, tenant: &str) -> Result<Vec<u8>, KeyringError> {
        let issuer_key = KeyPair::try_from(self.intermediate.key_der.as_slice()).map_err(|e| {
            KeyringError::InvalidMaterial(format!("intermediate key parse failed: {e}"))
        })?;
        let issuer_cert = CertificateDer::from(self.intermediate.cert_der.clone());
        let issuer = Issuer::from_ca_cert_der(&issuer_cert, issuer_key).map_err(|e| {
            KeyringError::InvalidMaterial(format!("intermediate cert parse failed: {e}"))
        })?;

        let tenant_key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256)
            .map_err(|error| KeyringError::Signing(error.to_string()))?;
        let params = ca_params(&format!("Selium Tenant CA {tenant}"));
        let cert = params
            .signed_by(&tenant_key, &issuer)
            .map_err(|error| KeyringError::Signing(error.to_string()))?;
        let cert_der = cert.der().to_vec();
        self.tenant_cas.insert(
            tenant,
            TenantCa {
                cert_der: cert_der.clone(),
                key_der: tenant_key.serialize_der(),
            },
        )?;
        Ok(cert_der)
    }

    /// Signs a client-supplied SPKI via the tenant's CA key, returning the
    /// DER-encoded short-TTL leaf certificate. The client's private key never
    /// leaves the client; only its public key crosses the boundary.
    pub fn sign_user_cert(&self, tenant: &str, spki_der: &[u8]) -> Result<Vec<u8>, KeyringError> {
        let ca = self
            .tenant_cas
            .get(tenant)
            .ok_or_else(|| KeyringError::TenantCaNotFound {
                tenant: tenant.to_string(),
            })?;
        let ca_key = KeyPair::try_from(ca.key_der.as_slice()).map_err(|e| {
            KeyringError::InvalidMaterial(format!("tenant CA key parse failed: {e}"))
        })?;
        let ca_cert = CertificateDer::from(ca.cert_der.clone());
        let issuer = Issuer::from_ca_cert_der(&ca_cert, ca_key).map_err(|e| {
            KeyringError::InvalidMaterial(format!("tenant CA cert parse failed: {e}"))
        })?;
        let subject = SubjectPublicKeyInfo::from_der(spki_der)
            .map_err(|_error| KeyringError::UnsupportedSubjectKey)?;

        let mut params = leaf_params(tenant);
        let not_before = OffsetDateTime::now_utc() - Duration::minutes(5);
        params.not_before = not_before;
        params.not_after = not_before + DEFAULT_LEAF_TTL;

        let cert = params
            .signed_by(&subject, &issuer)
            .map_err(|error| KeyringError::Signing(error.to_string()))?;
        Ok(cert.der().to_vec())
    }

    /// Deletes a tenant CA key from the keyring.
    pub fn revoke_ca(&mut self, tenant: &str) -> Result<(), KeyringError> {
        if self.tenant_cas.remove(tenant).is_none() {
            return Err(KeyringError::TenantCaNotFound {
                tenant: tenant.to_string(),
            });
        }
        Ok(())
    }
}

impl FileCaStore {
    /// Creates a store whose material lives under `dir`.
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }

    fn cert_path(&self, tenant: &str) -> PathBuf {
        self.dir
            .join(format!("{}.cert.der", encode_component(tenant)))
    }

    fn key_path(&self, tenant: &str) -> PathBuf {
        self.dir
            .join(format!("{}.key.der", encode_component(tenant)))
    }
}

impl CaStore for FileCaStore {
    fn insert(&mut self, tenant: &str, ca: TenantCa) -> Result<(), KeyringError> {
        std::fs::create_dir_all(&self.dir)?;
        write_private(&self.cert_path(tenant), &ca.cert_der)?;
        write_private(&self.key_path(tenant), &ca.key_der)?;
        Ok(())
    }

    fn get(&self, tenant: &str) -> Option<TenantCa> {
        let cert_der = std::fs::read(self.cert_path(tenant)).ok()?;
        let key_der = std::fs::read(self.key_path(tenant)).ok()?;
        Some(TenantCa { cert_der, key_der })
    }

    fn remove(&mut self, tenant: &str) -> Option<TenantCa> {
        let ca = self.get(tenant)?;
        drop(std::fs::remove_file(self.cert_path(tenant)));
        drop(std::fs::remove_file(self.key_path(tenant)));
        Some(ca)
    }
}

impl KeyringStore {
    /// Creates a store rooted under `state_dir`.
    pub fn new(state_dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: state_dir.into().join("keys"),
        }
    }

    /// Loads the persisted hierarchy, or generates and persists a fresh one
    /// when the state directory holds none.
    pub fn load_or_generate(&self) -> Result<Keyring, KeyringError> {
        if let Some(bootstrap) = self.load()? {
            return Keyring::initialize(Box::new(FileCaStore::new(self.tenants_dir())), bootstrap);
        }
        let keyring = Keyring::generate()?;
        self.persist(&keyring)?;
        Ok(keyring.with_tenant_store(Box::new(FileCaStore::new(self.tenants_dir()))))
    }

    fn tenants_dir(&self) -> PathBuf {
        self.dir.join("tenants")
    }

    fn material_paths(&self, name: &str) -> (PathBuf, PathBuf) {
        (
            self.dir.join(format!("{name}.cert.der")),
            self.dir.join(format!("{name}.key.der")),
        )
    }

    /// Reads the persisted hierarchy, returning `None` when it is absent.
    fn load(&self) -> Result<Option<KeyringBootstrap>, KeyringError> {
        let (intermediate_cert, intermediate_key) = self.material_paths("intermediate");
        let Some(intermediate) = read_material(&intermediate_cert, &intermediate_key)? else {
            return Ok(None);
        };
        let (root_cert, root_key) = self.material_paths("root");
        let root = read_material(&root_cert, &root_key)?;
        Ok(Some(KeyringBootstrap {
            root,
            intermediate: Some(intermediate),
        }))
    }

    /// Writes the whole hierarchy to the state directory.
    fn persist(&self, keyring: &Keyring) -> Result<(), KeyringError> {
        std::fs::create_dir_all(&self.dir)?;
        let material = keyring.bootstrap_material();
        let root = material.root.ok_or(KeyringError::IntermediateMissing)?;
        let (root_cert, root_key) = self.material_paths("root");
        write_private(&root_cert, &root.cert_der)?;
        write_private(&root_key, &root.key_der)?;

        let intermediate = material
            .intermediate
            .ok_or(KeyringError::IntermediateMissing)?;
        let (intermediate_cert, intermediate_key) = self.material_paths("intermediate");
        write_private(&intermediate_cert, &intermediate.cert_der)?;
        write_private(&intermediate_key, &intermediate.key_der)?;

        std::fs::create_dir_all(self.tenants_dir())?;
        Ok(())
    }
}

/// Parameters for a CA certificate: CA basic constraints plus key-cert-sign.
fn ca_params(common_name: &str) -> CertificateParams {
    let mut params = CertificateParams::default();
    params.distinguished_name = distinguished_name(common_name);
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    params.use_authority_key_identifier_extension = true;
    params
}

fn distinguished_name(common_name: &str) -> DistinguishedName {
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, common_name);
    dn
}

/// Encodes a tenant name into a single filesystem-safe component.
fn encode_component(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for byte in raw.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-') {
            out.push(byte as char);
        } else {
            out.push('%');
            if let Some(high) = char::from_digit(u32::from(byte >> 4), 16) {
                out.push(high);
            }
            if let Some(low) = char::from_digit(u32::from(byte & 0x0f), 16) {
                out.push(low);
            }
        }
    }
    out
}

/// Parameters for a user leaf: no CA bit, digital signature + client auth.
fn leaf_params(tenant: &str) -> CertificateParams {
    let mut params = CertificateParams::default();
    params.distinguished_name = distinguished_name(&format!("Selium Client {tenant}"));
    params.is_ca = IsCa::NoCa;
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    params.use_authority_key_identifier_extension = true;
    params
}

fn material_from(cert: &Certificate, key: &KeyPair) -> KeyMaterial {
    KeyMaterial {
        cert_der: cert.der().to_vec(),
        key_der: key.serialize_der(),
    }
}

/// Reads a certificate/key pair, returning `None` when either half is absent.
fn read_material(cert: &Path, key: &Path) -> Result<Option<KeyMaterial>, KeyringError> {
    match (std::fs::read(cert), std::fs::read(key)) {
        (Ok(cert_der), Ok(key_der)) => Ok(Some(KeyMaterial { cert_der, key_der })),
        (Err(error), _) | (_, Err(error)) if error.kind() == std::io::ErrorKind::NotFound => {
            Ok(None)
        }
        (Err(error), _) | (_, Err(error)) => Err(error.into()),
    }
}

/// Validates that stored material round-trips through a signing handle.
fn validate_material(material: &KeyMaterial) -> Result<(), KeyringError> {
    let key = KeyPair::try_from(material.key_der.as_slice()).map_err(|error| {
        KeyringError::InvalidMaterial(format!("private key parse failed: {error}"))
    })?;
    let cert = CertificateDer::from(material.cert_der.clone());
    let _issuer = Issuer::from_ca_cert_der(&cert, key).map_err(|error| {
        KeyringError::InvalidMaterial(format!("certificate parse failed: {error}"))
    })?;
    Ok(())
}

/// Writes host-private material, restricting permissions where the platform
/// supports it.
fn write_private(path: &Path, bytes: &[u8]) -> Result<(), KeyringError> {
    std::fs::write(path, bytes)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rcgen::PublicKeyData;
    use x509_parser::prelude::FromDer;

    #[test]
    fn generate_produces_intermediate_and_root() {
        let keyring = Keyring::generate().expect("generate keyring");
        assert!(!keyring.intermediate_cert().is_empty());
        assert!(keyring.root_cert().is_some());
    }

    #[test]
    fn initialize_fails_loudly_without_intermediate() {
        let result = Keyring::initialize(
            Box::new(InMemoryCaStore::default()),
            KeyringBootstrap {
                root: None,
                intermediate: None,
            },
        );
        assert!(matches!(result, Err(KeyringError::IntermediateMissing)));
    }

    #[test]
    fn initialize_rejects_invalid_intermediate() {
        let result = Keyring::initialize(
            Box::new(InMemoryCaStore::default()),
            KeyringBootstrap {
                root: None,
                intermediate: Some(KeyMaterial {
                    cert_der: b"not a certificate".to_vec(),
                    key_der: b"not a key".to_vec(),
                }),
            },
        );
        assert!(matches!(result, Err(KeyringError::InvalidMaterial(_))));
    }

    #[test]
    fn sign_tenant_ca_retains_key_and_returns_certificate() {
        let mut keyring = Keyring::generate().expect("generate");
        let cert_der = keyring.sign_tenant_ca("acme").expect("sign tenant ca");

        // The anchor is a CA certificate.
        let (_, parsed) = x509_parser::parse_x509_certificate(&cert_der).expect("parse anchor");
        assert!(parsed.tbs_certificate.is_ca(), "tenant CA must be a CA");

        // The key was retained: a leaf can be signed under it.
        let leaf = keyring
            .sign_user_cert("acme", &test_spki())
            .expect("sign user cert");
        assert!(!leaf.is_empty());
    }

    #[test]
    fn sign_user_cert_embeds_the_client_spki() {
        let mut keyring = Keyring::generate().expect("generate");
        keyring.sign_tenant_ca("acme").expect("sign tenant ca");

        let spki = test_spki();
        let leaf = keyring.sign_user_cert("acme", &spki).expect("sign leaf");

        let (_, input) =
            x509_parser::x509::SubjectPublicKeyInfo::from_der(&spki).expect("parse input spki");
        let (_, parsed) = x509_parser::parse_x509_certificate(&leaf).expect("parse leaf");
        let leaf_pki = parsed.tbs_certificate.subject_pki;

        // The leaf carries the client's subject public key, not a host key.
        assert_eq!(leaf_pki.algorithm, input.algorithm);
        assert_eq!(
            leaf_pki.subject_public_key.as_ref(),
            input.subject_public_key.as_ref(),
        );
    }

    #[test]
    fn sign_user_cert_leaf_expires_in_bounded_window() {
        let mut keyring = Keyring::generate().expect("generate");
        keyring.sign_tenant_ca("acme").expect("sign tenant ca");

        let leaf = keyring
            .sign_user_cert("acme", &test_spki())
            .expect("sign leaf");
        let (_, parsed) = x509_parser::parse_x509_certificate(&leaf).expect("parse leaf");
        let validity = parsed.validity();
        let now = OffsetDateTime::now_utc();

        // The leaf is currently valid and expires within the short-TTL window.
        assert!(validity.not_before.to_datetime() <= now);
        assert!(validity.not_after.to_datetime() > now);
        assert!(validity.not_after.to_datetime() < now + DEFAULT_LEAF_TTL);
    }

    #[test]
    fn sign_user_cert_unknown_tenant_fails() {
        let keyring = Keyring::generate().expect("generate");
        let result = keyring.sign_user_cert("missing", &test_spki());
        assert!(matches!(result, Err(KeyringError::TenantCaNotFound { .. })));
    }

    #[test]
    fn revoke_ca_removes_key() {
        let mut keyring = Keyring::generate().expect("generate");
        keyring.sign_tenant_ca("acme").expect("sign tenant ca");
        keyring.revoke_ca("acme").expect("revoke ca");

        // Signing under the revoked CA key fails, and double-revoke is not
        // mistaken for success.
        let _ = keyring.sign_user_cert("acme", &test_spki()).unwrap_err();
        assert!(matches!(
            keyring.revoke_ca("acme"),
            Err(KeyringError::TenantCaNotFound { .. })
        ));
    }

    /// Generates a client SPKI via rcgen, mirroring the client-side key
    /// generation whose public half the identity guest forwards to the host.
    fn test_spki() -> Vec<u8> {
        let key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).expect("keygen");
        key.subject_public_key_info()
    }

    /// A unique temporary state directory for a test.
    fn temp_state_dir(tag: &str) -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "selium-{tag}-{}-{nanos}-{unique}",
            std::process::id()
        ))
    }

    #[test]
    fn first_boot_generates_and_persists_the_whole_hierarchy() {
        let dir = temp_state_dir("keyring-fresh");
        let keyring = KeyringStore::new(&dir)
            .load_or_generate()
            .expect("generate");
        let intermediate = keyring.intermediate_cert().to_vec();
        assert!(!intermediate.is_empty());
        for name in ["root", "intermediate"] {
            assert!(
                dir.join("keys").join(format!("{name}.cert.der")).exists(),
                "{name} certificate is persisted"
            );
            assert!(
                dir.join("keys").join(format!("{name}.key.der")).exists(),
                "{name} private key is persisted"
            );
        }

        // The next boot reuses the same material rather than replacing it.
        let reopened = KeyringStore::new(&dir).load_or_generate().expect("reload");
        assert_eq!(reopened.intermediate_cert(), intermediate.as_slice());
        assert_eq!(
            reopened.root_cert().map(<[u8]>::to_vec),
            keyring.root_cert().map(<[u8]>::to_vec)
        );
        drop(std::fs::remove_dir_all(&dir));
    }

    #[test]
    fn a_certificate_minted_after_a_restart_chains_to_the_recorded_tenant_ca() {
        let dir = temp_state_dir("keyring-restart");

        // First lifetime: onboard the tenant, recording its CA.
        let mut first = KeyringStore::new(&dir)
            .load_or_generate()
            .expect("first boot");
        let tenant_ca_der = first.sign_tenant_ca("acme").expect("tenant ca");

        // Restart: a fresh keyring loaded from the same state directory.
        let second = KeyringStore::new(&dir).load_or_generate().expect("restart");
        let leaf_der = second
            .sign_user_cert("acme", &test_spki())
            .expect("leaf minted after restart");

        let (_, tenant_ca) =
            x509_parser::parse_x509_certificate(&tenant_ca_der).expect("parse tenant ca");
        let (_, leaf) = x509_parser::parse_x509_certificate(&leaf_der).expect("parse leaf");
        assert_eq!(
            leaf.issuer().to_string(),
            tenant_ca.subject().to_string(),
            "the leaf chains to the tenant CA recorded before the restart"
        );
        drop(std::fs::remove_dir_all(&dir));
    }

    #[test]
    fn revocation_removes_the_persisted_tenant_ca() {
        let dir = temp_state_dir("keyring-revoke");
        let mut keyring = KeyringStore::new(&dir).load_or_generate().expect("boot");
        keyring.sign_tenant_ca("acme").expect("tenant ca");
        keyring.revoke_ca("acme").expect("revoke");

        // A restart sees no tenant CA: revocation is durable, not per-boot.
        let restarted = KeyringStore::new(&dir).load_or_generate().expect("restart");
        assert!(matches!(
            restarted.sign_user_cert("acme", &test_spki()),
            Err(KeyringError::TenantCaNotFound { .. })
        ));
        drop(std::fs::remove_dir_all(&dir));
    }
}
