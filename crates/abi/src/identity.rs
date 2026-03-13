use std::{fmt, str::FromStr};

use rkyv::{Archive, Deserialize, Serialize};

/// Stable kind of principal recognised by Selium.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Archive, Serialize, Deserialize,
)]
#[rkyv(bytecheck())]
pub enum PrincipalKind {
    /// Human operator or user principal.
    User,
    /// Machine or automation principal.
    Machine,
    /// Selium runtime peer or cluster node principal.
    RuntimePeer,
    /// Workload-owned principal.
    Workload,
    /// Internal Selium principal.
    Internal,
}

impl PrincipalKind {
    /// Return the wire-friendly label for this principal kind.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Machine => "machine",
            Self::RuntimePeer => "runtime-peer",
            Self::Workload => "workload",
            Self::Internal => "internal",
        }
    }
}

impl fmt::Display for PrincipalKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for PrincipalKind {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "user" => Ok(Self::User),
            "machine" => Ok(Self::Machine),
            "runtime-peer" | "runtime_peer" => Ok(Self::RuntimePeer),
            "workload" => Ok(Self::Workload),
            "internal" => Ok(Self::Internal),
            _ => Err("unknown principal kind"),
        }
    }
}

/// Stable external principal identifier authenticated by Selium.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct PrincipalRef {
    /// Principal category.
    pub kind: PrincipalKind,
    /// Opaque external subject identifier.
    pub external_subject_id: String,
}

impl PrincipalRef {
    /// Construct a new principal reference.
    pub fn new(kind: PrincipalKind, external_subject_id: impl Into<String>) -> Self {
        Self {
            kind,
            external_subject_id: external_subject_id.into(),
        }
    }
}

impl fmt::Display for PrincipalRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.kind, self.external_subject_id)
    }
}
