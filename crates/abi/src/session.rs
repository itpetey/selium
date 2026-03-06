use rkyv::{Archive, Deserialize, Serialize};

use crate::{Capability, GuestUint};

/// Request to create a new session.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct SessionCreate {
    /// Parent session handle.
    pub session_id: GuestUint,
    /// Public key to associate with the new session.
    pub pubkey: [u8; 32],
}

/// Request to add or remove entitlements from a session.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct SessionEntitlement {
    /// Parent session handle.
    pub session_id: GuestUint,
    /// Target session handle.
    pub target_id: GuestUint,
    /// Capability to add or remove.
    pub capability: Capability,
}

/// Request to attach or detach a resource from a session entitlement.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct SessionResource {
    /// Parent session handle.
    pub session_id: GuestUint,
    /// Target session handle.
    pub target_id: GuestUint,
    /// Capability being modified.
    pub capability: Capability,
    /// Resource handle.
    pub resource_id: crate::GuestResourceId,
}

/// Request to remove a session.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct SessionRemove {
    /// Parent session handle.
    pub session_id: GuestUint,
    /// Target session handle.
    pub target_id: GuestUint,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{decode_rkyv, encode_rkyv};

    #[test]
    fn session_create_round_trips_with_rkyv() {
        let payload = SessionCreate {
            session_id: 5,
            pubkey: [7; 32],
        };

        let encoded = encode_rkyv(&payload).expect("encode");
        let decoded = decode_rkyv::<SessionCreate>(&encoded).expect("decode");
        assert_eq!(decoded, payload);
    }

    #[test]
    fn session_resource_round_trips_with_rkyv() {
        let payload = SessionResource {
            session_id: 1,
            target_id: 2,
            capability: Capability::SharedMemory,
            resource_id: 42,
        };

        let encoded = encode_rkyv(&payload).expect("encode");
        let decoded = decode_rkyv::<SessionResource>(&encoded).expect("decode");
        assert_eq!(decoded, payload);
    }
}
