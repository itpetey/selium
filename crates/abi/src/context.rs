use std::collections::HashSet;

use crate::Capability;
use crate::GuestId;

#[derive(Debug, Clone)]
pub struct GuestContext {
    pub guest_id: GuestId,
    pub capabilities: HashSet<Capability>,
}

impl GuestContext {
    pub fn new(guest_id: GuestId, capabilities: HashSet<Capability>) -> Self {
        Self {
            guest_id,
            capabilities,
        }
    }

    pub fn has_capability(&self, capability: Capability) -> bool {
        self.capabilities.contains(&capability)
    }

    pub fn with_capabilities(
        guest_id: GuestId,
        capabilities: impl IntoIterator<Item = Capability>,
    ) -> Self {
        Self {
            guest_id,
            capabilities: capabilities.into_iter().collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn has_capability_present() {
        let ctx =
            GuestContext::with_capabilities(1, [Capability::TimeRead, Capability::StorageLogRead]);
        assert!(ctx.has_capability(Capability::TimeRead));
        assert!(ctx.has_capability(Capability::StorageLogRead));
    }

    #[test]
    fn has_capability_missing() {
        let ctx = GuestContext::with_capabilities(1, [Capability::TimeRead]);
        assert!(!ctx.has_capability(Capability::StorageLogRead));
    }

    #[test]
    fn empty_capabilities() {
        let ctx = GuestContext::new(1, HashSet::new());
        assert!(!ctx.has_capability(Capability::TimeRead));
    }
}
