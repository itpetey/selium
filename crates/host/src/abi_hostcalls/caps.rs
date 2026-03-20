use selium_abi::{Capability, GuestContext};
use wasmtime::Linker;

pub fn caps_check(ctx: &GuestContext, capability: Capability) -> bool {
    ctx.has_capability(capability)
}

pub fn caps_list(ctx: &GuestContext) -> Vec<Capability> {
    ctx.capabilities.iter().cloned().collect()
}

pub fn add_to_linker(_linker: &mut Linker<GuestContext>) -> anyhow::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_caps_check_present() {
        let ctx =
            GuestContext::with_capabilities(1, [Capability::TimeRead, Capability::StorageLogRead]);
        assert!(caps_check(&ctx, Capability::TimeRead));
        assert!(caps_check(&ctx, Capability::StorageLogRead));
    }

    #[test]
    fn test_caps_check_missing() {
        let ctx = GuestContext::with_capabilities(1, [Capability::TimeRead]);
        assert!(!caps_check(&ctx, Capability::StorageLogRead));
    }

    #[test]
    fn test_caps_list() {
        let ctx =
            GuestContext::with_capabilities(1, [Capability::TimeRead, Capability::StorageLogRead]);
        let caps = caps_list(&ctx);
        assert_eq!(caps.len(), 2);
        assert!(caps.contains(&Capability::TimeRead));
        assert!(caps.contains(&Capability::StorageLogRead));
    }
}
