//! Reference runtime adapter for Wasmtime.

use selium_runtime_adapter_spi::{
    AdapterError, AdapterKind, ExecutionProfile, ModuleSpec, RuntimeAdapter,
};

pub struct WasmtimeAdapter;

impl RuntimeAdapter for WasmtimeAdapter {
    fn kind(&self) -> AdapterKind {
        AdapterKind::Wasmtime
    }

    fn adapter_name(&self) -> &'static str {
        "wasmtime"
    }

    fn supported_profiles(&self) -> &'static [ExecutionProfile] {
        &[ExecutionProfile::Standard, ExecutionProfile::Hardened]
    }

    fn validate(&self, spec: &ModuleSpec) -> Result<(), AdapterError> {
        if spec.module_id.trim().is_empty() || spec.entrypoint.trim().is_empty() {
            return Err(AdapterError::NotConfigured);
        }

        if !spec.module_id.ends_with(".wasm") {
            return Err(AdapterError::InvalidModuleId(spec.module_id.clone()));
        }

        if !self.supported_profiles().contains(&spec.profile) {
            return Err(AdapterError::UnsupportedProfile(spec.profile));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use selium_runtime_adapter_spi::ExecutionProfile;

    use super::*;

    #[test]
    fn validates_wasm_spec() {
        let adapter = WasmtimeAdapter;
        let spec = ModuleSpec {
            module_id: "mods/echo.wasm".to_string(),
            entrypoint: "start".to_string(),
            capabilities: Vec::new(),
            profile: ExecutionProfile::Standard,
        };
        adapter.validate(&spec).expect("valid spec");
    }
}
