//! Reference runtime adaptor for Wasmtime.

use selium_runtime_adaptor_spi::{
    AdaptorError, AdaptorKind, ExecutionProfile, ModuleSpec, RuntimeAdaptor,
};

pub struct WasmtimeAdaptor;

impl RuntimeAdaptor for WasmtimeAdaptor {
    fn kind(&self) -> AdaptorKind {
        AdaptorKind::Wasmtime
    }

    fn adaptor_name(&self) -> &'static str {
        "wasmtime"
    }

    fn supported_profiles(&self) -> &'static [ExecutionProfile] {
        &[ExecutionProfile::Standard, ExecutionProfile::Hardened]
    }

    fn validate(&self, spec: &ModuleSpec) -> Result<(), AdaptorError> {
        if spec.module_id.trim().is_empty() || spec.entrypoint.trim().is_empty() {
            return Err(AdaptorError::NotConfigured);
        }

        if !spec.module_id.ends_with(".wasm") {
            return Err(AdaptorError::InvalidModuleId(spec.module_id.clone()));
        }

        if !self.supported_profiles().contains(&spec.profile) {
            return Err(AdaptorError::UnsupportedProfile(spec.profile));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use selium_runtime_adaptor_spi::ExecutionProfile;

    use super::*;

    #[test]
    fn validates_wasm_spec() {
        let adaptor = WasmtimeAdaptor;
        let spec = ModuleSpec {
            module_id: "mods/echo.wasm".to_string(),
            entrypoint: "start".to_string(),
            capabilities: Vec::new(),
            profile: ExecutionProfile::Standard,
        };
        adaptor.validate(&spec).expect("valid spec");
    }
}
