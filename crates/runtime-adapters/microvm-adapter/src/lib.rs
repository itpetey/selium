//! Escape-hatch runtime adapter surface for microVM execution.

use selium_runtime_adapter_spi::{
    AdapterError, AdapterKind, ExecutionProfile, ModuleSpec, RuntimeAdapter,
};

pub struct MicroVmAdapter;

impl RuntimeAdapter for MicroVmAdapter {
    fn kind(&self) -> AdapterKind {
        AdapterKind::Microvm
    }

    fn adapter_name(&self) -> &'static str {
        "microvm"
    }

    fn supported_profiles(&self) -> &'static [ExecutionProfile] {
        &[ExecutionProfile::Microvm]
    }

    fn validate(&self, spec: &ModuleSpec) -> Result<(), AdapterError> {
        if spec.module_id.trim().is_empty() {
            return Err(AdapterError::NotConfigured);
        }
        if !self.supported_profiles().contains(&spec.profile) {
            return Err(AdapterError::UnsupportedProfile(spec.profile));
        }

        if !(spec.module_id.ends_with(".oci") || spec.module_id.ends_with(".vm")) {
            return Err(AdapterError::InvalidModuleId(spec.module_id.clone()));
        }

        Ok(())
    }

    fn executable(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use selium_runtime_adapter_spi::ExecutionProfile;

    use super::*;

    #[test]
    fn validates_image_extension() {
        let adapter = MicroVmAdapter;
        let spec = ModuleSpec {
            module_id: "images/control-plane.oci".to_string(),
            entrypoint: "init".to_string(),
            capabilities: Vec::new(),
            profile: ExecutionProfile::Microvm,
        };
        adapter.validate(&spec).expect("valid microvm spec");
        assert!(!adapter.executable());
    }
}
