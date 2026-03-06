//! Escape-hatch runtime adaptor surface for microVM execution.

use selium_runtime_adaptor_spi::{
    AdaptorError, AdaptorKind, ExecutionProfile, ModuleSpec, RuntimeAdaptor,
};

pub struct MicroVmAdaptor;

impl RuntimeAdaptor for MicroVmAdaptor {
    fn kind(&self) -> AdaptorKind {
        AdaptorKind::Microvm
    }

    fn adaptor_name(&self) -> &'static str {
        "microvm"
    }

    fn supported_profiles(&self) -> &'static [ExecutionProfile] {
        &[ExecutionProfile::Microvm]
    }

    fn validate(&self, spec: &ModuleSpec) -> Result<(), AdaptorError> {
        if spec.module_id.trim().is_empty() {
            return Err(AdaptorError::NotConfigured);
        }
        if !self.supported_profiles().contains(&spec.profile) {
            return Err(AdaptorError::UnsupportedProfile(spec.profile));
        }

        if !(spec.module_id.ends_with(".oci") || spec.module_id.ends_with(".vm")) {
            return Err(AdaptorError::InvalidModuleId(spec.module_id.clone()));
        }

        Ok(())
    }

    fn executable(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use selium_runtime_adaptor_spi::ExecutionProfile;

    use super::*;

    #[test]
    fn validates_image_extension() {
        let adaptor = MicroVmAdaptor;
        let spec = ModuleSpec {
            module_id: "images/control-plane.oci".to_string(),
            entrypoint: "init".to_string(),
            capabilities: Vec::new(),
            profile: ExecutionProfile::Microvm,
        };
        adaptor.validate(&spec).expect("valid microvm spec");
        assert!(!adaptor.executable());
    }
}
