use std::{
    any::{Any, TypeId},
    collections::HashMap,
    num::TryFromIntError,
    sync::Arc,
};

use thiserror::Error;

use crate::registry::RegistryError;

pub mod r#async;
pub mod guest_error;
pub mod hostcalls;
pub mod registry;
pub mod services;
pub mod spi;

pub struct Kernel {
    capabilities: HashMap<TypeId, Arc<dyn Any>>,
}

#[derive(Default)]
pub struct KernelBuilder {
    capabilities: HashMap<TypeId, Arc<dyn Any>>,
}

#[derive(Error, Debug)]
pub enum KernelError {
    #[error("Engine error: {0}")]
    Engine(String),
    #[error("Could not access guest memory")]
    MemoryAccess(String),
    #[error("Guest did not reserve enough memory for this call")]
    MemoryCapacity,
    #[error("Could not retrieve guest memory from `Caller`")]
    MemoryMissing,
    #[error("Could not convert int to usize")]
    IntConvert(#[from] TryFromIntError),
    #[error("Invalid resource handle provided by guest")]
    InvalidHandle,
    #[error("Registry error")]
    Registry(#[from] RegistryError),
    #[error("Driver error: {0}")]
    Driver(String),
}

impl Kernel {
    pub fn build() -> KernelBuilder {
        KernelBuilder::default()
    }

    pub fn get<C: 'static>(&self) -> Option<&C> {
        self.capabilities
            .get(&TypeId::of::<C>())
            .and_then(|cap| cap.downcast_ref::<C>())
    }
}

impl KernelBuilder {
    pub fn add_capability<C: 'static>(&mut self, capability: Arc<C>) -> Arc<C> {
        self.capabilities
            .insert(TypeId::of::<C>(), capability.clone());
        capability
    }

    pub fn build(self) -> Result<Kernel, KernelError> {
        Ok(Kernel {
            capabilities: self.capabilities,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct CapabilityA(u32);

    #[test]
    fn kernel_builder_stores_and_recovers_capabilities_by_type() {
        let mut builder = Kernel::build();
        let cap = builder.add_capability(Arc::new(CapabilityA(7)));
        assert_eq!(cap.0, 7);

        let kernel = builder.build().expect("build kernel");
        let recovered = kernel.get::<CapabilityA>().expect("capability present");
        assert_eq!(recovered.0, 7);
    }
}
