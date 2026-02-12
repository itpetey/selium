//! Guest helpers for registering and resolving singleton dependencies.

use selium_abi::{DependencyId, GuestResourceId, SingletonLookup, SingletonRegister};

use crate::driver::{DriverError, DriverFuture, RkyvDecoder, encode_args};

/// Register a shared resource handle under the supplied dependency identifier.
pub async fn register(id: DependencyId, resource: GuestResourceId) -> Result<(), DriverError> {
    let args = encode_args(&SingletonRegister { id, resource })?;
    DriverFuture::<singleton_register::Module, RkyvDecoder<()>>::new(&args, 0, RkyvDecoder::new())?
        .await?;
    Ok(())
}

/// Look up the shared resource handle registered for the dependency identifier.
pub async fn lookup(id: DependencyId) -> Result<GuestResourceId, DriverError> {
    let args = encode_args(&SingletonLookup { id })?;
    let handle = DriverFuture::<singleton_lookup::Module, RkyvDecoder<GuestResourceId>>::new(
        &args,
        8,
        RkyvDecoder::new(),
    )?
    .await?;
    Ok(handle)
}

driver_module!(
    singleton_register,
    SINGLETON_REGISTER,
    "selium::singleton::register"
);
driver_module!(
    singleton_lookup,
    SINGLETON_LOOKUP,
    "selium::singleton::lookup"
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_returns_kernel_error_with_native_stub_driver() {
        let err =
            crate::block_on(register(DependencyId([0; 16]), 1)).expect_err("stub should fail");
        assert!(matches!(err, DriverError::Kernel(2)));
    }

    #[test]
    fn lookup_returns_kernel_error_with_native_stub_driver() {
        let err = crate::block_on(lookup(DependencyId([1; 16]))).expect_err("stub should fail");
        assert!(matches!(err, DriverError::Kernel(2)));
    }
}
