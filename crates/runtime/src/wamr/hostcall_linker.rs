//! WAMR adapter that bridges kernel hostcall operations.

use std::sync::Arc;

use selium_abi::{GuestUint, decode_rkyv};
use selium_kernel::{
    KernelError,
    guest_error::GuestResult,
    hostcalls::{Contract, HostcallContext, Operation as KernelOperation},
};

/// Trait object for hostcall operations that can be dispatched from WAMR native bindings.
pub trait WamrHostcallOperation: Send + Sync {
    /// Return the hostcall module namespace.
    fn module(&self) -> &'static str;

    /// Decode and create a hostcall state from raw guest input bytes.
    fn create(
        &self,
        context: &mut dyn HostcallContext,
        args: &[u8],
    ) -> Result<GuestUint, KernelError>;

    /// Poll a previously created hostcall state.
    fn poll(
        &self,
        context: &mut dyn HostcallContext,
        state_id: GuestUint,
        task_id: GuestUint,
    ) -> Result<GuestResult<Vec<u8>>, KernelError>;

    /// Drop a previously created hostcall state.
    fn drop_state(
        &self,
        context: &mut dyn HostcallContext,
        state_id: GuestUint,
    ) -> Result<GuestResult<Vec<u8>>, KernelError>;
}

/// Extension trait for turning kernel operations into WAMR-dispatchable operations.
pub trait WamrOperationExt {
    /// Convert this operation into a dispatch-ready trait object.
    fn as_wamr_operation(&self) -> Arc<dyn WamrHostcallOperation>;
}

struct WamrOperation<Driver> {
    operation: Arc<KernelOperation<Driver>>,
}

struct BorrowedHostcallContext<'a> {
    inner: &'a mut dyn HostcallContext,
}

impl HostcallContext for BorrowedHostcallContext<'_> {
    fn registry(&self) -> &selium_kernel::registry::InstanceRegistry {
        self.inner.registry()
    }

    fn registry_mut(&mut self) -> &mut selium_kernel::registry::InstanceRegistry {
        self.inner.registry_mut()
    }

    fn mailbox_base(&mut self) -> Option<usize> {
        self.inner.mailbox_base()
    }
}

impl<Driver> WamrHostcallOperation for WamrOperation<Driver>
where
    Driver: Contract + Send + Sync + 'static,
    for<'a> <Driver::Input as rkyv::Archive>::Archived: 'a
        + rkyv::Deserialize<Driver::Input, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>
        + rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
    for<'a> <Driver::Output as rkyv::Archive>::Archived: 'a
        + rkyv::Deserialize<Driver::Output, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>
        + rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
{
    fn module(&self) -> &'static str {
        self.operation.module()
    }

    fn create(
        &self,
        context: &mut dyn HostcallContext,
        args: &[u8],
    ) -> Result<GuestUint, KernelError> {
        let input =
            decode_rkyv(args).map_err(|err| KernelError::Driver(format!("decode input: {err}")))?;
        let mut wrapped = BorrowedHostcallContext { inner: context };
        self.operation.create_with_input(&mut wrapped, input)
    }

    fn poll(
        &self,
        context: &mut dyn HostcallContext,
        state_id: GuestUint,
        task_id: GuestUint,
    ) -> Result<GuestResult<Vec<u8>>, KernelError> {
        let mut wrapped = BorrowedHostcallContext { inner: context };
        self.operation.poll_state(&mut wrapped, state_id, task_id)
    }

    fn drop_state(
        &self,
        context: &mut dyn HostcallContext,
        state_id: GuestUint,
    ) -> Result<GuestResult<Vec<u8>>, KernelError> {
        let mut wrapped = BorrowedHostcallContext { inner: context };
        self.operation.drop_state(&mut wrapped, state_id)
    }
}

impl<Driver> WamrOperationExt for Arc<KernelOperation<Driver>>
where
    Driver: Contract + Send + Sync + 'static,
    for<'a> <Driver::Input as rkyv::Archive>::Archived: 'a
        + rkyv::Deserialize<Driver::Input, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>
        + rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
    for<'a> <Driver::Output as rkyv::Archive>::Archived: 'a
        + rkyv::Deserialize<Driver::Output, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>
        + rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
{
    fn as_wamr_operation(&self) -> Arc<dyn WamrHostcallOperation> {
        Arc::new(WamrOperation {
            operation: Arc::clone(self),
        })
    }
}
