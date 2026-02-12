//! Wasmtime adapter that links kernel hostcall operations.

use std::sync::Arc;

use selium_kernel::{
    KernelError,
    hostcalls::{Contract, HostcallContext, Operation as KernelOperation},
    registry::InstanceRegistry,
};
use wasmtime::{Caller, Linker};

use super::guest_data::{GuestInt, GuestUint, read_rkyv_value, write_poll_result};

/// Trait object for operations that can be linked into a Wasmtime linker.
pub trait LinkableOperation: Send + Sync {
    /// Link this operation into the provided linker.
    fn link(&self, linker: &mut Linker<InstanceRegistry>) -> Result<(), KernelError>;
}

/// Extension trait for turning kernel operations into Wasmtime-linkable operations.
pub trait WasmtimeOperationExt {
    /// Convert this operation into a linker-ready trait object.
    fn as_linkable(&self) -> Arc<dyn LinkableOperation>;
}

struct WasmtimeOperation<Driver> {
    operation: Arc<KernelOperation<Driver>>,
}

struct WasmtimeContext<'a, 'b> {
    caller: &'a mut Caller<'b, InstanceRegistry>,
}

impl<'a, 'b> WasmtimeContext<'a, 'b> {
    fn new(caller: &'a mut Caller<'b, InstanceRegistry>) -> Self {
        Self { caller }
    }
}

impl HostcallContext for WasmtimeContext<'_, '_> {
    fn registry(&self) -> &InstanceRegistry {
        self.caller.data()
    }

    fn registry_mut(&mut self) -> &mut InstanceRegistry {
        self.caller.data_mut()
    }

    fn mailbox_base(&mut self) -> Option<usize> {
        self.caller
            .get_export("memory")
            .and_then(|export| export.into_memory())
            .map(|memory| memory.data_ptr(&mut *self.caller) as usize)
    }
}

impl<Driver> LinkableOperation for WasmtimeOperation<Driver>
where
    Driver: Contract + Send + Sync + 'static,
    for<'a> <Driver::Input as rkyv::Archive>::Archived: 'a
        + rkyv::Deserialize<Driver::Input, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>
        + rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
    for<'a> <Driver::Output as rkyv::Archive>::Archived: 'a
        + rkyv::Deserialize<Driver::Output, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>
        + rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
{
    fn link(&self, linker: &mut Linker<InstanceRegistry>) -> Result<(), KernelError> {
        let module = self.operation.module();

        let create_op = Arc::clone(&self.operation);
        linker
            .func_wrap(
                module,
                "create",
                move |mut caller: Caller<'_, InstanceRegistry>,
                      args_ptr: GuestInt,
                      args_len: GuestUint| {
                    let input = read_rkyv_value::<Driver::Input>(&mut caller, args_ptr, args_len)?;
                    let mut context = WasmtimeContext::new(&mut caller);
                    create_op
                        .create_with_input(&mut context, input)
                        .map_err(Into::into)
                },
            )
            .map_err(|err| KernelError::Engine(err.to_string()))?;

        let poll_op = Arc::clone(&self.operation);
        linker
            .func_wrap(
                module,
                "poll",
                move |mut caller: Caller<'_, InstanceRegistry>,
                      state_id: GuestUint,
                      task_id: GuestUint,
                      result_ptr: GuestInt,
                      result_capacity: GuestUint| {
                    let guest_result = {
                        let mut context = WasmtimeContext::new(&mut caller);
                        poll_op.poll_state(&mut context, state_id, task_id)?
                    };
                    write_poll_result(&mut caller, result_ptr, result_capacity, guest_result)
                        .map_err(Into::into)
                },
            )
            .map_err(|err| KernelError::Engine(err.to_string()))?;

        let drop_op = Arc::clone(&self.operation);
        linker
            .func_wrap(
                module,
                "drop",
                move |mut caller: Caller<'_, InstanceRegistry>,
                      state_id: GuestUint,
                      result_ptr: GuestInt,
                      result_capacity: GuestUint| {
                    let guest_result = {
                        let mut context = WasmtimeContext::new(&mut caller);
                        drop_op.drop_state(&mut context, state_id)?
                    };
                    write_poll_result(&mut caller, result_ptr, result_capacity, guest_result)
                        .map_err(Into::into)
                },
            )
            .map_err(|err| KernelError::Engine(err.to_string()))?;

        Ok(())
    }
}

impl<Driver> WasmtimeOperationExt for Arc<KernelOperation<Driver>>
where
    Driver: Contract + Send + Sync + 'static,
    for<'a> <Driver::Input as rkyv::Archive>::Archived: 'a
        + rkyv::Deserialize<Driver::Input, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>
        + rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
    for<'a> <Driver::Output as rkyv::Archive>::Archived: 'a
        + rkyv::Deserialize<Driver::Output, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>
        + rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
{
    fn as_linkable(&self) -> Arc<dyn LinkableOperation> {
        Arc::new(WasmtimeOperation {
            operation: Arc::clone(self),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use selium_kernel::hostcalls::Operation;

    struct NoopContract;

    impl Contract for NoopContract {
        type Input = ();
        type Output = ();

        #[allow(clippy::manual_async_fn)]
        fn to_future<C>(
            &self,
            _context: &mut C,
            _input: Self::Input,
        ) -> impl std::future::Future<
            Output = selium_kernel::guest_error::GuestResult<Self::Output>,
        > + Send
        + 'static
        where
            C: HostcallContext,
        {
            async { Ok(()) }
        }
    }

    #[test]
    fn linkable_operation_registers_create_poll_and_drop_functions() {
        let engine = wasmtime::Engine::default();
        let mut linker = Linker::<InstanceRegistry>::new(&engine);
        let op = Operation::new(NoopContract, "selium::test::noop");
        let linkable = op.as_linkable();

        linkable.link(&mut linker).expect("link operation");
    }
}
