/*
 * Copyright (C) 2026 Selium Labs. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
 */

//! Shared heap management for attaching host-managed memory to WAMR instances.

use std::{ffi::c_void, ptr::NonNull};

use wamr_sys::{
    wasm_runtime_chain_shared_heaps, wasm_runtime_create_shared_heap, wasm_shared_heap_t,
    SharedHeapInitArgs,
};

use crate::RuntimeError;

/// Handle to a WAMR shared heap or shared heap chain.
#[derive(Clone, Copy, Debug)]
pub struct SharedHeap {
    shared_heap: wasm_shared_heap_t,
}

impl SharedHeap {
    /// Create a runtime-managed shared heap with the requested size in bytes.
    pub fn create(size: u32) -> Result<Self, RuntimeError> {
        let mut init_args = SharedHeapInitArgs {
            size,
            ..SharedHeapInitArgs::default()
        };

        let shared_heap = unsafe { wasm_runtime_create_shared_heap(&mut init_args) };
        if shared_heap.is_null() {
            return Err(RuntimeError::SharedHeapFailure(
                "create shared heap failed".to_string(),
            ));
        }

        Ok(Self { shared_heap })
    }

    /// Create a shared heap from a pre-allocated host buffer.
    pub fn create_from_preallocated(buffer: &mut [u8]) -> Result<Self, RuntimeError> {
        let size = u32::try_from(buffer.len()).map_err(|_| {
            RuntimeError::SharedHeapFailure("pre-allocated buffer is too large".to_string())
        })?;
        let pre_allocated_addr =
            NonNull::new(buffer.as_mut_ptr().cast::<c_void>()).ok_or_else(|| {
                RuntimeError::SharedHeapFailure("pre-allocated buffer is empty".to_string())
            })?;

        let mut init_args = SharedHeapInitArgs {
            pre_allocated_addr: pre_allocated_addr.as_ptr(),
            size,
        };

        let shared_heap = unsafe { wasm_runtime_create_shared_heap(&mut init_args) };
        if shared_heap.is_null() {
            return Err(RuntimeError::SharedHeapFailure(
                "create pre-allocated shared heap failed".to_string(),
            ));
        }

        Ok(Self { shared_heap })
    }

    /// Chain two shared heaps together as a single address range for wasm modules.
    pub fn chain(head: &Self, body: &Self) -> Result<Self, RuntimeError> {
        let shared_heap =
            unsafe { wasm_runtime_chain_shared_heaps(head.shared_heap, body.shared_heap) };
        if shared_heap.is_null() {
            return Err(RuntimeError::SharedHeapFailure(
                "create shared heap chain failed".to_string(),
            ));
        }
        Ok(Self { shared_heap })
    }

    pub(crate) fn get_inner_shared_heap(&self) -> wasm_shared_heap_t {
        self.shared_heap
    }
}

#[cfg(all(test, feature = "shared-heap"))]
mod tests {
    use crate::{
        function::Function, instance::Instance, module::Module, runtime::Runtime, value::WasmValue,
    };

    use super::SharedHeap;

    const PRODUCER_WAT: &str = r#"
        (module
            (import "env" "shared_heap_malloc" (func $shared_heap_malloc (param i32) (result i32)))
            (memory (export "memory") 1)
            (func (export "produce") (result i32)
                (local $ptr i32)
                i32.const 4
                call $shared_heap_malloc
                local.tee $ptr
                i32.const 305419896
                i32.store
                local.get $ptr))
    "#;

    const CONSUMER_WAT: &str = r#"
        (module
            (memory (export "memory") 1)
            (func (export "consume") (param i32) (result i32)
                local.get 0
                i32.load))
    "#;

    const INITIAL_VALUE: i32 = 305_419_896;
    const UPDATED_VALUE: u32 = 2_596_069_104;

    #[test]
    fn shared_heap_data_is_visible_across_instances_and_host() {
        let runtime = Runtime::builder().use_system_allocator().build().unwrap();

        let producer_module = Module::from_vec(
            &runtime,
            wat::parse_str(PRODUCER_WAT).unwrap(),
            "shared_heap_producer",
        )
        .unwrap();
        let consumer_module = Module::from_vec(
            &runtime,
            wat::parse_str(CONSUMER_WAT).unwrap(),
            "shared_heap_consumer",
        )
        .unwrap();

        let producer = Instance::new(&runtime, &producer_module, 64 * 1024).unwrap();
        let consumer = Instance::new(&runtime, &consumer_module, 64 * 1024).unwrap();

        let shared_heap = SharedHeap::create(64 * 1024).unwrap();
        producer.attach_shared_heap(&shared_heap).unwrap();
        consumer.attach_shared_heap(&shared_heap).unwrap();

        let produce_fn = Function::find_export_func(&producer, "produce").unwrap();
        let produced = produce_fn.call(&producer, &vec![]).unwrap();
        let app_offset = match produced.first() {
            Some(WasmValue::I32(offset)) => *offset as u32,
            other => panic!("unexpected produce result: {other:?}"),
        };

        let consume_fn = Function::find_export_func(&consumer, "consume").unwrap();
        let consumed = consume_fn
            .call(&consumer, &vec![WasmValue::I32(app_offset as i32)])
            .unwrap();
        assert_eq!(consumed, vec![WasmValue::I32(INITIAL_VALUE)]);

        let native_ptr = producer.app_addr_to_native(u64::from(app_offset)).unwrap();
        unsafe {
            *(native_ptr.cast::<u32>()) = UPDATED_VALUE;
        }

        let consumed_after_host_write = consume_fn
            .call(&consumer, &vec![WasmValue::I32(app_offset as i32)])
            .unwrap();
        assert_eq!(
            consumed_after_host_write,
            vec![WasmValue::I32(UPDATED_VALUE as i32)]
        );

        producer.shared_heap_free(u64::from(app_offset));
        producer.detach_shared_heap();
        consumer.detach_shared_heap();
    }
}
