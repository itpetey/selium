//! Wait for host wake signals.
//!
//! On WASM, this waits on the mailbox flag and drains pending wakes.
//! On non-WASM (testing), this is a no-op.

#[cfg(target_arch = "wasm32")]
mod wasm_wait {
    use selium_abi::mailbox::{CAPACITY, FLAG_OFFSET, HEAD_OFFSET, SLOT_SIZE, TAIL_OFFSET};

    #[link(wasm_import_module = "selium::async")]
    unsafe extern "C" {
        fn park();
    }

    /// Check if the mailbox flag is set.
    #[inline(always)]
    unsafe fn flag_set() -> bool {
        let flag = (FLAG_OFFSET as *const u32).read();
        flag != 0
    }

    /// Read a task ID from the ring buffer.
    #[inline(always)]
    unsafe fn read_ring(slot: usize) -> u32 {
        let ptr = (crate::async_::RING_OFFSET + slot) as *const u32;
        ptr.read()
    }

    /// Drain the mailbox ring buffer and wake tasks.
    unsafe fn drain() {
        let mut head = {
            let ptr = HEAD_OFFSET as *const u32;
            ptr.read()
        };
        let tail = {
            let ptr = TAIL_OFFSET as *const u32;
            ptr.read()
        };

        while head != tail {
            let slot = ((head % CAPACITY) as usize) * SLOT_SIZE;
            let id = read_ring(slot);

            // Wake the task
            let waker = Box::from_raw(id as *mut std::task::Waker);
            waker.wake();

            head = head.wrapping_add(1);
        }

        // Update head and clear flag
        {
            let ptr = HEAD_OFFSET as *mut u32;
            ptr.write(head);
        }
        {
            let ptr = FLAG_OFFSET as *mut u32;
            ptr.write(0);
        }
    }

    /// Wait for a host wake signal.
    pub fn wait() {
        unsafe {
            if flag_set() {
                drain();
            } else {
                park();
                if flag_set() {
                    drain();
                }
            }
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
mod host_wait {
    pub fn wait() {
        // On host, we don't wait (the executor polls)
        std::hint::spin_loop();
    }
}

/// Wait for the host to enqueue wake signals.
pub fn wait() {
    #[cfg(target_arch = "wasm32")]
    wasm_wait::wait();

    #[cfg(not(target_arch = "wasm32"))]
    host_wait::wait();
}
