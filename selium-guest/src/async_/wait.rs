//! Wait for host wake signals.
//!
//! On WASM, this waits on the mailbox flag and drains pending wakes.
//! On non-WASM (testing), this is a no-op.

#[cfg(target_arch = "wasm32")]
mod wasm_wait {
    #[link(wasm_import_module = "selium::async")]
    unsafe extern "C" {
        fn park();
    }

    pub fn wait() {
        unsafe {
            park();
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
mod host_wait {
    pub fn wait() {
        std::hint::spin_loop();
    }
}

pub fn wait() {
    #[cfg(target_arch = "wasm32")]
    wasm_wait::wait();

    #[cfg(not(target_arch = "wasm32"))]
    host_wait::wait();
}
