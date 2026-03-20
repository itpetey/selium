#[cfg(target_arch = "wasm32")]
mod wasm {
    #[link(wasm_import_module = "selium::time")]
    extern "C" {
        pub fn now() -> u64;
        pub fn monotonic() -> u64;
    }
}

#[cfg(target_arch = "wasm32")]
pub fn time_now() -> u64 {
    unsafe { wasm::now() }
}

#[cfg(target_arch = "wasm32")]
pub fn time_monotonic() -> u64 {
    unsafe { wasm::monotonic() }
}

#[cfg(not(target_arch = "wasm32"))]
pub fn time_now() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

#[cfg(not(target_arch = "wasm32"))]
pub fn time_monotonic() -> u64 {
    use std::time::Instant;
    static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    START.get_or_init(Instant::now).elapsed().as_nanos() as u64
}
