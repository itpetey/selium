//! Async host function implementations for WASM guests.
//!
//! These functions are exported to guests via the `selium::async` import namespace.
//!
//! Note: These are synchronous wrappers. The actual async behavior (parking,
//! waking, shutdown signaling) is driven by the host's poll loop and shared state.

use wasmtime::Linker;

pub fn add_to_linker<T: Send + 'static>(linker: &mut Linker<T>) -> anyhow::Result<()> {
    linker.func_wrap("selium::async", "park", || {})?;
    linker.func_wrap("selium::async", "yield_now", || {})?;
    linker.func_wrap("selium::async", "wait_for_shutdown", || {})?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use wasmtime::Engine;

    #[test]
    fn test_add_to_linker() {
        let engine = Engine::default();
        let mut linker: Linker<()> = Linker::new(&engine);
        add_to_linker(&mut linker).unwrap();
    }
}
