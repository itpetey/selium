use proc_macro::TokenStream;

mod entrypoint;

/// Marks a top-level guest function as a Selium runtime entrypoint.
///
/// The macro keeps your Rust function body intact and generates the exported ABI wrapper that
/// the runtime calls. In guest modules, this is usually used through the re-exported
/// [`selium_guest::entrypoint`](https://docs.rs/selium-guest/latest/selium_guest/attr.entrypoint.html)
/// path.
///
/// # Example
///
/// ```
/// use selium_guest_macros::entrypoint;
///
/// #[entrypoint]
/// fn launch(service: &str) -> Result<(), std::convert::Infallible> {
///     let _ = service;
///     Ok(())
/// }
///
/// fn main() {}
/// ```
///
/// Supported entrypoints are free functions that:
///
/// - may be synchronous or `async fn`
/// - use zero or more named parameters with identifier patterns
/// - return `()` or `Result<(), E>`
///
/// Important constraints:
///
/// - `#[entrypoint]` takes no attribute arguments
/// - generic functions, receiver arguments, and `Context` parameters are rejected
/// - scalar values and raw pointers are forwarded directly; `&str` and other typed values are
///   decoded from the inbound payload before your function runs
#[proc_macro_attribute]
pub fn entrypoint(attr: TokenStream, item: TokenStream) -> TokenStream {
    entrypoint::expand(attr, item)
}
