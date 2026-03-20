pub use selium_guest_macros::spawn;

mod selium_guest_macros {
    /// Spawn a future as a background task.
    #[proc_macro]
    pub fn spawn(_item: TokenStream) -> TokenStream {
        quote! {
            selium_guest::async_::spawn
        }
    }

    /// Yield to allow other tasks to progress.
    #[proc_macro]
    pub fn yield_now(_item: TokenStream) -> TokenStream {
        quote! {
            selium_guest::async_::yield_now
        }
    }
}
