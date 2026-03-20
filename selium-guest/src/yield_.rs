pub use selium_guest_macros::yield_now;

mod selium_guest_macros {
    /// Yield to allow other tasks to progress.
    #[proc_macro]
    pub fn yield_now(_item: TokenStream) -> TokenStream {
        quote! {
            selium_guest::async_::yield_now
        }
    }
}
