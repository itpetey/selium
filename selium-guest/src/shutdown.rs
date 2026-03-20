pub use selium_guest_macros::shutdown;

mod selium_guest_macros {
    /// Wait for host shutdown signal.
    #[proc_macro]
    pub fn shutdown(_item: TokenStream) -> TokenStream {
        quote! {
            selium_guest::async_::shutdown
        }
    }
}
