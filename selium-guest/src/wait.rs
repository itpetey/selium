pub use selium_guest_macros::wait;

mod selium_guest_macros {
    /// Wait for host wake signal.
    #[proc_macro]
    pub fn wait(_item: TokenStream) -> TokenStream {
        quote! {
            selium_guest::async_::wait
        }
    }
}
