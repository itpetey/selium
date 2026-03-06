use proc_macro::TokenStream;

mod entrypoint;

#[proc_macro_attribute]
pub fn entrypoint(attr: TokenStream, item: TokenStream) -> TokenStream {
    entrypoint::expand(attr, item)
}
