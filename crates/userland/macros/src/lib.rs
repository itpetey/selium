use proc_macro::TokenStream;

mod dependency_id;
mod entrypoint;

/// Compute a singleton dependency identifier from a string literal.
#[proc_macro]
pub fn dependency_id(item: TokenStream) -> TokenStream {
    dependency_id::expand(item)
}

#[proc_macro_attribute]
pub fn entrypoint(attr: TokenStream, item: TokenStream) -> TokenStream {
    entrypoint::expand(attr, item)
}
