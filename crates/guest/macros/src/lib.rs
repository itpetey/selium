use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{ItemFn, ItemTrait, ReturnType, parse_macro_input};

#[proc_macro_attribute]
pub fn entrypoint(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let function = parse_macro_input!(item as ItemFn);
    if function.sig.asyncness.is_none() {
        return syn::Error::new_spanned(
            function.sig.fn_token,
            "#[entrypoint] requires an async function",
        )
        .to_compile_error()
        .into();
    }
    if !function.sig.inputs.is_empty() {
        return syn::Error::new_spanned(
            &function.sig.inputs,
            "#[entrypoint] does not support function arguments",
        )
        .to_compile_error()
        .into();
    }
    if !function.sig.generics.params.is_empty() {
        return syn::Error::new_spanned(
            &function.sig.generics,
            "#[entrypoint] does not support generic parameters",
        )
        .to_compile_error()
        .into();
    }
    if !matches!(function.sig.output, ReturnType::Default) {
        return syn::Error::new_spanned(
            &function.sig.output,
            "#[entrypoint] requires an async function returning ()",
        )
        .to_compile_error()
        .into();
    }
    let ident = function.sig.ident.clone();
    let metadata_fn = format_ident!("{}_entrypoint_metadata", ident);
    let export_ident = format_ident!("__selium_guest_entrypoint_{}", ident);
    let export_name = ident.to_string();

    quote! {
        #function

        #[unsafe(export_name = #export_name)]
        pub extern "C" fn #export_ident() {
            ::selium_guest::run_entrypoint_safely(#ident());
        }

        pub fn #metadata_fn() -> ::selium_guest::EntrypointMetadata {
            ::selium_guest::EntrypointMetadata::new(stringify!(#ident))
        }
    }
    .into()
}

#[proc_macro_attribute]
pub fn pattern_interface(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let interface = parse_macro_input!(item as ItemTrait);
    let ident = interface.ident.clone();
    let metadata_fn = format_ident!("{}_pattern_metadata", ident.to_string().to_lowercase());
    let methods = interface
        .items
        .iter()
        .filter_map(|item| match item {
            syn::TraitItem::Fn(method) => Some(method.sig.ident.to_string()),
            _ => None,
        })
        .collect::<Vec<_>>();

    quote! {
        #interface

        pub fn #metadata_fn() -> ::selium_guest::InterfaceMetadata {
            ::selium_guest::InterfaceMetadata::new(
                stringify!(#ident),
                vec![#(::std::string::String::from(#methods)),*],
            )
        }
    }
    .into()
}
