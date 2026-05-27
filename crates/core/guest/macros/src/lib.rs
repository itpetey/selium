//! Procedural macros for Selium guest entrypoints and pattern metadata.

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{ItemFn, ItemTrait, ReturnType, parse_macro_input};

/// Marks a function as an exported Selium guest entrypoint.
///
/// Accepts `fn entrypoint()`, `async fn entrypoint()`, `fn entrypoint(ctx: Context)`,
/// `async fn entrypoint(ctx: Context)`, or `fn entrypoint(handle: u64)`.
#[proc_macro_attribute]
pub fn entrypoint(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let function = parse_macro_input!(item as ItemFn);
    if function.sig.inputs.len() > 1 {
        return syn::Error::new_spanned(
            &function.sig.inputs,
            "#[entrypoint] supports at most one argument",
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
            "#[entrypoint] requires a function returning ()",
        )
        .to_compile_error()
        .into();
    }
    let ident = function.sig.ident.clone();
    let metadata_fn = format_ident!("{}_entrypoint_metadata", ident);
    let export_ident = format_ident!("__selium_guest_entrypoint_{}", ident);
    let export_name = ident.to_string();
    let is_async = function.sig.asyncness.is_some();

    let param_kind = function.sig.inputs.iter().next().and_then(|arg| {
        if let syn::FnArg::Typed(pat_type) = arg {
            if let syn::Type::Path(type_path) = &*pat_type.ty {
                type_path
                    .path
                    .segments
                    .last()
                    .map(|seg| seg.ident.to_string())
            } else {
                None
            }
        } else {
            None
        }
    });

    let generated = match (function.sig.inputs.len(), param_kind.as_deref()) {
        (0, _) => {
            let call = if is_async {
                quote!(#ident().await)
            } else {
                quote!(#ident())
            };
            quote! {
                #function

                #[unsafe(export_name = #export_name)]
                pub extern "C" fn #export_ident() {
                    ::selium_guest::run_entrypoint_safely(async move {
                        #call;
                    });
                }
            }
        }
        (1, Some("Context")) => {
            let call = if is_async {
                quote!(#ident(ctx).await)
            } else {
                quote!(#ident(ctx))
            };
            quote! {
                #function

                #[unsafe(export_name = #export_name)]
                pub extern "C" fn #export_ident(discovery_handle: i64) {
                    ::selium_guest::run_entrypoint_safely(async move {
                        let ctx = ::selium_guest::Context::from_raw(discovery_handle as u64)
                            .await
                            .expect("failed to construct bootstrap context");
                        #call;
                    });
                }
            }
        }
        (1, _) => {
            let call = if is_async {
                quote!(#ident(handle as u64).await)
            } else {
                quote!(#ident(handle as u64))
            };
            quote! {
                #function

                #[unsafe(export_name = #export_name)]
                pub extern "C" fn #export_ident(handle: i64) {
                    ::selium_guest::run_entrypoint_safely(async move {
                        #call;
                    });
                }
            }
        }
        _ => {
            return syn::Error::new_spanned(
                &function.sig.inputs,
                "#[entrypoint] supports at most one argument",
            )
            .to_compile_error()
            .into();
        }
    };

    quote! {
        #generated

        #[unsafe(export_name = "__selium_guest_poll")]
        pub extern "C" fn __selium_guest_poll() {
            ::selium_guest::poll_safely();
        }

        pub fn #metadata_fn() -> ::selium_guest::EntrypointMetadata {
            ::selium_guest::EntrypointMetadata::new(stringify!(#ident))
        }
    }
    .into()
}

/// Generates Selium pattern metadata for a trait interface.
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
