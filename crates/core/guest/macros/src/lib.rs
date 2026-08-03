//! Procedural macros for Selium guest entrypoints and pattern metadata.

#![allow(
    clippy::collapsible_if,
    clippy::map_err_ignore,
    clippy::panic,
    clippy::type_complexity,
    clippy::unreachable,
    clippy::unwrap_in_result,
    clippy::unwrap_used,
    reason = "proc-macro helpers intentionally panic on malformed input"
)]

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{GenericArgument, ItemFn, ItemTrait, PathArguments, ReturnType, Type, parse_macro_input};

mod schema;

/// Whether the entrypoint returns `()` or `Result<()>`.
#[derive(Clone, Copy, PartialEq, Eq)]
enum EntrypointReturn {
    Unit,
    ResultUnit,
}

/// Inspects the return type to determine whether it is `()`, `Result<()>`, or
/// unsupported. Returns `None` for unsupported types.
fn inspect_return_type(output: &ReturnType) -> Option<EntrypointReturn> {
    match output {
        ReturnType::Default => Some(EntrypointReturn::Unit),
        ReturnType::Type(_, ty) => {
            if is_result_of_unit(ty) {
                Some(EntrypointReturn::ResultUnit)
            } else {
                None
            }
        }
    }
}

/// Returns true if `ty` is `Result<()>` or `Result<(), E>` (accepts both the
/// two-argument form and single-argument aliases such as `anyhow::Result<()>`
/// whose error type has a default).
fn is_result_of_unit(ty: &Type) -> bool {
    if let Type::Path(type_path) = ty {
        let segments = &type_path.path.segments;
        if let Some(last) = segments.last() {
            if last.ident != "Result" {
                return false;
            }
            if let PathArguments::AngleBracketed(args) = &last.arguments {
                // `Result<(), E>` (two args) or `Result<()>` (alias with default error)
                if matches!(args.args.len(), 1 | 2) {
                    if let Some(GenericArgument::Type(inner_ty)) = args.args.first() {
                        return is_unit_tuple(inner_ty);
                    }
                }
            }
        }
    }
    false
}

/// Returns true if `ty` is the unit type `()`.
fn is_unit_tuple(ty: &Type) -> bool {
    matches!(ty, Type::Tuple(tuple) if tuple.elems.is_empty())
}

/// Marks a function as an exported Selium guest entrypoint.
///
/// Accepts entrypoints returning `()` or `Result<()>` with 0–2 `u64` parameters
/// or a single `Context` parameter, in both sync and async variants.
#[proc_macro_attribute]
pub fn entrypoint(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let function = parse_macro_input!(item as ItemFn);
    if function.sig.inputs.len() > 2 {
        return syn::Error::new_spanned(
            &function.sig.inputs,
            "#[entrypoint] supports at most two arguments",
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

    let return_kind = match inspect_return_type(&function.sig.output) {
        Some(kind) => kind,
        None => {
            return syn::Error::new_spanned(
                &function.sig.output,
                "#[entrypoint] return type must be `()` or `Result<()>`",
            )
            .to_compile_error()
            .into();
        }
    };

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

    let init_call = quote! {
        ::selium_guest::init().expect("failed to install Selium guest runtime");
    };

    let generated = match (function.sig.inputs.len(), param_kind.as_deref()) {
        (0, _) => generate_zero_param(
            &function,
            return_kind,
            is_async,
            &ident,
            &export_name,
            &export_ident,
            &init_call,
        ),
        (1, Some("Context")) => generate_context_param(
            &function,
            return_kind,
            is_async,
            &ident,
            &export_name,
            &export_ident,
            &init_call,
        ),
        (1, _) => generate_one_param(
            &function,
            return_kind,
            is_async,
            &ident,
            &export_name,
            &export_ident,
            &init_call,
        ),
        (2, _) => generate_two_params(
            &function,
            return_kind,
            is_async,
            &ident,
            &export_name,
            &export_ident,
            &init_call,
        ),
        _ => {
            return syn::Error::new_spanned(
                &function.sig.inputs,
                "#[entrypoint] supports at most two arguments",
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

/// Generates the call expression for the user function, optionally
/// discarding the result (for `()` return) or returning it (for `Result<()>`).
fn make_call(
    _is_async: bool,
    return_kind: EntrypointReturn,
    call_tokens: proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    match return_kind {
        EntrypointReturn::Unit => quote!(#call_tokens;),
        EntrypointReturn::ResultUnit => call_tokens,
    }
}

/// Generates the extern wrapper body that invokes the user function and
/// optionally matches on `Result<()>` to return `i32`.
fn make_wrapper_body(
    return_kind: EntrypointReturn,
    inner_block: proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    match return_kind {
        EntrypointReturn::Unit => {
            quote! {
                ::selium_guest::run_entrypoint_safely(async move {
                    #inner_block
                });
            }
        }
        EntrypointReturn::ResultUnit => {
            quote! {
                let result = ::selium_guest::run_entrypoint_with_result(async move {
                    #inner_block
                });
                fn __selium_guest_assert_error<E: ::core::fmt::Display>(_: &::core::result::Result<(), E>) {}
                __selium_guest_assert_error(&result);
                match result {
                    ::core::result::Result::Ok(()) => 0,
                    ::core::result::Result::Err(e) => {
                        ::selium_guest::error!("{e}");
                        1
                    }
                }
            }
        }
    }
}

/// Returns the return type suffix for the extern "C" signature: nothing for
/// `()`, `-> i32` for `Result<()>`.
fn extern_return_type(return_kind: EntrypointReturn) -> proc_macro2::TokenStream {
    match return_kind {
        EntrypointReturn::Unit => quote!(),
        EntrypointReturn::ResultUnit => quote!(-> i32),
    }
}

fn generate_zero_param(
    function: &ItemFn,
    return_kind: EntrypointReturn,
    is_async: bool,
    ident: &syn::Ident,
    export_name: &str,
    export_ident: &syn::Ident,
    init_call: &proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let call_tokens = if is_async {
        quote!(#ident().await)
    } else {
        quote!(#ident())
    };
    let call = make_call(is_async, return_kind, call_tokens);
    let body = make_wrapper_body(return_kind, call);
    let ret = extern_return_type(return_kind);

    quote! {
        #function

        #[unsafe(export_name = #export_name)]
        pub extern "C" fn #export_ident() #ret {
            #init_call
            #body
        }
    }
}

fn generate_one_param(
    function: &ItemFn,
    return_kind: EntrypointReturn,
    is_async: bool,
    ident: &syn::Ident,
    export_name: &str,
    export_ident: &syn::Ident,
    init_call: &proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let call_tokens = if is_async {
        quote!(#ident(handle as u64).await)
    } else {
        quote!(#ident(handle as u64))
    };
    let call = make_call(is_async, return_kind, call_tokens);
    let body = make_wrapper_body(return_kind, call);
    let ret = extern_return_type(return_kind);

    quote! {
        #function

        #[unsafe(export_name = #export_name)]
        pub extern "C" fn #export_ident(handle: i64) #ret {
            #init_call
            #body
        }
    }
}

fn generate_context_param(
    function: &ItemFn,
    return_kind: EntrypointReturn,
    is_async: bool,
    ident: &syn::Ident,
    export_name: &str,
    export_ident: &syn::Ident,
    init_call: &proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let call_tokens = if is_async {
        quote!(#ident(ctx).await)
    } else {
        quote!(#ident(ctx))
    };
    let call = make_call(is_async, return_kind, call_tokens);
    let body = make_wrapper_body(
        return_kind,
        quote! {
            let ctx = ::selium_guest::Context::from_raw(discovery_handle as u64)
                .await
                .expect("failed to construct bootstrap context");
            #call
        },
    );
    let ret = extern_return_type(return_kind);

    quote! {
        #function

        #[unsafe(export_name = #export_name)]
        pub extern "C" fn #export_ident(discovery_handle: i64) #ret {
            #init_call
            #body
        }
    }
}

fn generate_two_params(
    function: &ItemFn,
    return_kind: EntrypointReturn,
    is_async: bool,
    ident: &syn::Ident,
    export_name: &str,
    export_ident: &syn::Ident,
    init_call: &proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let call_tokens = if is_async {
        quote!(#ident(arg0 as u64, arg1 as u64).await)
    } else {
        quote!(#ident(arg0 as u64, arg1 as u64))
    };
    let call = make_call(is_async, return_kind, call_tokens);
    let body = make_wrapper_body(return_kind, call);
    let ret = extern_return_type(return_kind);

    quote! {
        #function

        #[unsafe(export_name = #export_name)]
        pub extern "C" fn #export_ident(arg0: i64, arg1: i64) #ret {
            #init_call
            #body
        }
    }
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

/// Struct-level schema annotation declaring a message type.
#[proc_macro_attribute]
pub fn schema(attr: TokenStream, item: TokenStream) -> TokenStream {
    schema::expand(attr, item)
}
