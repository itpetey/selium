use super::classify::{
    has_schema_codec, is_scalar_ident, nested_inner_of_field, option_inner, vec_inner,
    vec_inner_of_field,
};
use super::expand::encoding_path;
use super::*;

/// Returns the decode expression for one variant field. Skipped fields decode
/// to their `Default` value; required nested fields decode strictly.
pub(super) fn decode_data_field(field: &DataField) -> proc_macro2::TokenStream {
    let wire_ident = format_ident!("{}", field.wire_name);
    let wire_name_lit = syn::LitStr::new(&field.wire_name, proc_macro2::Span::call_site());
    let enc = encoding_path();
    match field.kind {
        DataFieldKind::Skip => quote! { ::core::default::Default::default() },
        DataFieldKind::String => quote! { #enc::StringFieldValue::into_owned(view.#wire_ident()) },
        DataFieldKind::Scalar => quote! { view.#wire_ident() },
        DataFieldKind::Bytes => {
            quote! { view.#wire_ident().map(|value| value.bytes().to_vec()).unwrap_or_default() }
        }
        DataFieldKind::OptionString => quote! { view.#wire_ident().map(|value| value.to_string()) },
        DataFieldKind::OptionScalar => {
            quote! { { let value = view.#wire_ident(); (value != 0).then_some(value) } }
        }
        DataFieldKind::Nested { optional: false } => {
            let ty = &field.ty;
            quote! {
                match view.#wire_ident() {
                    Some(value) => #ty::from_flatbuffer(value)?,
                    None => {
                        return ::flatbuffers::InvalidFlatbuffer::new_missing_required(#wire_name_lit);
                    }
                }
            }
        }
        DataFieldKind::Nested { optional: true } => {
            let ty = nested_inner_of_field(field);
            quote! {
                match view.#wire_ident() {
                    Some(value) => Some(#ty::from_flatbuffer(value)?),
                    None => None,
                }
            }
        }
        DataFieldKind::NestedVec => {
            let inner = vec_inner_of_field(field);
            quote! {
                match view.#wire_ident() {
                    Some(value) => value
                        .iter()
                        .map(#inner::from_flatbuffer)
                        .collect::<::std::result::Result<::std::vec::Vec<_>, flatbuffers::InvalidFlatbuffer>>()?,
                    None => ::std::vec::Vec::new(),
                }
            }
        }
    }
}

pub(super) fn decode_field(field: &syn::Field) -> proc_macro2::TokenStream {
    let id = field.ident.as_ref().unwrap();
    let enc = encoding_path();
    match &field.ty {
        syn::Type::Path(tp) => {
            if let Some(inner) = option_inner(tp) {
                decode_option_field(id, inner)
            } else if let Some(inner) = vec_inner(tp) {
                decode_vec_field(id, inner, true)
            } else {
                let ident = &tp.path.segments.last().unwrap().ident;
                if has_schema_codec(field) {
                    quote! { #id: #enc::FieldDecoder::decode_field(view.#id())? }
                } else if ident == "String" {
                    quote! { #id: #enc::StringFieldValue::into_owned(view.#id()) }
                } else if is_scalar_ident(ident) {
                    quote! { #id: view.#id() }
                } else {
                    // A FlatBuffers native enum field (a unit-only enum bound to
                    // an `enum` type): `view.#id()` already yields the binding
                    // and `from_flatbuffer` is infallible.
                    quote! { #id: #tp::from_flatbuffer(view.#id()) }
                }
            }
        }
        _ => quote! { #id: view.#id() },
    }
}

pub(super) fn decode_option_field(id: &syn::Ident, inner: &syn::Type) -> proc_macro2::TokenStream {
    if let syn::Type::Path(tp) = inner {
        if let Some(vec_inner) = vec_inner(tp) {
            return decode_vec_field(id, vec_inner, false);
        }
        let ident = &tp.path.segments.last().unwrap().ident;
        if ident == "String" {
            return quote! { #id: view.#id().map(|value| value.to_string()) };
        }
        if is_scalar_ident(ident) {
            return quote! { #id: view.#id() };
        }
        return quote! {
            #id: match view.#id() {
                Some(value) => Some(#tp::from_flatbuffer(value)?),
                None => None,
            }
        };
    }

    quote! { #id: view.#id() }
}

pub(super) fn decode_vec_field(
    id: &syn::Ident,
    inner: &syn::Type,
    is_required: bool,
) -> proc_macro2::TokenStream {
    if let syn::Type::Path(tp) = inner {
        let ident = &tp.path.segments.last().unwrap().ident;
        if ident == "u8" {
            if is_required {
                return quote! {
                    #id: view.#id().map(|value| value.bytes().to_vec()).unwrap_or_default()
                };
            }
            return quote! {
                #id: view.#id().map(|value| value.bytes().to_vec())
            };
        }
        if ident == "String" {
            let map_expr = quote! { value.iter().map(|item| item.to_string()).collect::<::std::vec::Vec<_>>() };
            if is_required {
                return quote! { #id: view.#id().map(|value| #map_expr).unwrap_or_default() };
            }
            return quote! { #id: view.#id().map(|value| #map_expr) };
        }
        if is_scalar_ident(ident) {
            if is_required {
                return quote! {
                    #id: view.#id().map(|value| value.iter().collect::<::std::vec::Vec<_>>()).unwrap_or_default()
                };
            }
            return quote! { #id: view.#id().map(|value| value.iter().collect::<::std::vec::Vec<_>>()) };
        }
    }

    let map_expr = quote! {
        value
            .iter()
            .map(#inner::from_flatbuffer)
            .collect::<::std::result::Result<::std::vec::Vec<_>, flatbuffers::InvalidFlatbuffer>>()?
    };
    if is_required {
        return quote! {
            #id: match view.#id() {
                Some(value) => #map_expr,
                None => ::std::vec::Vec::new(),
            }
        };
    }

    quote! {
        #id: match view.#id() {
            Some(value) => Some(#map_expr),
            None => None,
        }
    }
}
