use super::classify::{is_scalar_ident, option_inner, vec_inner};
use super::expand::encoding_path;
use super::*;

/// Returns the encode statement for one variant field binding, or `None` for a
/// skipped field.
pub(super) fn encode_data_field(
    field: &DataField,
    bind: &syn::Ident,
) -> Option<proc_macro2::TokenStream> {
    let wire_ident = format_ident!("{}", field.wire_name);
    let statement = match field.kind {
        DataFieldKind::Skip => return None,
        DataFieldKind::String => quote! { args.#wire_ident = Some(builder.create_string(#bind)); },
        DataFieldKind::Scalar => quote! { args.#wire_ident = *#bind; },
        DataFieldKind::Bytes => quote! { args.#wire_ident = Some(builder.create_vector(#bind)); },
        DataFieldKind::OptionString => {
            quote! { args.#wire_ident = #bind.as_ref().map(|value| builder.create_string(value)); }
        }
        DataFieldKind::OptionScalar => {
            quote! { args.#wire_ident = #bind.unwrap_or_default(); }
        }
        DataFieldKind::Nested { optional: false } => {
            quote! { args.#wire_ident = Some(#bind.write_flatbuffer(builder)); }
        }
        DataFieldKind::Nested { optional: true } => {
            quote! { args.#wire_ident = #bind.as_ref().map(|value| value.write_flatbuffer(builder)); }
        }
        DataFieldKind::NestedVec => {
            let offsets_ident = format_ident!("{}_offsets", field.wire_name);
            quote! {
                let #offsets_ident: ::std::vec::Vec<_> = #bind
                    .iter()
                    .map(|item| item.write_flatbuffer(builder))
                    .collect();
                args.#wire_ident = Some(builder.create_vector(&#offsets_ident));
            }
        }
    };
    Some(statement)
}

pub(super) fn encode_field(field: &syn::Field) -> proc_macro2::TokenStream {
    let id = field.ident.as_ref().unwrap();
    let enc = encoding_path();
    match &field.ty {
        syn::Type::Path(tp) => {
            if let Some(inner) = option_inner(tp) {
                encode_option_field(id, inner)
            } else if let Some(inner) = vec_inner(tp) {
                encode_vec_field(id, inner, true)
            } else {
                let ident = &tp.path.segments.last().unwrap().ident;
                if ident == "String" {
                    quote! { args.#id = Some(builder.create_string(&self.#id)); }
                } else if is_scalar_ident(ident) {
                    quote! { args.#id = self.#id; }
                } else {
                    quote! {
                        args.#id = #enc::FieldEncoder::encode_field(
                            &self.#id,
                            builder,
                        );
                    }
                }
            }
        }
        _ => quote! { args.#id = self.#id; },
    }
}

pub(super) fn encode_option_field(id: &syn::Ident, inner: &syn::Type) -> proc_macro2::TokenStream {
    if let syn::Type::Path(tp) = inner {
        if let Some(vec_inner) = vec_inner(tp) {
            return encode_vec_field(id, vec_inner, false);
        }
        let ident = &tp.path.segments.last().unwrap().ident;
        if ident == "String" {
            return quote! {
                args.#id = self.#id.as_ref().map(|value| builder.create_string(value));
            };
        }
        if is_scalar_ident(ident) {
            return quote! { args.#id = self.#id; };
        }
        return quote! {
            args.#id = self.#id.as_ref().map(|value| value.write_flatbuffer(builder));
        };
    }

    quote! { args.#id = self.#id.as_ref().cloned(); }
}

pub(super) fn encode_vec_field(
    id: &syn::Ident,
    inner: &syn::Type,
    is_required: bool,
) -> proc_macro2::TokenStream {
    if let syn::Type::Path(tp) = inner {
        let ident = &tp.path.segments.last().unwrap().ident;
        if ident == "u8" {
            if is_required {
                return quote! { args.#id = Some(builder.create_vector(&self.#id)); };
            }
            return quote! {
                args.#id = self.#id.as_ref().map(|value| builder.create_vector(value));
            };
        }
        if ident == "String" {
            let offsets_ident =
                syn::Ident::new(&format!("{}_offsets", id), proc_macro2::Span::call_site());
            if is_required {
                return quote! {
                    let #offsets_ident: Vec<_> = self.#id.iter().map(|value| builder.create_string(value)).collect();
                    args.#id = Some(builder.create_vector(&#offsets_ident));
                };
            }
            return quote! {
                args.#id = self.#id.as_ref().map(|value| {
                    let #offsets_ident: Vec<_> = value.iter().map(|item| builder.create_string(item)).collect();
                    builder.create_vector(&#offsets_ident)
                });
            };
        }
        if is_scalar_ident(ident) {
            if is_required {
                return quote! { args.#id = Some(builder.create_vector(&self.#id)); };
            }
            return quote! {
                args.#id = self.#id.as_ref().map(|value| builder.create_vector(value));
            };
        }
        let offsets_ident =
            syn::Ident::new(&format!("{}_offsets", id), proc_macro2::Span::call_site());
        if is_required {
            return quote! {
                let #offsets_ident: Vec<_> = self.#id
                    .iter()
                    .map(|item| item.write_flatbuffer(builder))
                    .collect();
                args.#id = Some(builder.create_vector(&#offsets_ident));
            };
        }
        return quote! {
            args.#id = self.#id.as_ref().map(|value| {
                let #offsets_ident: Vec<_> = value
                    .iter()
                    .map(|item| item.write_flatbuffer(builder))
                    .collect();
                builder.create_vector(&#offsets_ident)
            });
        };
    }

    quote! { args.#id = self.#id.as_ref().map(|value| value.write_flatbuffer(builder)); }
}
