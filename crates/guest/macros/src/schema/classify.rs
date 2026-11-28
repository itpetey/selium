use super::*;

/// Builds the `BindingArgs` path for a table `Binding` path.
pub(super) fn binding_args_path(binding_path: &SynPath) -> SynPath {
    let binding_ident = binding_path.segments.last().unwrap().ident.clone();
    let args_ident = format_ident!("{}Args", binding_ident);
    let mut segments = binding_path.segments.clone();
    segments.pop();
    segments.push(syn::PathSegment {
        ident: args_ident,
        arguments: syn::PathArguments::None,
    });
    SynPath {
        leading_colon: binding_path.leading_colon,
        segments,
    }
}

/// Classifies a data-carrying variant field against the supported field shapes.
pub(super) fn classify_data_field(field: &syn::Field) -> syn::Result<DataFieldKind> {
    if has_schema_skip(field) {
        return Ok(DataFieldKind::Skip);
    }
    let syn::Type::Path(tp) = &field.ty else {
        return Err(syn::Error::new_spanned(
            &field.ty,
            "#[schema] data enums require path-typed or `Option`-wrapped fields",
        ));
    };
    if let Some(inner) = option_inner(tp) {
        if is_string_type(inner) {
            Ok(DataFieldKind::OptionString)
        } else if is_scalar_type(inner) {
            Ok(DataFieldKind::OptionScalar)
        } else {
            Ok(DataFieldKind::Nested { optional: true })
        }
    } else if let Some(inner) = vec_inner(tp) {
        if is_u8_type(inner) {
            Ok(DataFieldKind::Bytes)
        } else if is_string_type(inner) || is_scalar_type(inner) {
            Err(syn::Error::new_spanned(
                &field.ty,
                "#[schema] data enums support `Vec<u8>` and vectors of nested `#[schema]` messages",
            ))
        } else {
            Ok(DataFieldKind::NestedVec)
        }
    } else if is_string_type(&field.ty) {
        Ok(DataFieldKind::String)
    } else if is_scalar_type(&field.ty) {
        Ok(DataFieldKind::Scalar)
    } else {
        Ok(DataFieldKind::Nested { optional: false })
    }
}

pub(super) fn classify_field(field: &syn::Field, generic_params: &[syn::Ident]) -> FieldKind {
    match &field.ty {
        syn::Type::Path(tp) => {
            if let Some(inner) = option_inner(tp) {
                if type_references_generic(inner, generic_params) {
                    FieldKind::OptionGeneric
                } else if is_scalar_type(inner) {
                    FieldKind::OptionScalar
                } else {
                    FieldKind::Concrete
                }
            } else if type_references_generic(&field.ty, generic_params) {
                FieldKind::Generic
            } else {
                FieldKind::Concrete
            }
        }
        _ => FieldKind::Concrete,
    }
}

pub(super) fn collect_generic_params(generics: &syn::Generics) -> Vec<syn::Ident> {
    generics
        .params
        .iter()
        .filter_map(|p| {
            if let syn::GenericParam::Type(tp) = p {
                Some(tp.ident.clone())
            } else {
                None
            }
        })
        .collect()
}

/// Returns whether an enum has data-carrying variants (named fields or tuple
/// payloads), distinguishing it from a unit-only enum bound to a FlatBuffers
/// native enum type.
pub(super) fn data_enum_has_fields(en: &ItemEnum) -> bool {
    en.variants.iter().any(|variant| match &variant.fields {
        syn::Fields::Named(named) => !named.named.is_empty(),
        syn::Fields::Unnamed(_) => true,
        syn::Fields::Unit => false,
    })
}

/// Returns whether a field carries `#[schema(codec)]`, marking a foreign
/// (non-schema) field type whose decode is strict and fallible via
/// `FieldDecoder`.
pub(super) fn has_schema_codec(field: &syn::Field) -> bool {
    field.attrs.iter().any(|attr| {
        attr.path().is_ident("schema")
            && matches!(
                attr.meta,
                syn::Meta::List(ref list) if list.tokens.to_string().trim() == "codec"
            )
    })
}

/// Returns whether a field carries `#[schema(skip)]`.
pub(super) fn has_schema_skip(field: &syn::Field) -> bool {
    field.attrs.iter().any(|attr| {
        attr.path().is_ident("schema")
            && matches!(
                attr.meta,
                syn::Meta::List(ref list) if list.tokens.to_string().trim() == "skip"
            )
    })
}

pub(super) fn is_scalar_ident(ident: &proc_macro2::Ident) -> bool {
    matches!(
        ident.to_string().as_str(),
        "bool"
            | "u8"
            | "u16"
            | "u32"
            | "u64"
            | "i8"
            | "i16"
            | "i32"
            | "i64"
            | "usize"
            | "isize"
            | "f32"
            | "f64"
    )
}

pub(super) fn is_scalar_type(ty: &syn::Type) -> bool {
    if let syn::Type::Path(tp) = ty {
        if let Some(ident) = tp.path.get_ident() {
            return is_scalar_ident(ident);
        }
    }
    false
}

pub(super) fn is_string_type(ty: &syn::Type) -> bool {
    matches!(ty, syn::Type::Path(tp) if tp.path.get_ident().is_some_and(|id| id == "String"))
}

pub(super) fn is_u8_type(ty: &syn::Type) -> bool {
    matches!(ty, syn::Type::Path(tp) if tp.path.get_ident().is_some_and(|id| id == "u8"))
}

/// Returns the inner type of an `Option<T>` data-enum field.
pub(super) fn nested_inner_of_field(field: &DataField) -> &syn::Type {
    let syn::Type::Path(tp) = &field.ty else {
        unreachable!("optional nested field is a path type");
    };
    option_inner(tp).expect("optional nested field is an Option")
}

pub(super) fn option_inner(tp: &syn::TypePath) -> Option<&syn::Type> {
    let last = tp.path.segments.last().unwrap();
    if last.ident != "Option" {
        return None;
    }

    if let syn::PathArguments::AngleBracketed(args) = &last.arguments
        && let Some(syn::GenericArgument::Type(inner)) = args.args.first()
    {
        return Some(inner);
    }

    None
}

pub(super) fn type_references_generic(ty: &syn::Type, generic_params: &[syn::Ident]) -> bool {
    if let syn::Type::Path(tp) = ty {
        for seg in &tp.path.segments {
            if generic_params.iter().any(|gp| gp == &seg.ident) {
                return true;
            }
        }
    }
    false
}

/// Reads the `#[tag(n)]` override on a variant, if present.
pub(super) fn variant_tag(variant: &syn::Variant) -> syn::Result<Option<u8>> {
    for attr in &variant.attrs {
        if attr.path().is_ident("tag") {
            let lit: syn::LitInt = attr.parse_args().map_err(|error| {
                syn::Error::new_spanned(attr, format!("#[tag] expects an integer literal: {error}"))
            })?;
            let tag = lit.base10_parse::<u8>().map_err(|_| {
                syn::Error::new_spanned(attr, "tag must be within the u8 range (0..=255)")
            })?;
            return Ok(Some(tag));
        }
    }
    Ok(None)
}

pub(super) fn vec_inner(tp: &syn::TypePath) -> Option<&syn::Type> {
    let last = tp.path.segments.last().unwrap();
    if last.ident != "Vec" {
        return None;
    }

    if let syn::PathArguments::AngleBracketed(args) = &last.arguments
        && let Some(syn::GenericArgument::Type(inner)) = args.args.first()
    {
        return Some(inner);
    }

    None
}

/// Returns the inner element type of a `Vec<T>` data-enum field.
pub(super) fn vec_inner_of_field(field: &DataField) -> &syn::Type {
    let syn::Type::Path(tp) = &field.ty else {
        unreachable!("NestedVec field is a path type");
    };
    vec_inner(tp).expect("NestedVec field is a Vec")
}
