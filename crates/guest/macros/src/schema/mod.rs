use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{
    Expr, ExprLit, ExprMacro, Item, ItemEnum, ItemStruct, Lit, LitStr, Path as SynPath,
    parse::Parser, parse_macro_input, spanned::Spanned,
};

#[derive(Clone)]
enum FieldKind {
    /// `field: K` — direct generic type parameter.
    Generic,
    /// `field: Option<V>` where `V` is a generic type parameter.
    OptionGeneric,
    /// `field: Option<u64>` — optional scalar (sentinel encoding).
    OptionScalar,
    /// Concrete non-generic type — pass through unchanged.
    Concrete,
}

/// Classified field shape of a data-carrying enum variant.
enum DataFieldKind {
    /// `String` table field.
    String,
    /// Primitive numeric/bool table field.
    Scalar,
    /// `Vec<u8>` table field.
    Bytes,
    /// `Option<String>` table field.
    OptionString,
    /// `Option<scalar>` table field encoded with a zero sentinel.
    OptionScalar,
    /// Nested `#[schema]` message field (required or optional).
    Nested { optional: bool },
    /// `Vec<T>` of nested `#[schema]` messages.
    NestedVec,
    /// `#[schema(skip)]`: omitted on encode, defaulted on decode.
    Skip,
}

/// One declared field inside a data-carrying variant.
struct DataField {
    /// The Rust field name; `None` for a single unnamed-field (tuple) variant.
    ident: Option<syn::Ident>,
    /// The FlatBuffers table field name it maps to.
    wire_name: String,
    /// The field's declared Rust type.
    ty: syn::Type,
    kind: DataFieldKind,
}

/// The declared shape of a data-carrying variant.
enum DataVariantShape {
    Unit,
    Named(Vec<DataField>),
    Tuple(Box<DataField>),
}

/// One processed data-carrying variant.
struct DataVariant {
    ident: syn::Ident,
    tag: u8,
    shape: DataVariantShape,
}

pub fn expand(attr: TokenStream, item: TokenStream) -> TokenStream {
    let parser = |input: syn::parse::ParseStream| -> syn::Result<(
        Option<String>,
        Option<String>,
        Option<SynPath>,
        Option<syn::Ident>,
    )> {
        let mut path_lit: Option<String> = None;
        let mut fqname: Option<String> = None;
        let mut binding_path: Option<SynPath> = None;
        let mut wire_ident: Option<syn::Ident> = None;
        while !input.is_empty() {
            let key: syn::Ident = input.parse()?;
            input.parse::<syn::Token![=]>()?;
            if key == "binding" {
                if input.peek(syn::LitStr) {
                    let s: LitStr = input.parse()?;
                    binding_path = Some(syn::parse_str(&s.value())?);
                } else {
                    let p: SynPath = input.parse()?;
                    binding_path = Some(p);
                }
            } else if key == "wire" {
                if input.peek(syn::LitStr) {
                    let s: LitStr = input.parse()?;
                    wire_ident = Some(syn::Ident::new(&s.value(), s.span()));
                } else {
                    wire_ident = Some(input.parse()?);
                }
            } else if key == "path" || key == "ty" {
                let expr: Expr = input.parse()?;
                let value = parse_string_expr(expr)?;
                if key == "path" {
                    path_lit = Some(value);
                } else {
                    fqname = Some(value);
                }
            } else {
                return Err(input.error("unknown key in #[schema]"));
            }
            if input.peek(syn::Token![,]) {
                let _comma: syn::Token![,] = input.parse()?;
            }
        }
        Ok((path_lit, fqname, binding_path, wire_ident))
    };

    let (path_lit, fqname, binding_path, wire_ident) =
        parser.parse(attr).expect("invalid #[schema] attributes");
    let fbs_path = path_lit.expect("#[schema] requires path = \"...\"");
    let fqname = fqname.expect("#[schema] requires ty = \"ns.Type\"");
    let binding_path = binding_path.expect("#[schema] requires binding = path::to::Type");

    let item = parse_macro_input!(item as Item);

    // Early check: generic structs require the wire parameter.
    // This runs before .fbs file I/O so the error is clean.
    if let Item::Struct(ref st) = item {
        let generic_params = collect_generic_params(&st.generics);
        if !generic_params.is_empty() && wire_ident.is_none() {
            return syn::Error::new_spanned(
                &st.generics,
                "struct has generic type parameters; add wire = WireTypeName to #[schema] to generate a wire type",
            )
            .to_compile_error()
            .into();
        }
    }

    let base = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR missing");
    let full = std::path::Path::new(&base).join(&fbs_path);
    let bytes =
        std::fs::read(&full).unwrap_or_else(|e| panic!("cannot read {}: {}", full.display(), e));
    let hash = blake3::hash(&bytes);
    let hash_bytes = &hash.as_bytes()[0..16];
    let hash_lit = syn::LitByteStr::new(hash_bytes, proc_macro2::Span::call_site());

    match item {
        Item::Struct(st) => expand_struct(st, fbs_path, fqname, binding_path, hash_lit, wire_ident),
        Item::Enum(en) => {
            if data_enum_has_fields(&en) {
                expand_data_enum(en, fqname, binding_path, hash_lit)
                    .unwrap_or_else(|error| error.into_compile_error().into())
            } else {
                expand_enum(en, fqname, binding_path, hash_lit)
            }
        }
        other => syn::Error::new_spanned(other, "#[schema] requires a struct or enum")
            .to_compile_error()
            .into(),
    }
}

mod attrs;
mod classify;
mod decode;
mod encode;
mod expand;

use self::attrs::parse_string_expr;
use self::classify::{collect_generic_params, data_enum_has_fields};
use self::expand::{expand_data_enum, expand_enum, expand_struct};
