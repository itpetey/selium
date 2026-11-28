use super::*;

/// Reads a `#[field("wire_name")]` rename attribute, if present.
pub(super) fn field_rename(attrs: &[syn::Attribute]) -> syn::Result<Option<String>> {
    for attr in attrs {
        if attr.path().is_ident("field") {
            let lit: syn::LitStr = attr.parse_args().map_err(|error| {
                syn::Error::new_spanned(attr, format!("#[field] expects a string literal: {error}"))
            })?;
            return Ok(Some(lit.value()));
        }
    }
    Ok(None)
}

pub(super) fn parse_concat_macro(mac: syn::Macro) -> syn::Result<String> {
    let tokens = syn::parse::Parser::parse2(
        syn::punctuated::Punctuated::<Expr, syn::Token![,]>::parse_terminated,
        mac.tokens,
    )?;
    let mut out = String::new();
    for expr in tokens {
        out.push_str(&parse_string_expr(expr)?);
    }
    Ok(out)
}

pub(super) fn parse_env_macro(mac: syn::Macro) -> syn::Result<String> {
    let span = mac.path.span();
    let args = syn::parse::Parser::parse2(
        syn::punctuated::Punctuated::<Expr, syn::Token![,]>::parse_terminated,
        mac.tokens.clone(),
    )?;
    let first = args
        .first()
        .ok_or_else(|| syn::Error::new(span, "env! requires an argument"))?;
    if let Expr::Lit(ExprLit {
        lit: Lit::Str(lit), ..
    }) = first
    {
        let var = lit.value();
        std::env::var(&var).map_err(|_| {
            syn::Error::new_spanned(lit, format!("environment variable {var} not set"))
        })
    } else {
        Err(syn::Error::new_spanned(
            first,
            "env! argument must be a string literal",
        ))
    }
}

pub(super) fn parse_string_expr(expr: Expr) -> syn::Result<String> {
    match expr {
        Expr::Lit(ExprLit {
            lit: Lit::Str(lit), ..
        }) => Ok(lit.value()),
        Expr::Macro(ExprMacro { mac, .. }) if mac.path.is_ident("concat") => {
            parse_concat_macro(mac)
        }
        Expr::Macro(ExprMacro { mac, .. }) if mac.path.is_ident("env") => parse_env_macro(mac),
        other => Err(syn::Error::new_spanned(other, "expected string literal")),
    }
}

/// Strips the schema-related attributes (`schema`, `tag`, `field`) from a list,
/// keeping doc comments and any user attributes.
pub(super) fn strip_schema_attrs(attrs: &[syn::Attribute]) -> Vec<syn::Attribute> {
    attrs
        .iter()
        .filter(|attr| {
            !(attr.path().is_ident("schema")
                || attr.path().is_ident("tag")
                || attr.path().is_ident("field"))
        })
        .cloned()
        .collect()
}
