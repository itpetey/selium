use super::attrs::{field_rename, strip_schema_attrs};
use super::classify::{
    binding_args_path, classify_data_field, classify_field, collect_generic_params, option_inner,
    variant_tag,
};
use super::decode::{decode_data_field, decode_field};
use super::encode::{encode_data_field, encode_field};
use super::*;

/// Returns the path prefix for encoding types.
///
/// The types live at the `selium-service` crate root. Inside `selium-service`
/// itself they are referred to as `crate`; every other consumer refers to them
/// through the `selium_service` dependency. We detect this via
/// `CARGO_CRATE_NAME`.
pub(super) fn encoding_path() -> proc_macro2::TokenStream {
    match std::env::var("CARGO_CRATE_NAME").as_deref() {
        Ok("selium_service") => quote! { crate },
        Ok("selium_wire") => quote! { selium_service },
        Ok("selium_proto_http") => quote! { selium_service },
        Ok("selium_proto_dns") => quote! { selium_service },
        _ => quote! { selium_service },
    }
}

/// Emits the FlatMsg/HasSchema codec for a data-carrying enum bound to a
/// flattened `variant: ubyte` table.
pub(super) fn expand_data_enum(
    en: ItemEnum,
    fqname: String,
    binding_path: SynPath,
    hash_lit: syn::LitByteStr,
) -> syn::Result<TokenStream> {
    // Cleaned enum: strip schema/tag/field attributes, keep everything else.
    let mut en2 = en.clone();
    en2.attrs = strip_schema_attrs(&en.attrs);
    for variant in &mut en2.variants {
        let attrs = strip_schema_attrs(&variant.attrs);
        variant.attrs = attrs;
        match &mut variant.fields {
            syn::Fields::Named(named) => {
                for field in &mut named.named {
                    let attrs = strip_schema_attrs(&field.attrs);
                    field.attrs = attrs;
                }
            }
            syn::Fields::Unnamed(unnamed) => {
                for field in &mut unnamed.unnamed {
                    let attrs = strip_schema_attrs(&field.attrs);
                    field.attrs = attrs;
                }
            }
            syn::Fields::Unit => {}
        }
    }

    // Assign tags and classify every variant.
    let mut used_tags = std::collections::HashSet::new();
    let mut variants: Vec<DataVariant> = Vec::with_capacity(en.variants.len());
    for (index, variant) in en.variants.iter().enumerate() {
        let tag = match variant_tag(variant)? {
            Some(tag) => tag,
            None => u8::try_from(index).map_err(|_| {
                syn::Error::new_spanned(
                    variant,
                    "too many variants; #[schema] tags must fit within the u8 range",
                )
            })?,
        };
        if !used_tags.insert(tag) {
            return Err(syn::Error::new_spanned(
                variant,
                format!("duplicate #[tag] value {tag}"),
            ));
        }

        let shape = match &variant.fields {
            syn::Fields::Unit => DataVariantShape::Unit,
            syn::Fields::Named(named) => {
                let mut fields = Vec::with_capacity(named.named.len());
                for field in &named.named {
                    let ident = field.ident.clone().expect("named field has an ident");
                    let wire_name = match field_rename(&field.attrs)? {
                        Some(name) => name,
                        None => ident.to_string(),
                    };
                    let kind = classify_data_field(field)?;
                    fields.push(DataField {
                        ident: Some(ident),
                        wire_name,
                        ty: field.ty.clone(),
                        kind,
                    });
                }
                DataVariantShape::Named(fields)
            }
            syn::Fields::Unnamed(unnamed) => {
                let field = unnamed.unnamed.first().ok_or_else(|| {
                    syn::Error::new_spanned(
                        variant,
                        "#[schema] data enum tuple variants require a single field",
                    )
                })?;
                let wire_name = field_rename(&variant.attrs)?.ok_or_else(|| {
                    syn::Error::new_spanned(
                        variant,
                        "single unnamed-field variants require #[field(\"wire_name\")]",
                    )
                })?;
                let kind = classify_data_field(field)?;
                DataVariantShape::Tuple(Box::new(DataField {
                    ident: None,
                    wire_name,
                    ty: field.ty.clone(),
                    kind,
                }))
            }
        };

        variants.push(DataVariant {
            ident: variant.ident.clone(),
            tag,
            shape,
        });
    }

    let enum_ident = &en.ident;
    let schema_ident = format_ident!("{}Schema", enum_ident);
    let fq_lit = fqname.clone();
    let binding_path_ts = quote! { #binding_path };
    let args_path = binding_args_path(&binding_path);
    let enc = encoding_path();
    let unknown_lit = syn::LitStr::new(
        &format!("known {enum_ident} variant"),
        proc_macro2::Span::call_site(),
    );

    // Encode arms.
    let encode_arms = variants.iter().map(|variant| {
        let ident = &variant.ident;
        let tag = variant.tag;
        match &variant.shape {
            DataVariantShape::Unit => quote! {
                Self::#ident => { args.variant = #tag; }
            },
            DataVariantShape::Named(fields) => {
                let pats = fields
                    .iter()
                    .filter(|field| !matches!(field.kind, DataFieldKind::Skip))
                    .map(|field| field.ident.clone().unwrap());
                let stmts = fields
                    .iter()
                    .filter_map(|field| encode_data_field(field, field.ident.as_ref().unwrap()));
                quote! {
                    Self::#ident { #(#pats,)* .. } => {
                        args.variant = #tag;
                        #(#stmts)*
                    }
                }
            }
            DataVariantShape::Tuple(field) => {
                let value = format_ident!("value");
                let stmt = encode_data_field(field, &value).expect("tuple fields are encodable");
                quote! {
                    Self::#ident(value) => {
                        args.variant = #tag;
                        #stmt
                    }
                }
            }
        }
    });

    // Decode arms.
    let decode_arms = variants.iter().map(|variant| {
        let ident = &variant.ident;
        let tag = variant.tag;
        match &variant.shape {
            DataVariantShape::Unit => quote! { #tag => Ok(Self::#ident), },
            DataVariantShape::Named(fields) => {
                let inits = fields.iter().map(|field| {
                    let name = field.ident.as_ref().unwrap();
                    let expr = decode_data_field(field);
                    quote! { #name: #expr }
                });
                quote! { #tag => Ok(Self::#ident { #(#inits),* }), }
            }
            DataVariantShape::Tuple(field) => {
                let expr = decode_data_field(field);
                quote! { #tag => Ok(Self::#ident(#expr)), }
            }
        }
    });

    let expanded = quote! {
        #en2

        #[allow(non_upper_case_globals)]
        pub const #schema_ident: #enc::SchemaDescriptor = #enc::SchemaDescriptor {
            fqname: #fq_lit,
            hash: *#hash_lit,
        };

        impl #enc::HasSchema for #enum_ident {
            const SCHEMA: #enc::SchemaDescriptor = #schema_ident;
        }

        impl #enum_ident {
            pub fn write_flatbuffer<'bldr, A: flatbuffers::Allocator + 'bldr>(
                &self,
                builder: &mut flatbuffers::FlatBufferBuilder<'bldr, A>,
            ) -> flatbuffers::WIPOffset<#binding_path_ts<'bldr>> {
                let mut args = #args_path::default();
                match self {
                    #(#encode_arms)*
                }
                #binding_path_ts::create(builder, &args)
            }

            pub fn from_flatbuffer(
                view: #binding_path_ts<'_>,
            ) -> ::std::result::Result<Self, flatbuffers::InvalidFlatbuffer> {
                match view.variant() {
                    #(#decode_arms)*
                    _ => {
                        return ::flatbuffers::InvalidFlatbuffer::new_missing_required(#unknown_lit);
                    }
                }
            }
        }

        impl #enc::FlatMsg for #enum_ident {
            fn encode(value: &Self) -> Vec<u8> {
                let mut builder = flatbuffers::FlatBufferBuilder::new();
                let root = value.write_flatbuffer(&mut builder);
                builder.finish(root, None);
                builder.finished_data().to_vec()
            }

            fn decode(
                bytes: &[u8],
            ) -> ::std::result::Result<Self, flatbuffers::InvalidFlatbuffer> {
                let view = flatbuffers::root::<#binding_path_ts<'_>>(bytes)?;
                Self::from_flatbuffer(view)
            }
        }
    };

    Ok(expanded.into())
}

pub(super) fn expand_enum(
    en: ItemEnum,
    fqname: String,
    binding_path: SynPath,
    hash_lit: syn::LitByteStr,
) -> TokenStream {
    let mut en2 = en.clone();
    en2.attrs = en
        .attrs
        .iter()
        .filter(|attr| !attr.path().is_ident("schema"))
        .cloned()
        .collect();
    let enum_ident = en.ident.clone();
    let schema_ident = syn::Ident::new(
        &format!("{}Schema", enum_ident),
        proc_macro2::Span::call_site(),
    );
    let fq_lit = fqname.clone();
    let binding_path_ts = quote! { #binding_path };

    let mut unit_variants = Vec::new();
    let mut fallback_variant: Option<(syn::Ident, syn::Type)> = None;
    for variant in en.variants.iter() {
        match &variant.fields {
            syn::Fields::Unit => unit_variants.push(variant.ident.clone()),
            syn::Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {
                if fallback_variant.is_some() {
                    return syn::Error::new_spanned(
                        variant,
                        "#[schema] enums may only include a single tuple variant",
                    )
                    .to_compile_error()
                    .into();
                }
                let Some(field) = fields.unnamed.first() else {
                    return syn::Error::new_spanned(
                        variant,
                        "#[schema] enums require a single tuple field",
                    )
                    .to_compile_error()
                    .into();
                };
                fallback_variant = Some((variant.ident.clone(), field.ty.clone()));
            }
            _ => {
                return syn::Error::new_spanned(
                    variant,
                    "#[schema] enums require unit variants and at most one tuple fallback",
                )
                .to_compile_error()
                .into();
            }
        }
    }

    if unit_variants.is_empty() {
        return syn::Error::new_spanned(en, "#[schema] enums require at least one unit variant")
            .to_compile_error()
            .into();
    }

    let to_flatbuffer_variants = unit_variants.iter().map(|variant| {
        quote! { Self::#variant => #binding_path_ts::#variant, }
    });
    let from_flatbuffer_variants = unit_variants.iter().map(|variant| {
        quote! { #binding_path_ts::#variant => Self::#variant, }
    });

    let fallback_to_flatbuffer = fallback_variant.as_ref().map(|(ident, _)| {
        quote! { Self::#ident(value) => #binding_path_ts(*value), }
    });

    let default_variant = unit_variants.first().cloned();
    let fallback_from_flatbuffer = if let Some((ident, _ty)) = fallback_variant {
        quote! { other => Self::#ident(other.0), }
    } else if let Some(variant) = default_variant {
        quote! { _ => Self::#variant, }
    } else {
        quote! { _ => unreachable!(), }
    };

    let enc = encoding_path();

    let expanded = quote! {
        #en2

        #[allow(non_upper_case_globals)]
        pub const #schema_ident: #enc::SchemaDescriptor = #enc::SchemaDescriptor {
            fqname: #fq_lit,
            hash: *#hash_lit,
        };

        impl #enc::HasSchema for #enum_ident {
            const SCHEMA: #enc::SchemaDescriptor = #schema_ident;
        }

        impl #enc::FieldEncoder for #enum_ident {
            type Output<'bldr> = #binding_path_ts;

            fn encode_field<'bldr, A: flatbuffers::Allocator + 'bldr>(
                &self,
                builder: &mut flatbuffers::FlatBufferBuilder<'bldr, A>,
            ) -> Self::Output<'bldr> {
                self.write_flatbuffer(builder)
            }
        }

        impl #enum_ident {
            pub fn write_flatbuffer<'bldr, A: flatbuffers::Allocator + 'bldr>(
                &self,
                _builder: &mut flatbuffers::FlatBufferBuilder<'bldr, A>,
            ) -> #binding_path_ts {
                match self {
                    #( #to_flatbuffer_variants )*
                    #fallback_to_flatbuffer
                }
            }

            pub fn from_flatbuffer(value: #binding_path_ts) -> Self {
                match value {
                    #( #from_flatbuffer_variants )*
                    #fallback_from_flatbuffer
                }
            }
        }
    };

    expanded.into()
}

pub(super) fn expand_struct(
    st: ItemStruct,
    fbs_path: String,
    fqname: String,
    binding_path: SynPath,
    hash_lit: syn::LitByteStr,
    wire_ident: Option<syn::Ident>,
) -> TokenStream {
    let generic_params = collect_generic_params(&st.generics);

    // Error: generic struct without wire parameter.
    if !generic_params.is_empty() && wire_ident.is_none() {
        return syn::Error::new_spanned(
            &st.generics,
            "struct has generic type parameters; add wire = WireTypeName to #[schema] to generate a wire type",
        )
        .to_compile_error()
        .into();
    }

    // Wire generation path.
    if let Some(ref wire) = wire_ident {
        return expand_struct_with_wire(
            st,
            fbs_path,
            fqname,
            binding_path,
            hash_lit,
            wire,
            &generic_params,
        );
    }

    // Existing direct-impl path (non-generic structs).
    expand_struct_direct(st, fqname, binding_path, hash_lit)
}

/// Direct-impl path: generates FlatMsg, HasSchema, FieldEncoder, new, write_flatbuffer,
/// from_flatbuffer on the annotated struct itself.
pub(super) fn expand_struct_direct(
    st: ItemStruct,
    fqname: String,
    binding_path: SynPath,
    hash_lit: syn::LitByteStr,
) -> TokenStream {
    let mut st2 = st.clone();
    st2.attrs = st
        .attrs
        .iter()
        .filter(|attr| !attr.path().is_ident("schema"))
        .cloned()
        .collect();
    if let syn::Fields::Named(named) = &mut st2.fields {
        for field in &mut named.named {
            let attrs = field
                .attrs
                .iter()
                .filter(|attr| !attr.path().is_ident("schema"))
                .cloned()
                .collect();
            field.attrs = attrs;
        }
    }
    let struct_ident = st.ident.clone();
    let schema_ident = syn::Ident::new(
        &format!("{}Schema", struct_ident),
        proc_macro2::Span::call_site(),
    );
    let binding_ident = binding_path.segments.last().unwrap().ident.clone();
    let args_ident = syn::Ident::new(
        &format!("{}Args", binding_ident),
        proc_macro2::Span::call_site(),
    );

    let mut args_segments = binding_path.segments.clone();
    args_segments.pop();
    args_segments.push(syn::PathSegment {
        ident: args_ident.clone(),
        arguments: syn::PathArguments::None,
    });
    let args_path = syn::Path {
        leading_colon: binding_path.leading_colon,
        segments: args_segments,
    };

    let fields = match &st.fields {
        syn::Fields::Named(named) => named.named.iter().collect::<Vec<_>>(),
        _ => panic!("#[schema] requires a struct with named fields"),
    };

    let ctor_params = fields.iter().map(|f| {
        let id = f.ident.as_ref().unwrap();
        let ty = &f.ty;
        quote! { #id: #ty }
    });
    let ctor_inits = fields.iter().map(|f| {
        let id = f.ident.as_ref().unwrap();
        quote! { #id }
    });

    let encode_fields = fields.iter().map(|f| encode_field(f));
    let decode_fields = fields.iter().map(|f| decode_field(f));

    let fq_lit = fqname.clone();
    let binding_path_ts = quote! { #binding_path };
    let enc = encoding_path();

    let expanded = quote! {
        #st2

        #[allow(non_upper_case_globals)]
        pub const #schema_ident: #enc::SchemaDescriptor = #enc::SchemaDescriptor {
            fqname: #fq_lit,
            hash: *#hash_lit,
        };

        impl #enc::HasSchema for #struct_ident {
            const SCHEMA: #enc::SchemaDescriptor = #schema_ident;
        }

        impl #enc::FieldEncoder for #struct_ident {
            type Output<'bldr> = Option<flatbuffers::WIPOffset<#binding_path_ts<'bldr>>>;

            fn encode_field<'bldr, A: flatbuffers::Allocator + 'bldr>(
                &self,
                builder: &mut flatbuffers::FlatBufferBuilder<'bldr, A>,
            ) -> Self::Output<'bldr> {
                Some(self.write_flatbuffer(builder))
            }
        }

        impl #struct_ident {
            pub fn new( #( #ctor_params ),* ) -> Self {
                Self { #( #ctor_inits, )* }
            }

            pub fn write_flatbuffer<'bldr, A: flatbuffers::Allocator + 'bldr>(
                &self,
                builder: &mut flatbuffers::FlatBufferBuilder<'bldr, A>,
            ) -> flatbuffers::WIPOffset<#binding_path_ts<'bldr>> {
                let mut args = #args_path::default();
                #( #encode_fields )*
                #binding_path_ts::create(builder, &args)
            }

            pub fn from_flatbuffer(
                view: #binding_path_ts<'_>,
            ) -> ::std::result::Result<Self, flatbuffers::InvalidFlatbuffer> {
                Ok(Self { #( #decode_fields, )* })
            }
        }

        impl #enc::FlatMsg for #struct_ident {
            fn encode(value: &Self) -> Vec<u8> {
                let mut builder = flatbuffers::FlatBufferBuilder::new();
                let root = value.write_flatbuffer(&mut builder);
                builder.finish(root, None);
                builder.finished_data().to_vec()
            }

            fn decode(bytes: &[u8]) -> ::std::result::Result<Self, flatbuffers::InvalidFlatbuffer> {
                let view = flatbuffers::root::<#binding_path_ts<'_>>(bytes)?;
                Self::from_flatbuffer(view)
            }
        }
    };

    expanded.into()
}

/// Wire path: emits the cleaned domain struct, the wire struct (with `#[schema]`),
/// and bridge FlatMsg/HasSchema impls on the domain type.
pub(super) fn expand_struct_with_wire(
    st: ItemStruct,
    fbs_path: String,
    fqname: String,
    binding_path: SynPath,
    _hash_lit: syn::LitByteStr,
    wire_ident: &syn::Ident,
    generic_params: &[syn::Ident],
) -> TokenStream {
    // Cleaned domain struct (strip #[schema] attribute, keep everything else).
    let mut cleaned = st.clone();
    cleaned.attrs = st
        .attrs
        .iter()
        .filter(|attr| !attr.path().is_ident("schema"))
        .cloned()
        .collect();

    let wire_struct = generate_wire_struct(
        &st,
        wire_ident,
        &fbs_path,
        &fqname,
        &binding_path,
        generic_params,
    );

    let bridge = generate_bridge_impls(&st, wire_ident, generic_params);

    let expanded = quote! {
        #cleaned

        #wire_struct

        #bridge
    };

    expanded.into()
}

pub(super) fn generate_bridge_impls(
    st: &ItemStruct,
    wire_ident: &syn::Ident,
    generic_params: &[syn::Ident],
) -> proc_macro2::TokenStream {
    let domain_ident = &st.ident;
    let enc = encoding_path();

    let fields = match &st.fields {
        syn::Fields::Named(named) => named.named.iter().collect::<Vec<_>>(),
        _ => panic!("#[schema] wire generation requires named fields"),
    };

    // --- Encode: domain fields → wire constructor args ---
    let encode_exprs: Vec<proc_macro2::TokenStream> = fields
        .iter()
        .map(|f| {
            let field_name = f.ident.as_ref().unwrap();
            let kind = classify_field(f, generic_params);
            match kind {
                FieldKind::Generic => {
                    quote! { #enc::FlatMsg::encode(&value.#field_name) }
                }
                FieldKind::OptionGeneric => {
                    quote! {
                        match &value.#field_name {
                            Some(v) => #enc::FlatMsg::encode(v),
                            None => Vec::new(),
                        }
                    }
                }
                FieldKind::OptionScalar => {
                    quote! { value.#field_name.unwrap_or_default() }
                }
                FieldKind::Concrete => {
                    quote! { value.#field_name.clone() }
                }
            }
        })
        .collect();

    // --- Decode: wire fields → domain field initialisers ---
    let decode_fields: Vec<proc_macro2::TokenStream> = fields
        .iter()
        .map(|f| {
            let field_name = f.ident.as_ref().unwrap();
            let kind = classify_field(f, generic_params);
            match kind {
                FieldKind::Generic => {
                    let wire_field = format_ident!("{}_bytes", field_name);
                    quote! { #field_name: #enc::FlatMsg::decode(&wire.#wire_field)? }
                }
                FieldKind::OptionGeneric => {
                    let wire_field = format_ident!("{}_bytes", field_name);
                    quote! {
                        #field_name: if wire.#wire_field.is_empty() {
                            None
                        } else {
                            Some(#enc::FlatMsg::decode(&wire.#wire_field)?)
                        }
                    }
                }
                FieldKind::OptionScalar => {
                    let ty = &f.ty;
                    // Extract the inner scalar type from Option<T>.
                    let inner_ty = match ty {
                        syn::Type::Path(tp) => option_inner(tp).unwrap(),
                        _ => unreachable!(),
                    };
                    quote! {
                        #field_name: {
                            let v: #inner_ty = wire.#field_name;
                            let zero: #inner_ty = ::std::default::Default::default();
                            if v == zero { None } else { Some(v) }
                        }
                    }
                }
                FieldKind::Concrete => {
                    quote! { #field_name: wire.#field_name }
                }
            }
        })
        .collect();

    // Generic bounds: each type param gets `: FlatMsg`.
    let bounded_params: Vec<proc_macro2::TokenStream> = generic_params
        .iter()
        .map(|gp| quote! { #gp: #enc::FlatMsg })
        .collect();

    let generic_args: Vec<_> = generic_params.iter().collect();

    let wire_schema_ident = format_ident!("{}Schema", wire_ident);

    quote! {
        impl<#(#bounded_params),*> #enc::FlatMsg for #domain_ident<#(#generic_args),*> {
            fn encode(value: &Self) -> Vec<u8> {
                let wire = #wire_ident::new(#(#encode_exprs),*);
                #enc::FlatMsg::encode(&wire)
            }

            fn decode(bytes: &[u8]) -> ::std::result::Result<Self, flatbuffers::InvalidFlatbuffer> {
                let wire: #wire_ident = #enc::FlatMsg::decode(bytes)?;
                Ok(Self {
                    #(#decode_fields),*
                })
            }
        }

        impl<#(#bounded_params),*> #enc::HasSchema for #domain_ident<#(#generic_args),*> {
            const SCHEMA: #enc::SchemaDescriptor = #wire_schema_ident;
        }
    }
}

pub(super) fn generate_wire_struct(
    st: &ItemStruct,
    wire_ident: &syn::Ident,
    fbs_path: &str,
    fqname: &str,
    binding_path: &SynPath,
    generic_params: &[syn::Ident],
) -> proc_macro2::TokenStream {
    let fields = match &st.fields {
        syn::Fields::Named(named) => named.named.iter().collect::<Vec<_>>(),
        _ => panic!("#[schema] wire generation requires named fields"),
    };

    let wire_fields: Vec<proc_macro2::TokenStream> = fields
        .iter()
        .map(|f| {
            let field_name = f.ident.as_ref().unwrap();
            let kind = classify_field(f, generic_params);
            match kind {
                FieldKind::Generic => {
                    let wire_name = format_ident!("{}_bytes", field_name);
                    quote! { pub #wire_name: Vec<u8> }
                }
                FieldKind::OptionGeneric => {
                    let wire_name = format_ident!("{}_bytes", field_name);
                    quote! { pub #wire_name: Vec<u8> }
                }
                FieldKind::OptionScalar => {
                    // Option<scalar> → scalar (sentinel value)
                    let inner = option_inner(match &f.ty {
                        syn::Type::Path(tp) => tp,
                        _ => unreachable!(),
                    })
                    .unwrap();
                    quote! { pub #field_name: #inner }
                }
                FieldKind::Concrete => {
                    let ty = &f.ty;
                    quote! { pub #field_name: #ty }
                }
            }
        })
        .collect();

    // Copy derive attributes from the domain struct.
    let derive_attrs: Vec<_> = st
        .attrs
        .iter()
        .filter(|attr| attr.path().is_ident("derive"))
        .collect();

    let fbs_path_lit = fbs_path;
    let fqname_lit = fqname;

    quote! {
        #(#derive_attrs)*
        #[schema(
            path = #fbs_path_lit,
            ty = #fqname_lit,
            binding = #binding_path
        )]
        pub struct #wire_ident {
            #(#wire_fields),*
        }
    }
}
