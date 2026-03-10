use proc_macro2::{Ident, Literal, TokenStream};
use quote::{format_ident, quote};

use crate::{
    ContractPackage, EventDef, SchemaDef, ServiceBodyDef, ServiceBodyMode, ServiceDef,
    ServiceFieldBinding, StreamDef,
};

pub(super) fn runtime_helper_tokens(package: &ContractPackage) -> TokenStream {
    if package.services.is_empty() && package.streams.is_empty() {
        return TokenStream::new();
    }

    quote! {
        #[allow(dead_code)]
        fn __selium_extract_path_params(
            template: &str,
            actual: &str,
        ) -> Option<std::collections::BTreeMap<String, String>> {
            let template_segments = if template.trim_matches('/').is_empty() {
                Vec::new()
            } else {
                template.trim_matches('/').split('/').collect::<Vec<_>>()
            };
            let actual_segments = if actual.trim_matches('/').is_empty() {
                Vec::new()
            } else {
                actual.trim_matches('/').split('/').collect::<Vec<_>>()
            };
            if template_segments.len() != actual_segments.len() {
                return None;
            }
            let mut params = std::collections::BTreeMap::new();
            for (template_segment, actual_segment) in
                template_segments.into_iter().zip(actual_segments)
            {
                if template_segment.starts_with('{')
                    && template_segment.ends_with('}')
                    && template_segment.len() > 2
                {
                    params.insert(
                        template_segment[1..template_segment.len() - 1].to_string(),
                        actual_segment.to_string(),
                    );
                } else if template_segment != actual_segment {
                    return None;
                }
            }
            Some(params)
        }

        #[allow(dead_code)]
        async fn __selium_read_stream_message(
            stream: &selium_guest::network::StreamChannel,
            max_bytes: u32,
            timeout_ms: u32,
        ) -> anyhow::Result<Vec<u8>> {
            let mut payload = Vec::new();
            loop {
                let Some(chunk) = stream.recv(max_bytes, timeout_ms).await? else {
                    continue;
                };
                payload.extend_from_slice(&chunk.bytes);
                if chunk.finish {
                    return Ok(payload);
                }
            }
        }
    }
}

pub(super) fn schema_tokens(schema: &SchemaDef) -> TokenStream {
    let schema_ident = type_ident(&schema.name);
    let fields = schema.fields.iter().map(|field| {
        let field_ident = field_ident(&field.name);
        let ty = schema_field_type_tokens(&field.ty);
        quote!(pub #field_ident: #ty,)
    });
    let encode_fields = schema.fields.iter().map(|field| {
        let field_ident = field_ident(&field.name);
        quote!(encoder.encode_value(&self.#field_ident)?;)
    });
    let decode_fields = schema.fields.iter().map(|field| {
        let field_ident = field_ident(&field.name);
        quote!(#field_ident: decoder.decode_value()?,)
    });

    quote! {
        #[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
        #[rkyv(bytecheck())]
        pub struct #schema_ident {
            #(#fields)*
        }

        impl selium_abi::CanonicalSerialize for #schema_ident {
            fn encode_to(
                &self,
                encoder: &mut selium_abi::CanonicalEncoder,
            ) -> Result<(), selium_abi::ContractCodecError> {
                #(#encode_fields)*
                Ok(())
            }
        }

        impl selium_abi::CanonicalDeserialize for #schema_ident {
            fn decode_from(
                decoder: &mut selium_abi::CanonicalDecoder<'_>,
            ) -> Result<Self, selium_abi::ContractCodecError> {
                Ok(Self {
                    #(#decode_fields)*
                })
            }
        }
    }
}

pub(super) fn event_const_tokens(event: &EventDef) -> TokenStream {
    let const_ident = event_const_ident(&event.name);
    let name = string_lit(&event.name);
    quote!(pub const #const_ident: &str = #name;)
}

pub(super) fn service_const_tokens(service: &ServiceDef) -> TokenStream {
    let const_ident = service_const_ident(&service.name);
    let name = string_lit(&service.name);
    let request_schema = string_lit(&service.request_schema);
    let response_schema = string_lit(&service.response_schema);
    let protocol = option_string_tokens(service.protocol.as_deref());
    let method = option_string_tokens(service.method.as_deref());
    let path = option_string_tokens(service.path.as_deref());
    let request_headers = field_bindings_tokens(&service.request_headers);
    let request_body = body_binding_tokens(&service.request_body);
    let response_body = body_binding_tokens(&service.response_body);

    quote! {
        pub const #const_ident: ServiceBinding = ServiceBinding {
            name: #name,
            request_schema: #request_schema,
            response_schema: #response_schema,
            protocol: #protocol,
            method: #method,
            path: #path,
            request_headers: #request_headers,
            request_body: #request_body,
            response_body: #response_body,
        };
    }
}

pub(super) fn stream_const_tokens(stream: &StreamDef) -> TokenStream {
    let const_ident = stream_const_ident(&stream.name);
    let name = string_lit(&stream.name);
    quote!(pub const #const_ident: &str = #name;)
}

pub(super) fn parse_type_tokens(ty: &str) -> TokenStream {
    syn::parse_str::<TokenStream>(ty).expect("generated Selium type should parse")
}

pub(super) fn rust_type_tokens(ty: &str) -> TokenStream {
    parse_type_tokens(&rust_type_name(ty))
}

pub(super) fn schema_field_type_tokens(ty: &str) -> TokenStream {
    parse_type_tokens(map_field_type(ty))
}

pub(super) fn type_ident(raw: &str) -> Ident {
    format_ident!("{raw}")
}

pub(super) fn field_ident(raw: &str) -> Ident {
    format_ident!("{raw}")
}

pub(super) fn module_ident(raw: &str) -> Ident {
    format_ident!("{}", module_name(raw))
}

pub(super) fn event_const_ident(raw: &str) -> Ident {
    format_ident!("EVENT_{}", const_name(raw))
}

pub(super) fn service_const_ident(raw: &str) -> Ident {
    format_ident!("SERVICE_{}", const_name(raw))
}

pub(super) fn stream_const_ident(raw: &str) -> Ident {
    format_ident!("STREAM_{}", const_name(raw))
}

pub(super) fn string_lit(value: &str) -> Literal {
    Literal::string(value)
}

pub(super) fn option_string_tokens(value: Option<&str>) -> TokenStream {
    match value {
        Some(value) => {
            let value = string_lit(value);
            quote!(Some(#value))
        }
        None => quote!(None),
    }
}

pub(super) fn body_binding_tokens(body: &ServiceBodyDef) -> TokenStream {
    let mode = match body.mode {
        ServiceBodyMode::None => quote!(ServiceBodyMode::None),
        ServiceBodyMode::Buffered => quote!(ServiceBodyMode::Buffered),
        ServiceBodyMode::Stream => quote!(ServiceBodyMode::Stream),
    };
    let schema = option_string_tokens(body.schema.as_deref());
    quote!(ServiceBodyBinding { mode: #mode, schema: #schema })
}

pub(super) fn field_bindings_tokens(bindings: &[ServiceFieldBinding]) -> TokenStream {
    let bindings = bindings.iter().map(|binding| {
        let field = string_lit(&binding.field);
        let target = string_lit(&binding.target);
        quote!(ServiceFieldBinding { field: #field, target: #target })
    });
    quote!(&[#(#bindings),*])
}

pub(super) fn rust_type_name(ty: &str) -> String {
    match ty {
        "string" => "String".to_string(),
        "bytes" => "Vec<u8>".to_string(),
        "bool" => "bool".to_string(),
        "u8" => "u8".to_string(),
        "u16" => "u16".to_string(),
        "u32" => "u32".to_string(),
        "u64" => "u64".to_string(),
        "i8" => "i8".to_string(),
        "i16" => "i16".to_string(),
        "i32" => "i32".to_string(),
        "i64" => "i64".to_string(),
        "f32" => "f32".to_string(),
        "f64" => "f64".to_string(),
        _ => ty.to_string(),
    }
}

fn map_field_type(ty: &str) -> &'static str {
    match ty {
        "string" => "String",
        "bytes" => "Vec<u8>",
        "bool" => "bool",
        "u8" => "u8",
        "u16" => "u16",
        "u32" => "u32",
        "u64" => "u64",
        "i8" => "i8",
        "i16" => "i16",
        "i32" => "i32",
        "i64" => "i64",
        "f32" => "f32",
        "f64" => "f64",
        _ => "String",
    }
}

fn module_name(raw: &str) -> String {
    let mut out = String::new();
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
        } else if !out.ends_with('_') {
            out.push('_');
        }
    }
    let out = out.trim_matches('_').to_string();
    if out.is_empty() {
        "_contract".to_string()
    } else if out.chars().next().is_some_and(|ch| ch.is_ascii_digit()) {
        format!("_{out}")
    } else {
        out
    }
}

fn const_name(raw: &str) -> String {
    raw.chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{module_ident, rust_type_name, schema_field_type_tokens};

    #[test]
    fn module_ident_sanitizes_symbols_and_leading_digits() {
        assert_eq!(module_ident("camera.raw").to_string(), "camera_raw");
        assert_eq!(module_ident("9-patch").to_string(), "_9_patch");
        assert_eq!(module_ident("///").to_string(), "_contract");
    }

    #[test]
    fn rust_type_name_preserves_named_schema_types() {
        assert_eq!(rust_type_name("string"), "String");
        assert_eq!(rust_type_name("Frame"), "Frame");
    }

    #[test]
    fn unknown_schema_field_types_fall_back_to_string() {
        assert_eq!(schema_field_type_tokens("Frame").to_string(), "String");
    }
}
