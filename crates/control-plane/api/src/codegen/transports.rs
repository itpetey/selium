use proc_macro2::TokenStream;
use quote::quote;

use crate::model::{schema_field_type, schema_fields};
use crate::{ContractPackage, ServiceBodyMode, ServiceDef, StreamDef};

use super::shared::{
    field_ident, module_ident, rust_type_tokens, service_const_ident, stream_const_ident,
    string_lit,
};

pub(super) fn generate_http_service_module(
    package: &ContractPackage,
    service: &ServiceDef,
) -> TokenStream {
    let module = module_ident(&service.name);
    let const_ident = service_const_ident(&service.name);
    let request_type = rust_type_tokens(&service.request_schema);
    let response_body_schema = service
        .response_body
        .schema
        .as_deref()
        .unwrap_or(&service.response_schema);
    let response_body_type = rust_type_tokens(response_body_schema);
    let method = string_lit(service.method.as_deref().unwrap_or("POST"));
    let path = string_lit(service.path.as_deref().unwrap_or("/"));
    let path_params = extract_path_parameters(service.path.as_deref().unwrap_or("/"));
    let request_fields = schema_fields(package, &service.request_schema).unwrap_or(&[]);

    let path_replacements = path_params.iter().map(|param| {
        let field = field_ident(param);
        let placeholder = string_lit(&format!("{{{param}}}"));
        let expr = field_to_string_expr(
            quote!(request.#field),
            schema_field_type(package, &service.request_schema, param).unwrap_or("string"),
            &format!("serialize path parameter `{param}`"),
        );
        quote!(path = path.replace(#placeholder, &#expr);)
    });

    let metadata_inserts = service.request_headers.iter().map(|header| {
        let field = field_ident(&header.field);
        let target = string_lit(&header.target);
        let expr = field_to_string_expr(
            quote!(request.#field),
            schema_field_type(package, &service.request_schema, &header.field).unwrap_or("string"),
            &format!("serialize header `{}`", header.target),
        );
        quote!(metadata.insert(#target.to_string(), #expr);)
    });

    let request_inits = request_fields.iter().filter_map(|field| {
        let field_ident = field_ident(&field.name);
        if path_params.iter().any(|param| param == &field.name) {
            let field_name = string_lit(&field.name);
            let source = quote!(
                params
                    .get(#field_name)
                    .ok_or_else(|| anyhow::anyhow!(concat!("missing path parameter `", #field_name, "`")))?
            );
            let expr = field_from_string_expr(
                source,
                &field.ty,
                &format!("parse path parameter `{}`", field.name),
            );
            Some(quote!(#field_ident: #expr,))
        } else {
            service
                .request_headers
                .iter()
                .find(|binding| binding.field == field.name)
                .map(|binding| {
                    let target = string_lit(&binding.target);
                    let source = quote!(
                        head
                            .metadata
                            .get(#target)
                            .ok_or_else(|| anyhow::anyhow!(concat!("missing header `", #target, "`")))?
                    );
                    let expr = field_from_string_expr(
                        source,
                        &field.ty,
                        &format!("parse header `{}`", binding.target),
                    );
                    quote!(#field_ident: #expr,)
                })
        }
    });

    let await_buffered_response = if service.response_body.mode == ServiceBodyMode::Buffered {
        quote! {
            pub async fn await_buffered_response(
                self,
                max_bytes: u32,
                timeout_ms: u32,
            ) -> anyhow::Result<Option<#response_body_type>> {
                match self.await_response(timeout_ms).await? {
                    Some(response) => Ok(Some(response.decode_buffered(max_bytes, timeout_ms).await?)),
                    None => Ok(None),
                }
            }
        }
    } else {
        TokenStream::new()
    };

    let decode_buffered = if service.response_body.mode == ServiceBodyMode::Buffered {
        let decode_expr = if response_body_schema == "bytes" {
            quote!(Ok(body))
        } else {
            quote!(
                selium_abi::decode_rkyv::<#response_body_type>(&body)
                    .context("decode buffered response body")
            )
        };
        quote! {
            pub async fn decode_buffered(
                self,
                max_bytes: u32,
                timeout_ms: u32,
            ) -> anyhow::Result<#response_body_type> {
                let body = self.body.read_all(max_bytes, timeout_ms).await?;
                #decode_expr
            }
        }
    } else {
        TokenStream::new()
    };

    let respond_buffered = if service.response_body.mode == ServiceBodyMode::Buffered {
        let response_body_expr = if response_body_schema == "bytes" {
            quote!(response.clone())
        } else {
            quote!(selium_abi::encode_rkyv(response).context("encode buffered response body")?)
        };
        quote! {
            pub async fn respond_buffered(
                self,
                response: &#response_body_type,
                timeout_ms: u32,
            ) -> anyhow::Result<()> {
                self.exchange
                    .respond(
                        selium_abi::NetworkRpcResponse {
                            head: selium_abi::NetworkRpcResponseHead {
                                status: 200,
                                metadata: std::collections::BTreeMap::new(),
                            },
                            body: #response_body_expr,
                        },
                        timeout_ms,
                    )
                    .await?;
                Ok(())
            }
        }
    } else {
        TokenStream::new()
    };

    let unexpected_method_msg = string_lit(&format!(
        "unexpected HTTP method for {}: {{}}",
        service.name
    ));
    let unexpected_path_msg =
        string_lit(&format!("unexpected HTTP path for {}: {{}}", service.name));

    quote! {
        #[allow(dead_code)]
        pub mod #module {
            use super::*;

            pub const DEF: ServiceBinding = super::#const_ident;

            pub struct Client<'a> {
                session: &'a selium_guest::network::Session,
            }

            pub struct PendingRequest {
                exchange: selium_guest::network::ClientExchange,
                request_body: selium_guest::network::BodyWriter,
            }

            pub struct Response {
                pub head: selium_abi::NetworkRpcResponseHead,
                pub body: selium_guest::network::BodyReader,
            }

            pub struct AcceptedRequest {
                exchange: selium_guest::network::ServerExchange,
                request: #request_type,
            }

            impl<'a> Client<'a> {
                pub fn new(session: &'a selium_guest::network::Session) -> Self {
                    Self { session }
                }

                pub async fn start(
                    &self,
                    request: &#request_type,
                    timeout_ms: u32,
                ) -> anyhow::Result<Option<PendingRequest>> {
                    let request_head = build_request_head(request)?;
                    Ok(selium_guest::network::rpc::start(self.session, request_head, timeout_ms)
                        .await?
                        .map(|(exchange, request_body)| PendingRequest {
                            exchange,
                            request_body,
                        }))
                }
            }

            impl PendingRequest {
                pub fn request_body(&self) -> &selium_guest::network::BodyWriter {
                    &self.request_body
                }

                pub async fn await_response(
                    self,
                    timeout_ms: u32,
                ) -> anyhow::Result<Option<Response>> {
                    Ok(self.exchange.await_response(timeout_ms).await?.map(|(head, body)| {
                        Response { head, body }
                    }))
                }

                #await_buffered_response
            }

            impl Response {
                #decode_buffered
            }

            pub async fn accept(
                session: &selium_guest::network::Session,
                timeout_ms: u32,
            ) -> anyhow::Result<Option<AcceptedRequest>> {
                let Some(exchange) = selium_guest::network::rpc::accept(session, timeout_ms).await? else {
                    return Ok(None);
                };
                let request = parse_request_head(exchange.request_head())?;
                Ok(Some(AcceptedRequest { exchange, request }))
            }

            impl AcceptedRequest {
                pub fn request(&self) -> &#request_type {
                    &self.request
                }

                pub fn request_body(&self) -> &selium_guest::network::BodyReader {
                    self.exchange.request_body()
                }

                #respond_buffered
            }

            fn build_request_head(
                request: &#request_type,
            ) -> anyhow::Result<selium_abi::NetworkRpcRequestHead> {
                let mut path = #path.to_string();
                #(#path_replacements)*
                let mut metadata = std::collections::BTreeMap::new();
                #(#metadata_inserts)*
                Ok(selium_abi::NetworkRpcRequestHead {
                    method: #method.to_string(),
                    path,
                    metadata,
                })
            }

            fn parse_request_head(
                head: &selium_abi::NetworkRpcRequestHead,
            ) -> anyhow::Result<#request_type> {
                if head.method != #method {
                    return Err(anyhow::anyhow!(#unexpected_method_msg, head.method));
                }
                let params = __selium_extract_path_params(#path, &head.path)
                    .ok_or_else(|| anyhow::anyhow!(#unexpected_path_msg, head.path))?;
                Ok(#request_type {
                    #(#request_inits)*
                })
            }
        }
    }
}

pub(super) fn generate_quic_service_module(service: &ServiceDef) -> TokenStream {
    let module = module_ident(&service.name);
    let const_ident = service_const_ident(&service.name);
    let request_type = rust_type_tokens(&service.request_schema);
    let response_type = rust_type_tokens(&service.response_schema);
    let method = string_lit(service.method.as_deref().unwrap_or(&service.name));
    let path = string_lit(service.path.as_deref().unwrap_or(""));

    let request_body_schema = service
        .request_body
        .schema
        .as_deref()
        .unwrap_or(&service.request_schema);
    let response_body_schema = service
        .response_body
        .schema
        .as_deref()
        .unwrap_or(&service.response_schema);

    let request_body_expr = if request_body_schema == "bytes" {
        quote!(request.clone())
    } else {
        quote!(selium_abi::encode_rkyv(request).context("encode QUIC request")?)
    };

    let response_decode_expr = if response_body_schema == "bytes" {
        quote!(Ok(Some(response.body)))
    } else {
        quote!(
            Ok(Some(
                selium_abi::decode_rkyv::<#response_type>(&response.body)
                    .context("decode QUIC response")?,
            ))
        )
    };

    let request_decode = if request_body_schema == "bytes" {
        quote!(let request = buffered.body;)
    } else {
        quote!(
            let request = selium_abi::decode_rkyv::<#request_type>(&buffered.body)
                .context("decode QUIC request")?;
        )
    };

    let response_body_expr = if response_body_schema == "bytes" {
        quote!(response.clone())
    } else {
        quote!(selium_abi::encode_rkyv(response).context("encode QUIC response")?)
    };

    quote! {
        #[allow(dead_code)]
        pub mod #module {
            use super::*;

            pub const DEF: ServiceBinding = super::#const_ident;

            pub struct Client<'a> {
                session: &'a selium_guest::network::Session,
            }

            pub struct AcceptedRequest {
                exchange: selium_guest::network::ServerExchange,
                request: #request_type,
            }

            impl<'a> Client<'a> {
                pub fn new(session: &'a selium_guest::network::Session) -> Self {
                    Self { session }
                }

                pub async fn invoke(
                    &self,
                    request: &#request_type,
                    timeout_ms: u32,
                ) -> anyhow::Result<Option<#response_type>> {
                    let Some(response) = selium_guest::network::rpc::invoke(
                        self.session,
                        selium_abi::NetworkRpcRequest {
                            head: selium_abi::NetworkRpcRequestHead {
                                method: #method.to_string(),
                                path: #path.to_string(),
                                metadata: std::collections::BTreeMap::new(),
                            },
                            body: #request_body_expr,
                        },
                        timeout_ms,
                    )
                    .await? else {
                        return Ok(None);
                    };
                    #response_decode_expr
                }
            }

            pub async fn accept(
                session: &selium_guest::network::Session,
                timeout_ms: u32,
            ) -> anyhow::Result<Option<AcceptedRequest>> {
                let Some(exchange) = selium_guest::network::rpc::accept(session, timeout_ms).await? else {
                    return Ok(None);
                };
                if exchange.request_head().method != #method || exchange.request_head().path != #path {
                    return Err(anyhow::anyhow!("unexpected QUIC RPC route"));
                }
                let buffered = exchange.buffered_request(8192, timeout_ms).await?;
                #request_decode
                Ok(Some(AcceptedRequest { exchange, request }))
            }

            impl AcceptedRequest {
                pub fn request(&self) -> &#request_type {
                    &self.request
                }

                pub async fn respond(
                    self,
                    response: &#response_type,
                    timeout_ms: u32,
                ) -> anyhow::Result<()> {
                    self.exchange
                        .respond(
                            selium_abi::NetworkRpcResponse {
                                head: selium_abi::NetworkRpcResponseHead {
                                    status: 200,
                                    metadata: std::collections::BTreeMap::new(),
                                },
                                body: #response_body_expr,
                            },
                            timeout_ms,
                        )
                        .await?;
                    Ok(())
                }
            }
        }
    }
}

pub(super) fn generate_stream_module(stream: &StreamDef) -> TokenStream {
    let module = module_ident(&stream.name);
    let const_ident = stream_const_ident(&stream.name);
    let payload_type = rust_type_tokens(&stream.payload_schema);
    let send_body = if stream.payload_schema == "bytes" {
        quote!(stream.send(payload.clone(), true, timeout_ms).await?;)
    } else {
        quote!(
            stream
                .send(
                    selium_abi::encode_rkyv(payload).context("encode QUIC stream payload")?,
                    true,
                    timeout_ms,
                )
                .await?;
        )
    };
    let recv_expr = if stream.payload_schema == "bytes" {
        quote!(Ok(payload))
    } else {
        quote!(
            selium_abi::decode_rkyv::<#payload_type>(&payload)
                .context("decode QUIC stream payload")
        )
    };

    quote! {
        #[allow(dead_code)]
        pub mod #module {
            use super::*;

            pub const DEF: &str = super::#const_ident;

            pub async fn open(
                session: &selium_guest::network::Session,
            ) -> anyhow::Result<Option<selium_guest::network::StreamChannel>> {
                Ok(selium_guest::network::stream::open(session).await?)
            }

            pub async fn accept(
                session: &selium_guest::network::Session,
                timeout_ms: u32,
            ) -> anyhow::Result<Option<selium_guest::network::StreamChannel>> {
                Ok(selium_guest::network::stream::accept(session, timeout_ms).await?)
            }

            pub async fn send(
                stream: &selium_guest::network::StreamChannel,
                payload: &#payload_type,
                timeout_ms: u32,
            ) -> anyhow::Result<()> {
                #send_body
                Ok(())
            }

            pub async fn recv(
                stream: &selium_guest::network::StreamChannel,
                max_bytes: u32,
                timeout_ms: u32,
            ) -> anyhow::Result<#payload_type> {
                let payload = __selium_read_stream_message(stream, max_bytes, timeout_ms).await?;
                #recv_expr
            }
        }
    }
}

fn field_to_string_expr(access: TokenStream, ty: &str, context: &str) -> TokenStream {
    let context = string_lit(context);
    match ty {
        "string" => quote!((#access).clone()),
        "bytes" => quote!(String::from_utf8((#access).clone()).context(#context)?),
        "bool" | "u8" | "u16" | "u32" | "u64" | "i8" | "i16" | "i32" | "i64" | "f32" | "f64" => {
            quote!((#access).to_string())
        }
        _ => quote!(selium_abi::encode_rkyv(&(#access)).context(#context)?.len().to_string()),
    }
}

fn field_from_string_expr(source: TokenStream, ty: &str, context: &str) -> TokenStream {
    let context = string_lit(context);
    match ty {
        "string" => quote!((#source).to_string()),
        "bytes" => quote!((#source).as_bytes().to_vec()),
        "bool" | "u8" | "u16" | "u32" | "u64" | "i8" | "i16" | "i32" | "i64" | "f32" | "f64" => {
            let ty = rust_type_tokens(ty);
            quote!((#source).parse::<#ty>().with_context(|| #context.to_string())?)
        }
        _ => quote!(unreachable!("unsupported HTTP field binding type")),
    }
}

fn extract_path_parameters(path: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = path;
    while let Some(start) = rest.find('{') {
        rest = &rest[start + 1..];
        let Some(end) = rest.find('}') else {
            break;
        };
        let name = rest[..end].trim();
        if !name.is_empty() {
            out.push(name.to_string());
        }
        rest = &rest[end + 1..];
    }
    out
}

#[cfg(test)]
mod tests {
    use crate::{
        ContractPackage, SchemaDef, SchemaField, ServiceBodyDef, ServiceBodyMode, ServiceDef,
        ServiceFieldBinding, StreamDef,
    };

    use super::{extract_path_parameters, generate_http_service_module, generate_stream_module};

    fn package_with_upload_service() -> (ContractPackage, ServiceDef) {
        let service = ServiceDef {
            name: "upload".to_string(),
            request_schema: "UploadHead".to_string(),
            response_schema: "Frame".to_string(),
            protocol: Some("http".to_string()),
            method: Some("POST".to_string()),
            path: Some("/upload/{camera_id}".to_string()),
            request_headers: vec![ServiceFieldBinding {
                field: "ts_ms".to_string(),
                target: "x-ts-ms".to_string(),
            }],
            request_body: ServiceBodyDef {
                mode: ServiceBodyMode::Buffered,
                schema: Some("UploadHead".to_string()),
            },
            response_body: ServiceBodyDef {
                mode: ServiceBodyMode::Buffered,
                schema: Some("Frame".to_string()),
            },
        };
        let package = ContractPackage {
            package: "media.pipeline.v1".to_string(),
            namespace: "media.pipeline".to_string(),
            version: "v1".to_string(),
            schemas: vec![
                SchemaDef {
                    name: "UploadHead".to_string(),
                    fields: vec![
                        SchemaField {
                            name: "camera_id".to_string(),
                            ty: "string".to_string(),
                        },
                        SchemaField {
                            name: "ts_ms".to_string(),
                            ty: "u64".to_string(),
                        },
                    ],
                },
                SchemaDef {
                    name: "Frame".to_string(),
                    fields: Vec::new(),
                },
            ],
            events: Vec::new(),
            services: vec![service.clone()],
            streams: Vec::new(),
        };

        (package, service)
    }

    #[test]
    fn extract_path_parameters_returns_named_segments() {
        assert_eq!(
            extract_path_parameters("/upload/{camera_id}/raw/{frame_id}"),
            vec!["camera_id".to_string(), "frame_id".to_string()]
        );
    }

    #[test]
    fn http_service_module_contains_path_and_header_binding_logic() {
        let (package, service) = package_with_upload_service();
        let tokens = generate_http_service_module(&package, &service).to_string();

        assert!(tokens.contains("replace"));
        assert!(tokens.contains("{camera_id}"));
        assert!(tokens.contains("x-ts-ms"));
        assert!(tokens.contains("parse :: < u64 >"));
    }

    #[test]
    fn stream_module_uses_raw_bytes_for_byte_streams() {
        let tokens = generate_stream_module(&StreamDef {
            name: "camera.raw".to_string(),
            payload_schema: "bytes".to_string(),
        })
        .to_string();

        assert!(tokens.contains("stream"));
        assert!(tokens.contains("payload . clone ()"));
        assert!(!tokens.contains("encode_rkyv"));
        assert!(!tokens.contains("decode_rkyv"));
    }
}
