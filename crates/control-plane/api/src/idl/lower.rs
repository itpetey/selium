use crate::{
    ApiError, ContractPackage, DeliveryGuarantee, EventDef, SchemaDef, SchemaField, ServiceBodyDef,
    ServiceBodyMode, ServiceDef, ServiceFieldBinding, StreamDef,
};

use super::ast::{
    AstEventDef, AstServiceDef, IdlDocument, Span, ValueToken, parse_error_at, render_value_tokens,
};
use super::validate::validate_package;

pub(super) fn lower_document(
    document: IdlDocument,
) -> std::result::Result<ContractPackage, ApiError> {
    let package = document
        .package
        .ok_or_else(|| ApiError::Parse("missing package declaration".into()))?;
    let (namespace, version) = split_namespace_version(&package.name);

    let schemas = document
        .schemas
        .into_iter()
        .map(|schema| SchemaDef {
            name: schema.name,
            fields: schema
                .fields
                .into_iter()
                .map(|field| SchemaField {
                    name: field.name,
                    ty: field.ty,
                })
                .collect(),
        })
        .collect();

    let events = document
        .events
        .into_iter()
        .map(lower_event)
        .collect::<std::result::Result<Vec<_>, _>>()?;

    let services = document
        .services
        .into_iter()
        .map(lower_service)
        .collect::<std::result::Result<Vec<_>, _>>()?;

    let streams = document
        .streams
        .into_iter()
        .map(|stream| StreamDef {
            name: stream.name,
            payload_schema: stream.payload_schema,
        })
        .collect();

    let package = ContractPackage {
        package: package.name,
        namespace,
        version,
        schemas,
        events,
        services,
        streams,
    };
    validate_package(&package)?;
    Ok(package)
}

fn lower_scalar_value(
    span: Span,
    value: &[ValueToken],
    context: &str,
) -> std::result::Result<String, ApiError> {
    match value {
        [ValueToken::Ident(text)] | [ValueToken::String(text)] => Ok(text.clone()),
        _ => Err(parse_error_at(
            span,
            format!("invalid {context}: {}", render_value_tokens(value)),
        )),
    }
}

fn lower_service_body(
    span: Span,
    value: &[ValueToken],
) -> std::result::Result<ServiceBodyDef, ApiError> {
    match value {
        [ValueToken::Ident(text)] | [ValueToken::String(text)] => {
            if text.eq_ignore_ascii_case("none") {
                Ok(ServiceBodyDef {
                    mode: ServiceBodyMode::None,
                    schema: None,
                })
            } else {
                Ok(ServiceBodyDef {
                    mode: ServiceBodyMode::Buffered,
                    schema: Some(text.clone()),
                })
            }
        }
        [
            ValueToken::Ident(mode),
            ValueToken::LAngle,
            ValueToken::Ident(schema),
            ValueToken::RAngle,
        ]
        | [
            ValueToken::Ident(mode),
            ValueToken::LAngle,
            ValueToken::String(schema),
            ValueToken::RAngle,
        ] => {
            if schema.is_empty() {
                return Err(parse_error_at(
                    span,
                    "service body schema must not be empty".to_string(),
                ));
            }

            let mode = match mode.as_str() {
                "buffered" => ServiceBodyMode::Buffered,
                "stream" => ServiceBodyMode::Stream,
                _ => {
                    return Err(parse_error_at(
                        span,
                        format!(
                            "invalid service body definition: {}",
                            render_value_tokens(value)
                        ),
                    ));
                }
            };

            Ok(ServiceBodyDef {
                mode,
                schema: Some(schema.clone()),
            })
        }
        _ => Err(parse_error_at(
            span,
            format!(
                "invalid service body definition: {}",
                render_value_tokens(value)
            ),
        )),
    }
}

fn lower_service_binding(
    span: Span,
    value: &[ValueToken],
) -> std::result::Result<ServiceFieldBinding, ApiError> {
    match value {
        [
            ValueToken::Ident(field),
            ValueToken::Eq,
            ValueToken::Ident(target),
        ]
        | [
            ValueToken::Ident(field),
            ValueToken::Eq,
            ValueToken::String(target),
        ] => {
            if field.is_empty() || target.is_empty() {
                return Err(parse_error_at(
                    span,
                    format!(
                        "invalid service field binding: {}",
                        render_value_tokens(value)
                    ),
                ));
            }

            Ok(ServiceFieldBinding {
                field: field.clone(),
                target: target.clone(),
            })
        }
        _ => Err(parse_error_at(
            span,
            format!(
                "invalid service field binding: {}",
                render_value_tokens(value)
            ),
        )),
    }
}

fn lower_event(event: AstEventDef) -> std::result::Result<EventDef, ApiError> {
    let mut partitions = 1;
    let mut retention = "24h".to_string();
    let mut delivery = DeliveryGuarantee::AtLeastOnce;
    let mut replay_enabled = true;

    for property in event.properties {
        match property.key.as_str() {
            "partitions" => {
                let value = lower_scalar_value(property.span, &property.value, "partitions value")?;
                partitions = value.parse::<u16>().map_err(|_| {
                    parse_error_at(
                        property.span,
                        format!("invalid partitions value `{value}` in event"),
                    )
                })?;
            }
            "retention" => {
                retention = lower_scalar_value(property.span, &property.value, "retention value")?;
            }
            "delivery" => {
                let value = lower_scalar_value(property.span, &property.value, "delivery value")?;
                delivery = match value.as_str() {
                    "at_least_once" => DeliveryGuarantee::AtLeastOnce,
                    "at_most_once" => DeliveryGuarantee::AtMostOnce,
                    "exactly_once" => DeliveryGuarantee::ExactlyOnce,
                    _ => {
                        return Err(parse_error_at(
                            property.span,
                            format!("unknown delivery guarantee `{value}`"),
                        ));
                    }
                };
            }
            "replay" => {
                let value = lower_scalar_value(property.span, &property.value, "replay value")?;
                replay_enabled = match value.as_str() {
                    "enabled" | "true" => true,
                    "disabled" | "false" => false,
                    _ => {
                        return Err(parse_error_at(
                            property.span,
                            format!("unknown replay value `{value}`"),
                        ));
                    }
                };
            }
            _ => {
                return Err(parse_error_at(
                    property.span,
                    format!("unknown event property `{}`", property.key),
                ));
            }
        }
    }

    Ok(EventDef {
        name: event.name,
        payload_schema: event.payload_schema,
        partitions,
        retention,
        delivery,
        replay_enabled,
    })
}

fn lower_service(service: AstServiceDef) -> std::result::Result<ServiceDef, ApiError> {
    let mut service_def = ServiceDef {
        name: service.name,
        request_schema: service.request_schema.clone(),
        response_schema: service.response_schema.clone(),
        protocol: None,
        method: None,
        path: None,
        request_headers: Vec::new(),
        request_body: ServiceBodyDef {
            mode: ServiceBodyMode::Buffered,
            schema: Some(service.request_schema),
        },
        response_body: ServiceBodyDef {
            mode: ServiceBodyMode::Buffered,
            schema: Some(service.response_schema),
        },
    };

    for property in service.properties {
        match property.key.as_str() {
            "protocol" => {
                service_def.protocol = Some(
                    lower_scalar_value(property.span, &property.value, "protocol value")?
                        .to_ascii_lowercase(),
                );
            }
            "method" => {
                service_def.method = Some(lower_scalar_value(
                    property.span,
                    &property.value,
                    "method value",
                )?);
            }
            "path" => {
                service_def.path = Some(lower_scalar_value(
                    property.span,
                    &property.value,
                    "path value",
                )?);
            }
            "request-header" => {
                service_def
                    .request_headers
                    .push(lower_service_binding(property.span, &property.value)?);
            }
            "request-body" => {
                service_def.request_body = lower_service_body(property.span, &property.value)?;
            }
            "response-body" => {
                service_def.response_body = lower_service_body(property.span, &property.value)?;
            }
            _ => {
                return Err(parse_error_at(
                    property.span,
                    format!("unknown service property `{}`", property.key),
                ));
            }
        }
    }

    Ok(service_def)
}

fn split_namespace_version(package: &str) -> (String, String) {
    let mut parts = package.split('.').collect::<Vec<_>>();
    if let Some(last) = parts.last().copied()
        && last.starts_with('v')
        && last[1..].chars().all(|ch| ch.is_ascii_digit())
    {
        parts.pop();
        let namespace = parts.join(".");
        return (namespace, last.to_string());
    }

    (package.to_string(), "v1".to_string())
}

#[cfg(test)]
mod tests {
    use crate::{
        ApiError, ServiceBodyMode,
        idl::ast::{AstPackageDecl, AstProperty, AstSchemaDef, AstServiceDef, IdlDocument, Span},
    };

    use super::{ValueToken, lower_document, split_namespace_version};

    #[test]
    fn split_namespace_version_defaults_to_v1_when_absent() {
        assert_eq!(
            split_namespace_version("media.pipeline"),
            ("media.pipeline".to_string(), "v1".to_string())
        );
        assert_eq!(
            split_namespace_version("media.pipeline.v12"),
            ("media.pipeline".to_string(), "v12".to_string())
        );
    }

    #[test]
    fn lower_document_applies_default_service_bodies() {
        let document = IdlDocument {
            package: Some(AstPackageDecl {
                name: "media.pipeline.v1".to_string(),
                span: Span { line: 1, column: 1 },
            }),
            schemas: vec![
                AstSchemaDef {
                    name: "Req".to_string(),
                    span: Span { line: 2, column: 1 },
                    fields: Vec::new(),
                },
                AstSchemaDef {
                    name: "Res".to_string(),
                    span: Span { line: 3, column: 1 },
                    fields: Vec::new(),
                },
            ],
            events: Vec::new(),
            services: vec![AstServiceDef {
                name: "detect".to_string(),
                request_schema: "Req".to_string(),
                response_schema: "Res".to_string(),
                span: Span { line: 4, column: 1 },
                properties: Vec::new(),
            }],
            streams: Vec::new(),
        };

        let package = lower_document(document).expect("lower");
        let service = &package.services[0];
        assert_eq!(service.request_body.mode, ServiceBodyMode::Buffered);
        assert_eq!(service.request_body.schema.as_deref(), Some("Req"));
        assert_eq!(service.response_body.mode, ServiceBodyMode::Buffered);
        assert_eq!(service.response_body.schema.as_deref(), Some("Res"));
    }

    #[test]
    fn lower_document_rejects_unknown_service_properties() {
        let document = IdlDocument {
            package: Some(AstPackageDecl {
                name: "media.pipeline.v1".to_string(),
                span: Span { line: 1, column: 1 },
            }),
            schemas: vec![
                AstSchemaDef {
                    name: "Req".to_string(),
                    span: Span { line: 2, column: 1 },
                    fields: Vec::new(),
                },
                AstSchemaDef {
                    name: "Res".to_string(),
                    span: Span { line: 3, column: 1 },
                    fields: Vec::new(),
                },
            ],
            events: Vec::new(),
            services: vec![AstServiceDef {
                name: "detect".to_string(),
                request_schema: "Req".to_string(),
                response_schema: "Res".to_string(),
                span: Span { line: 4, column: 1 },
                properties: vec![AstProperty {
                    key: "unexpected".to_string(),
                    span: Span { line: 5, column: 3 },
                    value: vec![ValueToken::Ident("value".to_string())],
                }],
            }],
            streams: Vec::new(),
        };

        let err = lower_document(document).expect_err("must fail");
        assert!(matches!(err, ApiError::Parse(_)));
        assert!(
            err.to_string()
                .contains("unknown service property `unexpected`")
        );
    }
}
