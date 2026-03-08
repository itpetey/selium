use std::collections::BTreeSet;

use crate::model::schema_fields;
use crate::{ApiError, ContractPackage};

pub(super) fn validate_package(package: &ContractPackage) -> std::result::Result<(), ApiError> {
    for service in &package.services {
        if service.protocol.as_deref() != Some("http") {
            continue;
        }

        let fields = schema_fields(package, &service.request_schema).ok_or_else(|| {
            ApiError::Parse(format!(
                "HTTP service `{}` references unknown request schema `{}`",
                service.name, service.request_schema
            ))
        })?;

        let path_params = service
            .path
            .as_deref()
            .map(extract_path_parameters)
            .unwrap_or_default();
        let mut bound_fields = BTreeSet::new();

        for path_param in path_params {
            if !fields.iter().any(|field| field.name == path_param) {
                return Err(ApiError::Parse(format!(
                    "HTTP service `{}` path binds unknown field `{}`",
                    service.name, path_param
                )));
            }
            bound_fields.insert(path_param);
        }

        for header in &service.request_headers {
            if !fields.iter().any(|field| field.name == header.field) {
                return Err(ApiError::Parse(format!(
                    "HTTP service `{}` header binding references unknown field `{}`",
                    service.name, header.field
                )));
            }
            bound_fields.insert(header.field.clone());
        }

        for field in fields {
            if !bound_fields.contains(&field.name) {
                return Err(ApiError::Parse(format!(
                    "HTTP service `{}` must bind request field `{}` to path or header",
                    service.name, field.name
                )));
            }
        }
    }

    Ok(())
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
        ContractPackage, DeliveryGuarantee, EventDef, SchemaDef, SchemaField, ServiceBodyDef,
        ServiceBodyMode, ServiceDef,
    };

    use super::{extract_path_parameters, validate_package};

    fn http_service_package(request_headers: Vec<crate::ServiceFieldBinding>) -> ContractPackage {
        ContractPackage {
            package: "media.pipeline.v1".to_string(),
            namespace: "media.pipeline".to_string(),
            version: "v1".to_string(),
            schemas: vec![SchemaDef {
                name: "Upload".to_string(),
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
            }],
            events: vec![EventDef {
                name: "camera.frames".to_string(),
                payload_schema: "Upload".to_string(),
                partitions: 1,
                retention: "24h".to_string(),
                delivery: DeliveryGuarantee::AtLeastOnce,
                replay_enabled: true,
            }],
            services: vec![ServiceDef {
                name: "upload".to_string(),
                request_schema: "Upload".to_string(),
                response_schema: "Upload".to_string(),
                protocol: Some("http".to_string()),
                method: Some("POST".to_string()),
                path: Some("/upload/{camera_id}".to_string()),
                request_headers,
                request_body: ServiceBodyDef {
                    mode: ServiceBodyMode::Buffered,
                    schema: Some("Upload".to_string()),
                },
                response_body: ServiceBodyDef {
                    mode: ServiceBodyMode::Buffered,
                    schema: Some("Upload".to_string()),
                },
            }],
            streams: Vec::new(),
        }
    }

    #[test]
    fn extract_path_parameters_ignores_empty_placeholders() {
        assert_eq!(
            extract_path_parameters("/upload/{camera_id}/raw/{}/ts/{ts_ms}"),
            vec!["camera_id".to_string(), "ts_ms".to_string()]
        );
    }

    #[test]
    fn validate_package_rejects_unbound_http_request_fields() {
        let package = http_service_package(Vec::new());
        let err = validate_package(&package).expect_err("must fail");

        assert!(
            err.to_string()
                .contains("must bind request field `ts_ms` to path or header")
        );
    }

    #[test]
    fn validate_package_accepts_complete_http_bindings() {
        let package = http_service_package(vec![crate::ServiceFieldBinding {
            field: "ts_ms".to_string(),
            target: "x-ts-ms".to_string(),
        }]);

        validate_package(&package).expect("valid package");
    }
}
