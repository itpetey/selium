use std::collections::BTreeMap;

use crate::{
    ApiError, CompatibilityReport, ContractKind, ContractPackage, ContractRef, ContractRegistry,
    ResolvedContract,
};

impl ContractRegistry {
    pub fn register_package(
        &mut self,
        package: ContractPackage,
    ) -> std::result::Result<CompatibilityReport, ApiError> {
        let namespace = package.namespace.clone();
        let version = package.version.clone();

        let versions = self.packages.entry(namespace.clone()).or_default();
        if versions.contains_key(&version) {
            return Err(ApiError::DuplicateVersion { namespace, version });
        }

        let mut report = CompatibilityReport {
            previous_version: String::new(),
            current_version: package.version.clone(),
            warnings: Vec::new(),
        };

        if let Some((prev_version, prev_pkg)) = highest_version(versions) {
            ensure_compatible(prev_pkg, &package).map_err(|reason| ApiError::Incompatible {
                namespace: package.namespace.clone(),
                from: prev_version.to_string(),
                to: package.version.clone(),
                reason,
            })?;
            report.previous_version = prev_version.to_string();
            if prev_pkg.events.len() != package.events.len() {
                report
                    .warnings
                    .push("event set changed between versions".to_string());
            }
        }

        versions.insert(package.version.clone(), package);
        Ok(report)
    }

    pub fn resolve(&self, contract: &ContractRef) -> Option<ResolvedContract> {
        let package = self
            .packages
            .get(&contract.namespace)?
            .get(&contract.version)?;

        if let Some(event) = package
            .events
            .iter()
            .find(|event| event.name == contract.name)
        {
            return Some(ResolvedContract {
                kind: ContractKind::Event,
                schema: Some(event.payload_schema.clone()),
            });
        }

        if let Some(service) = package
            .services
            .iter()
            .find(|service| service.name == contract.name)
        {
            return Some(ResolvedContract {
                kind: ContractKind::Service,
                schema: Some(service.request_schema.clone()),
            });
        }

        if let Some(stream) = package
            .streams
            .iter()
            .find(|stream| stream.name == contract.name)
        {
            return Some(ResolvedContract {
                kind: ContractKind::Stream,
                schema: Some(stream.payload_schema.clone()),
            });
        }

        None
    }

    pub fn has_contract(&self, contract: &ContractRef) -> bool {
        self.resolve(contract).is_some()
    }
}

fn highest_version(
    versions: &BTreeMap<String, ContractPackage>,
) -> Option<(&String, &ContractPackage)> {
    versions
        .iter()
        .max_by_key(|(version, _)| version_number(version))
}

fn version_number(version: &str) -> u64 {
    version
        .strip_prefix('v')
        .and_then(|num| num.parse::<u64>().ok())
        .unwrap_or(0)
}

fn ensure_compatible(
    previous: &ContractPackage,
    next: &ContractPackage,
) -> std::result::Result<(), String> {
    let previous_schemas = previous
        .schemas
        .iter()
        .map(|schema| (schema.name.as_str(), schema))
        .collect::<BTreeMap<_, _>>();
    let next_schemas = next
        .schemas
        .iter()
        .map(|schema| (schema.name.as_str(), schema))
        .collect::<BTreeMap<_, _>>();

    for (name, old_schema) in previous_schemas {
        let new_schema = next_schemas
            .get(name)
            .ok_or_else(|| format!("schema `{name}` was removed"))?;

        let old_fields = old_schema
            .fields
            .iter()
            .map(|field| (field.name.as_str(), field.ty.as_str()))
            .collect::<BTreeMap<_, _>>();
        let new_fields = new_schema
            .fields
            .iter()
            .map(|field| (field.name.as_str(), field.ty.as_str()))
            .collect::<BTreeMap<_, _>>();

        for (field_name, field_ty) in old_fields {
            let Some(new_ty) = new_fields.get(field_name) else {
                return Err(format!(
                    "schema `{name}` removed field `{field_name}` from prior version"
                ));
            };

            if field_ty != *new_ty {
                return Err(format!(
                    "schema `{name}` changed field `{field_name}` type `{field_ty}` -> `{new_ty}`"
                ));
            }
        }
    }

    let previous_events = previous
        .events
        .iter()
        .map(|event| (event.name.as_str(), event))
        .collect::<BTreeMap<_, _>>();
    let next_events = next
        .events
        .iter()
        .map(|event| (event.name.as_str(), event))
        .collect::<BTreeMap<_, _>>();

    for (name, old_event) in previous_events {
        let Some(new_event) = next_events.get(name) else {
            return Err(format!("event `{name}` was removed"));
        };

        if old_event.payload_schema != new_event.payload_schema {
            return Err(format!(
                "event `{name}` changed payload schema `{}` -> `{}`",
                old_event.payload_schema, new_event.payload_schema
            ));
        }
    }

    let previous_services = previous
        .services
        .iter()
        .map(|service| (service.name.as_str(), service))
        .collect::<BTreeMap<_, _>>();
    let next_services = next
        .services
        .iter()
        .map(|service| (service.name.as_str(), service))
        .collect::<BTreeMap<_, _>>();

    for (name, old_service) in previous_services {
        let Some(new_service) = next_services.get(name) else {
            return Err(format!("service `{name}` was removed"));
        };

        if old_service.request_schema != new_service.request_schema {
            return Err(format!(
                "service `{name}` changed request schema `{}` -> `{}`",
                old_service.request_schema, new_service.request_schema
            ));
        }
        if old_service.response_schema != new_service.response_schema {
            return Err(format!(
                "service `{name}` changed response schema `{}` -> `{}`",
                old_service.response_schema, new_service.response_schema
            ));
        }
        if old_service.protocol != new_service.protocol
            || old_service.method != new_service.method
            || old_service.path != new_service.path
            || old_service.request_headers != new_service.request_headers
            || old_service.request_body != new_service.request_body
            || old_service.response_body != new_service.response_body
        {
            return Err(format!("service `{name}` changed transport binding"));
        }
    }

    Ok(())
}
