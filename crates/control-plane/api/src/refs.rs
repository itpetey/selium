use anyhow::Result;

use crate::{ContractKind, ContractRef};

pub fn parse_contract_ref(raw: &str) -> Result<ContractRef> {
    let (lhs, version) = raw.split_once('@').ok_or_else(|| {
        anyhow::anyhow!(
            "contract ref must be namespace/name@version or namespace/kind:name@version"
        )
    })?;
    let (namespace, rhs) = lhs.split_once('/').ok_or_else(|| {
        anyhow::anyhow!(
            "contract ref must be namespace/name@version or namespace/kind:name@version"
        )
    })?;

    let (kind, name) = if let Some((kind, name)) = rhs.split_once(':') {
        let kind = match kind {
            "event" => ContractKind::Event,
            "service" => ContractKind::Service,
            "stream" => ContractKind::Stream,
            other => return Err(anyhow::anyhow!("unknown contract kind `{other}`")),
        };
        (kind, name)
    } else {
        (ContractKind::Event, rhs)
    };

    if namespace.trim().is_empty() || name.trim().is_empty() || version.trim().is_empty() {
        return Err(anyhow::anyhow!(
            "contract ref must have namespace/name@version or namespace/kind:name@version"
        ));
    }

    Ok(ContractRef {
        namespace: namespace.to_string(),
        kind,
        name: name.to_string(),
        version: version.to_string(),
    })
}

impl Ord for ContractRef {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (&self.namespace, self.kind, &self.name, &self.version).cmp(&(
            &other.namespace,
            other.kind,
            &other.name,
            &other.version,
        ))
    }
}

impl PartialOrd for ContractRef {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
