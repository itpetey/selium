use anyhow::Result;

use crate::ContractRef;

pub fn parse_contract_ref(raw: &str) -> Result<ContractRef> {
    let (lhs, version) = raw
        .split_once('@')
        .ok_or_else(|| anyhow::anyhow!("contract ref must be namespace/name@version"))?;
    let (namespace, name) = lhs
        .split_once('/')
        .ok_or_else(|| anyhow::anyhow!("contract ref must be namespace/name@version"))?;

    if namespace.trim().is_empty() || name.trim().is_empty() || version.trim().is_empty() {
        return Err(anyhow::anyhow!(
            "contract ref must have namespace/name@version"
        ));
    }

    Ok(ContractRef {
        namespace: namespace.to_string(),
        name: name.to_string(),
        version: version.to_string(),
    })
}

impl Ord for ContractRef {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (&self.namespace, &self.name, &self.version).cmp(&(
            &other.namespace,
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
