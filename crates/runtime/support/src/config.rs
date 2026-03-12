use std::{fs, path::Path};

use anyhow::{Context, Result};
use serde::de::DeserializeOwned;

/// Load and parse a TOML config file from disk.
pub fn load_toml_config<T>(path: &Path) -> Result<T>
where
    T: DeserializeOwned,
{
    let contents =
        fs::read_to_string(path).with_context(|| format!("read config {}", path.display()))?;
    toml::from_str(&contents).with_context(|| format!("parse config {}", path.display()))
}
