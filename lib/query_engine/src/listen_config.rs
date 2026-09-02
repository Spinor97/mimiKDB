use std::fs;
use anyhow::Result;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct ListenConfig {
    pub addr: Option<String>,
    pub port: Option<String>,
    pub timeout: Option<u32>,
}

impl Default for ListenConfig {
    fn default() -> Self {
        Self { addr: Some("0.0.0.0".to_string()), port: Some("5432".to_string()), timeout: Some(300)}
    }
}

impl ListenConfig {
    pub fn new(file: &str) -> Result<Self> {
        let context = fs::read_to_string(file)?;

        Self::from_yaml(&context)
    }

    pub fn from_yaml(yaml: &str) -> Result<Self> {
        let config: ListenConfig = serde_yaml::from_str(yaml)?;

        Ok(config)
    }
}