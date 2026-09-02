use std::fs;

use serde::{Deserialize, Serialize};
use anyhow::Result;


#[derive(Debug, Serialize, Deserialize)]
pub struct BatchConfig {
    pub max_rows: usize, 
    pub max_age: usize, 
    pub data_dir: String, 
    pub compact_interval: usize, 
    pub compact_threshold: usize,
}

impl BatchConfig {
    pub fn new(file: &str) -> Result<Self> {
        let context = fs::read_to_string(file)?;

        Self::from_yaml(&context)
    }

    pub fn from_yaml(yaml: &str) -> Result<Self> {
        let config: BatchConfig = serde_yaml::from_str(yaml)?;

        Ok(config)
    }
}