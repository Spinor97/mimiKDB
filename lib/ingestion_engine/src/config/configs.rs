use serde::{Deserialize, Serialize};
use anyhow::Result;
use std::fs;


#[derive(Debug, Serialize, Deserialize)]
pub struct LogOnConfig {
    pub host: String,
    pub port: String,
    pub sender_id: String,
    pub target_id: String,
    pub username: String,
    pub password: String,
    pub reset_seq_no: bool,
    pub heart_bt_int: u32, //(1, 1)
}

impl LogOnConfig {
    pub fn new(file: &str) -> Result<Self> {
        let context = fs::read_to_string(file)?;

        Self::from_yaml(&context)
    }

    pub fn from_yaml(yaml: &str) -> Result<Self> {
        let config: LogOnConfig = serde_yaml::from_str(yaml)?;

        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_heart_bt_int_from_yaml() {
        let yaml = "\
host: exchange.example.com
port: \"9001\"
sender_id: US
target_id: EX
username: alice
password: secret
reset_seq_no: false
heart_bt_int: 30
";

        let config = LogOnConfig::from_yaml(yaml).unwrap();

        assert_eq!(config.heart_bt_int, 30);
    }
}