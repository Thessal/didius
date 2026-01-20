use aws_config::SdkConfig;
use aws_credential_types::Credentials;
use aws_types::region::Region;
use serde::Deserialize;
use std::fs;
use anyhow::{Result, anyhow};

#[derive(Debug, Deserialize)]
pub struct AwsConfigYaml {
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
}

pub async fn load_aws_config(config_path: &str) -> Result<SdkConfig> {
    let content = fs::read_to_string(config_path)
        .map_err(|e| anyhow!("Failed to read aws config file {}: {}", config_path, e))?;
        
    let cfg: AwsConfigYaml = serde_yaml::from_str(&content)
        .map_err(|e| anyhow!("Failed to parse aws config yaml: {}", e))?;

    let region = Region::new(cfg.region.clone());
    
    let credentials = Credentials::new(
        cfg.access_key_id,
        cfg.secret_access_key,
        None,
        None,
        "yaml_config"
    );

    let config = aws_config::from_env()
        .region(region)
        .credentials_provider(credentials)
        .load()
        .await;

    Ok(config)
}
