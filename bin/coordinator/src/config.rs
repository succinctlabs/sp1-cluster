// use crate::autoscaler::AutoscalerSettings;
use config::{Config, Environment};
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    /// Server
    #[serde(default = "default_addr")]
    pub addr: String,
    #[serde(default)]
    pub disable_proof_status_update: bool,
}

// Server
fn default_addr() -> String {
    "127.0.0.1:50051".to_string()
}

impl Settings {
    pub fn new() -> Result<Self, config::ConfigError> {
        let builder = Config::builder().add_source(Environment::with_prefix("COORDINATOR"));
        let config = builder.build()?;
        let settings = config.try_deserialize::<Settings>()?;
        Ok(settings)
    }
}
