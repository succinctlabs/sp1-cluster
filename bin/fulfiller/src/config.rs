use alloy_primitives::{Address, B256};
use config::{Config, ConfigError, Environment};
use serde::Deserialize;
use spn_utils::{deserialize_domain, LogFormat};

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub rpc_grpc_addr: String,
    pub metrics_addr: String,
    pub sp1_private_key: String,
    pub cluster_rpc: String,
    pub version: String,
    #[serde(deserialize_with = "deserialize_domain")]
    pub domain: B256,
    pub log_format: LogFormat,
    /// Fulfiller addresses to filter requests by. If not set, the signer address will be used.
    pub addresses: Option<Vec<Address>>,
}

impl Settings {
    pub fn new() -> Result<Self, ConfigError> {
        let settings: Self = Config::builder()
            .add_source(
                Environment::with_prefix("FULFILLER")
                    .list_separator(",")
                    .with_list_parse_key("addresses")
                    .try_parsing(true),
            )
            .build()?
            .try_deserialize()?;

        Ok(settings)
    }
}
