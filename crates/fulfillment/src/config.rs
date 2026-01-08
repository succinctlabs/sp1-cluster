use alloy_primitives::{Address, B256};
use config::{Config, ConfigError, Environment};
use serde::Deserialize;
use spn_utils::{deserialize_domain, LogFormat};

#[derive(Debug, Deserialize)]
pub struct FulfillerSettings {
    pub rpc_grpc_addr: String,
    pub metrics_addr: String,
    pub sp1_private_key: String,
    pub use_aws_kms: bool,
    pub cluster_rpc: String,
    pub version: String,
    pub log_format: LogFormat,
    #[serde(deserialize_with = "deserialize_domain")]
    pub domain: B256,
    /// Fulfiller addresses to filter requests by. If not set, the signer address will be used.
    pub addresses: Option<Vec<Address>>,
    pub network_s3_region: Option<String>,
    pub cluster_artifact_store: String,
    pub cluster_s3_region: Option<String>,
    pub cluster_s3_bucket: Option<String>,
    pub cluster_s3_concurrency: Option<usize>,
    pub cluster_redis_nodes: Option<Vec<String>>,
    pub cluster_redis_pool_max_size: Option<usize>,
}

impl FulfillerSettings {
    pub fn new() -> Result<Self, ConfigError> {
        let settings: Self = Config::builder()
            .add_source(
                Environment::with_prefix("FULFILLER")
                    .list_separator(",")
                    .with_list_parse_key("addresses")
                    .with_list_parse_key("cluster_redis_nodes")
                    .try_parsing(true),
            )
            .build()?
            .try_deserialize()?;

        Ok(settings)
    }
}
