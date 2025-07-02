use alloy::primitives::{B256, U256};
use config::{Config, ConfigError, Environment};
use serde::Deserialize;

use spn_utils::{deserialize_domain, LogFormat};

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub rpc_grpc_addr: String,
    pub metrics_addr: String,
    pub sp1_private_key: String,
    pub version: String,
    pub log_format: LogFormat,
    #[serde(deserialize_with = "deserialize_domain")]
    pub domain: B256,
    /// Total cluster throughput in million gas per second
    pub throughput_mgas: f64,
    /// Maximum number of concurrent proofs the cluster can handle
    pub max_concurrent_proofs: u32,
    /// Token bid amount per PGU in wei
    pub bid_amount: U256,
}

impl Settings {
    pub fn new() -> Result<Self, ConfigError> {
        Config::builder()
            .add_source(Environment::with_prefix("BIDDER"))
            .build()?
            .try_deserialize()
    }
}
