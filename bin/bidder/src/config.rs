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
    /// Base safety buffer in seconds applied to all proofs
    #[serde(default = "default_buffer_sec")]
    pub buffer_sec: u64,
    /// Additional buffer for Groth16 proofs in seconds
    #[serde(default = "default_groth16_buffer_sec")]
    pub groth16_buffer_sec: u64,
    /// Additional buffer for Plonk proofs in seconds
    #[serde(default = "default_plonk_buffer_sec")]
    pub plonk_buffer_sec: u64,
    /// Whether to bid on Groth16 proofs
    #[serde(default = "default_groth16_enabled")]
    pub groth16_enabled: bool,
    /// Whether to bid on Plonk proofs
    #[serde(default = "default_plonk_enabled")]
    pub plonk_enabled: bool,
}

impl Settings {
    pub fn new() -> Result<Self, ConfigError> {
        Config::builder()
            .add_source(Environment::with_prefix("BIDDER"))
            .build()?
            .try_deserialize()
    }
}

fn default_buffer_sec() -> u64 {
    30
}

fn default_groth16_buffer_sec() -> u64 {
    30
}

fn default_plonk_buffer_sec() -> u64 {
    80
}

fn default_groth16_enabled() -> bool {
    true
}

fn default_plonk_enabled() -> bool {
    true
}
