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
    /// Aggressive mode: bid on all requests without capacity/time checks
    #[serde(default)]
    pub aggressive_mode: bool,
    /// Minimum deadline in seconds to bid on (optional safety check, even in aggressive mode)
    pub min_deadline_secs: Option<u64>,
    /// Opt-in USD-pegged bidding. When `Some`, the bidder polls `GetProvePrice` and
    /// converts the target to PROVE wei. When `None`, the bidder uses the static
    /// `bid_amount` unconditionally.
    ///
    /// Unit note: BPGU = "billion PGU" = 10⁹ PGU. The wire-level field is `max_price_per_pgu`
    /// (wei per PGU); BPGU is a billing-side convenience for stating targets at human scale.
    #[serde(default)]
    pub usd_pricing: Option<UsdPricingConfig>,
}

/// USD-pegged bidding parameters. The presence of this struct in `Settings` is itself
/// the "feature on" signal; absence means "stay on the static `bid_amount` path."
#[derive(Debug, Deserialize, Clone)]
pub struct UsdPricingConfig {
    /// USD target in µUSD per BPGU (1 BPGU = 10⁹ PGU).
    pub target_usd_micros_per_bpgu: u64,
    /// How often to refresh the PROVE/USD cache, in seconds.
    #[serde(default = "default_refresh_interval_secs")]
    pub refresh_interval_secs: u64,
    /// Cached PROVE/USD older than this is treated as stale; bidder falls back to
    /// `bid_amount` until the cache refreshes.
    #[serde(default = "default_staleness_max_secs")]
    pub staleness_max_secs: u64,
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

fn default_refresh_interval_secs() -> u64 {
    60
}

fn default_staleness_max_secs() -> u64 {
    3600
}
