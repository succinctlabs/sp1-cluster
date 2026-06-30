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
    /// Static bid per PGU in wei. Used unconditionally when `usd_bid_enabled` is `false`;
    /// otherwise serves as the safety-net bid when the PROVE/USD cache is missing or stale.
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
    /// USD-denominated bid master switch. `true` (default) routes bids through the
    /// dynamic path when fresh PROVE/USD is available; otherwise falls back to the
    /// static `bid_amount`. `false` keeps the bidder on the static `bid_amount` path
    /// unconditionally.
    ///
    /// Dynamic path: target denominated in USD. The bidder converts to wei using current
    /// PROVE/USD, so your USD revenue per PGU stays steady as PROVE moves.
    /// Static path: target denominated in wei. Your PROVE revenue per PGU stays fixed,
    /// so the USD value moves with the PROVE price.
    #[serde(default = "default_usd_bid_enabled")]
    pub usd_bid_enabled: bool,
    /// USD bid target in µUSD per BPGU (1 BPGU = 10⁹ PGU). Tune to your cost basis.
    #[serde(default = "default_usd_bid_target")]
    pub usd_bid_target: u64,
    /// How often to refresh the PROVE/USD cache, in seconds.
    #[serde(default = "default_usd_bid_refresh_interval_secs")]
    pub usd_bid_refresh_interval_secs: u64,
    /// Cached PROVE/USD older than this is treated as stale; bidder falls back to
    /// `bid_amount` until the cache refreshes.
    #[serde(default = "default_usd_bid_staleness_max_secs")]
    pub usd_bid_staleness_max_secs: u64,
}

/// Runtime view of the USD bid parameters. Built from [`Settings::usd_bid`] only when
/// the dynamic path is enabled, so its presence is equivalent to the dynamic path being
/// active. `target` is denominated in µUSD per BPGU (1 BPGU = 10⁹ PGU); the wire-level
/// bid is wei per PGU, derived as `target * 10⁹ / prove_usd_micros`.
#[derive(Debug, Clone)]
pub struct UsdBidConfig {
    pub target: u64,
    pub refresh_interval_secs: u64,
    pub staleness_max_secs: u64,
}

impl Settings {
    pub fn new() -> Result<Self, ConfigError> {
        Config::builder()
            .add_source(Environment::with_prefix("BIDDER"))
            .build()?
            .try_deserialize()
    }

    /// Returns `Some(UsdBidConfig)` when the dynamic path is enabled, otherwise `None`.
    pub fn usd_bid(&self) -> Option<UsdBidConfig> {
        self.usd_bid_enabled.then_some(UsdBidConfig {
            target: self.usd_bid_target,
            refresh_interval_secs: self.usd_bid_refresh_interval_secs,
            staleness_max_secs: self.usd_bid_staleness_max_secs,
        })
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

fn default_usd_bid_enabled() -> bool {
    true
}

fn default_usd_bid_target() -> u64 {
    120_000 // $0.12/BPGU
}

fn default_usd_bid_refresh_interval_secs() -> u64 {
    60
}

fn default_usd_bid_staleness_max_secs() -> u64 {
    7200
}
