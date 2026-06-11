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
    /// Static bid per PGU in wei. Used unconditionally when `usd_floor` is `None`;
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
    /// USD-pegged bid floor. `enabled` polls `GetProvePrice` and converts the target to
    /// PROVE wei per PGU; setting it to `false` keeps the bidder on the static `bid_amount`
    /// path.
    ///
    /// BPGU = 10⁹ PGU; the wire-level `max_price_per_pgu` is wei per PGU. BPGU is a
    /// billing-side convenience for stating targets at human scale.
    #[serde(flatten)]
    pub usd_floor: UsdFloorConfig,
}

/// USD-pegged bidding parameters. Enabled by default with sensible defaults so the bidder
/// runs USD-pegged out of the box; set `BIDDER_USD_FLOOR_ENABLED=false` to fall back to
/// the static `bid_amount` (wei/PGU) path.
///
/// USD-pegged mode pins your USD revenue per PGU: earnings stay predictable across
/// PROVE-price moves, and your bids stay within requester price limits. Static mode
/// pins wei/PGU instead: PROVE revenue is fixed, but bids can fall outside requester
/// limits when PROVE moves significantly.
#[derive(Debug, Deserialize, Clone)]
pub struct UsdFloorConfig {
    /// Master switch. `true` (default) routes bids through the USD-pegged path; `false`
    /// keeps the bidder on the static `bid_amount` path unconditionally.
    #[serde(rename = "usd_floor_enabled", default = "default_enabled")]
    pub enabled: bool,
    /// USD floor target in µUSD per BPGU (1 BPGU = 10⁹ PGU). Tune to your cost basis.
    #[serde(rename = "usd_floor_target", default = "default_target")]
    pub target: u64,
    /// How often to refresh the PROVE/USD cache, in seconds.
    #[serde(
        rename = "usd_floor_refresh_interval_secs",
        default = "default_refresh_interval_secs"
    )]
    pub refresh_interval_secs: u64,
    /// Cached PROVE/USD older than this is treated as stale; bidder falls back to
    /// `bid_amount` until the cache refreshes.
    #[serde(
        rename = "usd_floor_staleness_max_secs",
        default = "default_staleness_max_secs"
    )]
    pub staleness_max_secs: u64,
}

impl Default for UsdFloorConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            target: default_target(),
            refresh_interval_secs: default_refresh_interval_secs(),
            staleness_max_secs: default_staleness_max_secs(),
        }
    }
}

impl Settings {
    pub fn new() -> Result<Self, ConfigError> {
        Config::builder()
            .add_source(
                Environment::with_prefix("BIDDER")
                    .prefix_separator("_")
                    .separator("__"),
            )
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

fn default_enabled() -> bool {
    true
}

fn default_target() -> u64 {
    120_000 // $0.12/BPGU
}

fn default_refresh_interval_secs() -> u64 {
    60
}

fn default_staleness_max_secs() -> u64 {
    1800
}
