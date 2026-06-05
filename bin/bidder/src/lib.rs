use std::{sync::Arc, time::Duration};

use crate::{config::UsdFloorConfig, metrics::BidderMetrics};
use alloy::primitives::{Address, U256};
use alloy_signer_local::PrivateKeySigner;
use anyhow::{Context, Result};
use futures::future::join_all;
use time::OffsetDateTime;
use tokio::{
    sync::RwLock,
    time::{interval, sleep, MissedTickBehavior},
};
use tonic::transport::Channel;
use tracing::{error, info, instrument, warn};

use spn_network_types::{
    prover_network_client::ProverNetworkClient, BidRequest, BidRequestBody, FulfillmentStatus,
    GetNonceRequest, GetOwnerRequest, GetProvePriceRequest, MessageFormat, ProofMode, Signable,
    TransactionVariant,
};
use spn_pricing::ProvePrice;
use spn_utils::time_now;

pub mod config;
pub mod grpc;
pub mod metrics;

/// How long to wait between checking if there are any requested proofs to bid on.
///
/// Lower values will result in faster E2E latency for the user, but more outgoing requests to the
/// network.
const REFRESH_INTERVAL_SEC: u64 = 3;

// Per-instance configurable safety buffers are provided via Settings

/// The maximum number of requests to handle in a single refresh loop.
const REQUEST_LIMIT: u32 = 100;

/// Synchronous prime-fetch cap at startup so a slow RPC can't hang the bidder.
const PRIME_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Clone)]
pub struct Bidder {
    network: ProverNetworkClient<Channel>,
    version: String,
    signer: PrivateKeySigner,
    metrics: BidderMetrics,
    domain_bytes: Vec<u8>,
    /// Total cluster throughput in million gas per second
    throughput_mgas: f64,
    /// Maximum number of concurrent proofs the cluster can handle
    max_concurrent_proofs: u32,
    /// Token bid amount per PGU in wei
    bid_amount: U256,
    /// Base safety buffer in seconds applied to all proofs
    buffer_sec: u64,
    /// Additional buffer for Groth16 proofs in seconds
    groth16_buffer_sec: u64,
    /// Additional buffer for Plonk proofs in seconds
    plonk_buffer_sec: u64,
    /// Whether to bid on Groth16 proofs
    groth16_enabled: bool,
    /// Whether to bid on Plonk proofs
    plonk_enabled: bool,
    /// Aggressive mode: bid on all requests without capacity/time checks
    aggressive_mode: bool,
    /// Minimum deadline in seconds to bid on (optional safety check, even in aggressive mode)
    min_deadline_secs: Option<u64>,
    /// USD-pegged bidding parameters. When `usd_floor.enabled`, the bidder polls
    /// `GetProvePrice` and converts the target to PROVE wei via
    /// `target * 10^9 / prove_usd_micros`. Otherwise the bidder stays on the static
    /// `bid_amount` path.
    ///
    /// BPGU = 10⁹ PGU; see `UsdFloorConfig` for the unit convention.
    usd_floor: UsdFloorConfig,
    /// Cache of the last fetched PROVE/USD price. Populated by the refresh task spawned
    /// in `run()` when `usd_floor.enabled`.
    prove_usd_cache: Arc<RwLock<Option<ProvePrice>>>,
}

impl Bidder {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        network: ProverNetworkClient<Channel>,
        version: String,
        signer: PrivateKeySigner,
        metrics: BidderMetrics,
        domain_bytes: Vec<u8>,
        throughput_mgas: f64,
        max_concurrent_proofs: u32,
        bid_amount: U256,
        buffer_sec: u64,
        groth16_buffer_sec: u64,
        plonk_buffer_sec: u64,
        groth16_enabled: bool,
        plonk_enabled: bool,
        aggressive_mode: bool,
        min_deadline_secs: Option<u64>,
        usd_floor: UsdFloorConfig,
    ) -> Self {
        Self {
            network,
            version,
            signer,
            metrics,
            domain_bytes,
            throughput_mgas,
            max_concurrent_proofs,
            bid_amount,
            buffer_sec,
            groth16_buffer_sec,
            plonk_buffer_sec,
            groth16_enabled,
            plonk_enabled,
            aggressive_mode,
            min_deadline_secs,
            usd_floor,
            prove_usd_cache: Arc::new(RwLock::new(None)),
        }
    }

    /// Calculate if we can fulfill a proof request within its deadline
    fn can_fulfill_proof(
        &self,
        active_proofs: u32,
        gas_limit: u64,
        deadline_secs: u64,
        mode: ProofMode,
    ) -> bool {
        // If the proof mode is disabled, we cannot fulfill it
        match mode {
            ProofMode::Groth16 if !self.groth16_enabled => return false,
            ProofMode::Plonk if !self.plonk_enabled => return false,
            _ => {}
        }
        // Calculate effective throughput per proof when at max capacity
        let effective_throughput = self.throughput_mgas / self.max_concurrent_proofs as f64;

        // Calculate time needed to complete this proof (in seconds)
        let completion_time_secs = (gas_limit as f64 / 1_000_000.0) / effective_throughput;

        // Add buffers for safety
        let mut total_time_needed = completion_time_secs + self.buffer_sec as f64;

        match mode {
            ProofMode::Groth16 => {
                total_time_needed += self.groth16_buffer_sec as f64;
            }
            ProofMode::Plonk => {
                total_time_needed += self.plonk_buffer_sec as f64;
            }
            _ => {}
        }

        // Check if we have enough time and capacity
        let has_capacity = active_proofs < self.max_concurrent_proofs;
        let has_time = total_time_needed <= deadline_secs as f64;

        has_capacity && has_time
    }

    /// Returns the bid amount to use for this iteration.
    ///
    /// Uses the USD-pegged amount when the USD floor is enabled and the cached PROVE/USD
    /// reading is fresh; otherwise falls back to the static `bid_amount`.
    async fn effective_bid_amount(&self) -> U256 {
        let cache_snapshot = self.prove_usd_cache.read().await.as_ref().map(|c| {
            // Clamp negative durations (clock skew between upstream timestamp and this read) to 0.
            let age_secs = (OffsetDateTime::now_utc() - c.as_of).whole_seconds().max(0) as u64;
            (c.usd_micros, age_secs)
        });
        let usd_floor = self.usd_floor.enabled.then_some(&self.usd_floor);
        match bid_amount_outcome(usd_floor, cache_snapshot, self.bid_amount) {
            BidAmountOutcome::Static(v) => {
                self.metrics.static_floor_used_total.increment(1);
                v
            }
            BidAmountOutcome::Dynamic(v) => {
                self.metrics.dynamic_floor_used_total.increment(1);
                v
            }
        }
    }

    /// Synchronously seed the PROVE/USD cache once, bounded by `PRIME_TIMEOUT`. Called by
    /// the dynamic-pricing setup path in `run()`; failure leaves the cache empty and the
    /// bidder falls back to static until the refresh task takes over.
    async fn prime_prove_usd_cache(&self) {
        let mut network = self.network.clone();
        let fetch = fetch_and_cache_prove_usd(&mut network, &self.prove_usd_cache, &self.metrics);
        match tokio::time::timeout(PRIME_TIMEOUT, fetch).await {
            Ok(Ok(usd_micros)) => info!(usd_micros, "primed PROVE/USD cache"),
            Ok(Err(e)) => warn!(
                error = %e,
                "failed to prime PROVE/USD cache; starting with static fallback",
            ),
            Err(_) => warn!(
                timeout_secs = PRIME_TIMEOUT.as_secs(),
                "timed out priming PROVE/USD cache; starting with static fallback",
            ),
        }
    }

    /// Spawn a background task that polls `GetProvePrice` on a tick and refreshes the cache.
    /// Keeps the *cache* fresh, not the upstream price (alert on the indexer's
    /// `prove_price_latest_age_seconds` gauge for that).
    fn spawn_prove_usd_refresh(&self) {
        let cache = self.prove_usd_cache.clone();
        let mut network = self.network.clone();
        let metrics = self.metrics.clone();
        let mut ticker = interval(Duration::from_secs(self.usd_floor.refresh_interval_secs));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        tokio::spawn(async move {
            loop {
                ticker.tick().await;
                match fetch_and_cache_prove_usd(&mut network, &cache, &metrics).await {
                    Ok(usd_micros) => info!(usd_micros, "refreshed PROVE/USD cache"),
                    Err(e) => warn!(error = %e, "PROVE/USD refresh failed; keeping previous cache"),
                }
            }
        });
    }

    /// Runs the bidder loop.
    pub async fn run(mut self) -> Result<()> {
        info!("starting the bidder");

        if self.usd_floor.enabled {
            self.prime_prove_usd_cache().await;
            self.spawn_prove_usd_refresh();
        }

        // Get the prover.
        let prover_bytes = self
            .network
            .clone()
            .get_owner(GetOwnerRequest {
                address: self.signer.address().to_vec(),
            })
            .await?
            .into_inner()
            .owner;
        let prover = Address::from_slice(&prover_bytes);

        loop {
            // Check for requests to bid on.
            if let Err(e) = self.bid_requests(prover).await {
                error!("bidding on requests: {:?}", e);
                self.metrics.main_loop_errors.increment(1);
            }
            // Wait for the next interval.
            sleep(Duration::from_secs(REFRESH_INTERVAL_SEC)).await;
        }
    }

    /// Checks for requested proof requests that are in the network and bids on eligible ones.
    #[instrument(skip_all)]
    async fn bid_requests(&mut self, prover: Address) -> Result<()> {
        // Get all requests from the network that are biddable.
        let request = spn_network_types::GetFilteredProofRequestsRequest {
            version: Some(self.version.clone()),
            fulfillment_status: Some(FulfillmentStatus::Requested.into()),
            execution_status: None,
            execute_fail_cause: None,
            minimum_deadline: Some(time_now()),
            vk_hash: None,
            requester: None,
            fulfiller: None,
            limit: Some(REQUEST_LIMIT),
            page: None,
            from: None,
            to: None,
            mode: None,
            not_bid_by: Some(prover.to_vec()),
            error: None,
            settlement_status: None,
        };
        let network_requests_resp = self
            .network
            .clone()
            .get_filtered_proof_requests(request)
            .await?;
        let requests = network_requests_resp.into_inner().requests;
        self.metrics.biddable_requests.set(requests.len() as f64);

        // Get all requests from the network assigned to our address so we know how many additional
        // requests we can bid on.
        // Note this is done after the biddable requests query to ensure a proof is not ommitted
        // from both queries if it became assigned just after the assigned query.
        let assigned_requests = self
            .network
            .clone()
            .get_filtered_proof_requests(spn_network_types::GetFilteredProofRequestsRequest {
                version: Some(self.version.clone()),
                fulfillment_status: Some(FulfillmentStatus::Assigned.into()),
                execution_status: None,
                execute_fail_cause: None,
                minimum_deadline: Some(time_now()),
                vk_hash: None,
                requester: None,
                fulfiller: Some(prover.to_vec()),
                limit: Some(REQUEST_LIMIT),
                page: None,
                from: None,
                to: None,
                mode: None,
                not_bid_by: None,
                error: None,
                settlement_status: None,
            })
            .await?
            .into_inner()
            .requests;
        let mut active_proofs = assigned_requests.len() as u32;

        if requests.is_empty() {
            info!("found no biddable requests");
            return Ok(());
        }
        info!("found {} biddable requests", requests.len());

        // Resolve the bid amount once for this loop iteration so all spawned tasks share the
        // same price snapshot.
        let bid_amount = self.effective_bid_amount().await;

        let mut failure_tasks = Vec::new();
        for request in requests {
            let self_clone = self.clone();
            let mode = request.mode();
            let request_id = hex::encode(request.request_id);

            let request_duration = request.deadline.saturating_sub(time_now());

            // In aggressive mode, skip capacity/time checks but optionally enforce min deadline.
            if self.aggressive_mode {
                if let Some(min_deadline) = self.min_deadline_secs {
                    if request_duration < min_deadline {
                        info!(
                            "Skipping request 0x{} with deadline in {}s (below minimum {}s)",
                            request_id, request_duration, min_deadline
                        );
                        continue;
                    }
                }
            } else {
                // Normal mode: check capacity and time constraints.
                if !self.can_fulfill_proof(active_proofs, request.gas_limit, request_duration, mode)
                {
                    info!(
                        "Cannot fulfill request 0x{} with gas limit {} and deadline in {}s",
                        request_id, request.gas_limit, request_duration
                    );
                    continue;
                }
                active_proofs += 1;
            }
            failure_tasks.push(tokio::spawn(async move {
                match self_clone
                    .bid_request(prover, &request_id, bid_amount)
                    .await
                {
                    Ok(_) => {
                        info!("bid on request 0x{}", request_id);
                        self_clone.metrics.requests_bid.increment(1);
                        self_clone.metrics.total_requests_processed.increment(1);
                    }
                    Err(e) => {
                        error!("failed to bid on request 0x{}: {:?}", request_id, e);
                        self_clone.metrics.request_bid_failures.increment(1);
                    }
                }
            }));
        }

        join_all(failure_tasks).await;

        Ok(())
    }

    async fn bid_request(&self, prover: Address, request_id: &str, amount: U256) -> Result<()> {
        // Send the bid request to the network.
        let nonce = self
            .network
            .clone()
            .get_nonce(GetNonceRequest {
                address: self.signer.address().to_vec(),
            })
            .await?
            .into_inner()
            .nonce;
        let body = BidRequestBody {
            nonce,
            request_id: hex::decode(request_id).context("failed to decode request_id")?,
            amount: amount.to_string(),
            domain: self.domain_bytes.clone(),
            prover: prover.to_vec(),
            variant: TransactionVariant::BidVariant.into(),
        };
        let bid_request = BidRequest {
            format: MessageFormat::Binary.into(),
            signature: body.sign(&self.signer).into(),
            body: Some(body),
        };
        self.network.clone().bid(bid_request).await?;

        Ok(())
    }
}

/// Fetch PROVE/USD and write it into the cache on success. Returns the µUSD value for the
/// caller to log; cache is untouched on `Err`. Shared by the prime and refresh paths.
async fn fetch_and_cache_prove_usd(
    network: &mut ProverNetworkClient<Channel>,
    cache: &RwLock<Option<ProvePrice>>,
    metrics: &BidderMetrics,
) -> Result<u64> {
    let price = fetch_prove_price(network).await?;
    let usd_micros = price.usd_micros;
    let as_of = price.as_of;
    *cache.write().await = Some(price);
    let age_secs = (OffsetDateTime::now_utc() - as_of).whole_seconds().max(0) as f64;
    metrics.prove_usd_age_seconds.set(age_secs);
    Ok(usd_micros)
}

/// Fetch the current PROVE/USD reading. `as_of` tracks the upstream `last_updated`
/// timestamp so cache age reflects feed freshness.
async fn fetch_prove_price(network: &mut ProverNetworkClient<Channel>) -> Result<ProvePrice> {
    let resp = network
        .get_prove_price(GetProvePriceRequest {})
        .await?
        .into_inner();
    Ok(ProvePrice::parse(&resp.price, resp.last_updated)?)
}

/// Outcome of the effective-`bid_amount` decision. Carries which side (static vs dynamic)
/// so the caller can bump the matching metric.
#[derive(Debug, PartialEq, Eq)]
enum BidAmountOutcome {
    Static(U256),
    Dynamic(U256),
}

/// Pure logic behind [`Bidder::effective_bid_amount`]. Falls back to `Static(static_bid)`
/// when the USD floor is disabled, the cache is empty, the cache is stale, or the pricing
/// math errors. Only returns `Dynamic(_)` on a fresh cache with successful math.
///
/// `cache_snapshot` is `(prove_usd_micros, age_secs)` captured by the caller — keeps the
/// helper free of clock types so it can be unit-tested directly.
fn bid_amount_outcome(
    usd_floor: Option<&UsdFloorConfig>,
    cache_snapshot: Option<(u64, u64)>,
    static_bid: U256,
) -> BidAmountOutcome {
    let Some(usd_floor) = usd_floor else {
        return BidAmountOutcome::Static(static_bid);
    };
    let Some((prove_usd_micros, age_secs)) = cache_snapshot else {
        return BidAmountOutcome::Static(static_bid);
    };
    if age_secs >= usd_floor.staleness_max_secs {
        warn!(
            age_secs,
            max_secs = usd_floor.staleness_max_secs,
            "PROVE/USD cache is stale; falling back to static bid"
        );
        return BidAmountOutcome::Static(static_bid);
    }
    match spn_pricing::compute_max_price_per_pgu_wei(usd_floor.target, prove_usd_micros) {
        Ok(wei) => BidAmountOutcome::Dynamic(wei),
        Err(e) => {
            warn!(
                error = %e,
                target_micros_per_bpgu = usd_floor.target,
                prove_usd_micros, "USD→wei conversion failed; falling back to static bid",
            );
            BidAmountOutcome::Static(static_bid)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Reference anchor reused across the outcome tests: $0.20/BPGU at PROVE=$0.40 → 500M wei.
    const TARGET_MICROS_PER_BPGU: u64 = 200_000;
    const PROVE_USD_MICROS: u64 = 400_000;
    const EXPECTED_DYNAMIC_WEI: u64 = 500_000_000;

    fn floor(staleness_max_secs: u64) -> UsdFloorConfig {
        UsdFloorConfig {
            enabled: true,
            target: TARGET_MICROS_PER_BPGU,
            refresh_interval_secs: 60,
            staleness_max_secs,
        }
    }

    fn static_bid() -> U256 {
        U256::from(1_000u64)
    }

    /// Helper receives `None` when the feature is off — falls back to static `bid_amount`.
    #[test]
    fn no_usd_floor_returns_static() {
        let out = bid_amount_outcome(None, None, static_bid());
        assert_eq!(out, BidAmountOutcome::Static(static_bid()));
    }

    /// Cold start: empty cache → static `bid_amount` until the refresh task seeds it.
    #[test]
    fn missing_cache_returns_static() {
        let cfg = floor(3600);
        let out = bid_amount_outcome(Some(&cfg), None, static_bid());
        assert_eq!(out, BidAmountOutcome::Static(static_bid()));
    }

    /// Cache older than `staleness_max_secs` → static `bid_amount` (don't act on stale data).
    #[test]
    fn stale_cache_returns_static() {
        let cfg = floor(60);
        let out = bid_amount_outcome(Some(&cfg), Some((PROVE_USD_MICROS, 120)), static_bid());
        assert_eq!(out, BidAmountOutcome::Static(static_bid()));
    }

    /// Fresh cache + configured pricing → USD-derived dynamic bid via
    /// `spn_pricing::compute_max_price_per_pgu_wei`. Anchor: $0.20/BPGU at PROVE=$0.40 → 500M wei/PGU.
    #[test]
    fn fresh_cache_returns_dynamic() {
        let cfg = floor(3600);
        let out = bid_amount_outcome(Some(&cfg), Some((PROVE_USD_MICROS, 10)), static_bid());
        assert_eq!(
            out,
            BidAmountOutcome::Dynamic(U256::from(EXPECTED_DYNAMIC_WEI))
        );
    }

    /// Defense-in-depth: `prove_usd_micros = 0` makes `spn_pricing::compute_max_price_per_pgu_wei`
    /// error → static `bid_amount` (not propagated). Unreachable in production since
    /// `fetch_prove_price` rejects non-positive prices, but the branch is kept covered.
    #[test]
    fn math_error_returns_static() {
        let cfg = floor(3600);
        let out = bid_amount_outcome(Some(&cfg), Some((0, 10)), static_bid());
        assert_eq!(out, BidAmountOutcome::Static(static_bid()));
    }
}
