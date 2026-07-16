use spn_metrics::{
    metrics,
    metrics::{Counter, Gauge},
    Metrics,
};

#[derive(Metrics, Clone)]
#[metrics(scope = "bidder")]
pub struct BidderMetrics {
    /// The number of proof requests that are currently biddable.
    pub biddable_requests: Gauge,

    /// The number of proof requests bid on successfully.
    pub requests_bid: Counter,

    /// The number of proof request bid attempts that failed.
    pub request_bid_failures: Counter,

    /// The number of bids rejected in normal operation (over max price, lost race,
    /// stale nonce).
    pub request_bid_rejections: Counter,

    /// The total number of proof requests processed (bid on).
    pub total_requests_processed: Counter,

    /// The number of errors encountered during the main loop.
    pub main_loop_errors: Counter,

    /// Age in seconds of the most recently cached PROVE/USD price snapshot.
    pub prove_usd_age_seconds: Gauge,

    /// Number of bid evaluations where the dynamic USD-derived `bid_amount` was used.
    pub dynamic_bid_used_total: Counter,

    /// Number of bid evaluations where we fell back to the static `bid_amount`
    /// (either dynamic not configured, or the cached price is stale/absent).
    pub static_bid_used_total: Counter,

    /// Bid evaluations where the fulfillment deadline was tightened by the network's
    /// published performance requirements. Counts evaluations, not distinct requests:
    /// an unbid request is re-evaluated (and re-counted) every bid pass.
    pub perf_clamped: Counter,

    /// Whether this prover is currently suspended by the network (1 = suspended).
    pub suspended: Gauge,

    /// Requirements/status sync errors (caches kept stale, bidding unaffected).
    pub requirements_sync_errors: Counter,
}
