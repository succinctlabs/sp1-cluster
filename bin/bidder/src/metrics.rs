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

    /// The total number of proof requests processed (bid on).
    pub total_requests_processed: Counter,

    /// The number of errors encountered during the main loop.
    pub main_loop_errors: Counter,
}
