use spn_metrics::{
    metrics,
    metrics::{Counter, Gauge},
    Metrics,
};

#[derive(Metrics, Clone)]
#[metrics(scope = "fulfiller")]
pub struct FulfillerMetrics {
    /// The number of proof requests submitted successfully.
    pub requests_submitted: Counter,

    /// The number of proof request submission attempts that failed.
    pub request_submission_failures: Counter,

    /// The number of proof requests that have failed successfully.
    pub requests_failed: Counter,

    /// The number of proof request fail attempts that failed.
    pub request_fail_failures: Counter,

    /// The number of proof requests scheduled successfully.
    pub requests_scheduled: Counter,

    /// The number of proof request schedule attempts that failed.
    pub request_schedule_failures: Counter,

    /// The number of proof request cancellations.
    pub requests_cancelled: Counter,

    /// The number of proof request cancellation attempts that failed.
    pub request_cancel_failures: Counter,

    /// The total number of proof requests processed (submitted, scheduled, or failed).
    pub total_requests_processed: Counter,

    /// The number of errors encountered during the main loop.
    pub main_loop_errors: Counter,

    /// The current number of submittable proof requests found in the last check.
    pub submittable_requests: Gauge,

    /// The current number of failable proof requests found in the last check.
    pub failable_requests: Gauge,

    /// The current number of cancelable proof requests found in the last check.
    pub cancelable_requests: Gauge,

    /// The current number of schedulable proof requests found in the last check.
    pub schedulable_requests: Gauge,
}
