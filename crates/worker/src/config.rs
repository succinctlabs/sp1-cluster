use sp1_core_executor::{SP1CoreOpts, ELEMENT_THRESHOLD};

use sp1_prover::worker::SP1WorkerConfig;

/// The core opts for the cluster.
///
/// Experiment: match direct sp1-perf's `local_gpu_opts` threshold tiers so shard
/// sizing doesn't artificially inflate task count on a single GPU. Controlled
/// by the `SP1_CLUSTER_SHARD_THRESHOLD_TIER` env var: `"small"` (default, legacy
/// value), `"sp1"` (direct sp1-perf's 24-GB bucket), `"full"` (no reduction).
pub fn cluster_opts() -> SP1CoreOpts {
    let mut opts = SP1CoreOpts::default();

    let shard_threshold = match std::env::var("SP1_CLUSTER_SHARD_THRESHOLD_TIER")
        .ok()
        .as_deref()
    {
        Some("sp1") => ELEMENT_THRESHOLD - (1 << 26) - (1 << 25),
        Some("full") => ELEMENT_THRESHOLD,
        _ => ELEMENT_THRESHOLD - (1 << 27) - (1 << 26),
    };

    println!("Shard threshold: {shard_threshold}");
    opts.sharding_threshold.element_threshold = shard_threshold;
    opts.global_dependencies_opt = true;

    opts
}

/// The worker config for the cluster.
pub fn cluster_worker_config() -> SP1WorkerConfig {
    // Right now we are only changing the core opts, in the future we might want to change other
    // parts of the config as well.
    let mut config = SP1WorkerConfig::default();
    config.controller_config.opts = cluster_opts();
    config
}
