use sp1_core_executor::{SP1CoreOpts, ELEMENT_THRESHOLD};

use sp1_prover::worker::SP1WorkerConfig;

/// The core opts for the cluster.
pub fn cluster_opts() -> SP1CoreOpts {
    let mut opts = SP1CoreOpts::default();

    let shard_threshold = ELEMENT_THRESHOLD - (1 << 27);

    println!("Shard threshold: {shard_threshold}");
    opts.sharding_threshold.element_threshold = shard_threshold;

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
