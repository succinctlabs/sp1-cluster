use sp1_core_executor::{SP1CoreOpts, ELEMENT_THRESHOLD};

use sp1_prover::worker::SP1WorkerConfig;

/// Default `log2(shard_size)` for the cluster — matches sp1-gpu's
/// `local_gpu_opts()` (`crates/prover_components/src/builder.rs`) for the
/// 24GB GPU tier. Override with `SP1_CLUSTER_LOG2_SHARD_SIZE`.
const DEFAULT_LOG2_SHARD_SIZE: u32 = 24;

/// Default sharding `element_threshold` offset from `ELEMENT_THRESHOLD`,
/// also from sp1-gpu's 24GB tier. The cluster's previous value of
/// `(1 << 27) + (1 << 26)` (192M) was a tier too conservative — the GPU
/// is sized for `(1 << 26) + (1 << 25)` (96M), so shards came out smaller
/// than the GPU could handle and we paid extra shards' worth of
/// per-shard overhead. Override with `SP1_CLUSTER_SHARD_THRESHOLD`.
const DEFAULT_SHARD_THRESHOLD_OFFSET: u64 = (1 << 26) + (1 << 25);

fn read_env_u32(name: &str) -> Option<u32> {
    std::env::var(name).ok().and_then(|s| s.parse().ok())
}

fn read_env_u64(name: &str) -> Option<u64> {
    std::env::var(name).ok().and_then(|s| s.parse().ok())
}

/// The core opts for the cluster.
///
/// TODO: long-term these should be imported from `sp1-gpu` directly so
/// the cluster automatically tracks SP1's tuning. The current copy
/// matches `local_gpu_opts()` for ≤30GB GPUs as of sp1 b07cea1e.
pub fn cluster_opts() -> SP1CoreOpts {
    let mut opts = SP1CoreOpts::default();

    let log2_shard_size =
        read_env_u32("SP1_CLUSTER_LOG2_SHARD_SIZE").unwrap_or(DEFAULT_LOG2_SHARD_SIZE);
    opts.shard_size = 1 << log2_shard_size;

    let shard_threshold = read_env_u64("SP1_CLUSTER_SHARD_THRESHOLD")
        .unwrap_or(ELEMENT_THRESHOLD - DEFAULT_SHARD_THRESHOLD_OFFSET);
    opts.sharding_threshold.element_threshold = shard_threshold;

    opts.global_dependencies_opt = true;

    tracing::info!(log2_shard_size, shard_threshold, "cluster_opts configured");
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
