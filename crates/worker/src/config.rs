use sp1_core_executor::{SP1CoreOpts, ELEMENT_THRESHOLD};
use sp1_core_machine::riscv::RiscvAir;
use sp1_prover::worker::SP1WorkerConfig;

/// Default `log2(shard_size)` for the cluster, matching sp1-gpu's
/// `local_gpu_opts()` for the 24GB GPU tier. Override with
/// `SP1_CLUSTER_LOG2_SHARD_SIZE`.
const DEFAULT_LOG2_SHARD_SIZE: u32 = 24;

/// Sharding `element_threshold` offset below `ELEMENT_THRESHOLD`, matching
/// sp1-gpu's 24GB tier. Sizing this above the tier produces shards smaller
/// than the GPU can fill, adding per-shard overhead. Override with
/// `SP1_CLUSTER_SHARD_THRESHOLD`.
const DEFAULT_SHARD_THRESHOLD_OFFSET: u64 = (1 << 26) + (1 << 25);

fn read_env<T: std::str::FromStr>(name: &str) -> Option<T> {
    std::env::var(name).ok().and_then(|s| s.parse().ok())
}

/// The core opts for the cluster.
///
/// Hand-maintained copy of `local_gpu_opts()`'s ≤30GB GPU tier; can drift from
/// upstream. Can't import it directly — it does a CUDA memory probe and panics
/// off-GPU, but this config also runs on CPU workers. TODO: upstream a pure tier
/// table (no hardware probe) in `sp1-gpu` that both can share.
pub fn cluster_opts() -> SP1CoreOpts {
    let mut opts = SP1CoreOpts::default();

    let log2_shard_size =
        read_env("SP1_CLUSTER_LOG2_SHARD_SIZE").unwrap_or(DEFAULT_LOG2_SHARD_SIZE);
    opts.shard_size = 1 << log2_shard_size;

    let shard_threshold = read_env("SP1_CLUSTER_SHARD_THRESHOLD")
        .unwrap_or(ELEMENT_THRESHOLD - DEFAULT_SHARD_THRESHOLD_OFFSET);
    opts.sharding_threshold.element_threshold = shard_threshold;

    opts.global_dependencies_opt = option_env!("SP1_CLUSTER_CPU_ONLY").is_none();

    tracing::info!(log2_shard_size, shard_threshold, "cluster_opts configured");
    opts
}

/// The worker config for the cluster.
pub fn cluster_worker_config() -> SP1WorkerConfig {
    // Right now we are only changing the core opts, in the future we might want to change other
    // parts of the config as well.
    let mut config = SP1WorkerConfig::new(RiscvAir::machine());
    config.controller_config.opts = cluster_opts();
    config
}
