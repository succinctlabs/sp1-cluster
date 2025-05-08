use std::{net::SocketAddr, sync::Arc};

use anyhow::Result;
use spn_metrics::{
    metrics::{
        self, counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram,
        Counter, Gauge,
    },
    server::{MetricServer, MetricServerConfig},
    version::VersionInfo,
    Metrics,
};
use tokio::{sync::oneshot, task::JoinHandle};

use crate::{VERGEN_BUILD_TIMESTAMP, VERGEN_GIT_SHA};

#[derive(Metrics, Clone)]
#[metrics(scope = "cluster_worker")]
pub struct WorkerMetrics {
    /// Current memory usage by tasks in bytes.
    pub memory_usage_bytes: Gauge,

    /// Current memory usage by tasks in %.
    pub memory_usage_percent: Gauge,

    /// 1 if the worker is a GPU worker, 0 otherwise.
    pub num_gpu_workers: Gauge,

    /// Number of proofs claimed by this worker.
    pub proofs_claimed: Counter,

    /// Number of proofs unclaimed by this worker.
    pub proofs_unclaimed: Counter,

    /// Amount of time spent within the GPU lock.
    pub gpu_busy_time: Counter,
}

impl WorkerMetrics {
    pub fn increment_tasks_in_progress(&self, task_type: String) {
        describe_gauge!(
            "cluster_worker_tasks_in_progress",
            "Number of tasks currently running onthis worker.",
        );
        gauge!(
            "cluster_worker_tasks_in_progress",
            "task_type" => task_type
        )
        .increment(1);
    }

    pub fn decrement_tasks_in_progress(&self, task_type: String) {
        describe_gauge!(
            "cluster_worker_tasks_in_progress",
            "Number of tasks currently running on this worker.",
        );
        gauge!(
            "cluster_worker_tasks_in_progress",
            "task_type" => task_type
        )
        .decrement(1);
    }

    pub fn increment_tasks_processed(&self, task_type: String) {
        describe_counter!(
            "cluster_worker_tasks_processed",
            "Number of tasks processed by this worker.",
        );
        counter!(
            "cluster_worker_tasks_processed",
            "task_type" => task_type
        )
        .increment(1);
    }

    pub fn record_task_processing_duration(&self, task_type: String, duration_ms: f64) {
        describe_histogram!(
            "cluster_worker_task_processing_duration_ms",
            "Distribution of task processing times",
        );
        histogram!(
            "cluster_worker_task_processing_duration_ms",
            "task_type" => task_type,
        )
        .record(duration_ms);
    }

    pub fn increment_task_failures(&self, task_type: String, retryable: bool) {
        describe_counter!("cluster_worker_task_failures", "Number of task failures",);
        counter!(
            "cluster_worker_task_failures",
            "task_type" => task_type,
            "retryable" => retryable.to_string()
        )
        .increment(1);
    }

    pub fn increment_task_successes(&self, task_type: String) {
        describe_counter!("cluster_worker_task_successes", "Number of task successes",);
        counter!(
            "cluster_worker_task_successes",
            "task_type" => task_type
        )
        .increment(1);
    }
}

pub async fn initialize_metrics() -> Result<(
    Arc<WorkerMetrics>,
    Option<JoinHandle<()>>,
    tokio::sync::broadcast::Sender<()>,
)> {
    // Initialize metrics server first
    let version_info = VersionInfo {
        version: env!("CARGO_PKG_VERSION").to_string(),
        build_timestamp: VERGEN_BUILD_TIMESTAMP.to_string(),
        cargo_features: env!("VERGEN_CARGO_FEATURES").to_string(),
        git_sha: VERGEN_GIT_SHA.to_string(),
        target_triple: env!("VERGEN_CARGO_TARGET_TRIPLE").to_string(),
        build_profile: {
            #[cfg(debug_assertions)]
            let profile = "debug";
            #[cfg(not(debug_assertions))]
            let profile = "release";
            profile.to_string()
        },
    };

    // Parse metrics address from env or use default
    let metrics_addr: SocketAddr = std::env::var("WORKER_METRICS_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:9090".to_string())
        .parse()?;

    let (ready_tx, ready_rx) = oneshot::channel();

    let metrics_server_config =
        MetricServerConfig::new(metrics_addr, version_info, "sp1-worker".to_string())
            .with_ready_signal(ready_tx);

    let metrics_server = MetricServer::new(metrics_server_config);

    // Spawn metrics server
    let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);
    let metrics_server_handle = Some(tokio::spawn(async move {
        if let Err(e) = metrics_server.serve(shutdown_rx).await {
            log::error!("metrics server error: {:?}", e);
        }
    }));

    // Wait for metrics server to be ready
    match ready_rx.await {
        Ok(_) => log::info!("metrics server is ready"),
        Err(e) => log::warn!("failed to receive metrics server ready signal: {}", e),
    }

    // Initialize metrics after server is ready
    let metrics = Arc::new(WorkerMetrics::default());
    WorkerMetrics::describe();

    Ok((metrics, metrics_server_handle, shutdown_tx))
}
