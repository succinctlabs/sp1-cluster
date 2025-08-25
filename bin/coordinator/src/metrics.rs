use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use spn_metrics::{
    metrics::{self},
    server::{MetricServer, MetricServerConfig},
    version::VersionInfo,
    Metrics,
};
use tokio::{sync::oneshot, task::JoinHandle};

use crate::BUILD_VERSION;

/// Coordinator metrics
#[derive(Clone, Metrics)]
#[metrics(scope = "cluster_coordinator")]
pub struct CoordinatorMetrics {}

impl CoordinatorMetrics {
    /// Create a new instance of CoordinatorMetrics
    pub fn new() -> Self {
        Default::default()
    }
}

pub async fn initialize_metrics() -> Result<(
    Arc<CoordinatorMetrics>,
    Option<JoinHandle<()>>,
    tokio::sync::broadcast::Sender<()>,
)> {
    // Initialize metrics server first
    let version_info = VersionInfo {
        version: env!("CARGO_PKG_VERSION").to_string(),
        build_timestamp: BUILD_VERSION.to_string(),
        cargo_features: option_env!("CARGO_FEATURES").unwrap_or("").to_string(),
        git_sha: option_env!("CARGO_GIT_SHA").unwrap_or("").to_string(),
        target_triple: option_env!("TARGET").unwrap_or("unknown").to_string(),
        build_profile: {
            #[cfg(debug_assertions)]
            let profile = "debug";
            #[cfg(not(debug_assertions))]
            let profile = "release";
            profile.to_string()
        },
    };

    // Parse metrics address from env or use default
    let metrics_addr: SocketAddr = std::env::var("COORDINATOR_METRICS_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:9090".to_string())
        .parse()?;

    let (ready_tx, ready_rx) = oneshot::channel();

    let metrics_server_config =
        MetricServerConfig::new(metrics_addr, version_info, "sp1-coordinator".to_string())
            .with_ready_signal(ready_tx);

    let metrics_server = MetricServer::new(metrics_server_config);

    // Spawn metrics server
    let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);
    let metrics_server_handle = Some(tokio::spawn(async move {
        if let Err(e) = metrics_server.serve(shutdown_rx).await {
            tracing::error!("metrics server error: {:?}", e);
        }
    }));

    // Wait for metrics server to be ready
    match ready_rx.await {
        Ok(_) => tracing::info!("metrics server is ready"),
        Err(e) => tracing::warn!("failed to receive metrics server ready signal: {}", e),
    }

    // Initialize metrics after server is ready
    let metrics = Arc::new(CoordinatorMetrics::new());

    Ok((metrics, metrics_server_handle, shutdown_tx))
}
