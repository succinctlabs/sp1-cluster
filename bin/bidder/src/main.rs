use std::{net::SocketAddr, str::FromStr};

use alloy_signer_local::PrivateKeySigner;
use anyhow::{Context, Result};
use dotenv::dotenv;
use sp1_cluster_bidder::{config::Settings, grpc, metrics::BidderMetrics, Bidder};
use spn_metrics::{
    server::{MetricServer, MetricServerConfig},
    version::VersionInfo,
};
use spn_network_types::prover_network_client::ProverNetworkClient;
use tokio::{signal, sync::oneshot};
use tracing::{error, info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    // Load environment variables.
    dotenv().ok();

    // Initialize the crypto provider.
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    // Load configuration
    let settings = Settings::new().context("Failed to load bidder env")?;

    // Initialize logging
    spn_utils::init_logger(settings.log_format);

    // Initialize the dependencies.
    let network_channel = grpc::configure_endpoint(settings.rpc_grpc_addr.clone())?
        .connect()
        .await?;
    let network = ProverNetworkClient::new(network_channel);

    let signer = PrivateKeySigner::from_str(&settings.sp1_private_key)?;

    // Initialize the metrics server.
    // let version_info = get_version!();
    let version_info = VersionInfo {
        version: "".to_string(),
        build_timestamp: "".to_string(),
        cargo_features: "".to_string(),
        git_sha: "".to_string(),
        target_triple: "".to_string(),
        build_profile: "".to_string(),
    };
    let metrics_addr: SocketAddr = settings.metrics_addr.parse()?;
    let (ready_tx, ready_rx) = oneshot::channel();

    let metrics_server_config =
        MetricServerConfig::new(metrics_addr, version_info, "spn-bidder".to_string())
            .with_ready_signal(ready_tx);
    let metrics_server = MetricServer::new(metrics_server_config);

    // Create a shutdown channel.
    let (_shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);

    // Spawn the metrics server task.
    let mut metrics_server_handle = Some(tokio::spawn(async move {
        if let Err(e) = metrics_server.serve(shutdown_rx).await {
            error!("metrics server error: {:?}", e);
        }
    }));

    // Wait for metrics server to be ready.
    match ready_rx.await {
        Ok(_) => info!("metrics server is ready"),
        Err(e) => warn!("failed to receive metrics server ready signal: {}", e),
    }

    // Describe and initialize the metrics.
    let metrics = BidderMetrics::default();
    BidderMetrics::describe();

    // Initialize the Bidder with metrics.
    let bidder = Bidder::new(
        network,
        settings.version,
        signer,
        metrics,
        settings.domain.to_vec(),
        settings.throughput_mgas,
        settings.max_concurrent_proofs,
        settings.bid_amount,
        settings.buffer_sec,
        settings.groth16_buffer_sec,
        settings.plonk_buffer_sec,
        settings.groth16_enabled,
        settings.plonk_enabled,
    );

    // Spawn the bidder task.
    let mut bidder_handle = Some(tokio::spawn(async move {
        if let Err(e) = bidder.run().await {
            error!("bidder task exited unexpectedly: {:?}", e);
        }
    }));

    // Wait for the tasks to finish or a signal to shutdown.
    tokio::select! {
        _ = metrics_server_handle.as_mut().unwrap() => {
            error!("metrics server task exited unexpectedly");
        }
        _ = bidder_handle.as_mut().unwrap() => {
            error!("bidder task exited unexpectedly");
        }
        _ = signal::ctrl_c() => {
            warn!("ctrl-c received, shutting down");
        }
    }

    // Abort the tasks if they're still running.
    if let Some(handle) = bidder_handle.take() {
        handle.abort();
        let _ = handle.await;
    }
    if let Some(handle) = metrics_server_handle.take() {
        handle.abort();
        let _ = handle.await;
    }
    info!("graceful shutdown complete");

    Ok(())
}
