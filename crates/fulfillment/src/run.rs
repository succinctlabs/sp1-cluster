use crate::{
    config::FulfillerSettings, metrics::FulfillerMetrics, network::FulfillmentNetwork, Fulfiller,
};
use anyhow::Result;
use sp1_cluster_artifact::ArtifactClient;
use sp1_cluster_common::client::ClusterServiceClient;
use sp1_sdk::network::signer::NetworkSigner;
use spn_metrics::{
    server::{MetricServer, MetricServerConfig},
    version::VersionInfo,
};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::{signal, sync::oneshot};
use tracing::{error, info, warn};

pub async fn run_fulfiller<A: ArtifactClient, N: FulfillmentNetwork>(
    artifact_client: A,
    network: N,
) -> Result<()> {
    // Load configuration
    let settings = FulfillerSettings::new()?;

    // Initialize logging
    spn_utils::init_logger(settings.log_format);

    // Log configuration details
    info!(
        "fulfiller starting with addresses: {:?}",
        settings.addresses
    );

    let cluster = ClusterServiceClient::new(settings.cluster_rpc.clone())
        .await
        .expect("failed to connect to cluster");

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
        MetricServerConfig::new(metrics_addr, version_info, "spn-fulfiller".to_string())
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
    let metrics = FulfillerMetrics::default();
    FulfillerMetrics::describe();

    let signer = if settings.use_aws_kms {
        info!("using AWS KMS signer");
        NetworkSigner::aws_kms(&settings.sp1_private_key)
            .await
            .map_err(|e| anyhow::anyhow!("failed to initialize AWS KMS signer: {e}"))?
    } else {
        info!("using local private key signer");
        NetworkSigner::local(&settings.sp1_private_key)
            .map_err(|e| anyhow::anyhow!("failed to parse private key: {e}"))?
    };
    info!("signer address: {}", signer.address());

    // Initialize the Fulfiller with metrics.
    let fulfiller = Fulfiller::new(
        network,
        cluster,
        artifact_client,
        settings.version,
        settings.domain,
        metrics,
        settings.addresses,
        signer,
        true,
    );
    let fulfiller = Arc::new(fulfiller);

    // Spawn the fulfiller task.
    let mut fulfiller_handle = Some(tokio::spawn(async move {
        if let Err(e) = fulfiller.run().await {
            error!("fulfiller task exited unexpectedly: {:?}", e);
        }
    }));

    // Wait for the tasks to finish or a signal to shutdown.
    tokio::select! {
        _ = metrics_server_handle.as_mut().unwrap() => {
            error!("metrics server task exited unexpectedly");
        }
        _ = fulfiller_handle.as_mut().unwrap() => {
            error!("fulfiller task exited unexpectedly");
        }
        _ = signal::ctrl_c() => {
            warn!("ctrl-c received, shutting down");
        }
    }

    // Abort the tasks if they're still running.
    if let Some(handle) = fulfiller_handle.take() {
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
