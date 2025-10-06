use std::{net::SocketAddr, str::FromStr};

use alloy_signer_local::PrivateKeySigner;
use anyhow::Result;
use dotenv::dotenv;
use sp1_cluster_artifact::{
    redis::RedisArtifactClient,
    s3::{S3ArtifactClient, S3DownloadMode},
    ArtifactClient,
};
use sp1_cluster_common::client::ClusterServiceClient;
use sp1_cluster_fulfiller::{config::Settings, grpc, metrics::FulfillerMetrics, Fulfiller};
use spn_metrics::{
    server::{MetricServer, MetricServerConfig},
    version::VersionInfo,
};
use spn_network_types::prover_network_client::ProverNetworkClient;
use tokio::{signal, sync::oneshot};
use tracing::{error, info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    if let Err(e) = dotenv() {
        eprintln!("not loading .env file: {}", e);
    }
    rustls::crypto::ring::default_provider()
        .install_default()
        .unwrap();

    match std::env::var("FULFILLER_CLUSTER_ARTIFACT_STORE")
        .unwrap_or("s3".to_string())
        .as_str()
    {
        "s3" => {
            eprintln!("using s3 artifact store");
            let region = std::env::var("FULFILLER_CLUSTER_S3_REGION")
                .expect("FULFILLER_CLUSTER_S3_REGION is not set");
            run(S3ArtifactClient::new(
                region.clone(),
                std::env::var("FULFILLER_CLUSTER_S3_BUCKET")
                    .expect("FULFILLER_CLUSTER_S3_BUCKET is not set"),
                std::env::var("FULFILLER_S3_CONCURRENCY")
                    .map(|s| s.parse().unwrap_or(32))
                    .unwrap_or(32),
                S3DownloadMode::AwsSDK(
                    S3ArtifactClient::create_s3_sdk_download_client(region).await,
                ),
            )
            .await)
            .await
        }
        "redis" => {
            eprintln!("using redis artifact store");
            run(RedisArtifactClient::new(
                std::env::var("FULFILLER_CLUSTER_REDIS_NODES")
                    .expect("REDIS_NODES is not set")
                    .split(',')
                    .map(|s| s.to_string())
                    .collect(),
                std::env::var("FULFILLER_CLUSTER_REDIS_POOL_MAX_SIZE")
                    .unwrap_or("16".to_string())
                    .parse()
                    .unwrap(),
            ))
            .await
        }
        _ => panic!("invalid artifact store"),
    }?;

    Ok(())
}

async fn run<A: ArtifactClient>(artifact_client: A) -> Result<()> {
    // Load configuration
    let settings = Settings::new()?;

    // Initialize logging
    spn_utils::init_logger(settings.log_format);

    // Log configuration details
    info!(
        "fulfiller starting with addresses: {:?}",
        settings.addresses
    );

    // Initialize the dependencies.
    let network_channel = grpc::configure_endpoint(settings.rpc_grpc_addr.clone())?
        .connect()
        .await?;
    let network = ProverNetworkClient::new(network_channel);

    let cluster_rpc =
        std::env::var("FULFILLER_CLUSTER_RPC").unwrap_or("http://127.0.0.1:50051".to_string());
    let cluster = ClusterServiceClient::new(cluster_rpc)
        .await
        .expect("failed to connect to cluster");

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

    // Initialize the Fulfiller with metrics.
    let fulfiller = Fulfiller::new(
        network,
        cluster,
        artifact_client,
        settings.version,
        settings.domain,
        signer,
        metrics,
        settings.addresses,
        true,
    );

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
