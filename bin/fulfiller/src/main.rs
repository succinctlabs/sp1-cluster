use crate::mainnet::MainnetFulfiller;
use anyhow::Result;
use dotenv::dotenv;
use sp1_cluster_artifact::{
    redis::RedisArtifactClient,
    s3::{S3ArtifactClient, S3DownloadMode},
};
use sp1_cluster_fulfillment::{config::FulfillerSettings, grpc, run::run_fulfiller};
use spn_network_types::prover_network_client::ProverNetworkClient;

pub mod mainnet;

#[tokio::main]
async fn main() -> Result<()> {
    if let Err(e) = dotenv() {
        eprintln!("not loading .env file: {e}");
    }
    rustls::crypto::ring::default_provider()
        .install_default()
        .unwrap();

    // Load configuration
    let settings = FulfillerSettings::new("FULFILLER")?;

    // Initialize the dependencies.
    println!("connecting to rpc: {}", settings.rpc_grpc_addr);
    let network_channel = grpc::configure_endpoint(settings.rpc_grpc_addr.clone())?
        .connect()
        .await?;
    println!("connected to network");
    let network = ProverNetworkClient::new(network_channel);

    let network = MainnetFulfiller::new(network);

    match std::env::var("FULFILLER_CLUSTER_ARTIFACT_STORE")
        .unwrap_or("s3".to_string())
        .as_str()
    {
        "s3" => {
            eprintln!("using s3 artifact store");
            let region = settings
                .cluster_s3_region
                .clone()
                .expect("FULFILLER_CLUSTER_S3_REGION is not set");
            run_fulfiller(
                S3ArtifactClient::new(
                    region.clone(),
                    std::env::var("FULFILLER_CLUSTER_S3_BUCKET")
                        .expect("FULFILLER_CLUSTER_S3_BUCKET is not set"),
                    settings.cluster_s3_concurrency.unwrap_or(32),
                    S3DownloadMode::AwsSDK(
                        S3ArtifactClient::create_s3_sdk_download_client(region).await,
                    ),
                )
                .await,
                network,
                &settings,
            )
            .await
        }
        "redis" => {
            eprintln!("using redis artifact store");
            run_fulfiller(
                RedisArtifactClient::new(
                    settings
                        .cluster_redis_nodes
                        .clone()
                        .expect("FULFILLER_CLUSTER_REDIS_NODES is not set"),
                    settings.cluster_redis_pool_max_size.unwrap_or(16),
                ),
                network,
                &settings,
            )
            .await
        }
        _ => panic!("invalid artifact store"),
    }?;

    Ok(())
}
