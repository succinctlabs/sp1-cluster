use anyhow::{bail, Context, Result};
use clap::Parser;
use opentelemetry_sdk::Resource;
use sp1_cluster_artifact::{
    redis::RedisArtifactClient,
    s3::{S3ArtifactClient, S3DownloadMode},
};
use sp1_cluster_common::logger;
use sp1_cluster_network_gateway::{config::Config, run};
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    if let Err(e) = dotenv::dotenv() {
        eprintln!("not loading .env file: {e}");
    }
    rustls::crypto::ring::default_provider()
        .install_default()
        .unwrap();
    logger::init(Resource::empty());

    let cfg = Config::parse();
    info!(
        grpc_addr = %cfg.grpc_addr,
        http_addr = %cfg.http_addr,
        cluster_rpc = %cfg.cluster_rpc,
        artifact_store = %cfg.artifact_store,
        "starting network-gateway"
    );

    match cfg.artifact_store.as_str() {
        "s3" => {
            let region = cfg
                .s3_region
                .clone()
                .context("GATEWAY_S3_REGION is required for s3 backend")?;
            let bucket = cfg
                .s3_bucket
                .clone()
                .context("GATEWAY_S3_BUCKET is required for s3 backend")?;
            let client = S3ArtifactClient::new(
                region.clone(),
                bucket,
                cfg.s3_concurrency,
                S3DownloadMode::AwsSDK(
                    S3ArtifactClient::create_s3_sdk_download_client(region).await,
                ),
            )
            .await;
            run(cfg, client).await
        }
        "redis" => {
            let nodes = cfg
                .redis_nodes
                .clone()
                .context("GATEWAY_REDIS_NODES is required for redis backend")?;
            let client = RedisArtifactClient::new(nodes, cfg.redis_pool_max_size);
            run(cfg, client).await
        }
        other => bail!("unknown GATEWAY_ARTIFACT_STORE={other} (expected s3 or redis)"),
    }
}
