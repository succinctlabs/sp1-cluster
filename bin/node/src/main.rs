mod config;

use eyre::Result;
use jemallocator::Jemalloc;
use opentelemetry::KeyValue;
use pyroscope::PyroscopeAgent;
use pyroscope_pprofrs::{pprof_backend, PprofConfig};
use sp1_cluster_artifact::{
    redis::RedisArtifactClient,
    s3::{S3ArtifactClient, S3DownloadMode},
};
use sp1_cluster_common::util::get_private_ip;
use sp1_cluster_node::config::{ArtifactStoreConfig, NodeConfig, PyroscopeConfig};
use sp1_cluster_worker::metrics::initialize_metrics;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

#[global_allocator]
pub static ALLOCATOR: Jemalloc = Jemalloc;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    std::env::set_var("PROVER_CORE_CACHE_SIZE", "1");
    std::env::set_var("PROVER_COMPRESS_CACHE_SIZE", "1");
    match dotenv::dotenv() {
        std::result::Result::Ok(_) => {}
        Err(e) => {
            println!("failed to load .env file: {:?}", e);
        }
    }

    let config = NodeConfig::load().await;
    sp1_cluster_common::logger::init(opentelemetry_sdk::Resource::new(vec![
        KeyValue::new("service.name", "sp1-cluster-node"),
        KeyValue::new("node.cluster", config.cluster.clone()),
        KeyValue::new("node.id", config.worker_id.clone()),
    ]));

    tracing::info!("worker type: {:?}", config.worker_type);
    match get_private_ip() {
        Ok(Some(ip)) => {
            tracing::info!("private IP address: {ip}");
        }
        Ok(None) => {
            tracing::info!("no private IP address found");
        }
        Err(e) => {
            tracing::error!("failed to get private IP address: {e:?}");
        }
    }

    let agent = PyroscopeConfig::from_env().map(|config| {
        PyroscopeAgent::builder(config.url, config.application_name)
            .basic_auth(config.user, config.password)
            .backend(pprof_backend(
                PprofConfig::new().sample_rate(config.samplerate),
            ))
            .tags(
                [
                    ("env", config.env_tag.as_str()),
                    ("worker_type", config.worker_type.as_str()),
                ]
                .to_vec(),
            )
            .build()
            .unwrap()
            .start()
            .unwrap()
    });

    let (metrics, _metrics_server_handle, metrics_shutdown_tx) =
        initialize_metrics().await.map_err(|e| eyre::eyre!(e))?;

    let token = CancellationToken::new();
    {
        let shutting_down = Arc::new(AtomicBool::new(false));
        let token = token.clone();
        ctrlc::set_handler({
            move || {
                // Atomic CAS so two near-simultaneous signals can't both enter the first branch.
                let was_first_signal = shutting_down
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok();
                if was_first_signal {
                    println!(
                "\nReceived signal, waiting for tasks to finish... (Ctrl+C again to force exit)"
            );
                    // No receivers means `run_worker` already unwound — force-exit.
                    token.cancel();
                    let _ = metrics_shutdown_tx.send(());
                } else {
                    std::process::exit(1);
                }
            }
        })
        .unwrap();
    }
    match ArtifactStoreConfig::from_env() {
        ArtifactStoreConfig::S3 {
            region,
            bucket,
            concurrency,
        } => {
            eprintln!("using s3 artifact store");
            let s3 = S3ArtifactClient::new(
                region.clone(),
                bucket,
                concurrency,
                S3DownloadMode::AwsSDK(
                    S3ArtifactClient::create_s3_sdk_download_client(region).await,
                ),
            )
            .await;
            sp1_cluster_node::run(config, s3, token, Some(metrics)).await?;
        }
        ArtifactStoreConfig::Redis {
            nodes,
            pool_max_size,
        } => {
            eprintln!("using redis artifact store");
            let redis = RedisArtifactClient::new(nodes, pool_max_size);
            redis
                .validate_config()
                .await
                .map_err(|e| eyre::eyre!("{e}"))?;
            eprintln!("redis is set up");
            sp1_cluster_node::run(config, redis, token, Some(metrics)).await?;
        }
    }

    if let Some(agent) = agent {
        agent.stop().unwrap();
    }

    tracing::info!("Shutdown complete");

    Ok(())
}
