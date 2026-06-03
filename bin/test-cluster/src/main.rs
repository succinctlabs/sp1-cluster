mod env;

use std::time::Duration;

use rand::Rng;
use sp1_cluster_api::ApiConfig;
use sp1_cluster_artifact::redis::RedisArtifactClient;
use sp1_cluster_common::proto::WorkerType;
use sp1_cluster_coordinator::config::Settings as CoordinatorSettings;
use sp1_cluster_coordinator::policy::balanced::BalancedPolicy;
use sp1_cluster_coordinator::server::start_coordinator_server_custom;
use sp1_cluster_network_gateway::auth::AuthMode;
use sp1_cluster_network_gateway::config::Config as GatewayConfig;
use sp1_sdk::network::signer::NetworkSigner;
use tokio_util::future::FutureExt;
use tokio_util::sync::CancellationToken;

use sp1_sdk::prelude::*;
use sp1_sdk::ProverClient;

use sp1_cluster_common::shutdown::wait_for_shutdown_signal;

const REDIS_POOL_MAX_SIZE: usize = 1;
const API_HTTP_ADDR: &str = "127.0.0.1:3000";
const API_GRPC_ADDR: &str = "127.0.0.1:50051";
const COORDINATOR_ADDR: &str = "127.0.0.1:50052";
const GATEWAY_GRPC_ADDR: &str = "127.0.0.1:50061";
const GATEWAY_HTTP_ADDR: &str = "127.0.0.1:8081";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    sp1_cluster_common::logger::init(opentelemetry_sdk::Resource::empty());

    let token = CancellationToken::new();
    let _shutdown_handle = tokio::spawn(wait_for_shutdown_signal(token.clone()));
    let (redis, postgres) = tokio::try_join!(
        env::Redis::start(token.clone()),
        env::Postgres::start(token.clone()),
    )?;
    let artifact_client = RedisArtifactClient::new(vec![redis.addr().into()], REDIS_POOL_MAX_SIZE);

    let api = tokio::spawn(
        sp1_cluster_api::run(ApiConfig {
            http_addr: API_HTTP_ADDR.to_string(),
            grpc_addr: API_GRPC_ADDR.to_string(),
            postgres_uri: postgres.addr().to_string(),
            postgres_auto_migrate: true,
        })
        .with_cancellation_token_owned(token.clone()),
    );
    wait_for_tcp(API_GRPC_ADDR, "api gRPC").await?;

    let (_coordinator, coordinator_future) = start_coordinator_server_custom::<BalancedPolicy>(
        CoordinatorSettings {
            addr: COORDINATOR_ADDR.to_string(),
            cluster_rpc: format!("http://{API_GRPC_ADDR}"),
            disable_proof_status_update: false,
            execute_only_mode: false,
        },
        token.clone(),
    )
    .await
    .map_err(|e| anyhow::anyhow!("coordinator startup failed: {e}"))?;
    let coordinator_handle = tokio::spawn(coordinator_future);
    wait_for_tcp(COORDINATOR_ADDR, "coordinator gRPC").await?;

    let gateway = tokio::spawn(
        sp1_cluster_network_gateway::run(
            GatewayConfig {
                grpc_addr: GATEWAY_GRPC_ADDR.to_string(),
                http_addr: GATEWAY_HTTP_ADDR.to_string(),
                public_http_url: format!("http://{GATEWAY_HTTP_ADDR}"),
                cluster_rpc: format!("http://{API_GRPC_ADDR}"),
                artifact_store: "redis".to_string(),
                s3_bucket: None,
                s3_region: None,
                s3_concurrency: 32,
                redis_nodes: Some(vec![redis.addr().to_string()]),
                redis_pool_max_size: REDIS_POOL_MAX_SIZE,
                balance_amount: None,
                auth_mode: AuthMode::None,
                auth_allowlist: None,
                program_store: "memory".to_string(),
                program_store_dir: None,
            },
            artifact_client.clone(),
        )
        .with_cancellation_token_owned(token.clone()),
    );

    let cpu_node = tokio::spawn(sp1_cluster_node::run(
        sp1_cluster_node::config::NodeConfig {
            worker_id: "test-cluster-node-cpu".to_string(),
            worker_type: WorkerType::Cpu,
            coordinator_rpc: format!("http://{COORDINATOR_ADDR}"),
            ..Default::default()
        },
        artifact_client.clone(),
        token.clone(),
        None,
    ));

    #[cfg(feature = "gpu")]
    let gpu_node = tokio::spawn(sp1_cluster_node::run(
        sp1_cluster_node::config::NodeConfig {
            worker_id: "test-cluster-node-gpu".to_string(),
            worker_type: WorkerType::Gpu,
            coordinator_rpc: format!("http://{COORDINATOR_ADDR}"),
            ..Default::default()
        },
        artifact_client.clone(),
        token.clone(),
        None,
    ));

    if let Some(result) = submit_proof_request(Elf::Static(&*test_artifacts::FIBONACCI_ELF), {
        let mut stdin = SP1Stdin::new();
        stdin.write(&500u32);
        stdin
    })
    .with_cancellation_token_owned(token.clone())
    .await
    {
        result?;
    }

    token.cancel();

    #[cfg(feature = "gpu")]
    {
        tracing::info!("Shutting down gpu node");
        let _ = gpu_node.await?;
    }
    tracing::info!("Shutting down cpu node");
    let _ = cpu_node.await?;
    tracing::info!("Shutting down gateway");
    let _ = gateway.await?;
    tracing::info!("Shutting down coordinator");
    let _ = coordinator_handle.await;
    tracing::info!("Shutting down api");
    let _ = api.await?;
    tracing::info!("Shutdown complete");

    Ok(())
}

/// Poll-connect to `addr` until it accepts a TCP connection (or time out).
async fn wait_for_tcp(addr: &str, what: &str) -> anyhow::Result<()> {
    for _ in 0..600 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            tracing::info!("{what} ready at {addr}");
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    anyhow::bail!("timed out waiting for {what} at {addr}");
}

async fn submit_proof_request(elf: Elf, stdin: SP1Stdin) -> anyhow::Result<()> {
    tracing::info!("Initializing prover");
    let prover = ProverClient::builder()
        .network()
        .hosted()
        .rpc_url(&format!("http://{GATEWAY_GRPC_ADDR}"))
        .signer(random_local_signer())
        .build()
        .await;
    let pk = prover.setup(elf).await?;
    let proof = prover.prove(&pk, stdin).compressed().await?;
    prover.verify(&proof, pk.verifying_key(), None)?;
    tracing::info!("Proof verified successfully!");
    Ok(())
}

pub fn random_local_signer() -> NetworkSigner {
    NetworkSigner::local(&format!(
        "{:032x}{:032x}",
        rand::rng().random::<u128>(),
        rand::rng().random::<u128>()
    ))
    .expect("should be a valid signer")
}
