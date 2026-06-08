use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use sp1_cluster_api::ApiConfig;
use sp1_cluster_artifact::redis::RedisArtifactClient;
use sp1_cluster_artifact::s3::{S3ArtifactClient, S3DownloadMode};
use sp1_cluster_artifact::{ArtifactClient, CompressedUpload};
use sp1_cluster_common::client::ClusterServiceClient;
use sp1_cluster_common::proto::worker_service_client::WorkerServiceClient as RawCoordinatorClient;
use sp1_cluster_common::proto::WorkerType;
use sp1_cluster_coordinator::config::Settings as CoordinatorSettings;
use sp1_cluster_coordinator::policy::balanced::BalancedPolicy;
use sp1_cluster_coordinator::server::start_coordinator_server_custom;
use sp1_cluster_network_gateway::auth::AuthMode;
use sp1_cluster_network_gateway::config::Config as GatewayConfig;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;

use crate::{env, utils};

pub const REDIS_POOL_MAX_SIZE: usize = 1;
pub const GATEWAY_GRPC_ADDR: &str = "127.0.0.1:50061";
pub const GATEWAY_HTTP_ADDR: &str = "127.0.0.1:8081";
pub const API_HTTP_ADDR: &str = "127.0.0.1:3000";
pub const API_GRPC_ADDR: &str = "127.0.0.1:50051";
pub const COORDINATOR_ADDR: &str = "127.0.0.1:50052";
pub const COORDINATOR_METRICS_ADDR: &str = "0.0.0.0:9090";
pub const MINIO_BUCKET: &str = "sp1-test-cluster-artifacts";

/// How long `start()` waits for all requested workers to register with the coordinator.
/// Generous: CPU nodes download groth16+plonk circuit artifacts before connecting.
const WORKER_READY_TIMEOUT: Duration = Duration::from_secs(30 * 60);

/// Marker bound for everything the cluster needs from an artifact store.
pub trait StoreClient: ArtifactClient + CompressedUpload + Clone {}
impl<T: ArtifactClient + CompressedUpload + Clone> StoreClient for T {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoordinatorKind {
    Prover,
    ExecuteOnly,
}

type RespawnFn = Box<dyn Fn(CancellationToken) -> JoinHandle<()> + Send + Sync>;

struct Component {
    token: CancellationToken,
    /// None once the task has been awaited to completion (a JoinHandle must not be
    /// polled again after it resolves).
    handle: Option<JoinHandle<()>>,
    /// Re-create this component under a fresh child token (restart scenarios).
    respawn: RespawnFn,
}

pub struct ClusterBuilder {
    coordinator: CoordinatorKind,
    cpu_nodes: usize,
    gpu_nodes: usize,
    worker_heartbeat_timeout_secs: u64,
}

pub struct Cluster<A: StoreClient> {
    pub root: CancellationToken,
    components: HashMap<String, Component>,
    // Containers stop when dropped; keep them alive for the cluster lifetime.
    _redis: Option<env::Redis>,
    _minio: Option<env::Minio>,
    _postgres: env::Postgres,
    artifact_client: A,
}

impl Cluster<RedisArtifactClient> {
    pub fn builder() -> ClusterBuilder {
        ClusterBuilder {
            coordinator: CoordinatorKind::Prover,
            cpu_nodes: 0,
            gpu_nodes: 0,
            worker_heartbeat_timeout_secs:
                sp1_cluster_coordinator::DEFAULT_WORKER_HEARTBEAT_TIMEOUT,
        }
    }

    /// Spec "standard" shape: 1 CPU + 1 GPU node on the gpu flavor, 1 CPU node on cpu-only.
    pub fn standard() -> ClusterBuilder {
        let gpu_nodes = if cfg!(feature = "gpu") { 1 } else { 0 };
        Self::builder().cpu_nodes(1).gpu_nodes(gpu_nodes)
    }
}

impl<A: StoreClient> Cluster<A> {
    pub fn gateway_rpc_url(&self) -> String {
        format!("http://{GATEWAY_GRPC_ADDR}")
    }

    pub fn artifact_client(&self) -> A {
        self.artifact_client.clone()
    }

    pub async fn api_client(&self) -> Result<ClusterServiceClient> {
        ClusterServiceClient::new(format!("http://{API_GRPC_ADDR}"))
            .await
            .map_err(|e| anyhow!("failed to connect to cluster api: {e}"))
    }

    pub async fn coordinator_client(&self) -> Result<RawCoordinatorClient<Channel>> {
        RawCoordinatorClient::connect(format!("http://{COORDINATOR_ADDR}"))
            .await
            .context("failed to connect to coordinator")
    }

    /// Crash a component: abort its task. Futures drop, TCP connections close, the
    /// coordinator's heartbeat cleanup eventually notices (worker components).
    pub fn kill(&mut self, name: &str) -> Result<()> {
        let c = self
            .components
            .get(name)
            .ok_or_else(|| anyhow!("unknown component {name}"))?;
        tracing::info!("killing component {name} (abort)");
        if let Some(handle) = c.handle.as_ref() {
            handle.abort();
        }
        Ok(())
    }

    /// Gracefully stop a component: cancel its token and await its task. The component
    /// stays registered, so `restart` can bring it back.
    pub async fn stop(&mut self, name: &str) -> Result<()> {
        let c = self
            .components
            .get_mut(name)
            .ok_or_else(|| anyhow!("unknown component {name}"))?;
        tracing::info!("stopping component {name} (cancel + await)");
        c.token.cancel();
        if let Some(handle) = c.handle.take() {
            let _ = tokio::time::timeout(Duration::from_secs(60), handle).await;
        }
        Ok(())
    }

    /// Restart a component (after `kill` or `stop`, or live): cancel/abort the old task,
    /// then respawn it with the same config on a fresh child token.
    pub async fn restart(&mut self, name: &str) -> Result<()> {
        let c = self
            .components
            .get_mut(name)
            .ok_or_else(|| anyhow!("unknown component {name}"))?;
        tracing::info!("restarting component {name}");
        c.token.cancel();
        if let Some(mut handle) = c.handle.take() {
            if tokio::time::timeout(Duration::from_secs(30), &mut handle)
                .await
                .is_err()
            {
                tracing::warn!("component {name} did not stop within 30s; aborting");
                handle.abort();
                let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
            }
        }
        let token = self.root.child_token();
        c.handle = Some((c.respawn)(token.clone()));
        c.token = token;
        Ok(())
    }

    /// Shut the whole cluster down: cancel root, await every component (bounded).
    pub async fn shutdown(mut self) {
        self.root.cancel();
        for (name, c) in self.components.drain() {
            let Some(handle) = c.handle else { continue };
            match tokio::time::timeout(Duration::from_secs(60), handle).await {
                Ok(_) => tracing::info!("component {name} exited"),
                Err(_) => tracing::warn!("component {name} did not exit within 60s"),
            }
        }
    }

    /// Poll coordinator GetStats until at least (cpu, gpu) workers are registered.
    pub async fn wait_for_workers(&self, cpu: u32, gpu: u32, timeout: Duration) -> Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if let Ok(mut client) = self.coordinator_client().await {
                if let Ok(stats) = client.get_stats(tonic::Request::new(())).await {
                    let stats = stats.into_inner();
                    if stats.cpu_workers >= cpu && stats.gpu_workers >= gpu {
                        tracing::info!(
                            "workers ready: {} cpu, {} gpu",
                            stats.cpu_workers,
                            stats.gpu_workers
                        );
                        return Ok(());
                    }
                    tracing::info!(
                        "waiting for workers: have {}/{} cpu, {}/{} gpu",
                        stats.cpu_workers,
                        cpu,
                        stats.gpu_workers,
                        gpu
                    );
                }
            }
            if tokio::time::Instant::now() >= deadline {
                anyhow::bail!("timed out waiting for {cpu} cpu / {gpu} gpu workers");
            }
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(2)) => {}
                _ = self.root.cancelled() => {
                    anyhow::bail!(
                        "cluster root cancelled while waiting for workers \
                         (component startup failure — see error logs above)"
                    );
                }
            }
        }
    }
}

impl ClusterBuilder {
    pub fn coordinator(mut self, kind: CoordinatorKind) -> Self {
        self.coordinator = kind;
        self
    }

    pub fn cpu_nodes(mut self, n: usize) -> Self {
        self.cpu_nodes = n;
        self
    }

    /// Shorten the dead-worker heartbeat timeout (worker-death scenarios shouldn't idle 30s).
    pub fn worker_heartbeat_timeout_secs(mut self, secs: u64) -> Self {
        self.worker_heartbeat_timeout_secs = secs;
        self
    }

    pub fn gpu_nodes(mut self, n: usize) -> Self {
        assert!(
            n == 0 || cfg!(feature = "gpu"),
            "gpu_nodes requires the gpu feature"
        );
        self.gpu_nodes = n;
        self
    }

    /// Start with the redis artifact store (the default everywhere but `s3-artifacts`).
    pub async fn start(self) -> Result<Cluster<RedisArtifactClient>> {
        let root = CancellationToken::new();
        let (redis, postgres) = tokio::try_join!(
            env::Redis::start(root.clone()),
            env::Postgres::start(root.clone()),
        )?;
        let artifact_client =
            RedisArtifactClient::new(vec![redis.addr().into()], REDIS_POOL_MAX_SIZE);
        let redis_nodes = Some(vec![redis.addr().to_string()]);
        self.boot(
            root,
            Some(redis),
            None,
            postgres,
            artifact_client,
            "redis",
            redis_nodes,
        )
        .await
    }

    /// Start with a MinIO-backed S3 artifact store (`s3-artifacts` scenario). The AWS SDK
    /// is pointed at MinIO via standard env vars — safe because every scenario runs in its
    /// own process.
    pub async fn start_s3(self) -> Result<Cluster<S3ArtifactClient>> {
        let root = CancellationToken::new();
        let (minio, postgres) = tokio::try_join!(
            env::Minio::start(root.clone()),
            env::Postgres::start(root.clone()),
        )?;
        std::env::set_var("AWS_ENDPOINT_URL", &minio.endpoint);
        std::env::set_var("AWS_ACCESS_KEY_ID", env::MINIO_USER);
        std::env::set_var("AWS_SECRET_ACCESS_KEY", env::MINIO_PASSWORD);
        std::env::set_var("AWS_REGION", "us-east-1");
        std::env::set_var("AWS_S3_FORCE_PATH_STYLE", "1");

        let s3_dl_mode = S3DownloadMode::AwsSDK(
            S3ArtifactClient::create_s3_sdk_download_client("us-east-1".to_string()).await,
        );
        let artifact_client = S3ArtifactClient::new(
            "us-east-1".to_string(),
            MINIO_BUCKET.to_string(),
            32,
            s3_dl_mode,
        )
        .await;
        artifact_client
            .sdk_client
            .create_bucket()
            .bucket(MINIO_BUCKET)
            .send()
            .await
            .context("create minio bucket")?;

        self.boot(
            root,
            None,
            Some(minio),
            postgres,
            artifact_client,
            "s3",
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn boot<A: StoreClient>(
        self,
        root: CancellationToken,
        redis: Option<env::Redis>,
        minio: Option<env::Minio>,
        postgres: env::Postgres,
        artifact_client: A,
        artifact_store_name: &str,
        gateway_redis_nodes: Option<Vec<String>>,
    ) -> Result<Cluster<A>> {
        let mut components: HashMap<String, Component> = HashMap::new();

        // api
        {
            let postgres_uri = postgres.addr().to_string();
            let respawn = component_factory("api", root.clone(), move |token| {
                let config = ApiConfig {
                    http_addr: API_HTTP_ADDR.to_string(),
                    grpc_addr: API_GRPC_ADDR.to_string(),
                    postgres_uri: postgres_uri.clone(),
                    postgres_auto_migrate: true,
                };
                async move {
                    sp1_cluster_api::run_with_shutdown(config, token)
                        .await
                        .map_err(|e| anyhow!("api exited: {e:?}"))
                }
            });
            insert_component(&mut components, &root, "api", respawn);
        }
        utils::wait_for_tcp(API_GRPC_ADDR, "api gRPC").await?;

        // coordinator
        {
            let settings = CoordinatorSettings {
                addr: COORDINATOR_ADDR.to_string(),
                cluster_rpc: format!("http://{API_GRPC_ADDR}"),
                metrics_addr: COORDINATOR_METRICS_ADDR.to_string(),
                disable_proof_status_update: false,
                execute_only_mode: self.coordinator == CoordinatorKind::ExecuteOnly,
                worker_heartbeat_timeout_secs: self.worker_heartbeat_timeout_secs,
            };
            let respawn = component_factory("coordinator", root.clone(), move |token| {
                let settings = settings.clone();
                async move {
                    let (_coordinator, fut) =
                        start_coordinator_server_custom::<BalancedPolicy>(settings, token)
                            .await
                            .map_err(|e| anyhow!("coordinator startup failed: {e}"))?;
                    fut.await
                        .map_err(|e| anyhow!("coordinator server exited: {e:?}"))
                }
            });
            insert_component(&mut components, &root, "coordinator", respawn);
        }
        utils::wait_for_tcp(COORDINATOR_ADDR, "coordinator gRPC").await?;

        // gateway
        {
            let config = GatewayConfig {
                grpc_addr: GATEWAY_GRPC_ADDR.to_string(),
                http_addr: GATEWAY_HTTP_ADDR.to_string(),
                public_http_url: format!("http://{GATEWAY_HTTP_ADDR}"),
                cluster_rpc: format!("http://{API_GRPC_ADDR}"),
                artifact_store: artifact_store_name.to_string(),
                s3_bucket: None,
                s3_region: None,
                s3_concurrency: 32,
                redis_nodes: gateway_redis_nodes,
                redis_pool_max_size: REDIS_POOL_MAX_SIZE,
                balance_amount: None,
                auth_mode: AuthMode::None,
                auth_allowlist: None,
                program_store: "memory".to_string(),
                program_store_dir: None,
            };
            let client = artifact_client.clone();
            let respawn = component_factory("gateway", root.clone(), move |token| {
                let config = config.clone();
                let client = client.clone();
                async move {
                    sp1_cluster_network_gateway::run_with_shutdown(config, client, token)
                        .await
                        .map_err(|e| anyhow!("gateway exited: {e:?}"))
                }
            });
            insert_component(&mut components, &root, "gateway", respawn);
        }
        utils::wait_for_tcp(GATEWAY_GRPC_ADDR, "gateway gRPC").await?;

        // nodes
        for i in 0..self.cpu_nodes {
            spawn_node(
                &mut components,
                &root,
                &artifact_client,
                format!("cpu-node-{i}"),
                WorkerType::Cpu,
            );
        }
        for i in 0..self.gpu_nodes {
            spawn_node(
                &mut components,
                &root,
                &artifact_client,
                format!("gpu-node-{i}"),
                WorkerType::Gpu,
            );
        }

        let cluster = Cluster {
            root,
            components,
            _redis: redis,
            _minio: minio,
            _postgres: postgres,
            artifact_client,
        };
        cluster
            .wait_for_workers(
                self.cpu_nodes as u32,
                self.gpu_nodes as u32,
                WORKER_READY_TIMEOUT,
            )
            .await?;
        Ok(cluster)
    }
}

fn spawn_node<A: StoreClient>(
    components: &mut HashMap<String, Component>,
    root: &CancellationToken,
    artifact_client: &A,
    name: String,
    worker_type: WorkerType,
) {
    let client = artifact_client.clone();
    let worker_id = format!("test-cluster-{name}");
    let respawn = component_factory(&name.clone(), root.clone(), move |token| {
        let client = client.clone();
        let worker_id = worker_id.clone();
        async move {
            sp1_cluster_node::run(
                sp1_cluster_node::config::NodeConfig {
                    worker_id,
                    worker_type,
                    coordinator_rpc: format!("http://{COORDINATOR_ADDR}"),
                    ..Default::default()
                },
                client,
                token,
                None,
            )
            .await
            .map_err(|e| anyhow!("node exited: {e:?}"))
        }
    });
    insert_component(components, root, &name, respawn);
}

/// Build a respawn factory for a component: each call spawns the component's future under
/// the given token, wrapped so an Err cancels the cluster root (fail-fast).
fn component_factory<F, Fut>(name: &str, root: CancellationToken, make: F) -> RespawnFn
where
    F: Fn(CancellationToken) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    let name = name.to_string();
    let make = Arc::new(make);
    Box::new(move |token: CancellationToken| {
        let name = name.clone();
        let root = root.clone();
        let fut = (make.clone())(token);
        tokio::spawn(async move {
            match fut.await {
                Ok(()) => tracing::info!("component {name} exited cleanly"),
                Err(e) => {
                    tracing::error!("component {name} failed: {e:?}");
                    root.cancel();
                }
            }
        })
    })
}

fn insert_component(
    components: &mut HashMap<String, Component>,
    root: &CancellationToken,
    name: &str,
    respawn: RespawnFn,
) {
    let token = root.child_token();
    let handle = respawn(token.clone());
    components.insert(
        name.to_string(),
        Component {
            token,
            handle: Some(handle),
            respawn,
        },
    );
}
