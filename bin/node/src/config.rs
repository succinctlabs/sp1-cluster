use rand::Rng;

use sp1_cluster_common::proto::WorkerType;
use sp1_cluster_worker::location;
use std::time::Duration;
use tracing::warn;

/// To prevent stuck tasks from accumulating due to any deadlock bug or similar issue, tasks will be
/// killed after running for 6 hours.
pub const DEFAULT_TASK_TIMEOUT: Duration = Duration::from_secs(6 * 60 * 60);

/// Drain bound: tasks still running at this point are cancelled mid-execution and
/// reassigned by the coordinator on heartbeat timeout. Stays under ECS `stopTimeout`
/// (3600s) so we exit cleanly instead of being SIGKILLed.
pub const DEFAULT_DRAIN_TIMEOUT: Duration = Duration::from_secs(30 * 60);

#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub worker_id: String,
    pub worker_type: WorkerType,
    pub coordinator_rpc: String,
    pub cluster: String,
    pub task_timeout: Duration,
    pub drain_timeout: Duration,
    /// Where this worker runs, as an opaque label the coordinator groups
    /// workers by. `None` when the environment does not identify one.
    pub location: Option<String>,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            worker_id: format!("unknown_{:032x}", rand::rng().random::<u128>()),
            worker_type: WorkerType::All,
            coordinator_rpc: Default::default(),
            cluster: "unknown".to_string(),
            task_timeout: DEFAULT_TASK_TIMEOUT,
            drain_timeout: DEFAULT_DRAIN_TIMEOUT,
            location: None,
        }
    }
}

impl NodeConfig {
    pub async fn load() -> Self {
        let ecs_task_info = location::get_ecs_task_info(&reqwest::Client::new()).await;
        let ambient_region = location::ambient_region();
        // Identity from the task metadata endpoint, with fallbacks for a worker
        // that cannot reach it.
        let (worker_id, location, cluster) = match ecs_task_info {
            Ok(info) => (
                info.task_id().to_string(),
                location::resolve(info.region(), ambient_region),
                info.cluster,
            ),
            Err(e) => {
                warn!("ECS task metadata unavailable, falling back to AWS_REGION: {e}");
                (
                    format!("unknown_{:032x}", rand::rng().random::<u128>()),
                    location::resolve(None, ambient_region),
                    "unknown".to_string(),
                )
            }
        };
        Self {
            worker_id,
            worker_type: {
                let worker_type = std::env::var("WORKER_TYPE").expect("WORKER_TYPE is not set");
                WorkerType::from_str_name(&worker_type).expect("invalid worker type")
            },
            coordinator_rpc: std::env::var("NODE_COORDINATOR_RPC")
                .unwrap_or("http://[::1]:50051".to_string()),
            cluster,
            task_timeout: DEFAULT_TASK_TIMEOUT,
            drain_timeout: DEFAULT_DRAIN_TIMEOUT,
            location,
        }
    }
}

#[derive(Debug, Clone)]
pub enum ArtifactStoreConfig {
    S3 {
        region: String,
        bucket: String,
        concurrency: usize,
    },
    Redis {
        nodes: Vec<String>,
        pool_max_size: usize,
    },
}

impl ArtifactStoreConfig {
    pub fn from_env() -> Self {
        let artifact_store = std::env::var("NODE_ARTIFACT_STORE").unwrap_or("s3".to_string());
        match artifact_store.as_str() {
            "s3" => Self::S3 {
                region: std::env::var("NODE_S3_REGION").expect("NODE_S3_REGION is not set"),
                bucket: std::env::var("NODE_S3_BUCKET").expect("NODE_S3_BUCKET is not set"),
                concurrency: std::env::var("NODE_S3_CONCURRENCY")
                    .map(|s| s.parse().unwrap_or(32))
                    .unwrap_or(32),
            },
            "redis" => Self::Redis {
                nodes: std::env::var("NODE_REDIS_NODES")
                    .expect("NODE_REDIS_NODES is not set")
                    .split(',')
                    .map(|s| s.to_string())
                    .collect(),
                pool_max_size: std::env::var("NODE_REDIS_POOL_MAX_SIZE")
                    .unwrap_or("16".to_string())
                    .parse()
                    .unwrap(),
            },
            artifact_store => {
                panic!(
                    r#"Invalid NODE_ARTIFACT_STORE value "{artifact_store}". Supported values: "s3", "redis""#
                )
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct PyroscopeConfig {
    pub application_name: String,
    pub user: String,
    pub password: String,
    pub url: String,
    pub samplerate: u32,
    pub worker_type: String,
    pub env_tag: String,
}

impl PyroscopeConfig {
    pub fn from_env() -> Option<Self> {
        let user = std::env::var("PYROSCOPE_USER").ok()?;
        Some(Self {
            application_name: "sp1-cluster".to_string(),
            user,
            password: std::env::var("PYROSCOPE_PASSWORD").unwrap(),
            url: std::env::var("PYROSCOPE_URL").unwrap(),
            samplerate: std::env::var("PYROSCOPE_SAMPLE_RATE")
                .unwrap()
                .to_string()
                .parse()
                .unwrap(),
            worker_type: std::env::var("WORKER_TYPE").unwrap(),
            env_tag: std::env::var("PYROSCOPE_ENV").unwrap_or_else(|_| "default".to_string()),
        })
    }
}
