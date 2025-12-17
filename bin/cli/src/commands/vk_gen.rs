use std::time::{Duration, SystemTime};

use clap::{Args, Parser, Subcommand};
use either::Either;
use eyre::Result;
use rand::Rng;
use sp1_cluster_artifact::redis::RedisArtifactClient;
use sp1_cluster_artifact::s3::{S3ArtifactClient, S3DownloadMode};
use sp1_cluster_artifact::ArtifactClient;
use sp1_cluster_utils::{request_config_from_env, ArtifactStoreConfig};
use sp1_cluster_worker::client::WorkerServiceClient;
use sp1_cluster_worker::utils::{current_context, task_metadata};
use sp1_prover::worker::{
    ProofId, TaskError, TaskId, VkeyMapControllerInput, VkeyMapControllerOutput, WorkerClient,
};
use sp1_prover_types::{
    cluster, ArtifactId, CreateDummyProofRequest, CreateTaskRequest, GetTaskStatusesRequest,
    TaskData, TaskStatus, TaskType,
};
use sp1_sdk::network::proto::types::ProofMode;

#[derive(Debug, Args)]
pub struct CommonArgs {
    /// The cluster API gRPC endpoint.
    #[arg(long, env = "CLI_CLUSTER_RPC")]
    pub cluster_rpc: String,

    /// Whether to execute the proof request before submitting it to the cluster.
    #[arg(long, default_value_t = false)]
    pub execute: bool,

    /// The S3 bucket the cluster artifact store is using.
    #[arg(long, env = "CLI_S3_BUCKET")]
    pub s3_bucket: Option<String>,

    /// The S3 region the cluster artifact store is using.
    #[arg(long, env = "CLI_S3_REGION")]
    pub s3_region: Option<String>,

    /// The Redis nodes the cluster artifact store is using. If not specified, the artifact store will be S3.
    #[arg(long, env = "CLI_REDIS_NODES")]
    pub redis_nodes: Option<String>,

    #[arg(short, long, default_value="compressed", value_parser = parse_proof_mode)]
    pub mode: ProofMode,

    #[arg(short, long, default_value_t = 1)]
    pub count: u32,
}

pub fn parse_proof_mode(s: &str) -> Result<ProofMode> {
    ProofMode::from_str_name(&s.to_ascii_uppercase())
        .ok_or_else(|| eyre::eyre!("Invalid proof mode"))
}

#[derive(Subcommand)]
pub enum BuildVkeys {
    BuildVkeys {
        #[clap(long)]
        limit: Option<usize>,

        #[clap(long, default_value = "1000")]
        chunk_size: usize,

        #[clap(long, default_value = "4")]
        reduce_batch_size: usize,
    },
}

impl BuildVkeys {
    pub async fn run(&self) -> Result<()> {
        let BuildVkeys::BuildVkeys {
            limit,
            chunk_size,
            reduce_batch_size,
        } = self;
        let input = VkeyMapControllerInput {
            range_or_limit: limit.map(Either::Right),
            chunk_size: *chunk_size,
            reduce_batch_size: *reduce_batch_size,
        };

        // Random hex string
        let mut rng = rand::thread_rng();
        let mut node_id = "unknown_".to_string();
        for _ in 0..16 {
            node_id.push_str(&format!("{:02x}", rng.gen::<u8>()));
        }

        let cluster_rpc = std::env::var("CLI_CLUSTER_RPC").unwrap();

        let mut cluster_client = WorkerServiceClient::new(cluster_rpc, node_id.clone())
            .await
            .unwrap();
        // TODO: Fix

        let request_config = request_config_from_env(ProofMode::Core, 24);

        let (bucket, region) = match &request_config.artifact_store {
            ArtifactStoreConfig::S3 { bucket, region } => Some((bucket.clone(), region.clone())),
            _ => None,
        }
        .unwrap();
        let artifact_client = S3ArtifactClient::new(
            region.clone(),
            bucket,
            32,
            S3DownloadMode::AwsSDK(S3ArtifactClient::create_s3_sdk_download_client(region).await),
        )
        .await;

        let input_artifact = artifact_client
            .create_artifact()
            .map_err(|e| eyre::eyre!(e))?;
        artifact_client
            .upload(&input_artifact, input)
            .await
            .map_err(|e| eyre::eyre!(e))?;

        let output_artifact = artifact_client
            .create_artifact()
            .map_err(|e| eyre::eyre!(e))?;

        let proof_id = format!(
            "build-vkeys-{}",
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        );

        tracing::info!("Creating dummy proof for proof id: {}", proof_id);

        cluster_client
            .client
            .create_dummy_proof(CreateDummyProofRequest {
                worker_id: "cli".to_string(),
                proof_id: proof_id.clone(),
                expires_at: SystemTime::now()
                    .checked_add(Duration::from_secs(60 * 60 * 4))
                    .unwrap()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as i64,
                requester: "cli".to_string(),
            })
            .await
            .map_err(|e| {
                tracing::warn!("Failed to create dummy proof: {:?}", e);
                e
            })
            .unwrap();

        //         TaskType::Sp1UtilVkeyMapController,
        // &[input_artifact.id.clone()],
        // &[output_artifact.id.clone()],
        // proof_id.clone(),
        // None,
        // None,
        // "cli".to_string(),

        let context = current_context();
        let metadata = serde_json::to_string(&task_metadata(&context))?;

        let task_data: TaskData = TaskData {
            task_type: TaskType::UtilVkeyMapController as i32,
            inputs: vec![input_artifact.id().to_string()],
            outputs: vec![output_artifact.id().to_string()],
            metadata: metadata,
            proof_id: proof_id.clone(),
            parent_id: None,
            weight: 0,
            requester: "cli".to_string(),
        };

        let task_request: CreateTaskRequest = CreateTaskRequest {
            data: Some(task_data),
            worker_id: node_id.clone(),
        };

        let task = cluster_client.client.create_task(task_request).await?;

        tracing::info!("created task: {:?}", task);

        tokio::time::sleep(Duration::from_secs(1)).await;
        let subscriber = cluster_client
            .subscriber(ProofId::new(proof_id.clone()))
            .await
            .map_err(|e| TaskError::Fatal(e.into()))?
            .per_task();
        let status = subscriber
            .wait_task(TaskId::new(task.get_ref().task_id.clone()))
            .await
            .map_err(|e| TaskError::Fatal(e.into()))?;

        tracing::warn!("Task Status: {:?}", status);

        assert!(matches!(status, TaskStatus::Succeeded));

        let result = artifact_client
            .download::<VkeyMapControllerOutput>(&output_artifact)
            .await
            .map_err(|e| eyre::eyre!(e))?;

        let mut file = std::fs::File::create("vk_map.bin")?;
        bincode::serialize_into(&mut file, &result.vk_map)?;

        assert_eq!(result.vk_map.len(), limit.unwrap_or(213681));
        assert_eq!(result.panic_indices.len(), 0);

        Ok(())
    }
}
