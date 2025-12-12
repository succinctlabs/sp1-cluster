use std::time::{Duration, SystemTime};

use clap::{Args, Parser};
use either::Either;
use eyre::Result;
use rand::Rng;
use sp1_cluster_artifact::s3::{S3ArtifactClient, S3DownloadMode};
use sp1_cluster_artifact::ArtifactClient;
use sp1_cluster_worker::client::WorkerServiceClient;
use sp1_prover::worker::{VkeyMapControllerInput, VkeyMapControllerOutput};
use sp1_prover_types::{
    ArtifactId, CreateTaskRequest, GetTaskStatusesRequest, TaskData, TaskStatus, TaskType,
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

#[derive(Parser)]
pub struct BuildVkeys {
    #[clap(long)]
    pub limit: Option<usize>,

    #[clap(long, default_value = "1000")]
    pub chunk_size: usize,

    #[clap(long, default_value = "4")]
    pub reduce_batch_size: usize,
}

impl BuildVkeys {
    pub async fn run(&self) -> Result<()> {
        let input = VkeyMapControllerInput {
            range_or_limit: self.limit.map(Either::Right),
            chunk_size: self.chunk_size,
            reduce_batch_size: self.reduce_batch_size,
        };

        // Random hex string
        let mut rng = rand::thread_rng();
        let mut node_id = "unknown_".to_string();
        for _ in 0..16 {
            node_id.push_str(&format!("{:02x}", rng.gen::<u8>()));
        }

        let mut cluster_client =
            WorkerServiceClient::new(std::env::var("CLUSTER_V2_RPC").unwrap(), node_id.clone())
                .await
                .unwrap();
        // TODO: Fix
        let s3_client = S3ArtifactClient::new(
            "us-east-2".to_string(),
            std::env::var("S3_BUCKET").unwrap(),
            16,
            S3DownloadMode::AwsSDK(
                S3ArtifactClient::create_s3_sdk_download_client("us-east-2".to_string()).await,
            ),
        )
        .await;

        let input_artifact = s3_client.create_artifact().map_err(|e| eyre::eyre!(e))?;
        s3_client
            .upload(&input_artifact, input)
            .await
            .map_err(|e| eyre::eyre!(e))?;

        let output_artifact = s3_client.create_artifact().map_err(|e| eyre::eyre!(e))?;

        let proof_id = format!(
            "build-vkeys-{}",
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        );

        //         TaskType::Sp1UtilVkeyMapController,
        // &[input_artifact.id.clone()],
        // &[output_artifact.id.clone()],
        // proof_id.clone(),
        // None,
        // None,
        // "cli".to_string(),

        let task_data: TaskData = TaskData {
            task_type: TaskType::UtilVkeyMapController as i32,
            inputs: vec![input_artifact.id().to_string()],
            outputs: vec![output_artifact.id().to_string()],
            metadata: "".to_string(),
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

        println!("created task: {:?}", task);
        loop {
            let task_status = cluster_client
                .client
                .get_task_statuses(GetTaskStatusesRequest {
                    task_ids: vec![task.get_ref().task_id.clone()],
                    worker_id: node_id.clone(),
                    proof_id: proof_id.clone(),
                })
                .await?;

            match task_status.get_ref().statuses[0].key() {
                TaskStatus::Succeeded => {
                    tracing::info!(
                        "Vk generation completed for task {}",
                        task.get_ref().task_id
                    );

                    let output = s3_client
                        .download::<VkeyMapControllerOutput>(&output_artifact)
                        .await
                        .expect("failed to download proof");

                    println!("got {} vkeys", output.vk_map.len());
                    println!("got {} panic indices", output.panic_indices.len());

                    // Save vk map to file
                    let vk_map_path = format!("vk_map_{}.bin", output.vk_map.len());
                    let vk_map_file = std::fs::File::create(vk_map_path.clone())?;
                    let output = (output.vk_map, output.panic_indices);
                    bincode::serialize_into(vk_map_file, &output)?;

                    println!("saved vk map to file: {}", vk_map_path);

                    break;
                }
                TaskStatus::FailedFatal | TaskStatus::FailedRetryable => {
                    return Err(eyre::eyre!(
                        "Vk generation failed for task {}",
                        task.get_ref().task_id
                    ));
                }
                _ => {}
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        Ok(())
    }
}
