use std::{collections::HashSet, path::PathBuf, time::Instant};

use clap::{Args, Subcommand};
use eyre::Result;
use sp1_cluster_artifact::{
    redis::RedisArtifactClient,
    s3::{S3ArtifactClient, S3DownloadMode},
    ArtifactClient, ArtifactType,
};
use sp1_cluster_common::{
    client::ClusterServiceClient,
    proto::{self, ProofRequestStatus},
};
use sp1_sdk::{network::proto::types::ProofMode, SP1Stdin};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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

#[derive(Subcommand, Debug)]
pub enum BenchCommand {
    Fibonacci {
        /// Number of cycles in millions to run the benchmark for.
        #[clap(default_value_t = 5)]
        mcycles: u32,
        #[clap(flatten)]
        common: CommonArgs,
    },
    Input {
        elf_file: PathBuf,
        stdin_file: PathBuf,
        #[clap(flatten)]
        common: CommonArgs,
    },
}

impl BenchCommand {
    pub async fn run(&self) -> Result<()> {
        // Run the actual benchmark
        match self {
            BenchCommand::Fibonacci { mcycles, common } => {
                tracing::info!(
                    "Running {}x Fibonacci {:?} for {} million cycles...",
                    common.count,
                    common.mode,
                    mcycles
                );
                let mut stdin = SP1Stdin::new();
                stdin.write(&(mcycles * 83333));

                let elf = include_bytes!("../../../../artifacts/fibonacci.bin");
                Self::run_benchmark(
                    elf.to_vec(),
                    bincode::serialize(&stdin).unwrap(),
                    common,
                    Some(*mcycles as u64 * 1_000_000),
                )
                .await?;
            }
            BenchCommand::Input {
                elf_file,
                stdin_file,
                common,
            } => {
                tracing::info!(
                    "Running {:?} / {:?} {:?} benchmark...",
                    elf_file,
                    stdin_file,
                    common.mode
                );
                let elf = std::fs::read(elf_file)?;
                let stdin = std::fs::read(stdin_file)?;
                Self::run_benchmark(elf.to_vec(), stdin.to_vec(), common, None).await?;
            }
        }
        Ok(())
    }

    async fn run_benchmark(
        elf: Vec<u8>,
        stdin: Vec<u8>,
        common: &CommonArgs,
        cycles_estimate: Option<u64>,
    ) -> Result<()> {
        let client = ClusterServiceClient::new(common.cluster_rpc.clone()).await?;

        let (elf_id, stdin_id, proof_output_ids) = if let Some(redis_nodes) = &common.redis_nodes {
            tracing::info!("using redis artifact store");
            let artifact_client = RedisArtifactClient::new(
                redis_nodes
                    .clone()
                    .split(',')
                    .map(|s| s.to_string())
                    .collect(),
                16,
            );
            Self::setup_artifacts(artifact_client, elf, stdin, common.count).await?
        } else {
            if common.s3_bucket.is_none() || common.s3_region.is_none() {
                return Err(eyre::eyre!(
                    "S3 bucket and region or Redis nodes must be specified"
                ));
            }
            tracing::info!("using s3 artifact store");
            let artifact_client = S3ArtifactClient::new(
                common.s3_region.clone().unwrap(),
                common.s3_bucket.clone().unwrap(),
                32,
                S3DownloadMode::AwsSDK(
                    S3ArtifactClient::create_s3_sdk_download_client(
                        common.s3_region.clone().unwrap(),
                    )
                    .await,
                ),
            )
            .await;
            Self::setup_artifacts(artifact_client, elf, stdin, common.count).await?
        };

        let base_id = format!(
            "cli_{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis()
        );
        tracing::info!("base_id: {}", base_id);
        // Worst case timeout is 4 hours.
        let deadline = SystemTime::now() + Duration::from_secs(4 * 60 * 60);
        for i in 0..common.count {
            client
                .create_proof_request(sp1_cluster_common::proto::ProofRequestCreateRequest {
                    proof_id: format!("{}_{}", base_id, i),
                    program_artifact_id: elf_id.clone(),
                    stdin_artifact_id: stdin_id.clone(),
                    options_artifact_id: Some((common.mode as i32).to_string()),
                    proof_artifact_id: Some(proof_output_ids[i as usize].clone()),
                    requester: vec![],
                    deadline: deadline.duration_since(UNIX_EPOCH).unwrap().as_secs(),
                    cycle_limit: 0,
                    gas_limit: 0,
                })
                .await?;
        }
        let start_time = Instant::now();
        // Poll until the proof request is completed.
        let mut completed = HashSet::new();
        loop {
            if deadline < SystemTime::now() {
                return Err(eyre::eyre!(
                    "Timeout exceeded after {:?}",
                    start_time.elapsed()
                ));
            }

            for i in 0..common.count {
                if completed.contains(&i) {
                    continue;
                }
                let resp = client
                    .get_proof_request(proto::ProofRequestGetRequest {
                        proof_id: format!("{}_{}", base_id, i),
                    })
                    .await?;
                let Some(proof_request) = resp else {
                    return Err(eyre::eyre!(
                        "Proof request {} not found after {:?}",
                        i,
                        start_time.elapsed()
                    ));
                };
                match proof_request.proof_status() {
                    ProofRequestStatus::Completed => {
                        completed.insert(i);
                    }
                    ProofRequestStatus::Failed | ProofRequestStatus::Cancelled => {
                        return Err(eyre::eyre!(
                            "Proof request {:?} after {:?}",
                            proof_request.proof_status(),
                            start_time.elapsed()
                        ));
                    }
                    _ => {}
                }
            }
            if completed.len() == common.count as usize {
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        tracing::info!("Completed after {:?}", start_time.elapsed());
        if let Some(cycles_estimate) = cycles_estimate {
            tracing::info!(
                "Total Cycles: {:.2} | Aggregate MHz: {:.2}",
                cycles_estimate * common.count as u64,
                cycles_estimate as f64 * common.count as f64
                    / start_time.elapsed().as_secs_f64()
                    / 1_000_000.0
            );
        }
        Ok(())
    }

    async fn setup_artifacts<A: ArtifactClient>(
        artifact_client: A,
        elf: Vec<u8>,
        stdin: Vec<u8>,
        count: u32,
    ) -> Result<(String, String, Vec<String>)> {
        let elf_id = artifact_client.create_artifact().unwrap();
        artifact_client
            .upload_with_type(&elf_id, ArtifactType::Program, elf)
            .await
            .map_err(|e| eyre::eyre!(e))?;
        let stdin_id = artifact_client.create_artifact().unwrap();
        artifact_client
            .upload_raw(&stdin_id, ArtifactType::Stdin, stdin)
            .await
            .map_err(|e| eyre::eyre!(e))?;
        let proof_output_ids = (0..count)
            .map(|_| artifact_client.create_artifact().unwrap().to_id())
            .collect();
        Ok((elf_id.to_id(), stdin_id.to_id(), proof_output_ids))
    }
}
