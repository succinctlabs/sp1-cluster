use std::{path::PathBuf, time::Instant};

use clap::{Args, Subcommand};
use eyre::Result;
use sp1_cluster_artifact::{
    redis::RedisArtifactClient, s3::S3ArtifactClient, ArtifactClient, ArtifactType,
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
}

fn parse_proof_mode(s: &str) -> Result<ProofMode> {
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
                    "Running Fibonacci {:?} for {} million cycles...",
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

        let (elf_id, stdin_id, proof_output_id) = if let Some(redis_nodes) = &common.redis_nodes {
            tracing::info!("using redis artifact store");
            let artifact_client = RedisArtifactClient::new(
                redis_nodes
                    .clone()
                    .split(',')
                    .map(|s| s.to_string())
                    .collect(),
                16,
            );
            Self::upload(artifact_client, elf, stdin).await?
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
            )
            .await;
            Self::upload(artifact_client, elf, stdin).await?
        };

        let proof_id = format!(
            "cli_{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis()
        );
        tracing::info!("proof_id: {}", proof_id);
        // Worst case timeout is 4 hours.
        let deadline = SystemTime::now() + Duration::from_secs(4 * 60 * 60);
        client
            .create_proof_request(sp1_cluster_common::proto::ProofRequestCreateRequest {
                proof_id: proof_id.clone(),
                program_artifact_id: elf_id,
                stdin_artifact_id: stdin_id,
                options_artifact_id: Some((common.mode as i32).to_string()),
                proof_artifact_id: Some(proof_output_id),
                requester: vec![],
                deadline: deadline.duration_since(UNIX_EPOCH).unwrap().as_secs(),
                cycle_limit: 0,
                gas_limit: 0,
            })
            .await?;
        let start_time = Instant::now();
        // Poll until the proof request is completed.
        loop {
            if deadline < SystemTime::now() {
                return Err(eyre::eyre!(
                    "Timeout exceeded after {:?}",
                    start_time.elapsed()
                ));
            }

            let resp = client
                .get_proof_request(proto::ProofRequestGetRequest {
                    proof_id: proof_id.clone(),
                })
                .await?;
            let Some(proof_request) = resp else {
                return Err(eyre::eyre!(
                    "Proof request not found after {:?}",
                    start_time.elapsed()
                ));
            };
            match proof_request.proof_status() {
                ProofRequestStatus::Completed => {
                    tracing::info!("Proof request completed after {:?}", start_time.elapsed());
                    if let Some(cycles_estimate) = cycles_estimate {
                        tracing::info!(
                            "Aggregate MHz: {:.2}",
                            cycles_estimate as f64
                                / start_time.elapsed().as_secs_f64()
                                / 1_000_000.0
                        );
                    }
                    break;
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
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        Ok(())
    }

    async fn upload<A: ArtifactClient>(
        artifact_client: A,
        elf: Vec<u8>,
        stdin: Vec<u8>,
    ) -> Result<(String, String, String)> {
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
        Ok((
            elf_id.to_id(),
            stdin_id.to_id(),
            artifact_client.create_artifact().unwrap().to_id(),
        ))
    }
}
