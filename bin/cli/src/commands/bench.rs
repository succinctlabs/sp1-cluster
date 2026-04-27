use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::{Args, Subcommand};
use eyre::Result;
use sp1_cluster_artifact::{redis::RedisArtifactClient, ArtifactClient, ArtifactType};
use sp1_cluster_common::proto::{CreateDummyProofRequest, TaskStatus, TaskType};
use sp1_cluster_utils::{request_proof_from_env, ClusterElf, ProofRequestResults};
use sp1_cluster_worker::client::WorkerServiceClient;
use sp1_prover::worker::{
    ProofId, RawTaskRequest, RequesterId, SP1LightNode, TaskContext, WorkerClient,
};
use sp1_prover_types::Artifact;
use sp1_sdk::{network::proto::types::ProofMode, SP1Stdin};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

#[derive(Debug, Args)]
pub struct CommonArgs {
    /// The cluster/coordinator gRPC endpoint.
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
    S3 {
        /// S3 path to the program (e.g., "path/to/program" for s3://bucket/path/to/program/)
        s3_path: String,
        #[arg(short, long, default_value = "")]
        param: String,
        /// S3 bucket to download from
        #[arg(long, env = "CLI_BENCH_S3_BUCKET", default_value = "sp1-testing-suite")]
        bucket: String,
        #[clap(flatten)]
        common: CommonArgs,
    },
    /// Execute-only benchmark using S3 program (hits executor path, no proving)
    ExecuteS3 {
        /// S3 path to the program (e.g., "path/to/program" for s3://bucket/path/to/program/)
        s3_path: String,
        #[arg(short, long, default_value = "")]
        param: String,
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
                Self::run_benchmark(elf.to_vec(), stdin.clone(), common).await?;
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
                let stdin = bincode::deserialize::<SP1Stdin>(&std::fs::read(stdin_file)?)?;
                let proof_ids = Self::run_benchmark(elf.to_vec(), stdin, common).await?;

                let elf_name = elf_file.file_name().unwrap().to_str().unwrap();
                let workload_name = stdin_file.file_name().unwrap().to_str().unwrap();
                // Write all proof IDs to CSV
                for (proof_id, duration) in proof_ids {
                    if let Err(e) =
                        Self::append_to_csv(&proof_id, elf_name, Some(workload_name), duration)
                    {
                        tracing::warn!("Failed to write to CSV: {}", e);
                    }
                }
            }
            BenchCommand::S3 {
                s3_path,
                bucket,
                common,
                param,
            } => {
                tracing::info!("Downloading program from s3://{}/{}...", bucket, s3_path);
                let (elf, stdin) = Self::download_from_s3(bucket, s3_path, param).await?;
                tracing::info!("Running S3 program {:?} benchmark...", s3_path);
                let proof_ids = Self::run_benchmark(elf, stdin, common).await?;

                // Write all proof IDs to CSV
                for (proof_id, duration) in proof_ids {
                    if let Err(e) =
                        Self::append_to_csv(&proof_id, s3_path, Some(param.as_str()), duration)
                    {
                        tracing::warn!("Failed to write to CSV: {}", e);
                    }
                }
            }
            BenchCommand::ExecuteS3 {
                s3_path,
                param,
                common,
            } => {
                tracing::info!(
                    "Downloading program from s3://sp1-testing-suite/{}...",
                    s3_path
                );
                let (elf, stdin) =
                    Self::download_from_s3("sp1-testing-suite", s3_path, param).await?;
                tracing::info!("Running execute-only benchmark for {:?}...", s3_path);
                Self::run_execute_benchmark(elf, stdin, common).await?;
            }
        }
        Ok(())
    }

    /// Runs a benchmark for a given elf and stdin, returning the proof ids and elapsed times.
    ///
    /// TODO: Expensive clone + reuploading of elf and stdin for when running for multiple repetitions.
    async fn run_benchmark(
        elf: Vec<u8>,
        stdin: SP1Stdin,
        common: &CommonArgs,
    ) -> Result<Vec<(String, Duration)>> {
        let client = SP1LightNode::new().await;

        let vk = client.setup(&elf).await.expect("failed to setup elf");

        let mut proof_ids = Vec::with_capacity(common.count as usize);
        for _ in 0..common.count {
            let cluster_elf = ClusterElf::NewElf(elf.clone());

            let ProofRequestResults {
                proof_id,
                proof,
                elapsed,
            } = request_proof_from_env(common.mode, 4, cluster_elf, stdin.clone()).await?;

            // Verify proof to CSV
            client
                .verify(&vk, &proof.proof)
                .expect("failed to verify proof");

            tracing::info!("Proof completed in {:?}", elapsed);

            proof_ids.push((proof_id, elapsed));
        }
        Ok(proof_ids)
    }

    async fn download_from_s3(
        bucket: &str,
        s3_path: &str,
        param: &str,
    ) -> Result<(Vec<u8>, SP1Stdin)> {
        // Download program.bin from S3
        let program_output = std::process::Command::new("aws")
            .args([
                "s3",
                "cp",
                &format!("s3://{}/{}/program.bin", bucket, s3_path),
                "program.bin",
            ])
            .output()?;

        if !program_output.status.success() {
            return Err(eyre::eyre!(
                "Failed to download program.bin: {}",
                String::from_utf8_lossy(&program_output.stderr)
            ));
        }

        // Download stdin.bin from S3
        let stdin_output = if param.is_empty() {
            std::process::Command::new("aws")
                .args([
                    "s3",
                    "cp",
                    &format!("s3://{}/{}/stdin.bin", bucket, s3_path),
                    "stdin.bin",
                ])
                .output()?
        } else {
            std::process::Command::new("aws")
                .args([
                    "s3",
                    "cp",
                    &format!("s3://{}/{}/input/{}.bin", bucket, s3_path, param),
                    "stdin.bin",
                ])
                .output()?
        };

        if !stdin_output.status.success() {
            return Err(eyre::eyre!(
                "Failed to download stdin.bin: {}",
                String::from_utf8_lossy(&stdin_output.stderr)
            ));
        }

        // Read the downloaded files
        let program = std::fs::read("program.bin")?;
        let stdin_bytes = std::fs::read("stdin.bin")?;
        let stdin: SP1Stdin = bincode::deserialize(&stdin_bytes)?;

        // Clean up the downloaded files
        std::fs::remove_file("program.bin").ok();
        std::fs::remove_file("stdin.bin").ok();

        Ok((program, stdin))
    }

    fn append_to_csv(
        proof_id: &str,
        s3_path: &str,
        param: Option<&str>,
        duration: Duration,
    ) -> Result<()> {
        let csv_path = "data/bench_results.csv";

        // Create data directory if it doesn't exist
        if let Some(parent) = Path::new(csv_path).parent() {
            std::fs::create_dir_all(parent)?;
        }

        let file_exists = Path::new(csv_path).exists();

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(csv_path)?;

        // Write header if file is new
        if !file_exists {
            writeln!(file, "proof_id,s3_path,param,duration_ms")?;
        }

        // Write data row
        writeln!(
            file,
            "{},{},{},{}",
            proof_id,
            s3_path,
            param.unwrap_or(""),
            duration.as_millis()
        )?;

        Ok(())
    }

    /// Runs an execute-only benchmark, connecting directly to the coordinator.
    /// Supports concurrent requests via common.count.
    async fn run_execute_benchmark(
        elf: Vec<u8>,
        stdin: SP1Stdin,
        common: &CommonArgs,
    ) -> Result<()> {
        let redis_nodes = common
            .redis_nodes
            .as_ref()
            .ok_or_else(|| eyre::eyre!("CLI_REDIS_NODES is required for execute-s3"))?;

        // Create artifact client
        let nodes: Vec<String> = redis_nodes.split(',').map(|s| s.to_string()).collect();
        let artifact_client = RedisArtifactClient::new(nodes.clone(), 10);

        // Upload artifacts once (shared across all concurrent requests)
        tracing::info!("Uploading artifacts...");
        let elf_artifact = artifact_client
            .create_artifact()
            .map_err(|e| eyre::eyre!("Failed to create elf artifact: {}", e))?;
        artifact_client
            .upload_with_type(&elf_artifact, ArtifactType::Program, elf)
            .await
            .map_err(|e| eyre::eyre!("Failed to upload program: {}", e))?;

        let stdin_artifact = artifact_client
            .create_artifact()
            .map_err(|e| eyre::eyre!("Failed to create stdin artifact: {}", e))?;
        artifact_client
            .upload_with_type(&stdin_artifact, ArtifactType::Stdin, stdin)
            .await
            .map_err(|e| eyre::eyre!("Failed to upload stdin: {}", e))?;

        let count = common.count;
        let cluster_rpc = common.cluster_rpc.clone();

        tracing::info!(
            "Submitting {} concurrent execute-only requests to {}...",
            count,
            cluster_rpc
        );

        // Spawn concurrent tasks
        let mut handles = Vec::with_capacity(count as usize);
        let start = Instant::now();

        for i in 0..count {
            let cluster_rpc = cluster_rpc.clone();
            let elf_artifact = elf_artifact.clone();
            let stdin_artifact = stdin_artifact.clone();

            let handle = tokio::spawn(async move {
                Self::run_single_execute_request(i, &cluster_rpc, elf_artifact, stdin_artifact)
                    .await
            });
            handles.push(handle);
        }

        // Wait for all to complete and collect results
        let mut success_count = 0;
        let mut fail_count = 0;

        for (i, handle) in handles.into_iter().enumerate() {
            match handle.await {
                Ok(Ok(duration)) => {
                    tracing::info!("Request {} completed in {:?}", i, duration);
                    success_count += 1;
                }
                Ok(Err(e)) => {
                    tracing::error!("Request {} failed: {}", i, e);
                    fail_count += 1;
                }
                Err(e) => {
                    tracing::error!("Request {} panicked: {}", i, e);
                    fail_count += 1;
                }
            }
        }

        let total_elapsed = start.elapsed();
        tracing::info!(
            "All {} requests completed in {:?} ({} succeeded, {} failed)",
            count,
            total_elapsed,
            success_count,
            fail_count
        );

        if fail_count > 0 {
            Err(eyre::eyre!("{} requests failed", fail_count))
        } else {
            Ok(())
        }
    }

    /// Runs a single execute-only request.
    async fn run_single_execute_request(
        index: u32,
        cluster_rpc: &str,
        elf_artifact: Artifact,
        stdin_artifact: Artifact,
    ) -> Result<Duration> {
        // Connect to coordinator
        let worker_id = format!(
            "cli-bench-{}-{}",
            index,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis()
        );

        let client = WorkerServiceClient::new(cluster_rpc.to_string(), worker_id.clone())
            .await
            .map_err(|e| eyre::eyre!("Failed to connect to coordinator: {}", e))?;

        // Create proof ID
        let proof_id = format!(
            "cli_exec_{}_{}",
            index,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis()
        );

        tracing::info!("Request {} created proof_id: {}", index, proof_id);

        // Create dummy proof entry
        let deadline = SystemTime::now() + Duration::from_secs(4 * 60 * 60);
        client
            .client
            .clone()
            .create_dummy_proof(CreateDummyProofRequest {
                worker_id: worker_id.clone(),
                proof_id: proof_id.clone(),
                requester: "cli".to_string(),
                expires_at: deadline.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
            })
            .await
            .map_err(|e| eyre::eyre!("Failed to create dummy proof: {}", e))?;

        // Create task subscriber BEFORE submitting the task
        let subscriber = client
            .subscriber(ProofId::new(proof_id.clone()))
            .await
            .map_err(|e| eyre::eyre!("Failed to create subscriber: {}", e))?
            .per_task();

        let start = Instant::now();

        // Submit ExecuteOnly task
        let task_id = subscriber
            .client()
            .submit_task(
                TaskType::ExecuteOnly,
                RawTaskRequest {
                    inputs: vec![elf_artifact, stdin_artifact],
                    outputs: vec![],
                    context: TaskContext {
                        proof_id: ProofId::new(proof_id.clone()),
                        parent_id: None,
                        parent_context: None,
                        requester_id: RequesterId::new("cli".to_string()),
                    },
                },
            )
            .await
            .map_err(|e| eyre::eyre!("Failed to submit task: {}", e))?;

        // Wait for task completion
        let status = subscriber
            .wait_task(task_id)
            .await
            .map_err(|e| eyre::eyre!("Task wait failed: {}", e))?;

        let elapsed = start.elapsed();
        match status {
            TaskStatus::Succeeded => Ok(elapsed),
            TaskStatus::FailedFatal | TaskStatus::FailedRetryable => {
                Err(eyre::eyre!("Task failed with status: {:?}", status))
            }
            _ => Err(eyre::eyre!("Unexpected task status: {:?}", status)),
        }
    }
}
