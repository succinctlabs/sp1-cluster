use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

use clap::{Args, Subcommand};
use eyre::Result;
use sp1_cluster_artifact::{
    redis::RedisArtifactClient,
    s3::{S3ArtifactClient, S3DownloadMode},
    ArtifactClient, ArtifactType,
};
use sp1_cluster_common::client::ClusterServiceClient;
use sp1_cluster_utils::{
    check_proof_status, create_request, request_config_from_env, request_proof_from_env,
    ClusterElf, ProofRequest, ProofRequestResults,
};
use sp1_sdk::{network::proto::types::ProofMode, CpuProver, Elf, Prover, ProvingKey, SP1Stdin};

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
    S3Batch {
        /// S3 path to the program (e.g., "path/to/program" for s3://bucket/path/to/program/)
        s3_path: String,
        /// Number of concurrent proof requests
        #[arg(long, default_value_t = 4)]
        batch_size: usize,
        /// S3 bucket to download from
        #[arg(long, env = "CLI_BENCH_S3_BUCKET", default_value = "sp1-testing-suite")]
        bucket: String,
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
            BenchCommand::S3Batch {
                s3_path,
                batch_size,
                bucket,
                common,
            } => {
                tracing::info!(
                    "Running S3 batch benchmark from s3://{}/{} with batch_size={}...",
                    bucket,
                    s3_path,
                    batch_size
                );

                // List all available params
                let params = Self::list_s3_params(bucket, s3_path).await?;
                if params.is_empty() {
                    return Err(eyre::eyre!(
                        "No params found in s3://{}/{}/input/",
                        bucket,
                        s3_path
                    ));
                }
                tracing::info!("Found {} params to benchmark: {:?}", params.len(), params);

                // Run batch benchmark
                let results =
                    Self::run_batch_benchmark(bucket, s3_path, params, *batch_size, common).await?;

                // Write all proof IDs to CSV
                for (proof_id, param, duration) in results {
                    if let Err(e) = Self::append_to_csv(&proof_id, s3_path, Some(&param), duration)
                    {
                        tracing::warn!("Failed to write to CSV: {}", e);
                    }
                }
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
        let elf_arc = Elf::from(elf.clone());
        let mut handle = Some(tokio::spawn(async move {
            let client = CpuProver::new_experimental().await;
            let pk = client.setup(elf_arc).await.expect("failed to setup elf");
            (client, pk)
        }));
        let mut client_pk = None;

        let mut proof_ids = Vec::with_capacity(common.count as usize);
        for _ in 0..common.count {
            let cluster_elf = ClusterElf::NewElf(elf.clone());

            let ProofRequestResults {
                proof_id,
                proof,
                elapsed,
            } = request_proof_from_env(common.mode, 4, cluster_elf, stdin.clone()).await?;

            // Verify proof to CSV
            if let Some(handle) = handle.take() {
                let res = handle.await.expect("failed to get client");
                client_pk = Some(res);
            }
            let (client, pk) = client_pk.as_ref().unwrap();
            client
                .verify(&proof.into(), pk.verifying_key(), None)
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
        let program = Self::download_program_from_s3(bucket, s3_path).await?;
        let stdin = if param.is_empty() {
            Self::download_default_stdin_from_s3(bucket, s3_path).await?
        } else {
            Self::download_stdin_from_s3(bucket, s3_path, param).await?
        };
        Ok((program, stdin))
    }

    /// Downloads the default stdin.bin from S3.
    async fn download_default_stdin_from_s3(bucket: &str, s3_path: &str) -> Result<SP1Stdin> {
        let output = std::process::Command::new("aws")
            .args([
                "s3",
                "cp",
                &format!("s3://{}/{}/stdin.bin", bucket, s3_path),
                "stdin.bin",
            ])
            .output()?;

        if !output.status.success() {
            return Err(eyre::eyre!(
                "Failed to download stdin.bin: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }

        let stdin_bytes = std::fs::read("stdin.bin")?;
        let stdin: SP1Stdin = bincode::deserialize(&stdin_bytes)?;
        std::fs::remove_file("stdin.bin").ok();
        Ok(stdin)
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

    /// Lists all available params from the S3 input directory.
    async fn list_s3_params(bucket: &str, s3_path: &str) -> Result<Vec<String>> {
        let s3_url = format!("s3://{}/{}/input/", bucket, s3_path);
        tracing::info!("Listing params from: {}", s3_url);
        let output = std::process::Command::new("aws")
            .args(["s3", "ls", &s3_url])
            .output()?;

        if !output.status.success() {
            return Err(eyre::eyre!(
                "Failed to list S3 input directory {}: {}",
                s3_url,
                String::from_utf8_lossy(&output.stderr)
            ));
        }

        // Parse output: each line is like "2024-01-01 12:00:00  12345 param_name.bin"
        let stdout = String::from_utf8_lossy(&output.stdout);
        tracing::debug!("Raw S3 ls output:\n{}", stdout);
        let params: Vec<String> = stdout
            .lines()
            .filter_map(|line| {
                let filename = line.split_whitespace().last()?;
                if filename.ends_with(".bin") {
                    Some(filename.trim_end_matches(".bin").to_string())
                } else {
                    None
                }
            })
            .collect();

        tracing::info!("Found {} params in S3 input directory", params.len());
        if !params.is_empty() {
            tracing::debug!("First param: {:?}, last param: {:?}", params.first(), params.last());
        }
        Ok(params)
    }

    /// Downloads only the program.bin from S3.
    async fn download_program_from_s3(bucket: &str, s3_path: &str) -> Result<Vec<u8>> {
        let output = std::process::Command::new("aws")
            .args([
                "s3",
                "cp",
                &format!("s3://{}/{}/program.bin", bucket, s3_path),
                "program.bin",
            ])
            .output()?;

        if !output.status.success() {
            return Err(eyre::eyre!(
                "Failed to download program.bin: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }

        let program = std::fs::read("program.bin")?;
        std::fs::remove_file("program.bin").ok();
        Ok(program)
    }

    /// Downloads stdin for a specific param from S3.
    async fn download_stdin_from_s3(bucket: &str, s3_path: &str, param: &str) -> Result<SP1Stdin> {
        // Use unique temp filename to avoid conflicts with concurrent downloads
        let temp_file = format!("stdin_{}.bin", param);
        let s3_url = format!("s3://{}/{}/input/{}.bin", bucket, s3_path, param);
        tracing::debug!("Downloading stdin from: {}", s3_url);
        let output = std::process::Command::new("aws")
            .args(["s3", "cp", &s3_url, &temp_file])
            .output()?;

        if !output.status.success() {
            return Err(eyre::eyre!(
                "Failed to download stdin from {}: {}",
                s3_url,
                String::from_utf8_lossy(&output.stderr)
            ));
        }

        let stdin_bytes = std::fs::read(&temp_file)?;
        let stdin: SP1Stdin = bincode::deserialize(&stdin_bytes)?;
        std::fs::remove_file(&temp_file).ok();
        Ok(stdin)
    }

    /// Runs batch benchmark with concurrent proof requests.
    async fn run_batch_benchmark(
        bucket: &str,
        s3_path: &str,
        params: Vec<String>,
        batch_size: usize,
        common: &CommonArgs,
    ) -> Result<Vec<(String, String, Duration)>> {
        // Download program once
        let program = Self::download_program_from_s3(bucket, s3_path).await?;
        tracing::info!("Downloaded program ({} bytes)", program.len());

        // Get config
        let config = request_config_from_env(common.mode, 4);

        // Create artifact client and cluster client based on config
        let cluster_client = ClusterServiceClient::new(config.cluster_rpc.clone()).await?;

        // Upload ELF once and setup verification in background
        let elf_arc = Elf::from(program.clone());
        let verify_handle = tokio::spawn(async move {
            let client = CpuProver::new_experimental().await;
            let pk = client.setup(elf_arc).await.expect("failed to setup elf");
            (client, pk)
        });

        // Create artifact client based on config
        match &config.artifact_store {
            sp1_cluster_utils::ArtifactStoreConfig::Redis { nodes } => {
                let artifact_client = RedisArtifactClient::new(nodes.clone(), 16);
                let results = Self::run_batch_benchmark_with_client(
                    artifact_client,
                    &cluster_client,
                    &config,
                    program,
                    params,
                    batch_size,
                    bucket,
                    s3_path,
                )
                .await?;

                // Verify all proofs
                let (verifier, pk) = verify_handle.await?;
                for (_proof_id, param, proof, _elapsed) in &results {
                    verifier
                        .verify(&proof.clone().into(), pk.verifying_key(), None)
                        .expect(&format!("failed to verify proof for param {}", param));
                }

                Ok(results
                    .into_iter()
                    .map(|(id, param, _, elapsed)| (id, param, elapsed))
                    .collect())
            }
            sp1_cluster_utils::ArtifactStoreConfig::S3 {
                bucket: artifact_bucket,
                region,
            } => {
                let artifact_client = S3ArtifactClient::new(
                    region.clone(),
                    artifact_bucket.clone(),
                    32,
                    S3DownloadMode::AwsSDK(
                        S3ArtifactClient::create_s3_sdk_download_client(region.clone()).await,
                    ),
                )
                .await;
                let results = Self::run_batch_benchmark_with_client(
                    artifact_client,
                    &cluster_client,
                    &config,
                    program,
                    params,
                    batch_size,
                    bucket,
                    s3_path,
                )
                .await?;

                // Verify all proofs
                let (verifier, pk) = verify_handle.await?;
                for (_proof_id, param, proof, _elapsed) in &results {
                    verifier
                        .verify(&proof.clone().into(), pk.verifying_key(), None)
                        .expect(&format!("failed to verify proof for param {}", param));
                }

                Ok(results
                    .into_iter()
                    .map(|(id, param, _, elapsed)| (id, param, elapsed))
                    .collect())
            }
        }
    }

    /// Inner function that runs batch benchmark with a specific artifact client.
    async fn run_batch_benchmark_with_client<A: ArtifactClient + Clone>(
        artifact_client: A,
        cluster_client: &ClusterServiceClient,
        config: &sp1_cluster_utils::ProofRequestConfig,
        program: Vec<u8>,
        params: Vec<String>,
        batch_size: usize,
        bucket: &str,
        s3_path: &str,
    ) -> Result<Vec<(String, String, sp1_sdk::ProofFromNetwork, Duration)>> {
        // Upload ELF once
        let elf_artifact = artifact_client
            .create_artifact()
            .map_err(|e| eyre::eyre!(e))?;
        artifact_client
            .upload_with_type(&elf_artifact, ArtifactType::Program, program)
            .await
            .map_err(|e| eyre::eyre!(e))?;
        tracing::info!("Uploaded ELF artifact: {:?}", elf_artifact.clone().to_id());

        let mut results = Vec::new();
        let mut pending_requests: Vec<(String, ProofRequest)> = Vec::new();
        let mut params_iter = params.into_iter();

        loop {
            // Submit new requests up to batch_size
            while pending_requests.len() < batch_size {
                if let Some(param) = params_iter.next() {
                    let stdin = Self::download_stdin_from_s3(bucket, s3_path, &param).await?;
                    let elf = ClusterElf::ExistingElf(elf_artifact.clone());
                    let proof_request =
                        create_request(artifact_client.clone(), elf, stdin, config).await?;
                    tracing::info!("Submitted proof request for param: {}", param);
                    pending_requests.push((param, proof_request));
                } else {
                    break;
                }
            }

            if pending_requests.is_empty() {
                break;
            }

            // Poll all pending requests
            let mut still_pending = Vec::new();
            for (param, request) in pending_requests {
                match check_proof_status(artifact_client.clone(), request.clone(), cluster_client)
                    .await?
                {
                    Some(result) => {
                        tracing::info!(
                            "Proof for {} completed in {:?} (proof_id: {})",
                            param,
                            result.elapsed,
                            result.proof_id
                        );
                        results.push((result.proof_id, param, result.proof, result.elapsed));
                    }
                    None => {
                        still_pending.push((param, request));
                    }
                }
            }
            pending_requests = still_pending;

            if !pending_requests.is_empty() {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }

        Ok(results)
    }
}
