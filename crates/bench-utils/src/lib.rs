use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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
use sp1_prover_types::Artifact;
use sp1_sdk::{network::proto::types::ProofMode, ProofFromNetwork, SP1Stdin};

pub struct BenchmarkConfig {
    pub cluster_rpc: String,
    pub count: u32,
    pub mode: ProofMode,
    pub timeout_hours: u64,
    pub artifact_store: ArtifactStoreConfig,
}

pub struct BenchmarkResults {
    pub proof_ids: Vec<String>,
    pub proofs: Vec<ProofFromNetwork>,
}

pub enum ArtifactStoreConfig {
    Redis { nodes: Vec<String> },
    S3 { bucket: String, region: String },
}

pub enum ClusterElf {
    NewElf(Vec<u8>),
    ExistingElf(Artifact),
}

pub const ELF_ID_PATH: &str = "ELF_ID";

pub async fn run_benchmark<A: ArtifactClient>(
    artifact_client: A,
    elf: ClusterElf,
    stdin: SP1Stdin,
    config: &BenchmarkConfig,
    cycles_estimate: Option<u64>,
) -> Result<BenchmarkResults> {
    let client = ClusterServiceClient::new(config.cluster_rpc.clone()).await?;

    let (elf_id, stdin_id, proof_output_ids) =
        setup_artifacts(artifact_client.clone(), elf, stdin, config.count).await?;

    let base_id = format!(
        "cli_{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );
    tracing::info!("base_id: {}", base_id);

    // Worst case timeout is 4 hours per request.
    let deadline = SystemTime::now() + Duration::from_secs(config.timeout_hours * 60 * 60);
    let mut start_time = Instant::now();
    let mut proof_ids = Vec::new();
    let mut proofs = Vec::new();

    // Synchronously create each proof request and wait for it to complete
    for i in 0..config.count {
        let proof_id = format!("{}_{}", base_id, i);

        // Create the proof request
        client
            .create_proof_request(sp1_cluster_common::proto::ProofRequestCreateRequest {
                proof_id: proof_id.clone(),
                program_artifact_id: elf_id.clone().to_id(),
                stdin_artifact_id: stdin_id.clone().to_id(),
                options_artifact_id: Some((config.mode as i32).to_string()),
                proof_artifact_id: Some(proof_output_ids[i as usize].clone().to_id()),
                requester: vec![],
                deadline: deadline.duration_since(UNIX_EPOCH).unwrap().as_secs(),
                cycle_limit: 0,
                gas_limit: 0,
            })
            .await?;

        start_time = Instant::now();
        tracing::info!("Created proof request {} ({})", i, proof_id);

        // Poll until this specific proof request is completed
        loop {
            if deadline < SystemTime::now() {
                return Err(eyre::eyre!(
                    "Timeout exceeded for proof request {} after {:?}",
                    i,
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
                    "Proof request {} not found after {:?}",
                    i,
                    start_time.elapsed()
                ));
            };

            match proof_request.proof_status() {
                ProofRequestStatus::Completed => {
                    tracing::info!(
                        "Proof request {} completed after {:?}",
                        i,
                        start_time.elapsed()
                    );

                    proof_ids.push(proof_id.clone());
                    let proof = artifact_client
                        .download_with_type(&proof_output_ids[i as usize], ArtifactType::Proof)
                        .await
                        .expect("failed to download proof");
                    proofs.push(proof);
                    break;
                }
                ProofRequestStatus::Failed | ProofRequestStatus::Cancelled => {
                    return Err(eyre::eyre!(
                        "Proof request {} {:?} after {:?}",
                        i,
                        proof_request.proof_status(),
                        start_time.elapsed()
                    ));
                }
                _ => {}
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    tracing::info!("Completed after {:?}", start_time.elapsed());
    if let Some(cycles_estimate) = cycles_estimate {
        tracing::info!(
            "Total Cycles: {:.2} | Aggregate MHz: {:.2}",
            cycles_estimate * config.count as u64,
            cycles_estimate as f64 * config.count as f64
                / start_time.elapsed().as_secs_f64()
                / 1_000_000.0
        );
    }

    let result = BenchmarkResults { proof_ids, proofs };
    Ok(result)
}

async fn setup_artifacts<A: ArtifactClient>(
    artifact_client: A,
    elf: ClusterElf,
    stdin: SP1Stdin,
    count: u32,
) -> Result<(Artifact, Artifact, Vec<Artifact>)> {
    let elf_id = match elf {
        ClusterElf::NewElf(elf) => {
            let elf_id = artifact_client.create_artifact().unwrap();
            artifact_client
                .upload_with_type(&elf_id, ArtifactType::Program, elf)
                .await
                .map_err(|e| eyre::eyre!(e))?;
            elf_id
        }
        ClusterElf::ExistingElf(elf_id) => elf_id,
    };
    let stdin_id = artifact_client.create_artifact().unwrap();
    artifact_client
        .upload_with_type(&stdin_id, ArtifactType::Stdin, stdin)
        .await
        .map_err(|e| eyre::eyre!(e))?;
    let proof_output_ids = (0..count)
        .map(|_| artifact_client.create_artifact().unwrap())
        .collect();

    // Save the elf id to ELF_ID file in the cargo manifest directory
    let elf_id_path = std::env::var("CARGO_MANIFEST_DIR")
        .map(|dir| format!("{}/{}", dir, ELF_ID_PATH))
        .unwrap_or_else(|_| ELF_ID_PATH.to_string());
    std::fs::write(&elf_id_path, elf_id.clone().to_id())?;
    Ok((elf_id, stdin_id, proof_output_ids))
}

/// Public API to run a benchmark with automatic artifact client setup
pub async fn run_benchmark_with_config(
    elf: ClusterElf,
    stdin: SP1Stdin,
    config: &BenchmarkConfig,
    cycles_estimate: Option<u64>,
) -> Result<BenchmarkResults> {
    match &config.artifact_store {
        ArtifactStoreConfig::Redis { nodes } => {
            tracing::info!("using redis artifact store");
            let artifact_client = RedisArtifactClient::new(nodes.clone(), 16);
            run_benchmark(artifact_client, elf, stdin, config, cycles_estimate).await
        }
        ArtifactStoreConfig::S3 { bucket, region } => {
            tracing::info!("using s3 artifact store");
            let artifact_client = S3ArtifactClient::new(
                region.clone(),
                bucket.clone(),
                32,
                S3DownloadMode::AwsSDK(
                    S3ArtifactClient::create_s3_sdk_download_client(region.clone()).await,
                ),
            )
            .await;
            run_benchmark(artifact_client, elf, stdin, config, cycles_estimate).await
        }
    }
}

/// Get the benchmark config from env
pub fn benchmark_config_from_env(proof_mode: ProofMode, timeout_hours: u64) -> BenchmarkConfig {
    let cluster_rpc = std::env::var("CLI_CLUSTER_RPC").unwrap();

    let redis_nodes = std::env::var("CLI_REDIS_NODES");
    let s3_bucket = std::env::var("CLI_S3_BUCKET");
    let s3_region = std::env::var("CLI_S3_REGION");

    let artifact_store_config = match (redis_nodes, s3_bucket) {
        (Ok(redis_nodes), Err(_)) => ArtifactStoreConfig::Redis {
            nodes: redis_nodes
                .clone()
                .split(',')
                .map(|s| s.to_string())
                .collect(),
        },
        (Err(_), Ok(s3_bucket)) => ArtifactStoreConfig::S3 {
            bucket: s3_bucket.clone(),
            region: s3_region.unwrap().clone(),
        },
        _ => {
            panic!("Exactly one of Redis nodes or S3 bucket must be specified");
        }
    };

    BenchmarkConfig {
        cluster_rpc,
        count: 1,
        mode: proof_mode,
        timeout_hours: timeout_hours,
        artifact_store: artifact_store_config,
    }
}

pub async fn run_benchmark_from_env(
    mode: ProofMode,
    timeout_hours: u64,
    elf: ClusterElf,
    stdin: SP1Stdin,
    cycles_estimate: Option<u64>,
) -> Result<BenchmarkResults> {
    let config = benchmark_config_from_env(mode, timeout_hours);
    run_benchmark_with_config(elf, stdin, &config, cycles_estimate).await
}
