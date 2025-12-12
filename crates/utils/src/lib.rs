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

pub struct ProofRequestConfig {
    /// The RPC Url to connect to.
    pub cluster_rpc: String,
    /// The proof mode to use.
    pub mode: ProofMode,
    /// The timeout for each proof request in hours.
    pub timeout_hours: u64,
    /// The artifact store to use.
    pub artifact_store: ArtifactStoreConfig,
}

pub struct ProofRequestResults {
    /// The proof id for the requested proof.
    pub proof_id: String,
    /// The proof returned from the cluster.
    pub proof: ProofFromNetwork,
    /// The elapsed time for the proof request.
    pub elapsed: Duration,
}

/// The output of a proof request.
#[derive(Clone)]
pub struct ProofRequest {
    pub proof_id: String,
    pub proof_output_id: Artifact,
    pub deadline: SystemTime,
    pub start_time: Instant,
}

pub enum ArtifactStoreConfig {
    Redis { nodes: Vec<String> },
    S3 { bucket: String, region: String },
}

pub enum ClusterElf {
    NewElf(Vec<u8>),
    ExistingElf(Artifact),
}

/// Creates a proof request and returns the proof id, deadline, and start time.
pub async fn create_request<A: ArtifactClient>(
    artifact_client: A,
    elf: ClusterElf,
    stdin: SP1Stdin,
    config: &ProofRequestConfig,
) -> Result<ProofRequest> {
    let client = ClusterServiceClient::new(config.cluster_rpc.clone()).await?;

    let (elf_id, stdin_id, proof_output_id) =
        setup_artifacts(artifact_client.clone(), elf, stdin).await?;

    let base_id = format!(
        "cli_{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );

    let deadline = SystemTime::now() + Duration::from_secs(config.timeout_hours * 60 * 60);
    let proof_id = format!("{}", base_id);

    // Create the proof request.
    client
        .create_proof_request(sp1_cluster_common::proto::ProofRequestCreateRequest {
            proof_id: proof_id.clone(),
            program_artifact_id: elf_id.clone().to_id(),
            stdin_artifact_id: stdin_id.clone().to_id(),
            options_artifact_id: Some((config.mode as i32).to_string()),
            proof_artifact_id: Some(proof_output_id.clone().to_id()),
            requester: vec![],
            deadline: deadline.duration_since(UNIX_EPOCH).unwrap().as_secs(),
            cycle_limit: 0,
            gas_limit: 0,
        })
        .await?;

    let start_time: Instant = Instant::now();
    tracing::info!("Successfully created proof request {}", proof_id);
    Ok(ProofRequest {
        proof_id,
        proof_output_id,
        deadline,
        start_time,
    })
}

/// Checks the status of a proof request and returns the ProofRequestResult if it is completed.
pub async fn check_proof_status<A: ArtifactClient>(
    artifact_client: A,
    proof_request: ProofRequest,
    client: &ClusterServiceClient,
) -> Result<Option<ProofRequestResults>> {
    let ProofRequest {
        proof_id,
        proof_output_id,
        deadline,
        start_time,
    } = proof_request;
    let proof;
    if deadline < SystemTime::now() {
        return Err(eyre::eyre!(
            "Timeout exceeded for proof request after {:?}",
            start_time.elapsed()
        ));
    }

    let resp = client
        .get_proof_request(proto::ProofRequestGetRequest {
            proof_id: proof_id.to_string(),
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

            let completed_proof = artifact_client
                .download_with_type(&proof_output_id, ArtifactType::Proof)
                .await
                .expect("failed to download proof");
            proof = Some(completed_proof);
        }
        ProofRequestStatus::Failed | ProofRequestStatus::Cancelled => {
            return Err(eyre::eyre!(
                "Proof request {:?} after {:?}",
                proof_request.proof_status(),
                start_time.elapsed()
            ));
        }
        _ => proof = None,
    }

    let elapsed = start_time.elapsed();

    match proof {
        Some(proof) => {
            tracing::info!("Completed after {:?}", elapsed);
            let result = ProofRequestResults {
                proof_id: proof_id.to_string(),
                proof,
                elapsed,
            };
            Ok(Some(result))
        }
        None => Ok(None),
    }
}

/// Sets up the elf, stdin, and proof output artifacts.
async fn setup_artifacts<A: ArtifactClient>(
    artifact_client: A,
    elf: ClusterElf,
    stdin: SP1Stdin,
) -> Result<(Artifact, Artifact, Artifact)> {
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
    let proof_output_id = artifact_client.create_artifact().unwrap();

    // TODO: what to do with this
    // // Save the elf id to ELF_ID file in the cargo manifest directory
    // let elf_id_path = std::env::var("CARGO_MANIFEST_DIR")
    //     .map(|dir| format!("{}/{}", dir, ELF_ID_PATH))
    //     .unwrap_or_else(|_| ELF_ID_PATH.to_string());
    // std::fs::write(&elf_id_path, elf_id.clone().to_id())?;
    Ok((elf_id, stdin_id, proof_output_id))
}

pub async fn request_proof<A: ArtifactClient>(
    artifact_client: A,
    elf: ClusterElf,
    stdin: SP1Stdin,
    config: &ProofRequestConfig,
) -> Result<ProofRequestResults> {
    let proof_request = create_request(artifact_client.clone(), elf, stdin, config).await?;
    let mut proof_request_result = None;
    let client = ClusterServiceClient::new(config.cluster_rpc.clone()).await?;
    while proof_request_result.is_none() {
        proof_request_result =
            check_proof_status(artifact_client.clone(), proof_request.clone(), &client).await?;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Ok(proof_request_result.unwrap())
}

/// Request a proof from the cluster. Waits for the proof to complete.
pub async fn request_proof_with_config(
    elf: ClusterElf,
    stdin: SP1Stdin,
    config: &ProofRequestConfig,
) -> Result<ProofRequestResults> {
    match &config.artifact_store {
        ArtifactStoreConfig::Redis { nodes } => {
            tracing::info!("using redis artifact store");
            let artifact_client = RedisArtifactClient::new(nodes.clone(), 16);
            request_proof(artifact_client, elf, stdin, config).await
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
            request_proof(artifact_client, elf, stdin, config).await
        }
    }
}

/// Get the request config from env.
pub fn request_config_from_env(proof_mode: ProofMode, timeout_hours: u64) -> ProofRequestConfig {
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

    ProofRequestConfig {
        cluster_rpc,
        mode: proof_mode,
        timeout_hours: timeout_hours,
        artifact_store: artifact_store_config,
    }
}

pub async fn request_proof_from_env(
    mode: ProofMode,
    timeout_hours: u64,
    elf: ClusterElf,
    stdin: SP1Stdin,
) -> Result<ProofRequestResults> {
    let config = request_config_from_env(mode, timeout_hours);
    request_proof_with_config(elf, stdin, &config).await
}
