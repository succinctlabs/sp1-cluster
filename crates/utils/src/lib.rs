#![recursion_limit = "256"]

use std::num::NonZeroU16;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use eyre::Result;
use sp1_cluster_artifact::{
    redis::RedisArtifactClient,
    s3::{S3ArtifactClient, S3DownloadMode},
    ArtifactClient, ArtifactType,
};
use sp1_cluster_common::{
    client::ClusterServiceClient,
    proto::{self, ProofRequestCancelRequest, ProofRequestStatus},
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

/// `ClusterServiceClient::new` retries a refused connection forever, which leaves the CLI logging
/// warnings with nothing to show for it.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

async fn connect(cluster_rpc: &str) -> Result<ClusterServiceClient> {
    tokio::time::timeout(
        CONNECT_TIMEOUT,
        ClusterServiceClient::new(cluster_rpc.to_string()),
    )
    .await
    .map_err(|_| {
        eyre::eyre!(
            "no cluster at {} after {:?}: start the cluster, or set CLI_CLUSTER_RPC to its API",
            cluster_rpc,
            CONNECT_TIMEOUT
        )
    })?
}

/// Creates a proof request and returns the proof id, deadline, and start time.
pub async fn create_request<A: ArtifactClient>(
    artifact_client: A,
    elf: ClusterElf,
    stdin: SP1Stdin,
    config: &ProofRequestConfig,
) -> Result<ProofRequest> {
    let client = connect(&config.cluster_rpc).await?;
    let mut requests = create_requests(
        &client,
        artifact_client,
        elf,
        stdin,
        NonZeroU16::MIN,
        config,
    )
    .await?;
    Ok(requests.pop().expect("one request created"))
}

async fn create_requests<A: ArtifactClient>(
    client: &ClusterServiceClient,
    artifact_client: A,
    elf: ClusterElf,
    stdin: SP1Stdin,
    count: NonZeroU16,
    config: &ProofRequestConfig,
) -> Result<Vec<ProofRequest>> {
    let (elf_id, stdin_id) = setup_artifacts(artifact_client.clone(), elf, stdin).await?;

    let base_id = format!(
        "cli_{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );

    let deadline = SystemTime::now() + Duration::from_secs(config.timeout_hours * 60 * 60);

    let mut requests = Vec::with_capacity(usize::from(count.get()));
    for i in 0..count.get() {
        let proof_id = format!("{base_id}_{i}");
        let proof_output_id = match artifact_client.create_artifact() {
            Ok(proof_output_id) => proof_output_id,
            Err(error) => {
                return Err(error_after_cancelling(client, &requests, eyre::eyre!(error)).await)
            }
        };

        if let Err(error) = client
            .create_proof_request(sp1_cluster_common::proto::ProofRequestCreateRequest {
                proof_id: proof_id.clone(),
                program_artifact_id: elf_id.clone().to_id(),
                stdin_artifact_id: stdin_id.clone().to_id(),
                options_artifact_id: Some((config.mode as i32).to_string()),
                proof_artifact_id: Some(proof_output_id.clone().to_id()),
                requester: vec![],
                deadline: deadline.duration_since(UNIX_EPOCH).unwrap().as_secs(),
                cycle_limit: u64::MAX,
                gas_limit: u64::MAX,
                scheduled_by: None,
                stdin_private: false,
            })
            .await
        {
            return Err(error_after_cancelling(client, &requests, error).await);
        }

        let start_time: Instant = Instant::now();
        tracing::info!("Successfully created proof request {}", proof_id);
        requests.push(ProofRequest {
            proof_id,
            proof_output_id,
            deadline,
            start_time,
        });
    }
    Ok(requests)
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
            tracing::info!(
                "Proof request for proof id {} completed after {:?}",
                proof_id,
                start_time.elapsed()
            );

            let completed_proof = artifact_client
                .download_with_type(&proof_output_id, ArtifactType::Proof)
                .await
                .map_err(|error| eyre::eyre!("failed to download proof {proof_id}: {error}"))?;
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

async fn setup_artifacts<A: ArtifactClient>(
    artifact_client: A,
    elf: ClusterElf,
    stdin: SP1Stdin,
) -> Result<(Artifact, Artifact)> {
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

    Ok((elf_id, stdin_id))
}

pub async fn request_proof<A: ArtifactClient>(
    artifact_client: A,
    elf: ClusterElf,
    stdin: SP1Stdin,
    config: &ProofRequestConfig,
) -> Result<ProofRequestResults> {
    let mut results = request_proofs(artifact_client, elf, stdin, NonZeroU16::MIN, config).await?;
    Ok(results.pop().expect("one proof requested"))
}

/// Creates every request before polling any of them, allowing the cluster to prove concurrently.
pub async fn request_proofs<A: ArtifactClient>(
    artifact_client: A,
    elf: ClusterElf,
    stdin: SP1Stdin,
    count: NonZeroU16,
    config: &ProofRequestConfig,
) -> Result<Vec<ProofRequestResults>> {
    let client = connect(&config.cluster_rpc).await?;
    let mut pending =
        create_requests(&client, artifact_client.clone(), elf, stdin, count, config).await?;

    let mut results = Vec::with_capacity(pending.len());
    while !pending.is_empty() {
        let mut still_pending = Vec::with_capacity(pending.len());
        let mut pending_iter = pending.into_iter();
        while let Some(proof_request) = pending_iter.next() {
            match check_proof_status(artifact_client.clone(), proof_request.clone(), &client).await
            {
                Ok(Some(result)) => results.push(result),
                Ok(None) => still_pending.push(proof_request),
                Err(error) => {
                    still_pending.push(proof_request);
                    still_pending.extend(pending_iter);
                    return Err(error_after_cancelling(&client, &still_pending, error).await);
                }
            }
        }
        pending = still_pending;
        if !pending.is_empty() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    Ok(results)
}

async fn cancel_pending_requests(
    client: &ClusterServiceClient,
    pending: &[ProofRequest],
) -> Result<()> {
    let mut failures = Vec::new();
    for request in pending {
        if let Err(error) = client
            .cancel_proof_request(ProofRequestCancelRequest {
                proof_id: request.proof_id.clone(),
            })
            .await
        {
            failures.push(format!("{}: {error}", request.proof_id));
        }
    }

    if failures.is_empty() {
        Ok(())
    } else {
        Err(eyre::eyre!(
            "failed to cancel {} proof requests: {}",
            failures.len(),
            failures.join("; ")
        ))
    }
}

async fn error_after_cancelling(
    client: &ClusterServiceClient,
    pending: &[ProofRequest],
    error: eyre::Report,
) -> eyre::Report {
    match cancel_pending_requests(client, pending).await {
        Ok(()) => error,
        Err(cancel_error) => eyre::eyre!("{error:#}; batch cleanup also failed: {cancel_error:#}"),
    }
}

/// Request a proof from the cluster. Waits for the proof to complete.
pub async fn request_proof_with_config(
    elf: ClusterElf,
    stdin: SP1Stdin,
    config: &ProofRequestConfig,
) -> Result<ProofRequestResults> {
    let mut results = request_proofs_with_config(elf, stdin, NonZeroU16::MIN, config).await?;
    Ok(results.pop().expect("one proof requested"))
}

pub async fn request_proofs_with_config(
    elf: ClusterElf,
    stdin: SP1Stdin,
    count: NonZeroU16,
    config: &ProofRequestConfig,
) -> Result<Vec<ProofRequestResults>> {
    match &config.artifact_store {
        ArtifactStoreConfig::Redis { nodes } => {
            tracing::info!("using redis artifact store");
            let artifact_client = RedisArtifactClient::new(nodes.clone(), 16);
            request_proofs(artifact_client, elf, stdin, count, config).await
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
            request_proofs(artifact_client, elf, stdin, count, config).await
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
        timeout_hours,
        artifact_store: artifact_store_config,
    }
}

pub async fn request_proof_from_env(
    mode: ProofMode,
    timeout_hours: u64,
    elf: ClusterElf,
    stdin: SP1Stdin,
) -> Result<ProofRequestResults> {
    let mut results =
        request_proofs_from_env(mode, timeout_hours, elf, stdin, NonZeroU16::MIN).await?;
    Ok(results.pop().expect("one proof requested"))
}

pub async fn request_proofs_from_env(
    mode: ProofMode,
    timeout_hours: u64,
    elf: ClusterElf,
    stdin: SP1Stdin,
    count: NonZeroU16,
) -> Result<Vec<ProofRequestResults>> {
    let config = request_config_from_env(mode, timeout_hours);
    request_proofs_with_config(elf, stdin, count, &config).await
}
