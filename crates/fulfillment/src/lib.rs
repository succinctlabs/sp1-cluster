use crate::metrics::FulfillerMetrics;
use crate::network::{FulfillmentNetwork, NetworkRequest};
use alloy_primitives::{Address, B256};
use anyhow::{anyhow, Result};
use futures::{future::join_all, TryFutureExt};
use sp1_cluster_artifact::{ArtifactClient, ArtifactType, CompressedUpload};
use sp1_cluster_common::{
    client::ClusterServiceClient,
    proto::{
        ProofRequest, ProofRequestCancelRequest, ProofRequestCreateRequest,
        ProofRequestListRequest, ProofRequestStatus, ProofRequestUpdateRequest,
    },
};
use sp1_sdk::network::signer::NetworkSigner;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{collections::HashSet, sync::Arc, time::Duration};
use tokio::{task::JoinSet, time::sleep};
use tracing::{debug, error, info, instrument};

pub mod config;
pub mod grpc;
pub mod metrics;
pub mod network;
pub mod run;

/// How long to wait between checking for requesters to start proving on and fulfilling proofs.
///
/// Lower values will result in faster E2E latency for the user, but more outgoing requests to the
/// cluster.
const REFRESH_INTERVAL_SEC: u64 = 3;
/// The maximum number of requests to handle in a single refresh loop.
const REQUEST_LIMIT: u32 = 1000;
/// Terminal errors that can occur when submitting a proof to the network. These represent
/// permanent rejection conditions where retrying will never succeed.
#[derive(Debug, Clone, Copy)]
enum TerminalSubmitError {
    /// The execution oracle marked the request as unexecutable (e.g., guest panic).
    Unexecutable,
    /// The verification key does not match what the network expects for the program.
    VkMismatch,
}

impl TerminalSubmitError {
    /// Classifies a submission error into a terminal error, if it matches.
    fn classify(err: &anyhow::Error) -> Option<Self> {
        const UNEXECUTABLE_PATTERNS: &[&str] = &["not in an executed state"];
        const VK_MISMATCH_PATTERNS: &[&str] = &[
            "InvalidPowWitness",
            "sp1 vk hash mismatch",
            "vk hash from syscall does not match vkey from input",
        ];

        let err_text = format!("{err:?}");
        if UNEXECUTABLE_PATTERNS.iter().any(|s| err_text.contains(s)) {
            Some(Self::Unexecutable)
        } else if VK_MISMATCH_PATTERNS.iter().any(|s| err_text.contains(s)) {
            Some(Self::VkMismatch)
        } else {
            None
        }
    }

    /// The error code to send to the network when failing the request.
    fn network_error_code(self) -> Option<i32> {
        match self {
            // ProofRequestError::ExecutionFailure = 1.
            Self::Unexecutable => Some(1),
            // ProofRequestError::VerificationKeyMismatch = 2.
            Self::VkMismatch => Some(2),
        }
    }
}

impl std::fmt::Display for TerminalSubmitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unexecutable => write!(f, "unexecutable"),
            Self::VkMismatch => write!(f, "VK mismatch"),
        }
    }
}

#[derive(Clone)]
pub struct Fulfiller<A: ArtifactClient + CompressedUpload, N: FulfillmentNetwork> {
    network: N,
    cluster: ClusterServiceClient,
    cluster_artifact_client: A,
    version: String,
    domain: B256,
    metrics: FulfillerMetrics,
    addresses: Option<Vec<Address>>,
    signer: NetworkSigner,
    copy_artifacts: bool,
    /// Disable sending fulfillment requests to the network (for testing/dry-run)
    disable_fulfillment: bool,
    /// Probability (0.0-1.0) of processing a request. Default is 1.0 (100%).
    request_probability: f64,
}

impl<A: ArtifactClient + CompressedUpload, N: FulfillmentNetwork> Fulfiller<A, N> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        network: N,
        cluster: ClusterServiceClient,
        cluster_artifact_client: A,
        version: String,
        domain: B256,
        metrics: FulfillerMetrics,
        addresses: Option<Vec<Address>>,
        signer: NetworkSigner,
        copy_artifacts: bool,
        disable_fulfillment: bool,
        request_probability: f64,
    ) -> Self {
        Self {
            network,
            cluster,
            cluster_artifact_client,
            version,
            domain,
            metrics,
            addresses,
            signer,
            copy_artifacts,
            disable_fulfillment,
            request_probability,
        }
    }

    /// Fails a request on the network with an optional error code and marks it as handled on
    /// the cluster. Used when submission is permanently rejected (unexecutable, VK mismatch).
    async fn fail_and_mark_handled(&self, request_id: &str, error: Option<i32>) -> Result<()> {
        self.network
            .fail_request_with_error(request_id, error, self.domain.as_slice(), &self.signer)
            .await?;

        if let Err(e) = self
            .cluster
            .update_proof_request(ProofRequestUpdateRequest {
                proof_id: request_id.to_string(),
                handled: Some(true),
                ..Default::default()
            })
            .await
        {
            error!(
                "failed to mark request 0x{} as handled: {:?}",
                request_id, e
            );
        }

        Ok(())
    }

    /// Runs the fulfiller loop.
    pub async fn run(self: Arc<Self>) -> Result<()> {
        info!("starting the fulfiller");

        // Get the prover.
        // TODO: Use backoff here.
        let prover = self.network.init(&self.signer).await?;
        info!("prover address: {}", prover);

        loop {
            // Check for requests to submit.
            if let Err(e) = self.submit_requests().await {
                error!("submitting requests: {:?}", e);
                self.metrics.main_loop_errors.increment(1);
            }
            // Check for requests to fail.
            if let Err(e) = self.fail_requests().await {
                error!("failing requests: {:?}", e);
                self.metrics.main_loop_errors.increment(1);
            }
            // Check for requests to cancel.
            if let Err(e) = self.cancel_requests(prover).await {
                error!("cancelling requests: {:?}", e);
                self.metrics.main_loop_errors.increment(1);
            }
            // Check for requests to schedule.
            if let Err(e) = self.schedule_requests(prover).await {
                error!("scheduling requests: {:?}", e);
                self.metrics.main_loop_errors.increment(1);
            }
            // Wait for the next interval.
            sleep(Duration::from_secs(REFRESH_INTERVAL_SEC)).await;
        }
    }

    /// Checks for submitable proofs that are in the cluster and submits them to the network.
    #[instrument(skip_all)]
    async fn submit_requests(self: &Arc<Self>) -> Result<()> {
        // Get all submitable requests from the cluster.
        let requests = self
            .cluster
            .get_proof_requests(ProofRequestListRequest {
                proof_status: vec![ProofRequestStatus::Completed.into()],
                limit: Some(REQUEST_LIMIT),
                minimum_deadline: Some(time_now()),
                handled: Some(false),
                ..Default::default()
            })
            .map_err(|e| anyhow!("failed to get requests: {}", e))
            .await?;
        debug!("got requests: {:?}", requests);

        self.metrics.submittable_requests.set(requests.len() as f64);

        if requests.is_empty() {
            info!("found no submitable requests");
            return Ok(());
        }
        info!("found {} submitable requests", requests.len());

        let submission_tasks = requests.into_iter().map(|request| {
            let self_clone = self.clone();
            let request_id = request.id.clone();
            tokio::spawn(async move {
                match self_clone.submit_request(request).await {
                    Ok(_) => {
                        info!("submitted request 0x{}", request_id);
                        self_clone.metrics.requests_submitted.increment(1);
                        self_clone.metrics.total_requests_processed.increment(1);
                    }
                    Err(e) => {
                        error!("failed to submit request 0x{}: {:?}", request_id, e);
                        self_clone.metrics.request_submission_failures.increment(1);

                        // Classify the submission error and fail the request permanently
                        // on the network if it matches a known terminal condition.
                        if let Some(terminal) = TerminalSubmitError::classify(&e) {
                            match self_clone
                                .fail_and_mark_handled(&request_id, terminal.network_error_code())
                                .await
                            {
                                Ok(()) => {
                                    info!(
                                        "request 0x{} rejected and handled: {}",
                                        request_id, terminal
                                    );
                                    self_clone.metrics.requests_failed.increment(1);
                                    self_clone.metrics.total_requests_processed.increment(1);
                                }
                                Err(e) => {
                                    error!(
                                        "request 0x{} rejected ({}), failed to handle: {:?}",
                                        request_id, terminal, e
                                    );
                                }
                            }
                        }
                    }
                }
            })
        });

        join_all(submission_tasks).await;

        Ok(())
    }

    async fn submit_request(&self, request: ProofRequest) -> Result<()> {
        // Download the raw proof bytes from the artifact.
        let proof_bytes = if let Some(id) = request.proof_artifact_id.clone() {
            if N::should_download_proofs() {
                let proof_bytes = self
                    .cluster_artifact_client
                    .download_raw(&id, sp1_cluster_artifact::ArtifactType::Proof)
                    .await?;
                Some(proof_bytes)
            } else {
                None
            }
        } else {
            None
        };

        let Ok(_) = hex::decode(request.id.clone()) else {
            tracing::warn!("ignoring request with invalid id {}", request.id);

            // Update the status to fulfilled on the cluster.
            self.cluster
                .update_proof_request(ProofRequestUpdateRequest {
                    proof_id: request.id,
                    handled: Some(true),
                    ..Default::default()
                })
                .map_err(|e| anyhow!("failed to update proof request status: {}", e))
                .await?;
            return Ok(());
        };

        // Submit the fulfill request to the network.
        if !self.disable_fulfillment {
            self.network
                .submit_request(&request, proof_bytes, self.domain.as_slice(), &self.signer)
                .await?;
        }

        // Update the status to fulfilled on the cluster.
        self.cluster
            .update_proof_request(ProofRequestUpdateRequest {
                proof_id: request.id.clone(),
                handled: Some(true),
                ..Default::default()
            })
            .map_err(|e| anyhow!("failed to update proof request status: {}", e))
            .await?;

        if let Some(id) = request.proof_artifact_id.clone() {
            // Clean up the proof artifact since it's no longer needed
            self.cluster_artifact_client
                .try_delete(&id, sp1_cluster_artifact::ArtifactType::Proof)
                .await?;
        }

        Ok(())
    }

    /// Checks for requests on the cluster that have failed and fails them on the network.
    async fn fail_requests(self: &Arc<Self>) -> Result<()> {
        // Get all failed requests from the cluster.
        let requests = self
            .cluster
            .get_proof_requests(ProofRequestListRequest {
                proof_status: vec![ProofRequestStatus::Failed.into()],
                limit: Some(REQUEST_LIMIT),
                minimum_deadline: Some(time_now()),
                handled: Some(false),
                ..Default::default()
            })
            .map_err(|e| anyhow!("failed to get requests: {}", e))
            .await?;
        self.metrics.failable_requests.set(requests.len() as f64);

        if requests.is_empty() {
            info!("found no failed requests");
            return Ok(());
        }
        info!("found {} failed requests", requests.len());

        let failure_tasks = requests.into_iter().map(|request| {
            let self_clone = self.clone();
            tokio::spawn(async move {
                let request_id = request.id.clone();
                match self_clone.fail_request(request).await {
                    Ok(_) => {
                        info!("failed request 0x{}", request_id);
                        self_clone.metrics.requests_failed.increment(1);
                        self_clone.metrics.total_requests_processed.increment(1);
                    }
                    Err(e) => {
                        error!("failed to fail request 0x{}: {:?}", request_id, e);
                        self_clone.metrics.request_fail_failures.increment(1);
                    }
                }
            })
        });

        join_all(failure_tasks).await;

        Ok(())
    }

    async fn fail_request(&self, request: ProofRequest) -> Result<()> {
        // Send the failed fulfillment to the network.
        if !self.disable_fulfillment {
            self.network
                .fail_request(&request, self.domain.as_slice(), &self.signer)
                .await?;
        }

        // Mark the request as handled in the cluster.
        self.cluster
            .update_proof_request(ProofRequestUpdateRequest {
                proof_id: request.id,
                handled: Some(true),
                ..Default::default()
            })
            .map_err(|e| anyhow!("failed to update proof request status: {}", e))
            .await?;

        Ok(())
    }

    /// Checks for requests in the network that have an ExecutionStatus of UNEXECUTABLE and cancels
    /// them on the cluster.
    async fn cancel_requests(self: &Arc<Self>, prover: Address) -> Result<()> {
        // Get all requested unexecutable requests from the network that are assigned to our
        // address.
        let fulfiller_addresses = self
            .addresses
            .as_ref()
            .map(|v| v.clone().into_iter().map(|a| a.to_vec()).collect())
            .unwrap_or(vec![prover.to_vec()]);

        let requests = self
            .network
            .get_cancelable_requests(
                &self.version,
                fulfiller_addresses,
                time_now(),
                REQUEST_LIMIT,
            )
            .await?;

        self.metrics.cancelable_requests.set(requests.len() as f64);

        if requests.is_empty() {
            info!("found no cancelable requests");
            return Ok(());
        }
        info!("found {} cancelable requests", requests.len());

        let failure_tasks = requests.into_iter().map(|request| {
            let self_clone = self.clone();
            tokio::spawn(async move {
                let request_id = request.request_id();
                match self_clone.cancel_request(&request_id).await {
                    Ok(_) => {
                        info!("cancelled request 0x{}", request_id);
                        self_clone.metrics.requests_cancelled.increment(1);
                        self_clone.metrics.total_requests_processed.increment(1);
                    }
                    Err(e) => {
                        error!("failed to cancel request 0x{}: {:?}", request_id, e);
                        self_clone.metrics.request_cancel_failures.increment(1);
                    }
                }
            })
        });

        join_all(failure_tasks).await;

        Ok(())
    }

    /// Cancels a request by notifying the network and forcing the cluster to stop any
    /// in-progress proving. Falls back to marking handled=true when the cluster cancel
    /// fails (request no longer Pending), which triggers coordinator orphan detection
    /// and aborts running proof tasks within ~500ms.
    async fn cancel_request(&self, request_id: &str) -> Result<()> {
        // Send the cancellation to the network.
        if !self.disable_fulfillment {
            self.network
                .cancel_request(request_id, &self.signer)
                .await?;
        }

        // Try to cancel on the cluster (only works for Pending requests).
        let cancel_result = self
            .cluster
            .cancel_proof_request(ProofRequestCancelRequest {
                proof_id: request_id.to_string(),
            })
            .await;

        if cancel_result.is_err() {
            // Request is no longer Pending (proving in progress or completed).
            // Mark as handled to trigger coordinator orphan detection, which
            // will abort any running proof tasks within ~500ms.
            info!(
                "cancel failed for 0x{}, marking handled to abort in-progress proving",
                request_id
            );
            self.cluster
                .update_proof_request(ProofRequestUpdateRequest {
                    proof_id: request_id.to_string(),
                    handled: Some(true),
                    ..Default::default()
                })
                .map_err(|e| anyhow!("failed to mark request as handled: {}", e))
                .await?;
        }

        Ok(())
    }

    /// Schedules the given requests to be fulfilled by the network, accounting for any cluster
    /// requests that are already present.
    async fn schedule_given_requests(
        self: &Arc<Self>,
        fulfiller_address: Vec<u8>,
        cluster_requests: &HashSet<String>,
    ) -> Result<usize> {
        let network_requests = self
            .network
            .get_schedulable_requests(
                &self.version,
                vec![fulfiller_address],
                time_now(),
                REQUEST_LIMIT,
            )
            .await?;

        // Filter requested requests that are in the network but not in the cluster. Requests are
        // returned in order of oldest first, so we also reverse to schedule newest requests first
        // instead.
        let requests: Vec<_> = network_requests
            .into_iter()
            .rev()
            .filter(|request| {
                let request_id_hex = request.request_id();

                // If request_probability is 1, skip the deterministic check and process all
                // requests
                let is_process_all = self.request_probability == 1.0;

                // Note: We can't check fulfiller via NetworkRequest trait, so we skip this check
                // The network layer should handle filtering requests with fulfillers

                // Skip requests that the execution oracle has marked as unexecutable.
                if request.is_unexecutable() {
                    info!("skipping unexecutable request 0x{}", request_id_hex);
                    return false;
                }

                // Check if it's already in the cluster
                if cluster_requests.contains(&request_id_hex) {
                    info!("skipping request 0x{} already in cluster", request_id_hex);
                    return false;
                }

                // If processing all requests, return true immediately
                if is_process_all {
                    return true;
                }

                // Use a deterministic hash of the request ID to decide whether to process
                let hash = request_id_hex
                    .as_bytes()
                    .iter()
                    .fold(0u64, |acc, &b| acc.wrapping_add(b as u64));
                let should_process = (hash % 100) < (self.request_probability * 100.0) as u64;

                if !should_process {
                    info!(
                        "deterministically skipping request 0x{} based on hash",
                        request_id_hex
                    );
                }

                should_process
            })
            .collect();
        self.metrics.schedulable_requests.set(requests.len() as f64);

        if requests.is_empty() {
            return Ok(0);
        }

        let num_requests = requests.len();

        // Schedule each request in the cluster to start proving.
        let schedule_tasks = requests.into_iter().map(|request| {
            let self_clone = self.clone();
            let request_id_hex = request.request_id();
            tokio::spawn(async move {
                tracing::info!(
                    "scheduling request 0x{} {}",
                    request_id_hex,
                    hex::encode(request.requester()),
                );
                match self_clone.schedule_request(request).await {
                    Ok(_) => {
                        info!("scheduled request 0x{}", request_id_hex);
                        self_clone.metrics.requests_scheduled.increment(1);
                        self_clone.metrics.total_requests_processed.increment(1);
                    }
                    Err(e) => {
                        error!("failed to schedule request 0x{}: {:?}", request_id_hex, e);
                        self_clone.metrics.request_schedule_failures.increment(1);
                    }
                }
            })
        });

        join_all(schedule_tasks).await;
        Ok(num_requests)
    }

    /// Checks for assigned requests that are in the network but not in the cluster, and schedules
    /// them in the cluster to start proving.
    #[instrument(skip_all)]
    async fn schedule_requests(self: &Arc<Self>, prover: Address) -> Result<()> {
        // Get all requested requests from the cluster.
        let cluster_requests_resp = self
            .cluster
            .get_proof_requests(ProofRequestListRequest {
                limit: Some(REQUEST_LIMIT),
                minimum_deadline: Some(time_now()),
                ..Default::default()
            })
            .map_err(|e| anyhow!("failed to get requests: {}", e))
            .await?;
        debug!("cluster_requests_resp: {:?}", cluster_requests_resp);

        // Setup fulfiller addresses for each fulfiller, or just the prover address if the filter's unset.
        let fulfiller_addresses = self
            .addresses
            .as_ref()
            .map(|v| v.clone().into_iter().map(|a| a.to_vec()).collect())
            .unwrap_or(vec![prover.to_vec()]);

        // Schedule the requests in parallel for each fulfiller.
        let mut join_set = JoinSet::new();
        let cluster_requests_len = cluster_requests_resp.len();
        let cluster_requests: HashSet<_> =
            cluster_requests_resp.into_iter().map(|r| r.id).collect();
        let cluster_requests = Arc::new(cluster_requests);
        for address in fulfiller_addresses {
            let self_clone = self.clone();
            let address = address.clone();
            let cluster_requests_clone = cluster_requests.clone();
            join_set.spawn(async move {
                self_clone
                    .schedule_given_requests(address, &cluster_requests_clone)
                    .await
            });
        }

        let mut total = 0;
        while let Some(request) = join_set.join_next().await {
            total += request.unwrap()?;
        }
        tracing::info!(
            "scheduled {} requests, {} in cluster",
            total,
            cluster_requests_len
        );

        Ok(())
    }

    async fn copy_artifact(
        &self,
        id: String,
        uri: &str,
        artifact_type: ArtifactType,
    ) -> Result<()> {
        if !self
            .cluster_artifact_client
            .exists(&id, artifact_type)
            .await?
        {
            let bytes = self
                .network
                .download_artifact(&id, uri, artifact_type)
                .await?;
            // Bytes from network bucket are already zstd-compressed, bypass compression
            self.cluster_artifact_client
                .upload_raw_compressed(&id, artifact_type, bytes)
                .await?;
        }
        Ok(())
    }

    async fn schedule_request(
        &self,
        request: <N as FulfillmentNetwork>::NetworkRequest,
    ) -> Result<()> {
        use crate::network::NetworkRequest;
        let request_id = request.request_id();
        let program_artifact_id = extract_artifact_name(request.program_uri())?;
        let stdin_artifact_id = extract_artifact_name(request.stdin_uri())?;
        let deadline = request.deadline();

        // Create an empty proof artifact, where the cluster will upload the proof to.
        let proof_artifact_id = self.cluster_artifact_client.create_artifact()?;

        // If copying artifacts is enabled, copy the program and stdin artifacts to the cluster bucket.
        if self.copy_artifacts {
            self.copy_artifact(
                program_artifact_id.clone(),
                request.program_public_uri(),
                ArtifactType::Program,
            )
            .await?;
            self.copy_artifact(
                stdin_artifact_id.clone(),
                request.stdin_public_uri(),
                ArtifactType::Stdin,
            )
            .await?;
        }

        // Schedule the request to start proving.
        self.cluster
            .create_proof_request(ProofRequestCreateRequest {
                proof_id: request_id,
                program_artifact_id,
                stdin_artifact_id,
                options_artifact_id: Some(request.mode().to_string()),
                proof_artifact_id: Some(proof_artifact_id.to_id()),
                requester: request.requester().to_vec(),
                deadline,
                cycle_limit: request.cycle_limit(),
                gas_limit: request.gas_limit(),
            })
            .map_err(|e| anyhow!("failed to create proof request: {}", e))
            .await?;

        Ok(())
    }
}

#[must_use]
pub fn time_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time went backwards")
        .as_secs()
}

/// Given a S3 URL (e.g. <s3://prover-network-staging/artifacts/artifact_01j92x39ngfnrra5br9n8zr07x>),
/// extract the artifact name from the URL (e.g. `artifact_01j92x39ngfnrra5br9n8zr07x`).
///
/// This is used because the cluster assumes a specific bucket and path already, and just operates
/// on the artifact name.
pub fn extract_artifact_name(s3_url: &str) -> Result<String> {
    s3_url
        .split('/')
        .next_back()
        .map(String::from)
        .ok_or_else(|| anyhow!("Invalid S3 URL format"))
}
