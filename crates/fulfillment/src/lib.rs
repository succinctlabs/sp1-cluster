use crate::metrics::FulfillerMetrics;
use crate::network::{FulfillmentNetwork, NetworkRequest};
use alloy_primitives::{Address, B256};
use anyhow::{anyhow, Result};
use futures::{future::join_all, TryFutureExt};
use sp1_cluster_artifact::{ArtifactClient, ArtifactType, CompressedUpload};
use sp1_cluster_common::{
    client::ClusterServiceClient,
    failure::ProvingFailure,
    proto::{
        ExecutionResult, ProofRequest, ProofRequestCancelRequest, ProofRequestCreateRequest,
        ProofRequestListRequest, ProofRequestStatus, ProofRequestUpdateRequest,
    },
};
use sp1_sdk::network::signer::NetworkSigner;
use spn_network_types::ProofRequestError;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{collections::HashSet, sync::Arc, time::Duration};
use tokio::{sync::Mutex, task::JoinSet, time::sleep};
use tracing::{debug, error, info, instrument, warn};

pub mod config;
pub mod grpc;
pub mod metrics;
pub mod network;
pub mod prover_info;
pub mod run;

/// The maximum number of requests to handle in a single refresh loop.
const REQUEST_LIMIT: u32 = 1000;
/// The error strings that should trigger a VERIFICATION_KEY_MISMATCH error.
const VK_MISMATCH_STRINGS: &[&str] = &[
    "InvalidPowWitness",
    "sp1 vk hash mismatch",
    "vk hash from syscall does not match vkey from input",
];

/// Whether a cancelable candidate should be cancelled at all.
///
/// A `Fulfilled` candidate that is NOT in the cluster's in-flight set is a no-op:
/// the network already succeeded and our cluster isn't wasting work on it, so
/// cancelling it would only pollute `requests_cancelled`. Every other candidate
/// (unexecutable, unfulfillable, or fulfilled-while-still-in-flight) is cancelable.
fn should_cancel(is_fulfilled: bool, in_flight: bool) -> bool {
    !is_fulfilled || in_flight
}

/// Whether the network still expects an answer for this request, i.e. whether we
/// should call `fail_fulfillment` (`cancel_on_network`).
///
/// We only release it on the network when it is neither already `Unfulfillable`
/// nor `Fulfilled`. A `Fulfilled` request succeeded, so failing it would be wrong;
/// an `Unfulfillable` one is already terminal, so failing it is a wasteful no-op.
/// For non-fulfilled requests this is identical to the existing `!is_unfulfillable`
/// gate.
fn should_cancel_on_network(is_unfulfillable: bool, is_fulfilled: bool) -> bool {
    !is_unfulfillable && !is_fulfilled
}

/// Derive the network's `ProofRequestError` from the worker's `extra_data`.
/// An `ExecutionResult` with non-zero `failure_cause` means execute_only itself
/// faulted -> `EXECUTION_FAILURE`. A `ProvingFailure` payload means a
/// post-execute task failed -> `PROVING_FAILURE`.
pub fn request_error_from_extra_data(extra_data: Option<&str>) -> Option<i32> {
    let s = extra_data?;
    if let Ok(er) = serde_json::from_str::<ExecutionResult>(s) {
        if er.failure_cause != 0 {
            return Some(ProofRequestError::ExecutionFailure as i32);
        }
    }
    ProvingFailure::from_extra_data(s).map(|_| ProofRequestError::ProvingFailure as i32)
}

/// The maximum age of the API-held cluster component manifest before the fulfiller
/// treats it as stale and skips the report. The coordinator re-pushes every ~60s, so
/// 5 minutes tolerates a few missed pushes and modest clock skew (`updated_at` is
/// API-server-owned) while still catching a dead coordinator well within one report
/// interval.
pub const MAX_MANIFEST_AGE_SECS: u64 = 5 * 60;

/// Gate the API-held manifest on freshness before it is forwarded to the SPN.
///
/// Returns `Err` (caller skips the report and retries next tick) when the API has no
/// manifest yet (`updated_at == 0`: no coordinator has pushed since the API started)
/// or the manifest is older than [`MAX_MANIFEST_AGE_SECS`] (coordinator stopped
/// pushing). Forwarding either would publish a wrong full snapshot.
pub fn fresh_manifest_components(
    manifest: sp1_cluster_common::proto::ClusterComponentManifest,
    now: u64,
) -> Result<Vec<sp1_cluster_common::proto::ClusterComponentInfo>> {
    if manifest.updated_at == 0 {
        return Err(anyhow!(
            "cluster API has no component manifest yet (coordinator has not pushed one)"
        ));
    }
    let age = now.saturating_sub(manifest.updated_at);
    if age > MAX_MANIFEST_AGE_SECS {
        return Err(anyhow!(
            "cluster component manifest is stale ({age}s old > {MAX_MANIFEST_AGE_SECS}s); \
             coordinator may be down"
        ));
    }
    Ok(manifest.components)
}

/// Assemble the public component list for a single `ReportProverInfo`: the
/// fulfiller's own component first, then the cluster manifest (coordinator +
/// workers) mapped onto the public `ComponentInfo` (which has no `instance_id`).
///
/// Manifest entries are deduped by build identity
/// `(component, version, git_sha, image_tag)`, so many workers on the same build
/// collapse to one entry while distinct builds (a rolling deploy) are kept.
///
/// Returns `Err` for a malformed manifest, so the caller skips the report rather than
/// emit a bad snapshot: the manifest must yield exactly one `coordinator` build after
/// dedup (zero — a manifest missing its coordinator — or many are malformed), and
/// there must never be more than one `fulfiller`.
pub fn assemble_components(
    identity: &prover_info::BuildIdentity,
    cluster_components: Vec<sp1_cluster_common::proto::ClusterComponentInfo>,
) -> Result<Vec<spn_network_types::ComponentInfo>> {
    let mut components = vec![prover_info::fulfiller_component(identity)];
    let mut seen = HashSet::with_capacity(cluster_components.len());
    for c in cluster_components {
        let mapped = prover_info::component_from_cluster(c);
        // Dedup by build identity: collapse same-build workers to one entry.
        if seen.insert((
            mapped.component.clone(),
            mapped.version.clone(),
            mapped.git_sha.clone(),
            mapped.image_tag.clone(),
        )) {
            components.push(mapped);
        }
    }

    // The fulfiller is added exactly once; reject a manifest that injected another.
    let fulfillers = components
        .iter()
        .filter(|c| c.component == "fulfiller")
        .count();
    if fulfillers > 1 {
        return Err(anyhow!(
            "manifest reported {fulfillers} fulfiller builds; refusing to send a malformed report"
        ));
    }

    // A cluster manifest must report exactly one coordinator build after dedup;
    // zero (manifest missing its coordinator) or many are malformed.
    let coordinators = components
        .iter()
        .filter(|c| c.component == "coordinator")
        .count();
    if coordinators != 1 {
        return Err(anyhow!(
            "cluster manifest reported {coordinators} coordinator builds (expected exactly 1); refusing to send a malformed report"
        ));
    }

    Ok(components)
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
    /// Identifies this fulfiller instance. When set, requests are tagged on creation and
    /// filtered on submit/fail, preventing cross-submission when multiple fulfillers share
    /// a coordinator.
    name: Option<String>,
    /// How often the fulfiller polls for new/completed requests.
    refresh_interval: Duration,
    /// Serializes signed network submissions. Each submission fetches the signer's nonce before
    /// submitting; without serialization, concurrent submissions sign the same nonce and all but
    /// one fail verification. Guards a single process only — replicas sharing a signer still race.
    nonce_lock: Arc<Mutex<()>>,
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
        name: Option<String>,
        refresh_interval_sec: u64,
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
            name,
            refresh_interval: Duration::from_secs(refresh_interval_sec),
            nonce_lock: Arc::new(Mutex::new(())),
        }
    }

    /// Runs the fulfiller loop.
    pub async fn run(self: Arc<Self>) -> Result<()> {
        info!(
            "starting the fulfiller with refresh interval {}s",
            self.refresh_interval.as_secs()
        );

        // Get the prover.
        // TODO: Use backoff here.
        let prover = self.network.init(&self.signer).await?;
        info!("prover address: {}", prover);

        // Resolve this fulfiller's build identity once and report it to the
        // network at startup, then on a low-frequency interval inside the main
        // loop below. Best-effort: a failure here never blocks fulfillment.
        let build_identity = prover_info::BuildIdentity::resolve();
        info!(
            version = %build_identity.version,
            git_sha = %build_identity.git_sha,
            image_tag = %build_identity.image_tag,
            "fulfiller build identity",
        );
        self.spawn_report_prover_info(prover, build_identity.clone());
        let mut last_prover_info_report = std::time::Instant::now();
        let report_interval = Duration::from_secs(prover_info::REPORT_INTERVAL_SECS);

        loop {
            // Re-report build identity on a low-frequency interval (best-effort).
            if last_prover_info_report.elapsed() >= report_interval {
                self.spawn_report_prover_info(prover, build_identity.clone());
                last_prover_info_report = std::time::Instant::now();
            }
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
            sleep(self.refresh_interval).await;
        }
    }

    /// Spawns [`Self::report_prover_info`] as a detached background task so this
    /// best-effort telemetry can never block the fulfiller's startup or its
    /// submit/fail/cancel/schedule loop — even though the report itself uses
    /// bounded timeouts. Fire-and-forget: failures are logged inside the task.
    /// Skipped when fulfillment is disabled (dry-run).
    fn spawn_report_prover_info(
        self: &Arc<Self>,
        prover: Address,
        identity: prover_info::BuildIdentity,
    ) {
        if self.disable_fulfillment {
            return;
        }
        let this = self.clone();
        tokio::spawn(async move {
            this.report_prover_info(prover, &identity).await;
        });
    }

    /// Reports cluster build identity to the network (best-effort).
    ///
    /// Sends `[fulfiller self] ++ cluster manifest (coordinator + workers)` as a full
    /// snapshot in ONE `ReportProverInfo`. The manifest is read from the cluster API
    /// (where the coordinator periodically pushes it), so no direct
    /// fulfiller->coordinator connection or topology config is needed. When the fetch
    /// fails, the API has no manifest yet, or the manifest is stale, this report is
    /// SKIPPED: the receiver treats the payload as a full snapshot, so sending a
    /// partial one would prune coordinator/worker rows. The next tick retries.
    ///
    /// On any error, logs a warning and returns — this telemetry never fails the
    /// fulfiller. `ReportProverInfo` carries no nonce, so unlike fulfill/fail it needs
    /// no `nonce_lock` serialization.
    async fn report_prover_info(&self, prover: Address, identity: &prover_info::BuildIdentity) {
        let manifest = match self.cluster.get_cluster_component_info().await {
            Ok(manifest) => manifest,
            Err(e) => {
                warn!(
                    "cluster component manifest fetch failed; skipping this report to avoid \
                     pruning coordinator/worker rows (retrying next tick): {e:?}"
                );
                return;
            }
        };

        let cluster_components = match fresh_manifest_components(manifest, time_now()) {
            Ok(components) => components,
            Err(e) => {
                warn!("skipping prover-info report (retrying next tick): {e}");
                return;
            }
        };

        let components = match assemble_components(identity, cluster_components) {
            Ok(components) => components,
            Err(e) => {
                warn!("refusing to send malformed prover-info report (retrying next tick): {e:?}");
                return;
            }
        };

        if let Err(e) = self
            .network
            .report_prover_info(self.domain.as_slice(), prover, components, &self.signer)
            .await
        {
            warn!("failed to report prover info (continuing): {:?}", e);
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
                scheduled_by: self.name.clone(),
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

                        // Fail the request if the verification key does not match the one
                        // expected for the program.
                        let err_text = format!("{e:?}");
                        if VK_MISMATCH_STRINGS.iter().any(|s| err_text.contains(s)) {
                            // ProofRequestError::VerificationKeyMismatch = 2
                            const VK_MISMATCH_ERROR: i32 = 2;
                            let _nonce_guard = self_clone.nonce_lock.lock().await;
                            match self_clone
                                .network
                                .fail_request_with_error(
                                    &request_id,
                                    Some(VK_MISMATCH_ERROR),
                                    // VK mismatch carries no cluster extra_data trace.
                                    None,
                                    self_clone.domain.as_slice(),
                                    &self_clone.signer,
                                )
                                .await
                            {
                                Ok(_) => {
                                    info!("failed request 0x{} due to VK mismatch", request_id);
                                    self_clone.metrics.requests_failed.increment(1);
                                    self_clone.metrics.total_requests_processed.increment(1);
                                }
                                Err(e) => {
                                    error!(
                                        "failed to fail request 0x{} with VK mismatch: {:?}",
                                        request_id, e
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
            let _nonce_guard = self.nonce_lock.lock().await;
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
                scheduled_by: self.name.clone(),
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
            let _nonce_guard = self.nonce_lock.lock().await;
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

        // Fetch the cluster's in-flight (Pending) request IDs so we can tell whether a
        // `Fulfilled` candidate still has wasted cluster work to abort. Mirrors the
        // listing in `schedule_requests`. Only `Fulfilled` candidates consult this set, so
        // skip the extra cluster RPC entirely when none are present (e.g. mainnet, which
        // never produces a `Fulfilled` cancelable).
        let in_flight: HashSet<String> = if requests.iter().any(|r| r.is_fulfilled()) {
            self.cluster
                .get_proof_requests(ProofRequestListRequest {
                    limit: Some(REQUEST_LIMIT),
                    minimum_deadline: Some(time_now()),
                    ..Default::default()
                })
                .map_err(|e| anyhow!("failed to get requests: {}", e))
                .await?
                .into_iter()
                .filter(|r| r.proof_status == ProofRequestStatus::Pending as i32)
                .map(|r| r.id)
                .collect()
        } else {
            HashSet::new()
        };
        let in_flight = Arc::new(in_flight);

        let failure_tasks = requests.into_iter().filter_map(|request| {
            let request_id = request.request_id();
            let is_fulfilled = request.is_fulfilled();
            let in_flight = in_flight.contains(&request_id);

            // Skip no-op cancels: a request the network already fulfilled that our cluster
            // is not proving has nothing to abort and no fulfillment to release.
            if !should_cancel(is_fulfilled, in_flight) {
                debug!(
                    "skipping fulfilled request 0x{} not in cluster in-flight set",
                    request_id
                );
                return None;
            }

            let self_clone = self.clone();
            Some(tokio::spawn(async move {
                let result = async {
                    // Only fail on the network while it still expects one. A request the network
                    // has already finalized (unfulfillable) treats the fail as a no-op, and a
                    // fulfilled request succeeded — failing either would be wrong or wasteful.
                    // The cluster unclaim runs either way.
                    if should_cancel_on_network(request.is_unfulfillable(), is_fulfilled) {
                        self_clone.cancel_on_network(&request_id).await?;
                    }
                    self_clone.cancel_on_cluster(&request_id).await
                }
                .await;
                match result {
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
            }))
        });

        join_all(failure_tasks).await;

        Ok(())
    }

    /// Releases the request on the network by failing its fulfillment.
    async fn cancel_on_network(&self, request_id: &str) -> Result<()> {
        if !self.disable_fulfillment {
            let _nonce_guard = self.nonce_lock.lock().await;
            self.network
                .cancel_request(request_id, &self.signer)
                .await?;
        }
        Ok(())
    }

    /// Marks the request cancelled on the cluster so workers stop proving it.
    async fn cancel_on_cluster(&self, request_id: &str) -> Result<()> {
        self.cluster
            .cancel_proof_request(ProofRequestCancelRequest {
                proof_id: request_id.to_string(),
            })
            .map_err(|e| anyhow!("failed to update proof request status: {}", e))
            .await?;
        Ok(())
    }

    /// Schedules the given requests to be fulfilled by the network, accounting for any cluster
    /// requests that are already present.
    async fn schedule_given_requests(
        self: &Arc<Self>,
        fulfiller_address: Vec<u8>,
        cluster_requests: &HashSet<String>,
    ) -> Result<(usize, usize)> {
        let network_requests = self
            .network
            .get_schedulable_requests(
                &self.version,
                vec![fulfiller_address],
                time_now(),
                REQUEST_LIMIT,
            )
            .await?;
        let network_request_count = network_requests.len();

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
            return Ok((network_request_count, 0));
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
        Ok((network_request_count, num_requests))
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

        // Count unexecuted requests (pending + not yet handled by coordinator).
        let unexecuted_count = cluster_requests_resp
            .iter()
            .filter(|r| r.proof_status == ProofRequestStatus::Pending as i32 && !r.handled)
            .count();
        self.metrics
            .cluster_unexecuted_requests
            .set(unexecuted_count as f64);

        // Schedule the requests in parallel for each fulfiller.
        let mut join_set = JoinSet::new();
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

        let mut total_network = 0;
        let mut total_scheduled = 0;
        while let Some(result) = join_set.join_next().await {
            let (network_count, scheduled_count) =
                result.map_err(|e| anyhow!("schedule task panicked: {e}"))??;
            total_network += network_count;
            total_scheduled += scheduled_count;
        }
        self.metrics
            .network_unexecuted_requests
            .set(total_network as f64);
        tracing::info!(
            "scheduled {} requests, {} in cluster",
            total_scheduled,
            cluster_requests.len()
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

            // For private stdin, the public URI is empty and the artifact key
            // is under the private-stdins/ prefix. Fetch a presigned URL and
            // use the matching ArtifactType so the cluster-side S3 key lines
            // up with the network-side key.
            let stdin_fetch_uri = self.network.fetch_stdin_uri(&request, &self.signer).await?;
            let stdin_artifact_type = if request.stdin_private() {
                ArtifactType::PrivateStdin
            } else {
                ArtifactType::Stdin
            };
            self.copy_artifact(
                stdin_artifact_id.clone(),
                &stdin_fetch_uri,
                stdin_artifact_type,
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
                scheduled_by: self.name.clone(),
                stdin_private: request.stdin_private(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use sp1_cluster_common::proto::ClusterComponentInfo;

    /// `should_cancel` skips exactly one case: a fulfilled request that is not
    /// in the cluster's in-flight set (a no-op cancel). All other combinations
    /// are cancelable.
    #[test]
    fn should_cancel_all_combos() {
        // (is_fulfilled, in_flight) -> expected
        assert!(should_cancel(false, false)); // unexecutable/unfulfillable, not in-flight
        assert!(should_cancel(false, true)); // unexecutable/unfulfillable, in-flight
        assert!(should_cancel(true, true)); // fulfilled while still proving -> abort
        assert!(!should_cancel(true, false)); // fulfilled, nothing to abort -> skip
    }

    /// `should_cancel_on_network` only releases the request on the network when it
    /// is neither already unfulfillable nor fulfilled. This generalizes the old
    /// `!is_unfulfillable` gate: for non-fulfilled requests the result is identical.
    #[test]
    fn should_cancel_on_network_all_combos() {
        // (is_unfulfillable, is_fulfilled) -> expected
        assert!(should_cancel_on_network(false, false)); // network still expects an answer
        assert!(!should_cancel_on_network(true, false)); // already terminal (unfulfillable)
        assert!(!should_cancel_on_network(false, true)); // succeeded -> must not fail it
        assert!(!should_cancel_on_network(true, true)); // both terminal -> no-op
    }

    /// For non-fulfilled requests, `should_cancel_on_network` must match the old
    /// `!is_unfulfillable` behavior exactly.
    #[test]
    fn should_cancel_on_network_preserves_legacy_gate() {
        for is_unfulfillable in [false, true] {
            assert_eq!(
                should_cancel_on_network(is_unfulfillable, false),
                !is_unfulfillable
            );
        }
    }

    fn fulfiller_identity() -> prover_info::BuildIdentity {
        prover_info::BuildIdentity {
            version: "2.5.0".to_string(),
            git_sha: "fulfillersha".to_string(),
            image_tag: "base-fulfillersha".to_string(),
        }
    }

    /// Build a cluster manifest entry, keyed by build identity (same-build workers
    /// produce identical entries).
    fn cluster_entry(component: &str, git_sha: &str) -> ClusterComponentInfo {
        ClusterComponentInfo {
            component: component.to_string(),
            version: "2.5.0".to_string(),
            git_sha: git_sha.to_string(),
            image_tag: format!("{component}-{git_sha}"),
        }
    }

    #[test]
    fn assemble_components_includes_fulfiller_coordinator_and_workers() {
        let cluster = vec![
            cluster_entry("coordinator", "coordsha"),
            cluster_entry("gpu-node", "gpusha"),
            cluster_entry("cpu-node", "cpusha"),
        ];

        let components = assemble_components(&fulfiller_identity(), cluster).unwrap();

        // fulfiller + coordinator + 2 workers.
        assert_eq!(components.len(), 4);
        assert_eq!(
            components[0].component, "fulfiller",
            "fulfiller must be first"
        );
        assert_eq!(components[0].git_sha, "fulfillersha");
        assert_eq!(components[1].component, "coordinator");
        assert_eq!(components[1].git_sha, "coordsha");
        assert!(components
            .iter()
            .any(|c| c.component == "gpu-node" && c.git_sha == "gpusha"));
        assert!(components
            .iter()
            .any(|c| c.component == "cpu-node" && c.git_sha == "cpusha"));
    }

    #[test]
    fn assemble_components_collapses_same_build_workers() {
        // Two gpu-node workers on the same build collapse to one public entry;
        // fulfiller + coordinator are still included.
        let cluster = vec![
            cluster_entry("coordinator", "coordsha"),
            cluster_entry("gpu-node", "samesha"),
            cluster_entry("gpu-node", "samesha"),
        ];

        let components = assemble_components(&fulfiller_identity(), cluster).unwrap();

        assert_eq!(
            components.len(),
            3,
            "fulfiller + coordinator + one collapsed gpu-node build"
        );
        assert_eq!(components[0].component, "fulfiller");
        let gpu = components
            .iter()
            .filter(|c| c.component == "gpu-node")
            .count();
        assert_eq!(gpu, 1, "same-build gpu workers collapse to one entry");
    }

    #[test]
    fn assemble_components_keeps_distinct_builds_for_rolling_deploy() {
        // Old + new gpu-node builds during a rolling deploy: both remain.
        let cluster = vec![
            cluster_entry("coordinator", "coordsha"),
            cluster_entry("gpu-node", "oldsha"),
            cluster_entry("gpu-node", "newsha"),
        ];

        let components = assemble_components(&fulfiller_identity(), cluster).unwrap();

        let builds: HashSet<_> = components
            .iter()
            .filter(|c| c.component == "gpu-node")
            .map(|c| c.git_sha.as_str())
            .collect();
        assert_eq!(
            builds,
            HashSet::from(["oldsha", "newsha"]),
            "both builds kept"
        );
    }

    #[test]
    fn assemble_components_rejects_two_coordinator_builds() {
        // Two distinct coordinator builds is malformed (singleton) => Err, not sent.
        let cluster = vec![
            cluster_entry("coordinator", "coordsha-a"),
            cluster_entry("coordinator", "coordsha-b"),
        ];

        assert!(
            assemble_components(&fulfiller_identity(), cluster).is_err(),
            "two distinct coordinator builds must be rejected"
        );
    }

    #[test]
    fn assemble_components_rejects_manifest_without_coordinator() {
        // A pushed manifest must contain the coordinator: a manifest with no
        // coordinator entry (workers-only, or empty) is malformed and must not be sent.
        let workers_only = vec![cluster_entry("gpu-node", "gpusha")];
        assert!(
            assemble_components(&fulfiller_identity(), workers_only).is_err(),
            "workers-only manifest (no coordinator) must be rejected"
        );
        assert!(
            assemble_components(&fulfiller_identity(), vec![]).is_err(),
            "empty manifest (no coordinator) must be rejected"
        );
    }

    #[test]
    fn fresh_manifest_components_rejects_missing_manifest() {
        // updated_at == 0 => the API has never received a push; skip, don't report.
        let manifest = sp1_cluster_common::proto::ClusterComponentManifest {
            components: vec![],
            updated_at: 0,
        };
        assert!(fresh_manifest_components(manifest, 1_000_000).is_err());
    }

    #[test]
    fn fresh_manifest_components_rejects_stale_manifest() {
        // Older than MAX_MANIFEST_AGE_SECS => coordinator stopped pushing; skip.
        let now = 1_000_000;
        let manifest = sp1_cluster_common::proto::ClusterComponentManifest {
            components: vec![cluster_entry("coordinator", "coordsha")],
            updated_at: now - MAX_MANIFEST_AGE_SECS - 1,
        };
        assert!(fresh_manifest_components(manifest, now).is_err());
    }

    #[test]
    fn fresh_manifest_components_accepts_fresh_manifest() {
        let now = 1_000_000;
        let manifest = sp1_cluster_common::proto::ClusterComponentManifest {
            components: vec![
                cluster_entry("coordinator", "coordsha"),
                cluster_entry("cpu-node", "cpusha"),
            ],
            updated_at: now - 30,
        };
        let components = fresh_manifest_components(manifest, now).unwrap();
        assert_eq!(components.len(), 2);
    }

    #[test]
    fn fresh_manifest_components_tolerates_small_clock_skew() {
        // updated_at slightly in the future (API clock ahead) must not be "stale".
        let now = 1_000_000;
        let manifest = sp1_cluster_common::proto::ClusterComponentManifest {
            components: vec![cluster_entry("coordinator", "coordsha")],
            updated_at: now + 30,
        };
        assert!(fresh_manifest_components(manifest, now).is_ok());
    }
}
