use std::{collections::HashSet, sync::Arc, time::SystemTime};

use crate::metrics::CoordinatorMetrics;
use crate::{AssignmentPolicy, Coordinator, ProofResult};
use dashmap::{DashMap, DashSet};
use hex;
use sp1_cluster_common::{
    client::ClusterServiceClient,
    proto::{
        CreateProofRequest, ProofRequestListRequest, ProofRequestStatus, ProofRequestUpdateRequest,
    },
};
use sp1_prover::worker::ControllerInputMetadata;
use tokio::{sync::mpsc, task::JoinHandle};

/// What the coordinator knows about a proof it owns.
#[derive(Clone)]
pub enum TaskState {
    /// Claimed and still proving.
    Pending,
    /// Finished — the exact completion write, kept so a lost one can be re-issued verbatim.
    Terminal(ProofRequestUpdateRequest),
}

/// Re-issue a proof's lost terminal write. Fire-and-forget so it never stalls the claimer loop,
/// deduped via `inflight`, and drops the `task_map` entry once the write lands or is moot.
fn reissue_lost_status_write(
    api_client: &Arc<ClusterServiceClient>,
    metrics: &Arc<CoordinatorMetrics>,
    inflight: &Arc<DashSet<String>>,
    task_map: &Arc<DashMap<String, TaskState>>,
    update: ProofRequestUpdateRequest,
) {
    let proof_id = update.proof_id.clone();
    if !inflight.insert(proof_id.clone()) {
        return; // a re-issue for this proof is already running
    }
    tracing::warn!("re-issuing lost status write for proof {}", proof_id);

    let api_client = api_client.clone();
    let metrics = metrics.clone();
    let inflight = inflight.clone();
    let task_map = task_map.clone();
    tokio::task::spawn(async move {
        match api_client.update_proof_request(update).await {
            Ok(()) => {
                metrics.increment_reissued_status_writes();
                // stop tracking — but only if it's still our Terminal, not a re-claimed Pending.
                task_map.remove_if(&proof_id, |_, v| matches!(v, TaskState::Terminal(_)));
            }
            Err(e) => {
                tracing::warn!(
                    "re-issue of lost status write for {} failed: {}",
                    proof_id,
                    e
                )
            }
        }
        inflight.remove(&proof_id);
    });
}

/// Spawn a task to update proof statuses with the cluster API given a channel of proof
/// completions. Stops when `token` fires (or all senders drop).
pub fn spawn_proof_status_task<P: AssignmentPolicy>(
    api_client: Arc<ClusterServiceClient>,
    task_map: Arc<DashMap<String, TaskState>>,
    mut completed_rx: mpsc::UnboundedReceiver<ProofResult<P>>,
    token: tokio_util::sync::CancellationToken,
) -> JoinHandle<()> {
    tokio::task::spawn(async move {
        loop {
            let next = tokio::select! {
                next = completed_rx.recv() => next,
                _ = token.cancelled() => break,
            };
            let Some(ProofResult {
                id,
                success,
                metadata,
                extra_data,
            }) = next
            else {
                break;
            };
            match task_map.get(&id).map(|s| matches!(*s, TaskState::Pending)) {
                None => {
                    tracing::error!("proof {} not found", id);
                    continue;
                }
                Some(false) => {
                    tracing::error!(
                        "proof {} completed from an unexpected (non-Pending) state",
                        id
                    );
                }
                Some(true) => {}
            }
            let status = if success {
                ProofRequestStatus::Completed
            } else {
                ProofRequestStatus::Failed
            };
            tracing::info!("Setting proof status to {:?} for {}", status, id);
            let metadata_string = serde_json::to_string(&metadata).unwrap_or_else(|e| {
                tracing::error!("Failed to serialize metadata: {}", e);
                "null".to_string()
            });
            let update = ProofRequestUpdateRequest {
                proof_id: id.clone(),
                proof_status: Some(status.into()),
                metadata: Some(metadata_string),
                extra_data,
                ..Default::default()
            };
            // Record the write before attempting it; the claimer re-issues it if it's lost.
            task_map.insert(id.clone(), TaskState::Terminal(update.clone()));
            match api_client.update_proof_request(update).await {
                Ok(()) => {
                    // confirmed (or moot) → stop tracking; conditional so a re-claim isn't dropped.
                    task_map.remove_if(&id, |_, v| matches!(v, TaskState::Terminal(_)));
                }
                // transient failure → keep the Terminal entry so the claimer re-issues it
                Err(e) => tracing::error!("Failed to update proof status: {}", e),
            }
        }
    })
}

/// Spawn a task to claim proofs from the cluster API. Stops when `token` fires.
pub fn spawn_proof_claimer_task<P: AssignmentPolicy>(
    api_client: Arc<ClusterServiceClient>,
    coordinator: Arc<Coordinator<P>>,
    task_map: Arc<DashMap<String, TaskState>>,
    metrics: Arc<CoordinatorMetrics>,
    token: tokio_util::sync::CancellationToken,
) -> JoinHandle<()> {
    tokio::task::spawn({
        async move {
            // Proof ids whose re-issue is in flight, so the next poll doesn't spawn a duplicate.
            let reissue_inflight: Arc<DashSet<String>> = Arc::new(DashSet::new());
            loop {
                match api_client
                    .get_proof_requests(ProofRequestListRequest {
                        proof_status: vec![ProofRequestStatus::Pending.into()],
                        handled: Some(false),
                        minimum_deadline: Some(
                            SystemTime::now()
                                .duration_since(SystemTime::UNIX_EPOCH)
                                .unwrap()
                                .as_secs(),
                        ),
                        limit: Some(1000),
                        ..Default::default()
                    })
                    .await
                {
                    Ok(response) => {
                        let mut seen_set = HashSet::new();
                        for proof in response.into_iter().rev() {
                            seen_set.insert(proof.id.clone());
                            if task_map.contains_key(&proof.id) {
                                // Already tracking it (proving, or a lost write the sweep handles).
                                continue;
                            }
                            let Some(options_artifact_id) = proof.options_artifact_id else {
                                tracing::error!(
                                    "Options artifact ID not found for proof {}",
                                    proof.id
                                );
                                continue;
                            };
                            let Some(cycle_limit) = proof.cycle_limit else {
                                tracing::error!("Cycle limit not found for proof {}", proof.id);
                                continue;
                            };
                            let Some(proof_artifact_id) = proof.proof_artifact_id else {
                                tracing::error!(
                                    "Proof artifact ID not found for proof {}",
                                    proof.id
                                );
                                continue;
                            };

                            let proof_nonce = String::new();
                            let metadata = match serde_json::to_string(&ControllerInputMetadata {
                                stdin_private: proof.stdin_private,
                            }) {
                                Ok(metadata) => metadata,
                                Err(e) => {
                                    tracing::error!(
                                        "Failed to serialize ControllerInputMetadata: {e}"
                                    );
                                    continue;
                                }
                            };

                            // TODO: could bulk create
                            let inputs = vec![
                                proof.program_artifact_id,
                                proof.stdin_artifact_id,
                                options_artifact_id,
                                cycle_limit.to_string(),
                                proof_nonce,
                                metadata,
                            ];
                            let outputs = vec![proof_artifact_id];
                            tracing::info!("inputs: {:?}", inputs);
                            tracing::info!("outputs: {:?}", outputs);
                            match coordinator
                                .create_proof(CreateProofRequest {
                                    proof_id: proof.id.clone(),
                                    inputs,
                                    outputs,
                                    requester: hex::encode(proof.requester),
                                    expires_at: proof.deadline as i64,
                                })
                                .await
                            {
                                Ok(task_id) => {
                                    tracing::info!(
                                        "Created proof {} with task {}",
                                        proof.id,
                                        task_id
                                    );
                                    task_map.insert(proof.id, TaskState::Pending);
                                }
                                Err(e) => {
                                    tracing::error!("Failed to create proof: {}", e);
                                }
                            }
                        }
                        // Fail a Pending proof that vanished from the DB list. Keep every Terminal
                        // entry (it may just be on a later page) — removed only when its write lands.
                        let mut proofs_to_fail = vec![];
                        task_map.retain(|id, state| match state {
                            TaskState::Pending if !seen_set.contains(id) => {
                                tracing::warn!(
                                    "Running proof {} is no longer in cluster DB list",
                                    id
                                );
                                proofs_to_fail.push(id.clone());
                                false
                            }
                            _ => true,
                        });
                        for id in proofs_to_fail {
                            if let Err(e) =
                                coordinator.fail_proof(id.clone(), None, false, None).await
                            {
                                tracing::error!("Failed to fail expired proof: {:?}", e);
                            }
                        }

                        // Sweep all unconfirmed terminal writes (not just this page), so a lost write
                        // beyond the 1000-row page still recovers.
                        let unconfirmed: Vec<ProofRequestUpdateRequest> = task_map
                            .iter()
                            .filter_map(|e| match e.value() {
                                TaskState::Terminal(update) => Some(update.clone()),
                                TaskState::Pending => None,
                            })
                            .collect();
                        for update in unconfirmed {
                            reissue_lost_status_write(
                                &api_client,
                                &metrics,
                                &reissue_inflight,
                                &task_map,
                                update,
                            );
                        }
                    }
                    Err(e) => {
                        tracing::error!("Failed to get filtered tasks: {}", e);
                    }
                }
                tokio::select! {
                    _ = tokio::time::sleep(std::time::Duration::from_millis(500)) => {}
                    _ = token.cancelled() => break,
                }
            }
        }
    })
}
