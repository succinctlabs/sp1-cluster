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

/// For a row the DB still reports PENDING: if we already hold a *terminal* status (Completed/Failed)
/// in memory, the original completion write was lost (e.g. a cluster-DB outage) — return it to
/// re-write. Otherwise (still proving) `None`.
fn lost_status_write(in_memory: ProofRequestStatus) -> Option<ProofRequestStatus> {
    match in_memory {
        ProofRequestStatus::Completed | ProofRequestStatus::Failed => Some(in_memory),
        _ => None,
    }
}

/// Re-write a proof's lost terminal status to the cluster API. Spawned fire-and-forget so a slow
/// write never stalls the claimer loop, and deduped via `inflight` so a still-PENDING row on the
/// next poll doesn't start a second write while the first runs.
fn reissue_lost_status_write(
    api_client: &Arc<ClusterServiceClient>,
    metrics: &Arc<CoordinatorMetrics>,
    inflight: &Arc<DashSet<String>>,
    proof_id: String,
    status: ProofRequestStatus,
) {
    if !inflight.insert(proof_id.clone()) {
        return; // a re-issue for this proof is already running
    }
    tracing::warn!("re-issuing lost {:?} status write for proof {}", status, proof_id);

    let api_client = api_client.clone();
    let metrics = metrics.clone();
    let inflight = inflight.clone();
    tokio::task::spawn(async move {
        let update = ProofRequestUpdateRequest {
            proof_id: proof_id.clone(),
            proof_status: Some(status.into()),
            ..Default::default()
        };
        match api_client.update_proof_request(update).await {
            Ok(()) => metrics.increment_reissued_status_writes(),
            Err(e) => {
                tracing::warn!("re-issue of lost status write for {} failed: {}", proof_id, e)
            }
        }
        inflight.remove(&proof_id);
    });
}

/// Spawn a task to update proof statuses with the cluster API given a channel of proof completions.
pub fn spawn_proof_status_task<P: AssignmentPolicy>(
    api_client: Arc<ClusterServiceClient>,
    task_map: Arc<DashMap<String, ProofRequestStatus>>,
    mut completed_rx: mpsc::UnboundedReceiver<ProofResult<P>>,
) -> JoinHandle<()> {
    tokio::task::spawn(async move {
        while let Some(ProofResult {
            id,
            success,
            metadata,
            extra_data,
        }) = completed_rx.recv().await
        {
            let Some(mut status) = task_map.get_mut(&id) else {
                tracing::error!("proof {} not found", id);
                continue;
            };
            if *status != ProofRequestStatus::Pending {
                tracing::error!("proof {} is in unexpected state {:?}", id, status);
            }
            *status = if success {
                ProofRequestStatus::Completed
            } else {
                ProofRequestStatus::Failed
            };
            tracing::info!("Setting proof status to {:?} for {}", *status, id);
            let status_copy = *status;
            let metadata_string = serde_json::to_string(&metadata).unwrap_or_else(|e| {
                tracing::error!("Failed to serialize metadata: {}", e);
                "null".to_string()
            });
            drop(status);

            if let Err(e) = api_client
                .update_proof_request(ProofRequestUpdateRequest {
                    proof_id: id.clone(),
                    proof_status: Some(status_copy.into()),
                    metadata: Some(metadata_string),
                    extra_data,
                    ..Default::default()
                })
                .await
            {
                tracing::error!("Failed to update proof status: {}", e);
            }
        }
    })
}

/// Spawn a task to claim proofs from the cluster API.
pub fn spawn_proof_claimer_task<P: AssignmentPolicy>(
    api_client: Arc<ClusterServiceClient>,
    coordinator: Arc<Coordinator<P>>,
    task_map: Arc<DashMap<String, ProofRequestStatus>>,
    metrics: Arc<CoordinatorMetrics>,
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
                            if let Some(in_memory) = task_map.get(&proof.id).map(|s| *s) {
                                // Tracked already. Terminal in memory but still PENDING here → its
                                // completion write was lost; re-issue it.
                                if let Some(status) = lost_status_write(in_memory) {
                                    reissue_lost_status_write(
                                        &api_client,
                                        &metrics,
                                        &reissue_inflight,
                                        proof.id.clone(),
                                        status,
                                    );
                                }
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
                                    task_map.insert(proof.id, ProofRequestStatus::Pending);
                                }
                                Err(e) => {
                                    tracing::error!("Failed to create proof: {}", e);
                                }
                            }
                        }
                        // Remove proofs from task_map that are no longer Claimed.
                        let mut proofs_to_fail = vec![];
                        task_map.retain(|id, status| {
                            let was_seen = seen_set.contains(id);
                            if !was_seen && *status == ProofRequestStatus::Pending {
                                tracing::warn!(
                                    "Running proof {} is no longer in cluster DB list",
                                    id
                                );
                                proofs_to_fail.push(id.clone());
                            }
                            was_seen
                        });
                        for id in proofs_to_fail {
                            if let Err(e) =
                                coordinator.fail_proof(id.clone(), None, false, None).await
                            {
                                tracing::error!("Failed to fail expired proof: {:?}", e);
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!("Failed to get filtered tasks: {}", e);
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
        }
    })
}
