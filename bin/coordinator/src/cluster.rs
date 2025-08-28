use std::{collections::HashSet, sync::Arc, time::SystemTime};

use crate::{AssignmentPolicy, Coordinator, ProofResult};
use dashmap::DashMap;
use hex;
use sp1_cluster_common::{
    client::ClusterServiceClient,
    proto::{
        CreateProofRequest, ProofRequestListRequest, ProofRequestStatus, ProofRequestUpdateRequest,
    },
};
use tokio::{sync::mpsc, task::JoinHandle};

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
) -> JoinHandle<()> {
    tokio::task::spawn({
        async move {
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
                            // TODO: could bulk create
                            let inputs = vec![
                                proof.program_artifact_id,
                                proof.stdin_artifact_id,
                                options_artifact_id,
                                cycle_limit.to_string(),
                                "".to_string(), // TODO
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
                            if let Err(e) = coordinator.fail_proof(id.clone(), None, false).await {
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
