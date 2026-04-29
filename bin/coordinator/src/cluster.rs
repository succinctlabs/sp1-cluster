use std::{collections::HashSet, sync::Arc, time::SystemTime};

use crate::{AssignmentPolicy, Coordinator, ProofResult};
use dashmap::DashMap;
use hex;
use sp1_cluster_common::{
    client::ClusterServiceClient,
    proto::{
        events::SubscribeProofEventsRequest, CreateProofRequest, ProofRequest,
        ProofRequestGetRequest, ProofRequestListRequest, ProofRequestStatus,
        ProofRequestUpdateRequest,
    },
};
use tokio::{sync::mpsc, task::JoinHandle, time::Duration};

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

/// How often we re-list proof_requests as a safety net for missed NOTIFY events.
///
/// PgListener auto-reconnects on connection drops, but events emitted while
/// the connection was down are lost. The catch-up at startup uses the same
/// query, so a coordinator restart inherits everything pending. 1s feels
/// snappy and an indexed `(handled, deadline, proof_status)` SELECT is
/// effectively free at our scale; the dominant pickup path is still the
/// event stream and only flaps fall back here.
const SAFETY_NET_INTERVAL: Duration = Duration::from_secs(1);

/// Drive proof claims from a stream of `proof_event` NOTIFYs + a slow
/// safety-net poll. Replaces the previous tight 500ms poll loop — new
/// requests now hit the coordinator within ms instead of 0–500ms.
pub fn spawn_proof_claimer_task<P: AssignmentPolicy>(
    api_client: Arc<ClusterServiceClient>,
    coordinator: Arc<Coordinator<P>>,
    task_map: Arc<DashMap<String, ProofRequestStatus>>,
) -> JoinHandle<()> {
    tokio::task::spawn(async move {
        // Initial catch-up: claim any pending requests that already exist
        // (coordinator restart / pre-existing rows that won't fire NOTIFY).
        reconcile_pending(&api_client, &coordinator, &task_map).await;

        // Run the event-driven and safety-net loops concurrently. Either
        // one encountering a transient error logs and retries — we don't
        // want a stream blip to lose us all subsequent claims.
        let stream_loop =
            run_event_stream(api_client.clone(), coordinator.clone(), task_map.clone());
        let safety_loop = run_safety_net_poll(api_client, coordinator, task_map);

        tokio::join!(stream_loop, safety_loop);
    })
}

async fn run_event_stream<P: AssignmentPolicy>(
    api_client: Arc<ClusterServiceClient>,
    coordinator: Arc<Coordinator<P>>,
    task_map: Arc<DashMap<String, ProofRequestStatus>>,
) {
    let pending_status: i32 = ProofRequestStatus::Pending.into();
    let mut backoff = Duration::from_millis(200);
    loop {
        let mut events = api_client.events.clone();
        let stream = match events
            .subscribe_proof_events(SubscribeProofEventsRequest::default())
            .await
        {
            Ok(resp) => resp.into_inner(),
            Err(e) => {
                tracing::warn!(
                    "subscribe_proof_events failed: {e}; retrying in {:?}",
                    backoff
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(10));
                continue;
            }
        };
        backoff = Duration::from_millis(200);

        let mut stream = stream;
        loop {
            match stream.message().await {
                Ok(Some(event)) => {
                    // Only react to fresh PENDING rows; status transitions
                    // (Failed/Completed/Cancelled) don't need a coordinator
                    // claim. The safety-net loop reconciles task_map for
                    // out-of-band deletes / cancels.
                    if event.proof_status != pending_status || event.handled {
                        continue;
                    }
                    if task_map.contains_key(&event.proof_id) {
                        continue;
                    }
                    claim_one(&api_client, &coordinator, &task_map, &event.proof_id).await;
                }
                Ok(None) => {
                    tracing::warn!("proof_event stream closed; reconnecting");
                    break;
                }
                Err(e) => {
                    tracing::warn!("proof_event stream error: {e}; reconnecting");
                    break;
                }
            }
        }
    }
}

async fn run_safety_net_poll<P: AssignmentPolicy>(
    api_client: Arc<ClusterServiceClient>,
    coordinator: Arc<Coordinator<P>>,
    task_map: Arc<DashMap<String, ProofRequestStatus>>,
) {
    loop {
        tokio::time::sleep(SAFETY_NET_INTERVAL).await;
        reconcile_pending(&api_client, &coordinator, &task_map).await;
    }
}

/// Event-path: fetch a single proof_request by id and dispatch if new.
/// Cheaper than the safety-net's full list — at high event rates the load
/// stays O(1) per event instead of O(N) per event.
async fn claim_one<P: AssignmentPolicy>(
    api_client: &ClusterServiceClient,
    coordinator: &Arc<Coordinator<P>>,
    task_map: &Arc<DashMap<String, ProofRequestStatus>>,
    proof_id: &str,
) {
    let proof = match api_client
        .get_proof_request(ProofRequestGetRequest {
            proof_id: proof_id.to_string(),
        })
        .await
    {
        Ok(Some(proof)) => proof,
        Ok(None) => {
            tracing::warn!("event for unknown proof {proof_id}");
            return;
        }
        Err(e) => {
            tracing::error!("Failed to fetch proof {proof_id}: {e}");
            return;
        }
    };

    // The row may have transitioned between NOTIFY and our SELECT. Re-check
    // before claiming so a Cancelled row doesn't get scheduled.
    if proof.proof_status != i32::from(ProofRequestStatus::Pending) || proof.handled {
        return;
    }

    dispatch_proof(coordinator, task_map, proof).await;
}

/// Safety-net path: full list of pending proof_requests, dispatch any new
/// ones, and reconcile `task_map` against the DB. Catches missed events
/// (PG NOTIFY drops on listener reconnect) and out-of-band cancellations.
async fn reconcile_pending<P: AssignmentPolicy>(
    api_client: &ClusterServiceClient,
    coordinator: &Arc<Coordinator<P>>,
    task_map: &Arc<DashMap<String, ProofRequestStatus>>,
) {
    let response = match api_client
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
        Ok(response) => response,
        Err(e) => {
            tracing::error!("Failed to list pending proofs: {}", e);
            return;
        }
    };

    let mut seen_set = HashSet::new();
    for proof in response.into_iter().rev() {
        seen_set.insert(proof.id.clone());
        if task_map.contains_key(&proof.id) {
            continue;
        }
        dispatch_proof(coordinator, task_map, proof).await;
    }

    // Reconcile: drop entries that the cluster DB no longer lists (cancelled
    // out-of-band etc.) so the worker pipeline doesn't keep spinning on them.
    let mut proofs_to_fail = vec![];
    task_map.retain(|id, status| {
        let was_seen = seen_set.contains(id);
        if !was_seen && *status == ProofRequestStatus::Pending {
            tracing::warn!("Running proof {} is no longer in cluster DB list", id);
            proofs_to_fail.push(id.clone());
        }
        was_seen
    });
    for id in proofs_to_fail {
        if let Err(e) = coordinator.fail_proof(id.clone(), None, false, None).await {
            tracing::error!("Failed to fail expired proof: {:?}", e);
        }
    }
}

/// Build a CreateProofRequest from a DB row and hand it to the coordinator.
/// Skips and logs if any required artifact id is missing — same defensive
/// posture the old polling loop had.
async fn dispatch_proof<P: AssignmentPolicy>(
    coordinator: &Arc<Coordinator<P>>,
    task_map: &Arc<DashMap<String, ProofRequestStatus>>,
    proof: ProofRequest,
) {
    let Some(options_artifact_id) = proof.options_artifact_id else {
        tracing::error!("Options artifact ID not found for proof {}", proof.id);
        return;
    };
    let Some(cycle_limit) = proof.cycle_limit else {
        tracing::error!("Cycle limit not found for proof {}", proof.id);
        return;
    };
    let Some(proof_artifact_id) = proof.proof_artifact_id else {
        tracing::error!("Proof artifact ID not found for proof {}", proof.id);
        return;
    };
    let inputs = vec![
        proof.program_artifact_id,
        proof.stdin_artifact_id,
        options_artifact_id,
        cycle_limit.to_string(),
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
            tracing::info!("Created proof {} with task {}", proof.id, task_id);
            task_map.insert(proof.id, ProofRequestStatus::Pending);
        }
        Err(e) => {
            tracing::error!("Failed to create proof: {}", e);
        }
    }
}
