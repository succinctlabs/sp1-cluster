use std::time::Duration;

use sp1_sdk::SP1ProofMode;

use crate::assert::{assert_status_holds, get_proof_request, wait_proof_status, wait_stats};
use crate::cluster::Cluster;
use crate::programs;
use crate::request::request_only;
use crate::scenario::{Scenario, ScenarioFuture, Tier};
use sp1_cluster_common::proto::{
    CancelProofRequest, ProofRequestCancelRequest, ProofRequestStatus,
};

pub fn scenarios() -> Vec<Scenario> {
    vec![
        Scenario {
            name: "cancel-pending",
            timeout: Duration::from_mins(10),
            tier: Tier::Full,
            run: || -> ScenarioFuture { Box::pin(run_pending()) },
        },
        Scenario {
            name: "cancel-active",
            timeout: Duration::from_mins(10),
            tier: Tier::Full,
            run: || -> ScenarioFuture { Box::pin(run_active()) },
        },
    ]
}

/// Cancel a proof that can never start: the cluster has NO workers, so the request sits
/// pending/claimed with a queued task. Cancelling through the API must persist Cancelled,
/// and the status must stay Cancelled.
async fn run_pending() -> anyhow::Result<()> {
    // No nodes at all.
    let cluster = Cluster::builder().start().await?;
    let api = cluster.api_client().await?;
    let mut coordinator = cluster.coordinator_client().await?;

    let proof_id = request_only(
        &cluster.gateway_rpc_url(),
        programs::FIBONACCI_ELF.clone(),
        programs::FIBONACCI_STDIN.clone(),
        SP1ProofMode::Compressed,
    )
    .await?;

    // The coordinator claims it and queues a task no worker can take.
    wait_stats(
        &mut coordinator,
        "request claimed into the queue",
        Duration::from_mins(2),
        |s| s.active_proofs > 0,
    )
    .await?;
    let pr = get_proof_request(&api, &proof_id).await?;
    anyhow::ensure!(
        pr.proof_status() == ProofRequestStatus::Pending,
        "expected Pending before cancel, got {:?}",
        pr.proof_status()
    );

    cancel_everywhere(&api, &mut coordinator, &proof_id).await?;

    wait_proof_status(
        &api,
        &proof_id,
        ProofRequestStatus::Cancelled,
        Duration::from_mins(1),
    )
    .await?;

    // The coordinator must drop the queued work.
    wait_stats(
        &mut coordinator,
        "queued work dropped after cancel",
        Duration::from_mins(2),
        |s| s.active_proofs == 0 && s.active_tasks == 0,
    )
    .await?;

    // Status must STAY Cancelled (no late write flips it).
    assert_status_holds(
        &api,
        &proof_id,
        ProofRequestStatus::Cancelled,
        Duration::from_secs(5),
    )
    .await?;

    cluster.shutdown().await;
    Ok(())
}

/// Cancel a proof while a worker is actively proving it. Mirrors the production flow
/// (the fulfiller cancels both the API row and the coordinator): workers must drop the
/// work, the queue must drain, and the API must stay Cancelled.
async fn run_active() -> anyhow::Result<()> {
    let cluster = Cluster::standard().start().await?;
    let api = cluster.api_client().await?;
    let mut coordinator = cluster.coordinator_client().await?;

    let proof_id = request_only(
        &cluster.gateway_rpc_url(),
        programs::RSP_ELF.clone(),
        programs::RSP_STDIN.clone(),
        SP1ProofMode::Compressed,
    )
    .await?;

    wait_stats(
        &mut coordinator,
        "proof actively running",
        Duration::from_mins(5),
        |s| s.active_tasks > 0,
    )
    .await?;

    cancel_everywhere(&api, &mut coordinator, &proof_id).await?;

    wait_proof_status(
        &api,
        &proof_id,
        ProofRequestStatus::Cancelled,
        Duration::from_mins(1),
    )
    .await?;

    // Workers must abandon the proof: no active work left for it.
    wait_stats(
        &mut coordinator,
        "active work drained after cancel",
        Duration::from_mins(5),
        |s| s.active_proofs == 0 && s.active_tasks == 0,
    )
    .await?;

    // And the terminal status must hold.
    assert_status_holds(
        &api,
        &proof_id,
        ProofRequestStatus::Cancelled,
        Duration::from_secs(10),
    )
    .await?;

    cluster.shutdown().await;
    Ok(())
}

/// Cancel in both places, like the production fulfiller: the API row (user-facing state)
/// and the coordinator (stops scheduled/active tasks).
async fn cancel_everywhere(
    api: &sp1_cluster_common::client::ClusterServiceClient,
    coordinator: &mut sp1_cluster_common::proto::worker_service_client::WorkerServiceClient<
        tonic::transport::Channel,
    >,
    proof_id: &str,
) -> anyhow::Result<()> {
    api.cancel_proof_request(ProofRequestCancelRequest {
        proof_id: proof_id.to_string(),
    })
    .await
    .map_err(|e| anyhow::anyhow!("api cancel failed: {e}"))?;
    coordinator
        .cancel_proof(CancelProofRequest {
            worker_id: "test-cluster".to_string(),
            proof_id: proof_id.to_string(),
        })
        .await?;
    tracing::info!("cancelled {proof_id} in api + coordinator");
    Ok(())
}
