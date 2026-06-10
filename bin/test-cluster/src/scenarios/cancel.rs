use std::time::Duration;

use sp1_sdk::SP1ProofMode;

use crate::assert::{get_proof_request, poll_until, wait_proof_status, wait_stats};
use crate::cluster::Cluster;
use crate::programs;
use crate::request::request_only;
use crate::scenario::{Scenario, ScenarioFuture};
use crate::scenarios::long_program;
use sp1_cluster_common::proto::{
    CancelProofRequest, ProofRequestCancelRequest, ProofRequestStatus,
};

pub fn scenarios() -> Vec<Scenario> {
    vec![
        Scenario {
            name: "cancel-pending",
            cpu_timeout: Duration::from_secs(20 * 60),
            gpu_timeout: Duration::from_secs(10 * 60),
            run: || -> ScenarioFuture { Box::pin(run_pending()) },
        },
        Scenario {
            name: "cancel-active",
            cpu_timeout: Duration::from_secs(45 * 60),
            gpu_timeout: Duration::from_secs(10 * 60),
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
        Duration::from_secs(2 * 60),
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

    let pr = wait_proof_status(
        &api,
        &proof_id,
        ProofRequestStatus::Cancelled,
        Duration::from_secs(60),
    )
    .await?;
    anyhow::ensure!(pr.proof_status() == ProofRequestStatus::Cancelled);

    // The coordinator must drop the queued work.
    wait_stats(
        &mut coordinator,
        "queued work dropped after cancel",
        Duration::from_secs(2 * 60),
        |s| s.active_proofs == 0 && s.active_tasks == 0,
    )
    .await?;

    // Status must STAY Cancelled (no late write flips it).
    tokio::time::sleep(Duration::from_secs(5)).await;
    let pr = get_proof_request(&api, &proof_id).await?;
    anyhow::ensure!(
        pr.proof_status() == ProofRequestStatus::Cancelled,
        "status flipped away from Cancelled: {:?}",
        pr.proof_status()
    );

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

    let (elf, stdin) = long_program();
    let proof_id = request_only(
        &cluster.gateway_rpc_url(),
        elf,
        stdin,
        SP1ProofMode::Compressed,
    )
    .await?;

    wait_stats(
        &mut coordinator,
        "proof actively running",
        Duration::from_secs(5 * 60),
        |s| s.active_tasks > 0,
    )
    .await?;

    cancel_everywhere(&api, &mut coordinator, &proof_id).await?;

    wait_proof_status(
        &api,
        &proof_id,
        ProofRequestStatus::Cancelled,
        Duration::from_secs(60),
    )
    .await?;

    // Workers must abandon the proof: no active work left for it.
    wait_stats(
        &mut coordinator,
        "active work drained after cancel",
        Duration::from_secs(5 * 60),
        |s| s.active_proofs == 0 && s.active_tasks == 0,
    )
    .await?;

    // And the terminal status must hold.
    poll_until(
        "status stays Cancelled",
        Duration::from_secs(10),
        || async {
            let pr = get_proof_request(&api, &proof_id).await?;
            anyhow::ensure!(
                pr.proof_status() == ProofRequestStatus::Cancelled,
                "status flipped away from Cancelled: {:?}",
                pr.proof_status()
            );
            // Poll the full window; only time out ending the check successfully.
            Ok(None::<()>)
        },
    )
    .await
    .err()
    .filter(|e| !e.to_string().contains("timed out"))
    .map_or(Ok(()), Err)?;

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
