use std::time::Duration;

use sp1_sdk::SP1ProofMode;

use crate::assert::{assert_proof_artifact_downloadable, wait_proof_status, wait_stats};
use crate::cluster::Cluster;
use crate::programs;
use crate::request::request_only;
use crate::scenario::{Scenario, ScenarioFuture};
use sp1_cluster_common::proto::ProofRequestStatus;

pub fn scenario() -> Scenario {
    Scenario {
        name: "coordinator-restart",
        cpu_timeout: Duration::from_mins(60),
        gpu_timeout: Duration::from_mins(10),
        run: || -> ScenarioFuture { Box::pin(run()) },
    }
}

/// Rolling-restart safety: complete a proof, kill the coordinator (crash semantics),
/// restart it on the same port against the same API, restart the workers (their stream to
/// the old coordinator is gone), and verify a NEW proof completes on the restarted stack.
///
/// Note: mid-flight proofs do NOT survive a coordinator crash today — the claimer only
/// claims unhandled requests, so an already-claimed in-flight request is orphaned. That
/// recovery gap is product work; this scenario pins the restart/rebind/reconnect behavior
/// that must hold for rolling restarts of an idle-ish coordinator.
async fn run() -> anyhow::Result<()> {
    let mut cluster = Cluster::standard().start().await?;
    let api = cluster.api_client().await?;

    // Proof A on the original coordinator.
    let proof_a = request_only(
        &cluster.gateway_rpc_url(),
        programs::FIBONACCI_ELF.clone(),
        programs::FIBONACCI_STDIN.clone(),
        SP1ProofMode::Compressed,
    )
    .await?;
    let pr = wait_proof_status(
        &api,
        &proof_a,
        ProofRequestStatus::Completed,
        Duration::from_mins(30),
    )
    .await?;
    assert_proof_artifact_downloadable(&pr, &cluster.artifact_client()).await?;
    tracing::info!("proof A completed; killing coordinator");

    cluster.kill("coordinator")?;
    cluster.restart("coordinator").await?;
    crate::utils::wait_for_tcp(crate::cluster::COORDINATOR_ADDR, "restarted coordinator").await?;

    // The workers' streams died with the old coordinator; roll them too.
    let gpu_nodes = if cfg!(feature = "gpu") { 1 } else { 0 };
    cluster.restart("cpu-node-0").await?;
    if gpu_nodes > 0 {
        cluster.restart("gpu-node-0").await?;
    }
    let mut coordinator = cluster.coordinator_client().await?;
    wait_stats(
        &mut coordinator,
        "workers re-registered with restarted coordinator",
        Duration::from_mins(15),
        |s| s.cpu_workers >= 1 && s.gpu_workers >= gpu_nodes,
    )
    .await?;

    // Proof B on the restarted stack.
    let proof_b = request_only(
        &cluster.gateway_rpc_url(),
        programs::FIBONACCI_ELF.clone(),
        programs::FIBONACCI_STDIN.clone(),
        SP1ProofMode::Compressed,
    )
    .await?;
    let pr = wait_proof_status(
        &api,
        &proof_b,
        ProofRequestStatus::Completed,
        Duration::from_mins(30),
    )
    .await?;
    assert_proof_artifact_downloadable(&pr, &cluster.artifact_client()).await?;
    tracing::info!("proof B completed on restarted coordinator");

    cluster.shutdown().await;
    Ok(())
}
