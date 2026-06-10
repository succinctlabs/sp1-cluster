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
        name: "worker-restart",
        timeout: Duration::from_secs(60 * 60),
        skip_in_full: false,
        run: || -> ScenarioFuture { Box::pin(run()) },
    }
}

/// Graceful worker restart (Close/drain path — no heartbeat wait): proof A completes, the
/// CPU node is stopped gracefully and restarted, then proof B completes on the restarted
/// worker. Mid-task death is covered by worker-death-requeue; this pins the clean
/// drain/re-register path used by rolling worker deploys.
async fn run() -> anyhow::Result<()> {
    let mut cluster = Cluster::standard().start().await?;
    let api = cluster.api_client().await?;
    let mut coordinator = cluster.coordinator_client().await?;

    let proof_a = request_only(
        &cluster.gateway_rpc_url(),
        programs::FIBONACCI_ELF.clone(),
        programs::FIBONACCI_STDIN.clone(),
        SP1ProofMode::Compressed,
    )
    .await?;
    wait_proof_status(
        &api,
        &proof_a,
        ProofRequestStatus::Completed,
        Duration::from_secs(30 * 60),
    )
    .await?;
    tracing::info!("proof A completed; restarting cpu worker gracefully");

    cluster.stop("cpu-node-0").await?;
    wait_stats(
        &mut coordinator,
        "stopped worker deregistered",
        Duration::from_secs(2 * 60),
        |s| s.cpu_workers == 0,
    )
    .await?;
    cluster.restart("cpu-node-0").await?;
    wait_stats(
        &mut coordinator,
        "worker re-registered",
        Duration::from_secs(10 * 60),
        |s| s.cpu_workers >= 1,
    )
    .await?;

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
        Duration::from_secs(30 * 60),
    )
    .await?;
    assert_proof_artifact_downloadable(&pr, &cluster.artifact_client()).await?;

    cluster.shutdown().await;
    Ok(())
}
