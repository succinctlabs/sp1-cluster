use std::time::Duration;

use sp1_sdk::SP1ProofMode;

use crate::assert::{assert_proof_completed, wait_stats};
use crate::cluster::Cluster;
use crate::programs;
use crate::request::request_only;
use crate::scenario::{Scenario, ScenarioFuture, Tier};

pub fn scenario() -> Scenario {
    Scenario {
        name: "coordinator-restart",
        timeout: Duration::from_mins(20),
        tier: Tier::Full,
        run: || -> ScenarioFuture { Box::pin(run()) },
    }
}

/// Crash the coordinator while a proof is active, restart it on the same port against the
/// same API, and verify that the same proof completes without manually restarting workers.
async fn run() -> anyhow::Result<()> {
    let mut cluster = Cluster::standard()
        .process_isolated_coordinator()
        .start()
        .await?;
    let api = cluster.api_client().await?;
    let mut coordinator = cluster.coordinator_client().await?;

    let proof_id = request_only(
        &cluster.gateway_rpc_url(),
        programs::RSP_ELF.clone(),
        programs::RSP_STDIN.clone(),
        SP1ProofMode::Compressed,
    )
    .await?;
    tracing::info!("submitted {proof_id}");

    let has_gpu = cluster.has_component("gpu-node-0");
    wait_stats(
        &mut coordinator,
        "proof actively running before coordinator crash",
        Duration::from_mins(5),
        |s| {
            s.active_tasks > 0
                && (!has_gpu || (s.gpu_workers >= 1 && s.gpu_utilization_current > 0))
        },
    )
    .await?;
    tracing::info!("proof {proof_id} is active; killing coordinator");

    cluster.crash_coordinator_process().await?;
    cluster.restart_coordinator_process()?;
    crate::utils::wait_for_tcp(&cluster.addrs.coordinator, "restarted coordinator").await?;

    let mut coordinator = cluster.coordinator_client().await?;
    wait_stats(
        &mut coordinator,
        "workers reconnected to restarted coordinator",
        Duration::from_mins(15),
        |s| s.cpu_workers >= 1 && (!has_gpu || s.gpu_workers >= 1),
    )
    .await?;
    assert_proof_completed(
        &api,
        &proof_id,
        Duration::from_hours(1),
        &cluster.artifact_client(),
    )
    .await?;
    wait_stats(
        &mut coordinator,
        "workers still registered after recovered proof",
        Duration::from_secs(30),
        |s| s.cpu_workers >= 1 && (!has_gpu || s.gpu_workers >= 1),
    )
    .await?;
    if cluster.root.is_cancelled() {
        anyhow::bail!("cluster cancelled during coordinator crash recovery");
    }
    tracing::info!("proof {proof_id} completed after coordinator crash");

    cluster.shutdown().await;
    Ok(())
}
