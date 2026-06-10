use std::time::Duration;

use sp1_sdk::SP1ProofMode;

use crate::assert::{assert_proof_artifact_downloadable, wait_proof_status, wait_stats};
use crate::cluster::Cluster;
use crate::request::request_only;
use crate::scenario::{Flavors, Scenario, ScenarioFuture};
use crate::scenarios::long_program;
use sp1_cluster_common::proto::ProofRequestStatus;

pub fn scenario() -> Scenario {
    Scenario {
        name: "worker-death-requeue",
        flavors: Flavors::Both,
        timeout: Duration::from_secs(90 * 60),
        skip_in_full: false,
        run: || -> ScenarioFuture { Box::pin(run()) },
    }
}

/// Kill the CPU worker that owns active work, verify heartbeat cleanup removes it and the
/// work is requeued, then bring a worker back and verify the proof still completes.
///
/// Deterministic single-victim design: exactly one CPU node, so the killed node is
/// guaranteed to own the controller task. The heartbeat timeout is shortened to 5s so the
/// requeue path doesn't idle for the prod default of 30s.
async fn run() -> anyhow::Result<()> {
    let gpu_nodes = if cfg!(feature = "gpu") { 1 } else { 0 };
    let mut cluster = Cluster::builder()
        .cpu_nodes(1)
        .gpu_nodes(gpu_nodes)
        .worker_heartbeat_timeout_secs(5)
        .start()
        .await?;
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
    tracing::info!("submitted {proof_id}");

    // Wait until the proof's work is actually running on the victim.
    wait_stats(
        &mut coordinator,
        "active task on the cpu worker",
        Duration::from_secs(5 * 60),
        |s| s.active_tasks > 0,
    )
    .await?;

    cluster.kill("cpu-node-0")?;

    // Heartbeat cleanup (5s timeout) must notice the dead worker.
    wait_stats(
        &mut coordinator,
        "dead cpu worker removed from stats",
        Duration::from_secs(60),
        |s| s.cpu_workers == 0,
    )
    .await?;
    tracing::info!("dead worker removed; restarting it");

    // Bring a CPU worker back; the requeued work must complete on it.
    cluster.restart("cpu-node-0").await?;
    wait_stats(
        &mut coordinator,
        "cpu worker re-registered",
        Duration::from_secs(10 * 60),
        |s| s.cpu_workers >= 1,
    )
    .await?;

    let pr = wait_proof_status(
        &api,
        &proof_id,
        ProofRequestStatus::Completed,
        Duration::from_secs(60 * 60),
    )
    .await?;
    assert_proof_artifact_downloadable(&pr, &cluster.artifact_client()).await?;

    cluster.shutdown().await;
    Ok(())
}
