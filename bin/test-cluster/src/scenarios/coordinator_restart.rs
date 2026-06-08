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
        timeout: Duration::from_mins(10),
        tier: Tier::Full,
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
    assert_proof_completed(
        &api,
        &proof_a,
        Duration::from_mins(30),
        &cluster.artifact_client(),
    )
    .await?;
    tracing::info!("proof A completed; killing coordinator");

    cluster.kill("coordinator")?;
    cluster.restart("coordinator").await?;
    crate::utils::wait_for_tcp(&cluster.addrs.coordinator, "restarted coordinator").await?;

    // The workers' streams died with the old coordinator; roll them too. The cluster
    // knows whether a gpu node was actually spawned — no re-deriving the build flavor.
    let has_gpu = cluster.has_component("gpu-node-0");
    cluster.restart("cpu-node-0").await?;
    if has_gpu {
        cluster.restart("gpu-node-0").await?;
    }
    let mut coordinator = cluster.coordinator_client().await?;
    wait_stats(
        &mut coordinator,
        "workers re-registered with restarted coordinator",
        Duration::from_mins(15),
        |s| s.cpu_workers >= 1 && (!has_gpu || s.gpu_workers >= 1),
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
    assert_proof_completed(
        &api,
        &proof_b,
        Duration::from_mins(30),
        &cluster.artifact_client(),
    )
    .await?;
    tracing::info!("proof B completed on restarted coordinator");

    cluster.shutdown().await;
    Ok(())
}
