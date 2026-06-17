use std::time::Duration;

use sp1_sdk::SP1ProofMode;

use crate::assert::{assert_proof_completed, wait_stats};
use crate::cluster::Cluster;
use crate::programs;
use crate::request::request_only;
use crate::scenario::{Scenario, ScenarioFuture, Tier};

pub fn scenario() -> Scenario {
    Scenario {
        name: "api-outage",
        timeout: Duration::from_mins(20),
        tier: Tier::Full,
        run: || -> ScenarioFuture { Box::pin(run()) },
    }
}

/// Interrupt API writes while a proof is in flight: stop the API mid-proving, keep it down
/// long enough for the coordinator's status writes to fail, restart it (postgres persists),
/// and verify the terminal status is eventually written — the lost-terminal-write recovery
/// path (#126: the claimer re-issues unconfirmed terminal writes).
async fn run() -> anyhow::Result<()> {
    let mut cluster = Cluster::standard().start().await?;
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
    tracing::info!("proof in flight; taking the api down");

    cluster.stop("api").await?;

    // Outage window: Until the proof has fully drained from the coordinator,
    // so that we know that the final status update failed.
    wait_stats(
        &mut coordinator,
        "proof drained while the api is down",
        Duration::from_mins(15),
        |s| s.active_proofs == 0 && s.active_tasks == 0,
    )
    .await?;
    tracing::info!("proof drained with the api down; its terminal write is now lost");

    cluster.restart("api").await?;
    crate::utils::wait_for_tcp(&cluster.addrs.api_grpc, "restarted api").await?;
    tracing::info!("api restored");

    // The terminal write was lost while the API was down and the status task never retries, so
    // the only path left to a terminal status is the claimer re-issuing the unconfirmed write
    // (#126, swept every 500ms). Reaching Completed therefore proves that recovery path ran.
    let api = cluster.api_client().await?;
    assert_proof_completed(
        &api,
        &proof_id,
        Duration::from_mins(3),
        &cluster.artifact_client(),
    )
    .await?;

    cluster.shutdown().await;
    Ok(())
}
