use std::time::Duration;

use sp1_sdk::SP1ProofMode;

use crate::assert::{assert_proof_artifact_downloadable, wait_proof_status, wait_stats};
use crate::cluster::Cluster;
use crate::request::request_only;
use crate::scenario::{Scenario, ScenarioFuture};
use crate::scenarios::long_program;
use sp1_cluster_common::proto::ProofRequestStatus;

pub fn scenario() -> Scenario {
    Scenario {
        name: "api-outage",
        cpu_timeout: Duration::from_secs(90 * 60),
        gpu_timeout: Duration::from_secs(20 * 60),
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
    tracing::info!("proof in flight; taking the api down");

    cluster.stop("api").await?;
    // Outage window: long enough that in-window completions lose their terminal write.
    tokio::time::sleep(Duration::from_secs(20)).await;
    cluster.restart("api").await?;
    crate::utils::wait_for_tcp(crate::cluster::API_GRPC_ADDR, "restarted api").await?;
    tracing::info!("api restored");

    // The proof must reach Completed in the API despite the outage (either it finished
    // after restore, or the claimer re-issued the lost terminal write).
    let api = cluster.api_client().await?;
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
