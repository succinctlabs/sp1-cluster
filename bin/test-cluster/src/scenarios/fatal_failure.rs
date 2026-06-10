use std::time::Duration;

use anyhow::Context;
use sp1_sdk::SP1ProofMode;

use crate::assert::wait_proof_status;
use crate::cluster::Cluster;
use crate::programs;
use crate::request::request_with_cycle_limit;
use crate::scenario::{Flavors, Scenario, ScenarioFuture};
use sp1_cluster_common::proto::ProofRequestStatus;

pub fn scenario() -> Scenario {
    Scenario {
        name: "fatal-failure",
        flavors: Flavors::Both,
        timeout: Duration::from_secs(45 * 60),
        skip_in_full: false,
        run: || -> ScenarioFuture { Box::pin(run()) },
    }
}

/// A fatal failure on the PROVING path (the executor-path variant lives in execute-only).
/// A panicking program is still provable (a proof of a failed run), so the trigger here is
/// a cycle limit below the program's real cycle count: execution fails with
/// ExceededCycleLimit, which must mark the proof request Failed with failure metadata.
async fn run() -> anyhow::Result<()> {
    let cluster = Cluster::standard().start().await?;
    let api = cluster.api_client().await?;

    // The committed fibonacci input executes in ~18k cycles; cap far below that.
    let proof_id = request_with_cycle_limit(
        &cluster.gateway_rpc_url(),
        programs::FIBONACCI_ELF.clone(),
        programs::FIBONACCI_STDIN.clone(),
        SP1ProofMode::Compressed,
        Some(1_000),
    )
    .await?;
    tracing::info!("submitted {proof_id} (cycle_limit=1000 => ExceededCycleLimit)");

    let pr = wait_proof_status(
        &api,
        &proof_id,
        ProofRequestStatus::Failed,
        Duration::from_secs(15 * 60),
    )
    .await?;
    let extra_data = pr
        .extra_data
        .as_ref()
        .context("failed proof has no extra_data (failure metadata missing)")?;
    anyhow::ensure!(
        !extra_data.is_empty(),
        "failed proof has empty failure metadata"
    );
    tracing::info!("proof failed fatally with metadata: {extra_data}");

    cluster.shutdown().await;
    Ok(())
}
