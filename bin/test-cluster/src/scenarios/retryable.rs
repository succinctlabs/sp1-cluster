use std::time::Duration;

use sp1_sdk::SP1ProofMode;

use crate::assert::{assert_proof_artifact_downloadable, wait_proof_status};
use crate::cluster::Cluster;
use crate::programs;
use crate::request::request_only;
use crate::scenario::{Scenario, ScenarioFuture};
use sp1_cluster_common::proto::ProofRequestStatus;

pub fn scenario() -> Scenario {
    Scenario {
        name: "retryable-then-success",
        cpu_timeout: Duration::from_secs(45 * 60),
        gpu_timeout: Duration::from_secs(10 * 60),
        run: || -> ScenarioFuture { Box::pin(run()) },
    }
}

/// A retryable task failure that eventually succeeds: the `test-hooks` fault injection
/// (TEST_FAIL_TASK, compiled in only for this harness) fails the FIRST Controller attempt
/// with a retryable error; the coordinator must retry it (retry budget 3) and the proof
/// must still complete. The injected failure is visible in the worker log as
/// "TEST_FAIL_TASK injected retryable failure".
async fn run() -> anyhow::Result<()> {
    // Must be set before the cluster (and its in-process workers) starts; the hook reads
    // it lazily on first task. Scenario-per-process keeps this contained.
    std::env::set_var("TEST_FAIL_TASK", "CONTROLLER:1");

    let cluster = Cluster::standard().start().await?;
    let api = cluster.api_client().await?;

    let proof_id = request_only(
        &cluster.gateway_rpc_url(),
        programs::FIBONACCI_ELF.clone(),
        programs::FIBONACCI_STDIN.clone(),
        SP1ProofMode::Compressed,
    )
    .await?;
    tracing::info!("submitted {proof_id} (first CONTROLLER attempt will fail retryably)");

    let pr = wait_proof_status(
        &api,
        &proof_id,
        ProofRequestStatus::Completed,
        Duration::from_secs(30 * 60),
    )
    .await?;
    assert_proof_artifact_downloadable(&pr, &cluster.artifact_client()).await?;
    tracing::info!("proof completed despite injected retryable failure");

    cluster.shutdown().await;
    Ok(())
}
