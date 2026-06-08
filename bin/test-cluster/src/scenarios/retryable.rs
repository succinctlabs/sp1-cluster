use std::time::Duration;

use sp1_sdk::SP1ProofMode;

use crate::assert::assert_proof_completed;
use crate::cluster::Cluster;
use crate::programs;
use crate::request::request_only;
use crate::scenario::{Scenario, ScenarioFuture, Tier};

pub fn scenario() -> Scenario {
    Scenario {
        name: "retryable-then-success",
        timeout: Duration::from_mins(10),
        tier: Tier::Full,
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

    assert_proof_completed(
        &api,
        &proof_id,
        Duration::from_mins(30),
        &cluster.artifact_client(),
    )
    .await?;
    tracing::info!("proof completed despite injected retryable failure");

    cluster.shutdown().await;
    Ok(())
}
