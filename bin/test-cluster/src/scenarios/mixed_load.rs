use std::time::Duration;

use sp1_sdk::SP1ProofMode;

use crate::assert::assert_proof_completed;
use crate::cluster::Cluster;
use crate::programs;
use crate::request::request_only;
use crate::scenario::{Scenario, ScenarioFuture, Tier};

pub fn scenario() -> Scenario {
    Scenario {
        name: "mixed-load",
        timeout: Duration::from_mins(20),
        tier: Tier::Full,
        run: || -> ScenarioFuture { Box::pin(run()) },
    }
}

/// Mixed small and large proofs back-to-back while workers are already busy: one long
/// request first, then four small fib requests immediately after. Every request must
/// reach Completed within the per-request deadline — no starvation of the small ones
/// behind the long one or vice versa.
async fn run() -> anyhow::Result<()> {
    let cluster = Cluster::standard().start().await?;
    let api = cluster.api_client().await?;

    let mut proof_ids = Vec::new();
    proof_ids.push(
        request_only(
            &cluster.gateway_rpc_url(),
            programs::RSP_ELF.clone(),
            programs::RSP_STDIN.clone(),
            SP1ProofMode::Compressed,
        )
        .await?,
    );
    for _ in 0..4 {
        proof_ids.push(
            request_only(
                &cluster.gateway_rpc_url(),
                programs::FIBONACCI_ELF.clone(),
                programs::FIBONACCI_STDIN.clone(),
                SP1ProofMode::Compressed,
            )
            .await?,
        );
    }
    tracing::info!("submitted {} requests back-to-back", proof_ids.len());

    for proof_id in &proof_ids {
        assert_proof_completed(
            &api,
            proof_id,
            Duration::from_hours(1),
            &cluster.artifact_client(),
        )
        .await?;
        tracing::info!("{proof_id} completed");
    }

    cluster.shutdown().await;
    Ok(())
}
