use std::time::Duration;

use sp1_sdk::SP1ProofMode;

use crate::assert::{assert_proof_artifact_downloadable, wait_proof_status};
use crate::cluster::Cluster;
use crate::programs;
use crate::request::request_only;
use crate::scenario::{Scenario, ScenarioFuture};
use crate::scenarios::long_program;
use sp1_cluster_common::proto::ProofRequestStatus;

pub fn scenario() -> Scenario {
    Scenario {
        name: "mixed-load",
        cpu_timeout: Duration::from_mins(90),
        gpu_timeout: Duration::from_mins(20),
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

    let (long_elf, long_stdin) = long_program();
    let mut proof_ids = Vec::new();
    proof_ids.push(
        request_only(
            &cluster.gateway_rpc_url(),
            long_elf,
            long_stdin,
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
        let pr = wait_proof_status(
            &api,
            proof_id,
            ProofRequestStatus::Completed,
            Duration::from_hours(1),
        )
        .await?;
        assert_proof_artifact_downloadable(&pr, &cluster.artifact_client()).await?;
        tracing::info!("{proof_id} completed");
    }

    cluster.shutdown().await;
    Ok(())
}
