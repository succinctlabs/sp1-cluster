use std::time::Duration;

use sp1_sdk::SP1ProofMode;

use crate::assert::{assert_proof_completed, wait_stats};
use crate::cluster::Cluster;
use crate::programs;
use crate::request::request_only;
use crate::scenario::{Scenario, ScenarioFuture, Tier};

pub fn scenario() -> Scenario {
    Scenario {
        name: "multi-worker",
        timeout: Duration::from_mins(10),
        tier: Tier::Full,
        run: || -> ScenarioFuture { Box::pin(run()) },
    }
}

/// Assignment under capacity: more workers than one, more concurrent requests than
/// workers — 2 CPU + 1 GPU nodes (multi-GPU gated on the memory verification, spec
/// "multi-GPU gate"), 4 requests (2 core, 2 compressed).
///
/// Note: the coordinator does not expose per-worker task attribution over RPC, so the
/// per-worker ">=1 task" assert from the spec is approximated by: all workers registered
/// + all requests completed.
async fn run() -> anyhow::Result<()> {
    let cluster = Cluster::builder().cpu_nodes(2).gpu_nodes(1).start().await?;
    let api = cluster.api_client().await?;
    let mut coordinator = cluster.coordinator_client().await?;

    wait_stats(
        &mut coordinator,
        "all workers registered",
        Duration::from_mins(1),
        |s| s.cpu_workers >= 2 && s.gpu_workers >= 1,
    )
    .await?;

    let modes: &[SP1ProofMode] = &[
        SP1ProofMode::Core,
        SP1ProofMode::Core,
        SP1ProofMode::Compressed,
        SP1ProofMode::Compressed,
    ];
    let mut proof_ids = Vec::new();
    for &mode in modes {
        let proof_id = request_only(
            &cluster.gateway_rpc_url(),
            programs::FIBONACCI_ELF.clone(),
            programs::FIBONACCI_STDIN.clone(),
            mode,
        )
        .await?;
        tracing::info!("submitted {proof_id} ({mode:?})");
        proof_ids.push(proof_id);
    }

    for proof_id in &proof_ids {
        assert_proof_completed(
            &api,
            proof_id,
            Duration::from_mins(30),
            &cluster.artifact_client(),
        )
        .await?;
    }

    cluster.shutdown().await;
    Ok(())
}
