use std::time::Duration;

use sp1_sdk::SP1ProofMode;

use crate::assert::{assert_proof_artifact_downloadable, wait_proof_status, wait_stats};
use crate::cluster::Cluster;
use crate::programs;
use crate::request::request_only;
use crate::scenario::{Flavor, Scenario, ScenarioFuture};
use sp1_cluster_common::proto::ProofRequestStatus;

pub fn scenario() -> Scenario {
    Scenario {
        name: "multi-worker",
        timeout: Duration::from_secs(60 * 60),
        skip_in_full: false,
        run: || -> ScenarioFuture { Box::pin(run()) },
    }
}

/// Assignment under capacity: more workers than one, more concurrent requests than
/// workers. gpu flavor: 2 CPU + 1 GPU (multi-GPU gated on the memory verification, spec
/// "multi-GPU gate"), 4 requests (2 core, 2 compressed). cpu-only: 2 CPU nodes,
/// 3 core-mode requests — core only and fewer pipelines because every concurrent CPU
/// pipeline costs ~7-10GB and the sp1 executor runner kills children past a
/// machine-derived memory limit (observed at 4 concurrent pipelines on a 128GB box).
///
/// Note: the coordinator does not expose per-worker task attribution over RPC, so the
/// per-worker ">=1 task" assert from the spec is approximated by: all workers registered
/// + all requests completed.
async fn run() -> anyhow::Result<()> {
    let (cpu_nodes, gpu_nodes) = match Flavor::current() {
        Flavor::Gpu => (2, 1),
        Flavor::CpuOnly => (2, 0),
    };
    let cluster = Cluster::builder()
        .cpu_nodes(cpu_nodes)
        .gpu_nodes(gpu_nodes)
        .start()
        .await?;
    let api = cluster.api_client().await?;
    let mut coordinator = cluster.coordinator_client().await?;

    wait_stats(
        &mut coordinator,
        "all workers registered",
        Duration::from_secs(60),
        |s| s.cpu_workers >= cpu_nodes as u32 && s.gpu_workers >= gpu_nodes as u32,
    )
    .await?;

    let modes: &[SP1ProofMode] = match Flavor::current() {
        Flavor::Gpu => &[
            SP1ProofMode::Core,
            SP1ProofMode::Core,
            SP1ProofMode::Compressed,
            SP1ProofMode::Compressed,
        ],
        Flavor::CpuOnly => &[SP1ProofMode::Core, SP1ProofMode::Core, SP1ProofMode::Core],
    };
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
        let pr = wait_proof_status(
            &api,
            proof_id,
            ProofRequestStatus::Completed,
            Duration::from_secs(30 * 60),
        )
        .await?;
        assert_proof_artifact_downloadable(&pr, &cluster.artifact_client()).await?;
    }

    cluster.shutdown().await;
    Ok(())
}
