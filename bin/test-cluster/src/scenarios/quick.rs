use std::time::Duration;

use sp1_sdk::SP1ProofMode;

use crate::assert::{assert_proof_artifact_downloadable, wait_proof_status};
use crate::cluster::Cluster;
use crate::programs;
use crate::request::{submit_proof_requests_sequential, ClusterProofRequest, ClusterProofRequests};
use crate::scenario::{Scenario, ScenarioFuture};
use sp1_cluster_common::proto::ProofRequestStatus;

pub fn scenario() -> Scenario {
    Scenario {
        name: "quick",
        cpu_timeout: Duration::from_secs(45 * 60),
        gpu_timeout: Duration::from_secs(10 * 60),
        run: || -> ScenarioFuture { Box::pin(run()) },
    }
}

/// Cheap convenient run: a single fib compressed proof, verified locally. The default
/// dev loop and the cpu-only smoke proving scenario.
///
/// Exactly ONE request: the in-process GPU worker's device-memory high-water mark grows
/// per request (core + lazily-initialized recursion arenas are retained), and on a shared
/// desktop GPU a second request's ProveShard can OOM (observed: 24GB RTX 4090 with ~11GB
/// of desktop graphics — request #2 died at 23.9GB used). A single compressed request is
/// the proven-working footprint; multi-request coverage belongs to proof-modes (fresh
/// cluster per mode) and the full tier on dedicated GPUs.
async fn run() -> anyhow::Result<()> {
    let cluster = Cluster::standard().start().await?;

    let proof_ids = submit_proof_requests_sequential(ClusterProofRequests {
        rpc_url: cluster.gateway_rpc_url(),
        requests: vec![ClusterProofRequest {
            elf: programs::FIBONACCI_ELF.clone(),
            stdin: programs::FIBONACCI_STDIN.clone(),
            modes: vec![SP1ProofMode::Compressed],
        }],
    })
    .await?;

    let api = cluster.api_client().await?;
    for proof_id in &proof_ids {
        // SDK already returned the verified proof; this only waits for the coordinator's
        // async terminal-status write to land in the API. Generous bound for slow CI disks.
        let pr = wait_proof_status(
            &api,
            proof_id,
            ProofRequestStatus::Completed,
            Duration::from_secs(5 * 60),
        )
        .await?;
        assert_proof_artifact_downloadable(&pr, &cluster.artifact_client()).await?;
        tracing::info!("proof {proof_id} completed, artifact verified");
    }

    cluster.shutdown().await;
    Ok(())
}
