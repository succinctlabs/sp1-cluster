use std::time::Duration;

use sp1_sdk::SP1ProofMode;

use crate::assert::{assert_proof_artifact_downloadable, wait_proof_status};
use crate::cluster::Cluster;
use crate::programs;
use crate::request::{submit_proof_requests_sequential, ClusterProofRequest, ClusterProofRequests};
use crate::scenario::{Scenario, ScenarioFuture};
use sp1_cluster_common::proto::ProofRequestStatus;

/// One scenario per proof mode, each a single request against a fresh cluster in a fresh
/// process (the suite runner spawns one process per scenario).
///
/// Why not one scenario looping all four modes: (a) the in-process GPU worker's
/// device-memory high-water mark grows per request, OOMing on shared desktop GPUs (see
/// quick.rs); (b) components leak detached tokio tasks on token-cancel (api's spawned gRPC
/// server, coordinator periodic pollers, gateway), so a second Cluster in the same process
/// talks to zombie servers bound to the fixed ports whose backing stores are gone.
/// (b) is prod-relevant shutdown behavior to fix with the Plan-2 restart scenarios.
pub fn scenarios() -> Vec<Scenario> {
    vec![
        Scenario {
            name: "proof-mode-core",
            cpu_timeout: Duration::from_secs(30 * 60),
            gpu_timeout: Duration::from_secs(10 * 60),
            run: || -> ScenarioFuture { Box::pin(run(SP1ProofMode::Core)) },
        },
        Scenario {
            name: "proof-mode-compressed",
            cpu_timeout: Duration::from_secs(30 * 60),
            gpu_timeout: Duration::from_secs(10 * 60),
            run: || -> ScenarioFuture { Box::pin(run(SP1ProofMode::Compressed)) },
        },
        Scenario {
            name: "proof-mode-plonk",
            // gnark's plonk.Prove peaks at ~57GB scenario RSS and takes ~5min when RAM
            // is plentiful; on <64GB machines it livelocks silently (kernel reclaim /
            // Go GC thrash), which is why the full tier runs on a 128GB g6.8xlarge.
            // The gpu timeout is deliberately aggressive so a wedge fails fast with
            // logs; bump it if a healthy wrap ever brushes against it.
            cpu_timeout: Duration::from_secs(2 * 60 * 60),
            gpu_timeout: Duration::from_secs(5 * 60),
            run: || -> ScenarioFuture { Box::pin(run(SP1ProofMode::Plonk)) },
        },
        Scenario {
            name: "proof-mode-groth16",
            cpu_timeout: Duration::from_secs(60 * 60),
            gpu_timeout: Duration::from_secs(10 * 60),
            run: || -> ScenarioFuture { Box::pin(run(SP1ProofMode::Groth16)) },
        },
    ]
}

async fn run(mode: SP1ProofMode) -> anyhow::Result<()> {
    let cluster = Cluster::standard().start().await?;

    let proof_ids = submit_proof_requests_sequential(ClusterProofRequests {
        rpc_url: cluster.gateway_rpc_url(),
        requests: vec![ClusterProofRequest {
            elf: programs::FIBONACCI_ELF.clone(),
            stdin: programs::FIBONACCI_STDIN.clone(),
            modes: vec![mode],
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
    }

    cluster.shutdown().await;
    Ok(())
}
