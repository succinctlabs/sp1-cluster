use std::time::Duration;

use sp1_sdk::SP1ProofMode;

use crate::assert::{assert_proof_artifact_downloadable, wait_proof_status};
use crate::cluster::Cluster;
use crate::programs;
use crate::request::{submit_proof_requests_sequential, ClusterProofRequest, ClusterProofRequests};
use crate::scenario::{Flavors, Scenario, ScenarioFuture};
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
            flavors: Flavors::Both,
            timeout: Duration::from_secs(30 * 60),
            skip_in_full: false,
            run: || -> ScenarioFuture { Box::pin(run(SP1ProofMode::Core)) },
        },
        Scenario {
            name: "proof-mode-compressed",
            flavors: Flavors::Both,
            timeout: Duration::from_secs(30 * 60),
            skip_in_full: false,
            run: || -> ScenarioFuture { Box::pin(run(SP1ProofMode::Compressed)) },
        },
        Scenario {
            name: "proof-mode-plonk",
            flavors: Flavors::Both,
            timeout: Duration::from_secs(2 * 60 * 60),
            // The plonk wrap wedges silently on the g6.4xlarge runner (zero log output
            // for 2h after ShrinkWrap completes; gnark plonk SRS/pk load on 64GB RAM).
            // groth16 on the same finalize path takes ~112s. Excluded from `suite full`
            // until the prover-side hang is understood or the runner gets more RAM;
            // still runnable manually via `run proof-mode-plonk`.
            skip_in_full: true,
            run: || -> ScenarioFuture { Box::pin(run(SP1ProofMode::Plonk)) },
        },
        Scenario {
            name: "proof-mode-groth16",
            flavors: Flavors::Both,
            timeout: Duration::from_secs(60 * 60),
            skip_in_full: false,
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
