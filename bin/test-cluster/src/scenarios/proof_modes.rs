use std::time::Duration;

use sp1_sdk::{Elf, SP1ProofMode, SP1Stdin};

use crate::assert::assert_proof_completed;
use crate::cluster::Cluster;
use crate::programs;
use crate::request::{submit_proof_requests_sequential, ClusterProofRequest, ClusterProofRequests};
use crate::scenario::{Scenario, ScenarioFuture, Tier};

/// One scenario per proof mode, each a single request against a fresh cluster in a fresh
/// process (the suite runner spawns one process per scenario).
///
/// Why not one scenario looping all four modes: the in-process GPU worker's
/// device-memory high-water mark grows per request (retained core/recursion arenas),
/// OOMing on shared desktop GPUs; a fresh process per mode keeps every run at the
/// proven single-request footprint.
pub fn scenarios() -> Vec<Scenario> {
    vec![
        Scenario {
            name: "proof-mode-core",
            timeout: Duration::from_mins(10),
            tier: Tier::Smoke,
            run: || -> ScenarioFuture { Box::pin(run(SP1ProofMode::Core)) },
        },
        Scenario {
            name: "proof-mode-compressed",
            timeout: Duration::from_mins(10),
            tier: Tier::Smoke,
            run: || -> ScenarioFuture { Box::pin(run(SP1ProofMode::Compressed)) },
        },
        Scenario {
            name: "proof-mode-plonk",
            timeout: Duration::from_mins(10),
            tier: Tier::Full,
            run: || -> ScenarioFuture { Box::pin(run(SP1ProofMode::Plonk)) },
        },
        Scenario {
            name: "proof-mode-groth16",
            timeout: Duration::from_mins(10),
            tier: Tier::Smoke,
            run: || -> ScenarioFuture { Box::pin(run(SP1ProofMode::Groth16)) },
        },
        // RSP compressed, for benchmarking the execute-vs-prove split via per-task
        // timing (run with SP1_TASK_TIMING=1; not part of any suite tier).
        Scenario {
            name: "proof-mode-compressed-rsp",
            timeout: Duration::from_mins(30),
            tier: Tier::Full,
            run: || -> ScenarioFuture {
                Box::pin(run_program(
                    SP1ProofMode::Compressed,
                    programs::RSP_ELF.clone(),
                    programs::RSP_STDIN.clone(),
                    Duration::from_mins(15),
                ))
            },
        },
    ]
}

async fn run(mode: SP1ProofMode) -> anyhow::Result<()> {
    run_program(
        mode,
        programs::FIBONACCI_ELF.clone(),
        programs::FIBONACCI_STDIN.clone(),
        Duration::from_mins(5),
    )
    .await
}

/// Prove one program in `mode` against a fresh cluster and wait for terminal status.
async fn run_program(
    mode: SP1ProofMode,
    elf: Elf,
    stdin: SP1Stdin,
    completion_timeout: Duration,
) -> anyhow::Result<()> {
    let cluster = Cluster::standard().start().await?;

    let proof_ids = submit_proof_requests_sequential(ClusterProofRequests {
        rpc_url: cluster.gateway_rpc_url(),
        requests: vec![ClusterProofRequest { elf, stdin, modes: vec![mode] }],
    })
    .await?;

    let api = cluster.api_client().await?;
    for proof_id in &proof_ids {
        // SDK already returned the verified proof; this only waits for the coordinator's
        // async terminal-status write to land in the API. Generous bound for slow CI disks.
        assert_proof_completed(&api, proof_id, completion_timeout, &cluster.artifact_client())
            .await?;
    }

    cluster.shutdown().await;
    Ok(())
}
