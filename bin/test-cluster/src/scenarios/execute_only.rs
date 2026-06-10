use std::time::Duration;

use sp1_sdk::{SP1ProofMode, SP1Stdin};

use crate::assert::{assert_execution_result, task_statuses, wait_proof_status, ExpectedExecution};
use crate::cluster::{Cluster, CoordinatorKind};
use crate::programs;
use crate::request::request_only;
use crate::scenario::{Scenario, ScenarioFuture};
use sp1_cluster_common::proto::{ExecutionFailureCause, ExecutionStatus, ProofRequestStatus};

/// Pinned gas-oracle regression value for the committed fibonacci elf+stdin
/// (cycles=18320, gas=20173 — carried over from the pre-scenario test).
const EXPECTED_FIBONACCI_GAS: u64 = 20173;

pub fn scenario() -> Scenario {
    Scenario {
        name: "execute-only",
        timeout: Duration::from_secs(45 * 60),
        skip_in_full: false,
        run: || -> ScenarioFuture { Box::pin(run()) },
    }
}

/// Standalone executor cluster (prod is_executor_cluster shape): api + execute-only
/// coordinator + gateway + 1 CPU node. No prover, no proof artifacts — terminal state
/// and execution metadata come from the cluster API.
async fn run() -> anyhow::Result<()> {
    let cluster = Cluster::builder()
        .coordinator(CoordinatorKind::ExecuteOnly)
        .cpu_nodes(1)
        .start()
        .await?;
    let api = cluster.api_client().await?;
    let mut coordinator = cluster.coordinator_client().await?;

    // --- success case ---------------------------------------------------------------
    let proof_id = request_only(
        &cluster.gateway_rpc_url(),
        programs::FIBONACCI_ELF.clone(),
        programs::FIBONACCI_STDIN.clone(),
        SP1ProofMode::Compressed,
    )
    .await?;
    tracing::info!("submitted execute-only request {proof_id}");

    // Best-effort observation of the ExecuteOnly task while it runs (state is pruned
    // after completion, so this may legitimately see nothing for fast executions).
    if let Ok(statuses) = task_statuses(&mut coordinator, &proof_id).await {
        tracing::info!("task statuses during run: {statuses:?}");
    }

    let pr = wait_proof_status(
        &api,
        &proof_id,
        ProofRequestStatus::Completed,
        Duration::from_secs(10 * 60),
    )
    .await?;
    let er = assert_execution_result(
        &pr,
        &ExpectedExecution {
            status: ExecutionStatus::Executed,
            min_cycles: 1,
            gas: Some(EXPECTED_FIBONACCI_GAS),
            require_pv_hash: true,
        },
    )?;
    tracing::info!(
        "execute-only success: cycles={} gas={} pv_hash={} bytes",
        er.cycles,
        er.gas,
        er.public_values_hash.len()
    );

    // --- failure case: empty stdin makes fibonacci's io::read fail -------------------
    let fail_id = request_only(
        &cluster.gateway_rpc_url(),
        programs::FIBONACCI_ELF.clone(),
        SP1Stdin::new(),
        SP1ProofMode::Compressed,
    )
    .await?;
    tracing::info!("submitted execute-only failure request {fail_id}");

    let pr = wait_proof_status(
        &api,
        &fail_id,
        ProofRequestStatus::Failed,
        Duration::from_secs(10 * 60),
    )
    .await?;
    let er = assert_execution_result(
        &pr,
        &ExpectedExecution {
            status: ExecutionStatus::Failed,
            min_cycles: 0,
            gas: None,
            require_pv_hash: false,
        },
    )?;
    // Pinned from the observed run: the empty-stdin read panics inside the program,
    // which halts with a non-zero exit code.
    anyhow::ensure!(
        er.failure_cause() == ExecutionFailureCause::HaltWithNonZeroExitCode,
        "expected pinned failure cause HaltWithNonZeroExitCode, got {:?}",
        er.failure_cause()
    );
    tracing::info!("execution failure cause: {:?}", er.failure_cause());

    cluster.shutdown().await;
    Ok(())
}
