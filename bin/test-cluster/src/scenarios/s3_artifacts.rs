use std::time::Duration;

use sp1_sdk::SP1ProofMode;

use crate::assert::assert_proof_completed;
use crate::cluster::Cluster;
use crate::programs;
use crate::request::request_only;
use crate::scenario::{Scenario, ScenarioFuture, Tier};

pub fn scenario() -> Scenario {
    Scenario {
        name: "s3-artifacts",
        timeout: Duration::from_mins(20),
        tier: Tier::Full,
        run: || -> ScenarioFuture { Box::pin(run()) },
    }
}

/// The whole pipeline on an S3-compatible artifact store (MinIO stands in for prod S3):
/// program/stdin upload through the gateway, inter-task artifacts, and the final proof all
/// flow through S3. The long program exercises larger artifact upload/download paths.
async fn run() -> anyhow::Result<()> {
    let cluster = Cluster::standard().start_s3().await?;
    let api = cluster.api_client().await?;

    let proof_id = request_only(
        &cluster.gateway_rpc_url(),
        programs::RSP_ELF.clone(),
        programs::RSP_STDIN.clone(),
        SP1ProofMode::Compressed,
    )
    .await?;
    tracing::info!("submitted {proof_id} on the S3 (MinIO) store");

    // The artifact download goes through the S3 client — exercises endpoint/path-style.
    assert_proof_completed(
        &api,
        &proof_id,
        Duration::from_hours(1),
        &cluster.artifact_client(),
    )
    .await?;

    cluster.shutdown().await;
    Ok(())
}
