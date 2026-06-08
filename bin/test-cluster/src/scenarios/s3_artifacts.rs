use std::time::Duration;

use sp1_sdk::SP1ProofMode;

use crate::assert::{assert_proof_artifact_downloadable, wait_proof_status};
use crate::cluster::Cluster;
use crate::request::request_only;
use crate::scenario::{Flavors, Scenario, ScenarioFuture};
use crate::scenarios::long_program;
use sp1_cluster_common::proto::ProofRequestStatus;

pub fn scenario() -> Scenario {
    Scenario {
        name: "s3-artifacts",
        flavors: Flavors::Both,
        timeout: Duration::from_secs(90 * 60),
        run: || -> ScenarioFuture { Box::pin(run()) },
    }
}

/// The whole pipeline on an S3-compatible artifact store (MinIO stands in for prod S3):
/// program/stdin upload through the gateway, inter-task artifacts, and the final proof all
/// flow through S3. The long program exercises larger artifact upload/download paths.
async fn run() -> anyhow::Result<()> {
    let cluster = Cluster::standard().start_s3().await?;
    let api = cluster.api_client().await?;

    let (elf, stdin) = long_program();
    let proof_id = request_only(
        &cluster.gateway_rpc_url(),
        elf,
        stdin,
        SP1ProofMode::Compressed,
    )
    .await?;
    tracing::info!("submitted {proof_id} on the S3 (MinIO) store");

    let pr = wait_proof_status(
        &api,
        &proof_id,
        ProofRequestStatus::Completed,
        Duration::from_secs(60 * 60),
    )
    .await?;
    // Downloads the proof artifact through the S3 client — exercises endpoint/path-style.
    assert_proof_artifact_downloadable(&pr, &cluster.artifact_client()).await?;

    cluster.shutdown().await;
    Ok(())
}
