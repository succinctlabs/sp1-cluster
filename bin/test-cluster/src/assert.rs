use std::future::Future;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use sp1_cluster_artifact::{ArtifactClient, ArtifactType};
use sp1_cluster_common::client::ClusterServiceClient;
use sp1_cluster_common::proto::worker_service_client::WorkerServiceClient as RawCoordinatorClient;
use sp1_cluster_common::proto::{
    ExecutionStatus, GetStatsResponse, GetTaskStatusesRequest, ProofRequest,
    ProofRequestGetRequest, ProofRequestStatus, TaskStatus,
};
use tonic::transport::Channel;

pub const POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Bounded poll. `f` returns Ok(Some(v)) when done, Ok(None) to keep polling, Err to abort.
pub async fn poll_until<T, F, Fut>(what: &str, timeout: Duration, mut f: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<Option<T>>>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(v) = f().await? {
            return Ok(v);
        }
        if tokio::time::Instant::now() >= deadline {
            bail!("timed out after {timeout:?} waiting for {what}");
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

pub async fn get_proof_request(api: &ClusterServiceClient, proof_id: &str) -> Result<ProofRequest> {
    api.get_proof_request(ProofRequestGetRequest {
        proof_id: proof_id.to_string(),
    })
    .await
    .map_err(|e| anyhow::anyhow!("get_proof_request({proof_id}): {e}"))?
    .with_context(|| format!("proof request {proof_id} not found"))
}

fn is_terminal(status: ProofRequestStatus) -> bool {
    matches!(
        status,
        ProofRequestStatus::Completed | ProofRequestStatus::Failed | ProofRequestStatus::Cancelled
    )
}

/// Poll the API until `proof_id` reaches `expected`. Errors immediately if it reaches a
/// DIFFERENT terminal status (no point waiting out the timeout).
pub async fn wait_proof_status(
    api: &ClusterServiceClient,
    proof_id: &str,
    expected: ProofRequestStatus,
    timeout: Duration,
) -> Result<ProofRequest> {
    poll_until(
        &format!("proof {proof_id} to reach {expected:?}"),
        timeout,
        || async {
            let pr = get_proof_request(api, proof_id).await?;
            let status = pr.proof_status();
            if status == expected {
                Ok(Some(pr))
            } else if is_terminal(status) {
                bail!("proof {proof_id} reached terminal {status:?}, expected {expected:?}")
            } else {
                Ok(None)
            }
        },
    )
    .await
}

/// Wait for `proof_id` to reach Completed in the API, then assert its proof artifact is
/// downloadable. The standard happy-path ending of proving scenarios.
pub async fn assert_proof_completed(
    api: &ClusterServiceClient,
    proof_id: &str,
    timeout: Duration,
    artifact_client: &impl ArtifactClient,
) -> Result<ProofRequest> {
    let pr = wait_proof_status(api, proof_id, ProofRequestStatus::Completed, timeout).await?;
    assert_proof_artifact_downloadable(&pr, artifact_client).await?;
    Ok(pr)
}

/// Assert the proof's status stays `expected` for the whole `window` — catches a late
/// write flipping a terminal status.
pub async fn assert_status_holds(
    api: &ClusterServiceClient,
    proof_id: &str,
    expected: ProofRequestStatus,
    window: Duration,
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + window;
    while tokio::time::Instant::now() < deadline {
        let pr = get_proof_request(api, proof_id).await?;
        let status = pr.proof_status();
        if status != expected {
            bail!("proof {proof_id} status flipped away from {expected:?}: {status:?}");
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
    Ok(())
}

pub struct ExpectedExecution {
    pub status: ExecutionStatus,
    pub min_cycles: u64,
    /// Exact gas regression value, when pinned (execute-only fibonacci oracle).
    pub gas: Option<u64>,
    pub require_pv_hash: bool,
}

/// Assert the execution metadata recorded for a proof request and return it.
///
/// The cluster-authoritative record is the `extra_data` JSON (the ExecuteOnly task
/// completes the proof with a serialized `ExecutionResult` there; the coordinator's
/// terminal update never sets the `execution_result` proto field — that one is written
/// by external actors, and downstream fulfillment also parses `extra_data`, see
/// crates/fulfillment/src/lib.rs).
pub fn assert_execution_result(
    pr: &ProofRequest,
    expected: &ExpectedExecution,
) -> Result<sp1_cluster_common::proto::ExecutionResult> {
    let extra_data = pr.extra_data.as_ref().with_context(|| {
        format!(
            "proof {} has no extra_data (execution result missing)",
            pr.id
        )
    })?;
    let er: sp1_cluster_common::proto::ExecutionResult = serde_json::from_str(extra_data)
        .with_context(|| {
            format!(
                "proof {}: extra_data is not an ExecutionResult: {extra_data:?}",
                pr.id
            )
        })?;
    if er.status() != expected.status {
        bail!(
            "proof {}: execution status {:?}, expected {:?} (failure_cause {:?})",
            pr.id,
            er.status(),
            expected.status,
            er.failure_cause()
        );
    }
    if er.cycles < expected.min_cycles {
        bail!(
            "proof {}: cycles {} < expected min {}",
            pr.id,
            er.cycles,
            expected.min_cycles
        );
    }
    if let Some(gas) = expected.gas {
        if er.gas != gas {
            bail!("proof {}: gas {} != pinned {}", pr.id, er.gas, gas);
        }
    }
    if expected.require_pv_hash && er.public_values_hash.is_empty() {
        bail!("proof {}: empty public_values_hash", pr.id);
    }
    Ok(er)
}

/// The proof artifact referenced by the API row must actually be downloadable and nonempty.
pub async fn assert_proof_artifact_downloadable(
    pr: &ProofRequest,
    artifact_client: &impl ArtifactClient,
) -> Result<()> {
    let id = pr
        .proof_artifact_id
        .clone()
        .with_context(|| format!("proof {} has no proof_artifact_id", pr.id))?;
    let bytes = artifact_client
        .download_raw(&id, ArtifactType::Proof)
        .await
        .map_err(|e| anyhow::anyhow!("download proof artifact {id}: {e:?}"))?;
    if bytes.is_empty() {
        bail!("proof artifact {id} is empty");
    }
    tracing::info!("proof artifact {id} downloadable ({} bytes)", bytes.len());
    Ok(())
}

/// Poll coordinator GetStats until `pred` holds (e.g. "a task is active", "one cpu worker
/// left"). Returns the matching stats snapshot.
pub async fn wait_stats(
    coordinator: &mut RawCoordinatorClient<Channel>,
    what: &str,
    timeout: Duration,
    pred: impl Fn(&GetStatsResponse) -> bool,
) -> Result<GetStatsResponse> {
    poll_until(what, timeout, || {
        let mut c = coordinator.clone();
        let pred = &pred;
        async move {
            match c.get_stats(tonic::Request::new(())).await {
                Ok(resp) => {
                    let stats = resp.into_inner();
                    Ok(if pred(&stats) { Some(stats) } else { None })
                }
                // Coordinator may be down mid-scenario (restart tests); keep polling.
                Err(_) => Ok(None),
            }
        }
    })
    .await
}

/// Snapshot of task statuses for a proof: status -> task id list.
pub async fn task_statuses(
    coordinator: &mut RawCoordinatorClient<Channel>,
    proof_id: &str,
) -> Result<Vec<(TaskStatus, Vec<String>)>> {
    let resp = coordinator
        .get_task_statuses(GetTaskStatusesRequest {
            worker_id: String::new(), // unused by the handler (server.rs:198)
            proof_id: proof_id.to_string(),
            task_ids: vec![],
        })
        .await
        .context("get_task_statuses")?
        .into_inner();
    Ok(resp
        .statuses
        .into_iter()
        .map(|entry| {
            let status = entry.key();
            (status, entry.value.map(|l| l.ids).unwrap_or_default())
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn poll_until_returns_value() {
        let mut n = 0;
        let v = poll_until("test", Duration::from_secs(5), || {
            n += 1;
            let done = n >= 3;
            async move { Ok(if done { Some(42) } else { None }) }
        })
        .await
        .unwrap();
        assert_eq!(v, 42);
    }

    #[tokio::test]
    async fn poll_until_times_out() {
        let err = poll_until::<(), _, _>("never", Duration::from_millis(50), || async { Ok(None) })
            .await
            .unwrap_err();
        assert!(err.to_string().contains("timed out"), "{err}");
    }
}
