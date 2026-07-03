use std::sync::Arc;

use anyhow::Result;
use sp1_cluster_artifact::ArtifactClient;
use sp1_cluster_common::proto::WorkerTask;
use sp1_prover::{
    worker::{TaskMetadata, WorkerClient},
    SP1ProverComponents,
};

use crate::{error::TaskError, utils::worker_task_to_raw_task_request, SP1ClusterWorker};
impl<W: WorkerClient, A: ArtifactClient, C: SP1ProverComponents> SP1ClusterWorker<W, A, C> {
    /// Prove a single shard.
    pub async fn process_sp1_prove_shard(
        self: &Arc<Self>,
        task: &WorkerTask,
    ) -> Result<TaskMetadata, TaskError> {
        let data = task.data()?;
        let raw_task_request = worker_task_to_raw_task_request(data, None);
        // Bench-only (SP1_PROVE_BARRIER=<path>): hold every shard proof until
        // CoreExecute has materialized ALL shards (marker file written at its end).
        // Wall time from marker to proof completion = proving with infinitely fast
        // execution: all inputs ready up front, zero executor starvation. Valid only
        // in the single-process test harness where both tasks share a filesystem.
        if let Some(marker) = std::env::var_os("SP1_PROVE_BARRIER") {
            let marker = std::path::PathBuf::from(marker);
            if !marker.exists() {
                log::info!("PROVE_BARRIER waiting task={}", task.task_id);
                while !marker.exists() {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
                log::info!("PROVE_BARRIER released task={}", task.task_id);
            }
        }
        self.worker
            .prover_engine()
            .submit_prove_core_shard(raw_task_request)
            .await?
            .await
            .map_err(|e| {
                TaskError::Fatal(anyhow::anyhow!("failed to execute prove shard: {}", e))
            })?
    }
}
