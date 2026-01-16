use std::sync::Arc;

use anyhow::Result;
use sp1_cluster_artifact::ArtifactClient;
use sp1_cluster_common::proto::WorkerTask;
use sp1_prover::worker::{TaskMetadata, WorkerClient};

use crate::{error::TaskError, utils::worker_task_to_raw_task_request, SP1ClusterWorker};
impl<W: WorkerClient, A: ArtifactClient> SP1ClusterWorker<W, A> {
    /// Prove a single shard.
    pub async fn process_sp1_prove_shard(
        self: &Arc<Self>,
        task: &WorkerTask,
    ) -> Result<TaskMetadata, TaskError> {
        let data = task.data()?;
        let raw_task_request = worker_task_to_raw_task_request(data, None);
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
