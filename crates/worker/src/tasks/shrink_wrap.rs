use crate::error::TaskError;
use crate::utils::worker_task_to_raw_task_request;
use anyhow::Result;
use sp1_cluster_artifact::ArtifactClient;
use sp1_cluster_common::proto::WorkerTask;
use sp1_prover::worker::TaskMetadata;
use sp1_prover::worker::WorkerClient;
use std::sync::Arc;

use crate::SP1ClusterWorker;

impl<W: WorkerClient, A: ArtifactClient> SP1ClusterWorker<W, A> {
    pub async fn process_sp1_shrink_wrap(
        self: &Arc<Self>,
        task: &WorkerTask,
    ) -> Result<TaskMetadata, TaskError> {
        let data = task.data()?;
        let raw_task_request = worker_task_to_raw_task_request(data, None);
        self.worker
            .prover_engine()
            .run_shrink_wrap(raw_task_request)
            .await?;
        // TODO: return actual metadata
        Ok(TaskMetadata::default())
    }
}
