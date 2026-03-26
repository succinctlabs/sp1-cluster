use std::sync::Arc;

use anyhow::Result;
use sp1_cluster_artifact::ArtifactClient;
use sp1_cluster_common::proto::WorkerTask;
use sp1_prover::worker::{CoreExecuteTaskRequest, TaskId, TaskMetadata, WorkerClient};

use crate::{error::TaskError, utils::worker_task_to_raw_task_request, SP1ClusterWorker};

impl<W: WorkerClient, A: ArtifactClient> SP1ClusterWorker<W, A> {
    pub async fn process_sp1_core_execute(
        self: &Arc<Self>,
        task: &WorkerTask,
    ) -> Result<TaskMetadata, TaskError> {
        let data = task.data()?;
        let task_id = TaskId::new(task.task_id.clone());
        let raw_task_request = worker_task_to_raw_task_request(data, None);
        let request = CoreExecuteTaskRequest::from_raw(raw_task_request)?;
        self.worker.controller().execute(task_id, request).await?;
        Ok(TaskMetadata { gpu_ms: None })
    }
}
