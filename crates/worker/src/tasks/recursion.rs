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
    /// Generate a proof that verifies a batch of deferred proofs.
    pub async fn process_sp1_recursion_deferred_batch(
        self: &Arc<Self>,
        task: &WorkerTask,
    ) -> Result<TaskMetadata, TaskError> {
        let data = task.data()?;
        let raw_task_request = worker_task_to_raw_task_request(data, None);
        self.worker
            .prover_engine()
            .submit_prove_deferred(raw_task_request)
            .await?
            .await
            .map_err(|e| {
                TaskError::Fatal(anyhow::anyhow!(
                    "failed to execute prove deferred batch: {}",
                    e
                ))
            })?
    }

    /// Generate a proof that verifies a batch of recursive proofs (which can be verify-core,
    /// verify-deferred, or this proof itself)
    pub async fn process_sp1_recursion_reduce_batch(
        self: &Arc<Self>,
        task: &WorkerTask,
    ) -> Result<TaskMetadata, TaskError> {
        let data = task.data()?;
        let raw_task_request = worker_task_to_raw_task_request(data, None);
        self.worker
            .prover_engine()
            .submit_recursion_reduce(raw_task_request)
            .await?
            .await
            .map_err(|e| {
                TaskError::Fatal(anyhow::anyhow!(
                    "failed to execute recursion reduce batch: {}",
                    e
                ))
            })?
    }
}
