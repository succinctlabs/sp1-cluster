use std::sync::Arc;

use anyhow::Result;
use sp1_cluster_artifact::ArtifactClient;
use sp1_cluster_common::proto::WorkerTask;
use sp1_prover::worker::{TaskId, TaskMetadata, WorkerClient};
use sp1_prover_types::Artifact;

use crate::{error::TaskError, SP1ClusterWorker};
impl<W: WorkerClient, A: ArtifactClient> SP1ClusterWorker<W, A> {
    /// Prove a single shard.
    pub async fn process_sp1_prove_shard(
        self: &Arc<Self>,
        task: &WorkerTask,
    ) -> Result<TaskMetadata, TaskError> {
        let data = task.data()?;

        let task_id = TaskId::new(task.task_id.clone());
        let elf_artifact = Artifact::from(data.inputs[0].clone());
        let common_input_artifact = Artifact::from(data.inputs[1].clone());
        let record_artifact = Artifact::from(data.inputs[2].clone());
        // let opts_artifact = Artifact::from(data.inputs[3].clone());
        let output_artifact = Artifact::from(data.outputs[0].clone());

        self.worker
            .prover_engine()
            .submit_prove_core_shard(
                task_id,
                elf_artifact,
                common_input_artifact,
                record_artifact,
                output_artifact,
            )
            .await?
            .await
            .map_err(|e| TaskError::Retryable(anyhow::anyhow!(e)))?;

        Ok(TaskMetadata { gpu_time: None })
    }
}
