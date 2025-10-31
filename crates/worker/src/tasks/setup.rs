use crate::{error::TaskError, SP1ClusterWorker};
use sp1_cluster_artifact::ArtifactClient;
use sp1_cluster_common::proto::WorkerTask;
use sp1_prover::worker::{TaskId, TaskMetadata, WorkerClient};
use sp1_prover_types::Artifact;

impl<W: WorkerClient, A: ArtifactClient> SP1ClusterWorker<W, A> {
    pub async fn process_sp1_setup_vkey(
        &self,
        task: &WorkerTask,
    ) -> Result<TaskMetadata, TaskError> {
        let data = task.data()?;

        let task_id = TaskId::new(task.task_id.clone());
        let elf_artifact = Artifact::from(data.inputs[0].clone());
        let output_artifact = Artifact::from(data.outputs[0].clone());

        let (_, metadata) = self
            .worker
            .prover_engine()
            .submit_setup(task_id, elf_artifact, output_artifact)
            .await
            .expect("failed to submit setup")
            .await
            .expect("failed to wait for setup")?;
        Ok(metadata)
    }
}
