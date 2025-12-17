use crate::error::TaskError;
use crate::utils::worker_task_to_raw_task_request;
use crate::SP1ClusterWorker;
use anyhow::Result;
use opentelemetry::Context;
use sp1_cluster_artifact::ArtifactClient;
use sp1_cluster_common::proto::WorkerTask;
use sp1_prover::worker::{run_vk_generation, TaskMetadata, WorkerClient};
use std::sync::Arc;

impl<W: WorkerClient, A: ArtifactClient> SP1ClusterWorker<W, A> {
    /// The controller task for an SP1 proof. This task does all of the coordination for a full
    /// SP1 proof which can have mode core, compressed, plonk, and groth16.
    pub async fn process_sp1_generate_vk_controller(
        self: &Arc<Self>,
        parent: Context,
        task: &WorkerTask,
    ) -> Result<TaskMetadata, TaskError> {
        let data = task.data()?.clone();

        if data.inputs.is_empty() {
            log::info!("no inputs for task");
            return Err(TaskError::Fatal(anyhow::anyhow!("no inputs for task")));
        }

        let raw_task_request = worker_task_to_raw_task_request(&data, Some(parent));

        self.worker
            .controller()
            .run_sp1_util_vkey_map_controller(raw_task_request)
            .await?;

        Ok(TaskMetadata { gpu_time: None })
    }
}

impl<W: WorkerClient, A: ArtifactClient> SP1ClusterWorker<W, A> {
    /// Generate a single chunk of vks.
    pub async fn process_sp1_generate_vk_chunk(
        self: &Arc<Self>,
        task: &WorkerTask,
    ) -> Result<TaskMetadata, TaskError> {
        let data = task.data()?;

        let raw_task_request = worker_task_to_raw_task_request(&data, None);
        let vk_worker = Arc::new(self.worker.prover_engine().vk_worker.clone());
        let client = self.worker.artifact_client().clone();

        run_vk_generation(vk_worker, raw_task_request, client)
            .await
            .map(|_| TaskMetadata { gpu_time: None })
    }
}
