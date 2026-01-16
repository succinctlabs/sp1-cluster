use crate::error::TaskError;
use crate::utils::worker_task_to_raw_task_request;
use anyhow::Result;
use sp1_cluster_artifact::ArtifactClient;
use sp1_cluster_common::proto::WorkerTask;
use sp1_prover::worker::TaskMetadata;
use sp1_prover::worker::WorkerClient;
use sp1_prover_types::network_base_types::ProofMode;
use std::sync::Arc;

use crate::SP1ClusterWorker;

impl<W: WorkerClient, A: ArtifactClient> SP1ClusterWorker<W, A> {
    pub async fn process_sp1_finalize(
        self: &Arc<Self>,
        task: &WorkerTask,
        mode: ProofMode,
    ) -> Result<TaskMetadata, TaskError> {
        let data = task.data()?;
        let raw_task_request = worker_task_to_raw_task_request(data, None);
        match mode {
            ProofMode::Plonk => {
                self.worker
                    .prover_engine()
                    .run_plonk(raw_task_request)
                    .await?
            }
            ProofMode::Groth16 => {
                self.worker
                    .prover_engine()
                    .run_groth16(raw_task_request)
                    .await?
            }
            _ => return Err(TaskError::Fatal(anyhow::anyhow!("Invalid proof mode"))),
        }

        Ok(TaskMetadata::default())
    }
}
