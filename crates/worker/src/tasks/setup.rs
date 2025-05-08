use std::sync::atomic::AtomicU32;
use std::sync::Arc;

use sp1_cluster_artifact::ArtifactClient;
use sp1_cluster_common::proto::WorkerTask;
use sp1_stark::MachineProver;

use crate::acquire_gpu;
use crate::{client::WorkerService, error::TaskError, SP1Worker};

use super::TaskMetadata;

impl<W: WorkerService, A: ArtifactClient> SP1Worker<W, A> {
    pub async fn process_sp1_setup_vkey(
        &self,
        task: &WorkerTask,
    ) -> Result<TaskMetadata, TaskError> {
        let data = task.data()?;
        let elf = self
            .artifact_client
            .download_program(&data.inputs[0])
            .await?;

        let program = self.prover.get_program(&elf)?;

        let gpu_time = Arc::new(AtomicU32::new(0));
        let permit = acquire_gpu!(self, gpu_time);

        let (_, vk) = self.prover.core_prover.setup(&program);

        drop(permit);

        self.artifact_client.upload(&data.outputs[0], vk).await?;

        Ok(TaskMetadata::new(
            gpu_time.load(std::sync::atomic::Ordering::Relaxed),
        ))
    }
}
