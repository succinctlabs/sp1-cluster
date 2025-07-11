use std::sync::atomic::AtomicU32;
use std::sync::Arc;

use opentelemetry::Context;
use sp1_cluster_artifact::{ArtifactClient, ArtifactType};
use sp1_cluster_common::proto::WorkerTask;
use sp1_core_executor::SP1ReduceProof;
use sp1_prover::InnerSC;

use crate::acquire_gpu;
use crate::{client::WorkerService, error::TaskError, SP1Worker};

use super::TaskMetadata;

impl<W: WorkerService, A: ArtifactClient> SP1Worker<W, A> {
    pub async fn process_sp1_shrink_wrap(
        self: &Arc<Self>,
        _: Context,
        task: &WorkerTask,
    ) -> Result<TaskMetadata, TaskError> {
        let data = task.data.as_ref().unwrap();
        let proof = self
            .artifact_client
            .download::<SP1ReduceProof<InnerSC>>(&data.inputs[0])
            .await?;

        let gpu_time = Arc::new(AtomicU32::new(0));
        let permit = acquire_gpu!(self, gpu_time);

        let prover = self.prover.clone();
        let opts = self.prover_opts;
        let (permit, wrap_proof) = tokio::task::spawn_blocking(move || {
            let permit = permit;
            let shrink_proof = prover.shrink(
                SP1ReduceProof {
                    vk: proof.vk,
                    proof: proof.proof,
                },
                opts,
            )?;

            let res = prover.wrap_bn254(shrink_proof, opts)?;

            Ok::<_, TaskError>((permit, res))
        })
        .await
        .unwrap()?;

        drop(permit);

        self.artifact_client
            .upload(&data.outputs[0], wrap_proof)
            .await?;

        // Clean up input artifact since it's replaced by the wrapped proof
        self.artifact_client
            .try_delete(&data.inputs[0], ArtifactType::UnspecifiedArtifactType)
            .await;

        Ok(TaskMetadata::new(
            gpu_time.load(std::sync::atomic::Ordering::Relaxed),
        ))
    }
}
