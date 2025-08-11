use std::sync::atomic::AtomicU32;
use std::sync::Arc;

use anyhow::anyhow;
use opentelemetry::Context;
use sp1_cluster_artifact::{ArtifactClient, ArtifactType};
use sp1_cluster_common::proto::WorkerTask;
use sp1_core_executor::SP1ReduceProof;
use sp1_prover::InnerSC;
use sp1_sdk::{HashableKey, SP1VerifyingKey};

use crate::acquire_gpu;
use crate::tasks::CommonTaskInput;
use crate::{client::WorkerService, error::TaskError, SP1Worker};

use super::TaskMetadata;

impl<W: WorkerService, A: ArtifactClient> SP1Worker<W, A> {
    pub async fn process_sp1_shrink_wrap(
        self: &Arc<Self>,
        _: Context,
        task: &WorkerTask,
    ) -> Result<TaskMetadata, TaskError> {
        let data = task.data.as_ref().unwrap();
        let proof_future = self
            .artifact_client
            .download::<SP1ReduceProof<InnerSC>>(&data.inputs[0]);
        let common_input_artifact = data.inputs.get(1);
        let artifact_client = self.artifact_client.clone();
        let common_input_future = async move {
            if let Some(common_input_artifact) = common_input_artifact {
                let common_input = artifact_client
                    .download::<CommonTaskInput>(common_input_artifact)
                    .await?;
                Ok(Some(common_input))
            } else {
                Ok(None)
            }
        };
        let (proof, common_input) = tokio::try_join!(proof_future, common_input_future)?;

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
            // Verify shrink vkey (necessary to avoid 'vk not allowed' bug)
            let vkey_hash = shrink_proof.vk.hash_babybear();
            if !prover.recursion_vk_map.contains_key(&vkey_hash) {
                return Err(TaskError::Retryable(anyhow!(
                    "shrink vkey {:?} not found in map",
                    vkey_hash
                )));
            }

            if let Some(common_input) = common_input {
                // Verify shrink proof to avoid any other bug
                if let Err(e) = prover.verify_shrink(
                    &shrink_proof,
                    &SP1VerifyingKey {
                        vk: common_input.vk,
                    },
                ) {
                    return Err(TaskError::Retryable(anyhow!(
                        "verify shrink proof failed: {:?}",
                        e
                    )));
                }
            }

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
