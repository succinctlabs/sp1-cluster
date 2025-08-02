use std::sync::Arc;

use anyhow::anyhow;
use sp1_cluster_artifact::{ArtifactClient, ArtifactType};
use sp1_cluster_common::proto::{ProofRequestStatus, WorkerTask};
use sp1_core_executor::SP1ReduceProof;
use sp1_prover::OuterSC;
use sp1_sdk::{
    network::proto::types::ProofMode, ProofFromNetwork, SP1Proof, SP1PublicValues,
    SP1_CIRCUIT_VERSION,
};
use tracing::info_span;

use crate::{error::TaskError, SP1Worker, WorkerService};

use super::TaskMetadata;

impl<W: WorkerService, A: ArtifactClient> SP1Worker<W, A> {
    pub async fn process_sp1_finalize(
        self: &Arc<Self>,
        task: &WorkerTask,
        mode: ProofMode,
    ) -> Result<TaskMetadata, TaskError> {
        let data = task.data()?;

        if data.inputs.is_empty() || data.outputs.is_empty() {
            log::info!("no inputs or outputs for task");
            return Err(TaskError::Fatal(anyhow::anyhow!(
                "no inputs or outputs for task"
            )));
        }

        // Download the wrap proof and public values
        let (wrap_proof, public_values) = tokio::try_join!(
            self.artifact_client
                .download::<SP1ReduceProof<OuterSC>>(&data.inputs[0]),
            self.artifact_client
                .download::<SP1PublicValues>(&data.inputs[1]),
        )?;

        let artifacts_dir = if sp1_prover::build::sp1_dev_mode() {
            sp1_prover::build::try_build_plonk_bn254_artifacts_dev(
                &wrap_proof.vk,
                &wrap_proof.proof,
            )
        } else {
            let circuit_type = match mode {
                ProofMode::Plonk => "plonk",
                ProofMode::Groth16 => "groth16",
                _ => unreachable!(),
            };
            sp1_sdk::install::try_install_circuit_artifacts(circuit_type)
        };

        let span = info_span!("circuit");
        let prover = self.prover.clone();
        let circuit_proof = tokio::task::spawn_blocking(move || {
            let _guard = span.enter();
            match mode {
                ProofMode::Plonk => {
                    SP1Proof::Plonk(prover.wrap_plonk_bn254(wrap_proof, &artifacts_dir))
                }
                ProofMode::Groth16 => {
                    SP1Proof::Groth16(prover.wrap_groth16_bn254(wrap_proof, &artifacts_dir))
                }
                _ => unreachable!(),
            }
        })
        .await
        .map_err(|e| TaskError::Fatal(anyhow!("failed to prove: {}", e)))?;

        // Create the final proof with public values
        let result = ProofFromNetwork {
            proof: circuit_proof,
            public_values,
            sp1_version: SP1_CIRCUIT_VERSION.to_string(),
        };

        // Upload the final proof to the output artifact
        if std::env::var("WORKER_NO_UPLOAD").unwrap_or("false".to_string()) == "true" {
            tracing::warn!("WORKER_NO_UPLOAD=true, skipping upload");
        } else {
            self.artifact_client
                .upload_proof(&data.outputs[0], result)
                .await?;
        }

        // Complete the proof directly from the wrap task
        self.worker_client
            .complete_proof(
                data.proof_id.clone(),
                Some(task.task_id.clone()),
                ProofRequestStatus::Completed,
            )
            .await?;

        // Clean up input artifacts since they're replaced by the final proof
        self.artifact_client
            .try_delete_batch(&data.inputs[0..2], ArtifactType::UnspecifiedArtifactType)
            .await;

        Ok(TaskMetadata::default())
    }
}
