use crate::error::TaskError;
use crate::utils::worker_task_to_raw_task_request;
use anyhow::Result;
use sp1_cluster_artifact::ArtifactClient;
use sp1_cluster_common::proto::{ProofRequestStatus, WorkerTask};
use sp1_prover::worker::{
    ProofFromNetwork, ProofId, RawTaskRequest, SP1Proof, TaskId, TaskMetadata, WrapFinalizeInput,
    WorkerClient,
};
use sp1_prover::SP1_CIRCUIT_VERSION;
use sp1_prover_types::network_base_types::ProofMode;
use sp1_prover_types::ArtifactType;
use sp1_primitives::io::SP1PublicValues;
use std::sync::Arc;

use crate::SP1ClusterWorker;

impl<W: WorkerClient, A: ArtifactClient> SP1ClusterWorker<W, A> {
    pub async fn process_sp1_finalize(
        self: &Arc<Self>,
        task: &WorkerTask,
        mode: ProofMode,
    ) -> Result<TaskMetadata, TaskError> {
        let data = task.data()?.clone();
        let raw_task_request = worker_task_to_raw_task_request(&data, None);

        // Extract the optional finalize input (extra input from controller for deferred
        // proof completion). Strip it before passing to run_groth16/run_plonk which
        // expect exactly one input.
        let finalize_input_artifact = if raw_task_request.inputs.len() > 1 {
            Some(raw_task_request.inputs[1].clone())
        } else {
            None
        };

        let prove_request = RawTaskRequest {
            inputs: vec![raw_task_request.inputs[0].clone()],
            outputs: raw_task_request.outputs.clone(),
            context: raw_task_request.context.clone(),
        };

        match mode {
            ProofMode::Plonk => self.worker.prover_engine().run_plonk(prove_request).await?,
            ProofMode::Groth16 => self.worker.prover_engine().run_groth16(prove_request).await?,
            _ => return Err(TaskError::Fatal(anyhow::anyhow!("Invalid proof mode"))),
        }

        // If finalize input is present, assemble the final proof and complete.
        if let Some(finalize_artifact) = finalize_input_artifact {
            let finalize_input: WrapFinalizeInput = self
                .worker
                .artifact_client()
                .download(&finalize_artifact)
                .await?;

            let wrap_proof_artifact = &raw_task_request.outputs[0];

            let inner_proof = match mode {
                ProofMode::Groth16 => {
                    let proof = self.worker.artifact_client().download(wrap_proof_artifact).await?;
                    SP1Proof::Groth16(proof)
                }
                ProofMode::Plonk => {
                    let proof = self.worker.artifact_client().download(wrap_proof_artifact).await?;
                    SP1Proof::Plonk(proof)
                }
                _ => unreachable!(),
            };

            let public_values = SP1PublicValues::from(&finalize_input.public_value_stream);
            let proof = ProofFromNetwork {
                proof: inner_proof,
                public_values,
                sp1_version: SP1_CIRCUIT_VERSION.to_string(),
            };

            self.worker
                .artifact_client()
                .upload_proof(&finalize_input.output_artifact, proof)
                .await?;

            // Clean up artifacts.
            let mut cleanup = finalize_input.artifacts_to_cleanup;
            cleanup.push(finalize_artifact);
            cleanup.push(wrap_proof_artifact.clone());

            self.worker
                .artifact_client()
                .delete_batch(&cleanup, ArtifactType::UnspecifiedArtifactType)
                .await?;

            // Mark proof as completed.
            self.worker
                .worker_client()
                .complete_proof(
                    ProofId::new(data.proof_id.clone()),
                    Some(TaskId::new(task.task_id.clone())),
                    ProofRequestStatus::Completed,
                    "",
                )
                .await?;
        }

        Ok(TaskMetadata::default())
    }
}
