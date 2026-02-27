#![allow(clippy::too_many_arguments)]
#![allow(clippy::type_complexity)]

use crate::error::TaskError;
use crate::utils::worker_task_to_raw_task_request;
use crate::SP1ClusterWorker;
use anyhow::Result;
use opentelemetry::Context;
use sp1_cluster_artifact::ArtifactClient;
use sp1_cluster_common::proto::{ProofRequestStatus, WorkerTask};
use sp1_prover::worker::{ControllerOutput, ProofId, TaskId, TaskMetadata, WorkerClient};
use std::sync::Arc;

impl<W: WorkerClient, A: ArtifactClient> SP1ClusterWorker<W, A> {
    /// The controller task for an SP1 proof. This task does all of the coordination for a full
    /// SP1 proof which can have mode core, compressed, plonk, and groth16.
    ///
    /// For Groth16/Plonk modes, the controller defers proof completion: it submits the
    /// shrinkwrap + wrap tasks and returns immediately, releasing its memory weight.
    /// The wrap task (Groth16Wrap/PlonkWrap) handles the final proof assembly and calls
    /// complete_proof.
    pub async fn process_sp1_controller(
        self: &Arc<Self>,
        parent: Context,
        task: &WorkerTask,
    ) -> Result<TaskMetadata, TaskError> {
        let data = task.data()?.clone();

        if data.inputs.is_empty() {
            log::info!("no inputs for task");
            return Err(TaskError::Fatal(anyhow::anyhow!("no inputs for task")));
        }

        // Note: inputs[3] contains cycle_limit and inputs[4] is reserved.
        // The controller passes all inputs to the SP1 prover. Cycle limit enforcement
        // is handled by the executor cluster running execute_only tasks before
        // the prover cluster starts proving.

        let raw_task_request = worker_task_to_raw_task_request(&data, Some(parent));

        let (_, controller_output) = self.worker.controller().run(raw_task_request).await?;

        match controller_output {
            ControllerOutput::CompleteProof => {
                // Mark proof as completed immediately (Core/Compressed modes).
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
            ControllerOutput::DeferCompleteProof { .. } => {
                // Groth16/Plonk: the wrap task will complete the proof.
            }
        }

        Ok(TaskMetadata { gpu_time: None })
    }
}
