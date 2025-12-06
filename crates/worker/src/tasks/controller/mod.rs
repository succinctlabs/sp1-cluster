#![allow(clippy::too_many_arguments)]
#![allow(clippy::type_complexity)]

use crate::error::TaskError;
use crate::utils::worker_task_to_raw_task_request;
use crate::SP1ClusterWorker;
use anyhow::Result;
use opentelemetry::Context;
use sp1_cluster_artifact::ArtifactClient;
use sp1_cluster_common::proto::{ProofRequestStatus, WorkerTask};
use sp1_prover::worker::{ProofId, TaskId, TaskMetadata, WorkerClient};
use std::sync::Arc;

impl<W: WorkerClient, A: ArtifactClient> SP1ClusterWorker<W, A> {
    /// The controller task for an SP1 proof. This task does all of the coordination for a full
    /// SP1 proof which can have mode core, compressed, plonk, and groth16.
    pub async fn process_sp1_controller(
        self: &Arc<Self>,
        parent: Context,
        task: &WorkerTask,
    ) -> Result<TaskMetadata, TaskError> {
        let mut data = task.data()?.clone();

        if data.inputs.is_empty() {
            log::info!("no inputs for task");
            return Err(TaskError::Fatal(anyhow::anyhow!("no inputs for task")));
        }

        // Truncate the cycle limit and TODO thing for now.
        data.inputs = data.inputs.into_iter().take(3).collect();

        let raw_task_request = worker_task_to_raw_task_request(&data, Some(parent));

        self.worker.controller().run(raw_task_request).await?;

        // Mark proof as completed.
        self.worker
            .worker_client()
            .complete_proof(
                ProofId::new(data.proof_id.clone()),
                Some(TaskId::new(task.task_id.clone())),
                ProofRequestStatus::Completed,
            )
            .await?;

        Ok(TaskMetadata { gpu_time: None })
    }
}
