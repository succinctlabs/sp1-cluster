use std::sync::Arc;

use crate::{error::TaskError, SP1ClusterWorker};
use sp1_cluster_artifact::ArtifactClient;
use sp1_cluster_common::proto::{ProofRequestStatus, WorkerTask};
use sp1_prover::worker::{ProofId, TaskMetadata, WorkerClient};

impl<W: WorkerClient, A: ArtifactClient> SP1ClusterWorker<W, A> {
    /// Does execution only - used for the execution oracle.
    /// TODO: Implement proper execution logic using sp1_core_executor.
    pub async fn process_sp1_execute_only(
        self: &Arc<Self>,
        task: &WorkerTask,
    ) -> Result<TaskMetadata, TaskError> {
        let data = task.data.as_ref().expect("no task data");
        let proof_id = data.proof_id.clone();

        log::warn!("ExecuteOnly task type is not yet fully implemented");

        // Mark proof as completed (placeholder implementation)
        self.worker
            .worker_client()
            .complete_proof(ProofId::new(proof_id), None, ProofRequestStatus::Completed)
            .await?;

        Ok(Default::default())
    }
}
