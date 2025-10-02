pub mod balanced;
pub mod default;

use std::{fmt::Debug, sync::Arc};

use sp1_cluster_common::proto::{self, TaskType};
use tokio::sync::OwnedRwLockWriteGuard;
use tonic::Status;

use crate::{Coordinator, CoordinatorState, Proof, Task, Worker};

// Concrete metadata types that all policies use
use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TaskMetadata {
    pub gpu_ms: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProofMetadata {
    pub reserved_gpu_ms: u64,
    pub on_demand_gpu_ms: u64,
}

#[async_trait::async_trait]
pub trait AssignmentPolicy: Sized + Clone + Default + Send + Sync + 'static {
    /// Extra state tracked for a proof.
    type ProofState: Clone + Default + Send + Sync + 'static;

    /// Extra state tracked for a task.
    type TaskState: Clone + Default + Send + Sync + 'static;

    /// Extra state tracked for a worker.
    type WorkerState: Clone + Default + Send + Sync + Debug + 'static;

    /// Metadata to be sent to the API when a proof completes.
    type ProofResultMetadata: Clone + Default + Serialize + DeserializeOwned + Send + Sync + 'static;

    /// Create a proof state for a new proof given a request.
    fn create_proof_state(
        state: &CoordinatorState<Self>,
        request: &proto::CreateProofRequest,
    ) -> Self::ProofState;

    /// Enqueue a task.
    fn enqueue_task(state: &mut CoordinatorState<Self>, task: Task<Self>);

    /// Update proof state after a task has completed successfully.
    fn post_task_success_update_proof(
        proof: &mut Proof<Self>,
        task_extra: &Self::TaskState,
        metadata: TaskMetadata,
    );

    /// Update global state after a task has completed successfully.
    fn post_task_success_update_state(state: &mut CoordinatorState<Self>, task_type: TaskType);

    /// Update global state after a task has completed or failed.
    fn post_task_update_state(
        state: &mut CoordinatorState<Self>,
        proof_extra: Self::ProofState,
        task_id: &str,
        task_extra: Self::TaskState,
        task_weight: u32,
        proof_id: &str,
        task_type: TaskType,
    );

    /// Get debug info for `Self::ProofState`.
    fn debug_proof(proof: &Self::ProofState) -> &str;

    /// Update worker state after it has no more tasks.
    fn post_worker_empty(state: &mut CoordinatorState<Self>, worker: Worker<Self>) -> Worker<Self>;

    /// Get metadata for a task to be sent to the worker (optional - returns empty string by default).
    fn get_task_input_metadata(_state: &CoordinatorState<Self>, _task: &Task<Self>) -> String {
        String::new()
    }

    /// Assign tasks to workers.
    async fn assign_tasks(
        coord: &Arc<Coordinator<Self>>,
        state: OwnedRwLockWriteGuard<CoordinatorState<Self>>,
    ) -> Result<(), Status>;

    /// Get metadata for a proof to be sent to the API upon proof completion.
    fn get_proof_result_metadata(proof: &Proof<Self>) -> Self::ProofResultMetadata;

    /// Get the length of the CPU queue.
    fn cpu_queue_len(state: &CoordinatorState<Self>) -> u32;

    /// Get the length of the GPU queue.
    fn gpu_queue_len(state: &CoordinatorState<Self>) -> u32;

    /// Called when a proof is deleted/removed from the coordinator.
    /// Allows policies to clean up any proof-specific state.
    fn on_proof_deleted(state: &mut CoordinatorState<Self>, proof_id: &str) {
        // Default implementation does nothing
        let _ = (state, proof_id);
    }

    /// Called periodically to print the current state of the coordinator.
    fn debug_state(state: &CoordinatorState<Self>) {
        // Default implementation does nothing extra.
        let _ = state;
    }
}
