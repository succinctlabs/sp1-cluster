use std::{
    cmp::{max, Reverse},
    collections::{BinaryHeap, HashMap},
    sync::Arc,
    time::SystemTime,
};

use sp1_cluster_common::proto::{CreateProofRequest, TaskStatus, TaskType, WorkerType};
use tokio::sync::OwnedRwLockWriteGuard;
use tonic::Status;

use crate::{estimate_duration, track_latency, Coordinator, CoordinatorState, Proof, Task, Worker};

use super::{AssignmentPolicy, TaskMetadata};

/// Balanced policy with per-proof GPU task queues
#[derive(Clone, Default)]
pub struct BalancedPolicy {
    pub cpu_queue: BinaryHeap<Reverse<QueuedTask>>,
    /// Map of proof_id to its GPU tasks queue
    pub gpu_queues: HashMap<String, BinaryHeap<Reverse<QueuedTask>>>,
    /// Total weight of GPU tasks currently assigned per proof_id
    pub proof_gpu_weights: HashMap<String, u32>,
}

impl BalancedPolicy {
    fn get_queued_task(state: &CoordinatorState<Self>, task: &Task<Self>) -> QueuedTask {
        let proof = state
            .proofs
            .get(&task.data.proof_id)
            .unwrap_or_else(|| panic!("proof {} not found", task.data.proof_id));
        let proof_created_at = proof.created_at;

        let created_at = task.created_at;
        QueuedTask {
            id: task.id.clone(),
            proof_id: task.data.proof_id.clone(),
            task_type: task.data.task_type(),
            proof_created_at,
            created_at,
        }
    }

    /// Find the next task from the proof with the lowest GPU weight allocation
    /// Returns the proof_id and the next task to be assigned
    fn find_next_balanced_task(&self) -> Option<&QueuedTask> {
        if self.gpu_queues.is_empty() {
            return None;
        }

        let mut best_result = None;
        let mut lowest_weight = u32::MAX;
        let mut oldest_time = SystemTime::UNIX_EPOCH;

        for (proof_id, queue) in &self.gpu_queues {
            if queue.is_empty() {
                continue;
            }

            let current_weight = self.proof_gpu_weights.get(proof_id).copied().unwrap_or(0);

            // Get the proof creation time from the first task in queue for tiebreaking
            let queue_task = &queue.peek().unwrap().0;
            let proof_created_at = queue_task.proof_created_at;

            if current_weight < lowest_weight
                || (current_weight == lowest_weight && proof_created_at < oldest_time)
            {
                lowest_weight = current_weight;
                oldest_time = proof_created_at;
                best_result = Some(queue_task);
            }
        }

        best_result
    }

    async fn assign_task(
        coord: &Arc<Coordinator<Self>>,
        state: &mut CoordinatorState<Self>,
        task: Task<Self>,
    ) -> Result<Option<String>, Status> {
        let best_worker_id = track_latency!("coordinator.assign_task", {
            let task_type = task.data.task_type();
            let worker_type = WorkerType::from_task_type(task_type);

            // Do not assign WorkerType::None tasks.
            if worker_type == WorkerType::None {
                return Ok(None);
            }

            // Assign task by finding worker that will be available soonest.
            let mut best_worker = None;
            let mut best_worker_time = u128::MAX;
            let task_weight = task.data.weight;
            for worker in state.workers.values_mut() {
                if !worker.closed
                    // Matching worker type.
                    && (worker.worker_type == WorkerType::All || worker.worker_type == worker_type)
                    // Find worker with lowest next_free_time.
                    && worker.next_free_time < best_worker_time
                    // Check if worker has enough weight quota.
                    && worker.weight + task_weight <= worker.max_weight
                {
                    best_worker_time = worker.next_free_time;
                    best_worker = Some(worker);
                }
            }

            // Handle state updates if there's a free worker.
            if let Some(worker) = &mut best_worker {
                let start_time = max(
                    SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .map_err(|e| Status::internal(e.to_string()))?
                        .as_millis(),
                    worker.next_free_time,
                );
                let task_duration = estimate_duration(task.data.task_type());
                tracing::debug!(
                    "[tasks] starting task {} {} {:?} {}",
                    task.id,
                    worker.id,
                    task_type,
                    task.data.proof_id,
                );
                tracing::debug!(
                    "[update] worker {} duration {} -> {}, adding task {}",
                    worker.id,
                    start_time,
                    start_time + task_duration,
                    task.id
                );

                worker.weight += task_weight;
                worker.next_free_time = start_time + task_duration;
                worker
                    .active_tasks
                    .insert((task.data.proof_id.clone(), task.id.clone()));

                // Set worker
                let mut_task = state
                    .proofs
                    .get_mut(&task.data.proof_id)
                    .unwrap()
                    .tasks
                    .get_mut(&task.id)
                    .unwrap();
                mut_task.worker = Some(worker.id.clone());
                mut_task.status = TaskStatus::Running;

                // Update proof GPU weight tracking for GPU tasks
                if worker_type == WorkerType::Gpu {
                    *state
                        .policy
                        .proof_gpu_weights
                        .entry(task.data.proof_id.clone())
                        .or_insert(0) += task_weight;
                }
            } else {
                tracing::debug!("no worker available");
            }

            if let Some(worker) = &best_worker {
                coord.send_task(&task, worker, "null");

                // Pop the task from the appropriate queue
                match worker_type {
                    WorkerType::Cpu => {
                        state.policy.cpu_queue.pop();
                    }
                    WorkerType::Gpu => {
                        // Pop from the specific proof's GPU queue
                        if let Some(queue) = state.policy.gpu_queues.get_mut(&task.data.proof_id) {
                            queue.pop();
                            // Clean up empty queues
                            if queue.is_empty() {
                                state.policy.gpu_queues.remove(&task.data.proof_id);
                            }
                        }
                    }
                    _ => {}
                }
            }
            best_worker.map(|worker| worker.id.clone())
        });
        Ok(best_worker_id)
    }
}

#[async_trait::async_trait]
impl AssignmentPolicy for BalancedPolicy {
    type ProofState = ();
    type TaskState = ();
    type WorkerState = ();
    type ProofResultMetadata = ();

    fn create_proof_state(
        _state: &CoordinatorState<Self>,
        _request: &CreateProofRequest,
    ) -> Self::ProofState {
    }

    fn enqueue_task(state: &mut CoordinatorState<Self>, task: Task<Self>) {
        let queued_task = Self::get_queued_task(state, &task);
        let worker_type = WorkerType::from_task_type(task.data.task_type());
        match worker_type {
            WorkerType::Cpu => state.policy.cpu_queue.push(Reverse(queued_task)),
            WorkerType::Gpu => {
                // Add to the proof-specific GPU queue
                state
                    .policy
                    .gpu_queues
                    .entry(task.data.proof_id.clone())
                    .or_default()
                    .push(Reverse(queued_task));
            }
            _ => {}
        }
    }

    fn post_task_success_update_proof(
        _proof: &mut Proof<Self>,
        _task_extra: &Self::TaskState,
        _metadata: TaskMetadata,
    ) {
    }

    fn post_task_success_update_state(_state: &mut CoordinatorState<Self>, _task_type: TaskType) {}

    fn post_task_update_state(
        state: &mut CoordinatorState<Self>,
        _proof_extra: Self::ProofState,
        _task_id: &str,
        _task_extra: Self::TaskState,
        task_weight: u32,
        proof_id: &str,
        task_type: TaskType,
    ) {
        // Decrement GPU weight when a GPU task completes
        if WorkerType::from_task_type(task_type) == WorkerType::Gpu {
            if let Some(current_weight) = state.policy.proof_gpu_weights.get_mut(proof_id) {
                *current_weight = current_weight.saturating_sub(task_weight);
                if *current_weight == 0 {
                    state.policy.proof_gpu_weights.remove(proof_id);
                }
            }
        }
    }

    fn debug_proof(_proof: &Self::ProofState) -> &str {
        ""
    }

    fn post_worker_empty(
        _state: &mut CoordinatorState<Self>,
        mut worker: Worker<Self>,
    ) -> Worker<Self> {
        // Weight cleanup is handled in post_task_update_state when tasks complete
        worker.next_free_time = 0;
        worker.weight = 0;
        worker
    }

    async fn assign_tasks(
        coord: &Arc<Coordinator<Self>>,
        mut state: OwnedRwLockWriteGuard<CoordinatorState<Self>>,
    ) -> Result<(), Status> {
        track_latency!("assign_tasks", {
            if state.shutting_down
                || (state.policy.gpu_queues.is_empty() && state.policy.cpu_queue.is_empty())
            {
                return Ok(());
            }

            // Assign GPU tasks using balanced allocation
            while !state.policy.gpu_queues.is_empty() {
                // Find the next task from the most under-allocated proof
                let queue_task = match state.policy.find_next_balanced_task() {
                    Some(result) => result,
                    None => break,
                };
                let proof_id = queue_task.proof_id.clone();

                let task = coord.get_task_internal(&state, &proof_id, &queue_task.id);
                if task.is_none() {
                    tracing::warn!("gpu queue task {} not found", queue_task.id);
                    // Remove the invalid task from queue
                    if let Some(queue) = state.policy.gpu_queues.get_mut(&proof_id) {
                        queue.pop();
                        if queue.is_empty() {
                            state.policy.gpu_queues.remove(&proof_id);
                        }
                    }
                    continue;
                }
                let task = task.unwrap();

                // If the task is already finalized, remove it from the queue
                if task.status == TaskStatus::Succeeded || task.status == TaskStatus::FailedFatal {
                    tracing::warn!(
                        "gpu queue task is already finalized {} {}",
                        proof_id,
                        task.id
                    );
                    if let Some(queue) = state.policy.gpu_queues.get_mut(&proof_id) {
                        queue.pop();
                        if queue.is_empty() {
                            state.policy.gpu_queues.remove(&proof_id);
                        }
                    }
                    continue;
                }

                // Try to assign the task
                let worker = Self::assign_task(coord, &mut state, task).await?;
                if worker.is_none() {
                    // No available workers, break out of the loop
                    break;
                }
            }

            // Assign CPU tasks (unchanged from default policy)
            while !state.policy.cpu_queue.is_empty() {
                // Get the first task in the queue
                let queue_task = state.policy.cpu_queue.peek().unwrap();

                let task =
                    coord.get_task_internal(&state, &queue_task.0.proof_id, &queue_task.0.id);
                if task.is_none() {
                    tracing::warn!("cpu queue task {} not found", queue_task.0.id);
                    state.policy.cpu_queue.pop();
                    continue;
                }
                let task = task.unwrap();

                // If the task is already finalized, remove it from the queue
                if task.status == TaskStatus::Succeeded || task.status == TaskStatus::FailedFatal {
                    tracing::warn!(
                        "cpu queue task is already finalized {} {}",
                        queue_task.0.proof_id,
                        queue_task.0.id
                    );
                    state.policy.cpu_queue.pop();
                    continue;
                }

                // Try to assign the task
                let worker = Self::assign_task(coord, &mut state, task).await?;
                if worker.is_none() {
                    // No available workers, break out of the loop
                    break;
                }
            }

            Ok(())
        })
    }

    fn get_proof_result_metadata(_proof: &Proof<Self>) -> Self::ProofResultMetadata {}

    fn gpu_queue_len(state: &CoordinatorState<Self>) -> u32 {
        state
            .policy
            .gpu_queues
            .values()
            .map(|q| q.len() as u32)
            .sum()
    }

    fn cpu_queue_len(state: &CoordinatorState<Self>) -> u32 {
        state.policy.cpu_queue.len() as u32
    }

    fn on_proof_deleted(state: &mut CoordinatorState<Self>, proof_id: &str) {
        // Clean up GPU weight tracking for this proof
        state.policy.proof_gpu_weights.remove(proof_id);
    }
}

/// A task that is queued to be run and can be sorted by queue priority.
#[derive(Clone, PartialEq, Eq)]
pub struct QueuedTask {
    pub id: String,
    pub task_type: TaskType,
    pub proof_created_at: SystemTime,
    pub created_at: SystemTime,
    pub proof_id: String,
}

impl Ord for QueuedTask {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self.task_type, other.task_type) {
            // For CPU worker tasks:
            // 1. Order by proof_created_at
            // 2. Order by task created_at
            (
                TaskType::Controller | TaskType::PlonkWrap | TaskType::Groth16Wrap,
                TaskType::Controller | TaskType::PlonkWrap | TaskType::Groth16Wrap,
            ) => self
                .proof_created_at
                .cmp(&other.proof_created_at)
                .then(self.created_at.cmp(&other.created_at)),
            // For GPU worker tasks:
            // 1. SetupVkey tasks
            // 2. Order by proof_created_at
            // 3. ProveShard tasks over all others
            // 4. Order by task created_at
            (TaskType::SetupVkey, TaskType::SetupVkey) => self
                .proof_created_at
                .cmp(&other.proof_created_at)
                .then(self.created_at.cmp(&other.created_at)),
            (TaskType::SetupVkey, _) => std::cmp::Ordering::Less,
            (_, TaskType::SetupVkey) => std::cmp::Ordering::Greater,
            _ => self.proof_created_at.cmp(&other.proof_created_at).then(
                match (self.task_type, other.task_type) {
                    (TaskType::ProveShard, TaskType::ProveShard) => {
                        self.created_at.cmp(&other.created_at)
                    }
                    (TaskType::ProveShard, _) => std::cmp::Ordering::Less,
                    (_, TaskType::ProveShard) => std::cmp::Ordering::Greater,
                    _ => self.created_at.cmp(&other.created_at),
                },
            ),
        }
    }
}

impl PartialOrd for QueuedTask {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
