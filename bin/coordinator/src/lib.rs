pub mod cluster;
pub mod config;
pub mod latency;
pub mod metrics;
pub mod policy;
pub mod server;
pub mod util;

use dashmap::DashMap;
use eyre::Result;
use mti::prelude::{MagicTypeIdExt, V7};
pub use policy::AssignmentPolicy;

use sp1_cluster_common::consts::CONTROLLER_WEIGHT;
use sp1_cluster_common::proto::{self};
use sp1_cluster_common::proto::{
    server_message, server_sub_message, CancelTask, EndOfStream, GetStatsResponse, ServerMessage,
    ServerSubMessage, TaskData, TaskResult, TaskStatus, TaskType, WorkerTask, WorkerType,
};
use sp1_sdk::SP1_CIRCUIT_VERSION;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{mpsc, OwnedRwLockWriteGuard, RwLock};
use tonic::Status;
use tracing::{instrument, Instrument};

pub const BUILD_VERSION: &str = env!("BUILD_VERSION");

/// The interval in seconds at which the coordinator periodic task should run.
pub const COORDINATOR_PERIODIC_INTERVAL: Duration = Duration::from_secs(10);

/// Estimate the duration of a task based on its type. Used as a heuristic when assigning tasks to
/// workers.
pub fn estimate_duration(task_type: TaskType) -> u128 {
    match task_type {
        TaskType::Controller => 200,
        TaskType::PlonkWrap => 8000,
        TaskType::Groth16Wrap => 2000,
        TaskType::ProveShard => 2000,
        TaskType::RecursionDeferred => 400,
        TaskType::RecursionReduce => 400,
        TaskType::ShrinkWrap => 4000,
        TaskType::SetupVkey => 4000,
        TaskType::MarkerDeferredRecord => 0,
        TaskType::UnspecifiedTaskType => 0,
    }
}

/// Fails the entire proof if the task fails more than the allowed retries.
fn enable_proof_fail(task_type: TaskType) -> bool {
    matches!(
        task_type,
        TaskType::Controller | TaskType::Groth16Wrap | TaskType::PlonkWrap
    )
}

/// The number of retries a task can have before failing fatally.
const MAX_TASK_RETRIES: u8 = 3;

/// The number of seconds a worker can be inactive before it is considered dead.
const WORKER_HEARTBEAT_TIMEOUT: u64 = 30;

/// The default weight of a GPU instance
pub const DEFAULT_GPU_INSTANCE_WEIGHT: u32 = 24;

/// Parse a requester string by stripping "0x" prefix if present and converting to lowercase
pub fn parse_requester(requester: String) -> String {
    let requester = requester.to_lowercase();
    requester
        .strip_prefix("0x")
        .unwrap_or(&requester)
        .to_string()
}

/// A subscriber that is waiting for tasks to complete.
pub struct Subscriber {
    tx: mpsc::UnboundedSender<ServerSubMessage>,
    active_subscriptions: HashSet<String>,
    proof_id: String,
    last_update: SystemTime,
}

/// The task coordinator.
pub struct Coordinator<P: AssignmentPolicy> {
    /// Current state which can be accessed concurrently.
    pub state: Arc<RwLock<CoordinatorState<P>>>,

    /// Thread safe map of subscribers.
    pub subscribers: DashMap<String, Subscriber>,

    /// Metrics for the coordinator
    pub metrics: Option<Arc<metrics::CoordinatorMetrics>>,
}

pub trait TimeSource: Send + Sync {
    fn now(&self) -> SystemTime;
}

pub struct SystemTimeSource;
impl TimeSource for SystemTimeSource {
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }
}

impl<P: AssignmentPolicy> Coordinator<P> {
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(CoordinatorState {
                total_tasks: 0,
                workers: HashMap::new(),
                proofs: HashMap::new(),
                proofs_tx: None,
                shutting_down: false,
                policy: P::default(),
            })),
            subscribers: DashMap::new(),
            metrics: None,
        }
    }

    /// Sets the metrics for the coordinator
    pub fn set_metrics(&mut self, metrics: Arc<metrics::CoordinatorMetrics>) {
        self.metrics = Some(metrics);
    }
}

/// A proof being proven with tasks to complete.
#[derive(Clone)]
pub struct Proof<P: AssignmentPolicy> {
    pub id: String,
    pub tasks: HashMap<String, Task<P>>,
    pub created_at: SystemTime,
    pub expires_at: Option<SystemTime>,
    pub active_tasks: u32,
    pub extra: P::ProofState,
}

impl<P: AssignmentPolicy> Proof<P> {
    pub fn new(id: String, expires_at: Option<SystemTime>, extra: P::ProofState) -> Self {
        Self {
            id,
            tasks: HashMap::new(),
            created_at: SystemTime::now(),
            expires_at,
            active_tasks: 0,
            extra,
        }
    }
}

pub struct ProofResult<P: AssignmentPolicy> {
    pub id: String,
    pub success: bool,
    pub metadata: Option<P::ProofResultMetadata>,
}

/// The current state of the coordinator.
#[derive(Clone)]
pub struct CoordinatorState<P: AssignmentPolicy> {
    /// All time count of tasks created.
    pub total_tasks: u64,

    /// All active workers.
    pub workers: HashMap<String, Worker<P>>,

    /// All active proofs.
    pub proofs: HashMap<String, Proof<P>>,

    /// Channel to send completed proofs with success status to.
    pub proofs_tx: Option<mpsc::UnboundedSender<ProofResult<P>>>,

    /// Whether the worker is shutting down. If true, the coordinator will not send out new tasks
    /// or fail any proofs.
    pub shutting_down: bool,

    /// The assignment policy which tracks queued tasks and has assignment logic.
    pub policy: P,
}

#[derive(Clone)]
pub struct Worker<P: AssignmentPolicy> {
    /// The worker ID.
    pub id: String,

    /// The type of worker.
    pub worker_type: WorkerType,

    /// The maximum weight of the worker.
    pub max_weight: u32,

    /// The estimated unix timestamp in ms that the worker will complete all of its tasks.
    pub next_free_time: u128,

    /// The current weight of active tasks running on the worker.
    pub weight: u32,

    /// The set of active tasks running on the worker.
    pub active_tasks: HashSet<(String, String)>,

    /// The channel to send messages to the worker.
    pub channel: mpsc::UnboundedSender<Result<ServerMessage, Status>>,

    /// The last time the worker sent a heartbeat.
    pub last_heartbeat: u64,

    /// Whether the worker is closed and should not be sent any more tasks.
    pub closed: bool,

    /// Any extra state tracked by the assignment policy.
    pub extra: P::WorkerState,
}

impl<P: AssignmentPolicy> Worker<P> {
    pub fn new(
        id: String,
        worker_type: WorkerType,
        max_weight: u32,
        channel: mpsc::UnboundedSender<Result<ServerMessage, Status>>,
    ) -> Self {
        Self {
            id,
            weight: 0,
            worker_type,
            max_weight,
            next_free_time: 0,
            active_tasks: HashSet::new(),
            last_heartbeat: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            channel,
            closed: false,
            extra: P::WorkerState::default(),
        }
    }
}

/// A task to be completed.
#[derive(Clone)]
pub struct Task<P: AssignmentPolicy> {
    /// The task ID.
    pub id: String,

    /// The task data.
    pub data: TaskData,

    /// The time the task was created.
    pub created_at: SystemTime,

    /// The status of the task.
    /// TODO: this is unused/incorrect?
    pub status: TaskStatus,

    /// The number of times this task has been retried.
    pub retries: u8,

    /// The set of subscribers waiting for this task to complete. Some of them may not exist anymore.
    pub subscribers: HashSet<String>,

    /// The worker that is currently working on this task.
    pub worker: Option<String>,

    /// Any extra state tracked by the assignment policy.
    pub extra: P::TaskState,
}

impl<P: AssignmentPolicy> Default for Coordinator<P> {
    fn default() -> Self {
        Self::new()
    }
}

impl<P: AssignmentPolicy> Coordinator<P> {
    /// Create a proof, returning the controller task ID.
    pub async fn create_proof(
        self: &Arc<Self>,
        request: proto::CreateProofRequest,
    ) -> Result<String, Status> {
        // Get the vm_memory_kb from the request inputs
        // If the memory limit is 0 or not provided, use the default CONTROLLER_WEIGHT
        let weight = *CONTROLLER_WEIGHT;

        let mut state = self.state.clone().write_owned().await;
        if state.shutting_down {
            tracing::info!("Server shutting down, refusing to create proof");
            return Err(Status::failed_precondition("Server shutting down"));
        }

        let proof_extra = P::create_proof_state(&state, &request);

        let expires_at = request
            .expires_at
            .try_into()
            .ok()
            .filter(|&secs| secs > 0)
            .map(|secs| UNIX_EPOCH + Duration::from_secs(secs));

        if state.proofs.contains_key(&request.proof_id) {
            return Err(Status::already_exists(format!(
                "proof {} already exists",
                request.proof_id,
            )));
        }

        state.proofs.insert(
            request.proof_id.clone(),
            Proof::new(request.proof_id.clone(), expires_at, proof_extra),
        );

        let id = self
            .create_task_internal(
                state,
                TaskData {
                    task_type: TaskType::Controller as i32,
                    inputs: request.inputs,
                    outputs: request.outputs,
                    metadata: "{}".to_string(),
                    proof_id: request.proof_id,
                    parent_id: None,
                    weight: weight as u32,
                    requester: request.requester,
                },
            )
            .await?;
        Ok(id)
    }

    pub async fn create_dummy_proof(
        self: &Arc<Self>,
        request: proto::CreateDummyProofRequest,
    ) -> Result<String, Status> {
        let mut state = self.state.clone().write_owned().await;
        if state.shutting_down {
            tracing::info!("Server shutting down, refusing to create proof");
            return Err(Status::failed_precondition("Server shutting down"));
        }

        let expires_at = request
            .expires_at
            .try_into()
            .ok()
            .filter(|&secs| secs > 0)
            .map(|secs| UNIX_EPOCH + Duration::from_secs(secs));

        if state.proofs.contains_key(&request.proof_id) {
            return Err(Status::already_exists(format!(
                "proof {} already exists",
                request.proof_id,
            )));
        }

        state.proofs.insert(
            request.proof_id.clone(),
            Proof::new(
                request.proof_id.clone(),
                expires_at,
                P::ProofState::default(),
            ),
        );
        Ok("".to_string())
    }

    /// Set the channel to send completed proofs to.
    pub async fn set_proofs_tx(&self, tx: mpsc::UnboundedSender<ProofResult<P>>) {
        self.state
            .write()
            .instrument(tracing::debug_span!("acquire_write"))
            .await
            .proofs_tx = Some(tx);
    }

    /// Place a task in the queue.
    pub async fn enqueue_task(self: &Arc<Self>, state: &mut CoordinatorState<P>, task: Task<P>) {
        P::enqueue_task(state, task)
    }

    async fn create_task_internal(
        self: &Arc<Self>,
        mut state: OwnedRwLockWriteGuard<CoordinatorState<P>>,
        task: proto::TaskData,
    ) -> Result<String, Status> {
        // Generate ID
        let id = "task".create_type_id::<V7>().to_string();
        let task = Task {
            id: id.clone(),
            data: task,
            created_at: SystemTime::now(),
            status: TaskStatus::Pending,
            retries: 0,
            subscribers: HashSet::new(),
            worker: None,
            // allocation: None,
            extra: P::TaskState::default(),
        };
        tracing::debug!(
            "create task {} {} {:?}",
            task.data.proof_id,
            task.id,
            task.data.task_type()
        );

        // Update tasks and add task to proof
        state.total_tasks += 1;
        let Some(proof) = state.proofs.get_mut(&task.data.proof_id) else {
            tracing::error!("proof {} not found", task.data.proof_id);
            return Err(Status::not_found(format!(
                "proof {} not found",
                task.data.proof_id
            )));
        };
        proof.active_tasks += 1;
        proof.tasks.insert(task.id.clone(), task.clone());

        // Queue task and assign if possible
        self.enqueue_task(&mut state, task.clone()).await;
        self.assign_tasks(state).await?;

        Ok(id)
    }

    /// Create task and enqueue / assign to a worker if possible.
    pub async fn create_task(self: &Arc<Self>, task: proto::TaskData) -> Result<String, Status> {
        let state = self
            .state
            .clone()
            .write_owned()
            .instrument(tracing::debug_span!("acquire_write"))
            .await;
        track_latency!("worker.create_task", {
            self.create_task_internal(state, task).await
        })
    }

    /// Mark a task as completed.
    pub async fn complete_task(
        self: &Arc<Self>,
        worker_id: String,
        proof_id: String,
        task_id: String,
        metadata: policy::TaskMetadata,
    ) -> Result<(), Status> {
        let mut state = self
            .state
            .clone()
            .write_owned()
            .instrument(tracing::debug_span!("acquire_write"))
            .await;
        track_latency!("worker.complete_task", {
            let Some(proof) = state.proofs.get_mut(&proof_id) else {
                return Err(Status::not_found(format!("proof {} not found", proof_id)));
            };
            let Some(task) = proof.tasks.get_mut(&task_id) else {
                return Err(Status::not_found(format!("task {} not found", task_id)));
            };
            tracing::debug!(
                "[tasks] completing task {} {} {:?} {} {}",
                task_id,
                worker_id,
                task.data.task_type(),
                task.data.proof_id,
                P::debug_proof(&proof.extra),
            );
            // Don't repeat notify if task is already succeeded, which is possible rarely
            // for example when tasks are being retried.
            let subscribers = if task.status == TaskStatus::Succeeded {
                None
            } else {
                if task.status != TaskStatus::FailedFatal {
                    proof.active_tasks -= 1;
                }
                Some(std::mem::take(&mut task.subscribers))
            };
            // Update task status.
            task.status = TaskStatus::Succeeded;

            let remaining_tasks = proof.active_tasks;

            // Calculate task weight in order to update worker weight later.
            let task_type = task.data.task_type();
            let task_weight = task.data.weight;
            let task_extra = task.extra.clone();
            let proof_extra = proof.extra.clone();
            // Drop task here so we can borrow proof as mutable again.
            P::post_task_success_update_proof(proof, &task_extra, metadata);
            // Drop proof here so we can borrow state as mutable again.

            // Cleanup proof if there's no more active tasks. Drop it after state is released.
            let removed = if proof.active_tasks == 0 {
                tracing::info!("Proof {} has no more active tasks, removing", proof_id);
                P::on_proof_deleted(&mut state, &proof_id);
                Some(state.proofs.remove(&proof_id))
            } else {
                None
            };

            P::post_task_success_update_state(&mut state, task_type);

            P::post_task_update_state(
                &mut state,
                proof_extra,
                &task_id,
                task_extra,
                task_weight,
                &proof_id,
                task_type,
            );

            tracing::debug!(
                "Complete task {} for proof {}, {} tasks remaining",
                task_id,
                proof_id,
                remaining_tasks
            );
            // Update worker state.
            if let Some(worker) = state.workers.get_mut(&worker_id) {
                if worker
                    .active_tasks
                    .remove(&(proof_id.clone(), task_id.clone()))
                {
                    worker.weight = worker.weight.saturating_sub(task_weight);

                    // Handle logic if the worker has no more tasks.
                    if worker.active_tasks.is_empty() {
                        let worker = worker.clone();
                        let updated = P::post_worker_empty(&mut state, worker);
                        state.workers.insert(worker_id, updated);
                    }

                    // Assign tasks now that a worker is available.
                    self.assign_tasks(state).await?;
                } else if task_type != TaskType::MarkerDeferredRecord {
                    // This is only an issue if it's not a marker task.
                    tracing::warn!("worker {} was not working on task {}", worker_id, task_id);
                    drop(state);
                } else {
                    drop(state);
                }
            } else {
                tracing::error!(
                    "task {} was assigned to an unknown worker: {} not found",
                    task_id,
                    worker_id
                );
                drop(state);
            }

            // Notify subscribers of this task status update.
            if let Some(subscribers) = subscribers {
                self.notify_subscribers(&subscribers, proof_id, task_id, TaskStatus::Succeeded);
            }
            drop(removed);
        });

        Ok(())
    }

    /// Handle a heartbeat from a worker.
    pub async fn handle_heartbeat(
        self: &Arc<Self>,
        worker_id: &str,
        active_task_proof_ids: &[String],
        active_task_ids: &[String],
        current_weight: u32,
    ) -> Result<(), Status> {
        let mut state = self
            .state
            .clone()
            .write_owned()
            .instrument(tracing::debug_span!("acquire_write"))
            .await;
        let worker = state
            .workers
            .get_mut(worker_id)
            .ok_or_else(|| Status::not_found(format!("worker {} not found", worker_id)))?;
        let mut worker_set = HashSet::new();

        worker.last_heartbeat = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        worker.weight = current_weight;

        // Handle any tasks the worker is working on that are not tracked in the coordinator, and
        // cancel them.
        for (proof_id, task_id) in active_task_proof_ids.iter().zip(active_task_ids.iter()) {
            let tuple = (proof_id.clone(), task_id.clone());
            if !worker.active_tasks.contains(&tuple) {
                tracing::warn!(
                    "worker {} is working on unexpected task {:?}",
                    worker_id,
                    (proof_id, task_id)
                );
                if worker
                    .channel
                    .send(Ok(ServerMessage {
                        message: Some(server_message::Message::CancelTask(CancelTask {
                            task_id: task_id.to_string(),
                            proof_id: proof_id.to_string(),
                        })),
                    }))
                    .is_err()
                {
                    tracing::error!(
                        "Failed to send CancelTask to worker {} (subscriber connection closed?)",
                        worker_id
                    );
                }
            }
            worker_set.insert(tuple);
        }

        // Handle any tasks the worker is not working on that it should be working on, and resend them.
        let worker = worker.clone();
        let mut tasks_to_remove = vec![];
        for tuple in &worker.active_tasks {
            if !worker_set.contains(tuple) {
                tracing::warn!(
                    "worker {} is not working on expected task {:?}",
                    worker_id,
                    tuple
                );
                if let Some(task) = state
                    .proofs
                    .get(&tuple.0)
                    .and_then(|p| p.tasks.get(&tuple.1))
                {
                    let metadata = P::get_task_input_metadata(&state, task);
                    let metadata_string = serde_json::to_string(&metadata).unwrap_or_else(|e| {
                        tracing::error!("Failed to serialize metadata: {}", e);
                        "null".to_string()
                    });
                    self.send_task(task, &worker, &metadata_string);
                } else {
                    tracing::error!("task {} not found", tuple.1);
                    tasks_to_remove.push(tuple.clone());
                }
            }
        }
        let mut should_assign = false;
        let worker = state.workers.get_mut(worker_id).unwrap();
        if !tasks_to_remove.is_empty() {
            for tuple in tasks_to_remove {
                worker.active_tasks.remove(&tuple);
            }
            should_assign = true;
        }
        // As a catchall, handle logic here if the worker is empty.
        if worker.active_tasks.is_empty() {
            let id = worker.id.clone();
            let worker = worker.clone();
            let updated = P::post_worker_empty(&mut state, worker);
            state.workers.insert(id, updated);
            should_assign = true;
        }
        // Assign tasks if it's possible the worker will get new tasks.
        if should_assign {
            self.assign_tasks(state).await.unwrap();
        }
        Ok(())
    }

    /// Check if any workers have timed out.
    pub async fn cleanup_dead_workers(self: &Arc<Self>) {
        let state = self
            .state
            .read()
            .instrument(tracing::debug_span!("acquire"))
            .await;
        track_latency!("coordinator.cleanup_dead_workers", {
            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            let mut dead_workers = vec![];
            for (id, worker) in &state.workers {
                if worker.last_heartbeat + WORKER_HEARTBEAT_TIMEOUT < now {
                    tracing::warn!("worker {} has timed out", id);
                    dead_workers.push(id.clone());
                }
            }
            if !dead_workers.is_empty() {
                drop(state);
                let mut state = self
                    .state
                    .clone()
                    .write_owned()
                    .instrument(tracing::debug_span!("acquire_write"))
                    .await;
                for id in dead_workers {
                    self.remove_worker_internal(&mut state, id).await;
                }
                self.assign_tasks(state).await.unwrap()
            }
        });
    }

    /// Send a heartbeat to all workers.
    pub async fn send_heartbeats(self: &Arc<Self>) {
        let state = self
            .state
            .read()
            .instrument(tracing::debug_span!("acquire"))
            .await;

        track_latency!("coordinator.send_heartbeats", {
            // Create heartbeat message
            let heartbeat = ServerMessage {
                message: Some(server_message::Message::ServerHeartbeat(
                    proto::ServerHeartbeat {},
                )),
            };

            // Send heartbeat to each worker
            for (id, worker) in &state.workers {
                if let Err(e) = worker.channel.send(Ok(heartbeat.clone())) {
                    tracing::warn!("Failed to send heartbeat to worker {}: {}", id, e);
                }
            }
        });
    }

    /// Add a worker to the Coordinator. Returns true if worker already existed.
    pub async fn add_worker(
        self: &Arc<Self>,
        worker_id: String,
        worker_type: WorkerType,
        max_weight: u32,
        channel: mpsc::UnboundedSender<Result<ServerMessage, Status>>,
    ) -> Result<bool> {
        let mut state = self
            .state
            .clone()
            .write_owned()
            .instrument(tracing::debug_span!("acquire_write"))
            .await;

        track_latency!("worker.heartbeat.first", {
            // Check if worker already exists
            if let Some(worker) = state.workers.get_mut(&worker_id) {
                tracing::info!("Worker already exists");
                worker.channel = channel;
                return Ok(true);
            }

            state.workers.insert(
                worker_id.clone(),
                Worker::new(worker_id, worker_type, max_weight, channel),
            );
            // Assign tasks now that a worker is available.
            self.assign_tasks(state).await?;
        });
        Ok(false)
    }

    /// Just remove a worker with borrowed state and without reassigning tasks.
    async fn remove_worker_internal(
        self: &Arc<Self>,
        state: &mut CoordinatorState<P>,
        worker_id: String,
    ) {
        let worker = state.workers.remove(&worker_id).unwrap();
        // Reassign any tasks that were running on this worker.
        for (proof_id, task_id) in &worker.active_tasks {
            let Some(proof) = state.proofs.get(proof_id) else {
                continue;
            };
            let proof_extra = proof.extra.clone();
            let task = proof.tasks.get(task_id).cloned();
            if let Some(task) = task {
                P::post_task_update_state(
                    state,
                    proof_extra,
                    &task.id,
                    task.extra.clone(),
                    task.data.weight,
                    proof_id,
                    task.data.task_type(),
                );
                self.enqueue_task(state, task).await;
            }
        }
        P::post_worker_empty(state, worker);
    }

    /// Close a worker.
    pub async fn close_worker(self: &Arc<Self>, worker_id: String) -> Result<(), Status> {
        let mut state = self
            .state
            .clone()
            .write_owned()
            .instrument(tracing::debug_span!("acquire_write"))
            .await;
        let Some(worker) = state.workers.get_mut(&worker_id) else {
            return Err(Status::not_found(format!("worker {} not found", worker_id)));
        };
        // Set worker to closed
        worker.closed = true;
        Ok(())
    }

    /// Remove a worker.
    pub async fn remove_worker(self: &Arc<Self>, worker_id: String) {
        let mut state = self
            .state
            .clone()
            .write_owned()
            .instrument(tracing::debug_span!("acquire_write"))
            .await;
        self.remove_worker_internal(&mut state, worker_id).await;
        // Reassign tasks to workers
        if let Err(e) = self.assign_tasks(state).await {
            tracing::error!("Failed to reassign tasks: {:?}", e);
        }
    }

    /// Mark a task as failed.
    pub async fn fail_task(
        self: &Arc<Self>,
        worker_id: String,
        proof_id: String,
        task_id: String,
        retryable: bool,
    ) -> Result<(), Status> {
        tracing::debug!("Failing task {} {} {}", proof_id, task_id, retryable);

        let mut state = self
            .state
            .clone()
            .write_owned()
            .instrument(tracing::debug_span!("acquire_write"))
            .await;
        // Ensure the worker is working on this task.
        if let Some(worker) = state.workers.get(&worker_id) {
            if !worker
                .active_tasks
                .contains(&(proof_id.clone(), task_id.clone()))
            {
                return Err(Status::failed_precondition(format!(
                    "worker {} is not working on task {}",
                    worker_id, task_id
                )));
            }
        } else {
            return Err(Status::not_found(format!("worker {} not found", worker_id)));
        };

        let Some(proof) = state.proofs.get_mut(&proof_id) else {
            return Err(Status::not_found(format!("proof {} not found", proof_id)));
        };
        let Some(task) = proof.tasks.get_mut(&task_id) else {
            return Err(Status::not_found(format!("task {} not found", task_id)));
        };

        // If it's a controller task and we won't retry it, we want to manually fail the proof as
        // there's no way to continue.
        let manual_proof_fail = enable_proof_fail(task.data.task_type())
            && (!retryable || task.retries == MAX_TASK_RETRIES);
        // Compute task status and update active_tasks / retries.
        let status = if retryable {
            if task.retries == MAX_TASK_RETRIES {
                tracing::error!("task {} retries exhausted", task_id);
                if task.status != TaskStatus::Succeeded && task.status != TaskStatus::FailedFatal {
                    proof.active_tasks -= 1;
                }

                TaskStatus::FailedFatal
            } else {
                tracing::info!("retrying task {}", task_id);
                task.retries += 1;
                TaskStatus::FailedRetryable
            }
        } else {
            if task.status != TaskStatus::Succeeded && task.status != TaskStatus::FailedFatal {
                proof.active_tasks -= 1;
            }
            TaskStatus::FailedFatal
        };
        // Set task status.
        task.status = status;

        // Clone currently borrowed data so we can reborrow from state.
        let task = task.clone();
        let task_weight = task.data.weight;
        let task_extra = task.extra.clone();
        let proof_extra = proof.extra.clone();
        let subscribers = task.subscribers.clone();

        // Cleanup proof if there's no more active tasks. Drop it after state is released.
        let removed = if !manual_proof_fail && proof.active_tasks == 0 {
            tracing::info!("Proof {} has no more active tasks, removing", proof_id);
            P::on_proof_deleted(&mut state, &proof_id);
            Some(state.proofs.remove(&proof_id))
        } else {
            None
        };

        // Handle manual proof failure.
        if manual_proof_fail {
            tracing::info!("Proof {} controller has no more retries, failing", proof_id);
            self.fail_proof_internal(&mut state, proof_id.clone(), None, true)
                .await?;
        }

        // Have the policy update any state it needs to.
        P::post_task_update_state(
            &mut state,
            proof_extra,
            &task_id,
            task_extra,
            task_weight,
            &proof_id,
            task.data.task_type(),
        );

        // Update worker state.
        if let Some(worker) = state.workers.get_mut(&worker_id) {
            worker
                .active_tasks
                .remove(&(proof_id.clone(), task_id.clone()));
            worker.weight = worker.weight.saturating_sub(task.data.weight);
        }

        // Enqueue the task if it's being retried.
        if status == TaskStatus::FailedRetryable {
            self.enqueue_task(&mut state, task.clone()).await;
        }

        // Reassign tasks since the task is retryable / the worker is available.
        self.assign_tasks(state).await?;

        // Notify subscribers of this task if it's failed fatally.
        if status == TaskStatus::FailedFatal {
            self.notify_subscribers(&subscribers, proof_id, task_id, status);
        }

        drop(removed);

        Ok(())
    }

    /// Assign tasks to workers.
    async fn assign_tasks(
        self: &Arc<Self>,
        state: OwnedRwLockWriteGuard<CoordinatorState<P>>,
    ) -> Result<(), Status> {
        P::assign_tasks(self, state).await
    }

    /// Internal function to get a task.
    pub fn get_task_internal(
        &self,
        state: &CoordinatorState<P>,
        proof_id: &str,
        task_id: &str,
    ) -> Option<Task<P>> {
        state.proofs.get(proof_id)?.tasks.get(task_id).cloned()
    }

    /// Get the status of a task.
    pub async fn get_task_status(
        self: &Arc<Self>,
        proof_id: String,
        task_id: String,
    ) -> Result<TaskStatus, Status> {
        let state = self
            .state
            .read()
            .instrument(tracing::debug_span!("acquire"))
            .await;
        let Some(proof) = state.proofs.get(&proof_id) else {
            return Err(Status::not_found(format!("proof {} not found", proof_id)));
        };
        let Some(task) = proof.tasks.get(&task_id) else {
            return Err(Status::not_found(format!("task {} not found", task_id)));
        };
        Ok(task.status)
    }

    /// Get a task by ID.
    pub async fn get_task(
        self: &Arc<Self>,
        proof_id: String,
        task_id: String,
    ) -> Result<Task<P>, Status> {
        let state = self
            .state
            .read()
            .instrument(tracing::debug_span!("acquire"))
            .await;
        let Some(proof) = state.proofs.get(&proof_id) else {
            return Err(Status::not_found(format!("proof {} not found", proof_id)));
        };
        let Some(task) = proof.tasks.get(&task_id) else {
            return Err(Status::not_found(format!("task {} not found", task_id)));
        };
        Ok(task.clone())
    }

    /// Get the statuses of multiple tasks.
    #[instrument(skip(self))]
    pub async fn get_task_statuses(
        self: &Arc<Self>,
        proof_id: String,
        task_ids: Vec<String>,
    ) -> Result<HashMap<TaskStatus, Vec<String>>, Status> {
        let state = self
            .state
            .read()
            .instrument(tracing::debug_span!("acquire"))
            .await;
        let Some(proof) = state.proofs.get(&proof_id) else {
            return Err(Status::not_found(format!("proof {} not found", proof_id)));
        };
        let mut statuses: HashMap<TaskStatus, Vec<String>> = HashMap::new();
        for task_id in task_ids {
            let Some(task) = proof.tasks.get(&task_id) else {
                return Err(Status::not_found(format!("task {} not found", task_id)));
            };
            statuses.entry(task.status).or_default().push(task_id);
        }
        Ok(statuses)
    }

    /// Mark a proof as completed.
    #[instrument(skip(self))]
    pub async fn complete_proof(self: &Arc<Self>, proof_id: String) -> Result<(), Status> {
        let state = self
            .state
            .read()
            .instrument(tracing::debug_span!("acquire"))
            .await;
        let sender = state.proofs_tx.clone();
        let Some(proof) = state.proofs.get(&proof_id) else {
            tracing::error!("Proof {} not found", proof_id);
            return Err(Status::not_found(format!("Proof {} not found", proof_id)));
        };
        let metadata = P::get_proof_result_metadata(proof);
        drop(state);
        tracing::info!("Completed proof {}", proof_id);
        if let Some(sender) = sender {
            if let Err(e) = sender.send(ProofResult {
                id: proof_id.clone(),
                success: true,
                metadata: Some(metadata),
            }) {
                tracing::error!("Failed to send completed proof: {}", e);
            }
        }
        Ok(())
    }

    /// Get all proof IDs.
    #[instrument(skip(self))]
    pub async fn get_proofs(self: &Arc<Self>) -> Vec<String> {
        self.state
            .read()
            .instrument(tracing::debug_span!("acquire"))
            .await
            .proofs
            .keys()
            .cloned()
            .collect()
    }

    /// Fail a proof. If task id is given, ensure the task id is an expected task within the proof.
    /// This is done to handle cases where a worker is failing a proof from another coordinator
    /// instance due to zero downtime deployments.
    async fn fail_proof_internal(
        self: &Arc<Self>,
        state: &mut CoordinatorState<P>,
        proof_id: String,
        task_id: Option<String>,
        notify_sender: bool,
    ) -> Result<(), Status> {
        if state.shutting_down {
            tracing::info!(
                "Coordinator is shutting down, not failing proof {}",
                proof_id
            );
            return Ok(());
        }
        track_latency!("coordinator.fail_proof", {
            let sender = state.proofs_tx.clone();
            // Cancel all tasks
            P::on_proof_deleted(state, &proof_id);
            let proof = state.proofs.remove(&proof_id);
            let Some(proof) = proof else {
                tracing::warn!("proof {} not found", proof_id);
                return Ok(());
            };
            if let Some(task_id) = task_id {
                if !proof.tasks.contains_key(&task_id) {
                    tracing::warn!(
                        "Ignoring proof failure, task {} not found in proof {}",
                        task_id,
                        proof_id
                    );
                    state.proofs.insert(proof_id.clone(), proof);
                    return Err(Status::not_found(format!(
                        "task {} not found in proof {}",
                        task_id, proof_id
                    )));
                }
            }
            for task in proof.tasks.values() {
                if task.status == TaskStatus::Running {
                    if let Some(worker_id) = &task.worker {
                        if let Some(worker) = state.workers.get_mut(worker_id) {
                            if worker
                                .channel
                                .send(Ok(ServerMessage {
                                    message: Some(server_message::Message::CancelTask(
                                        CancelTask {
                                            proof_id: proof_id.clone(),
                                            task_id: task.id.clone(),
                                        },
                                    )),
                                }))
                                .is_err()
                            {
                                tracing::error!(
                                    "Failed to send CancelTask to worker {}",
                                    worker.id
                                );
                            }
                            worker
                                .active_tasks
                                .remove(&(proof_id.clone(), task.id.clone()));
                            worker.weight = worker.weight.saturating_sub(task.data.weight);

                            // If the worker has no more tasks, handle it.
                            if worker.active_tasks.is_empty() {
                                let id = worker.id.clone();
                                let worker = worker.clone();
                                let updated = P::post_worker_empty(state, worker);
                                state.workers.insert(id, updated);
                            }

                            P::post_task_update_state(
                                state,
                                proof.extra.clone(),
                                &task.id,
                                task.extra.clone(),
                                task.data.weight,
                                &proof_id,
                                task.data.task_type(),
                            );
                        }
                    }
                }
            }

            tracing::info!("Failed proof {} ", proof_id);

            if notify_sender {
                if let Some(sender) = sender {
                    if let Err(e) = sender.send(ProofResult {
                        id: proof_id.clone(),
                        success: false,
                        metadata: None,
                    }) {
                        tracing::error!("Failed to send failed proof: {}", e);
                    }
                }
            }
        });

        Ok(())
    }

    /// Mark a proof as failed. `notify_sender` indicates whether to notify the proof sender channel
    /// (ex. cluster API).
    #[instrument(skip(self))]
    pub async fn fail_proof(
        self: &Arc<Self>,
        proof_id: String,
        task_id: Option<String>,
        notify_sender: bool,
    ) -> Result<(), Status> {
        let mut state = self
            .state
            .clone()
            .write_owned()
            .instrument(tracing::debug_span!("acquire"))
            .await;

        self.fail_proof_internal(&mut state, proof_id, task_id, notify_sender)
            .await
    }

    /// Send a task to a worker.
    pub fn send_task(self: &Arc<Self>, task: &Task<P>, worker: &Worker<P>, metadata: &str) {
        let msg = ServerMessage {
            message: Some(server_message::Message::NewTask(WorkerTask {
                task_id: task.id.clone(),
                data: Some(task.data.clone()),
                metadata: metadata.to_string(),
            })),
        };
        if worker.channel.send(Ok(msg)).is_err() {
            tracing::error!(
                "Failed to send task to worker {}: (channel closed)",
                worker.id
            );
        }
    }

    /// Remove dead subscribers from the Coordinator and send heartbeats to the rest.
    pub async fn cleanup_dead_subscribers(self: &Arc<Self>) {
        track_latency!("coordinator.cleanup_dead_subscribers", {
            self.subscribers.retain(|sub_id, sub| {
                if sub.last_update.elapsed().unwrap_or_default()
                    > COORDINATOR_PERIODIC_INTERVAL.mul_f32(2.0)
                {
                    tracing::debug!("subscriber {} timed out", sub_id);
                    false
                } else {
                    if sub
                        .tx
                        .send(ServerSubMessage {
                            msg_id: "msg".create_type_id::<V7>().to_string(),
                            message: Some(server_sub_message::Message::ServerHeartbeat(
                                proto::ServerSubHeartbeat {},
                            )),
                        })
                        .is_err()
                    {
                        tracing::debug!("Subscriber {} is closed", sub_id);
                    }
                    true
                }
            });
        });
    }

    /// Subscribe a subscriber to a task.
    pub async fn create_subscriber(
        &self,
        sub_id: String,
        proof_id: String,
        task_ids: Vec<String>,
        tx: mpsc::UnboundedSender<ServerSubMessage>,
    ) -> Result<(), Status> {
        tracing::debug!("add subscription {} {} {:?}", sub_id, proof_id, task_ids);
        if let Some(mut sub) = self.subscribers.get_mut(&sub_id) {
            sub.tx = tx;
            sub.last_update = SystemTime::now();
        } else {
            self.subscribers.insert(
                sub_id.clone(),
                Subscriber {
                    tx,
                    active_subscriptions: HashSet::new(),
                    proof_id: proof_id.clone(),
                    last_update: SystemTime::now(),
                },
            );
        }

        if !task_ids.is_empty() {
            self.add_subscriptions(sub_id, task_ids).await?;
        }
        Ok(())
    }

    /// Add tasks to a subscriber.
    pub async fn add_subscriptions(
        &self,
        sub_id: String,
        task_ids: Vec<String>,
    ) -> Result<(), Status> {
        let Some(mut sub) = self.subscribers.get_mut(&sub_id) else {
            return Err(Status::not_found(format!(
                "subscriber {} not found",
                sub_id
            )));
        };
        sub.active_subscriptions.extend(task_ids.iter().cloned());
        sub.last_update = SystemTime::now();
        let proof_id = sub.proof_id.clone();
        let tx = sub.tx.clone();
        // Sub cannot be acquired while acquiring state.
        drop(sub);
        if !task_ids.is_empty() {
            let mut state = self
                .state
                .clone()
                .write_owned()
                .instrument(tracing::debug_span!("acquire_write"))
                .await;
            let proof = state.proofs.get_mut(&proof_id);
            if proof.is_none() {
                let _ = tx.send(ServerSubMessage {
                    msg_id: "msg".create_type_id::<V7>().to_string(),
                    message: Some(server_sub_message::Message::UnknownTask(
                        proto::UnknownTask {
                            proof_id: proof_id.clone(),
                            task_id: task_ids.first().unwrap().clone(),
                        },
                    )),
                });
                return Ok(());
            }
            let proof = proof.unwrap();
            for task_id in &task_ids {
                let task = proof.tasks.get_mut(task_id);
                // If the task doesn't exist, let the subscriber know immediately.
                if task.is_none() {
                    let _ = tx.send(ServerSubMessage {
                        msg_id: "msg".create_type_id::<V7>().to_string(),
                        message: Some(server_sub_message::Message::UnknownTask(
                            proto::UnknownTask {
                                proof_id: proof_id.clone(),
                                task_id: task_id.clone(),
                            },
                        )),
                    });
                    continue;
                }
                // If the task is already finalized, let the subscriber know immediately.
                let task = task.unwrap();
                if task.status == TaskStatus::FailedFatal || task.status == TaskStatus::Succeeded {
                    let _ = tx.send(ServerSubMessage {
                        msg_id: "msg".create_type_id::<V7>().to_string(),
                        message: Some(server_sub_message::Message::TaskResult(proto::TaskResult {
                            task_id: task_id.clone(),
                            task_status: task.status as i32,
                        })),
                    });
                    continue;
                }
                task.subscribers.insert(sub_id.clone());
            }
        }
        Ok(())
    }

    /// Get a worker by ID.
    pub async fn get_worker(&self, worker_id: &str) -> Option<Worker<P>> {
        let state = self
            .state
            .read()
            .instrument(tracing::debug_span!("acquire"))
            .await;
        state.workers.get(worker_id).cloned()
    }

    /// Notify subscribers of a task.
    fn notify_subscribers(
        &self,
        subscribers: &HashSet<String>,
        proof_id: String,
        task_id: String,
        task_status: TaskStatus,
    ) {
        tracing::debug!(
            "notify subscribers {:?} {} {}",
            subscribers,
            proof_id,
            task_id
        );
        for sub_id in subscribers.iter() {
            let sub = self.subscribers.get_mut(sub_id);
            if sub.is_none() {
                continue;
            }
            let mut sub = sub.unwrap();
            let tx = sub.tx.clone();
            sub.active_subscriptions.remove(&task_id);
            let msg = ServerSubMessage {
                msg_id: "msg".create_type_id::<V7>().to_string(),
                message: Some(server_sub_message::Message::TaskResult(TaskResult {
                    task_id: task_id.clone(),
                    task_status: task_status as i32,
                })),
            };
            tracing::debug!("notify {:?} to subscriber {}", msg, sub_id);
            if let Err(e) = tx.send(msg) {
                tracing::error!("Failed to send task result to subscriber: {}", e);
            }
        }
        tracing::debug!("notify subscribers done");
    }

    pub async fn get_info(&self) -> GetStatsResponse {
        let state = self
            .state
            .read()
            .instrument(tracing::debug_span!("acquire"))
            .await;

        GetStatsResponse {
            coordinator_version: BUILD_VERSION.to_string(),
            sp1_circuit_version: SP1_CIRCUIT_VERSION.to_string(),
            total_tasks: state.total_tasks,
            active_tasks: state.proofs.values().map(|p| p.active_tasks).sum::<u32>(),
            cpu_workers: state
                .workers
                .values()
                .filter(|w| w.worker_type == WorkerType::Cpu)
                .count() as u32,
            gpu_workers: state
                .workers
                .values()
                .filter(|w| w.worker_type == WorkerType::Gpu)
                .count() as u32,
            cpu_utilization_current: state
                .workers
                .values()
                .filter(|w| w.worker_type == WorkerType::Cpu)
                .map(|w| w.weight)
                .sum::<u32>(),
            cpu_utilization_max: state
                .workers
                .values()
                .filter(|w| w.worker_type == WorkerType::Cpu)
                .map(|w| w.max_weight)
                .sum::<u32>(),
            gpu_utilization_current: state
                .workers
                .values()
                .filter(|w| w.worker_type == WorkerType::Gpu)
                .map(|w| w.weight)
                .sum::<u32>(),
            gpu_utilization_max: state
                .workers
                .values()
                .filter(|w| w.worker_type == WorkerType::Gpu)
                .map(|w| w.max_weight)
                .sum::<u32>(),
            active_proofs: state.proofs.len() as u32,
            cpu_queue: P::cpu_queue_len(&state),
            gpu_queue: P::gpu_queue_len(&state),
            active_subscribers: self.subscribers.len() as u32,
        }
    }

    /// Print coordinator info.
    pub async fn print_info(&self) {
        let info = self.get_info().await;

        tracing::info!("[coordinator] {:?}", info);
        let state = self
            .state
            .read()
            .instrument(tracing::debug_span!("acquire"))
            .await;
        for worker in state.workers.iter() {
            let task_info = worker
                .1
                .active_tasks
                .iter()
                .map(|(proof_id, task_id)| {
                    (
                        proof_id,
                        task_id,
                        state
                            .proofs
                            .get(proof_id)
                            .map(|p| p.tasks.get(task_id).map(|t| t.data.task_type())),
                    )
                })
                .collect::<Vec<_>>();
            tracing::info!(
                "[coordinator] worker {} {:?} {:?} {} {:?}",
                worker.0,
                worker.1.worker_type,
                worker.1.extra,
                worker.1.next_free_time,
                task_info
            );
        }
        P::debug_state(&state);
    }

    /// Cancel expired proofs.
    pub async fn cleanup_cancel_expired_proofs(self: &Arc<Self>) {
        let state = self
            .state
            .read()
            .instrument(tracing::debug_span!("acquire"))
            .await;

        let now = SystemTime::now();

        let mut proofs_to_remove = vec![];
        for (id, proof) in state.proofs.iter() {
            if proof.expires_at.is_some() && proof.expires_at.unwrap() < now {
                proofs_to_remove.push(id.clone());
            }
        }
        if !proofs_to_remove.is_empty() {
            drop(state);
            let mut state = self
                .state
                .clone()
                .write_owned()
                .instrument(tracing::debug_span!("acquire_write"))
                .await;
            for id in proofs_to_remove {
                if let Err(e) = self.fail_proof_internal(&mut state, id, None, false).await {
                    tracing::error!("Failed to fail expired proof: {}", e);
                }
            }
            self.assign_tasks(state).await.unwrap()
        }
    }

    // Shutdown the coordinator.
    #[instrument(skip(self))]
    pub async fn shutdown(self: &Arc<Self>) {
        let mut state = self
            .state
            .write()
            .instrument(tracing::debug_span!("acquire_write"))
            .await;
        state.shutting_down = true;
        // Cancel all tasks.
        let mut total = 0;
        for worker in state.workers.values_mut() {
            for (proof_id, task_id) in &worker.active_tasks {
                total += 1;
                if let Err(e) = worker.channel.send(Ok(ServerMessage {
                    message: Some(server_message::Message::CancelTask(CancelTask {
                        proof_id: proof_id.clone(),
                        task_id: task_id.clone(),
                    })),
                })) {
                    tracing::error!("Failed to cancel task {} {}: {}", proof_id, task_id, e);
                } else {
                    tracing::info!("Cancelled task {} {}", proof_id, task_id);
                }
            }
            worker.active_tasks.clear();
        }
        tracing::info!("Cancelled {} tasks", total);
        // Close all sub channels.
        let mut subs = 0;
        for entry in &self.subscribers {
            entry
                .value()
                .tx
                .send(ServerSubMessage {
                    msg_id: "msg".create_type_id::<V7>().to_string(),
                    message: Some(server_sub_message::Message::EndOfStream(EndOfStream {})),
                })
                .unwrap();
            subs += 1;
        }
        tracing::info!("Closed {} subscribers", subs);
    }
}
