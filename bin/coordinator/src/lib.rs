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
    server_message, server_sub_message, CancelTask, EndOfStream, GetStatsResponse,
    MessageStreamResponse, ServerMessage, ServerSubMessage, TaskData, TaskResult, TaskStatus,
    TaskType, WorkerTask, WorkerType,
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

/// The git commit this coordinator was built from. Supplied by the base
/// `infra/Dockerfile`'s `VERGEN_GIT_SHA` build ARG/ENV (see build.rs, which maps
/// it to `BUILD_GIT_SHA`); `"unknown"` for plain local builds. Used for the
/// coordinator's own entry in the cluster component manifest.
pub const BUILD_GIT_SHA: &str = env!("BUILD_GIT_SHA");

/// The component name the coordinator reports itself as in the cluster
/// component manifest. Must be in the network's component allowlist.
pub const COORDINATOR_COMPONENT: &str = "coordinator";

/// The interval in seconds at which the coordinator periodic task should run.
pub const COORDINATOR_PERIODIC_INTERVAL: Duration = Duration::from_secs(10);

/// Map a worker type to its network component name for build-identity reporting.
///
/// The receiver contract's component allowlist only has `cpu-node` / `gpu-node`,
/// so:
/// - `Cpu` -> `cpu-node`
/// - `Gpu` -> `gpu-node`
/// - `All` -> `gpu-node`. Lossy compatibility mapping: an `All` worker is
///   GPU-capable and the contract has no dedicated label for it, so it is
///   reported as a gpu-node rather than dropped.
///
/// Returns `None` for `UnspecifiedWorkerType` / `None`, which are not real
/// build-reportable node roles. The caller skips and warns rather than
/// reporting them as a (false) cpu-node. Matched exhaustively (no wildcard) so a
/// new `WorkerType` variant forces a deliberate mapping decision here.
pub fn worker_component_name(worker_type: WorkerType) -> Option<&'static str> {
    match worker_type {
        WorkerType::Cpu => Some("cpu-node"),
        WorkerType::Gpu => Some("gpu-node"),
        WorkerType::All => Some("gpu-node"),
        WorkerType::UnspecifiedWorkerType | WorkerType::None => None,
    }
}

/// Whether a worker counts as a connected GPU node for capacity reporting. Each GPU node drives
/// one GPU, so the count of matching workers is a device count.
///
/// - `Gpu` and `All` both count: `All` is `NodeConfig::default()` and receives GPU tasks.
/// - A closed (draining) worker counts, intentionally: its completions still add to
///   [`CoordinatorState::gpu_busy_ms_total`]. If you exclude it, busy time becomes larger
///   than available time; the SPN stores such a snapshot with only a warning, so the
///   published utilization is silently wrong. Do not add `!closed` here.
/// - An expired heartbeat does not count: the exact complement of the
///   [`Coordinator::cleanup_dead_workers`] condition.
pub fn is_connected_gpu_node(
    worker_type: WorkerType,
    last_heartbeat: u64,
    now: u64,
    heartbeat_timeout_secs: u64,
) -> bool {
    matches!(worker_type, WorkerType::Gpu | WorkerType::All)
        && now.saturating_sub(last_heartbeat) <= heartbeat_timeout_secs
}

/// Group nodes into one [`proto::GpuClassCount`] per distinct `(name, memory_total_bytes)`.
///
/// The `node_count` values sum to the number of input nodes. Nodes with an unidentified GPU
/// group under the empty name with zero VRAM; they are not dropped.
///
/// The result is sorted, so snapshots of an unchanged cluster are identical.
pub fn group_gpu_classes(nodes: impl Iterator<Item = (String, u64)>) -> Vec<proto::GpuClassCount> {
    let mut counts: HashMap<(String, u64), u32> = HashMap::new();
    for node in nodes {
        *counts.entry(node).or_insert(0) += 1;
    }

    let mut classes: Vec<proto::GpuClassCount> = counts
        .into_iter()
        .map(
            |((name, memory_total_bytes), node_count)| proto::GpuClassCount {
                name,
                memory_total_bytes,
                node_count,
            },
        )
        .collect();
    classes.sort_by(|a, b| {
        a.name
            .cmp(&b.name)
            .then(a.memory_total_bytes.cmp(&b.memory_total_bytes))
    });
    classes
}

/// Add one tick to the GPU availability integral: `gpu_nodes * elapsed`.
///
/// `elapsed` must come from a monotonic [`std::time::Instant`]: a wall-clock step must not
/// change the integral. Arithmetic saturates, so the counter cannot decrease.
pub fn advance_gpu_available_ms(total: u64, gpu_nodes: u32, elapsed: Duration) -> u64 {
    let elapsed_ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);
    total.saturating_add(u64::from(gpu_nodes).saturating_mul(elapsed_ms))
}

/// Advance the GPU availability integral of `state` to now, and start the next interval.
///
/// Used by the periodic tick and by [`Coordinator::get_cluster_info`], which must advance
/// the integral itself so its snapshot is not one [`COORDINATOR_PERIODIC_INTERVAL`] behind
/// its busy counter.
///
/// The caller supplies `now` so the integral, the node count, and `observed_at` use one
/// timestamp.
fn advance_gpu_available_integral<P: AssignmentPolicy>(state: &mut CoordinatorState<P>, now: u64) {
    let gpu_nodes = state.connected_gpu_nodes(now).count() as u32;
    // Read the instant once, so no time is lost between two intervals.
    let tick = std::time::Instant::now();
    let elapsed = tick.saturating_duration_since(state.gpu_available_last_tick);
    state.gpu_available_last_tick = tick;
    state.gpu_available_ms_total =
        advance_gpu_available_ms(state.gpu_available_ms_total, gpu_nodes, elapsed);
}

/// The current unix timestamp in seconds.
fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

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
        TaskType::UtilVkeyMapChunk | TaskType::UtilVkeyMapController => 0,
        TaskType::ExecuteOnly => 200,
        TaskType::CoreExecute => 200,
    }
}

/// Fails the entire proof if the task fails more than the allowed retries.
fn enable_proof_fail(task_type: TaskType) -> bool {
    matches!(
        task_type,
        TaskType::Controller
            | TaskType::Groth16Wrap
            | TaskType::PlonkWrap
            | TaskType::ExecuteOnly
            | TaskType::CoreExecute
    )
}

/// The number of retries a task can have before failing fatally.
const MAX_TASK_RETRIES: u8 = 3;

/// The default number of seconds a worker can be inactive before it is considered dead.
/// Configurable per-coordinator via `Settings::worker_heartbeat_timeout_secs`.
pub const DEFAULT_WORKER_HEARTBEAT_TIMEOUT: u64 = 30;

/// How often workers are prompted to prove they are alive.
pub const SERVER_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);

/// Below this, healthy workers get evicted.
///
/// A worker only answers when prompted, and covering for a failed heartbeat
/// path costs it an interval to attempt a beat, an RPC deadline to give up on
/// it, another interval for the next prompt, and a second deadline for the
/// covering beat. Four intervals holds that with margin to spare.
pub const MIN_WORKER_HEARTBEAT_TIMEOUT: u64 = 4 * SERVER_HEARTBEAT_INTERVAL.as_secs();

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

pub struct MessageChannelState {
    inner: std::sync::Mutex<MessageChannelInner>,
}

struct MessageChannelInner {
    subscribers: Vec<mpsc::UnboundedSender<Result<MessageStreamResponse, Status>>>,
    buffer: Vec<Vec<u8>>,
    closed: bool,
    closed_at: Option<std::time::Instant>,
}

impl Default for MessageChannelState {
    fn default() -> Self {
        Self {
            inner: std::sync::Mutex::new(MessageChannelInner {
                subscribers: Vec::new(),
                buffer: Vec::new(),
                closed: false,
                closed_at: None,
            }),
        }
    }
}

impl MessageChannelState {
    /// Buffers the payload and sends it to each live subscriber.
    /// Removes dead subscribers. Drops the payload if the channel is closed.
    fn push(&self, task_id: &str, payload: Vec<u8>) {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if inner.closed {
            tracing::warn!(
                "Dropping message for already-closed task channel {}",
                task_id
            );
            return;
        }
        inner.buffer.push(payload.clone());
        let msg = Ok(MessageStreamResponse {
            message: Some(proto::message_stream_response::Message::Payload(payload)),
        });
        inner.subscribers.retain(|tx| tx.send(msg.clone()).is_ok());
    }

    /// Attaches a subscriber and replays the buffer from `start_offset`.
    /// If the channel is closed, sends EndOfStream after the replay.
    fn attach_subscriber(
        &self,
        start_offset: usize,
    ) -> mpsc::UnboundedReceiver<Result<MessageStreamResponse, Status>> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let (tx, rx) = mpsc::unbounded_channel();
        for payload in inner.buffer.iter().skip(start_offset) {
            let msg = Ok(MessageStreamResponse {
                message: Some(proto::message_stream_response::Message::Payload(
                    payload.clone(),
                )),
            });
            let _ = tx.send(msg);
        }
        if inner.closed {
            let _ = tx.send(Ok(end_of_stream_response()));
        } else {
            inner.subscribers.push(tx);
        }
        rx
    }
}

fn end_of_stream_response() -> MessageStreamResponse {
    MessageStreamResponse {
        message: Some(proto::message_stream_response::Message::EndOfStream(
            proto::EndOfStream {},
        )),
    }
}

/// The task coordinator.
pub struct Coordinator<P: AssignmentPolicy> {
    /// Current state which can be accessed concurrently.
    pub state: Arc<RwLock<CoordinatorState<P>>>,

    /// Thread safe map of subscribers.
    pub subscribers: DashMap<String, Subscriber>,

    /// Message channels keyed by task_id.
    pub task_channels: DashMap<String, MessageChannelState>,

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
                execute_only_mode: false,
                worker_heartbeat_timeout_secs: DEFAULT_WORKER_HEARTBEAT_TIMEOUT,
                gpu_busy_ms_total: 0,
                gpu_available_ms_total: 0,
                gpu_available_last_tick: std::time::Instant::now(),
                counters_since: unix_now(),
            })),
            subscribers: DashMap::new(),
            task_channels: DashMap::new(),
            metrics: None,
        }
    }

    /// Sets the metrics for the coordinator
    pub fn set_metrics(&mut self, metrics: Arc<metrics::CoordinatorMetrics>) {
        self.metrics = Some(metrics);
    }
}

/// The cluster telemetry the coordinator publishes to the API. One state read builds both
/// fields, so they describe the same instant.
#[derive(Clone, Debug)]
pub struct ClusterInfo {
    /// The coordinator's build identity plus one entry per connected worker.
    pub components: Vec<proto::ClusterComponentInfo>,

    /// GPU capacity and utilization from the same state read.
    pub capacity: proto::ClusterCapacitySnapshot,
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
    pub extra_data: Option<String>,
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

    /// Whether coordinator is an execute-only cluster. If so all proof requests only trigger
    /// EXECUTE_ONLY tasks instead of the default CONTROLLER task.
    pub execute_only_mode: bool,

    /// Seconds a worker can be inactive before it is considered dead and its tasks requeue.
    pub worker_heartbeat_timeout_secs: u64,

    /// Monotonic sum of per-task GPU busy time in GPU-milliseconds since [`Self::counters_since`].
    /// `TaskMetadata::gpu_ms` is added on each first successful completion. Kept outside
    /// [`AssignmentPolicy`] so the counter has one meaning for all policies.
    pub gpu_busy_ms_total: u64,

    /// Monotonic integral of the connected GPU node count over time, in GPU-milliseconds,
    /// since [`Self::counters_since`].
    pub gpu_available_ms_total: u64,

    /// The instant of the last integral advance. Monotonic, so a clock step cannot change the
    /// integral.
    pub gpu_available_last_tick: std::time::Instant,

    /// Unix seconds when this process started and reset both counters. Difference two
    /// snapshots only if their `counters_since` values match.
    pub counters_since: u64,
}

impl<P: AssignmentPolicy> CoordinatorState<P> {
    /// The workers that count as connected GPU nodes at `now` (unix seconds), per
    /// [`is_connected_gpu_node`].
    pub fn connected_gpu_nodes(&self, now: u64) -> impl Iterator<Item = &Worker<P>> {
        let heartbeat_timeout_secs = self.worker_heartbeat_timeout_secs;
        self.workers.values().filter(move |worker| {
            is_connected_gpu_node(
                worker.worker_type,
                worker.last_heartbeat,
                now,
                heartbeat_timeout_secs,
            )
        })
    }
}

/// What a worker says about itself when it registers (`OpenRequest`): the build
/// it is running, the GPU it is bound to, and where it runs.
///
/// Grouped rather than passed as four adjacent `String`s, where any two could be
/// swapped at a call site without the compiler noticing.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WorkerIdentity {
    /// Crate version of the worker.
    pub version: String,

    /// Git commit the worker was built from.
    pub git_sha: String,

    /// Container image tag the worker is running.
    pub image_tag: String,

    /// Name of the bound GPU from CUDA, for example "NVIDIA L4". Empty if the worker has
    /// no GPU or could not identify it.
    pub gpu_name: String,

    /// Total VRAM of the bound GPU in bytes. Zero if unknown.
    pub gpu_memory_total_bytes: u64,

    /// Where the worker runs, as an opaque label the worker derives from its
    /// own environment (empty = it could not tell). sp1-cluster does not
    /// interpret this value itself; it is read downstream (e.g. by an
    /// autoscaler) to group workers.
    pub location: String,
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

    /// What the worker reported about itself in `OpenRequest`.
    pub identity: WorkerIdentity,

    /// Any extra state tracked by the assignment policy.
    pub extra: P::WorkerState,
}

impl<P: AssignmentPolicy> Worker<P> {
    pub fn new(
        id: String,
        worker_type: WorkerType,
        max_weight: u32,
        channel: mpsc::UnboundedSender<Result<ServerMessage, Status>>,
        identity: WorkerIdentity,
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
            identity,
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

    /// Number of times this task has been re-enqueued by the dead-worker cleanup path
    /// (heartbeat timeout, not a worker-reported failure).
    ///
    /// Tracking-only. Does NOT consume retry budget and does NOT change `retries`
    /// semantics. Worker disappearance is treated as an infra/liveness event, distinct
    /// from a logical task failure.
    pub dead_worker_requeue_count: u32,

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

        let task_type = if state.execute_only_mode {
            TaskType::ExecuteOnly
        } else {
            TaskType::Controller
        };

        let id = self
            .create_task_internal(
                state,
                TaskData {
                    task_type: task_type as i32,
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

    /// Set execute only mode
    pub async fn set_execute_only_mode(&self, execute_only_mode: bool) {
        self.state
            .write()
            .instrument(tracing::debug_span!("acquire_write"))
            .await
            .execute_only_mode = execute_only_mode;
    }

    /// Set the dead-worker heartbeat timeout (seconds), never below
    /// [`MIN_WORKER_HEARTBEAT_TIMEOUT`].
    ///
    /// Clamped rather than honoured, because under the floor every healthy
    /// worker is evicted and requeued on a loop — a cluster-wide outage, not
    /// the faster failure detection the setting reads like.
    pub async fn set_worker_heartbeat_timeout(&self, secs: u64) {
        let secs = if secs < MIN_WORKER_HEARTBEAT_TIMEOUT {
            tracing::warn!(
                "worker heartbeat timeout {}s is under the {}s floor; using the floor, since \
                 workers cannot prove liveness that fast",
                secs,
                MIN_WORKER_HEARTBEAT_TIMEOUT
            );
            MIN_WORKER_HEARTBEAT_TIMEOUT
        } else {
            secs
        };
        self.state
            .write()
            .instrument(tracing::debug_span!("acquire_write"))
            .await
            .worker_heartbeat_timeout_secs = secs;
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
            dead_worker_requeue_count: 0,
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
                return Err(Status::not_found(format!("proof {proof_id} not found")));
            };
            let Some(task) = proof.tasks.get_mut(&task_id) else {
                return Err(Status::not_found(format!("task {task_id} not found")));
            };
            tracing::debug!(
                "[tasks] completing task {} {} {:?} {} {}",
                task_id,
                worker_id,
                task.data.task_type(),
                task.data.proof_id,
                P::debug_proof(&proof.extra),
            );
            // Copy the GPU busy time before the policy takes ownership of `metadata` below.
            let gpu_ms = metadata.gpu_ms;
            // A task can complete twice — a retry, or a preempted worker's reporter
            // landing next to its redelivery's. Only the first report notifies and
            // runs the success hooks; those record billing and scheduling history,
            // charged once per task.
            let already_succeeded = task.status == TaskStatus::Succeeded;
            let subscribers = if already_succeeded {
                None
            } else {
                if task.status != TaskStatus::FailedFatal {
                    proof.active_tasks -= 1;
                }
                Some(std::mem::take(&mut task.subscribers))
            };
            // Update task status.
            task.status = TaskStatus::Succeeded;

            self.close_task_channel(&task_id);

            let remaining_tasks = proof.active_tasks;

            // Calculate task weight in order to update worker weight later.
            let task_type = task.data.task_type();
            let task_weight = task.data.weight;
            let task_extra = task.extra.clone();
            let proof_extra = proof.extra.clone();
            // Drop task here so we can borrow proof as mutable again.
            if !already_succeeded {
                P::post_task_success_update_proof(proof, &task_extra, metadata);
                P::post_task_success_update_state(&mut state, task_type);
            }

            // Cleanup proof if there's no more active tasks. Drop it after state is released.
            let mut released_assignments = false;
            let removed = if remaining_tasks == 0 {
                tracing::info!("Proof {} has no more active tasks, removing", proof_id);
                P::on_proof_deleted(&mut state, &proof_id);
                let removed = state.proofs.remove(&proof_id);
                // A stale completion can finish a proof while a redelivered copy of
                // its final task still runs elsewhere. With the proof gone, that
                // worker's own report dies on NotFound, so its assignment must be
                // released here or its slot and policy weight leak.
                if let Some(proof) = &removed {
                    released_assignments = Self::release_remaining_assignments(
                        &mut state,
                        proof,
                        &proof_id,
                        Some((&worker_id, &task_id)),
                    );
                }
                removed
            } else {
                None
            };

            // Accumulate cluster GPU busy time. Both conditions are necessary.
            //
            // `!already_succeeded`: only the completion that moved the task to Succeeded
            // counts; a racing retry or a redelivered copy must not count the same device
            // time twice.
            //
            // `completed_on_gpu_node`: busy time is credited only for Gpu/All workers, the
            // set `gpu_available_ms_total` integrates over. Without this, a CPU-only cluster
            // (`SP1_CLUSTER_CPU_ONLY`) reports busy > 0 against available == 0, and the SPN
            // stores that as a silently wrong utilization. Do not also test `closed` or the
            // heartbeat here.
            let completed_on_gpu_node = state.workers.get(&worker_id).is_some_and(|worker| {
                matches!(worker.worker_type, WorkerType::Gpu | WorkerType::All)
            });
            if !already_succeeded && completed_on_gpu_node {
                state.gpu_busy_ms_total = state.gpu_busy_ms_total.saturating_add(gpu_ms);
            }

            // Policy weight is charged when a worker takes the task and released when it
            // gives it up. A completion from a worker that no longer holds it — preempted,
            // or superseded by a redelivery — was already released by whoever took it.
            let still_owned = state.workers.get(&worker_id).is_some_and(|w| {
                w.active_tasks
                    .contains(&(proof_id.clone(), task_id.clone()))
            });
            if still_owned {
                P::post_task_update_state(
                    &mut state,
                    proof_extra,
                    &task_id,
                    task_extra,
                    task_weight,
                    &proof_id,
                    task_type,
                );
            }

            tracing::debug!(
                "Complete task {} for proof {}, {} tasks remaining",
                task_id,
                proof_id,
                remaining_tasks
            );
            // Update worker state.
            let mut capacity_freed = released_assignments;
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
                    capacity_freed = true;
                } else if task_type != TaskType::MarkerDeferredRecord {
                    // This is only an issue if it's not a marker task.
                    tracing::warn!("worker {} was not working on task {}", worker_id, task_id);
                }
            } else {
                // Expected under dead-worker churn (e.g. spot eviction): the worker
                // was cleaned up between assignment and completion. Logged at warn so
                // it doesn't dominate error logs.
                tracing::warn!(
                    "task {} was assigned to an unknown worker: {} not found (possibly evicted)",
                    task_id,
                    worker_id
                );
            }
            if capacity_freed {
                self.assign_tasks(state).await?;
            } else {
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

    /// Releases every assignment still held on `proof`'s tasks, except `skip` —
    /// the (worker, task) pair the caller settles itself. Returns whether
    /// anything was released.
    fn release_remaining_assignments(
        state: &mut CoordinatorState<P>,
        proof: &Proof<P>,
        proof_id: &str,
        skip: Option<(&str, &str)>,
    ) -> bool {
        let mut released = false;
        for task in proof.tasks.values() {
            let Some(owner) = &task.worker else {
                continue;
            };
            if Some((owner.as_str(), task.id.as_str())) == skip {
                continue;
            }
            let Some(worker) = state.workers.get_mut(owner) else {
                continue;
            };
            if !worker
                .active_tasks
                .remove(&(proof_id.to_string(), task.id.clone()))
            {
                continue;
            }
            released = true;
            if worker
                .channel
                .send(Ok(ServerMessage {
                    message: Some(server_message::Message::CancelTask(CancelTask {
                        proof_id: proof_id.to_string(),
                        task_id: task.id.clone(),
                    })),
                }))
                .is_err()
            {
                tracing::error!("Failed to send CancelTask to worker {}", worker.id);
            }
            worker.weight = worker.weight.saturating_sub(task.data.weight);
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
                proof_id,
                task.data.task_type(),
            );
        }
        released
    }

    /// Total weight of the tasks this coordinator has assigned to a worker.
    ///
    /// Heartbeats carry the worker's own count, but it snapshots that before
    /// taking delivery of tasks already sent to it. Trusting the snapshot
    /// would free capacity that is spoken for and pile more work on a worker
    /// that is already behind.
    fn assigned_weight(
        state: &CoordinatorState<P>,
        active_tasks: &HashSet<(String, String)>,
    ) -> u32 {
        active_tasks
            .iter()
            .filter_map(|(proof_id, task_id)| state.proofs.get(proof_id)?.tasks.get(task_id))
            .map(|task| task.data.weight)
            .sum()
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
            .ok_or_else(|| Status::not_found(format!("worker {worker_id} not found")))?;
        let mut worker_set = HashSet::new();

        worker.last_heartbeat = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();

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
        let assigned = Self::assigned_weight(&state, &worker.active_tasks);
        if assigned != current_weight {
            tracing::debug!(
                "worker {} reports weight {} against {} assigned",
                worker_id,
                current_weight,
                assigned
            );
        }

        let mut should_assign = false;
        let worker = state.workers.get_mut(worker_id).unwrap();
        worker.weight = assigned;
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

    /// Advance the GPU availability integral by one periodic tick.
    ///
    /// Adds `connected GPU nodes * time since the previous tick`: the GPU time the cluster
    /// had available, the denominator for `gpu_busy_ms_total`.
    ///
    /// The node count is sampled at the end of each interval, so the error is at most one
    /// interval per node join or leave: small against the 240s push cadence.
    pub async fn accumulate_gpu_available_ms(self: &Arc<Self>) {
        let mut state = self
            .state
            .clone()
            .write_owned()
            .instrument(tracing::debug_span!("acquire_write"))
            .await;

        track_latency!("coordinator.accumulate_gpu_available_ms", {
            advance_gpu_available_integral(&mut state, unix_now());
        });
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
            // (worker_id, worker_type, heartbeat_age_secs at time of cleanup)
            let mut dead_workers: Vec<(String, WorkerType, u64)> = vec![];
            for (id, worker) in &state.workers {
                if worker.last_heartbeat + state.worker_heartbeat_timeout_secs < now {
                    let heartbeat_age = now.saturating_sub(worker.last_heartbeat);
                    tracing::warn!(
                        "worker {} has timed out (last heartbeat {}s ago)",
                        id,
                        heartbeat_age
                    );
                    dead_workers.push((id.clone(), worker.worker_type, heartbeat_age));
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
                for (id, worker_type, heartbeat_age) in dead_workers {
                    if let Some(metrics) = &self.metrics {
                        metrics.increment_dead_workers(worker_type);
                        metrics.record_dead_worker_heartbeat_age(worker_type, heartbeat_age as f64);
                    }
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
    ///
    /// `identity` is what the worker reported in `OpenRequest`: its build
    /// (surfaced via the cluster component manifest) and where it runs
    /// (consumed downstream, e.g. by an autoscaler, to group workers). See
    /// [`WorkerIdentity`].
    pub async fn add_worker(
        self: &Arc<Self>,
        worker_id: String,
        worker_type: WorkerType,
        max_weight: u32,
        channel: mpsc::UnboundedSender<Result<ServerMessage, Status>>,
        identity: WorkerIdentity,
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
                // Refresh the identity: a worker reconnecting after an upgrade
                // re-sends OpenRequest with new build fields. Without this the
                // coordinator would report the worker's pre-upgrade identity.
                worker.identity = identity;
                return Ok(true);
            }

            state.workers.insert(
                worker_id.clone(),
                Worker::new(worker_id, worker_type, max_weight, channel, identity),
            );
            // Assign tasks now that a worker is available.
            self.assign_tasks(state).await?;
        });
        Ok(false)
    }

    /// Just remove a worker with borrowed state and without reassigning tasks.
    ///
    /// Worker disappearance is treated as an infra/liveness event, not a logical
    /// task failure. Tasks are re-enqueued WITHOUT incrementing `task.retries`,
    /// WITHOUT decrementing `proof.active_tasks`, and WITHOUT closing the task
    /// channel. A tracking counter `Task::dead_worker_requeue_count` and the
    /// `coordinator_dead_worker_*` metrics observe this path.
    async fn remove_worker_internal(
        self: &Arc<Self>,
        state: &mut CoordinatorState<P>,
        worker_id: String,
    ) {
        let Some(worker) = state.workers.remove(&worker_id) else {
            tracing::warn!(
                "remove_worker_internal called for unknown worker: {}",
                worker_id
            );
            if let Some(metrics) = &self.metrics {
                metrics.increment_orphan_worker_removals("unknown_worker");
            }
            return;
        };
        let worker_type = worker.worker_type;
        // Reassign any tasks that were running on this worker.
        for (proof_id, task_id) in &worker.active_tasks {
            // Look up proof + task with a single scoped mutable borrow so we can both
            // increment the tracking counter on the stored task and clone it for the
            // queue. The borrow ends before we touch `state` again for
            // `post_task_update_state` / `enqueue_task`.
            let (proof_extra, task) = {
                let Some(proof) = state.proofs.get_mut(proof_id) else {
                    tracing::warn!(
                        "dead-worker cleanup: proof {} not found for task {} (worker {})",
                        proof_id,
                        task_id,
                        worker_id
                    );
                    if let Some(metrics) = &self.metrics {
                        metrics.increment_dead_worker_missing_proof();
                    }
                    continue;
                };
                let proof_extra = proof.extra.clone();
                let Some(stored) = proof.tasks.get_mut(task_id) else {
                    tracing::warn!(
                        "dead-worker cleanup: task {} not found in proof {} (worker {})",
                        task_id,
                        proof_id,
                        worker_id
                    );
                    if let Some(metrics) = &self.metrics {
                        metrics.increment_dead_worker_missing_task();
                    }
                    continue;
                };
                // Tracking-only: record that this task was re-enqueued via the
                // dead-worker path. Does NOT consume retry budget.
                stored.dead_worker_requeue_count =
                    stored.dead_worker_requeue_count.saturating_add(1);
                (proof_extra, stored.clone())
            };
            let task_type = task.data.task_type();
            if let Some(metrics) = &self.metrics {
                metrics.increment_dead_worker_requeues(worker_type, task_type);
            }
            P::post_task_update_state(
                state,
                proof_extra,
                &task.id,
                task.extra.clone(),
                task.data.weight,
                proof_id,
                task_type,
            );
            self.enqueue_task(state, task).await;
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
            return Err(Status::not_found(format!("worker {worker_id} not found")));
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

    /// Mark a task as failed. A task already recorded as succeeded keeps its status
    /// and only releases its worker slot.
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
                    "worker {worker_id} is not working on task {task_id}"
                )));
            }
        } else {
            return Err(Status::not_found(format!("worker {worker_id} not found")));
        };

        let Some(proof) = state.proofs.get_mut(&proof_id) else {
            return Err(Status::not_found(format!("proof {proof_id} not found")));
        };
        let Some(task) = proof.tasks.get_mut(&task_id) else {
            return Err(Status::not_found(format!("task {task_id} not found")));
        };

        // A success already recorded is final. Delivery is at-least-once, so a second
        // execution of a finished task can report failure afterwards — typically because
        // it found inputs the first execution had already reclaimed.
        if task.status == TaskStatus::Succeeded {
            let task_weight = task.data.weight;
            let task_type = task.data.task_type();
            let task_extra = task.extra.clone();
            let proof_extra = proof.extra.clone();
            tracing::warn!("Ignoring failure for already-succeeded task {}", task_id);
            // The task really did finish, so release it the way a completion would.
            P::post_task_update_state(
                &mut state,
                proof_extra,
                &task_id,
                task_extra,
                task_weight,
                &proof_id,
                task_type,
            );
            if let Some(worker) = state.workers.get_mut(&worker_id) {
                worker
                    .active_tasks
                    .remove(&(proof_id.clone(), task_id.clone()));
                worker.weight = worker.weight.saturating_sub(task_weight);
                if worker.active_tasks.is_empty() {
                    let worker = worker.clone();
                    let updated = P::post_worker_empty(&mut state, worker);
                    state.workers.insert(worker_id, updated);
                }
            }
            return self.assign_tasks(state).await;
        }

        // If it's a controller task and we won't retry it, we want to manually fail the proof as
        // there's no way to continue.
        let manual_proof_fail = enable_proof_fail(task.data.task_type())
            && (!retryable || task.retries == MAX_TASK_RETRIES);
        // Compute task status and update active_tasks / retries.
        let status = if retryable {
            if task.retries == MAX_TASK_RETRIES {
                tracing::error!("task {} retries exhausted", task_id);
                if task.status != TaskStatus::FailedFatal {
                    proof.active_tasks -= 1;
                }

                TaskStatus::FailedFatal
            } else {
                tracing::info!("retrying task {}", task_id);
                task.retries += 1;
                TaskStatus::FailedRetryable
            }
        } else {
            if task.status != TaskStatus::FailedFatal {
                proof.active_tasks -= 1;
            }
            TaskStatus::FailedFatal
        };
        // Set task status.
        task.status = status;

        self.close_task_channel(&task_id);

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
            let removed = state.proofs.remove(&proof_id);
            // Same orphan as complete_task's removal: another worker can still hold
            // a redelivered copy of one of this proof's tasks.
            if let Some(proof) = &removed {
                Self::release_remaining_assignments(
                    &mut state,
                    proof,
                    &proof_id,
                    Some((&worker_id, &task_id)),
                );
            }
            removed
        } else {
            None
        };

        // Handle manual proof failure.
        if manual_proof_fail {
            tracing::info!("Proof {} controller has no more retries, failing", proof_id);
            // The reporting pair's release belongs to this function's tail.
            self.fail_proof_internal(
                &mut state,
                proof_id.clone(),
                None,
                true,
                None,
                Some((&worker_id, &task_id)),
            )
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
            return Err(Status::not_found(format!("proof {proof_id} not found")));
        };
        let Some(task) = proof.tasks.get(&task_id) else {
            return Err(Status::not_found(format!("task {task_id} not found")));
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
            return Err(Status::not_found(format!("proof {proof_id} not found")));
        };
        let Some(task) = proof.tasks.get(&task_id) else {
            return Err(Status::not_found(format!("task {task_id} not found")));
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
            return Err(Status::not_found(format!("proof {proof_id} not found")));
        };
        let mut statuses: HashMap<TaskStatus, Vec<String>> = HashMap::new();
        for task_id in task_ids {
            let Some(task) = proof.tasks.get(&task_id) else {
                return Err(Status::not_found(format!("task {task_id} not found")));
            };
            statuses.entry(task.status).or_default().push(task_id);
        }
        Ok(statuses)
    }

    /// Mark a proof as completed.
    #[instrument(skip(self))]
    pub async fn complete_proof(
        self: &Arc<Self>,
        proof_id: String,
        extra_data: Option<String>,
    ) -> Result<(), Status> {
        let state = self
            .state
            .read()
            .instrument(tracing::debug_span!("acquire"))
            .await;
        let sender = state.proofs_tx.clone();
        let Some(proof) = state.proofs.get(&proof_id) else {
            tracing::error!("Proof {} not found", proof_id);
            return Err(Status::not_found(format!("Proof {proof_id} not found")));
        };
        let metadata = P::get_proof_result_metadata(proof);
        drop(state);
        tracing::info!("Completed proof {}", proof_id);
        if let Some(sender) = sender {
            if let Err(e) = sender.send(ProofResult {
                id: proof_id.clone(),
                success: true,
                metadata: Some(metadata),
                extra_data,
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
        extra_data: Option<String>,
        skip: Option<(&str, &str)>,
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
            // Vet the blamed task before tearing anything down: `on_proof_deleted` discards
            // policy state that re-inserting the proof would not restore.
            if let (Some(task_id), Some(proof)) = (task_id.as_ref(), state.proofs.get(&proof_id)) {
                let Some(task) = proof.tasks.get(task_id) else {
                    tracing::warn!(
                        "Ignoring proof failure, task {} not found in proof {}",
                        task_id,
                        proof_id
                    );
                    return Err(Status::not_found(format!(
                        "task {task_id} not found in proof {proof_id}"
                    )));
                };
                // The blamed task already succeeded, so the proof must outlive the
                // duplicate's report.
                if task.status == TaskStatus::Succeeded {
                    tracing::warn!(
                        "Ignoring proof failure blamed on already-succeeded task {}",
                        task_id
                    );
                    return Ok(());
                }
            }
            P::on_proof_deleted(state, &proof_id);
            let proof = state.proofs.remove(&proof_id);
            let Some(proof) = proof else {
                tracing::warn!("proof {} not found", proof_id);
                return Ok(());
            };
            // Deliver a terminal TaskResult to each task's subscribers so they drain
            // via the normal completion path once the proof is gone.
            for task in proof.tasks.values() {
                if !task.subscribers.is_empty() {
                    self.notify_subscribers(
                        &task.subscribers,
                        proof_id.clone(),
                        task.id.clone(),
                        TaskStatus::FailedFatal,
                    );
                }
            }
            // Holder-gated on `active_tasks`, not task status: a Succeeded task can
            // still be held as a redelivered copy, and a requeued Running task's
            // stale `task.worker` must not release a hold that is already gone.
            Self::release_remaining_assignments(state, &proof, &proof_id, skip);

            for task in proof.tasks.values() {
                self.close_task_channel(&task.id);
            }

            tracing::info!("Failed proof {} ", proof_id);

            if notify_sender {
                if let Some(sender) = sender {
                    if let Err(e) = sender.send(ProofResult {
                        id: proof_id.clone(),
                        success: false,
                        metadata: None,
                        extra_data,
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
        extra_data: Option<String>,
    ) -> Result<(), Status> {
        let mut state = self
            .state
            .clone()
            .write_owned()
            .instrument(tracing::debug_span!("acquire"))
            .await;

        self.fail_proof_internal(
            &mut state,
            proof_id,
            task_id,
            notify_sender,
            extra_data,
            None,
        )
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

    /// Remove task channel entries that have been closed for longer than the sweep threshold,
    /// or whose task no longer exists in any proof.
    pub fn cleanup_stale_task_channels(&self) {
        const STALE_THRESHOLD: Duration = Duration::from_secs(60);
        self.task_channels.retain(|_task_id, state| {
            let inner = state.inner.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(closed_at) = inner.closed_at {
                if closed_at.elapsed() > STALE_THRESHOLD {
                    return false;
                }
            }
            true
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
            return Err(Status::not_found(format!("subscriber {sub_id} not found")));
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
                // Emit UnknownTask for every stale task_id the subscriber sent.
                for task_id in &task_ids {
                    let _ = tx.send(ServerSubMessage {
                        msg_id: "msg".create_type_id::<V7>().to_string(),
                        message: Some(server_sub_message::Message::UnknownTask(
                            proto::UnknownTask {
                                proof_id: proof_id.clone(),
                                task_id: task_id.clone(),
                            },
                        )),
                    });
                }
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

    /// Build the cluster telemetry the manifest push publishes: the component build manifest
    /// and the GPU capacity snapshot.
    ///
    /// The manifest is the coordinator's build identity plus one entry per connected worker,
    /// named from the network's allowlist: "coordinator", "gpu-node", "cpu-node". One
    /// state-lock acquisition reads everything, so all fields describe `observed_at`.
    ///
    /// The lock is a write lock because this function must advance the availability integral
    /// itself; if it does not, the availability figure is up to one
    /// [`COORDINATOR_PERIODIC_INTERVAL`] behind the busy counter. This runs once per push
    /// (240s), so the write lock is cheap.
    pub async fn get_cluster_info(&self) -> ClusterInfo {
        // Acquire the same state lock used elsewhere; keep the registry in-memory.
        let mut state = self
            .state
            .write()
            .instrument(tracing::debug_span!("acquire_write"))
            .await;

        let observed_at = unix_now();
        advance_gpu_available_integral(&mut state, observed_at);

        // Coordinator's own build identity first.
        let mut components = vec![proto::ClusterComponentInfo {
            component: COORDINATOR_COMPONENT.to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            git_sha: BUILD_GIT_SHA.to_string(),
            image_tag: std::env::var("IMAGE_TAG").unwrap_or_default(),
        }];

        // One entry per connected worker (keyed by build identity, not instance).
        // Workers with a non-reportable worker_type (Unspecified/None) are skipped
        // with a warning rather than reported under a false component name. Same-build
        // workers produce identical entries; the fulfiller dedupes them downstream.
        components.extend(state.workers.values().filter_map(|w| {
            let Some(component) = worker_component_name(w.worker_type) else {
                tracing::warn!(
                    worker_id = %w.id,
                    worker_type = ?w.worker_type,
                    "skipping worker with non-reportable worker_type in cluster component manifest",
                );
                return None;
            };
            Some(proto::ClusterComponentInfo {
                component: component.to_string(),
                version: w.identity.version.clone(),
                git_sha: w.identity.git_sha.clone(),
                image_tag: w.identity.image_tag.clone(),
            })
        }));

        // Only workers that pass `is_connected_gpu_node` count. `gpu_nodes` is the sum of the
        // breakdown, so the two always agree.
        let gpus = group_gpu_classes(state.connected_gpu_nodes(observed_at).map(|w| {
            (
                w.identity.gpu_name.clone(),
                w.identity.gpu_memory_total_bytes,
            )
        }));
        let capacity = proto::ClusterCapacitySnapshot {
            observed_at,
            counters_since: state.counters_since,
            gpu_nodes: gpus.iter().map(|class| class.node_count).sum(),
            gpu_available_ms_total: state.gpu_available_ms_total,
            gpu_busy_ms_total: state.gpu_busy_ms_total,
            gpus,
        };

        ClusterInfo {
            components,
            capacity,
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
                if let Err(e) = self
                    .fail_proof_internal(&mut state, id, None, false, None, None)
                    .await
                {
                    tracing::error!("Failed to fail expired proof: {}", e);
                }
            }
            self.assign_tasks(state).await.unwrap()
        }
    }

    /// True if more messages can arrive on this task's channel. The task must
    /// exist in a proof and must not be final. A retrying task is not final:
    /// its next run can recreate an entry under the same task id.
    fn task_can_still_send(state: &CoordinatorState<P>, task_id: &str) -> bool {
        state.proofs.values().any(|proof| {
            proof.tasks.get(task_id).is_some_and(|task| {
                !matches!(task.status, TaskStatus::Succeeded | TaskStatus::FailedFatal)
            })
        })
    }

    /// Sends a message on a task's channel. Creates the entry if the task is
    /// live. Buffers the payload so late subscribers can replay it. Drops
    /// messages for finished or unknown tasks: their entry would stay open
    /// forever, and its subscribers would never get EndOfStream.
    pub async fn send_task_message(&self, task_id: &str, payload: Vec<u8>) {
        if let Some(state) = self.task_channels.get(task_id) {
            state.push(task_id, payload);
            return;
        }
        // Tasks finalize and close their channel under the state write lock.
        // Holding the read lock across the insert stops a task from finishing
        // between the liveness check and the insert.
        let state = self.state.read().await;
        // A concurrent call can create the entry during the lock wait.
        // Re-check and use it.
        if let Some(chan) = self.task_channels.get(task_id) {
            chan.push(task_id, payload);
        } else if Self::task_can_still_send(&state, task_id) {
            self.open_channel(&state, task_id).push(task_id, payload);
        } else if state
            .proofs
            .values()
            .any(|proof| proof.tasks.contains_key(task_id))
        {
            tracing::warn!("Dropping message for finished task {}", task_id);
        } else {
            // Stragglers from a removed proof are expected during teardown.
            tracing::debug!("Dropping message for unknown task {}", task_id);
        }
    }

    /// Creates or gets a task's channel entry. Takes the state guard the
    /// caller holds. While the guard is held, the task cannot finalize and
    /// leave behind an open channel that never ends.
    fn open_channel(
        &self,
        _state: &CoordinatorState<P>,
        task_id: &str,
    ) -> dashmap::mapref::one::RefMut<'_, String, MessageChannelState> {
        self.task_channels.entry(task_id.to_string()).or_default()
    }

    /// Close a task's message channel, sending end_of_stream to all current subscribers.
    /// The entry is kept (marked closed) so late subscribers can replay buffered messages.
    pub fn close_task_channel(&self, task_id: &str) {
        if let Some(state) = self.task_channels.get(task_id) {
            let mut inner = state.inner.lock().unwrap_or_else(|e| e.into_inner());
            if inner.closed {
                return;
            }
            inner.closed = true;
            inner.closed_at = Some(std::time::Instant::now());
            let eos = Ok(end_of_stream_response());
            for tx in inner.subscribers.drain(..) {
                tx.send(eos.clone()).ok();
            }
        }
    }

    /// Subscribes to a task's message channel and returns the receiver end.
    /// Replays buffered messages from `start_offset`. If the channel is
    /// closed, sends the buffer and then EndOfStream. If the task is finished
    /// or unknown and has no entry, sends only EndOfStream. That case is a
    /// subscriber that reconnects after the GC removed the closed entry. The
    /// buffer is gone, and a fresh open channel would never end.
    pub async fn subscribe_task_channel(
        &self,
        task_id: &str,
        start_offset: usize,
    ) -> mpsc::UnboundedReceiver<Result<MessageStreamResponse, Status>> {
        if let Some(state) = self.task_channels.get(task_id) {
            return state.attach_subscriber(start_offset);
        }
        // See send_task_message: the read lock makes the liveness check and
        // the insert atomic against task finalization.
        let state = self.state.read().await;
        // A sender can create the entry during the lock wait, and the task
        // can then finish. Re-check so the subscriber gets the buffered
        // replay, not a bare EndOfStream.
        if let Some(chan) = self.task_channels.get(task_id) {
            return chan.attach_subscriber(start_offset);
        }
        if Self::task_can_still_send(&state, task_id) {
            return self
                .open_channel(&state, task_id)
                .attach_subscriber(start_offset);
        }
        tracing::warn!(
            "Ending stream for finished or unknown task {}, subscriber reconnected after channel GC",
            task_id
        );
        let (tx, rx) = mpsc::unbounded_channel();
        let _ = tx.send(Ok(end_of_stream_response()));
        rx
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
        // Close all task message channels.
        let task_ids: Vec<String> = self.task_channels.iter().map(|e| e.key().clone()).collect();
        for task_id in &task_ids {
            self.close_task_channel(task_id);
        }
        // Close all sub channels. A subscriber may already be gone during shutdown —
        // a closed channel is not an error here.
        let mut subs = 0;
        for entry in &self.subscribers {
            if entry
                .value()
                .tx
                .send(ServerSubMessage {
                    msg_id: "msg".create_type_id::<V7>().to_string(),
                    message: Some(server_sub_message::Message::EndOfStream(EndOfStream {})),
                })
                .is_err()
            {
                tracing::debug!("subscriber channel already closed during shutdown");
            }
            subs += 1;
        }
        tracing::info!("Closed {} subscribers", subs);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use policy::default::DefaultPolicy;

    fn coordinator() -> Coordinator<DefaultPolicy> {
        Coordinator::new()
    }

    // Channel entries are created only for live tasks. Most channel tests
    // must register a live task first.
    async fn register_running_task(c: &Coordinator<DefaultPolicy>, task_id: &str) {
        let mut state = c.state.write().await;
        insert_proof_with_running_task(&mut state, "p1", task_id, None);
    }

    // Backdates a closed channel past the 60s stale threshold. The next
    // cleanup_stale_task_channels call then removes it.
    fn backdate_channel_close(c: &Coordinator<DefaultPolicy>, task_id: &str) {
        c.task_channels
            .get(task_id)
            .unwrap()
            .inner
            .lock()
            .unwrap()
            .closed_at = Some(std::time::Instant::now() - Duration::from_secs(61));
    }

    async fn set_task_status(c: &Coordinator<DefaultPolicy>, task_id: &str, status: TaskStatus) {
        let mut state = c.state.write().await;
        state
            .proofs
            .get_mut("p1")
            .unwrap()
            .tasks
            .get_mut(task_id)
            .unwrap()
            .status = status;
    }

    fn extract_payload(msg: Result<MessageStreamResponse, Status>) -> Option<Vec<u8>> {
        match msg.ok()?.message? {
            proto::message_stream_response::Message::Payload(data) => Some(data),
            _ => None,
        }
    }

    fn is_end_of_stream(msg: &Result<MessageStreamResponse, Status>) -> bool {
        matches!(
            msg.as_ref().ok().and_then(|r| r.message.as_ref()),
            Some(proto::message_stream_response::Message::EndOfStream(_))
        )
    }

    #[tokio::test]
    async fn subscribe_then_send() {
        let c = coordinator();
        register_running_task(&c, "t1").await;
        let mut rx = c.subscribe_task_channel("t1", 0).await;

        c.send_task_message("t1", vec![1, 2, 3]).await;
        c.send_task_message("t1", vec![4, 5]).await;

        assert_eq!(extract_payload(rx.try_recv().unwrap()), Some(vec![1, 2, 3]));
        assert_eq!(extract_payload(rx.try_recv().unwrap()), Some(vec![4, 5]));
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn send_then_subscribe_replays_buffer() {
        let c = coordinator();
        register_running_task(&c, "t1").await;

        c.send_task_message("t1", vec![10]).await;
        c.send_task_message("t1", vec![20]).await;
        c.send_task_message("t1", vec![30]).await;

        let mut rx = c.subscribe_task_channel("t1", 0).await;

        assert_eq!(extract_payload(rx.try_recv().unwrap()), Some(vec![10]));
        assert_eq!(extract_payload(rx.try_recv().unwrap()), Some(vec![20]));
        assert_eq!(extract_payload(rx.try_recv().unwrap()), Some(vec![30]));
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn close_sends_eos_to_active_subscriber() {
        let c = coordinator();
        register_running_task(&c, "t1").await;
        let mut rx = c.subscribe_task_channel("t1", 0).await;

        c.send_task_message("t1", vec![1]).await;
        c.close_task_channel("t1");

        assert_eq!(extract_payload(rx.try_recv().unwrap()), Some(vec![1]));
        assert!(is_end_of_stream(&rx.try_recv().unwrap()));
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn subscribe_after_close_gets_buffer_and_eos() {
        let c = coordinator();
        register_running_task(&c, "t1").await;

        c.send_task_message("t1", vec![1]).await;
        c.send_task_message("t1", vec![2]).await;
        c.close_task_channel("t1");

        let mut rx = c.subscribe_task_channel("t1", 0).await;

        assert_eq!(extract_payload(rx.try_recv().unwrap()), Some(vec![1]));
        assert_eq!(extract_payload(rx.try_recv().unwrap()), Some(vec![2]));
        assert!(is_end_of_stream(&rx.try_recv().unwrap()));
    }

    #[tokio::test]
    async fn subscribe_before_first_send_creates_empty_channel() {
        let c = coordinator();
        register_running_task(&c, "t1").await;
        let mut rx = c.subscribe_task_channel("t1", 0).await;

        // Channel is open but empty.
        assert!(rx.try_recv().is_err());
        assert_eq!(c.task_channels.len(), 1);
    }

    #[tokio::test]
    async fn subscribe_to_unknown_task_ends_immediately() {
        let c = coordinator();
        let mut rx = c.subscribe_task_channel("nonexistent", 0).await;

        assert!(is_end_of_stream(&rx.try_recv().unwrap()));
        assert!(c.task_channels.is_empty());
    }

    // A subscriber that reconnects after the GC removed a finished task's
    // entry must get EndOfStream. A fresh open channel would never end, and
    // its reader would wait forever.
    #[tokio::test]
    async fn resubscribe_after_gc_ends_the_stream() {
        let c = coordinator();
        register_running_task(&c, "t1").await;

        // The subscriber receives the first payload. Then its stream breaks.
        let mut rx = c.subscribe_task_channel("t1", 0).await;
        c.send_task_message("t1", vec![1]).await;
        assert_eq!(extract_payload(rx.try_recv().unwrap()), Some(vec![1]));
        drop(rx);

        // The task finishes and its channel closes while the subscriber is away.
        c.send_task_message("t1", vec![2]).await;
        set_task_status(&c, "t1", TaskStatus::Succeeded).await;
        c.close_task_channel("t1");

        backdate_channel_close(&c, "t1");
        c.cleanup_stale_task_channels();
        assert!(c.task_channels.get("t1").is_none());

        // The reconnect resumes after the one payload it already got.
        // Payload 2 is gone with the removed entry. The stream must end,
        // not hang open.
        let mut rx = c.subscribe_task_channel("t1", 1).await;
        assert!(is_end_of_stream(&rx.try_recv().unwrap()));
        assert!(c.task_channels.get("t1").is_none());
    }

    // A retryable failure also closes the channel. The task's next run sends
    // on the same task id. After the GC, the entry must come back open.
    #[tokio::test]
    async fn resubscribe_after_gc_for_a_retrying_task_reopens_the_channel() {
        let c = coordinator();
        register_running_task(&c, "t1").await;

        c.send_task_message("t1", vec![1]).await;
        set_task_status(&c, "t1", TaskStatus::FailedRetryable).await;
        c.close_task_channel("t1");

        backdate_channel_close(&c, "t1");
        c.cleanup_stale_task_channels();

        let mut rx = c.subscribe_task_channel("t1", 0).await;
        assert!(rx.try_recv().is_err());

        c.send_task_message("t1", vec![2]).await;
        assert_eq!(extract_payload(rx.try_recv().unwrap()), Some(vec![2]));
    }

    #[tokio::test]
    async fn send_to_finished_task_does_not_recreate_the_channel() {
        let c = coordinator();
        register_running_task(&c, "t1").await;
        set_task_status(&c, "t1", TaskStatus::Succeeded).await;

        c.send_task_message("t1", vec![1]).await;

        assert!(c.task_channels.is_empty());
    }

    #[tokio::test]
    async fn send_after_close_is_ignored() {
        let c = coordinator();
        register_running_task(&c, "t1").await;
        let mut rx = c.subscribe_task_channel("t1", 0).await;

        c.close_task_channel("t1");
        c.send_task_message("t1", vec![99]).await;

        assert!(is_end_of_stream(&rx.try_recv().unwrap()));
        // No payload after EOS — the send was discarded.
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn double_close_is_idempotent() {
        let c = coordinator();
        register_running_task(&c, "t1").await;
        let mut rx = c.subscribe_task_channel("t1", 0).await;

        c.close_task_channel("t1");
        c.close_task_channel("t1");

        assert!(is_end_of_stream(&rx.try_recv().unwrap()));
        // Only one EOS.
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn dead_subscriber_is_pruned_on_send() {
        let c = coordinator();
        register_running_task(&c, "t1").await;
        let rx = c.subscribe_task_channel("t1", 0).await;
        drop(rx);

        // Sending should not panic; it prunes the dead subscriber.
        c.send_task_message("t1", vec![1]).await;

        let inner = c.task_channels.get("t1").unwrap();
        let inner = inner.inner.lock().unwrap();
        assert!(inner.subscribers.is_empty());
        assert_eq!(inner.buffer.len(), 1);
    }

    #[tokio::test]
    async fn multiple_subscribers_all_receive() {
        let c = coordinator();
        register_running_task(&c, "t1").await;
        let mut rx1 = c.subscribe_task_channel("t1", 0).await;
        let mut rx2 = c.subscribe_task_channel("t1", 0).await;

        c.send_task_message("t1", vec![42]).await;

        assert_eq!(extract_payload(rx1.try_recv().unwrap()), Some(vec![42]));
        assert_eq!(extract_payload(rx2.try_recv().unwrap()), Some(vec![42]));
    }

    #[tokio::test]
    async fn late_second_subscriber_gets_full_replay() {
        let c = coordinator();
        register_running_task(&c, "t1").await;
        let mut rx1 = c.subscribe_task_channel("t1", 0).await;

        c.send_task_message("t1", vec![1]).await;
        c.send_task_message("t1", vec![2]).await;

        let mut rx2 = c.subscribe_task_channel("t1", 0).await;

        // rx1 got messages live.
        assert_eq!(extract_payload(rx1.try_recv().unwrap()), Some(vec![1]));
        assert_eq!(extract_payload(rx1.try_recv().unwrap()), Some(vec![2]));

        // rx2 gets the full replay.
        assert_eq!(extract_payload(rx2.try_recv().unwrap()), Some(vec![1]));
        assert_eq!(extract_payload(rx2.try_recv().unwrap()), Some(vec![2]));
    }

    #[tokio::test]
    async fn subscribe_with_offset_skips_earlier_messages() {
        let c = coordinator();
        register_running_task(&c, "t1").await;

        c.send_task_message("t1", vec![10]).await;
        c.send_task_message("t1", vec![20]).await;
        c.send_task_message("t1", vec![30]).await;

        let mut rx = c.subscribe_task_channel("t1", 2).await;

        assert_eq!(extract_payload(rx.try_recv().unwrap()), Some(vec![30]));
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn subscribe_with_offset_beyond_buffer_gets_nothing() {
        let c = coordinator();
        register_running_task(&c, "t1").await;

        c.send_task_message("t1", vec![10]).await;
        c.send_task_message("t1", vec![20]).await;

        let mut rx = c.subscribe_task_channel("t1", 5).await;

        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn fail_proof_closes_task_channels() {
        let c = Arc::new(coordinator());
        {
            let mut state = c.state.write().await;
            let mut proof = Proof::new("p1".into(), None, ());
            proof.tasks.insert(
                "t1".into(),
                Task {
                    id: "t1".into(),
                    data: TaskData {
                        proof_id: "p1".into(),
                        ..Default::default()
                    },
                    created_at: SystemTime::now(),
                    status: TaskStatus::Running,
                    retries: 0,
                    subscribers: HashSet::new(),
                    worker: None,
                    dead_worker_requeue_count: 0,
                    extra: Default::default(),
                },
            );
            proof.active_tasks = 1;
            state.proofs.insert("p1".into(), proof);
        }

        let mut rx = c.subscribe_task_channel("t1", 0).await;
        c.send_task_message("t1", vec![1]).await;

        let _ = c.fail_proof("p1".into(), None, false, None).await;

        assert_eq!(extract_payload(rx.try_recv().unwrap()), Some(vec![1]));
        assert!(is_end_of_stream(&rx.try_recv().unwrap()));
    }

    #[tokio::test]
    async fn send_after_mutex_poison_does_not_panic() {
        let c = coordinator();
        register_running_task(&c, "t1").await;
        c.send_task_message("t1", vec![1]).await;

        let state = c.task_channels.get("t1").unwrap();
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = state.inner.lock().unwrap();
            panic!("intentional poison");
        }));
        drop(state);

        c.send_task_message("t1", vec![2]).await;
    }

    #[tokio::test]
    async fn shutdown_closes_task_channels() {
        let c = Arc::new(coordinator());
        register_running_task(&c, "t1").await;
        let mut rx = c.subscribe_task_channel("t1", 0).await;
        c.send_task_message("t1", vec![42]).await;

        c.shutdown().await;

        assert_eq!(extract_payload(rx.try_recv().unwrap()), Some(vec![42]));
        assert!(is_end_of_stream(&rx.try_recv().unwrap()));
    }

    #[tokio::test]
    async fn cleanup_removes_stale_closed_channels() {
        let c = coordinator();
        register_running_task(&c, "t1").await;
        c.send_task_message("t1", vec![1]).await;
        c.close_task_channel("t1");

        backdate_channel_close(&c, "t1");
        c.cleanup_stale_task_channels();
        assert!(c.task_channels.is_empty());
    }

    #[tokio::test]
    async fn cleanup_keeps_recently_closed_channels() {
        let c = coordinator();
        register_running_task(&c, "t1").await;
        c.send_task_message("t1", vec![1]).await;
        c.close_task_channel("t1");

        c.cleanup_stale_task_channels();
        assert_eq!(c.task_channels.len(), 1);
    }

    #[tokio::test]
    async fn cleanup_keeps_open_channels() {
        let c = coordinator();
        register_running_task(&c, "t1").await;
        c.send_task_message("t1", vec![1]).await;

        c.cleanup_stale_task_channels();
        assert_eq!(c.task_channels.len(), 1);
    }

    // ============================================================================
    // Worker lifecycle / dead-worker requeue invariants
    // ----------------------------------------------------------------------------
    // These tests codify the dead-worker / close-worker invariants:
    //   - worker disappearance is an infra/liveness event (not a logical task failure)
    //   - dead-worker requeue does NOT consume retry budget
    //   - task remains non-terminal (proof.active_tasks unchanged)
    //   - subscriber channel remains open across requeue
    // Any change to these invariants should be deliberate, not accidental.
    // ============================================================================

    fn insert_proof_with_running_task(
        state: &mut CoordinatorState<DefaultPolicy>,
        proof_id: &str,
        task_id: &str,
        worker_id: Option<&str>,
    ) {
        let mut proof = Proof::new(proof_id.into(), None, ());
        proof.active_tasks = 1;
        proof.tasks.insert(
            task_id.into(),
            Task {
                id: task_id.into(),
                data: TaskData {
                    proof_id: proof_id.into(),
                    ..Default::default()
                },
                created_at: SystemTime::now(),
                status: TaskStatus::Running,
                retries: 0,
                subscribers: HashSet::new(),
                worker: worker_id.map(String::from),
                dead_worker_requeue_count: 0,
                extra: Default::default(),
            },
        );
        state.proofs.insert(proof_id.into(), proof);
    }

    fn insert_dead_worker(
        state: &mut CoordinatorState<DefaultPolicy>,
        worker_id: &str,
        worker_type: WorkerType,
        active_tasks: &[(&str, &str)],
    ) -> mpsc::UnboundedReceiver<Result<ServerMessage, Status>> {
        let (tx, rx) = mpsc::unbounded_channel();
        let mut worker = Worker::new(
            worker_id.into(),
            worker_type,
            24,
            tx,
            WorkerIdentity::default(),
        );
        // Force the heartbeat into the distant past so cleanup_dead_workers picks it up.
        worker.last_heartbeat = 0;
        for (proof_id, task_id) in active_tasks {
            worker
                .active_tasks
                .insert(((*proof_id).into(), (*task_id).into()));
        }
        state.workers.insert(worker_id.into(), worker);
        rx
    }

    #[tokio::test]
    async fn close_worker_marks_worker_closed() {
        let c = Arc::new(coordinator());
        let (tx, _rx) = mpsc::unbounded_channel();
        {
            let mut state = c.state.write().await;
            state.workers.insert(
                "w1".into(),
                Worker::new(
                    "w1".into(),
                    WorkerType::Gpu,
                    24,
                    tx,
                    WorkerIdentity::default(),
                ),
            );
        }

        c.close_worker("w1".into()).await.unwrap();

        let state = c.state.read().await;
        assert!(
            state.workers.get("w1").unwrap().closed,
            "close_worker must set worker.closed = true"
        );
    }

    #[tokio::test]
    async fn close_worker_unknown_returns_not_found() {
        let c = Arc::new(coordinator());
        let err = c.close_worker("nonexistent".into()).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn assign_tasks_skips_a_closed_worker() {
        let c = Arc::new(coordinator());
        {
            let mut state = c.state.write().await;
            insert_proof_with_running_gpu_task(&mut state, "p1", "t1", None);
            let queued = state
                .proofs
                .get("p1")
                .unwrap()
                .tasks
                .get("t1")
                .unwrap()
                .clone();
            c.enqueue_task(&mut state, queued).await;
            insert_live_worker(&mut state, "w1");
            state.workers.get_mut("w1").unwrap().closed = true;
        }

        let state = c.state.clone().write_owned().await;
        c.assign_tasks(state).await.unwrap();

        let state = c.state.read().await;
        assert!(
            state.workers.get("w1").unwrap().active_tasks.is_empty(),
            "a closed worker must not receive assignments"
        );
    }

    #[tokio::test]
    async fn heartbeat_timeout_cannot_be_set_below_the_floor() {
        let c = Arc::new(coordinator());

        c.set_worker_heartbeat_timeout(5).await;

        assert_eq!(
            c.state.read().await.worker_heartbeat_timeout_secs,
            MIN_WORKER_HEARTBEAT_TIMEOUT,
            "a timeout no worker can meet was accepted"
        );
    }

    #[tokio::test]
    async fn heartbeat_weight_follows_assignments_not_the_worker_snapshot() {
        let c = Arc::new(coordinator());
        let _rx = {
            let mut state = c.state.write().await;
            insert_proof_with_running_task(&mut state, "p1", "t1", Some("w1"));
            state
                .proofs
                .get_mut("p1")
                .unwrap()
                .tasks
                .get_mut("t1")
                .unwrap()
                .data
                .weight = 10;
            insert_dead_worker(&mut state, "w1", WorkerType::Gpu, &[("p1", "t1")])
        };

        // A worker that has not yet taken delivery reports no tasks, no weight.
        c.handle_heartbeat("w1", &[], &[], 0).await.unwrap();

        let state = c.state.read().await;
        assert_eq!(
            state.workers["w1"].weight, 10,
            "an under-reported snapshot freed capacity that is already assigned"
        );
    }

    #[tokio::test]
    async fn cleanup_dead_workers_removes_dead_worker() {
        let c = Arc::new(coordinator());
        {
            let mut state = c.state.write().await;
            insert_dead_worker(&mut state, "w1", WorkerType::Gpu, &[]);
        }

        c.cleanup_dead_workers().await;

        let state = c.state.read().await;
        assert!(
            !state.workers.contains_key("w1"),
            "dead worker should be removed from state.workers"
        );
    }

    /// Codifies invariants:
    ///   - dead-worker requeue does NOT decrement `proof.active_tasks`
    ///   - dead-worker requeue does NOT increment `task.retries`
    ///   - dead-worker requeue DOES increment `task.dead_worker_requeue_count` (tracking only)
    ///   - task remains in proof.tasks (re-enqueued for future assignment)
    #[tokio::test]
    async fn cleanup_dead_workers_requeue_preserves_state_invariants() {
        let c = Arc::new(coordinator());
        {
            let mut state = c.state.write().await;
            insert_proof_with_running_task(&mut state, "p1", "t1", Some("w1"));
            insert_dead_worker(&mut state, "w1", WorkerType::Gpu, &[("p1", "t1")]);
        }

        c.cleanup_dead_workers().await;

        let state = c.state.read().await;
        let proof = state
            .proofs
            .get("p1")
            .expect("proof should still exist after dead-worker requeue");
        let task = proof
            .tasks
            .get("t1")
            .expect("task should still exist after dead-worker requeue");

        // Invariant: proof.active_tasks unchanged (task still non-terminal).
        assert_eq!(
            proof.active_tasks, 1,
            "dead-worker requeue must NOT decrement proof.active_tasks"
        );
        // Invariant: retries unchanged — infra failure is separate from logical retry budget.
        assert_eq!(
            task.retries, 0,
            "dead-worker requeue must NOT increment task.retries"
        );
        // Tracking-only counter — incremented by 1.
        assert_eq!(
            task.dead_worker_requeue_count, 1,
            "dead_worker_requeue_count tracking counter should increment by 1"
        );
    }

    /// Codifies invariant: dead-worker requeue does NOT close the task channel.
    /// A subscriber attached before the dead-worker event must NOT receive EndOfStream;
    /// the channel stays open for the eventual completion on a re-assigned worker.
    #[tokio::test]
    async fn cleanup_dead_workers_does_not_close_task_channel() {
        let c = Arc::new(coordinator());
        {
            let mut state = c.state.write().await;
            insert_proof_with_running_task(&mut state, "p1", "t1", Some("w1"));
            insert_dead_worker(&mut state, "w1", WorkerType::Gpu, &[("p1", "t1")]);
        }
        let mut subscriber_rx = c.subscribe_task_channel("t1", 0).await;

        c.cleanup_dead_workers().await;

        // Subscriber must not receive ANY message: no payload, no EndOfStream.
        // The task is non-terminal — it will be re-assigned and the eventual
        // completion message goes through this same channel.
        match subscriber_rx.try_recv() {
            Err(mpsc::error::TryRecvError::Empty) => {}
            Err(mpsc::error::TryRecvError::Disconnected) => panic!(
                "task channel must stay open across dead-worker requeue (channel was disconnected)"
            ),
            Ok(msg) => panic!(
                "subscriber must not receive any message during dead-worker requeue, got: {msg:?}"
            ),
        }
    }

    /// Hardening: `remove_worker_internal` must not panic when called for a worker
    /// that's no longer in state.workers.
    #[tokio::test]
    async fn remove_worker_unknown_worker_does_not_panic() {
        let c = Arc::new(coordinator());
        // This used to panic via `state.workers.remove(&worker_id).unwrap()`.
        // After hardening, this should log + emit metric + return Ok-ish.
        c.remove_worker("nonexistent".into()).await;

        let state = c.state.read().await;
        assert!(state.workers.is_empty());
    }

    /// Hardening: dead-worker cleanup must not panic when a worker has an active_task
    /// entry that references a proof that has been concurrently removed.
    #[tokio::test]
    async fn cleanup_dead_workers_with_missing_proof_does_not_panic() {
        let c = Arc::new(coordinator());
        {
            let mut state = c.state.write().await;
            // Worker references proof "ghost" that's not in state.proofs.
            insert_dead_worker(&mut state, "w1", WorkerType::Gpu, &[("ghost", "t1")]);
        }

        c.cleanup_dead_workers().await;

        let state = c.state.read().await;
        assert!(state.workers.is_empty());
    }

    /// Hardening: dead-worker cleanup must not panic when a worker has an active_task
    /// entry whose task does not exist in the (existing) proof.
    #[tokio::test]
    async fn cleanup_dead_workers_with_missing_task_does_not_panic() {
        let c = Arc::new(coordinator());
        {
            let mut state = c.state.write().await;
            // Proof exists but doesn't contain "phantom_task".
            let proof = Proof::new("p1".into(), None, ());
            state.proofs.insert("p1".into(), proof);
            insert_dead_worker(&mut state, "w1", WorkerType::Gpu, &[("p1", "phantom_task")]);
        }

        c.cleanup_dead_workers().await;

        let state = c.state.read().await;
        assert!(state.workers.is_empty());
        // Proof still present (no spurious side effects).
        assert!(state.proofs.contains_key("p1"));
    }

    /// Repeated dead-worker requeue on the same task accumulates the tracking counter.
    /// Verifies the tracking field is monotonic across multiple dead-worker events.
    #[tokio::test]
    async fn dead_worker_requeue_count_accumulates_across_events() {
        let c = Arc::new(coordinator());
        {
            let mut state = c.state.write().await;
            insert_proof_with_running_task(&mut state, "p1", "t1", Some("w1"));
            insert_dead_worker(&mut state, "w1", WorkerType::Gpu, &[("p1", "t1")]);
        }
        c.cleanup_dead_workers().await;

        // Simulate a second worker picking it up and also dying.
        {
            let mut state = c.state.write().await;
            insert_dead_worker(&mut state, "w2", WorkerType::Gpu, &[("p1", "t1")]);
        }
        c.cleanup_dead_workers().await;

        let state = c.state.read().await;
        let task = state.proofs.get("p1").unwrap().tasks.get("t1").unwrap();
        assert_eq!(
            task.dead_worker_requeue_count, 2,
            "tracking counter should accumulate across multiple dead-worker events"
        );
        // Invariants still hold.
        assert_eq!(task.retries, 0, "retries must remain 0");
        assert_eq!(
            state.proofs.get("p1").unwrap().active_tasks,
            1,
            "active_tasks must remain 1"
        );
    }

    fn gpu_task<P: AssignmentPolicy>(
        id: &str,
        proof_id: &str,
        weight: u32,
        status: TaskStatus,
        worker: Option<&str>,
    ) -> Task<P> {
        Task {
            id: id.into(),
            data: TaskData {
                proof_id: proof_id.into(),
                task_type: TaskType::ProveShard as i32,
                weight,
                ..Default::default()
            },
            created_at: SystemTime::now(),
            status,
            retries: 0,
            subscribers: HashSet::new(),
            worker: worker.map(String::from),
            dead_worker_requeue_count: 0,
            extra: Default::default(),
        }
    }

    /// GPU worker with default capacity. The rx side of its message channel is
    /// dropped, so sends to it fail silently — fine for tests that only assert
    /// on state, not sent payloads.
    fn gpu_worker<P: AssignmentPolicy>(id: &str) -> Worker<P> {
        let (tx, _rx) = mpsc::unbounded_channel();
        Worker::new(
            id.into(),
            WorkerType::Gpu,
            24,
            tx,
            WorkerIdentity::default(),
        )
    }

    fn insert_live_worker(state: &mut CoordinatorState<DefaultPolicy>, worker_id: &str) {
        state
            .workers
            .insert(worker_id.into(), gpu_worker(worker_id));
    }

    /// Like `insert_proof_with_running_task` but explicitly sets the task_type so
    /// the requeue path actually routes through `DefaultPolicy::enqueue_task` into
    /// the matching queue. `TaskType::UnspecifiedTaskType` (the default used by the
    /// simpler helper) maps to `WorkerType::None`, which silently bypasses the
    /// queue — fine for tests that only assert on state, but wrong for tests that
    /// exercise the scheduler.
    fn insert_proof_with_running_gpu_task(
        state: &mut CoordinatorState<DefaultPolicy>,
        proof_id: &str,
        task_id: &str,
        worker_id: Option<&str>,
    ) {
        let mut proof = Proof::new(proof_id.into(), None, ());
        proof.active_tasks = 1;
        proof.tasks.insert(
            task_id.into(),
            gpu_task(task_id, proof_id, 0, TaskStatus::Running, worker_id),
        );
        state.proofs.insert(proof_id.into(), proof);
    }

    /// `BalancedPolicy` is used here because `DefaultPolicy`'s weight accounting is a
    /// no-op and would hide the double release.
    fn balanced_proof_on_worker(
        state: &mut CoordinatorState<policy::balanced::BalancedPolicy>,
        owner: &str,
    ) {
        let mut proof = Proof::new("p1".into(), None, ());
        // A second task keeps the proof alive past this completion.
        proof.active_tasks = 2;
        proof.tasks.insert(
            "t1".into(),
            gpu_task("t1", "p1", 8, TaskStatus::Running, Some(owner)),
        );
        state.proofs.insert("p1".into(), proof);
        // What assigning the task to `owner` charged.
        state.policy.proof_gpu_weights.insert("p1".into(), 8);

        let mut worker = gpu_worker(owner);
        worker.active_tasks.insert(("p1".into(), "t1".into()));
        worker.weight = 8;
        state.workers.insert(owner.into(), worker);

        state
            .workers
            .insert("w_stale".into(), gpu_worker("w_stale"));
    }

    /// Regression guard: after `cleanup_dead_workers` removes a dead worker and
    /// re-enqueues its task, the next `assign_tasks` cycle must place that task on a
    /// live worker. Verifies the requeue actually reaches the policy queue and the
    /// scheduler picks it up.
    #[tokio::test]
    async fn assign_tasks_after_dead_worker_picks_up_requeued_task() {
        let c = Arc::new(coordinator());
        {
            let mut state = c.state.write().await;
            insert_proof_with_running_gpu_task(&mut state, "p1", "t1", Some("w_dead"));
            insert_dead_worker(&mut state, "w_dead", WorkerType::Gpu, &[("p1", "t1")]);
            insert_live_worker(&mut state, "w_live");
        }

        c.cleanup_dead_workers().await;

        // Drive one assignment cycle.
        let state = c.state.clone().write_owned().await;
        c.assign_tasks(state).await.unwrap();

        let state = c.state.read().await;
        assert!(
            !state.workers.contains_key("w_dead"),
            "dead worker must be gone from state.workers"
        );
        let live = state
            .workers
            .get("w_live")
            .expect("live worker should still be present");
        assert!(
            live.active_tasks.contains(&("p1".into(), "t1".into())),
            "live worker should now own the requeued task"
        );
        let task = state
            .proofs
            .get("p1")
            .unwrap()
            .tasks
            .get("t1")
            .expect("task should still exist");
        assert_eq!(task.worker.as_deref(), Some("w_live"));
        assert_eq!(task.status, TaskStatus::Running);
    }

    /// Full lifecycle: task assigned to W1, W1 dies, task reassigned to W2 via
    /// `assign_tasks`, W2 completes the task, then a ghost late `complete_task`
    /// from W1 arrives.
    ///
    /// Invariants:
    ///   - the live completion is the authoritative one (task.status == Succeeded)
    ///   - proof.active_tasks accounting reaches 0 exactly once
    ///   - subscriber sees exactly one EndOfStream (from the live completion)
    ///   - retries == 0 (infra event did not consume retry budget)
    ///   - dead_worker_requeue_count == 1
    ///   - ghost late complete returns NotFound (proof cleaned up) and does NOT
    ///     panic, double-decrement, or emit a second EndOfStream
    #[tokio::test]
    async fn dead_worker_full_lifecycle_with_late_complete_is_no_op() {
        let c = Arc::new(coordinator());
        {
            let mut state = c.state.write().await;
            insert_proof_with_running_gpu_task(&mut state, "p1", "t1", Some("w_dead"));
            insert_dead_worker(&mut state, "w_dead", WorkerType::Gpu, &[("p1", "t1")]);
            insert_live_worker(&mut state, "w_live");
        }
        let mut subscriber_rx = c.subscribe_task_channel("t1", 0).await;

        // 1. Dead-worker cleanup: w_dead removed, task re-enqueued.
        c.cleanup_dead_workers().await;

        // 2. Scheduler picks up the requeued task on w_live.
        {
            let state = c.state.clone().write_owned().await;
            c.assign_tasks(state).await.unwrap();
        }

        // Snapshot the tracking counter BEFORE the proof is cleaned up by completion.
        {
            let state = c.state.read().await;
            let task = state.proofs.get("p1").unwrap().tasks.get("t1").unwrap();
            assert_eq!(
                task.dead_worker_requeue_count, 1,
                "dead_worker_requeue_count must be 1 after a single dead-worker event"
            );
            assert_eq!(task.retries, 0, "retries must NOT have been incremented");
        }

        // 3. w_live completes the task (the authoritative completion).
        c.complete_task(
            "w_live".into(),
            "p1".into(),
            "t1".into(),
            policy::TaskMetadata::default(),
        )
        .await
        .expect("live completion should succeed");

        // After live completion: proof has 0 active tasks and is removed from state.
        {
            let state = c.state.read().await;
            assert!(
                !state.proofs.contains_key("p1"),
                "proof must be cleaned up after its only task completes"
            );
        }

        // Subscriber must have received exactly one EndOfStream from the live completion.
        assert!(
            is_end_of_stream(&subscriber_rx.try_recv().unwrap()),
            "subscriber must receive EndOfStream from the live completion"
        );

        // 4. Ghost late complete arrives from w_dead. Proof is already gone, so this
        //    must return NotFound — NOT panic, NOT double-decrement, NOT re-emit EoS.
        let ghost = c
            .complete_task(
                "w_dead".into(),
                "p1".into(),
                "t1".into(),
                policy::TaskMetadata::default(),
            )
            .await;
        assert!(
            matches!(ghost, Err(ref e) if e.code() == tonic::Code::NotFound),
            "ghost late complete after proof cleanup must return NotFound, got: {ghost:?}"
        );

        // Subscriber must not receive any additional message after the ghost call.
        match subscriber_rx.try_recv() {
            Err(mpsc::error::TryRecvError::Empty) => {}
            Err(mpsc::error::TryRecvError::Disconnected) => {
                // Channel is allowed to be disconnected after close_task_channel +
                // cleanup_stale_task_channels. The key invariant is no duplicate EoS,
                // which is satisfied by Disconnected (no payload to read).
            }
            Ok(msg) => {
                panic!("ghost complete must not deliver a second subscriber message, got: {msg:?}")
            }
        }
    }

    // --- build-identity reporting (add_worker registry + manifest) ---

    /// Reads the stored identity for a worker out of the in-memory registry.
    async fn registered_identity(
        c: &Arc<Coordinator<DefaultPolicy>>,
        worker_id: &str,
    ) -> WorkerIdentity {
        let state = c.state.read().await;
        state
            .workers
            .get(worker_id)
            .expect("worker registered")
            .identity
            .clone()
    }

    /// Worker crate version for identity fixtures. No test asserts on it.
    const VERSION: &str = "2.5.0";

    /// The version a reconnecting worker comes back on. Differs from
    /// [`VERSION`], which is what proves the identity was refreshed.
    const UPGRADED_VERSION: &str = "2.6.0";

    /// A worker identity with no reported location, which is what most of
    /// these tests care about.
    fn build(version: &str, git_sha: &str, image_tag: &str) -> WorkerIdentity {
        WorkerIdentity {
            version: version.into(),
            git_sha: git_sha.into(),
            image_tag: image_tag.into(),
            ..WorkerIdentity::default()
        }
    }

    #[tokio::test]
    async fn add_worker_stores_build_identity() {
        let c = Arc::new(coordinator());
        let (tx, _rx) = mpsc::unbounded_channel();

        let identity = build(VERSION, "abc1234", "node-gpu-abc1234");
        let existed = c
            .add_worker("w1".into(), WorkerType::Gpu, 24, tx, identity.clone())
            .await
            .unwrap();
        assert!(!existed, "first add_worker must report the worker as new");

        assert_eq!(registered_identity(&c, "w1").await, identity);
    }

    #[tokio::test]
    async fn add_worker_reconnect_refreshes_build_identity() {
        let c = Arc::new(coordinator());

        let first = WorkerIdentity {
            location: "us-east-1".into(),
            ..build(VERSION, "oldsha", "node-gpu-oldsha")
        };
        let (tx1, _rx1) = mpsc::unbounded_channel();
        c.add_worker("w1".into(), WorkerType::Gpu, 24, tx1, first.clone())
            .await
            .unwrap();

        // Assert on the fresh-insert path (`Worker::new`) before the
        // reconnect below overwrites it — otherwise a regression dropping
        // `location` from `Worker::new` would pass this whole test.
        assert_eq!(
            registered_identity(&c, "w1").await,
            first,
            "identity must be set on the fresh-insert path"
        );

        // Same worker_id reconnects after an upgrade with a new build, from a
        // different location (e.g. the worker was redeployed to a different
        // site).
        let upgraded = WorkerIdentity {
            location: "us-west-2".into(),
            ..build(UPGRADED_VERSION, "newsha", "node-gpu-newsha")
        };
        let (tx2, _rx2) = mpsc::unbounded_channel();
        let existed = c
            .add_worker("w1".into(), WorkerType::Gpu, 24, tx2, upgraded.clone())
            .await
            .unwrap();
        assert!(existed, "reconnect with same worker_id must report existed");

        assert_eq!(
            registered_identity(&c, "w1").await,
            upgraded,
            "every identity field must refresh on reconnect, not just some"
        );
    }

    #[tokio::test]
    async fn get_cluster_component_info_includes_coordinator_and_workers() {
        let c = Arc::new(coordinator());

        let (tx_gpu, _rx_gpu) = mpsc::unbounded_channel();
        c.add_worker(
            "gpu1".into(),
            WorkerType::Gpu,
            24,
            tx_gpu,
            build(VERSION, "gpusha", "node-gpu-gpusha"),
        )
        .await
        .unwrap();

        let (tx_cpu, _rx_cpu) = mpsc::unbounded_channel();
        c.add_worker(
            "cpu1".into(),
            WorkerType::Cpu,
            24,
            tx_cpu,
            build(VERSION, "cpusha", "base-cpusha"),
        )
        .await
        .unwrap();

        let components = c.get_cluster_info().await.components;

        // Exactly one coordinator entry + one per worker.
        assert_eq!(components.len(), 3, "coordinator + 2 workers");

        let coord = components
            .iter()
            .find(|ci| ci.component == "coordinator")
            .expect("coordinator entry present");
        assert_eq!(coord.version, env!("CARGO_PKG_VERSION"));

        let gpu = components
            .iter()
            .find(|ci| ci.git_sha == "gpusha")
            .expect("gpu worker entry present");
        assert_eq!(gpu.component, "gpu-node", "Gpu maps to gpu-node");
        assert_eq!(gpu.image_tag, "node-gpu-gpusha");

        let cpu = components
            .iter()
            .find(|ci| ci.git_sha == "cpusha")
            .expect("cpu worker entry present");
        assert_eq!(cpu.component, "cpu-node", "Cpu maps to cpu-node");
    }

    #[tokio::test]
    async fn get_cluster_component_info_maps_all_to_gpu_node() {
        let c = Arc::new(coordinator());

        let (tx, _rx) = mpsc::unbounded_channel();
        c.add_worker(
            "all1".into(),
            WorkerType::All,
            24,
            tx,
            build(VERSION, "allsha", "node-gpu-allsha"),
        )
        .await
        .unwrap();

        let components = c.get_cluster_info().await.components;

        let all = components
            .iter()
            .find(|ci| ci.git_sha == "allsha")
            .expect("All worker entry present");
        // Lossy compatibility mapping: All is GPU-capable -> gpu-node.
        assert_eq!(all.component, "gpu-node", "All maps to gpu-node");
    }

    #[tokio::test]
    async fn get_cluster_component_info_skips_unspecified_and_none_workers() {
        let c = Arc::new(coordinator());

        let (tx_u, _rx_u) = mpsc::unbounded_channel();
        c.add_worker(
            "u1".into(),
            WorkerType::UnspecifiedWorkerType,
            24,
            tx_u,
            build(VERSION, "usha", "base-usha"),
        )
        .await
        .unwrap();

        let (tx_n, _rx_n) = mpsc::unbounded_channel();
        c.add_worker(
            "n1".into(),
            WorkerType::None,
            24,
            tx_n,
            build(VERSION, "nsha", "base-nsha"),
        )
        .await
        .unwrap();

        let components = c.get_cluster_info().await.components;

        // Non-reportable worker_types are skipped (never reported as a false
        // cpu-node); only the coordinator's own entry remains.
        assert!(
            components.iter().all(|ci| ci.git_sha != "usha"),
            "Unspecified worker must be skipped"
        );
        assert!(
            components.iter().all(|ci| ci.git_sha != "nsha"),
            "None worker must be skipped"
        );
        assert_eq!(components.len(), 1, "only the coordinator entry remains");
    }

    /// A preempted worker's late completion must not release weight the preemption
    /// already released, or the tenant is credited twice and over-admitted.
    #[tokio::test]
    async fn completion_from_a_worker_that_lost_the_task_does_not_release_weight() {
        let c = Arc::new(Coordinator::<policy::balanced::BalancedPolicy>::new());
        {
            let mut state = c.state.write().await;
            balanced_proof_on_worker(&mut state, "w_owner");
        }

        c.complete_task(
            "w_stale".into(),
            "p1".into(),
            "t1".into(),
            policy::TaskMetadata::default(),
        )
        .await
        .unwrap();

        let state = c.state.read().await;
        assert_eq!(
            state.policy.proof_gpu_weights.get("p1").copied(),
            Some(8),
            "a completion from a worker that no longer holds the task released weight twice"
        );
    }

    #[tokio::test]
    async fn completion_from_the_owning_worker_releases_weight() {
        let c = Arc::new(Coordinator::<policy::balanced::BalancedPolicy>::new());
        {
            let mut state = c.state.write().await;
            balanced_proof_on_worker(&mut state, "w_owner");
        }

        c.complete_task(
            "w_owner".into(),
            "p1".into(),
            "t1".into(),
            policy::TaskMetadata::default(),
        )
        .await
        .unwrap();

        let state = c.state.read().await;
        assert_eq!(
            state.policy.proof_gpu_weights.get("p1").copied(),
            None,
            "the owning worker's completion must release the weight it charged"
        );
    }

    /// Counts success-hook invocations; the built-in policies implement both
    /// hooks as no-ops, so double-running them is invisible through those.
    #[derive(Clone, Default)]
    struct CountingPolicy {
        success_state_calls: u32,
        release_calls: u32,
    }

    #[async_trait::async_trait]
    impl AssignmentPolicy for CountingPolicy {
        type ProofState = u32;
        type TaskState = ();
        type WorkerState = ();
        type ProofResultMetadata = ();

        fn create_proof_state(
            _state: &CoordinatorState<Self>,
            _request: &proto::CreateProofRequest,
        ) -> u32 {
            0
        }

        fn enqueue_task(_state: &mut CoordinatorState<Self>, _task: Task<Self>) {}

        fn post_task_success_update_proof(
            proof: &mut Proof<Self>,
            _task_extra: &(),
            _metadata: policy::TaskMetadata,
        ) {
            proof.extra += 1;
        }

        fn post_task_success_update_state(
            state: &mut CoordinatorState<Self>,
            _task_type: TaskType,
        ) {
            state.policy.success_state_calls += 1;
        }

        fn post_task_update_state(
            state: &mut CoordinatorState<Self>,
            _proof_extra: u32,
            _task_id: &str,
            _task_extra: (),
            _task_weight: u32,
            _proof_id: &str,
            _task_type: TaskType,
        ) {
            state.policy.release_calls += 1;
        }

        fn debug_proof(_proof: &u32) -> &str {
            ""
        }

        fn post_worker_empty(
            _state: &mut CoordinatorState<Self>,
            worker: Worker<Self>,
        ) -> Worker<Self> {
            worker
        }

        async fn assign_tasks(
            _coord: &Arc<Coordinator<Self>>,
            state: OwnedRwLockWriteGuard<CoordinatorState<Self>>,
        ) -> Result<(), Status> {
            drop(state);
            Ok(())
        }

        fn get_proof_result_metadata(_proof: &Proof<Self>) {}

        fn cpu_queue_len(_state: &CoordinatorState<Self>) -> u32 {
            0
        }

        fn gpu_queue_len(_state: &CoordinatorState<Self>) -> u32 {
            0
        }
    }

    /// Proof "p1" under `CountingPolicy` with the given `(id, status, worker)`
    /// tasks, each held by its worker at weight 8; `active_tasks` counts the
    /// non-terminal ones.
    fn counting_proof_with_holds(
        state: &mut CoordinatorState<CountingPolicy>,
        tasks: &[(&str, TaskStatus, &str)],
    ) {
        let mut proof = Proof::new("p1".into(), None, 0);
        proof.active_tasks = tasks
            .iter()
            .filter(|(_, status, _)| {
                *status != TaskStatus::Succeeded && *status != TaskStatus::FailedFatal
            })
            .count() as u32;
        for (id, status, worker_id) in tasks {
            proof.tasks.insert(
                (*id).into(),
                gpu_task(id, "p1", 8, *status, Some(worker_id)),
            );
            let worker = state
                .workers
                .entry((*worker_id).into())
                .or_insert_with(|| gpu_worker(worker_id));
            worker.active_tasks.insert(("p1".into(), (*id).into()));
            worker.weight += 8;
        }
        state.proofs.insert("p1".into(), proof);
    }

    /// A stale completion that finishes the proof leaves the redelivered copy's
    /// worker holding a slot and policy weight nothing else releases: with the
    /// proof gone, that worker's own report dies on NotFound before any cleanup.
    #[tokio::test]
    async fn stale_completion_finishing_a_proof_releases_the_redelivered_assignment() {
        let c = Arc::new(Coordinator::<CountingPolicy>::new());
        {
            let mut state = c.state.write().await;
            counting_proof_with_holds(&mut state, &[("t1", TaskStatus::Running, "w_owner")]);
        }

        c.complete_task(
            "w_stale".into(),
            "p1".into(),
            "t1".into(),
            policy::TaskMetadata::default(),
        )
        .await
        .unwrap();

        let state = c.state.read().await;
        assert!(
            !state.proofs.contains_key("p1"),
            "the proof must be removed"
        );
        let owner = state.workers.get("w_owner").unwrap();
        assert!(
            owner.active_tasks.is_empty(),
            "the redelivered assignment must be released with the proof"
        );
        assert_eq!(owner.weight, 0, "the worker's slot weight must be freed");
        assert_eq!(
            state.policy.release_calls, 1,
            "the policy weight the redelivery charged must be released exactly once"
        );
    }

    /// The completing worker can itself hold another already-terminal task of the
    /// proof — a redelivery whose stale twin reported first. Finishing the proof
    /// must release that hold too, not just other workers'.
    #[tokio::test]
    async fn finishing_a_proof_releases_the_completers_other_orphans() {
        let c = Arc::new(Coordinator::<CountingPolicy>::new());
        {
            let mut state = c.state.write().await;
            counting_proof_with_holds(
                &mut state,
                &[
                    ("t1", TaskStatus::Running, "w1"),
                    ("t2", TaskStatus::Succeeded, "w1"),
                ],
            );
        }

        c.complete_task(
            "w1".into(),
            "p1".into(),
            "t1".into(),
            policy::TaskMetadata::default(),
        )
        .await
        .unwrap();

        let state = c.state.read().await;
        let worker = state.workers.get("w1").unwrap();
        assert!(
            worker.active_tasks.is_empty(),
            "the orphaned hold on t2 must be released with the proof"
        );
        assert_eq!(worker.weight, 0, "both holds' slot weight must be freed");
        assert_eq!(
            state.policy.release_calls, 2,
            "one policy release per assignment: t2's orphan and t1's own"
        );
    }

    /// Success hooks record billing and scheduling history, charged once per
    /// task. A preempted worker's reporter landing next to its redelivery's
    /// report must not run them twice.
    #[tokio::test]
    async fn duplicate_completion_runs_the_success_hooks_once() {
        let c = Arc::new(Coordinator::<CountingPolicy>::new());
        {
            let mut state = c.state.write().await;
            let mut proof = Proof::new("p1".into(), None, 0);
            // A second live task keeps the proof alive across both completions.
            proof.active_tasks = 2;
            proof.tasks.insert(
                "t1".into(),
                gpu_task("t1", "p1", 0, TaskStatus::Running, Some("w_owner")),
            );
            state.proofs.insert("p1".into(), proof);
        }

        for worker in ["w_owner", "w_stale"] {
            c.complete_task(
                worker.into(),
                "p1".into(),
                "t1".into(),
                policy::TaskMetadata::default(),
            )
            .await
            .unwrap();
        }

        let state = c.state.read().await;
        assert_eq!(
            state.proofs.get("p1").unwrap().extra,
            1,
            "proof billing metadata was recorded per report, not per task"
        );
        assert_eq!(
            state.policy.success_state_calls, 1,
            "scheduling history was recorded per report, not per task"
        );
    }

    fn mark_task_succeeded(
        state: &mut CoordinatorState<DefaultPolicy>,
        proof_id: &str,
        task_id: &str,
    ) {
        state
            .proofs
            .get_mut(proof_id)
            .unwrap()
            .tasks
            .get_mut(task_id)
            .unwrap()
            .status = TaskStatus::Succeeded;
    }

    fn insert_worker_holding(
        state: &mut CoordinatorState<DefaultPolicy>,
        worker_id: &str,
        proof_id: &str,
        task_id: &str,
    ) {
        insert_live_worker(state, worker_id);
        let task_weight = state.proofs[proof_id].tasks[task_id].data.weight;
        let worker = state.workers.get_mut(worker_id).unwrap();
        worker
            .active_tasks
            .insert((proof_id.into(), task_id.into()));
        worker.weight = task_weight;
    }

    /// The path a fatal worker error actually takes: `try_unclaim_proof` sends
    /// `FailProofRequest` naming the task. A duplicate execution of a finished task must
    /// not take the proof down with it.
    #[tokio::test]
    async fn fail_proof_blamed_on_a_succeeded_task_is_ignored() {
        let c = Arc::new(coordinator());
        {
            let mut state = c.state.write().await;
            insert_proof_with_running_gpu_task(&mut state, "p1", "t1", Some("w1"));
            mark_task_succeeded(&mut state, "p1", "t1");
        }

        c.fail_proof("p1".into(), Some("t1".into()), true, None)
            .await
            .expect("a failure blamed on finished work must be ignored, not error");

        let state = c.state.read().await;
        let proof = state
            .proofs
            .get("p1")
            .expect("proof must survive a duplicate execution's failure");
        assert_eq!(
            proof.tasks.get("t1").unwrap().status,
            TaskStatus::Succeeded,
            "the recorded success must be untouched"
        );
    }

    #[tokio::test]
    async fn fail_proof_blamed_on_an_unknown_task_is_rejected() {
        let c = Arc::new(coordinator());
        {
            let mut state = c.state.write().await;
            insert_proof_with_running_gpu_task(&mut state, "p1", "t1", Some("w1"));
        }

        let result = c
            .fail_proof("p1".into(), Some("nope".into()), false, None)
            .await;

        assert!(
            matches!(result, Err(ref e) if e.code() == tonic::Code::NotFound),
            "unknown task must be rejected, got: {result:?}"
        );
        let state = c.state.read().await;
        assert!(
            state.proofs.contains_key("p1"),
            "proof must survive a failure naming a task it does not own"
        );
    }

    #[tokio::test]
    async fn fail_proof_blamed_on_a_running_task_still_fails_the_proof() {
        let c = Arc::new(coordinator());
        {
            let mut state = c.state.write().await;
            insert_proof_with_running_gpu_task(&mut state, "p1", "t1", Some("w1"));
        }

        c.fail_proof("p1".into(), Some("t1".into()), false, None)
            .await
            .unwrap();

        let state = c.state.read().await;
        assert!(
            !state.proofs.contains_key("p1"),
            "a genuine failure must still fail the proof"
        );
    }

    /// An execution error unclaims the proof, then the reporter's `fail_task` lands on
    /// a proof that is already gone.
    #[tokio::test]
    async fn fail_task_after_the_proof_was_unclaimed_is_rejected() {
        let c = Arc::new(coordinator());
        {
            let mut state = c.state.write().await;
            insert_live_worker(&mut state, "w1");
            state
                .workers
                .get_mut("w1")
                .unwrap()
                .active_tasks
                .insert(("p1".into(), "t1".into()));
        }

        let result = c
            .fail_task("w1".into(), "p1".into(), "t1".into(), false)
            .await;

        assert!(
            matches!(result, Err(ref e) if e.code() == tonic::Code::NotFound),
            "expected NotFound for a proof already unclaimed, got: {result:?}"
        );
    }

    /// Backstop when the unclaim RPC never landed: a fatal `fail_task` on a CoreExecute
    /// task must fail the proof itself.
    #[tokio::test]
    async fn fatal_failure_of_a_core_execute_task_fails_the_proof() {
        let c = Arc::new(coordinator());
        {
            let mut state = c.state.write().await;
            insert_proof_with_running_gpu_task(&mut state, "p1", "t1", Some("w1"));
            state
                .proofs
                .get_mut("p1")
                .unwrap()
                .tasks
                .get_mut("t1")
                .unwrap()
                .data
                .task_type = TaskType::CoreExecute as i32;
            insert_worker_holding(&mut state, "w1", "p1", "t1");
        }

        c.fail_task("w1".into(), "p1".into(), "t1".into(), false)
            .await
            .unwrap();

        let state = c.state.read().await;
        assert!(
            !state.proofs.contains_key("p1"),
            "a fatal CoreExecute failure must fail the proof when nothing else has"
        );
    }

    #[tokio::test]
    async fn fail_task_does_not_downgrade_a_succeeded_task() {
        let c = Arc::new(coordinator());
        {
            let mut state = c.state.write().await;
            insert_proof_with_running_gpu_task(&mut state, "p1", "t1", Some("w1"));
            mark_task_succeeded(&mut state, "p1", "t1");
            insert_worker_holding(&mut state, "w1", "p1", "t1");
        }

        c.fail_task("w1".into(), "p1".into(), "t1".into(), false)
            .await
            .expect("a late failure must be ignored, not surfaced as an error");

        let state = c.state.read().await;
        let proof = state
            .proofs
            .get("p1")
            .expect("proof must survive a late failure on a finished task");
        assert_eq!(
            proof.tasks.get("t1").unwrap().status,
            TaskStatus::Succeeded,
            "recorded success was downgraded by a late failure"
        );
        assert_eq!(
            proof.active_tasks, 1,
            "ignoring the failure must not touch the proof's task accounting"
        );
        assert!(
            !state
                .workers
                .get("w1")
                .unwrap()
                .active_tasks
                .contains(&("p1".into(), "t1".into())),
            "the finished task must still release its worker slot"
        );
    }

    /// A fatal failure can drain the proof the same way a completion can; the
    /// redelivered copy another worker still holds must be released here too.
    #[tokio::test]
    async fn failure_finishing_a_proof_releases_other_workers_holds() {
        let c = Arc::new(Coordinator::<CountingPolicy>::new());
        {
            let mut state = c.state.write().await;
            counting_proof_with_holds(
                &mut state,
                &[
                    ("t1", TaskStatus::Running, "w2"),
                    ("t2", TaskStatus::Succeeded, "w1"),
                ],
            );
        }

        c.fail_task("w2".into(), "p1".into(), "t1".into(), false)
            .await
            .unwrap();

        let state = c.state.read().await;
        assert!(
            !state.proofs.contains_key("p1"),
            "the proof must be removed"
        );
        let holder = state.workers.get("w1").unwrap();
        assert!(
            holder.active_tasks.is_empty(),
            "the redelivered hold must be released with the proof"
        );
        assert_eq!(holder.weight, 0);
        assert_eq!(
            state.policy.release_calls, 2,
            "one release per assignment: t1's own and t2's orphan"
        );
    }

    /// Proof teardown releases whoever actually holds a task, whatever its
    /// status: a Succeeded task can still be held as a redelivered copy.
    #[tokio::test]
    async fn failing_a_proof_releases_a_succeeded_but_held_redelivery() {
        let c = Arc::new(Coordinator::<CountingPolicy>::new());
        {
            let mut state = c.state.write().await;
            counting_proof_with_holds(
                &mut state,
                &[
                    ("t1", TaskStatus::Succeeded, "w1"),
                    ("t2", TaskStatus::Running, "w2"),
                ],
            );
        }

        c.fail_proof("p1".into(), None, false, None).await.unwrap();

        let state = c.state.read().await;
        for w in ["w1", "w2"] {
            let worker = state.workers.get(w).unwrap();
            assert!(worker.active_tasks.is_empty(), "{w} must be released");
            assert_eq!(worker.weight, 0, "{w} slot weight must be freed");
        }
        assert_eq!(state.policy.release_calls, 2);
    }

    /// A requeued task keeps a stale `task.worker` until reassignment; teardown
    /// must not release a hold that preemption already released.
    #[tokio::test]
    async fn failing_a_proof_ignores_a_stale_worker_reference() {
        let c = Arc::new(Coordinator::<CountingPolicy>::new());
        {
            let mut state = c.state.write().await;
            counting_proof_with_holds(&mut state, &[("t1", TaskStatus::Running, "w1")]);
            // Preemption released the hold; the task still names w1.
            let worker = state.workers.get_mut("w1").unwrap();
            worker.active_tasks.clear();
            worker.weight = 0;
        }

        c.fail_proof("p1".into(), None, false, None).await.unwrap();

        let state = c.state.read().await;
        assert_eq!(
            state.policy.release_calls, 0,
            "a hold released at preemption must not be released again"
        );
    }

    /// A fatal controller failure tears the proof down mid-`fail_task`; the
    /// reporting pair's release belongs to the tail, not the teardown sweep.
    #[tokio::test]
    async fn manual_proof_fail_releases_the_reporting_task_once() {
        let c = Arc::new(Coordinator::<CountingPolicy>::new());
        {
            let mut state = c.state.write().await;
            counting_proof_with_holds(&mut state, &[("t1", TaskStatus::Running, "w1")]);
            let proof = state.proofs.get_mut("p1").unwrap();
            proof.tasks.get_mut("t1").unwrap().data.task_type = TaskType::Controller as i32;
        }

        c.fail_task("w1".into(), "p1".into(), "t1".into(), false)
            .await
            .unwrap();

        let state = c.state.read().await;
        assert!(!state.proofs.contains_key("p1"), "the proof must be failed");
        assert_eq!(
            state.policy.release_calls, 1,
            "teardown must skip the reporting pair; fail_task's tail releases it"
        );
    }

    /// The duplicate's own assignment charged weight that nothing else releases, since
    /// the completion came from a worker that no longer held the task.
    #[tokio::test]
    async fn ignoring_a_duplicates_failure_releases_the_weight_it_charged() {
        let c = Arc::new(Coordinator::<policy::balanced::BalancedPolicy>::new());
        {
            let mut state = c.state.write().await;
            balanced_proof_on_worker(&mut state, "w_owner");
            state
                .proofs
                .get_mut("p1")
                .unwrap()
                .tasks
                .get_mut("t1")
                .unwrap()
                .status = TaskStatus::Succeeded;
        }

        c.fail_task("w_owner".into(), "p1".into(), "t1".into(), false)
            .await
            .unwrap();

        let state = c.state.read().await;
        assert_eq!(
            state.policy.proof_gpu_weights.get("p1").copied(),
            None,
            "the duplicate's assignment left weight charged to the proof"
        );
    }

    #[tokio::test]
    async fn ignored_failure_frees_the_worker_for_queued_work() {
        let c = Arc::new(coordinator());
        {
            let mut state = c.state.write().await;
            insert_proof_with_running_gpu_task(&mut state, "p1", "t1", Some("w1"));
            mark_task_succeeded(&mut state, "p1", "t1");
            insert_worker_holding(&mut state, "w1", "p1", "t1");

            // A second proof waiting on the only GPU worker.
            insert_proof_with_running_gpu_task(&mut state, "p2", "t2", None);
            let queued = state
                .proofs
                .get("p2")
                .unwrap()
                .tasks
                .get("t2")
                .unwrap()
                .clone();
            c.enqueue_task(&mut state, queued).await;
        }

        c.fail_task("w1".into(), "p1".into(), "t1".into(), false)
            .await
            .unwrap();

        let state = c.state.read().await;
        assert!(
            state
                .workers
                .get("w1")
                .unwrap()
                .active_tasks
                .contains(&("p2".into(), "t2".into())),
            "freeing the slot must schedule queued work, not wait for the next event"
        );
    }

    #[tokio::test]
    async fn retryable_failure_does_not_requeue_a_succeeded_task() {
        let c = Arc::new(coordinator());
        {
            let mut state = c.state.write().await;
            insert_proof_with_running_gpu_task(&mut state, "p1", "t1", Some("w1"));
            mark_task_succeeded(&mut state, "p1", "t1");
            insert_worker_holding(&mut state, "w1", "p1", "t1");
        }

        c.fail_task("w1".into(), "p1".into(), "t1".into(), true)
            .await
            .expect("a late retryable failure must be ignored");

        let state = c.state.read().await;
        let task = state.proofs.get("p1").unwrap().tasks.get("t1").unwrap();
        assert_eq!(task.status, TaskStatus::Succeeded);
        assert_eq!(task.retries, 0, "a succeeded task must not be retried");
        assert!(
            !state
                .workers
                .get("w1")
                .unwrap()
                .active_tasks
                .contains(&("p1".into(), "t1".into())),
            "a succeeded task must not be re-assigned"
        );
    }

    // --- GPU capacity reporting (node filter, counters, snapshot) ---

    const GIB: u64 = 1024 * 1024 * 1024;
    const TIMEOUT: u64 = DEFAULT_WORKER_HEARTBEAT_TIMEOUT;
    const NOW: u64 = 1_700_000_000;

    /// Insert a worker with the given type, closed flag, and heartbeat, without the
    /// registration RPC. For filter tests that need a draining or silent worker.
    fn add_worker_to_state(
        state: &mut CoordinatorState<DefaultPolicy>,
        worker_id: &str,
        worker_type: WorkerType,
        closed: bool,
        last_heartbeat: u64,
    ) {
        let (tx, _rx) = mpsc::unbounded_channel();
        let mut worker = Worker::new(
            worker_id.into(),
            worker_type,
            24,
            tx,
            WorkerIdentity::default(),
        );
        worker.closed = closed;
        worker.last_heartbeat = last_heartbeat;
        state.workers.insert(worker_id.into(), worker);
    }

    /// Insert a live worker of the given type. `insert_live_worker` always inserts a
    /// GPU worker; the busy-crediting tests need CPU and All workers too.
    fn insert_live_worker_of(
        state: &mut CoordinatorState<DefaultPolicy>,
        worker_id: &str,
        worker_type: WorkerType,
    ) {
        let (tx, _rx) = mpsc::unbounded_channel();
        state.workers.insert(
            worker_id.into(),
            Worker::new(
                worker_id.into(),
                worker_type,
                24,
                tx,
                WorkerIdentity::default(),
            ),
        );
    }

    /// Register a worker bound to the given GPU. `add_worker` sets its heartbeat to now.
    async fn add_gpu_worker(
        c: &Arc<Coordinator<DefaultPolicy>>,
        worker_id: &str,
        worker_type: WorkerType,
        gpu_name: &str,
        gpu_memory_total_bytes: u64,
    ) {
        // The receiver is dropped: these tests assert on registry state, not on messages.
        let (tx, _rx) = mpsc::unbounded_channel();
        c.add_worker(
            worker_id.into(),
            worker_type,
            24,
            tx,
            WorkerIdentity {
                gpu_name: gpu_name.into(),
                gpu_memory_total_bytes,
                ..build("2.6.0", "sha", "tag")
            },
        )
        .await
        .unwrap();
    }

    #[test]
    fn gpu_node_filter_counts_both_gpu_and_all_worker_types() {
        // `All` is NodeConfig::default() and receives GPU tasks, so it must count.
        assert!(is_connected_gpu_node(WorkerType::Gpu, NOW, NOW, TIMEOUT));
        assert!(is_connected_gpu_node(WorkerType::All, NOW, NOW, TIMEOUT));
    }

    #[test]
    fn gpu_node_filter_excludes_non_gpu_worker_types() {
        for worker_type in [
            WorkerType::Cpu,
            WorkerType::None,
            WorkerType::UnspecifiedWorkerType,
        ] {
            assert!(
                !is_connected_gpu_node(worker_type, NOW, NOW, TIMEOUT),
                "{worker_type:?} drives no GPU"
            );
        }
    }

    #[tokio::test]
    async fn gpu_node_filter_counts_a_draining_worker() {
        // A draining (closed) worker continues its in-flight tasks, and those completions add
        // to gpu_busy_ms_total. It must count as available too; if not, busy time can become
        // larger than available time and the published utilization is silently wrong. For
        // this reason, `is_connected_gpu_node` has no `closed` parameter.
        let c = Arc::new(coordinator());
        {
            let mut state = c.state.write().await;
            add_worker_to_state(&mut state, "draining", WorkerType::Gpu, true, unix_now());
            add_worker_to_state(
                &mut state,
                "draining_all",
                WorkerType::All,
                true,
                unix_now(),
            );
        }

        let state = c.state.read().await;
        assert_eq!(
            state.connected_gpu_nodes(unix_now()).count(),
            2,
            "a draining GPU node still occupies its GPU, so it still counts"
        );
    }

    #[test]
    fn gpu_node_filter_excludes_an_expired_heartbeat() {
        // At the timeout the worker is alive; one second later it is not: the exact
        // complement of the `cleanup_dead_workers` condition.
        assert!(is_connected_gpu_node(
            WorkerType::Gpu,
            NOW - TIMEOUT,
            NOW,
            TIMEOUT
        ));
        assert!(!is_connected_gpu_node(
            WorkerType::Gpu,
            NOW - TIMEOUT - 1,
            NOW,
            TIMEOUT
        ));
    }

    #[test]
    fn gpu_node_filter_agrees_with_the_dead_worker_reaper() {
        // The reaper removes a worker if `last_heartbeat + timeout < now`. The filter must
        // exclude exactly those workers.
        for age in 0..(TIMEOUT + 5) {
            let last_heartbeat = NOW - age;
            let reaper_considers_dead = last_heartbeat + TIMEOUT < NOW;
            let counted = is_connected_gpu_node(WorkerType::Gpu, last_heartbeat, NOW, TIMEOUT);
            assert_eq!(
                counted, !reaper_considers_dead,
                "disagreement at heartbeat age {age}s"
            );
        }
    }

    #[test]
    fn availability_integral_accumulates_nodes_times_elapsed() {
        // Two nodes for ten seconds is twenty GPU-seconds.
        assert_eq!(
            advance_gpu_available_ms(0, 2, Duration::from_secs(10)),
            20_000
        );
        // Monotonic: each tick adds to the running total.
        assert_eq!(
            advance_gpu_available_ms(20_000, 3, Duration::from_secs(10)),
            50_000
        );
        // An empty cluster adds nothing, for any tick length.
        assert_eq!(
            advance_gpu_available_ms(50_000, 0, Duration::from_secs(600)),
            50_000
        );
        // A zero-length tick also adds nothing.
        assert_eq!(advance_gpu_available_ms(50_000, 8, Duration::ZERO), 50_000);
    }

    #[test]
    fn availability_integral_saturates_instead_of_wrapping() {
        // Saturation keeps the counter monotonic for all inputs.
        assert_eq!(
            advance_gpu_available_ms(u64::MAX - 1, 8, Duration::from_secs(10)),
            u64::MAX
        );
    }

    #[tokio::test]
    async fn empty_cluster_reports_zero_gpu_nodes_and_no_classes() {
        let c = Arc::new(coordinator());
        let capacity = c.get_cluster_info().await.capacity;
        assert_eq!(capacity.gpu_nodes, 0);
        assert!(capacity.gpus.is_empty());
    }

    #[tokio::test]
    async fn add_worker_stores_the_bound_gpu() {
        let c = Arc::new(coordinator());
        add_gpu_worker(&c, "gpu1", WorkerType::Gpu, "NVIDIA L4", 24 * GIB).await;

        let state = c.state.read().await;
        let worker = state.workers.get("gpu1").expect("worker registered");
        assert_eq!(worker.identity.gpu_name, "NVIDIA L4");
        assert_eq!(worker.identity.gpu_memory_total_bytes, 24 * GIB);
    }

    #[tokio::test]
    async fn add_worker_reconnect_refreshes_the_bound_gpu() {
        let c = Arc::new(coordinator());
        add_gpu_worker(&c, "gpu1", WorkerType::Gpu, "NVIDIA L4", 24 * GIB).await;
        // The same worker id reconnects from different hardware.
        add_gpu_worker(
            &c,
            "gpu1",
            WorkerType::Gpu,
            "NVIDIA H100 80GB HBM3",
            80 * GIB,
        )
        .await;

        let state = c.state.read().await;
        let worker = state.workers.get("gpu1").expect("worker registered");
        assert_eq!(
            worker.identity.gpu_name, "NVIDIA H100 80GB HBM3",
            "the GPU must refresh on reconnect, not stay stale"
        );
        assert_eq!(worker.identity.gpu_memory_total_bytes, 80 * GIB);
    }

    #[tokio::test]
    async fn completing_a_task_accumulates_its_gpu_time() {
        let c = Arc::new(coordinator());
        {
            let mut state = c.state.write().await;
            insert_proof_with_running_task(&mut state, "p1", "t1", Some("w1"));
            insert_live_worker_of(&mut state, "w1", WorkerType::Gpu);
            state
                .workers
                .get_mut("w1")
                .unwrap()
                .active_tasks
                .insert(("p1".into(), "t1".into()));
        }

        c.complete_task(
            "w1".into(),
            "p1".into(),
            "t1".into(),
            policy::TaskMetadata { gpu_ms: 4_200 },
        )
        .await
        .unwrap();

        assert_eq!(c.state.read().await.gpu_busy_ms_total, 4_200);
    }

    #[tokio::test]
    async fn repeat_completion_does_not_double_count_gpu_time() {
        // A racing retry reports the same task twice, but the device time was spent once.
        let c = Arc::new(coordinator());
        {
            let mut state = c.state.write().await;
            insert_proof_with_running_task(&mut state, "p1", "t1", Some("w1"));
            // A second task keeps the proof alive, so the repeat completion finds it.
            let proof = state.proofs.get_mut("p1").unwrap();
            proof.active_tasks = 2;
            insert_live_worker_of(&mut state, "w1", WorkerType::Gpu);
            state
                .workers
                .get_mut("w1")
                .unwrap()
                .active_tasks
                .insert(("p1".into(), "t1".into()));
        }

        for _ in 0..2 {
            c.complete_task(
                "w1".into(),
                "p1".into(),
                "t1".into(),
                policy::TaskMetadata { gpu_ms: 4_200 },
            )
            .await
            .unwrap();
        }

        assert_eq!(
            c.state.read().await.gpu_busy_ms_total,
            4_200,
            "only the completion that moved the task to Succeeded may count"
        );
    }

    #[tokio::test]
    async fn completing_a_task_on_a_cpu_worker_accumulates_no_gpu_time() {
        // Under SP1_CLUSTER_CPU_ONLY, CPU workers run GPU task types and the cluster has zero
        // GPU nodes. If their completions added busy time, snapshots would show busy > 0
        // against available == 0, stored by the SPN as a silently wrong utilization.
        let c = Arc::new(coordinator());
        {
            let mut state = c.state.write().await;
            insert_proof_with_running_task(&mut state, "p1", "t1", Some("w1"));
            insert_live_worker_of(&mut state, "w1", WorkerType::Cpu);
            state
                .workers
                .get_mut("w1")
                .unwrap()
                .active_tasks
                .insert(("p1".into(), "t1".into()));
        }

        c.complete_task(
            "w1".into(),
            "p1".into(),
            "t1".into(),
            policy::TaskMetadata { gpu_ms: 4_200 },
        )
        .await
        .unwrap();

        assert_eq!(
            c.state.read().await.gpu_busy_ms_total,
            0,
            "a CPU worker is not in the population gpu_available_ms_total integrates over"
        );
    }

    #[tokio::test]
    async fn completing_a_task_on_an_all_worker_accumulates_gpu_time() {
        // `All` counts as a GPU node for availability, so its completions must add busy time.
        let c = Arc::new(coordinator());
        {
            let mut state = c.state.write().await;
            insert_proof_with_running_task(&mut state, "p1", "t1", Some("w1"));
            insert_live_worker_of(&mut state, "w1", WorkerType::All);
            state
                .workers
                .get_mut("w1")
                .unwrap()
                .active_tasks
                .insert(("p1".into(), "t1".into()));
        }

        c.complete_task(
            "w1".into(),
            "p1".into(),
            "t1".into(),
            policy::TaskMetadata { gpu_ms: 4_200 },
        )
        .await
        .unwrap();

        assert_eq!(c.state.read().await.gpu_busy_ms_total, 4_200);
    }

    #[tokio::test]
    async fn completing_a_task_on_a_draining_worker_still_accumulates_gpu_time() {
        // The mirror of `gpu_node_filter_counts_a_draining_worker`: a draining node still adds
        // availability, so it must still add busy time. Both counters must describe the same
        // set of workers.
        let c = Arc::new(coordinator());
        {
            let mut state = c.state.write().await;
            insert_proof_with_running_task(&mut state, "p1", "t1", Some("w1"));
            insert_live_worker_of(&mut state, "w1", WorkerType::Gpu);
            let worker = state.workers.get_mut("w1").unwrap();
            worker.closed = true;
            worker.active_tasks.insert(("p1".into(), "t1".into()));
        }

        c.complete_task(
            "w1".into(),
            "p1".into(),
            "t1".into(),
            policy::TaskMetadata { gpu_ms: 4_200 },
        )
        .await
        .unwrap();

        assert_eq!(c.state.read().await.gpu_busy_ms_total, 4_200);
    }

    #[tokio::test]
    async fn completing_a_task_from_an_unknown_worker_accumulates_no_gpu_time() {
        // The worker was reaped before completion: no worker_type to check, and no
        // availability accrues for it.
        let c = Arc::new(coordinator());
        {
            let mut state = c.state.write().await;
            insert_proof_with_running_task(&mut state, "p1", "t1", Some("w1"));
        }

        c.complete_task(
            "w1".into(),
            "p1".into(),
            "t1".into(),
            policy::TaskMetadata { gpu_ms: 4_200 },
        )
        .await
        .unwrap();

        assert_eq!(c.state.read().await.gpu_busy_ms_total, 0);
    }

    #[tokio::test]
    async fn snapshot_advances_the_availability_integral_itself() {
        // `get_cluster_info` must advance the integral itself; no periodic tick runs in this
        // test.
        let c = Arc::new(coordinator());
        add_gpu_worker(&c, "gpu1", WorkerType::Gpu, "NVIDIA L4", 24 * GIB).await;
        {
            let mut state = c.state.write().await;
            assert_eq!(
                state.gpu_available_ms_total, 0,
                "no tick has run, so nothing has accrued yet"
            );
            // Rewind the tick marker so a known interval is outstanding.
            state.gpu_available_last_tick =
                std::time::Instant::now() - std::time::Duration::from_secs(10);
        }

        let capacity = c.get_cluster_info().await.capacity;

        assert!(
            (9_000..=11_000).contains(&capacity.gpu_available_ms_total),
            "expected ~1 node * 10s = 10000 GPU-ms in the snapshot, got {}",
            capacity.gpu_available_ms_total
        );
        assert_eq!(
            c.state.read().await.gpu_available_ms_total,
            capacity.gpu_available_ms_total,
            "the snapshot publishes the advanced integral, it does not fork its own copy"
        );
    }
}
