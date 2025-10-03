#![allow(clippy::new_without_default)]
#![allow(clippy::option_map_unit_fn)]
#![allow(dropping_copy_types)]

use crate::error::TaskError;
use crate::metrics::WorkerMetrics;
use lazy_static::lazy_static;
use lru::LruCache;
use opentelemetry::Context;
use opentelemetry::{global, trace::TraceContextExt};
use sp1_cluster_artifact::ArtifactClient;
use sp1_cluster_common::proto::{ProofRequestStatus, TaskStatus, TaskType, WorkerTask, WorkerType};
use sp1_prover::{CoreSC, DeviceProvingKey, InnerSC, SP1Prover};
use sp1_recursion_circuit::machine::SP1CompressWithVkeyShape;
use sp1_sdk::network::proto::types::ProofMode;
use sp1_stark::{MachineProver, SP1ProverOpts, StarkVerifyingKey};
use std::sync::RwLock;
use std::time::Instant;
use std::{
    collections::{BTreeMap, HashMap},
    env,
    sync::Arc,
};
use tasks::TaskMetadata;
use tokio::sync::{Mutex, Semaphore};
use tracing::info_span;
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use utils::DeferGuard;

#[cfg(feature = "gpu")]
mod gpu {
    pub use moongate_prover::components::GpuProverComponents;
}
#[cfg(feature = "gpu")]
use gpu::*;

pub mod client;
pub mod error;
pub mod limiter;
pub mod metrics;
pub mod tasks;
pub mod utils;

pub use client::WorkerService;
pub use tasks::ShardEventData;

lazy_static! {
    /// Polling interval used for task popping/waiting and various polling loops.
    pub static ref POLLING_INTERVAL_MS: u64 = env::var("WORKER_POLLING_INTERVAL_MS")
        .unwrap_or("100".to_string())
        .parse()
        .unwrap();

    /// The layer that folding will happen instead of more tree joining.
    /// The total number of shards per fold is 2^FOLD_LAYER.
    pub static ref FOLD_LAYER: usize = std::env::var("WORKER_FOLD_LAYER")
        .map(|s| s.parse().unwrap())
        .unwrap_or(20);

    /// Max weight override.
    pub static ref MAX_WEIGHT_OVERRIDE: Option<usize> = std::env::var("WORKER_MAX_WEIGHT_OVERRIDE")
        .map(|s| s.parse().unwrap())
        .ok();
}

#[cfg(feature = "gpu")]
pub type ClusterProverComponents = GpuProverComponents;
#[cfg(not(feature = "gpu"))]
pub type ClusterProverComponents = sp1_prover::components::CpuProverComponents;

pub const VERGEN_GIT_SHA: &str = env!("VERGEN_GIT_SHA");
pub const VERGEN_BUILD_TIMESTAMP: &str = env!("VERGEN_BUILD_TIMESTAMP");

pub type CachedKeys = Arc<(
    DeviceProvingKey<ClusterProverComponents>,
    StarkVerifyingKey<InnerSC>,
)>;

pub struct SP1Worker<W: WorkerService, A: ArtifactClient> {
    pub prover: Arc<SP1Prover<ClusterProverComponents>>,
    pub prover_opts: SP1ProverOpts,
    pub gpu_semaphore: Arc<Semaphore>,
    pub compress_keys: RwLock<BTreeMap<SP1CompressWithVkeyShape, CachedKeys>>,
    pub metrics: Option<Arc<WorkerMetrics>>,
    pub worker_client: W,
    pub artifact_client: A,
    pub vkey_cache: Arc<Mutex<LruCache<String, StarkVerifyingKey<CoreSC>>>>,
}

/// The result of a task which is passed back to the main thread.
pub struct TaskResult {
    pub task_type: TaskType,
    pub task_id: String,
}

impl<W: WorkerService, A: ArtifactClient> SP1Worker<W, A> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        prover: Arc<SP1Prover<ClusterProverComponents>>,
        gpu_semaphore: Arc<Semaphore>,
        metrics: Option<Arc<WorkerMetrics>>,
        worker_client: W,
        artifact_client: A,
    ) -> Self {
        let gpu_mem_gb = std::env::var("WORKER_GPU_MEM_GB")
            .unwrap_or("24".to_string())
            .parse()
            .expect("WORKER_GPU_MEM_GB must be a number");
        tracing::info!("gpu_mem_gb: {}", gpu_mem_gb);
        let mut prover_opts = SP1ProverOpts::gpu(16, gpu_mem_gb);
        prover_opts.core_opts.shard_size = 1 << 21;

        tracing::info!("using prover opts: {:?}", prover_opts);

        // Cache all programs on GPU workers.
        let mut compress_keys = BTreeMap::new();
        let worker_type = env::var("WORKER_TYPE").unwrap_or("unknown".to_string());
        if worker_type == WorkerType::Gpu.as_str_name() {
            for (shape, program) in prover.join_programs_map.iter() {
                let (pk, vk) = prover.compress_prover.setup(program);
                compress_keys.insert(shape.clone(), Arc::new((pk, vk)));
            }
        }
        // Initialize base metrics
        #[cfg(feature = "gpu")]
        metrics.as_ref().map(|m| m.num_gpu_workers.set(1.0));

        Self {
            prover,
            prover_opts,
            gpu_semaphore,
            compress_keys: RwLock::new(compress_keys),
            metrics,
            worker_client,
            artifact_client,
            vkey_cache: Arc::new(Mutex::new(LruCache::new(20.try_into().unwrap()))),
        }
    }

    /// Process a single task.
    pub async fn run_task(
        self: &Arc<Self>,
        task: &WorkerTask,
    ) -> (TaskStatus, Option<TaskMetadata>) {
        let start_time = Instant::now();

        let data = task.data.as_ref().unwrap();
        let task_type = data.task_type();
        let task_type_str = task_type.as_str_name().to_string();
        self.metrics
            .as_ref()
            .map(|m| m.increment_tasks_in_progress(task_type_str.clone()));
        let _guard = DeferGuard::new((self.metrics.clone(), task_type_str), |(m, t)| {
            m.as_ref().map(|m| m.decrement_tasks_in_progress(t));
        });

        // Setup outermost tracing span for execute task
        let metadata = serde_json::from_str::<HashMap<String, String>>(&data.metadata).unwrap();
        let (span, parent_context) = {
            // If there wasn't an outer span created (which is the case if the task was created by fulfiller)
            let outer_span = if metadata.is_empty() && task_type == TaskType::Controller {
                Some(
                    info_span!(
                        "Prove",
                        otel.name = format!("Prove {}", data.proof_id),
                        proof.id = %data.proof_id,
                    )
                    .entered(),
                )
            } else {
                None
            };

            // Setup per-task span
            let span = tracing::info_span!(
                "task",
                task_id = task.task_id,
                proof_id = data.proof_id,
                otel.name = task_type.as_str_name(),
            );

            // Get parent_context which should be the outermost span context, passed down to each task
            // so that new proving phases appear as child spans of the outermost span
            let parent_context = if !metadata.is_empty() {
                // For non-execute tasks, get the context from task metadata
                let parent_context = global::get_text_map_propagator(|p| p.extract(&metadata));
                span.set_parent(parent_context.clone());
                Some(parent_context)
            } else {
                // This must be an execute task so we use the outer span's context as parent_context
                outer_span.as_ref().map(|outer_span| outer_span.context())
            };
            drop(outer_span);
            (span, parent_context)
        };

        // Create an inner span with no parent so any child spans appear in this span instead,
        // preventing the root span from becoming too large. The outer span will have a link to the
        // inner span and still shows the same duration as the inner span.
        let inner_span = info_span!(
            parent: None,
            "task",
            proof_id = data.proof_id,
            task_id = task.task_id,
            otel.name = task_type.as_str_name()
        );
        span.add_link(inner_span.context().span().span_context().clone());
        inner_span.add_link(span.context().span().span_context().clone());

        let result = async {
            let context = parent_context.unwrap_or_else(Context::current);

            match task_type {
                TaskType::Controller => self.process_sp1_controller(context, task).await,
                TaskType::ProveShard => self.process_sp1_prove_shard(task).await,
                TaskType::RecursionDeferred => {
                    self.process_sp1_recursion_deferred_batch(task).await
                }
                TaskType::RecursionReduce => self.process_sp1_recursion_reduce_batch(task).await,
                TaskType::ShrinkWrap => self.process_sp1_shrink_wrap(context, task).await,
                TaskType::SetupVkey => self.process_sp1_setup_vkey(task).await,
                TaskType::PlonkWrap => self.process_sp1_finalize(task, ProofMode::Plonk).await,
                TaskType::Groth16Wrap => self.process_sp1_finalize(task, ProofMode::Groth16).await,
                TaskType::MarkerDeferredRecord => {
                    log::error!("MarkerDeferredRecord is only a marker task");
                    Ok(TaskMetadata::default())
                }
                TaskType::UnspecifiedTaskType => {
                    log::error!("Unspecified task type");
                    Ok(TaskMetadata::default())
                }
            }
        }
        .instrument(inner_span)
        .instrument(span)
        .await;

        self.metrics.as_ref().map(|m| {
            m.record_task_processing_duration(
                task_type.as_str_name().to_string(),
                start_time.elapsed().as_millis() as f64,
            )
        });
        self.metrics
            .as_ref()
            .map(|m| m.increment_tasks_processed(task_type.as_str_name().to_string()));

        let status = if let Err(err) = &result {
            // Determine the failed status
            match err {
                TaskError::Retryable(err) => {
                    log::error!(
                        "Retryable error in task {} proof {}: TaskType: {:?}, Error: {:?}",
                        task.task_id,
                        data.proof_id,
                        task_type,
                        err
                    );
                    self.metrics.as_ref().map(|m| {
                        m.increment_task_failures(task_type.as_str_name().to_string(), true)
                    });
                    TaskStatus::FailedRetryable
                }
                TaskError::Fatal(err) => {
                    log::error!(
                        "Fatal error in task {} proof {}: TaskType: {:?}, Error: {:?}",
                        task.task_id,
                        data.proof_id,
                        task_type,
                        err
                    );
                    self.metrics.as_ref().map(|m| {
                        m.increment_task_failures(task_type.as_str_name().to_string(), false)
                    });

                    try_unclaim_proof(
                        &self.worker_client,
                        data.proof_id.clone(),
                        Some(task.task_id.to_string()),
                    )
                    .await;

                    TaskStatus::FailedFatal
                }
                TaskError::Execution(err) => {
                    log::error!(
                        "Execution error on task {} proof {}: {:?}",
                        task.task_id,
                        data.proof_id,
                        err
                    );

                    try_unclaim_proof(
                        &self.worker_client,
                        data.proof_id.clone(),
                        Some(task.task_id.to_string()),
                    )
                    .await;

                    TaskStatus::Succeeded
                }
            }
        } else {
            self.metrics
                .as_ref()
                .map(|m| m.increment_task_successes(task_type.as_str_name().to_string()));
            TaskStatus::Succeeded
        };

        (status, result.ok())
    }
}

/// Unclaims a proof request and sets all associated tasks to FAILED_FATAL.
pub async fn try_unclaim_proof<W: WorkerService>(
    cluster_client: &W,
    proof_id: String,
    task_id: Option<String>,
) {
    log::info!("Unclaiming proof {}", proof_id);

    if let Err(err) = cluster_client
        .complete_proof(proof_id, task_id, ProofRequestStatus::Failed)
        .await
    {
        log::error!("while unclaiming proof: {:?}", err);
    }
}
