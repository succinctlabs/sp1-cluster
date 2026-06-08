#![allow(clippy::new_without_default)]
#![allow(clippy::option_map_unit_fn)]
#![allow(dropping_copy_types)]

use crate::error::TaskError;
use crate::metrics::WorkerMetrics;
use lazy_static::lazy_static;
use opentelemetry::Context;
use opentelemetry::{global, trace::TraceContextExt};
use sp1_cluster_artifact::ArtifactClient;
use sp1_cluster_common::failure::ProvingFailure;
use sp1_cluster_common::proto::{ProofRequestStatus, TaskStatus, TaskType, WorkerTask};
use sp1_prover::worker::{ProofId, SP1Worker, TaskId, TaskMetadata, WorkerClient};
use sp1_prover::SP1ProverComponents;
use sp1_prover_types::network_base_types::ProofMode;
use std::time::Instant;
use std::{collections::HashMap, env, sync::Arc};
use tracing::info_span;
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use utils::DeferGuard;

pub mod client;
pub mod config;
pub mod error;
pub mod limiter;
pub mod metrics;
pub mod tasks;
pub mod utils;

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

pub const VERGEN_GIT_SHA: &str = env!("VERGEN_GIT_SHA");
pub const VERGEN_BUILD_TIMESTAMP: &str = env!("VERGEN_BUILD_TIMESTAMP");

pub struct SP1ClusterWorker<W: WorkerClient, A: ArtifactClient, C: SP1ProverComponents> {
    pub worker: Arc<SP1Worker<A, W, C>>,
    pub metrics: Option<Arc<WorkerMetrics>>,
}

/// The result of a task which is passed back to the main thread.
pub struct TaskResult {
    pub task_type: TaskType,
    pub task_id: String,
}

impl<W: WorkerClient, A: ArtifactClient, C: SP1ProverComponents> SP1ClusterWorker<W, A, C> {
    pub fn new(worker: Arc<SP1Worker<A, W, C>>, metrics: Option<Arc<WorkerMetrics>>) -> Self {
        Self { worker, metrics }
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

            #[cfg(feature = "test-hooks")]
            if test_hook_should_fail(task_type) {
                return Err(TaskError::Retryable(anyhow::anyhow!(
                    "TEST_FAIL_TASK injected retryable failure for {:?}",
                    task_type
                )));
            }

            match task_type {
                TaskType::Controller => self.process_sp1_controller(context, task).await,
                TaskType::ProveShard => self.process_sp1_prove_shard(task).await,
                TaskType::RecursionDeferred => {
                    self.process_sp1_recursion_deferred_batch(task).await
                }
                TaskType::RecursionReduce => self.process_sp1_recursion_reduce_batch(task).await,
                TaskType::ShrinkWrap => self.process_sp1_shrink_wrap(task).await,
                TaskType::SetupVkey => self.process_sp1_setup_vkey(task).await,
                TaskType::PlonkWrap => self.process_sp1_finalize(task, ProofMode::Plonk).await,
                TaskType::Groth16Wrap => self.process_sp1_finalize(task, ProofMode::Groth16).await,
                TaskType::UtilVkeyMapController => {
                    self.process_sp1_generate_vk_controller(context, task).await
                }
                TaskType::UtilVkeyMapChunk => self.process_sp1_generate_vk_chunk(task).await,
                TaskType::MarkerDeferredRecord => {
                    log::error!("MarkerDeferredRecord is only a marker task");
                    Ok(TaskMetadata::default())
                }
                TaskType::CoreExecute => self.process_sp1_core_execute(task).await,
                TaskType::ExecuteOnly => self.process_sp1_execute_only(task).await,
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
            );
            m.increment_tasks_processed(task_type.as_str_name().to_string());
            if let Ok(Some(busy_time)) = result.as_ref().map(|r| r.gpu_ms) {
                m.gpu_busy_time.increment(busy_time);
            }
        });

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
                        self.worker.worker_client(),
                        ProofId::new(data.proof_id.clone()),
                        Some(TaskId::new(task.task_id.clone())),
                        task_type,
                        err,
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
                        self.worker.worker_client(),
                        ProofId::new(data.proof_id.clone()),
                        Some(TaskId::new(task.task_id.clone())),
                        task_type,
                        err,
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

/// Unclaim a proof and stamp `extra_data` with a `proving_failure` payload
/// so the fulfiller can surface a real `ProofRequestError`.
pub async fn try_unclaim_proof<W: WorkerClient, E: std::fmt::Debug>(
    cluster_client: &W,
    proof_id: ProofId,
    task_id: Option<TaskId>,
    task_type: TaskType,
    err: &E,
) {
    log::info!("Unclaiming proof {proof_id}");

    let payload = ProvingFailure {
        task_type,
        reason: format!("{err:?}"),
    }
    .to_extra_data();

    if let Err(err) = cluster_client
        .complete_proof(proof_id, task_id, ProofRequestStatus::Failed, payload)
        .await
    {
        log::error!("while unclaiming proof: {err:?}");
    }
}

/// Test-only fault injection: `TEST_FAIL_TASK=<TASK_TYPE_NAME>:<n>` makes the first `n`
/// attempts of that task type fail with a retryable error. Compiled out of production
/// builds (gated behind the `test-hooks` feature, enabled only by the e2e test harness).
#[cfg(feature = "test-hooks")]
fn test_hook_should_fail(task_type: TaskType) -> bool {
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::sync::LazyLock;
    static REMAINING: LazyLock<Option<(String, AtomicI64)>> = LazyLock::new(|| {
        let spec = std::env::var("TEST_FAIL_TASK").ok()?;
        let (name, n) = spec.split_once(':')?;
        Some((
            name.trim().to_string(),
            AtomicI64::new(n.trim().parse().ok()?),
        ))
    });
    match REMAINING.as_ref() {
        Some((name, left)) if name == task_type.as_str_name() => {
            left.fetch_sub(1, Ordering::SeqCst) > 0
        }
        _ => false,
    }
}
