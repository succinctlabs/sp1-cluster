pub mod config;
mod heartbeat;

use config::NodeConfig;
use heartbeat::{send_beat, HeartbeatHandle};

use dashmap::DashMap;
use sp1_cluster_artifact::ArtifactClient;
use sp1_cluster_common::proto::{
    self, server_message, CloseRequest, CompleteTaskRequest, FailTaskRequest, TaskData, WorkerType,
};
use sp1_cluster_worker::client::WorkerServiceClient;
use sp1_cluster_worker::config::cluster_worker_config;
use sp1_cluster_worker::metrics::WorkerMetrics;
use sp1_cluster_worker::SP1ClusterWorker;
use sp1_prover::worker::WorkerClient;
use sp1_prover::SP1ProverComponents;
use sp1_sdk::install::try_install_circuit_artifacts;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};
use sysinfo::{MemoryRefreshKind, RefreshKind, System};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub async fn run(
    node_config: NodeConfig,
    artifact_client: impl ArtifactClient,
    token: CancellationToken,
    metrics: Option<Arc<WorkerMetrics>>,
) -> eyre::Result<()> {
    if let Some(metrics) = metrics.clone() {
        tokio::spawn(gather_memory_metrics(metrics, token.clone()));
    }

    if node_config.worker_type != WorkerType::Gpu {
        download_artifacts_for_cpu_workers().await?;
    }

    // Connect to server only after artifacts are ready.
    let worker_client = WorkerServiceClient::new(
        node_config.coordinator_rpc.clone(),
        node_config.worker_id.clone(),
        node_config.worker_type,
    )
    .await?;

    match node_config.worker_type {
        #[cfg(not(feature = "gpu"))]
        WorkerType::Gpu => panic!("The \"gpu\" feature must be enabled to use WorkerType::Gpu"),
        #[cfg(not(feature = "gpu"))]
        WorkerType::All => panic!("The \"gpu\" feature must be enabled to use WorkerType::All"),
        #[cfg(feature = "gpu")]
        WorkerType::Gpu | WorkerType::All => {
            run_gpu_worker(node_config, token, metrics, artifact_client, worker_client).await?
        }
        _ => run_cpu_worker(node_config, token, metrics, artifact_client, worker_client).await?,
    }
    Ok(())
}

async fn download_artifacts_for_cpu_workers() -> eyre::Result<()> {
    let start_time = std::time::Instant::now();
    tracing::info!("Downloading circuit artifacts before connecting to server");

    let (_, _) = tokio::try_join!(
        tokio::task::spawn(async move {
            tracing::info!("Downloading groth16 artifacts");
            try_install_circuit_artifacts("groth16").await
        }),
        tokio::task::spawn(async move {
            tracing::info!("Downloading plonk artifacts");
            try_install_circuit_artifacts("plonk").await
        })
    )?;

    let elapsed = start_time.elapsed();
    tracing::info!(
        "Circuit artifacts ready after {:.1} seconds",
        elapsed.as_secs_f64()
    );

    Ok(())
}

async fn gather_memory_metrics(metrics: Arc<WorkerMetrics>, token: CancellationToken) {
    loop {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(60)) => {
                let memory = System::new_with_specifics(
                    RefreshKind::nothing().with_memory(MemoryRefreshKind::everything()),
                );
                let used_memory = memory.used_memory();
                let total_memory = memory.total_memory();
                metrics.memory_usage_bytes.set(used_memory as f64);
                metrics
                    .memory_usage_percent
                    .set(used_memory as f64 / total_memory as f64 * 100.0);
            },
            _ = token.cancelled() => {
                break
            }
        }
    }
}

pub async fn run_cpu_worker(
    node_config: NodeConfig,
    token: CancellationToken,
    metrics: Option<Arc<WorkerMetrics>>,
    artifact_client: impl ArtifactClient,
    worker_client: WorkerServiceClient,
) -> eyre::Result<()> {
    run_worker_inner(
        node_config,
        token,
        worker_client.clone(),
        Arc::new(SP1ClusterWorker::new(
            Arc::new(
                sp1_prover::worker::cpu_worker_builder()
                    .with_config(|conf| *conf = cluster_worker_config())
                    .with_artifact_client(artifact_client)
                    .with_worker_client(worker_client)
                    .build()
                    .await
                    .map_err(|e| eyre::eyre!("failed to build cpu worker: {e}"))?,
            ),
            metrics,
        )),
    )
    .await
}

#[cfg(feature = "gpu")]
pub async fn run_gpu_worker(
    node_config: NodeConfig,
    token: CancellationToken,
    metrics: Option<Arc<WorkerMetrics>>,
    artifact_client: impl ArtifactClient,
    worker_client: WorkerServiceClient,
) -> eyre::Result<()> {
    if let Some(metrics) = metrics.clone() {
        metrics.num_gpu_workers.set(1.0);
    }
    sp1_gpu_cudart::spawn(move |t| async move {
        run_worker_inner(
            node_config,
            token,
            worker_client.clone(),
            Arc::new(SP1ClusterWorker::new(
                Arc::new(
                    sp1_gpu_prover::cuda_worker_builder(t)
                        .await
                        .with_config(|conf| *conf = cluster_worker_config())
                        .with_artifact_client(artifact_client)
                        .with_worker_client(worker_client)
                        .build()
                        .await
                        .map_err(|e| eyre::eyre!("failed to build gpu worker: {e}"))?,
                ),
                metrics,
            )),
        )
        .await
    })
    .await
    .unwrap()
}

pub(crate) type ActiveTask = (TaskData, JoinHandle<()>, Instant);

async fn run_worker_inner(
    node_config: NodeConfig,
    token: CancellationToken,
    worker_client: WorkerServiceClient,
    worker: Arc<SP1ClusterWorker<impl WorkerClient, impl ArtifactClient, impl SP1ProverComponents>>,
) -> eyre::Result<()> {
    let tasks: Arc<DashMap<(String, String), ActiveTask>> = Arc::new(DashMap::new());

    // Beats stop when this future ends (return or harness kill).
    let heartbeat = HeartbeatHandle::spawn(node_config.clone(), tasks.clone(), token.clone())?;

    let main_handle = tokio::spawn({
        let beats = heartbeat.clock();
        // `open()` is single-attempt; retry here so a worker booting before
        // the coordinator is reachable waits instead of crash-looping.
        let mut channel = loop {
            match worker_client.open().await {
                Ok(channel) => break channel,
                Err(e) => {
                    tracing::warn!("Failed to open coordinator channel, retrying: {}", e);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        };
        let tasks = tasks.clone();
        let token = token.clone();
        async move {
            let mut last_heartbeat = Instant::now();
            // A reconnect can "succeed" yet carry no message, and the dedicated
            // thread keeps beating, so the coordinator would hold this worker
            // forever. Exiting stops the beats and lets it requeue. Progress is
            // an inbound message or a failed connect — an unreachable
            // coordinator is an outage, not a wedge, so keep retrying.
            //
            // Prompts come every 5s and 10s of quiet forces a reconnect, so 45s
            // is about four reconnects that each carried nothing: no longer a
            // slow coordinator. Adding the coordinator's 30s eviction, a wedged
            // worker's tasks are back in the queue in ~75s.
            let mut last_progress = Instant::now();
            const MAX_CHANNEL_SILENCE: Duration = Duration::from_secs(45);
            const WATCHDOG_PERIOD: Duration = Duration::from_secs(5);
            // Three periods. Long enough that normal scheduling jitter never
            // trips it, short enough to catch the stalls that do.
            const STALLED_TICK_GAP: Duration = Duration::from_secs(15);
            let mut watchdog_ticker = tokio::time::interval(WATCHDOG_PERIOD);
            watchdog_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut last_tick = Instant::now();
            let mut closed = false;
            let mut drain_started_at: Option<Instant> = None;
            let mut last_drain_log_count: Option<usize> = None;
            let tasks = tasks.clone();
            loop {
                tokio::select! {
                    // Biased so a loop waking from a long stall drains buffered
                    // messages before any watchdog reads their timestamps and
                    // misreads the stall as a dead channel. Load-bearing: with
                    // random polling, a woken loop can reconnect on stale
                    // `last_heartbeat` and discard messages it has not read.
                    biased;
                    msg = channel.recv() => {
                        match msg {
                            Some(server_msg) => {
                                last_progress = Instant::now();
                                match server_msg.message {
                                Some(server_message::Message::NewTask(task)) => {
                                    let data = task.data().unwrap();
                                    tracing::info!("Received task: {}", task.task_id);
                                    if closed {
                                        tracing::warn!("Worker is closed, ignoring task {}", task.task_id);
                                        continue;
                                    }
                                    if tasks.contains_key(&(data.proof_id.clone(), task.task_id.clone())) {
                                        tracing::info!("Already working on task {}", task.task_id);
                                        continue;
                                    }

                                    let task_type = data.task_type();
                                    let proof_id = data.proof_id.clone();
                                    let key = (proof_id, task.task_id.clone());
                                    let handle = tokio::spawn({
                                        let worker = worker.clone();
                                        let task = task.clone();
                                        let data = data.clone();
                                        let worker_client = worker_client.clone();
                                        let key = key.clone();
                                        let worker_id = node_config.worker_id.clone();
                                        let tasks = tasks.clone();
                                        async move {
                                            let (status, metadata) = worker
                                                .run_task(&task)
                                                .await;
                                            match status {
                                                proto::TaskStatus::Succeeded => {
                                                    let metadata_string = serde_json::to_string(&metadata.expect("successful task should have metadata")).unwrap();
                                                    if let Err(e) = worker_client.complete_task(CompleteTaskRequest {
                                                        worker_id,
                                                        proof_id: data.proof_id.clone(),
                                                        task_id: task.task_id.clone(),
                                                        metadata: metadata_string,
                                                    }).await {
                                                        tracing::error!("Failed to complete task: {:?}", e);
                                                    }
                                                }
                                                _ => {
                                                    let retryable = status
                                                        == sp1_cluster_common::proto::TaskStatus::FailedRetryable;
                                                    if let Err(e) = worker_client.fail_task(FailTaskRequest {
                                                        worker_id,
                                                        proof_id: data.proof_id.clone(),
                                                        task_id: task.task_id.clone(),
                                                        retryable,
                                                    }).await {
                                                        tracing::error!("Failed to fail task: {:?}", e);
                                                    }
                                                }
                                            }
                                            let removed = tasks.remove(&key);
                                            tracing::info!(
                                                "Completed task {:?} {:?} after {:?}",
                                                task_type,
                                                key,
                                                removed.map(|r| r.1 .2.elapsed())
                                            );
                                        }
                                    });
                                    tasks.insert(key, (task.data.unwrap().clone(), handle, Instant::now()));
                                }
                                Some(server_message::Message::CancelTask(task)) => {
                                    if let Some(entry) =
                                        tasks.get(&(task.proof_id.clone(), task.task_id.clone()))
                                    {
                                        let (_, handle, _) = entry.value();
                                        tracing::info!("Aborting task {} {}", task.proof_id, task.task_id);
                                        handle.abort();
                                        drop(entry);
                                        tasks.remove(&(task.proof_id, task.task_id.clone()));
                                    }
                                }
                                Some(server_message::Message::ServerHeartbeat(_)) => {
                                    // A prompt proves the delivery stream works; liveness
                                    // replies are the dedicated heartbeat thread's job.
                                    last_heartbeat = Instant::now();
                                    // The thread has its own connection, which can fail
                                    // while this stream is fine, and a worker that stops
                                    // beating is evicted no matter how alive it is.
                                    if beats.is_stale() {
                                        tracing::debug!("dedicated beats stale; beating on the delivery stream");
                                        send_beat(&worker_client, &tasks).await;
                                    }
                                }
                                None => {}
                                }
                            }
                            None => {
                                tracing::error!("Server closed connection");
                                // Try to reconnect with exponential backoff
                                match worker_client.open().await
                                {
                                    Ok(new_channel) => {
                                        channel = new_channel;
                                        last_heartbeat = Instant::now(); // Reset heartbeat timer
                                    }
                                    Err(e) => {
                                        tracing::error!("Failed to reconnect: {}", e);
                                        last_progress = Instant::now();
                                        tokio::time::sleep(Duration::from_secs(1)).await;
                                        continue;
                                    }
                                }
                            }
                        }
                    }
                    _ = watchdog_ticker.tick() => {
                        // A tick this late means the runtime just woke from a stall,
                        // so inbound messages may still be sitting in the receive
                        // pump and `last_progress` reads staler than the channel
                        // really is. Skip the silence check until ticks recover.
                        let tick_gap = last_tick.elapsed();
                        let woke_from_stall = tick_gap > STALLED_TICK_GAP;
                        if woke_from_stall {
                            // Logged because it disables the silence check, and a
                            // worker stalling every tick keeps it disabled.
                            tracing::warn!(
                                "watchdog tick {tick_gap:?} late; skipping channel silence check"
                            );
                        }
                        last_tick = Instant::now();
                        // If the worker is closed and there's no tasks, break out of the loop.
                        if closed && tasks.is_empty() {
                            tracing::info!("Worker is closed and has no tasks, breaking out of loop");
                            break;
                        }
                        if let Some(started) = drain_started_at {
                            let elapsed = started.elapsed();
                            let in_flight = tasks.len();
                            if elapsed > node_config.drain_timeout {
                                let stuck: Vec<_> = tasks.iter().map(|e| e.key().clone()).collect();
                                tracing::warn!(
                                    ?stuck,
                                    "Drain timeout ({:?}) exceeded with {} task(s) still running; forcing exit",
                                    node_config.drain_timeout,
                                    in_flight,
                                );
                                break;
                            }
                            // Log only on count change to avoid spamming every tick.
                            if last_drain_log_count != Some(in_flight) {
                                tracing::info!(
                                    "Draining: {} task(s) in flight, {:?} elapsed",
                                    in_flight,
                                    elapsed
                                );
                                last_drain_log_count = Some(in_flight);
                            }
                        }
                        // Draining has its own timeout (drain_timeout); don't preempt it.
                        if !closed && !woke_from_stall && last_progress.elapsed() > MAX_CHANNEL_SILENCE {
                            tracing::error!(
                                "No message from coordinator for {:?} despite successful reconnects; exiting for a clean restart",
                                last_progress.elapsed()
                            );
                            // Best-effort trace flush; exit(1) would skip the post-loop shutdown.
                            let _ = tokio::time::timeout(
                                Duration::from_secs(5),
                                tokio::task::spawn_blocking(opentelemetry::global::shutdown_tracer_provider),
                            )
                            .await;
                            std::process::exit(1);
                        }
                        if last_heartbeat.elapsed() > Duration::from_secs(10) {
                            tracing::error!("Heartbeat timed out, reconnecting...");
                            match worker_client.open().await {
                                Ok(new_channel) => {
                                    channel = new_channel;
                                    last_heartbeat = Instant::now(); // Reset heartbeat timer
                                }
                                Err(e) => {
                                    tracing::error!("Failed to reconnect: {}", e);
                                    last_progress = Instant::now();
                                    tokio::time::sleep(Duration::from_secs(1)).await;
                                    continue;
                                }
                            }
                        }
                        // Handle panicked tasks
                        let mut panicked_tasks = HashSet::new();
                        let mut timed_out_tasks = HashSet::new();
                        for entry in tasks.iter_mut() {
                            // The threads should be removing from tasks when they complete, so
                            // if it's reached this point, it must be because it panicked.
                            if entry.value().1.is_finished() {
                                panicked_tasks.insert(entry.key().clone());
                            } else if entry.value().2.elapsed() > node_config.task_timeout {
                                timed_out_tasks.insert(entry.key().clone());
                            }
                        }
                        for task_id in panicked_tasks {
                            let Some((_, task)) = tasks.remove(&task_id) else {
                                tracing::warn!("Task {:?} was panicked but is not in tasks anymore", task_id);
                                continue;
                            };
                            if let Err(e) = task.1.await {
                                tracing::error!("Task {:?} panicked: {:?}", task_id, e);
                                if let Err(e) = worker_client.fail_task(FailTaskRequest {
                                    worker_id: node_config.worker_id.clone(),
                                    proof_id: task_id.0,
                                    task_id: task_id.1,
                                    retryable: false,
                                }).await {
                                    tracing::error!("Failed to update task status: {:?}", e);
                                }
                            } else {
                                tracing::warn!("Task completed without removing from tasks map? {:?}", task_id);
                            }
                        }
                        for task_id in timed_out_tasks {
                            let Some((_, task)) = tasks.remove(&task_id) else {
                                tracing::warn!("Task {:?} timed out but is not in tasks anymore", task_id);
                                continue;
                            };
                            tracing::error!("Task {:?} timed out after {:?}", task_id, node_config.task_timeout);
                            task.1.abort();
                            if let Err(e) = worker_client.fail_task(FailTaskRequest {
                                worker_id: node_config.worker_id.clone(),
                                proof_id: task_id.0,
                                task_id: task_id.1,
                                retryable: true,
                            }).await {
                                tracing::error!("Failed to update task status: {:?}", e);
                            }
                        }
                    }
                    _ = token.cancelled(), if !closed => {
                        closed = true;
                        drain_started_at = Some(Instant::now());
                        if let Err(e) = worker_client.close(CloseRequest {
                            worker_id: node_config.worker_id.clone(),
                        }).await {
                            tracing::error!("Failed to close worker: {:?}", e);
                        }
                        if tasks.is_empty() {
                            tracing::info!("No in-flight tasks; shutting down immediately");
                            break;
                        } else {
                            tracing::info!(
                                "Shutdown signal received; draining {} in-flight task(s)",
                                tasks.len()
                            );
                        }
                    }
                }
            }
        }
    });

    // Abort the spawned worker loop if this future is dropped (abrupt cancellation, or a
    // harness killing the node component): a detached loop would otherwise keep streaming
    // and answering heartbeats forever, so the coordinator never notices the worker died.
    let _abort_guard =
        sp1_cluster_worker::utils::DeferGuard::new(main_handle.abort_handle(), |h| h.abort());

    tracing::info!("Waiting for main loop...");

    main_handle.await?;

    tracing::info!("Main loop complete");

    // Try to shutdown with timeout
    tokio::select! {
        res = tokio::task::spawn_blocking(opentelemetry::global::shutdown_tracer_provider) => {
            if let Err(e) = res {
                tracing::error!("shutdown_tracer_provider error: {:?}", e);
            }
        },
        _ = tokio::time::sleep(Duration::from_secs(5)) => {
            println!("failed to shutdown_tracer_provider");
        },
    }

    Ok(())
}
