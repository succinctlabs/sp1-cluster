use cfg_if::cfg_if;
use dashmap::DashMap;
use eyre::Result;
use flate2::read::GzDecoder;
use jemallocator::Jemalloc;
use opentelemetry::KeyValue;
use rand::Rng;
use sp1_cluster_artifact::redis::RedisArtifactClient;
use sp1_cluster_artifact::s3::S3DownloadMode;
use sp1_cluster_artifact::s3_rest::S3RestClient;
use sp1_cluster_artifact::ArtifactClient;
use sp1_cluster_artifact::{s3::S3ArtifactClient, ArtifactType};
use sp1_cluster_common::proto::{
    self, server_message, CloseRequest, CompleteTaskRequest, FailTaskRequest, HeartbeatRequest,
    TaskData, WorkerType,
};
use sp1_cluster_common::util::get_private_ip;
use sp1_cluster_worker::client::WorkerServiceClient;
use sp1_cluster_worker::metrics::{initialize_metrics, WorkerMetrics};
use sp1_cluster_worker::utils::get_ecs_task_info;
use sp1_cluster_worker::SP1Worker;
use sp1_sdk::SP1_CIRCUIT_VERSION;
use std::collections::HashSet;
use std::env;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use sysinfo::{MemoryRefreshKind, RefreshKind, System};
use tar::Archive;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

#[global_allocator]
pub static ALLOCATOR: Jemalloc = Jemalloc;

type ActiveTask = (TaskData, JoinHandle<()>, Instant);

/// To prevent stuck tasks from accumulating due to any deadlock bug or similar issue, tasks will be
/// killed after running for 6 hours.
const TASK_TIMEOUT: Duration = Duration::from_secs(6 * 60 * 60);

#[tokio::main]
async fn main() -> Result<()> {
    std::env::set_var("PROVER_CORE_CACHE_SIZE", "1");
    std::env::set_var("PROVER_COMPRESS_CACHE_SIZE", "1");
    std::env::set_var("SP1_ALLOW_DEPRECATED_HOOKS", "true");

    // Initialize the enviroment variables.
    match dotenv::dotenv() {
        std::result::Result::Ok(_) => {}
        Err(e) => {
            println!("failed to load .env file: {:?}", e);
        }
    }

    let shutting_down = Arc::new(AtomicBool::new(false));
    let shutting_down_clone = shutting_down.clone();
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let (metrics, _metrics_server_handle, metrics_shutdown_tx) =
        initialize_metrics().await.map_err(|e| eyre::eyre!(e))?;

    ctrlc::set_handler(move || {
        let is_shutting_down = shutting_down_clone.load(Ordering::Relaxed);
        if !is_shutting_down {
            println!(
                "\nReceived signal, waiting for tasks to finish... (Ctrl+C again to force exit)"
            );
            shutting_down_clone.store(true, Ordering::Relaxed);
            shutdown_tx.send(true).unwrap();
            metrics_shutdown_tx.send(()).unwrap();
        } else {
            std::process::exit(1);
        }
    })
    .unwrap();

    if std::env::var("NODE_ARTIFACT_STORE").unwrap_or("s3".to_string()) == "s3" {
        eprintln!("using s3 artifact store");
        let region = std::env::var("NODE_S3_REGION").expect("NODE_S3_REGION is not set");
        let artifact_client = S3ArtifactClient::new(
            region.clone(),
            std::env::var("NODE_S3_BUCKET").expect("NODE_S3_BUCKET is not set"),
            std::env::var("NODE_S3_CONCURRENCY")
                .map(|s| s.parse().unwrap_or(32))
                .unwrap_or(32),
            S3DownloadMode::AwsSDK(S3ArtifactClient::create_s3_sdk_download_client(region).await),
        )
        .await;
        run_worker(shutting_down, shutdown_rx, Some(metrics), artifact_client).await?;
    } else {
        eprintln!("using redis artifact store");
        let artifact_client = RedisArtifactClient::new(
            std::env::var("NODE_REDIS_NODES")
                .expect("NODE_REDIS_NODES is not set")
                .split(',')
                .map(|s| s.to_string())
                .collect(),
            std::env::var("NODE_REDIS_POOL_MAX_SIZE")
                .unwrap_or("16".to_string())
                .parse()
                .unwrap(),
        );
        run_worker(shutting_down, shutdown_rx, Some(metrics), artifact_client).await?;
    };

    Ok(())
}

/// Install circuit artifacts using chunked parallel downloads.
///
/// This function downloads circuit artifacts using S3 chunked parallel downloads for better
/// performance, then extracts them using the same tar approach as the SDK.
fn install_circuit_artifacts(artifact_type: &str, artifact_client: S3ArtifactClient) -> PathBuf {
    let home_dir = dirs::home_dir().unwrap();
    let final_dir = match artifact_type {
        "groth16" => sp1_sdk::install::groth16_circuit_artifacts_dir(),
        "plonk" => sp1_sdk::install::plonk_circuit_artifacts_dir(),
        _ => panic!("invalid artifact type: {}", artifact_type),
    };
    if final_dir.exists() {
        // Clear the directory every time, since contents may not be valid.
        if std::fs::read_dir(&final_dir).unwrap().next().is_some() {
            return final_dir;
        }
    } else if let Err(e) = std::fs::create_dir_all(&final_dir) {
        tracing::info!("Not creating circuits dir: {:?}", e);
    }
    let temp_dir = home_dir
        .join(".sp1/circuits/temp")
        .join(artifact_type)
        .join(SP1_CIRCUIT_VERSION);
    if temp_dir.exists() {
        std::fs::remove_dir_all(&temp_dir).unwrap();
    }
    if let Err(e) = std::fs::create_dir_all(&temp_dir) {
        tracing::info!("Not creating temp dir: {:?}", e);
    }

    // Use chunked parallel download with provided client.
    let rt_handle = tokio::runtime::Handle::current();
    rt_handle.block_on(async {
        let artifact_type_enum = match artifact_type {
            "groth16" => ArtifactType::Groth16Circuit,
            "plonk" => ArtifactType::PlonkCircuit,
            _ => panic!("invalid artifact type: {}", artifact_type),
        };

        // Download tar.gz bytes using chunked parallel download.
        let download_start = std::time::Instant::now();
        let tar_gz_bytes = artifact_client
            .par_download_file(artifact_type_enum, SP1_CIRCUIT_VERSION)
            .await
            .expect("failed to download circuit artifacts");
        let download_duration = download_start.elapsed();
        tracing::info!(
            "{} download completed in {:.2} seconds ({} bytes)",
            artifact_type,
            download_duration.as_secs_f64(),
            tar_gz_bytes.len()
        );
        // Extract artifacts.
        let extract_start = std::time::Instant::now();
        let gz = GzDecoder::new(Cursor::new(tar_gz_bytes));
        let mut archive = Archive::new(gz);
        archive
            .unpack(&temp_dir)
            .expect("failed to extract archive");
        let extract_duration = extract_start.elapsed();
        tracing::info!(
            "{} extraction completed in {:.2} seconds",
            artifact_type,
            extract_duration.as_secs_f64()
        );

        let total_duration = download_start.elapsed();
        tracing::info!(
            "{} total download + extract completed in {:.2} seconds",
            artifact_type,
            total_duration.as_secs_f64()
        );
    });

    // Move to final location.
    std::fs::rename(&temp_dir, &final_dir).unwrap();

    final_dir
}

/// Run the worker.
async fn run_worker<A: ArtifactClient>(
    shutting_down: Arc<AtomicBool>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    metrics: Option<Arc<WorkerMetrics>>,
    artifact_client: A,
) -> Result<()> {
    {
        // Get info about the worker.
        let http_client = reqwest::Client::new();
        let ecs_task_info = get_ecs_task_info(&http_client).await;
        let cluster = ecs_task_info
            .as_ref()
            .map_or("unknown".to_string(), |info| info.cluster.clone());

        let worker_id = {
            match ecs_task_info {
                std::result::Result::Ok(info) => {
                    info.task_arn.split('/').next_back().unwrap().to_string()
                }
                _ => {
                    // Random hex string
                    let mut rng = rand::rng();
                    let mut node_id = "unknown_".to_string();
                    for _ in 0..16 {
                        node_id.push_str(&format!("{:02x}", rng.random::<u8>()));
                    }
                    node_id
                }
            }
        };

        // Setup logging/tracing resource values.
        let params = vec![
            KeyValue::new("service.name", "sp1-api2-client"),
            KeyValue::new("node.cluster", cluster),
            KeyValue::new("node.id", worker_id.clone()),
        ];
        let resource = opentelemetry_sdk::Resource::new(params);
        sp1_cluster_common::logger::init(resource);

        let addr = env::var("NODE_COORDINATOR_RPC").unwrap_or("http://[::1]:50051".to_string());

        // Print private IP for debugging
        let private_ip = get_private_ip();
        match private_ip {
            Ok(Some(ip)) => {
                tracing::info!("private IP address: {}", ip);
            }
            Ok(None) => {
                tracing::info!("no private IP address found");
            }
            Err(e) => {
                tracing::error!("failed to get private IP address: {:?}", e);
            }
        }

        let worker_type_str = env::var("WORKER_TYPE").expect("WORKER_TYPE is not set");
        let worker_type = WorkerType::from_str_name(&worker_type_str).expect("invalid worker type");
        tracing::info!("worker type: {:?}", worker_type);

        // For CPU workers, download circuit artifacts before connecting.
        if worker_type != WorkerType::Gpu {
            let start_time = std::time::Instant::now();
            tracing::info!("Downloading circuit artifacts before connecting to server");

            // Create a single S3ArtifactClient for both downloads.
            let concurrency = std::env::var("S3_CONCURRENCY")
                .map(|s| s.parse().unwrap_or(32))
                .unwrap_or(32);
            let region = "us-east-2".to_string();
            let bucket = "sp1-circuits".to_string();
            tracing::info!(
                "Creating S3ArtifactClient - concurrency: {}, region: {}, bucket: {}",
                concurrency,
                region.clone(),
                bucket
            );

            let artifact_client_rest = S3ArtifactClient::new(
                region.clone(),
                bucket,
                concurrency,
                S3DownloadMode::REST(Arc::new(S3RestClient::new(region.clone()))),
            )
            .await;
            // Download groth16 and plonk artifacts concurrently using shared client.
            // these artifacts will be downloaded with public s3 obj urls.
            let (_, _) = tokio::try_join!(
                tokio::task::spawn_blocking({
                    let artifact_client = artifact_client_rest.clone();
                    move || {
                        tracing::info!("Downloading groth16 artifacts");
                        install_circuit_artifacts("groth16", artifact_client)
                    }
                }),
                tokio::task::spawn_blocking({
                    let artifact_client = artifact_client_rest.clone();
                    move || {
                        tracing::info!("Downloading plonk artifacts");
                        install_circuit_artifacts("plonk", artifact_client)
                    }
                })
            )?;

            let elapsed = start_time.elapsed();
            tracing::info!(
                "Circuit artifacts ready after {:.1} seconds",
                elapsed.as_secs_f64()
            );
        }

        // Connect to server only after artifacts are ready.
        let client = WorkerServiceClient::new(addr.clone(), worker_id.clone()).await?;

        let tasks: Arc<DashMap<(String, String), ActiveTask>> = Arc::new(DashMap::new());

        let gpu_semaphore = Arc::new(Semaphore::new(1));

        // Gather memory metrics.
        tokio::spawn({
            let metrics = metrics.clone();
            let shutting_down = shutting_down.clone();
            async move {
                while !shutting_down.load(Ordering::Relaxed) {
                    let memory = System::new_with_specifics(
                        RefreshKind::nothing().with_memory(MemoryRefreshKind::everything()),
                    );
                    let used_memory = memory.used_memory();
                    let total_memory = memory.total_memory();

                    if let Some(ref m) = metrics {
                        m.memory_usage_bytes.set(used_memory as f64);
                        m.memory_usage_percent
                            .set(used_memory as f64 / total_memory as f64 * 100.0);
                    }
                    tokio::time::sleep(Duration::from_secs(60)).await;
                }
            }
        });

        // Setup SP1 prover.
        cfg_if! {
            if #[cfg(feature = "gpu")] {
                let inner_prover = moongate_prover::SP1GpuProver::new();
            } else {
                let inner_prover = sp1_prover::SP1Prover::new();
            }
        }
        let prover = Arc::new(inner_prover);

        // Create worker.
        let worker = Arc::new(SP1Worker::new(
            prover.clone(),
            gpu_semaphore,
            metrics, // Pass metrics to worker
            client.clone(),
            artifact_client,
        ));

        let mut channel = client.open().await?;

        // Spawn task to handle messages from the coordinator.
        let main_handle = tokio::spawn({
            let tasks = tasks.clone();
            let mut shutdown_rx = shutdown_rx.clone();
            async move {
                let mut last_heartbeat = Instant::now();
                let mut heartbeat_ticker = tokio::time::interval(Duration::from_secs(5));
                let mut closed = false;
                let tasks = tasks.clone();
                loop {
                    tokio::select! {
                        msg = channel.recv() => {
                            match msg {
                                Some(server_msg) => match server_msg.message {
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
                                            let client = client.clone();
                                            let key = key.clone();
                                            let worker_id = worker_id.clone();
                                            let tasks = tasks.clone();
                                            async move {
                                                let (status, metadata) = worker
                                                    .run_task(&task)
                                                    .await;
                                                match status {
                                                    proto::TaskStatus::Succeeded => {
                                                        let metadata_string = serde_json::to_string(&metadata.expect("successful task should have metadata")).unwrap();
                                                        if let Err(e) = client.complete_task(CompleteTaskRequest {
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
                                                        if let Err(e) = client.fail_task(FailTaskRequest {
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
                                        let (task_proof_ids, task_ids) = tasks.iter().map(|v| v.key().clone()).unzip();
                                        let current_weight = tasks.iter().map(|v| v.value().0.weight).sum();
                                        let request = HeartbeatRequest {
                                            worker_id: worker_id.clone(),
                                            active_task_proof_ids: task_proof_ids,
                                            active_task_ids: task_ids,
                                            current_weight,
                                        };
                                        if let Err(e) = client.heartbeat(request).await  {
                                            eprintln!("Failed to send heartbeat: {}", e);
                                            if e.code() == tonic::Code::NotFound {
                                                tracing::warn!("Worker not found, reconnecting...");
                                                match client.open().await {
                                                    Ok(new_channel) => {
                                                        channel = new_channel;
                                                    }
                                                    Err(e) => {
                                                        tracing::error!("Failed to reconnect: {}", e);
                                                        tokio::time::sleep(Duration::from_secs(1)).await;
                                                        continue;
                                                    }
                                                }
                                            }
                                        }

                                        last_heartbeat = Instant::now();
                                    }
                                    None => {}
                                },
                                None => {
                                    tracing::error!("Server closed connection");
                                    // Try to reconnect with exponential backoff
                                    match client.open().await
                                    {
                                        Ok(new_channel) => {
                                            channel = new_channel;
                                            last_heartbeat = Instant::now(); // Reset heartbeat timer
                                        }
                                        Err(e) => {
                                            tracing::error!("Failed to reconnect: {}", e);
                                            tokio::time::sleep(Duration::from_secs(1)).await;
                                            continue;
                                        }
                                    }
                                }
                            }
                        }
                        _ = heartbeat_ticker.tick() => {
                            // If the worker is closed and there's no tasks, break out of the loop.
                            if closed && tasks.is_empty() {
                                tracing::info!("Worker is closed and has no tasks, breaking out of loop");
                                break;
                            }
                            if last_heartbeat.elapsed() > Duration::from_secs(10) {
                                tracing::error!("Heartbeat timed out, reconnecting...");
                                match client.open().await {
                                    Ok(new_channel) => {
                                        channel = new_channel;
                                        last_heartbeat = Instant::now(); // Reset heartbeat timer
                                    }
                                    Err(e) => {
                                        tracing::error!("Failed to reconnect: {}", e);
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
                                } else if entry.value().2.elapsed() > TASK_TIMEOUT {
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
                                    if let Err(e) = client.fail_task(FailTaskRequest {
                                        worker_id: worker_id.clone(),
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
                                tracing::error!("Task {:?} timed out after {:?}", task_id, TASK_TIMEOUT);
                                task.1.abort();
                                if let Err(e) = client.fail_task(FailTaskRequest {
                                    worker_id: worker_id.clone(),
                                    proof_id: task_id.0,
                                    task_id: task_id.1,
                                    retryable: true,
                                }).await {
                                    tracing::error!("Failed to update task status: {:?}", e);
                                }
                            }
                        }
                        _ = shutdown_rx.changed() => {
                            closed = true;
                            if let Err(e) = client.close(CloseRequest {
                                worker_id: worker_id.clone(),
                            }).await {
                                tracing::error!("Failed to close worker: {:?}", e);
                            }
                            tracing::info!("Worker closed");
                        }
                    }
                }
            }
        });

        // Wait for tasks or shutdown signal
        if let Err(e) = shutdown_rx.wait_for(|v| *v).await {
            tracing::error!("shutdown error: {:?}", e);
        }

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

        tracing::info!("Shutdown complete");
    }

    Ok(())
}
