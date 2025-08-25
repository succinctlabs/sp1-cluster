use super::*;
use crate::cluster::{spawn_proof_claimer_task, spawn_proof_status_task};
use crate::config::Settings;
use crate::latency::print_latency;
use crate::metrics::initialize_metrics;
use crate::util::{spawn_coordinator_periodic_task, spawn_heartbeat_task, OkService};
use dashmap::DashMap;
use jemallocator::Jemalloc;
use mti::prelude::{MagicTypeIdExt, V7};
use sp1_cluster_common::client::ClusterServiceClient;
use sp1_cluster_common::logger;
use sp1_cluster_common::proto::{
    self, server_sub_message, CreateTaskResponse, GetStatsResponse, ProofRequestStatus,
    ServerMessage, ServerSubMessage,
};
use std::future::Future;
use std::marker::PhantomData;
use std::net::SocketAddr;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{transport::Server, Request, Response, Status};
use tonic_web::GrpcWebLayer;

#[global_allocator]
pub static ALLOCATOR: Jemalloc = Jemalloc;

/// Generic worker service that can work with any assignment policy.
pub struct GenericWorkerService<P: AssignmentPolicy + Send + Sync> {
    /// A map of each subscriber ID to its unacknowledged messages and handler task handle.
    pub subscribers: Arc<DashMap<String, GenericSubscriberHandler<P>>>,

    /// The coordinator.
    pub coordinator: Arc<Coordinator<P>>,

    /// Phantom data to hold the policy type
    _phantom: PhantomData<P>,
}

impl<P: AssignmentPolicy + Send + Sync> Default for GenericWorkerService<P> {
    fn default() -> Self {
        Self {
            subscribers: Arc::new(DashMap::new()),
            coordinator: Arc::new(Coordinator::new()),
            _phantom: PhantomData,
        }
    }
}

#[tonic::async_trait]
impl<P: AssignmentPolicy + Send + Sync + 'static>
    sp1_cluster_common::proto::worker_service_server::WorkerService for GenericWorkerService<P>
{
    type OpenStream = UnboundedReceiverStream<Result<ServerMessage, Status>>;

    async fn open(
        &self,
        request: Request<sp1_cluster_common::proto::OpenRequest>,
    ) -> Result<Response<Self::OpenStream>, Status> {
        let (tx, rx) = mpsc::unbounded_channel();
        let hb = request.into_inner();

        // Worker registration and message handling
        tokio::spawn({
            let tx = tx.clone();
            let coordinator = self.coordinator.clone();
            async move {
                tracing::info!("Worker registered: {}", hb.worker_id);

                if let Err(e) = coordinator
                    .add_worker(hb.worker_id.clone(), hb.worker_type(), hb.max_weight, tx)
                    .await
                {
                    tracing::error!("Failed to add worker: {:?}", e);
                }
            }
        });

        Ok(Response::new(UnboundedReceiverStream::new(rx)))
    }

    async fn close(
        &self,
        request: Request<sp1_cluster_common::proto::CloseRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        track_latency!("worker.close", {
            tokio::spawn({
                let coordinator = self.coordinator.clone();
                async move { coordinator.close_worker(request.worker_id).await }
            })
            .await
            .unwrap()?;
        });
        Ok(Response::new(()))
    }

    async fn heartbeat(
        &self,
        request: Request<sp1_cluster_common::proto::HeartbeatRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        track_latency!("worker.heartbeat", {
            tokio::spawn({
                let coordinator = self.coordinator.clone();
                async move {
                    coordinator
                        .handle_heartbeat(
                            &request.worker_id,
                            &request.active_task_proof_ids,
                            &request.active_task_ids,
                            request.current_weight,
                        )
                        .await
                }
            })
            .await
            .unwrap()?;
        });
        Ok(Response::new(()))
    }

    async fn complete_task(
        &self,
        request: Request<sp1_cluster_common::proto::CompleteTaskRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        tokio::spawn({
            let coordinator = self.coordinator.clone();
            async move {
                coordinator
                    .complete_task(
                        request.worker_id,
                        request.proof_id,
                        request.task_id,
                        serde_json::from_str(&request.metadata).unwrap_or_default(),
                    )
                    .await
            }
        })
        .await
        .unwrap()?;
        Ok(Response::new(()))
    }

    async fn fail_task(
        &self,
        request: Request<sp1_cluster_common::proto::FailTaskRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        track_latency!("worker.fail_task", {
            tokio::spawn({
                let coordinator = self.coordinator.clone();
                async move {
                    coordinator
                        .fail_task(
                            request.worker_id,
                            request.proof_id,
                            request.task_id,
                            request.retryable,
                        )
                        .await
                }
            })
            .await
            .unwrap()?;
        });
        Ok(Response::new(()))
    }

    async fn create_task(
        &self,
        request: Request<sp1_cluster_common::proto::CreateTaskRequest>,
    ) -> Result<Response<sp1_cluster_common::proto::CreateTaskResponse>, Status> {
        let id = tokio::spawn({
            let coordinator = self.coordinator.clone();
            async move {
                coordinator
                    .create_task(request.into_inner().data.unwrap())
                    .await
            }
        })
        .await
        .unwrap()?;
        Ok(Response::new(CreateTaskResponse { task_id: id }))
    }

    async fn get_task_statuses(
        &self,
        request: Request<sp1_cluster_common::proto::GetTaskStatusesRequest>,
    ) -> Result<Response<sp1_cluster_common::proto::GetTaskStatusesResponse>, Status> {
        track_latency!("worker.get_task_statuses", {
            let request = request.into_inner();
            let request_clone = request.clone();
            let result = tokio::spawn({
                let coordinator = self.coordinator.clone();
                async move {
                    coordinator
                        .get_task_statuses(request.proof_id, request.task_ids)
                        .await
                }
            })
            .await
            .unwrap()?;
            tracing::info!(
                "get_task_statuses proof_id: {}, task_ids: {:?}, result: {:?}",
                request_clone.proof_id,
                request_clone.task_ids,
                result
            );
            let mut response = sp1_cluster_common::proto::GetTaskStatusesResponse::default();
            for (key, value) in result.into_iter() {
                response
                    .statuses
                    .push(sp1_cluster_common::proto::TaskStatusMapEntry {
                        key: key as i32,
                        value: Some(sp1_cluster_common::proto::TaskIdList { ids: value }),
                    });
            }
            Ok(Response::new(response))
        })
    }

    async fn complete_proof(
        &self,
        request: Request<sp1_cluster_common::proto::CompleteProofRequest>,
    ) -> Result<Response<()>, Status> {
        track_latency!("worker.complete_proof", {
            tokio::spawn({
                let coordinator = self.coordinator.clone();
                async move {
                    coordinator
                        .complete_proof(request.into_inner().proof_id)
                        .await
                }
            })
            .await
            .unwrap()?;
        });
        Ok(Response::new(()))
    }

    async fn fail_proof(
        &self,
        request: Request<sp1_cluster_common::proto::FailProofRequest>,
    ) -> Result<Response<()>, Status> {
        track_latency!("worker.fail_proof", {
            let inner = request.into_inner();
            tokio::spawn({
                let coordinator = self.coordinator.clone();
                async move {
                    coordinator
                        .fail_proof(inner.proof_id, inner.task_id, true)
                        .await
                }
            })
            .await
            .unwrap()?;
        });
        Ok(Response::new(()))
    }

    async fn create_proof(
        &self,
        request: Request<sp1_cluster_common::proto::CreateProofRequest>,
    ) -> Result<Response<sp1_cluster_common::proto::CreateProofResponse>, Status> {
        track_latency!("worker.create_proof", {
            tokio::spawn({
                let coordinator = self.coordinator.clone();
                async move { coordinator.create_proof(request.into_inner()).await }
            })
            .await
            .unwrap()
            .map(|task_id| {
                Response::new(sp1_cluster_common::proto::CreateProofResponse { task_id })
            })
        })
    }

    async fn create_dummy_proof(
        &self,
        request: Request<sp1_cluster_common::proto::CreateDummyProofRequest>,
    ) -> Result<Response<()>, Status> {
        track_latency!("worker.create_dummy_proof", {
            self.coordinator
                .create_dummy_proof(request.into_inner())
                .await
                .map(|_| Response::new(()))
        })
    }

    async fn cancel_proof(
        &self,
        request: Request<sp1_cluster_common::proto::CancelProofRequest>,
    ) -> Result<Response<()>, Status> {
        track_latency!("worker.cancel_proof", {
            tokio::spawn({
                let coordinator = self.coordinator.clone();
                async move {
                    coordinator
                        .fail_proof(request.into_inner().proof_id, None, true)
                        .await
                }
            })
            .await
            .unwrap()?;
        });
        Ok(Response::new(()))
    }

    type OpenSubStream = UnboundedReceiverStream<Result<ServerSubMessage, Status>>;

    async fn open_sub(
        &self,
        request: Request<sp1_cluster_common::proto::OpenSubRequest>,
    ) -> Result<Response<Self::OpenSubStream>, Status> {
        let (tx, rx) = mpsc::unbounded_channel();

        // RPC may not be cancel safe
        track_latency!("worker.open_sub", {
            let request = request.into_inner();
            let coordinator = self.coordinator.clone();
            let (outer_tx, outer_rx) = mpsc::unbounded_channel::<ServerSubMessage>();

            if let Some(mut sub) = self.subscribers.get_mut(&request.sub_id) {
                // If the subscriber already exists, replace the handler.
                sub.sender_handle.abort();

                // Spawn new task with old map.
                sub.sender_handle = spawn_subscriber_channel_task(
                    request.sub_id.clone(),
                    outer_rx,
                    tx,
                    sub.unacked.clone(),
                    self.subscribers.clone(),
                );
            } else {
                let map = Arc::new(DashMap::new());

                let _ = tx.send(Ok(ServerSubMessage {
                    msg_id: "msg".create_type_id::<V7>().to_string(),
                    message: Some(server_sub_message::Message::ServerHeartbeat(
                        proto::ServerSubHeartbeat {},
                    )),
                }));

                // Spawn a task to forward messages to the stream and resend any unacked messages.
                let handle = spawn_subscriber_channel_task(
                    request.sub_id.clone(),
                    outer_rx,
                    tx,
                    map.clone(),
                    self.subscribers.clone(),
                );

                self.subscribers.insert(
                    request.sub_id.clone(),
                    GenericSubscriberHandler::new(handle, map),
                );
            }

            coordinator
                .create_subscriber(
                    request.sub_id,
                    request.proof_id,
                    request.task_ids,
                    outer_tx.clone(),
                )
                .await?;
        });

        Ok(Response::new(UnboundedReceiverStream::new(rx)))
    }

    async fn update_sub(
        &self,
        request: Request<sp1_cluster_common::proto::UpdateSubRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        track_latency!("worker.update_sub", {
            tokio::spawn({
                let coordinator = self.coordinator.clone();
                async move {
                    coordinator
                        .add_subscriptions(request.sub_id, request.task_ids)
                        .await
                }
            })
            .await
            .unwrap()?;
        });
        Ok(Response::new(()))
    }

    async fn ack_sub(
        &self,
        request: Request<sp1_cluster_common::proto::AckSubRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        // Not cancel safe
        track_latency!("worker.ack_sub", {
            if let Some(sub) = self.subscribers.get_mut(&request.sub_id) {
                sub.unacked.remove(&request.msg_id);
            } else {
                tracing::error!("Unknown subscription {}", request.sub_id);
                return Err(Status::not_found("Unknown subscription"));
            }
        });
        Ok(Response::new(()))
    }

    async fn healthcheck(&self, _request: Request<()>) -> Result<Response<()>, Status> {
        Ok(Response::new(()))
    }

    async fn get_stats(&self, _: Request<()>) -> Result<Response<GetStatsResponse>, Status> {
        let response = self.coordinator.get_info().await;
        Ok(Response::new(response))
    }
}

/// A subscriber handler.
pub struct GenericSubscriberHandler<P: Send + Sync> {
    pub unacked: Arc<DashMap<String, (SystemTime, ServerSubMessage, u32)>>,
    pub sender_handle: tokio::task::JoinHandle<()>,
    _phantom: PhantomData<P>,
}

impl<P: Send + Sync> GenericSubscriberHandler<P> {
    fn new(
        sender_handle: tokio::task::JoinHandle<()>,
        unacked: Arc<DashMap<String, (SystemTime, ServerSubMessage, u32)>>,
    ) -> Self {
        Self {
            unacked,
            sender_handle,
            _phantom: PhantomData,
        }
    }
}

/// Spawn a task to send messages to a subscriber channel.
fn spawn_subscriber_channel_task<P: Send + Sync + 'static>(
    sub_id: String,
    mut outer_rx: mpsc::UnboundedReceiver<ServerSubMessage>,
    tx: mpsc::UnboundedSender<Result<ServerSubMessage, Status>>,
    map: Arc<DashMap<String, (SystemTime, ServerSubMessage, u32)>>,
    subscribers: Arc<DashMap<String, GenericSubscriberHandler<P>>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn({
        async move {
            let current_task_id = tokio::task::try_id();
            let mut interval = tokio::time::interval(Duration::from_secs(2));
            loop {
                tokio::select! {
                    Some(msg) = outer_rx.recv() => {
                        tracing::debug!("sub message received {:?}", msg);
                        map.insert(msg.msg_id.clone(), (SystemTime::now(), msg.clone(), 0u32));
                        if let Err(e) = tx.send(Ok(msg)) {
                            tracing::warn!("Failed to send message to subscriber {}: {}", sub_id, e);
                        }
                    },
                    _ = interval.tick() => {
                        // If no more messages can be sent, return.
                        if tx.is_closed() {
                            return;
                        }
                        // Loop over map and resend any messages that haven't been acked for 4 seconds.
                        track_latency!("worker.sub.resend", {
                            let now = SystemTime::now();
                            map.retain(|_, (last_acked, msg, retries)| {
                                if now.duration_since(*last_acked).unwrap_or(Duration::from_secs(0)) > Duration::from_secs(4) {
                                    *last_acked = now;
                                    *retries += 1;
                                    tracing::warn!("Resending message {}", msg.msg_id);
                                    if tx.send(Ok(msg.clone())).is_err() {
                                        // Channel is closed, so no message can be sent.
                                        return false;
                                    }
                                    if *retries > 3 {
                                        // Remove messages that have failed 3 times.
                                        tracing::warn!("Max retries for message {} reached", msg.msg_id);
                                        return false;
                                    }
                                }
                                true
                            });
                            // If map is empty and outer_rx is closed, then we're done. Remove the
                            // subscriber.
                            if map.is_empty() && outer_rx.is_closed() && outer_rx.is_empty() {
                                // Only remove if this task is still the active one for this subscriber
                                if let Some(task_id) = current_task_id {
                                    subscribers.remove_if(&sub_id, |_, subscriber| {
                                        subscriber.sender_handle.id() == task_id
                                    });
                                }
                                break;
                            }
                        });
                    },
                }
            }
        }
    })
}

/// Generic main coordinator function that can work with any assignment policy.
/// Start the coordinator server and return the coordinator instance along with a future that completes when the server shuts down.
/// This allows spawning additional tasks with access to the coordinator instance.
pub async fn start_coordinator_server<P: AssignmentPolicy + Default + Send + Sync + 'static>(
) -> Result<(Arc<Coordinator<P>>, impl Future<Output = ()>)> {
    dotenv::dotenv().ok();

    logger::init(opentelemetry_sdk::Resource::new(vec![]));

    let config = Settings::new()?;

    let addr = config.addr.parse::<SocketAddr>().unwrap();

    let mut service = GenericWorkerService::<P>::default();

    // Initialize metrics server and metrics
    let (metrics, mut metrics_server_handle, metrics_shutdown_tx) =
        initialize_metrics().await.map_err(|e| eyre::eyre!(e))?;

    // Set metrics in the coordinator
    Arc::get_mut(&mut service.coordinator)
        .unwrap()
        .set_metrics(metrics.clone());

    let (completed_tx, completed_rx) = mpsc::unbounded_channel::<ProofResult<P>>();
    let task_map = Arc::new(DashMap::<String, ProofRequestStatus>::new());
    let api_rpc =
        std::env::var("COORDINATOR_CLUSTER_RPC").unwrap_or("http://127.0.0.1:50051".to_string());
    let api_client = Arc::new(ClusterServiceClient::new(api_rpc).await?);

    service.coordinator.set_proofs_tx(completed_tx).await;

    if !config.disable_proof_status_update {
        spawn_proof_status_task(api_client.clone(), task_map.clone(), completed_rx);
    } else {
        tracing::info!(
            "COORDINATOR_DISABLE_PROOF_STATUS_UPDATE=true, will not update proof statuses"
        );
    }

    spawn_proof_claimer_task(
        api_client.clone(),
        service.coordinator.clone(),
        task_map.clone(),
    );

    spawn_heartbeat_task(service.coordinator.clone());

    spawn_coordinator_periodic_task(service.coordinator.clone());

    tracing::info!(
        "coordinator (version {}) listening on {}",
        BUILD_VERSION,
        addr
    );

    // Create a channel to signal shutdown
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    // Store the shutdown sender in an Arc to share between handlers
    let shutdown_tx = Arc::new(std::sync::Mutex::new(Some(shutdown_tx)));

    let middleware = tower::ServiceBuilder::new()
        .layer_fn(|s| OkService { inner: s })
        .layer(GrpcWebLayer::new());

    // Handle shutdown gracefully.
    let coordinator_clone = service.coordinator.clone();
    let shutdown_tx_clone = shutdown_tx.clone();
    ctrlc::set_handler(move || {
        let shutdown_tx = shutdown_tx_clone.clone();
        let coordinator = coordinator_clone.clone();
        let metrics_shutdown_tx = metrics_shutdown_tx.clone();
        tokio::spawn(async move {
            print_latency().await;
            tracing::info!("Received ctrl-c, shutting down... (ctrl-c again to force exit)");

            // Initiate coordinator shutdown
            coordinator.shutdown().await;
            metrics_shutdown_tx.send(()).unwrap();
            tokio::time::sleep(Duration::from_secs(2)).await;

            // Signal the main task to shut down
            if let Some(tx) = shutdown_tx.lock().unwrap().take() {
                let _ = tx.send(());
            }
        });
    })
    .expect("failed to set ctrl-c handler");

    // Extract coordinator before moving service into server
    let coordinator = service.coordinator.clone();

    // Start the server
    let server = Server::builder()
        .tcp_keepalive(Some(Duration::from_secs(20)))
        .http2_keepalive_interval(Some(Duration::from_secs(15)))
        .http2_keepalive_timeout(Some(Duration::from_secs(35)))
        .layer(middleware)
        .add_service(
            sp1_cluster_common::proto::worker_service_server::WorkerServiceServer::new(service),
        )
        .serve(addr);

    // Create the server future
    let server_future = async move {
        // Wait for server to finish or shutdown signal
        tokio::select! {
            result = server => {
                if let Err(e) = result {
                    tracing::error!("Server error: {:?}", e);
                }
            }
            _ = shutdown_rx => {
                tracing::info!("Received shutdown signal");
            }
            _ = async {
                if let Some(ref mut handle) = metrics_server_handle {
                    handle.await.unwrap_or(())
                } else {
                    // If no metrics server, just wait forever (this branch will never be taken in select!)
                    std::future::pending::<()>().await
                }
            } => {
                tracing::error!("Metrics server task exited unexpectedly");
            }
        }

        // Clean up metrics server
        if let Some(handle) = metrics_server_handle.take() {
            handle.abort();
            tracing::info!("Metrics server aborted");
        }

        tracing::info!("Graceful shutdown complete");
    };

    Ok((coordinator, server_future))
}

/// Run the coordinator server to completion (backwards compatibility).
/// For more control, use `start_coordinator_server` instead.
pub async fn run_coordinator_server<P: AssignmentPolicy + Default + Send + Sync + 'static>(
) -> Result<()> {
    let (_, server_future) = start_coordinator_server::<P>().await?;
    server_future.await;
    Ok(())
}
