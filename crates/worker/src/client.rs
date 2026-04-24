use crate::limiter::get_max_weight;
use crate::utils::{current_context, task_metadata};
use eyre::Result;
use mti::prelude::{MagicTypeIdExt, V7};
use rand::seq::IteratorRandom;
use sp1_cluster_artifact::ArtifactId;
use sp1_cluster_common::client::reconnect_with_backoff;
use sp1_cluster_common::consts::task_weight;
use sp1_cluster_common::proto::{
    self, server_sub_message, worker_service_client::WorkerServiceClient as InnerWorkerClient,
    AckSubRequest, CloseRequest, CompleteTaskRequest, FailTaskRequest, HeartbeatRequest,
    OpenRequest, OpenSubRequest, ServerMessage, TaskData, UpdateSubRequest, WorkerType,
};
use sp1_cluster_common::util::{backoff_retry, status_to_backoff_error};
use sp1_prover::worker::{
    ProofId, RawTaskRequest, SubscriberBuilder, TaskContext, TaskId, TaskMetadata, WorkerClient,
};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{mpsc, watch, Mutex};
use tokio_stream::{Stream, StreamExt};
use tonic::transport::Channel;
use tonic::Status;

/// Retry policies. Each call site picks one explicitly — no shared default,
/// so failure semantics can't be inherited silently.
/// `infinite` for calls that must eventually land (heartbeat, reports).
/// `bounded` for calls whose `Err` is a signal the caller acts on.
mod retry {
    use backoff::{ExponentialBackoff, ExponentialBackoffBuilder};
    use std::time::Duration;

    const INITIAL_INTERVAL: Duration = Duration::from_millis(100);

    pub(super) fn infinite() -> ExponentialBackoff {
        ExponentialBackoffBuilder::new()
            .with_initial_interval(INITIAL_INTERVAL)
            .build()
    }

    pub(super) fn bounded(max_elapsed: Duration) -> ExponentialBackoff {
        ExponentialBackoffBuilder::new()
            .with_initial_interval(INITIAL_INTERVAL)
            .with_max_elapsed_time(Some(max_elapsed))
            .build()
    }
}

#[derive(Clone)]
pub struct WorkerServiceClient {
    pub client: InnerWorkerClient<Channel>,
    pub worker_id: String,
    pub worker_type: WorkerType,
}

impl WorkerServiceClient {
    pub async fn new(addr: String, worker_id: String) -> Result<WorkerServiceClient> {
        let channel = reconnect_with_backoff(&addr).await?;

        let client = InnerWorkerClient::new(channel.clone());
        let worker_type_str = std::env::var("WORKER_TYPE").unwrap_or_else(|_| "ALL".to_string());
        let worker_type = WorkerType::from_str_name(&worker_type_str).expect("Invalid worker type");

        Ok(Self {
            client,
            worker_id,
            worker_type,
        })
    }

    pub async fn open(&self) -> Result<mpsc::UnboundedReceiver<ServerMessage>> {
        // Prepare for receiving server messages
        let (server_tx, server_rx) = mpsc::unbounded_channel();

        // Perform the initial "Open" request
        let init_msg = OpenRequest {
            worker_id: self.worker_id.clone(),
            worker_type: self.worker_type as i32,
            max_weight: get_max_weight() as u32,
        };
        let response = backoff_retry(retry::infinite(), || {
            let mut client = self.client.clone();
            let init_msg = init_msg.clone();
            async move { client.open(init_msg).await }
        })
        .await?;
        let mut inbound = response.into_inner();

        // Spawn a receive task to pump server messages into `server_tx`
        tokio::spawn(async move {
            while let Some(server_msg) = inbound.next().await {
                match server_msg {
                    Ok(msg) => {
                        if msg.message.is_none() {
                            tracing::error!("Server closed connection");
                            break;
                        }
                        if let Err(e) = server_tx.send(msg) {
                            tracing::error!("Error sending server message: {}", e);
                            break;
                        }
                    }
                    Err(e) => {
                        tracing::error!("Server stream error: {}", e);
                        break;
                    }
                }
            }
        });

        Ok(server_rx)
    }

    pub async fn close(&self, request: CloseRequest) -> anyhow::Result<()> {
        backoff::future::retry(retry::infinite(), || async {
            self.client
                .clone()
                .close(request.clone())
                .await
                .map_err(status_to_backoff_error)
        })
        .await?;
        Ok(())
    }

    pub async fn heartbeat(&self, request: HeartbeatRequest) -> Result<(), Status> {
        backoff::future::retry(retry::infinite(), || async {
            self.client
                .clone()
                .heartbeat(request.clone())
                .await
                .map_err(status_to_backoff_error)
        })
        .await?;
        Ok(())
    }

    pub async fn complete_task(&self, request: CompleteTaskRequest) -> anyhow::Result<()> {
        backoff::future::retry(retry::infinite(), || async {
            self.client
                .clone()
                .complete_task(request.clone())
                .await
                .map_err(status_to_backoff_error)
        })
        .await?;
        Ok(())
    }

    pub async fn fail_task(&self, request: FailTaskRequest) -> anyhow::Result<()> {
        backoff::future::retry(retry::infinite(), || async {
            self.client
                .clone()
                .fail_task(request.clone())
                .await
                .map_err(status_to_backoff_error)
        })
        .await?;
        Ok(())
    }
}

impl WorkerClient for WorkerServiceClient {
    async fn submit_task(
        &self,
        task_type: proto::TaskType,
        task: RawTaskRequest,
    ) -> anyhow::Result<TaskId> {
        let RawTaskRequest {
            inputs: input_artifacts,
            outputs: output_artifacts,
            context: task_context,
        } = task;
        let TaskContext {
            proof_id,
            parent_id,
            parent_context,
            requester_id,
        } = task_context;
        let context = parent_context.unwrap_or_else(current_context);
        let metadata = serde_json::to_string(&task_metadata(&context))?;

        let request = proto::CreateTaskRequest {
            worker_id: self.worker_id.clone(),
            data: Some(TaskData {
                task_type: task_type as i32,
                inputs: input_artifacts.iter().map(|a| a.id().to_string()).collect(),
                outputs: output_artifacts
                    .iter()
                    .map(|a| a.id().to_string())
                    .collect(),
                metadata,
                proof_id: proof_id.to_string(),
                parent_id: parent_id.map(|p| p.to_string()),
                weight: task_weight(task_type) as u32,
                requester: requester_id.to_string(),
            }),
        };

        let response = backoff::future::retry(retry::infinite(), || async {
            self.client
                .clone()
                .create_task(request.clone())
                .await
                .map_err(status_to_backoff_error)
        })
        .await?;

        let result = response.into_inner().task_id;
        Ok(TaskId::new(result))
    }

    async fn complete_task(
        &self,
        proof_id: ProofId,
        task_id: TaskId,
        metadata: TaskMetadata,
    ) -> anyhow::Result<()> {
        let metadata_string = serde_json::to_string(&metadata).unwrap();
        let request = CompleteTaskRequest {
            worker_id: self.worker_id.clone(),
            proof_id: proof_id.to_string(),
            task_id: task_id.to_string(),
            metadata: metadata_string,
        };

        // We can simply delegate to the method above:
        self.complete_task(request).await
    }

    // I think this turned into "submit tasks"
    // async fn create_tasks(
    //     &self,
    //     task_type: proto::TaskType,
    //     input_artifact_ids: &[Vec<&impl ArtifactId>],
    //     output_artifact_ids: &[Vec<&impl ArtifactId>],
    //     proof_id: String,
    //     parent_id: Option<String>,
    //     parent_context: Option<Context>,
    //     requester: String,
    // ) -> anyhow::Result<(String, Vec<String>)> {
    //     let mut promises = Vec::new();
    //     for (inputs, outputs) in input_artifact_ids.iter().zip(output_artifact_ids.iter()) {
    //         promises.push(self.create_task(
    //             task_type,
    //             inputs,
    //             outputs,
    //             proof_id.clone(),
    //             parent_id.clone(),
    //             parent_context.clone(),
    //             requester.clone(),
    //         ))
    //     }
    //     let res = futures::future::try_join_all(promises).await?;
    //     Ok(("".to_string(), res))
    // }

    // async fn get_task_statuses(
    //     &self,
    //     proof_id: String,
    //     task_ids: &[String],
    // ) -> anyhow::Result<HashMap<proto::TaskStatus, Vec<String>>> {
    //     let request = proto::GetTaskStatusesRequest {
    //         worker_id: self.worker_id.clone(),
    //         proof_id,
    //         task_ids: task_ids.to_vec(),
    //     };

    //     let backoff = self.backoff.clone();
    //     let response = backoff::future::retry(backoff, || async {
    //         self.client
    //             .clone()
    //             .get_task_statuses(request.clone())
    //             .await
    //             .map_err(status_to_backoff_error)
    //     })
    //     .await?;

    //     let mut result = HashMap::new();
    //     for entry in response.into_inner().statuses {
    //         let task_status = proto::TaskStatus::try_from(entry.key).unwrap();
    //         result.insert(task_status, entry.value.map_or_else(Vec::new, |v| v.ids));
    //     }
    //     Ok(result)
    // }

    async fn subscriber(
        &self,
        proof_id: ProofId,
    ) -> anyhow::Result<SubscriberBuilder<WorkerServiceClient>> {
        let (sub_tx, mut sub_rx) = mpsc::unbounded_channel::<TaskId>();
        let (res_tx, res_rx) = mpsc::unbounded_channel::<(TaskId, proto::TaskStatus)>();

        let sub_id = "sub".create_type_id::<V7>().to_string();
        let tasks_set = Arc::new(Mutex::new(HashSet::new()));

        let (closed_tx, closed_rx) = watch::channel(false);

        // Bounded: Err here lets the caller fall back to polling.
        // The reconnect loop spawned below stays infinite.
        let connection = self.client.clone();
        let request = OpenSubRequest {
            sub_id: sub_id.clone(),
            proof_id: proof_id.to_string(),
            task_ids: Vec::new(),
        };
        let response =
            backoff::future::retry(retry::bounded(Duration::from_secs(10)), || async {
                connection
                    .clone()
                    .open_sub(request.clone())
                    .await
                    .map_err(status_to_backoff_error)
            })
            .await?;

        let mut inbound = response.into_inner();
        tokio::spawn({
            let tasks_set = tasks_set.clone();
            let sub_id = sub_id.clone();
            let mut closed_rx = closed_rx.clone();
            let connection = connection.clone();
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            let mut last_heartbeat = SystemTime::now();

            async move {
                loop {
                    if (*closed_rx.borrow()) && tasks_set.lock().await.is_empty() {
                        break;
                    }
                    tokio::select! {
                        Some(msg) = inbound.next() => {
                            match msg {
                                Ok(msg) => {
                                    // ack each message
                                    let ack_request = AckSubRequest {
                                        sub_id: sub_id.clone(),
                                        msg_id: msg.msg_id.clone(),
                                    };
                                    let _ = backoff::future::retry(retry::infinite(), || async {
                                        connection.clone().ack_sub(ack_request.clone())
                                            .await
                                            .map_err(status_to_backoff_error)
                                    }).await.map_err(|e| {
                                        tracing::error!("Failed to ack sub for {}: {:?}", &msg.msg_id, e);
                                    });

                                    match msg.message {
                                        Some(server_sub_message::Message::TaskResult(result)) => {
                                            let mut lock = tasks_set.lock().await;
                                            lock.remove(&result.task_id);
                                            drop(lock);
                                            let _ = res_tx.send((
                                                TaskId::new(result.task_id),
                                                proto::TaskStatus::try_from(result.task_status).unwrap(),
                                            ));
                                        }
                                        Some(server_sub_message::Message::EndOfStream(_)) => {
                                            break;
                                        }
                                        Some(server_sub_message::Message::UnknownTask(unknown_task)) => {
                                            // Report the task as fatally failed and keep serving the subscriber's
                                            // other tasks.
                                            tracing::error!(
                                                "Received UnknownTask from coordinator: task={} proof={}",
                                                unknown_task.task_id,
                                                unknown_task.proof_id,
                                            );
                                            let mut lock = tasks_set.lock().await;
                                            lock.remove(&unknown_task.task_id);
                                            drop(lock);
                                            let _ = res_tx.send((
                                                TaskId::new(unknown_task.task_id),
                                                proto::TaskStatus::FailedFatal,
                                            ));
                                        }
                                        Some(server_sub_message::Message::ServerHeartbeat(_)) => {
                                            // Send empty UpdateSub message to keep the subscriber alive.
                                            last_heartbeat = SystemTime::now();
                                            // Use a random subset of up to 30 task_ids to ensure
                                            // these tasks are still being worked on (not disappeared
                                            // or failed) without overloading the server.
                                            let task_ids = {
                                                let lock = tasks_set.lock().await;
                                                let mut rng = rand::rng();
                                                lock.iter().cloned().choose_multiple(&mut rng, 30)
                                            };
                                            if let Err(e) = backoff::future::retry(retry::infinite(), || {
                                                let request = UpdateSubRequest {
                                                    sub_id: sub_id.clone(),
                                                    task_ids: task_ids.clone(),
                                                };
                                                let mut connection = connection.clone();
                                                async move {
                                                    connection
                                                        .update_sub(request.clone())
                                                        .await
                                                        .map_err(status_to_backoff_error)
                                                }
                                            })
                                            .await
                                            {
                                                tracing::error!("Failed to send UpdateSub to subscriber: {}", e);
                                            }
                                        }
                                        None => {
                                            tracing::warn!("Received empty message");
                                            break;
                                        }
                                    }
                                }
                                Err(e) => {
                                    tracing::error!("Connection error: {:?}", e);
                                    match backoff::future::retry(retry::infinite(), || async {
                                        connection
                                            .clone()
                                            .open_sub(request.clone())
                                            .await
                                            .map_err(status_to_backoff_error)
                                    })
                                    .await {
                                        Ok(response) => {
                                            inbound = response.into_inner();
                                            last_heartbeat = SystemTime::now();
                                        }
                                        Err(e) => {
                                            tracing::error!("Failed to reconnect: {:?}", e);
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        Ok(_) = closed_rx.changed() => {
                            if tasks_set.lock().await.is_empty() {
                                tracing::debug!("Sub {} closed, shutting down subscription reader", sub_id);
                                break;
                            }
                            tracing::debug!("Sub {} closed but tasks remain, continuing...", sub_id);
                        }
                        _ = interval.tick() => {
                            if last_heartbeat.elapsed().unwrap_or_default() > Duration::from_secs(10) {
                                tracing::warn!("No heartbeats received from subscriber {}, reconnecting", sub_id);
                                match backoff::future::retry(retry::infinite(), || async {
                                    connection
                                        .clone()
                                        .open_sub(request.clone())
                                        .await
                                        .map_err(status_to_backoff_error)
                                })
                                .await {
                                    Ok(response) => {
                                        inbound = response.into_inner();
                                        last_heartbeat = SystemTime::now();
                                    }
                                    Err(e) => {
                                        tracing::error!("Failed to reconnect: {:?}", e);
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });

        tokio::spawn({
            let mut channel = self.client.clone();
            let tasks_set = tasks_set.clone();
            let sub_id = sub_id.clone();
            async move {
                while let Some(task_id) = sub_rx.recv().await {
                    let mut lock = tasks_set.lock().await;
                    lock.insert(task_id.to_string());
                    drop(lock);

                    let update_request = UpdateSubRequest {
                        sub_id: sub_id.clone(),
                        task_ids: vec![task_id.to_string()],
                    };
                    if let Err(e) = channel.update_sub(update_request).await {
                        tracing::error!("Error updating subscription: {}", e);
                    }
                }
                tracing::debug!("create_subscriber input closed, marking sub closed");
                let _ = closed_tx.send(true);
            }
        });

        Ok(SubscriberBuilder::new(self.clone(), sub_tx, res_rx))
    }

    async fn subscribe_task_messages(
        &self,
        task_id: &TaskId,
    ) -> anyhow::Result<mpsc::UnboundedReceiver<Vec<u8>>> {
        let request = proto::SubscribeTaskMessagesRequest {
            worker_id: self.worker_id.clone(),
            task_id: task_id.to_string(),
            start_offset: 0,
        };
        let response = backoff::future::retry(retry::infinite(), || async {
            self.client
                .clone()
                .subscribe_task_messages(request.clone())
                .await
                .map_err(status_to_backoff_error)
        })
        .await?;
        let inbound = response.into_inner();

        let (tx, rx) = mpsc::unbounded_channel();
        let client = self.client.clone();
        tokio::spawn(async move {
            drive_message_stream(inbound, tx, move |forwarded| {
                let client = client.clone();
                let mut request = request.clone();
                request.start_offset = forwarded as u64;
                async move {
                    match backoff::future::retry(retry::infinite(), || async {
                        client
                            .clone()
                            .subscribe_task_messages(request.clone())
                            .await
                            .map_err(status_to_backoff_error)
                    })
                    .await
                    {
                        Ok(response) => Some(response.into_inner()),
                        Err(e) => {
                            tracing::error!("Failed to reconnect task message stream: {:?}", e);
                            None
                        }
                    }
                }
            })
            .await;
        });

        Ok(rx)
    }

    async fn send_task_message(&self, task_id: &TaskId, payload: Vec<u8>) -> anyhow::Result<()> {
        let request = proto::SendTaskMessageRequest {
            worker_id: self.worker_id.clone(),
            task_id: task_id.to_string(),
            payload,
        };
        backoff::future::retry(retry::infinite(), || async {
            self.client
                .clone()
                .send_task_message(request.clone())
                .await
                .map_err(status_to_backoff_error)
        })
        .await?;
        Ok(())
    }

    async fn complete_proof(
        &self,
        proof_id: ProofId,
        task_id: Option<TaskId>,
        status: proto::ProofRequestStatus,
        extra_data: impl Into<String> + Send,
    ) -> anyhow::Result<()> {
        let extra_data: String = extra_data.into();
        let extra_data = if extra_data.is_empty() {
            None
        } else {
            Some(extra_data)
        };
        match status {
            proto::ProofRequestStatus::Completed => {
                let request = proto::CompleteProofRequest {
                    worker_id: self.worker_id.clone(),
                    proof_id: proof_id.to_string(),
                    extra_data,
                };
                backoff::future::retry(retry::infinite(), || async {
                    self.client
                        .clone()
                        .complete_proof(request.clone())
                        .await
                        .map_err(status_to_backoff_error)
                })
                .await?;
            }
            _ => {
                let request = proto::FailProofRequest {
                    worker_id: self.worker_id.clone(),
                    proof_id: proof_id.to_string(),
                    task_id: task_id.map(|t| t.to_string()),
                    extra_data,
                };
                backoff::future::retry(retry::infinite(), || async {
                    self.client
                        .clone()
                        .fail_proof(request.clone())
                        .await
                        .map_err(status_to_backoff_error)
                })
                .await?;
            }
        }
        Ok(())
    }
}

async fn drive_message_stream<S, F, Fut>(
    mut inbound: S,
    tx: mpsc::UnboundedSender<Vec<u8>>,
    reconnect: F,
) where
    S: Stream<Item = Result<proto::MessageStreamResponse, tonic::Status>> + Unpin,
    F: Fn(usize) -> Fut,
    Fut: std::future::Future<Output = Option<S>>,
{
    let mut forwarded: usize = 0;
    'outer: loop {
        while let Some(msg) = inbound.next().await {
            match msg {
                Ok(resp) => match resp.message {
                    Some(proto::message_stream_response::Message::Payload(data)) => {
                        forwarded += 1;
                        if tx.send(data).is_err() {
                            break 'outer;
                        }
                    }
                    Some(proto::message_stream_response::Message::EndOfStream(_)) | None => {
                        break 'outer;
                    }
                },
                Err(e) => {
                    tracing::warn!("Task message stream error, reconnecting: {:?}", e);
                    if let Some(new_stream) = reconnect(forwarded).await {
                        inbound = new_stream;
                        continue 'outer;
                    }
                    break 'outer;
                }
            }
        }
        tracing::warn!("stream ended without EndOfStream, reconnecting");
        if let Some(new_stream) = reconnect(forwarded).await {
            inbound = new_stream;
            continue 'outer;
        }
        break;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sp1_cluster_common::proto::{self, MessageStreamResponse};

    #[tokio::test]
    async fn reconnects_on_clean_stream_close() {
        use proto::message_stream_response::Message::{EndOfStream, Payload};
        use std::sync::atomic::{AtomicUsize, Ordering};

        let stream1 = tokio_stream::iter(vec![
            Ok(MessageStreamResponse {
                message: Some(Payload(vec![1])),
            }),
            Ok(MessageStreamResponse {
                message: Some(Payload(vec![2])),
            }),
        ]);

        let reconnect_forwarded = Arc::new(AtomicUsize::new(0));
        let reconnect_forwarded_clone = reconnect_forwarded.clone();

        let (tx, mut rx) = mpsc::unbounded_channel();
        drive_message_stream(stream1, tx, move |forwarded| {
            reconnect_forwarded_clone.store(forwarded, Ordering::SeqCst);
            async move {
                Some(tokio_stream::iter(vec![
                    Ok(MessageStreamResponse {
                        message: Some(Payload(vec![3])),
                    }),
                    Ok(MessageStreamResponse {
                        message: Some(EndOfStream(proto::EndOfStream {})),
                    }),
                ]))
            }
        })
        .await;

        assert_eq!(reconnect_forwarded.load(Ordering::SeqCst), 2);
        assert_eq!(rx.recv().await, Some(vec![1]));
        assert_eq!(rx.recv().await, Some(vec![2]));
        assert_eq!(rx.recv().await, Some(vec![3]));
        assert!(rx.recv().await.is_none());
    }
}
