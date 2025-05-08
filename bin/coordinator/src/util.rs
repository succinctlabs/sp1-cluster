use eyre::Result;
use std::{
    collections::HashMap,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tokio::task::JoinHandle;
use tonic::body::{boxed, empty_body, BoxBody};
use tower::Service;
use tracing::Instrument;

use crate::{
    estimate_duration, latency::print_latency, AssignmentPolicy, Coordinator,
    COORDINATOR_PERIODIC_INTERVAL,
};
use sp1_cluster_common::proto::{TaskType, WorkerType};

#[derive(Clone)]
pub struct OkService<S> {
    pub inner: S,
}

impl<S, B> Service<http::Request<B>> for OkService<S>
where
    S: Service<http::Request<B>, Response = http::Response<BoxBody>> + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future =
        Pin<Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<B>) -> Self::Future {
        let method = req.method().to_string();
        let path = req.uri().path().to_string();
        tracing::debug!("{} {}", method, path);
        match req.uri().path() {
            "/" => Box::pin(async move { Ok(http::Response::new(boxed("OK".to_string()))) }),
            "/healthz" => Box::pin(async move { Ok(http::Response::new(empty_body())) }),
            _ => Box::pin(self.inner.call(req).instrument(tracing::debug_span!(
                "call",
                method = ?method,
                path = path,
            ))),
        }
    }
}

pub fn spawn_heartbeat_task<P: AssignmentPolicy>(
    coordinator: Arc<Coordinator<P>>,
) -> JoinHandle<()> {
    tokio::task::spawn({
        async move {
            loop {
                coordinator.send_heartbeats().await;
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
        }
    })
}

pub fn spawn_coordinator_periodic_task<P: AssignmentPolicy>(
    coordinator: Arc<Coordinator<P>>,
) -> JoinHandle<()> {
    tokio::task::spawn({
        async move {
            loop {
                coordinator.cleanup_dead_workers().await;
                coordinator.cleanup_dead_subscribers().await;
                coordinator.cleanup_cancel_expired_proofs().await;
                coordinator.print_info().await;
                print_latency().await;
                tokio::time::sleep(COORDINATOR_PERIODIC_INTERVAL).await;
            }
        }
    })
}

pub fn estimate_gpu_duration(task_counts: &HashMap<TaskType, i32>) -> Duration {
    let mut total_duration = Duration::from_millis(0);

    for (task_type, count) in task_counts {
        if WorkerType::from_task_type(*task_type) != WorkerType::Gpu {
            continue;
        }
        let duration_ms = estimate_duration(*task_type) as u64;
        total_duration += Duration::from_millis(duration_ms) * (*count as u32);
    }

    total_duration
}
