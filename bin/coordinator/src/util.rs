use eyre::Result;
use sp1_cluster_common::auth::{AuthConfig, BearerAuthLayer};
use std::{
    collections::HashMap,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tokio::task::JoinHandle;
use tonic::body::Body;
use tonic_web::GrpcWebLayer;
use tower::layer::util::{Identity, Stack};
use tower::{Layer, Service, ServiceBuilder};
use tracing::Instrument;

use crate::{
    estimate_duration, latency::print_latency, AssignmentPolicy, Coordinator,
    COORDINATOR_PERIODIC_INTERVAL,
};
use sp1_cluster_common::proto::{TaskType, WorkerType};

pub type Middleware =
    ServiceBuilder<Stack<BearerAuthLayer, Stack<GrpcWebLayer, Stack<OkLayer, Identity>>>>;

/// OkService is the outer layer, so health probes do not need a token.
pub fn middleware(auth: AuthConfig) -> Middleware {
    ServiceBuilder::new()
        .layer(OkLayer)
        .layer(GrpcWebLayer::new())
        .layer(BearerAuthLayer::new(auth))
}

#[derive(Clone)]
pub struct OkLayer;

impl<S> Layer<S> for OkLayer {
    type Service = OkService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        OkService { inner }
    }
}

#[derive(Clone)]
pub struct OkService<S> {
    inner: S,
}

impl<S, B> Service<http::Request<B>> for OkService<S>
where
    S: Service<http::Request<B>, Response = http::Response<Body>> + Send + 'static,
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
            "/" => Box::pin(async move { Ok(http::Response::new(Body::new("OK".to_string()))) }),
            "/healthz" => Box::pin(async move { Ok(http::Response::new(Body::empty())) }),
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
    token: tokio_util::sync::CancellationToken,
) -> JoinHandle<()> {
    tokio::task::spawn({
        async move {
            loop {
                coordinator.send_heartbeats().await;
                tokio::select! {
                    _ = tokio::time::sleep(crate::SERVER_HEARTBEAT_INTERVAL) => {}
                    _ = token.cancelled() => break,
                }
            }
        }
    })
}

pub fn spawn_coordinator_periodic_task<P: AssignmentPolicy>(
    coordinator: Arc<Coordinator<P>>,
    token: tokio_util::sync::CancellationToken,
) -> JoinHandle<()> {
    tokio::task::spawn({
        async move {
            loop {
                coordinator.accumulate_gpu_available_ms().await;
                coordinator.cleanup_dead_workers().await;
                coordinator.cleanup_dead_subscribers().await;
                coordinator.cleanup_stale_task_channels();
                coordinator.cleanup_cancel_expired_proofs().await;
                coordinator.print_info().await;
                print_latency().await;
                tokio::select! {
                    _ = tokio::time::sleep(COORDINATOR_PERIODIC_INTERVAL) => {}
                    _ = token.cancelled() => break,
                }
            }
        }
    })
}

pub fn estimate_gpu_duration(task_counts: &HashMap<TaskType, i32>) -> Duration {
    let mut total_duration = Duration::from_millis(0);

    for (task_type, count) in task_counts {
        if worker_type_from_task_type(*task_type) != WorkerType::Gpu {
            continue;
        }
        let duration_ms = estimate_duration(*task_type) as u64;
        total_duration += Duration::from_millis(duration_ms) * (*count as u32);
    }

    total_duration
}

pub(crate) fn worker_type_from_task_type(task_type: TaskType) -> WorkerType {
    match WorkerType::from_task_type(task_type) {
        WorkerType::Gpu if option_env!("SP1_CLUSTER_CPU_ONLY").is_some() => WorkerType::Cpu,
        worker_type => worker_type,
    }
}

#[cfg(test)]
mod tests {
    use http::{Method, Request, Response, StatusCode, Version};
    use tower::{service_fn, ServiceExt};

    use super::*;

    #[tokio::test]
    async fn health_probes_skip_auth_but_rpcs_do_not() {
        let auth = AuthConfig {
            token: Some("s3cret".into()),
            required: true,
        };
        let service = middleware(auth).service(service_fn(|_: Request<Body>| async {
            Ok::<_, std::convert::Infallible>(Response::new(Body::empty()))
        }));

        let healthz = Request::builder()
            .method(Method::GET)
            .uri("/healthz")
            .body(Body::empty())
            .unwrap();
        let response = service.clone().oneshot(healthz).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().get("grpc-status").is_none());

        let rpc = Request::builder()
            .method(Method::POST)
            .uri("/worker.WorkerService/GetStats")
            .version(Version::HTTP_2)
            .header("content-type", "application/grpc")
            .body(Body::empty())
            .unwrap();
        let response = service.oneshot(rpc).await.unwrap();
        assert_eq!(response.headers()["grpc-status"], "16");
    }
}
