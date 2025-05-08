use cfg_if::cfg_if;
use opentelemetry::{global, Context};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::future::Future;
use std::sync::atomic::AtomicU32;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::OwnedSemaphorePermit;
use tracing::field::AsField;
use tracing::{info_span, Span, Value};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::metrics::WorkerMetrics;

pub fn chunk_vec<T>(mut vec: Vec<T>, chunk_size: usize) -> Vec<Vec<T>> {
    let mut result = Vec::new();
    while !vec.is_empty() {
        let current_chunk_size = std::cmp::min(chunk_size, vec.len());
        let current_chunk = vec.drain(..current_chunk_size).collect::<Vec<T>>();
        result.push(current_chunk);
    }
    result
}

pub fn create_http_client() -> Client {
    Client::builder()
        .pool_max_idle_per_host(0)
        .pool_idle_timeout(Duration::from_secs(240))
        .build()
        .unwrap()
}

/// https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-metadata-endpoint-v4-response.html
#[derive(Debug, Serialize, Deserialize)]
pub struct ECSTaskInfo {
    #[serde(rename = "Cluster")]
    pub cluster: String,
    #[serde(rename = "TaskARN")]
    pub task_arn: String,
}

pub async fn get_ecs_task_info(client: &Client) -> anyhow::Result<ECSTaskInfo> {
    let metadata_url = env::var("ECS_CONTAINER_METADATA_URI_V4")?;
    let response = client.get(metadata_url + "/task").send().await?;
    response.json().await.map_err(|e| e.into())
}

pub fn task_metadata(context: &Context) -> HashMap<String, String> {
    let mut metadata = HashMap::new();
    global::get_text_map_propagator(|p| p.inject_context(context, &mut metadata));
    metadata
}

pub fn current_context() -> Context {
    Span::current().context()
}

pub fn current_task_metadata() -> HashMap<String, String> {
    let context = Span::current().context();
    task_metadata(&context)
}

/// Record a field in the current span.
pub fn record_current<Q, V>(field: &Q, value: V)
where
    Q: AsField + ?Sized,
    V: Value,
{
    let span = Span::current();
    span.record(field, value);
}

pub fn with_parent(span: Span, parent: Context) -> Span {
    span.set_parent(parent);
    span
}

pub fn print_accelerator_info() {
    println!(
        "SP1_PROFILE: {}",
        option_env!("SP1_PROFILE").unwrap_or("unknown")
    );
    cfg_if! {
        if #[cfg(feature = "gpu")] {
            println!("GPU enabled");
        } else {
            println!("GPU disabled");
        }
    }
    cfg_if! {
        if #[cfg(all(target_arch = "aarch64", target_feature = "neon"))] {
            println!("NEON enabled");
        } else if #[cfg(all(
            target_arch = "x86_64",
            target_feature = "avx512f"
        ))] {
            println!("AVX512F enabled");
        } else if #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))] {
            println!("AVX2 enabled");
        } else {
            println!("CPU acceleration disabled");
        }
    }
}

/// A no-op type that implements Drop to hide clippy "let unit binding" / "drop non-drop" lints.
pub struct Empty;

impl Drop for Empty {
    fn drop(&mut self) {}
}

// This is a macro because we don't want a separate await point for each acquire_gpu call
#[macro_export]
macro_rules! acquire_gpu {
    ($worker:expr, $time:expr) => {{
        #[cfg(feature = "gpu")]
        {
            use tracing::Instrument;
            use $crate::utils::GpuLock;
            let permit = $worker
                .gpu_semaphore
                .clone()
                .acquire_owned()
                .instrument(tracing::info_span!("acquire_gpu"))
                .await
                .unwrap();
            moongate_core::sync_device().unwrap();
            let metrics = $worker.metrics.clone();
            let free_memory = moongate_core::device::memory::cuda_mem_get_info()
                .unwrap()
                .0;

            log::debug!("free memory: {}", free_memory);

            GpuLock::new(permit, metrics, $time.clone())
        }

        #[cfg(not(feature = "gpu"))]
        {
            let _ = &$time;
            $crate::utils::Empty
        }
    }};
}

/// Get the size of a tree layer given the total leaf count. Every layer will have an even number of
/// nodes unless there is only one node. Odd nodes will be effectively deferred to the next odd layer.
pub fn get_tree_layer_size(leaf_count: u32, layer: usize) -> usize {
    let mut width = leaf_count;
    // There's an extra node if the leaf count is odd and there's more than one node.
    let mut has_extra = leaf_count > 1 && leaf_count % 2 == 1;
    if has_extra {
        width -= 1;
    }
    for _ in 0..layer {
        width /= 2;
        if width % 2 == 1 {
            if has_extra {
                width += 1;
                has_extra = false;
            } else if width > 1 {
                width -= 1;
                has_extra = true;
            }
        }
    }
    width as usize
}

/// Useful in tokio::select! macro to wait on an Option<Future> if it's Some.
pub async fn conditional_future<T>(future: Option<impl Future<Output = T>>) -> Option<T> {
    match future {
        Some(fut) => Some(fut.await),
        None => None,
    }
}

/// Wrapper around SemaphorePermit for additional drop logic.
pub struct GpuLock(
    Option<OwnedSemaphorePermit>,
    Instant,
    Option<Arc<WorkerMetrics>>,
    Arc<AtomicU32>,
);

impl GpuLock {
    pub fn new(
        permit: OwnedSemaphorePermit,
        metrics: Option<Arc<WorkerMetrics>>,
        time: Arc<AtomicU32>,
    ) -> Self {
        Self(Some(permit), Instant::now(), metrics, time)
    }
}

impl Drop for GpuLock {
    fn drop(&mut self) {
        #[cfg(feature = "gpu")]
        tracing::debug_span!("sync_device").in_scope(|| moongate_core::sync_device().unwrap());
        let elapsed = self.1.elapsed().as_millis() as u64;
        drop(self.0.take());
        self.2.as_ref().map(|m| m.gpu_busy_time.increment(elapsed));
        self.3.fetch_add(
            elapsed.try_into().unwrap_or(u32::MAX),
            std::sync::atomic::Ordering::Relaxed,
        );
    }
}

lazy_static::lazy_static! {
    static ref GLOBAL_GPU_SPAN: Mutex<(Span, Instant)> =
        Mutex::new((info_span!(parent: None, "gpu"), Instant::now()));
}

/// Returns a global span for the GPU. A new one is created if the previous one is too old.
pub fn get_global_gpu_span() -> Span {
    let mut lock = GLOBAL_GPU_SPAN.lock().unwrap();
    let mut span = lock.0.clone();
    let created_at = lock.1;
    if created_at.elapsed() > Duration::from_secs(120) {
        span = info_span!(parent: None, "gpu");
        *lock = (span.clone(), Instant::now());
    }
    span
}

/// Simple wrapper around a value and a function to call on it when the wrapper is dropped.
pub struct DeferGuard<T, F: FnOnce(T)> {
    value: Option<T>,
    f: Option<F>,
}

impl<T, F: FnOnce(T)> Drop for DeferGuard<T, F> {
    fn drop(&mut self) {
        (self.f.take().unwrap())(self.value.take().unwrap());
    }
}

impl<T, F: FnOnce(T)> DeferGuard<T, F> {
    pub fn new(value: T, f: F) -> Self {
        Self {
            value: Some(value),
            f: Some(f),
        }
    }
}
