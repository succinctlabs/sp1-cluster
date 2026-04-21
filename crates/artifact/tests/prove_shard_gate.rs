//! Integration tests for [`ProveShardGate`] backed by a real Redis.
//!
//! These tests are gated behind `#[ignore]` so they only run when explicitly
//! invoked (e.g. from CI) with a Redis instance reachable at `REDIS_URL`
//! (default `redis://127.0.0.1:6379/`). Run with:
//!
//! ```sh
//! cargo test --release -p sp1-cluster-artifact --test prove_shard_gate -- \
//!     --ignored --test-threads=1
//! ```
//!
//! CI launches a `redis:7-alpine` service with `--maxmemory 200mb`, sets
//! `PROVE_SHARD_MAX_BYTES=50000000` so the per-node pool floors to 4 permits
//! (`MIN_PERMITS_PER_NODE`).

use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::Result;
use rand::RngCore;
use sp1_cluster_artifact::redis::RedisArtifactClient;
use sp1_prover::worker::{
    ProofId, ProveShardGate, RawTaskRequest, RequesterId, SubscriberBuilder, TaskContext, TaskId,
    TaskMetadata, WorkerClient,
};
use sp1_prover_types::{
    Artifact, ArtifactClient, ArtifactType, ProofRequestStatus, TaskStatus, TaskType,
};
use tokio::sync::{mpsc, watch, Mutex};

const PAYLOAD_BYTES: usize = 10_000_000;

fn redis_url() -> String {
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".into())
}

async fn reset_redis(url: &str) -> Result<()> {
    let client = redis::Client::open(url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;
    let _: () = redis::cmd("FLUSHALL").query_async(&mut conn).await?;
    Ok(())
}

fn random_bytes(n: usize) -> Vec<u8> {
    let mut buf = vec![0u8; n];
    rand::rngs::ThreadRng::default().fill_bytes(&mut buf);
    buf
}

fn dummy_context(proof_id: &str) -> TaskContext {
    TaskContext {
        proof_id: ProofId::new(proof_id),
        parent_id: None,
        parent_context: None,
        requester_id: RequesterId::new("test-requester"),
    }
}

/// Flush Redis and build (artifact_client, worker_client, gate) for `proof_id`.
async fn setup(
    proof_id: &str,
    completion_delay: Duration,
) -> (RedisArtifactClient, MockWorkerClient, ProveShardGate<RedisArtifactClient, MockWorkerClient>) {
    let url = redis_url();
    reset_redis(&url).await.expect("redis reset");
    let artifact_client = RedisArtifactClient::new(vec![url], 10);
    let worker_client = MockWorkerClient::new(completion_delay);
    let gate = ProveShardGate::new(
        artifact_client.clone(),
        worker_client.clone(),
        ProofId::new(proof_id),
    )
    .await
    .expect("gate construction");
    (artifact_client, worker_client, gate)
}

/// Submit a ProveShard task with a single input artifact and the proof's dummy context.
async fn submit_shard(
    worker: &MockWorkerClient,
    proof_id: &str,
    input: Artifact,
) -> TaskId {
    worker
        .submit_task(
            TaskType::ProveShard,
            RawTaskRequest {
                inputs: vec![input],
                outputs: vec![],
                context: dummy_context(proof_id),
            },
        )
        .await
        .expect("submit_task")
}

/// Assert that `fut` completes within `timeout`, panicking with `msg` otherwise.
async fn assert_completes<F: std::future::Future>(timeout: Duration, msg: &str, fut: F) -> F::Output {
    tokio::time::timeout(timeout, fut)
        .await
        .unwrap_or_else(|_| panic!("{msg}"))
}

/// Scenario 1 — single producer full lifecycle.
///
/// Acquire → upload → submit → consumer auto-completes → permit released.
#[tokio::test]
#[ignore = "requires Redis (set REDIS_URL or run with CI services block)"]
async fn scenario_1_lifecycle() {
    let proof_id = "test-proof-1";
    let (artifact_client, worker_client, gate) =
        setup(proof_id, Duration::from_millis(300)).await;

    let record: Artifact = "s1-record-0".to_string().into();
    let permit = gate.acquire(&record).await;
    artifact_client
        .upload_raw(&record, ArtifactType::UnspecifiedArtifactType, random_bytes(PAYLOAD_BYTES))
        .await
        .ok();
    let task_id = submit_shard(&worker_client, proof_id, record).await;
    gate.schedule_release(task_id, permit);

    // Fill the pool with 3 holds so the final probe acquire can only succeed
    // if `schedule_release` actually returned the original permit to the pool.
    let mut filler = Vec::new();
    for i in 0..3 {
        let a: Artifact = format!("s1-filler-{i}").into();
        filler.push(gate.acquire(&a).await);
    }

    // Consumer auto-completes after 300ms; give schedule_release time to fire.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let probe: Artifact = "s1-probe".to_string().into();
    assert_completes(
        Duration::from_millis(500),
        "permit not released after task completion",
        gate.acquire(&probe),
    )
    .await;
}

/// Scenario 2 — gate drop aborts pending release tasks, permits reclaim.
///
/// All 4 permits are handed to `schedule_release` tasks whose mock consumer
/// never completes (30s delay). Dropping the gate must abort those tasks so
/// the permits drop, otherwise the subscriber leak would hang them forever.
#[tokio::test]
#[ignore = "requires Redis (set REDIS_URL or run with CI services block)"]
async fn scenario_2_gate_drop_reclaims_permits() {
    let proof_id = "test-proof-2";
    let (artifact_client, worker_client, gate) =
        setup(proof_id, Duration::from_secs(30)).await;

    for i in 0..4 {
        let record: Artifact = format!("s2-record-{i}").into();
        let permit = gate.acquire(&record).await;
        let task_id = submit_shard(&worker_client, proof_id, record).await;
        gate.schedule_release(task_id, permit);
    }

    drop(gate);
    tokio::time::sleep(Duration::from_millis(200)).await;

    let probe: Artifact = "s2-probe".to_string().into();
    assert_completes(
        Duration::from_millis(500),
        "permits not reclaimed after gate drop",
        artifact_client.acquire_shard_permit(&probe),
    )
    .await;
}

/// Scenario 3 — `FailedRetryable` must NOT release the permit; `Succeeded` does.
///
/// Locks in the retry-loop contract in [`ProveShardGate::schedule_release`]:
/// only `Succeeded` / `FailedFatal` release the permit. On `FailedRetryable`
/// the coordinator is re-queuing the same `task_id`, so the record artifact
/// is still live in Redis and its permit must stay held until a truly
/// terminal status arrives.
#[tokio::test]
#[ignore = "requires Redis (set REDIS_URL or run with CI services block)"]
async fn scenario_3_failed_retryable_holds_permit() {
    let proof_id = "test-proof-3";
    // Long auto-completion delay — we drive status transitions manually.
    let (_artifact_client, worker_client, gate) =
        setup(proof_id, Duration::from_secs(30)).await;

    // Acquire permit A and hand it to schedule_release.
    let record_a: Artifact = "s3-record-a".to_string().into();
    let permit_a = gate.acquire(&record_a).await;
    let task_a = submit_shard(&worker_client, proof_id, record_a).await;
    gate.schedule_release(task_a.clone(), permit_a);

    // Let the spawned release task call wait_task and register task_a in the
    // subscriber's request_map before we broadcast.
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Drain the rest of the pool (4 - 1 = 3 fillers).
    let mut fillers = Vec::new();
    for i in 0..3 {
        let a: Artifact = format!("s3-filler-{i}").into();
        fillers.push(gate.acquire(&a).await);
    }

    // Transition task A to FailedRetryable — gate's retry loop should keep
    // the permit held.
    worker_client
        .transition(&task_a, TaskStatus::FailedRetryable)
        .await;
    tokio::time::sleep(Duration::from_millis(800)).await;

    // Drop fillers (frees 3 slots), reacquire 3 (hold them this time so the
    // pool stays at 3/4 used by us + 1/4 still held by permit A).
    drop(fillers);
    let mut refills = Vec::new();
    for i in 0..3 {
        let a: Artifact = format!("s3-refill-{i}").into();
        refills.push(
            tokio::time::timeout(Duration::from_millis(100), gate.acquire(&a))
                .await
                .expect("refill blocked"),
        );
    }
    // 4th probe must block: permit A is still held by schedule_release.
    let blocked_probe: Artifact = "s3-blocked-probe".to_string().into();
    let blocked = tokio::time::timeout(
        Duration::from_millis(500),
        gate.acquire(&blocked_probe),
    )
    .await;
    assert!(
        blocked.is_err(),
        "permit A released on FailedRetryable — should still be held"
    );

    // Transition task A to Succeeded — gate breaks out of loop, drops permit.
    worker_client
        .transition(&task_a, TaskStatus::Succeeded)
        .await;
    let recovered: Artifact = "s3-recovered-probe".to_string().into();
    assert_completes(
        Duration::from_millis(500),
        "permit A not released after Succeeded transition",
        gate.acquire(&recovered),
    )
    .await;
}

// ─── Mock WorkerClient ──────────────────────────────────────────────────────

#[derive(Clone)]
struct MockWorkerClient {
    inner: Arc<MockInner>,
}

struct MockInner {
    completion_delay: Duration,
    tasks: Mutex<HashMap<TaskId, watch::Sender<TaskStatus>>>,
    subscribers: Mutex<Vec<mpsc::UnboundedSender<(TaskId, TaskStatus)>>>,
    counter: Mutex<u64>,
}

impl MockWorkerClient {
    fn new(completion_delay: Duration) -> Self {
        Self {
            inner: Arc::new(MockInner {
                completion_delay,
                tasks: Mutex::new(HashMap::new()),
                subscribers: Mutex::new(Vec::new()),
                counter: Mutex::new(0),
            }),
        }
    }

    async fn transition(&self, task_id: &TaskId, status: TaskStatus) {
        if let Some(sender) = self.inner.tasks.lock().await.get(task_id) {
            let _ = sender.send(status);
        }
        for sub in self.inner.subscribers.lock().await.iter() {
            let _ = sub.send((task_id.clone(), status));
        }
    }
}

impl WorkerClient for MockWorkerClient {
    async fn submit_task(&self, _kind: TaskType, _task: RawTaskRequest) -> Result<TaskId> {
        let mut counter = self.inner.counter.lock().await;
        *counter += 1;
        let task_id = TaskId::new(format!("mock-task-{}", *counter));
        drop(counter);

        let (tx, _rx) = watch::channel(TaskStatus::Pending);
        self.inner.tasks.lock().await.insert(task_id.clone(), tx);

        let inner = self.inner.clone();
        let task_id_for_spawn = task_id.clone();
        tokio::spawn(async move {
            tokio::time::sleep(inner.completion_delay).await;
            if let Some(sender) = inner.tasks.lock().await.get(&task_id_for_spawn) {
                let _ = sender.send(TaskStatus::Succeeded);
            }
            for sub in inner.subscribers.lock().await.iter() {
                let _ = sub.send((task_id_for_spawn.clone(), TaskStatus::Succeeded));
            }
        });

        Ok(task_id)
    }

    async fn complete_task(
        &self,
        _proof_id: ProofId,
        _task_id: TaskId,
        _metadata: TaskMetadata,
    ) -> Result<()> {
        Ok(())
    }

    async fn complete_proof(
        &self,
        _proof_id: ProofId,
        _task_id: Option<TaskId>,
        _status: ProofRequestStatus,
        _extra_data: impl Into<String> + Send,
    ) -> Result<()> {
        Ok(())
    }

    async fn subscriber(&self, _proof_id: ProofId) -> Result<SubscriberBuilder<Self>> {
        let (input_tx, mut input_rx) = mpsc::unbounded_channel::<TaskId>();
        let (output_tx, output_rx) = mpsc::unbounded_channel::<(TaskId, TaskStatus)>();
        self.inner.subscribers.lock().await.push(output_tx.clone());
        tokio::spawn(async move {
            while input_rx.recv().await.is_some() {
                // broadcast model — discard incoming watches
            }
        });
        for (task_id, sender) in self.inner.tasks.lock().await.iter() {
            let status = *sender.borrow();
            if matches!(
                status,
                TaskStatus::Succeeded | TaskStatus::FailedFatal | TaskStatus::FailedRetryable
            ) {
                let _ = output_tx.send((task_id.clone(), status));
            }
        }
        Ok(SubscriberBuilder::new(self.clone(), input_tx, output_rx))
    }

    async fn subscribe_task_messages(
        &self,
        _task_id: &TaskId,
    ) -> Result<mpsc::UnboundedReceiver<Vec<u8>>> {
        let (_tx, rx) = mpsc::unbounded_channel();
        Ok(rx)
    }

    async fn send_task_message(&self, _task_id: &TaskId, _payload: Vec<u8>) -> Result<()> {
        Ok(())
    }
}
