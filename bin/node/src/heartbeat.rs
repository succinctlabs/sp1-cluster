//! Worker liveness, decoupled from everything that can starve it.
//!
//! A saturated worker holds every runtime thread in long polls for tens of
//! seconds at a time. Beats sent from that runtime go silent under exactly
//! that load, and the coordinator declares a live worker dead and re-delivers
//! its tasks. These beats come from a thread that shares nothing with prover
//! work, so a silent worker is genuinely stuck, not merely busy.
//!
//! Trade-off: the beats keep the worker registered even when its delivery
//! stream wedges, so recovering from that falls to the main loop's reconnect
//! and silence watchdogs.

use crate::config::NodeConfig;
use crate::ActiveTask;
use dashmap::DashMap;
use sp1_cluster_common::proto::HeartbeatRequest;
use sp1_cluster_worker::client::WorkerServiceClient;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

/// Interval between liveness heartbeats sent from the dedicated thread.
///
/// The coordinator evicts a worker after `COORDINATOR_WORKER_HEARTBEAT_TIMEOUT_SECS`
/// (default 30s) without a heartbeat, so this interval leaves ~6 beats of margin
/// before a false eviction.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
/// Per-attempt deadline. A missed beat is dropped, never retried — the next
/// tick is a fresher liveness signal than a stale delivery.
const HEARTBEAT_RPC_TIMEOUT: Duration = Duration::from_secs(3);
/// Backstop for a thread that stops beating without reporting a failure —
/// hung mid-send, or wedged before its first attempt. A thread that is still
/// running hands over on the failed beat itself, well inside this.
const BEATS_STALE_AFTER: Duration = Duration::from_secs(10);

/// Stops the heartbeat thread when dropped.
///
/// Ties the beats to the worker loop's lifetime, not the worker token: a
/// draining worker must keep beating or the coordinator evicts it mid-drain,
/// and a dropped loop (harness kill) must go silent so eviction notices.
#[must_use = "dropping this stops the heartbeat thread"]
pub(crate) struct HeartbeatHandle {
    stop: CancellationToken,
    clock: Arc<BeatClock>,
}

impl HeartbeatHandle {
    /// Beats from an OS thread with its own runtime until the handle drops.
    ///
    /// Fate-shared: if the thread dies on its own — connect failure or panic —
    /// the guard cancels `worker_token` so the worker restarts instead of
    /// running without proactive liveness.
    pub(crate) fn spawn(
        node_config: NodeConfig,
        tasks: Arc<DashMap<(String, String), ActiveTask>>,
        worker_token: CancellationToken,
    ) -> std::io::Result<Self> {
        let stop = CancellationToken::new();
        let clock = Arc::new(BeatClock::new());
        let (thread_stop, thread_clock) = (stop.clone(), clock.clone());
        std::thread::Builder::new()
            .name("worker-heartbeat".into())
            .spawn(move || {
                let _cancel_on_exit = sp1_cluster_worker::utils::DeferGuard::new(
                    (thread_stop.clone(), worker_token),
                    |(stop, worker_token)| {
                        if !stop.is_cancelled() && !worker_token.is_cancelled() {
                            tracing::error!("heartbeat thread exited; shutting worker down");
                            worker_token.cancel();
                        }
                    },
                );
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("failed to build heartbeat runtime");
                rt.block_on(heartbeat_loop(
                    node_config,
                    tasks,
                    thread_clock,
                    thread_stop,
                ));
            })?;
        Ok(Self { stop, clock })
    }

    /// Shares when the thread last beat, so the main loop can cover for it.
    pub(crate) fn clock(&self) -> Arc<BeatClock> {
        self.clock.clone()
    }
}

impl Drop for HeartbeatHandle {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

/// The dedicated thread's last beat, if it landed.
///
/// The thread owns its own connection, which can die while the delivery stream
/// stays healthy, and a worker that stops beating gets evicted no matter how
/// alive it is. So the main loop watches this and covers when it reads stale.
///
/// Holds only what is still worth trusting: a failed beat clears it outright
/// rather than ageing out, because the eviction timeout is configurable and
/// can be shorter than any threshold this could wait out. Tracks the dedicated
/// path alone — fallback beats deliberately do not refresh it, so the main
/// loop keeps covering until the thread itself recovers.
pub(crate) struct BeatClock(Mutex<Option<Instant>>);

impl BeatClock {
    /// Starts stale: the main loop covers until the thread proves its
    /// connection works.
    fn new() -> Self {
        Self(Mutex::new(None))
    }

    fn record(&self, landed: bool) {
        *self.0.lock().unwrap() = landed.then(Instant::now);
    }

    /// True when beats stopped landing, so the main loop should cover on the
    /// stream it knows works.
    pub(crate) fn is_stale(&self) -> bool {
        self.0
            .lock()
            .unwrap()
            .is_none_or(|last_ok| last_ok.elapsed() > BEATS_STALE_AFTER)
    }
}

/// Sends one beat and reports whether it landed.
///
/// Shared with the main loop's fallback so both paths classify and log an
/// outcome the same way.
pub(crate) async fn send_beat(
    client: &WorkerServiceClient,
    tasks: &DashMap<(String, String), ActiveTask>,
) -> bool {
    let request = build_heartbeat_request(&client.worker_id, tasks);
    match tokio::time::timeout(HEARTBEAT_RPC_TIMEOUT, client.heartbeat_once(request)).await {
        Ok(Ok(())) => true,
        // Expected until the main loop's `open()` registers this worker, and
        // after an eviction until it re-registers. Both self-heal.
        Ok(Err(status)) if status.code() == tonic::Code::NotFound => {
            tracing::debug!("heartbeat for unregistered worker: {status}");
            false
        }
        Ok(Err(status)) => {
            tracing::warn!("heartbeat rejected: {status}");
            false
        }
        Err(_) => {
            tracing::warn!("heartbeat timed out after {HEARTBEAT_RPC_TIMEOUT:?}");
            false
        }
    }
}

async fn heartbeat_loop(
    node_config: NodeConfig,
    tasks: Arc<DashMap<(String, String), ActiveTask>>,
    clock: Arc<BeatClock>,
    stop: CancellationToken,
) {
    // Own client on its own channel: a cloned client would share a connection
    // driven by the runtime this thread must stay independent of.
    let client = tokio::select! {
        _ = stop.cancelled() => return,
        result = WorkerServiceClient::new(
            node_config.coordinator_rpc.clone(),
            node_config.worker_id.clone(),
            node_config.worker_type,
            node_config.location.clone(),
        ) => match result {
            Ok(client) => client,
            Err(e) => {
                tracing::error!("heartbeat thread failed to connect, exiting: {e}");
                return;
            }
        },
    };

    // First beat after one full interval. This thread starts before the main
    // loop registers the worker, so beating immediately always misses.
    let mut ticker = tokio::time::interval_at(
        tokio::time::Instant::now() + HEARTBEAT_INTERVAL,
        HEARTBEAT_INTERVAL,
    );
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = stop.cancelled() => return,
            _ = ticker.tick() => {}
        }
        clock.record(send_beat(&client, &tasks).await);
    }
}

/// One-pass snapshot so the reported task ids and weight stay consistent — the
/// coordinator schedules on the weight. Separate passes could tear under
/// concurrent task churn.
fn build_heartbeat_request(
    worker_id: &str,
    tasks: &DashMap<(String, String), ActiveTask>,
) -> HeartbeatRequest {
    let mut active_task_proof_ids = Vec::with_capacity(tasks.len());
    let mut active_task_ids = Vec::with_capacity(tasks.len());
    let mut current_weight = 0;
    for entry in tasks.iter() {
        let (proof_id, task_id) = entry.key();
        active_task_proof_ids.push(proof_id.clone());
        active_task_ids.push(task_id.clone());
        current_weight += entry.value().data.weight;
    }
    HeartbeatRequest {
        worker_id: worker_id.to_string(),
        active_task_proof_ids,
        active_task_ids,
        current_weight,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sp1_cluster_common::proto::TaskData;

    /// Valid but unreachable, so the thread sits in its connect retry — the
    /// state a worker boots into when the coordinator is down.
    fn test_config() -> NodeConfig {
        NodeConfig {
            coordinator_rpc: "http://127.0.0.1:1".to_string(),
            ..Default::default()
        }
    }

    /// The thread holds a clone of the task map while it runs, so the strong
    /// count says whether it is still alive.
    fn wait_for_exit(tasks: &Arc<DashMap<(String, String), ActiveTask>>) -> bool {
        for _ in 0..100 {
            if Arc::strong_count(tasks) == 1 {
                return true;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        false
    }

    #[test]
    fn beats_outlive_worker_shutdown() {
        let tasks = Arc::new(DashMap::new());
        let worker_token = CancellationToken::new();
        let handle =
            HeartbeatHandle::spawn(test_config(), tasks.clone(), worker_token.clone()).unwrap();

        // Drain starts: the worker still runs tasks, so it must still beat or
        // the coordinator evicts it and requeues work it is finishing.
        worker_token.cancel();
        std::thread::sleep(Duration::from_millis(200));
        assert_eq!(
            Arc::strong_count(&tasks),
            2,
            "heartbeat thread stopped at drain start"
        );

        drop(handle);
        assert!(
            wait_for_exit(&tasks),
            "heartbeat thread outlived its handle"
        );
    }

    #[test]
    fn stopping_the_thread_leaves_the_worker_running() {
        let tasks = Arc::new(DashMap::new());
        let worker_token = CancellationToken::new();
        let handle =
            HeartbeatHandle::spawn(test_config(), tasks.clone(), worker_token.clone()).unwrap();

        drop(handle);

        assert!(wait_for_exit(&tasks));
        assert!(
            !worker_token.is_cancelled(),
            "a deliberate stop shut the worker down"
        );
    }

    #[test]
    fn beats_read_stale_until_one_lands() {
        let clock = BeatClock::new();

        // A worker whose connection never came up must be covered from the
        // start: on short eviction timeouts it has less than one stale window.
        assert!(clock.is_stale(), "a fresh clock claimed a beat had landed");

        clock.record(true);
        assert!(!clock.is_stale());
    }

    #[test]
    fn a_failed_beat_hands_over_without_waiting() {
        let clock = BeatClock::new();
        clock.record(true);

        clock.record(false);

        // Not "ages out of the window" — the eviction timeout is configurable
        // and can be shorter than any window we could wait out.
        assert!(clock.is_stale(), "a failed beat still read healthy");
    }

    #[test]
    fn beats_read_stale_once_they_stop_landing() {
        let clock = BeatClock::new();

        // A thread hung mid-send reports neither success nor failure.
        *clock.0.lock().unwrap() =
            Some(Instant::now() - BEATS_STALE_AFTER - Duration::from_secs(1));

        assert!(clock.is_stale());
    }

    #[tokio::test]
    async fn request_reports_every_task_with_its_weight() {
        let task = |weight| {
            let work = tokio::spawn(std::future::pending::<()>());
            ActiveTask {
                data: TaskData {
                    weight,
                    ..Default::default()
                },
                started_at: Instant::now(),
                work: work.abort_handle(),
                reporter: work,
                aborted_at: Arc::new(std::sync::OnceLock::new()),
            }
        };
        let tasks = DashMap::new();
        tasks.insert(("p1".to_string(), "t1".to_string()), task(3));
        tasks.insert(("p2".to_string(), "t2".to_string()), task(4));

        let request = build_heartbeat_request("w1", &tasks);

        assert_eq!(request.worker_id, "w1");
        assert_eq!(request.current_weight, 7);
        let mut reported: Vec<_> = request
            .active_task_proof_ids
            .iter()
            .zip(&request.active_task_ids)
            .collect();
        reported.sort();
        assert_eq!(
            reported,
            vec![
                (&"p1".to_string(), &"t1".to_string()),
                (&"p2".to_string(), &"t2".to_string())
            ]
        );
    }
}
