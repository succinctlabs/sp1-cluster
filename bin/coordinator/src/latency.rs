use std::{collections::HashMap, sync::Arc, time::Duration};

use tokio::sync::Mutex;

type LatencyRecord = (u128, u128, u128, usize);

pub struct LatencyTracker {
    state: Arc<Mutex<HashMap<String, LatencyRecord>>>,
}

impl Default for LatencyTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl LatencyTracker {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn record_latency(&self, kind: &str, duration: Duration) {
        let nanos = duration.as_nanos();
        if nanos > 100000000 {
            tracing::error!("latency {} for kind {} is too high", nanos, kind);
        }
        if *ENABLE_LATENCY_DEBUG {
            let mut state = self.state.lock().await;
            match state.get_mut(kind) {
                Some((min, max, sum, count)) => {
                    *min = std::cmp::min(*min, nanos);
                    *max = std::cmp::max(*max, nanos);
                    *sum += nanos;
                    *count += 1;
                }
                None => {
                    state.insert(kind.to_owned(), (nanos, nanos, nanos, 1));
                }
            }
        }
    }

    pub async fn display_latency(&self) {
        if *ENABLE_LATENCY_DEBUG {
            let state = self.state.lock().await;
            tracing::info!("[latency] {:?}", state.keys());
            for (kind, (min, max, sum, count)) in state.iter() {
                tracing::info!(
                    "[latency] {}: min = {}, max = {}, avg = {}, count = {}",
                    kind,
                    min,
                    max,
                    sum / (*count as u128),
                    count
                );
            }
        }
    }
}

// Global static latency tracker.
lazy_static::lazy_static! {
    pub static ref LATENCY_TRACKER: LatencyTracker = LatencyTracker::new();

    pub static ref ENABLE_LATENCY_DEBUG: bool = std::env::var("ENABLE_LATENCY_DEBUG").unwrap_or("false".to_string()).parse::<bool>().unwrap_or(false);
}

// Simple macro to track latency of a block. use like track_latency!("kind", { ... });
#[macro_export]
macro_rules! track_latency {
    ($kind:expr, $block:block) => {{
        let start = std::time::Instant::now();
        let result = $block;
        $crate::latency::LATENCY_TRACKER
            .record_latency($kind, start.elapsed())
            .await;
        result
    }};
}

pub async fn print_latency() {
    LATENCY_TRACKER.display_latency().await;
}
