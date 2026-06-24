//! Optional client the fulfiller uses to collect the coordinator's cluster
//! component manifest (coordinator + workers) via the `GetClusterComponentInfo`
//! RPC (sp1#2850), for forwarding to the SPN `ReportProverInfo` RPC.
//!
//! Two outcomes are distinct because the receiver treats each report as a full
//! snapshot: an *unset* coordinator means "fulfiller-only is the whole manifest"
//! (valid), whereas a *configured* coordinator that is unreachable / `Unimplemented`
//! / slow is a *failure* (we must not send a fulfiller-only snapshot that would
//! prune the coordinator/worker rows — the caller skips and retries). Both connect
//! and fetch are bounded by short explicit timeouts so telemetry can never stall
//! fulfillment.

use std::time::Duration;

use sp1_cluster_common::proto::{
    worker_service_client::WorkerServiceClient, ClusterComponentInfo,
    GetClusterComponentInfoRequest,
};
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tracing::warn;

/// Bounded timeout for the optional coordinator connect attempt made lazily
/// during telemetry collection. The shared `reconnect_with_backoff` helper
/// retries indefinitely; this cap turns each (re)connect into a one-shot bounded
/// attempt so an unreachable coordinator fails promptly (caller skips + retries)
/// instead of stalling the report path.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Bounded timeout for a single `GetClusterComponentInfo` fetch, so a slow or
/// hanging coordinator can never stall the fulfiller's report path.
const FETCH_TIMEOUT: Duration = Duration::from_secs(5);

/// Wraps a coordinator `WorkerServiceClient` for the single RPC the fulfiller
/// needs. Held as `Option` by the fulfiller — `None` means no coordinator was
/// configured and only the fulfiller's own component is reported.
#[derive(Clone)]
pub struct CoordinatorComponentClient {
    client: WorkerServiceClient<Channel>,
}

impl CoordinatorComponentClient {
    /// Connect to the coordinator's WorkerService at `addr`, bounded by
    /// [`CONNECT_TIMEOUT`]. A timeout or connection failure is returned as `Err`;
    /// because the coordinator is configured, the caller skips the report and retries.
    pub async fn connect(addr: &str) -> anyhow::Result<Self> {
        let channel = tokio::time::timeout(
            CONNECT_TIMEOUT,
            sp1_cluster_common::client::reconnect_with_backoff(addr),
        )
        .await
        .map_err(|_| {
            anyhow::anyhow!("connect coordinator WorkerService timed out after {CONNECT_TIMEOUT:?}")
        })?
        .map_err(|e| anyhow::anyhow!("connect coordinator WorkerService: {e}"))?;
        Ok(Self {
            client: WorkerServiceClient::new(channel),
        })
    }

    /// Fetch the coordinator's cluster component manifest (its own entry + one per
    /// connected worker), bounded by [`FETCH_TIMEOUT`]. Returns the raw
    /// `ClusterComponentInfo` list; the caller maps it onto the network
    /// `ComponentInfo` type.
    ///
    /// Returns `Err` on any failure — timeout, transport error, or `Unimplemented`
    /// from a coordinator built before sp1#2850 — which the caller propagates as a
    /// report failure (skip + retry), not as a fulfiller-only snapshot.
    pub async fn get_cluster_component_info(&self) -> anyhow::Result<Vec<ClusterComponentInfo>> {
        let resp = tokio::time::timeout(
            FETCH_TIMEOUT,
            self.client
                .clone()
                .get_cluster_component_info(GetClusterComponentInfoRequest {}),
        )
        .await
        .map_err(|_| {
            anyhow::anyhow!("get_cluster_component_info timed out after {FETCH_TIMEOUT:?}")
        })??
        .into_inner();
        Ok(resp.components)
    }
}

/// Collect the coordinator's cluster component manifest for one report tick,
/// managing the lazily-(re)connected client `cache`.
///
/// Returns:
/// - `Ok(None)` when `coordinator_rpc` is unset: no coordinator is configured, so a
///   fulfiller-only report is the correct full snapshot; the cache is left untouched.
/// - `Ok(Some(manifest))` when a configured coordinator is reached and the fetch
///   succeeds (the client is cached for later ticks).
/// - `Err(_)` when a configured coordinator's connect/fetch fails or times out. This
///   is NOT a fulfiller-only snapshot: the caller must skip the report and retry,
///   otherwise the receiver would prune the coordinator/worker rows from current.
///
/// Availability is decided per tick, not once at startup, so a coordinator that is
/// down when the fulfiller boots is still picked up on a later tick. Bounded
/// end-to-end (each connect/fetch is timeout-capped); intended to be called from the
/// fulfiller's detached background report task, never from the main fulfillment loop.
pub async fn collect_cluster_components(
    coordinator_rpc: &Option<String>,
    cache: &Mutex<Option<CoordinatorComponentClient>>,
) -> anyhow::Result<Option<Vec<ClusterComponentInfo>>> {
    let Some(addr) = coordinator_rpc.as_deref() else {
        // No coordinator configured -> fulfiller-only. Leave the cache untouched.
        return Ok(None);
    };

    let mut guard = cache.lock().await;

    // Fast path: reuse the cached client if one is connected.
    if let Some(client) = guard.as_ref() {
        match client.get_cluster_component_info().await {
            Ok(components) => return Ok(Some(components)),
            Err(e) => {
                warn!("coordinator component fetch failed on cached client; reconnecting: {e:?}");
                // Invalidate so the next step (and future ticks) reconnect.
                *guard = None;
            }
        }
    }

    // No (valid) client: bounded reconnect, then bounded fetch.
    let client = CoordinatorComponentClient::connect(addr).await?;
    let components = client.get_cluster_component_info().await?;
    // Cache the freshly connected client for subsequent ticks.
    *guard = Some(client);
    Ok(Some(components))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn unset_coordinator_returns_none_without_touching_cache() {
        let cache = Mutex::new(None);
        let result = collect_cluster_components(&None, &cache).await;
        // Unset coordinator => Ok(None): fulfiller-only is the valid full snapshot.
        assert!(matches!(result, Ok(None)), "unset coordinator => Ok(None)");
        assert!(
            cache.lock().await.is_none(),
            "cache must be left untouched when no coordinator is configured"
        );
    }

    #[tokio::test]
    async fn unreachable_configured_coordinator_errors_and_stays_recoverable() {
        // Closed port: connect fails (bounded by CONNECT_TIMEOUT), never hangs.
        let addr = Some("http://127.0.0.1:1".to_string());
        let cache = Mutex::new(None);

        let started = std::time::Instant::now();
        let result = collect_cluster_components(&addr, &cache).await;

        // A configured-but-unreachable coordinator is a failure, NOT a fulfiller-only
        // snapshot: the caller must skip the report and retry.
        assert!(
            result.is_err(),
            "configured + unreachable coordinator => Err"
        );
        // No client cached on failure => the NEXT tick reconnects from the retained
        // address. There is no permanent-disable state.
        assert!(
            cache.lock().await.is_none(),
            "failed connect must leave the cache empty so later ticks retry"
        );
        // Sanity: stayed within a bounded window (real worst case ~CONNECT_TIMEOUT).
        assert!(
            started.elapsed() < Duration::from_secs(20),
            "telemetry must be bounded, not block"
        );
    }
}
