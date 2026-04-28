//! Per-proof in-process broadcast wired to the cluster API's
//! `SubscribeProofEvents` stream.
//!
//! At gateway startup we open one subscription to the API and demux each
//! `ProofEvent` into a `broadcast::Sender` keyed by `proof_id`. Callers
//! that want to wake up the moment a proof transitions out of `Pending`
//! (today: nothing; Lever 3's `WaitForProofRequestStatus` long-poll: yes)
//! get a `broadcast::Receiver<ProofRequestStatus>` from `wait_for_status`.
//!
//! Rationale for not surfacing this through the SDK yet:
//!   - Lever 1+2 is the cluster-internal half of the latency win. The SDK
//!     keeps polling every 2s for now (`crates/sdk/src/network/prover.rs`).
//!   - Once Lever 3's proto change lands, the gateway already has the
//!     plumbing to satisfy long-polls in ms instead of waiting for the
//!     next SDK tick.

use std::sync::Arc;

use anyhow::{Context, Result};
use dashmap::DashMap;
use sp1_cluster_common::{
    client::ClusterServiceClient,
    proto::{events::SubscribeProofEventsRequest, status_from_i32, ProofRequestStatus},
};
use tokio::{sync::broadcast, time::Duration};
use tracing::{debug, info, warn};

/// Capacity per `proof_id` channel. Status transitions per proof are bounded
/// (Pending → Completed/Failed/Cancelled, ~3 events), so 16 leaves slack
/// without bloating memory.
const PER_PROOF_CAPACITY: usize = 16;

#[derive(Clone, Default)]
pub struct ProofEventsHub {
    senders: Arc<DashMap<String, broadcast::Sender<ProofRequestStatus>>>,
}

impl ProofEventsHub {
    pub fn new() -> Self {
        Self::default()
    }

    /// Subscribe to status transitions for a specific `proof_id`. Always
    /// returns a receiver — the sender is created on first subscribe and
    /// reused thereafter. Callers must check the latest known status via
    /// `cluster.get_proof_request` before blocking on this; otherwise
    /// they'll miss events that fired before they subscribed.
    pub fn subscribe(&self, proof_id: &str) -> broadcast::Receiver<ProofRequestStatus> {
        let entry = self
            .senders
            .entry(proof_id.to_string())
            .or_insert_with(|| broadcast::channel(PER_PROOF_CAPACITY).0);
        entry.subscribe()
    }

    /// Drop the channel for a `proof_id` once we're sure nobody else cares.
    /// Safe to call multiple times. Today nobody calls this — left as the
    /// hook for Lever 3 to clean up after a long-poll terminates.
    pub fn forget(&self, proof_id: &str) {
        self.senders.remove(proof_id);
    }

    fn publish(&self, proof_id: &str, status: ProofRequestStatus) {
        if let Some(entry) = self.senders.get(proof_id) {
            // Channel `send` errors only when there are no receivers, in
            // which case we just drop. The map entry hangs around until
            // someone calls `forget` — bounded by the request rate, fine
            // for self-hosted scale.
            let _ = entry.send(status);
        }
    }
}

/// Spawn the subscription task. Returns a clone of the hub for sharing into
/// gRPC handlers via `ProverNetworkImpl`.
pub async fn spawn(api_client: ClusterServiceClient) -> Result<ProofEventsHub> {
    let hub = ProofEventsHub::new();
    // Probe once so we fail fast on a misconfigured cluster RPC.
    api_client
        .clone()
        .events
        .clone()
        .subscribe_proof_events(SubscribeProofEventsRequest::default())
        .await
        .context("initial SubscribeProofEvents probe failed")?;

    let task_hub = hub.clone();
    tokio::spawn(async move {
        run(api_client, task_hub).await;
    });
    Ok(hub)
}

async fn run(api_client: ClusterServiceClient, hub: ProofEventsHub) {
    let mut backoff = Duration::from_millis(200);
    loop {
        let mut events = api_client.events.clone();
        let stream = match events
            .subscribe_proof_events(SubscribeProofEventsRequest::default())
            .await
        {
            Ok(resp) => resp.into_inner(),
            Err(e) => {
                warn!(
                    "subscribe_proof_events failed: {e}; retrying in {:?}",
                    backoff
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(10));
                continue;
            }
        };
        info!("subscribed to proof_event stream");
        backoff = Duration::from_millis(200);

        let mut stream = stream;
        loop {
            match stream.message().await {
                Ok(Some(event)) => {
                    let status = status_from_i32(event.proof_status);
                    debug!(proof_id = %event.proof_id, ?status, handled = event.handled, "proof_event");
                    hub.publish(&event.proof_id, status);
                }
                Ok(None) => {
                    warn!("proof_event stream closed; reconnecting");
                    break;
                }
                Err(e) => {
                    warn!("proof_event stream error: {e}; reconnecting");
                    break;
                }
            }
        }
    }
}
