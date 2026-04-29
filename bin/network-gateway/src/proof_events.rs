//! Per-proof in-process broadcast wired to the cluster API's
//! `SubscribeProofEvents` stream.
//!
//! At gateway startup we open one subscription to the API and demux each
//! `ProofEvent` into a `broadcast::Sender` keyed by `proof_id`. Callers
//! that want to wake up the moment a proof transitions out of `Pending`
//! get a [`Subscription`] guard via [`ProofEventsHub::subscribe`].
//!
//! ## Channel lifecycle
//!
//! Two cleanup paths keep the in-process map bounded:
//!
//! 1. **Terminal-publish.** When [`publish`](ProofEventsHub::publish)
//!    sees a terminal cluster status (`Completed` / `Failed` /
//!    `Cancelled`), the entry is removed immediately after the send.
//!    No further NOTIFY events will fire for that `proof_id`, so the
//!    channel is dead weight. Any subscribers still attached drain
//!    their buffered values then observe `Closed` on the next `recv`.
//!
//! 2. **Drop-on-idle.** [`Subscription`] is RAII: dropping it
//!    decrements `receiver_count`, then removes the map entry iff no
//!    receivers remain. Catches the case where someone subscribes to
//!    an *already-terminal* proof — terminal-publish can't fire a
//!    second time, so without this the entry would leak.

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

/// Cluster statuses past which no further NOTIFY events fire.
fn is_terminal(status: ProofRequestStatus) -> bool {
    matches!(
        status,
        ProofRequestStatus::Completed | ProofRequestStatus::Failed | ProofRequestStatus::Cancelled
    )
}

#[derive(Clone, Default)]
pub struct ProofEventsHub {
    senders: Arc<DashMap<String, broadcast::Sender<ProofRequestStatus>>>,
}

impl ProofEventsHub {
    pub fn new() -> Self {
        Self::default()
    }

    /// Subscribe to status transitions for `proof_id`. The returned
    /// [`Subscription`] is RAII — drop it (or let it go out of scope)
    /// to release the channel and let the hub GC empty entries.
    ///
    /// Callers should still consult the canonical status via
    /// `cluster.get_proof_request` *after* subscribing; otherwise they
    /// race against transitions that fired before the subscribe call.
    pub fn subscribe(&self, proof_id: &str) -> Subscription {
        let entry = self
            .senders
            .entry(proof_id.to_string())
            .or_insert_with(|| broadcast::channel(PER_PROOF_CAPACITY).0);
        Subscription {
            inner: Some(entry.subscribe()),
            hub: self.clone(),
            proof_id: proof_id.to_string(),
        }
    }

    fn publish(&self, proof_id: &str, status: ProofRequestStatus) {
        if let Some(entry) = self.senders.get(proof_id) {
            // `send` errors only when there are zero receivers; we don't
            // care — `recv` on an idle channel that later gets a value
            // works regardless.
            let _ = entry.send(status);
        }
        if is_terminal(status) {
            // Common path. Reaches before any subscriber's Drop runs,
            // so subscribers don't have to think about post-terminal
            // bookkeeping.
            self.senders.remove(proof_id);
        }
    }

    /// Remove the entry iff it has zero attached receivers. Driven from
    /// [`Subscription::drop`]; not part of the public API because
    /// callers should never need to think about hub bookkeeping.
    fn cleanup_if_idle(&self, proof_id: &str) {
        // `remove_if` locks the bucket while evaluating the predicate,
        // so a concurrent `subscribe` can't sneak a new receiver in
        // between the count check and the removal.
        self.senders
            .remove_if(proof_id, |_, sender| sender.receiver_count() == 0);
    }

    /// Test/diagnostic helper. Number of active per-proof channels.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.senders.len()
    }
}

/// RAII guard returned by [`ProofEventsHub::subscribe`]. Wraps a
/// [`broadcast::Receiver`]; on drop, decrements the receiver count and
/// asks the hub to GC the channel if nothing else is listening.
#[must_use = "dropping the subscription unregisters from the hub"]
pub struct Subscription {
    // `Option` so [`Drop`] can `take()` and decrement the broadcast
    // receiver count *before* the cleanup check. Always `Some` outside
    // of [`Drop::drop`].
    inner: Option<broadcast::Receiver<ProofRequestStatus>>,
    hub: ProofEventsHub,
    proof_id: String,
}

impl Subscription {
    pub async fn recv(&mut self) -> Result<ProofRequestStatus, broadcast::error::RecvError> {
        self.inner
            .as_mut()
            .expect("receiver only None inside Drop")
            .recv()
            .await
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        // Take and drop the receiver first so its decrement of
        // receiver_count is visible to `cleanup_if_idle`'s predicate.
        drop(self.inner.take());
        self.hub.cleanup_if_idle(&self.proof_id);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn subscribe_creates_entry_and_drop_removes_it() {
        let hub = ProofEventsHub::new();
        assert_eq!(hub.len(), 0);

        let sub = hub.subscribe("p1");
        assert_eq!(hub.len(), 1);

        drop(sub);
        assert_eq!(hub.len(), 0);
    }

    #[tokio::test]
    async fn second_subscriber_keeps_entry_alive_until_last_drops() {
        let hub = ProofEventsHub::new();
        let a = hub.subscribe("p1");
        let b = hub.subscribe("p1");
        assert_eq!(hub.len(), 1);

        drop(a);
        assert_eq!(hub.len(), 1, "b still attached");

        drop(b);
        assert_eq!(hub.len(), 0);
    }

    #[tokio::test]
    async fn terminal_publish_evicts_entry_even_with_active_receiver() {
        let hub = ProofEventsHub::new();
        let mut sub = hub.subscribe("p1");
        assert_eq!(hub.len(), 1);

        hub.publish("p1", ProofRequestStatus::Completed);
        assert_eq!(hub.len(), 0, "terminal publish removes the entry");

        // Subscriber still receives the buffered terminal status, then
        // observes `Closed` on the next recv.
        let got = sub.recv().await.unwrap();
        assert_eq!(got, ProofRequestStatus::Completed);
        let next = sub.recv().await;
        assert!(matches!(next, Err(broadcast::error::RecvError::Closed)));

        // Drop is a no-op (entry already gone); must not panic.
        drop(sub);
        assert_eq!(hub.len(), 0);
    }

    #[tokio::test]
    async fn non_terminal_publish_keeps_entry() {
        let hub = ProofEventsHub::new();
        let _sub = hub.subscribe("p1");
        hub.publish("p1", ProofRequestStatus::Pending);
        assert_eq!(hub.len(), 1);
    }

    #[tokio::test]
    async fn publish_after_drop_is_safe() {
        let hub = ProofEventsHub::new();
        // No subscribers — publish is a no-op even for terminal.
        hub.publish("p1", ProofRequestStatus::Completed);
        assert_eq!(hub.len(), 0);

        // Subscriber arrives after, channel is fresh.
        let sub = hub.subscribe("p1");
        assert_eq!(hub.len(), 1);
        drop(sub);
        assert_eq!(hub.len(), 0);
    }

    #[tokio::test]
    async fn subscriber_receives_published_value() {
        let hub = ProofEventsHub::new();
        let mut sub = hub.subscribe("p1");
        hub.publish("p1", ProofRequestStatus::Pending);
        assert_eq!(sub.recv().await.unwrap(), ProofRequestStatus::Pending);
    }
}
