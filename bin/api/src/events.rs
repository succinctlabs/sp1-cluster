//! Implementation of [`ClusterEventsService::SubscribeProofEvents`].
//!
//! At server startup we open one [`PgListener`] that LISTENs on the
//! `proof_event` channel — emitted by the AFTER INSERT/UPDATE trigger on
//! `proof_requests` (see migrations/20260427000000_proof_event_trigger.sql).
//! Each notify payload is a small JSON blob that we deserialize into a
//! [`ProofEvent`] and fan out via a [`tokio::sync::broadcast`] channel.
//! Every gRPC client that calls `subscribe_proof_events` gets its own
//! receiver and an optional `scheduled_by` filter applied client-side
//! within this process.
//!
//! Notes:
//!  - PG NOTIFY is fire-and-forget over the LISTEN connection. If the
//!    listener disconnects briefly we miss events; consumers (coordinator,
//!    gateway) keep a slow background poll as a safety net.
//!  - Broadcast capacity is bounded; slow subscribers see `Lagged` and we
//!    drop the connection. Consumers should reconnect.

use std::pin::Pin;

use futures::{Stream, StreamExt};
use serde::Deserialize;
use sp1_cluster_common::proto::events::{
    cluster_events_service_server::ClusterEventsService, ProofEvent, SubscribeProofEventsRequest,
};
use sqlx::postgres::{PgListener, PgPool};
use tokio::sync::broadcast;
use tokio_stream::wrappers::{errors::BroadcastStreamRecvError, BroadcastStream};
use tonic::{Request, Response, Status};
use tracing::{error, info, warn};

/// Bounded fan-out from the single PgListener task to all gRPC subscribers.
/// Sized for spike tolerance: a coordinator restart that reclaims thousands of
/// pending rows still fits without dropping anyone, but we don't unbound the
/// channel.
const BROADCAST_CAPACITY: usize = 1024;

/// Exact JSON shape the trigger emits — see migrations/.
#[derive(Debug, Deserialize)]
struct NotifyPayload {
    proof_id: String,
    proof_status: i32,
    handled: bool,
    scheduled_by: Option<String>,
}

impl From<NotifyPayload> for ProofEvent {
    fn from(p: NotifyPayload) -> Self {
        Self {
            proof_id: p.proof_id,
            proof_status: p.proof_status,
            handled: p.handled,
            scheduled_by: p.scheduled_by,
        }
    }
}

#[derive(Clone)]
pub struct ClusterEventsImpl {
    tx: broadcast::Sender<ProofEvent>,
}

impl ClusterEventsImpl {
    /// Spawn the PgListener task and return the gRPC service handle.
    pub async fn start(pool: PgPool) -> Result<Self, sqlx::Error> {
        let (tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        let mut listener = PgListener::connect_with(&pool).await?;
        listener.listen("proof_event").await?;
        info!("Listening for proof_event NOTIFY on Postgres");

        let task_tx = tx.clone();
        tokio::spawn(async move {
            loop {
                match listener.recv().await {
                    Ok(notification) => {
                        let payload = notification.payload();
                        match serde_json::from_str::<NotifyPayload>(payload) {
                            Ok(parsed) => {
                                // Send fails only when there are zero
                                // receivers, which is fine — we just drop.
                                let _ = task_tx.send(parsed.into());
                            }
                            Err(e) => {
                                warn!("malformed proof_event payload {payload:?}: {e}");
                            }
                        }
                    }
                    Err(e) => {
                        // PgListener auto-reconnects, but log so we notice
                        // long flaps that would let consumers fall behind.
                        error!("PgListener recv error: {e}");
                    }
                }
            }
        });
        Ok(Self { tx })
    }

}

type EventStream = Pin<Box<dyn Stream<Item = Result<ProofEvent, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl ClusterEventsService for ClusterEventsImpl {
    type SubscribeProofEventsStream = EventStream;

    async fn subscribe_proof_events(
        &self,
        request: Request<SubscribeProofEventsRequest>,
    ) -> Result<Response<Self::SubscribeProofEventsStream>, Status> {
        let filter = request.into_inner().scheduled_by;
        let rx = self.tx.subscribe();

        let stream = BroadcastStream::new(rx).filter_map(move |item| {
            // Filter is applied on the server side per subscriber so a
            // misbehaving client can't suck the entire firehose.
            let filter = filter.clone();
            async move {
                match item {
                    Ok(event) => {
                        if let Some(want) = filter.as_deref() {
                            if event.scheduled_by.as_deref() != Some(want) {
                                return None;
                            }
                        }
                        Some(Ok(event))
                    }
                    // Slow subscriber: surface so client resyncs via
                    // ProofRequestList rather than silently skipping events.
                    Err(BroadcastStreamRecvError::Lagged(n)) => Some(Err(
                        Status::resource_exhausted(format!("subscriber lagged {n} events")),
                    )),
                }
            }
        });

        Ok(Response::new(Box::pin(stream)))
    }
}
