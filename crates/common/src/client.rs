use crate::{
    proto::{
        self, cluster_service_client::ClusterServiceClient as InnerClusterClient,
        ProofRequestCancelRequest, ProofRequestCreateRequest, ProofRequestGetRequest,
        ProofRequestListRequest, ProofRequestUpdateRequest,
    },
    util::backoff_retry,
};
use backoff::{ExponentialBackoff, ExponentialBackoffBuilder};
use eyre::Result;
use std::future::Future;
use std::time::Duration;
use tonic::transport::{Channel, Endpoint};
use tonic::{Response, Status};

/// Per-attempt cap. `backoff_retry` only counts time *between* attempts, so a hung call (e.g. a DB
/// connection killed mid-restart) never retries — it blocks until the 60s channel timeout. Capping
/// each attempt turns the hang into a fast, retryable failure. 5s sits above a normal call (~ms) and
/// under the 10s backoff budget, so it fires only on a real stall yet still leaves room to retry.
const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);

/// One gRPC attempt under [`ATTEMPT_TIMEOUT`], body unwrapped. A timeout becomes a transient
/// `DeadlineExceeded` so `backoff_retry` re-issues it.
async fn with_timeout<T>(
    fut: impl Future<Output = Result<Response<T>, Status>>,
) -> Result<T, Status> {
    match tokio::time::timeout(ATTEMPT_TIMEOUT, fut).await {
        Ok(resp) => resp.map(|r| r.into_inner()),
        Err(_) => Err(Status::deadline_exceeded("cluster API attempt timed out")),
    }
}

pub async fn reconnect_with_backoff(addr: &str) -> Result<Channel> {
    let backoff = ExponentialBackoffBuilder::new()
        .with_initial_interval(Duration::from_millis(100))
        .with_max_interval(Duration::from_secs(4))
        .with_max_elapsed_time(None)
        .build();

    let op = || async {
        tracing::info!("connecting to {}", addr);
        let mut builder = Endpoint::from_shared(addr.to_string())
            .map_err(|e| backoff::Error::Permanent(eyre::eyre!(e)))?
            .keep_alive_while_idle(true)
            .http2_keep_alive_interval(Duration::from_secs(15))
            .keep_alive_timeout(Duration::from_secs(60));

        let _ = rustls::crypto::ring::default_provider().install_default();

        use tonic::transport::ClientTlsConfig;
        if addr.starts_with("https://") {
            builder = builder
                .tls_config(
                    ClientTlsConfig::new().with_enabled_roots(), // .with_native_roots(),
                )
                .unwrap();
        }

        let channel = builder
            .tcp_keepalive(Some(Duration::from_secs(15)))
            .timeout(Duration::from_secs(60))
            .connect()
            .await
            .map_err(|e| {
                tracing::warn!("Failed to connect: {:?}", e);
                backoff::Error::transient(eyre::eyre!(e))
            })?;
        Ok(channel)
    };

    backoff::future::retry(backoff, op).await
}

#[derive(Clone)]
pub struct ClusterServiceClient {
    pub rpc: InnerClusterClient<Channel>,
    pub backoff: ExponentialBackoff,
}

impl ClusterServiceClient {
    pub async fn new(addr: String) -> Result<Self> {
        let backoff = ExponentialBackoffBuilder::new()
            .with_initial_interval(Duration::from_millis(100))
            .with_max_elapsed_time(Some(Duration::from_secs(10)))
            .build();
        let channel = reconnect_with_backoff(&addr).await?;
        let rpc = InnerClusterClient::new(channel.clone());
        Ok(Self { rpc, backoff })
    }

    /// Shared call policy: retry transient failures within the backoff budget, each attempt bounded
    /// by [`ATTEMPT_TIMEOUT`]. `make_call` builds one fresh attempt per try (cloning client/request).
    async fn retry_call<T, Fut>(&self, make_call: impl Fn() -> Fut) -> Result<T>
    where
        Fut: Future<Output = Result<Response<T>, Status>>,
    {
        Ok(backoff_retry(self.backoff.clone(), || with_timeout(make_call())).await?)
    }

    pub async fn create_proof_request(&self, request: ProofRequestCreateRequest) -> Result<()> {
        self.retry_call(|| {
            let mut client = self.rpc.clone();
            let request = request.clone();
            async move { client.proof_request_create(request).await }
        })
        .await?;
        Ok(())
    }

    pub async fn cancel_proof_request(&self, request: ProofRequestCancelRequest) -> Result<()> {
        self.retry_call(|| {
            let mut client = self.rpc.clone();
            let request = request.clone();
            async move { client.proof_request_cancel(request).await }
        })
        .await?;
        Ok(())
    }

    pub async fn get_proof_requests(
        &self,
        request: ProofRequestListRequest,
    ) -> Result<Vec<proto::ProofRequest>> {
        let result = self
            .retry_call(|| {
                let mut client = self.rpc.clone();
                let request = request.clone();
                async move { client.proof_request_list(request).await }
            })
            .await?;
        Ok(result.proof_requests)
    }

    /// Fetches all rows that match the filters in `request`. The server caps
    /// one call at 1000 rows, so this walks `offset` page by page. It ignores
    /// any `limit` or `offset` set on `request`.
    ///
    /// Rows that change state during the walk shift page boundaries. This
    /// function removes the resulting duplicates. It can also miss a row
    /// for one call, so treat absence as transient.
    pub async fn get_all_proof_requests(
        &self,
        request: ProofRequestListRequest,
    ) -> Result<Vec<proto::ProofRequest>> {
        let rows = drain_pages(PROOF_REQUEST_PAGE_SIZE, |offset| {
            let mut request = request.clone();
            request.limit = Some(PROOF_REQUEST_PAGE_SIZE);
            request.offset = Some(offset);
            self.get_proof_requests(request)
        })
        .await?;
        Ok(dedup_by_id(rows))
    }

    pub async fn update_proof_request(&self, request: ProofRequestUpdateRequest) -> Result<()> {
        self.retry_call(|| {
            let mut client = self.rpc.clone();
            let request = request.clone();
            async move { client.proof_request_update(request).await }
        })
        .await?;
        Ok(())
    }

    pub async fn get_proof_request(
        &self,
        request: ProofRequestGetRequest,
    ) -> Result<Option<proto::ProofRequest>> {
        let result = self
            .retry_call(|| {
                let mut client = self.rpc.clone();
                let request = request.clone();
                async move { client.proof_request_get(request).await }
            })
            .await?;
        Ok(result.proof_request)
    }

    /// Replace the API's cluster component build manifest (full snapshot). Called
    /// periodically by the coordinator with its own build + one entry per connected
    /// worker, plus the GPU capacity snapshot from the same state read.
    ///
    /// `capacity` is `None` if the coordinator has no capacity data. The API stores it with
    /// the manifest under the same `updated_at`.
    pub async fn set_cluster_component_info(
        &self,
        components: Vec<proto::ClusterComponentInfo>,
        capacity: Option<proto::ClusterCapacitySnapshot>,
    ) -> Result<()> {
        self.retry_call(|| {
            let mut client = self.rpc.clone();
            let request = proto::SetClusterComponentInfoRequest {
                components: components.clone(),
                capacity: capacity.clone(),
            };
            async move { client.set_cluster_component_info(request).await }
        })
        .await?;
        Ok(())
    }

    /// Fetch the latest cluster component build manifest the coordinator pushed to
    /// the API, with the capacity snapshot from the same push.
    /// `updated_at == 0` means no coordinator has pushed one yet.
    pub async fn get_cluster_component_info(&self) -> Result<proto::ClusterComponentManifest> {
        self.retry_call(|| {
            let mut client = self.rpc.clone();
            async move { client.get_cluster_component_info(()).await }
        })
        .await
    }
}

/// Matches the server-side cap on `limit`, so each fetch is one full page.
const PROOF_REQUEST_PAGE_SIZE: u32 = 1000;

/// Stops a runaway walk, for example against a server that ignores `offset`,
/// and bounds memory at 100k rows. A healthy live set is far smaller.
const MAX_PAGES: u32 = 100;

const PAGE_WARN_THRESHOLD: u32 = 10;

/// Collects pages from `fetch(offset)` until a short page marks the end.
/// At the page cap it logs an error and returns the rows it has, so callers
/// keep partial progress. Rows past the cap stay invisible until the match
/// set shrinks, so treat absence as transient.
async fn drain_pages<T, F, Fut>(page_size: u32, fetch: F) -> Result<Vec<T>>
where
    F: Fn(u32) -> Fut,
    Fut: Future<Output = Result<Vec<T>>>,
{
    let mut all = Vec::new();
    for page_idx in 0..MAX_PAGES {
        let page = fetch(page_idx * page_size).await?;
        let full_page = page.len() as u32 == page_size;
        all.extend(page);
        if !full_page {
            return Ok(all);
        }
        if page_idx + 1 == PAGE_WARN_THRESHOLD {
            tracing::warn!(
                "proof request listing past {PAGE_WARN_THRESHOLD} pages of {page_size}; \
                 match set is abnormally large"
            );
        }
    }
    tracing::error!(
        "proof request listing exceeded {MAX_PAGES} pages of {page_size}; \
         returning a partial set (is the server ignoring offset?)"
    );
    Ok(all)
}

/// Drops rows already seen and keeps the first occurrence. Pages are separate
/// DB reads, so a row that changes state during the walk can appear twice.
fn dedup_by_id(rows: Vec<proto::ProofRequest>) -> Vec<proto::ProofRequest> {
    let mut seen = std::collections::HashSet::new();
    rows.into_iter()
        .filter(|row| seen.insert(row.id.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    type RecordedOffsets = std::sync::Arc<std::sync::Mutex<Vec<u32>>>;

    /// `fetch` serving `total` distinct items in pages, plus the offsets it was asked for.
    fn counting_fetch(
        total: u32,
    ) -> (
        impl Fn(u32) -> std::future::Ready<Result<Vec<u32>>>,
        RecordedOffsets,
    ) {
        let offsets = RecordedOffsets::default();
        let recorder = offsets.clone();
        let fetch = move |offset| {
            recorder.lock().unwrap().push(offset);
            let end = (offset + PROOF_REQUEST_PAGE_SIZE).min(total);
            std::future::ready(Ok((offset..end).collect()))
        };
        (fetch, offsets)
    }

    #[tokio::test]
    async fn drains_across_pages_without_loss_or_duplication() {
        // 2.5 pages: the regression shape — rows past the first page must still arrive.
        let total = PROOF_REQUEST_PAGE_SIZE * 2 + 500;
        let (fetch, offsets) = counting_fetch(total);

        let all = drain_pages(PROOF_REQUEST_PAGE_SIZE, fetch).await.unwrap();

        assert_eq!(all, (0..total).collect::<Vec<_>>());
        assert_eq!(*offsets.lock().unwrap(), vec![0, 1000, 2000]);
    }

    #[tokio::test]
    async fn an_exact_page_boundary_costs_one_empty_confirmation_fetch() {
        let (fetch, offsets) = counting_fetch(PROOF_REQUEST_PAGE_SIZE);

        let all = drain_pages(PROOF_REQUEST_PAGE_SIZE, fetch).await.unwrap();

        assert_eq!(all.len(), PROOF_REQUEST_PAGE_SIZE as usize);
        assert_eq!(*offsets.lock().unwrap(), vec![0, 1000]);
    }

    #[tokio::test]
    async fn hitting_the_page_cap_returns_a_partial_set() {
        // Always a full page regardless of offset.
        let all = drain_pages(PROOF_REQUEST_PAGE_SIZE, |_offset| {
            std::future::ready(Ok(vec![0u32; PROOF_REQUEST_PAGE_SIZE as usize]))
        })
        .await
        .unwrap();

        assert_eq!(all.len(), (MAX_PAGES * PROOF_REQUEST_PAGE_SIZE) as usize);
    }

    /// Filtered-churn shape: a row completing mid-fetch re-serves the boundary
    /// row on the next page; the duplicate must not reach consumers that act
    /// once per id (e.g. network submission).
    #[test]
    fn duplicate_rows_across_page_boundaries_collapse() {
        let row = |id: &str| proto::ProofRequest {
            id: id.into(),
            ..Default::default()
        };

        let deduped = dedup_by_id(vec![row("a"), row("b"), row("b"), row("c")]);

        let ids: Vec<_> = deduped.iter().map(|r| r.id.as_str()).collect();
        assert_eq!(ids, ["a", "b", "c"]);
    }

    #[tokio::test]
    async fn a_page_fetch_error_propagates() {
        let err = drain_pages(PROOF_REQUEST_PAGE_SIZE, |_offset| {
            std::future::ready(Err::<Vec<u32>, _>(eyre::eyre!("boom")))
        })
        .await
        .unwrap_err();

        assert_eq!(err.to_string(), "boom");
    }
}
