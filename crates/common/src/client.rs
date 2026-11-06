use crate::auth::{AuthChannel, BearerInterceptor};
use crate::{
    proto::{
        self, cluster_service_client::ClusterServiceClient as InnerClusterClient,
        ProofRequestCancelRequest, ProofRequestCreateRequest, ProofRequestGetRequest,
        ProofRequestListRequest, ProofRequestStatus, ProofRequestUpdateRequest,
    },
    util::backoff_retry,
};
use backoff::{ExponentialBackoff, ExponentialBackoffBuilder};
use eyre::Result;
use futures::{stream, StreamExt};
use std::collections::HashSet;
use std::future::Future;
use std::time::Duration;
use tonic::transport::{Channel, Endpoint};
use tonic::{Response, Status};

/// Per-attempt cap. `backoff_retry` only counts time *between* attempts, so a hung call (e.g. a DB
/// connection killed mid-restart) never retries — it blocks until the 60s channel timeout. Capping
/// each attempt turns the hang into a fast, retryable failure. 5s sits above a normal call (~ms) and
/// under the 10s backoff budget, so it fires only on a real stall yet still leaves room to retry.
const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);

const STATUS_LOOKUP_FAN_OUT: usize = 8;

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
    pub rpc: InnerClusterClient<AuthChannel>,
    pub backoff: ExponentialBackoff,
}

impl ClusterServiceClient {
    pub async fn new(addr: String, token: Option<String>) -> Result<Self> {
        let backoff = ExponentialBackoffBuilder::new()
            .with_initial_interval(Duration::from_millis(100))
            .with_max_elapsed_time(Some(Duration::from_secs(10)))
            .build();
        let channel = reconnect_with_backoff(&addr).await?;
        let rpc = InnerClusterClient::with_interceptor(
            channel,
            BearerInterceptor::new(token.as_deref())?,
        );
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
        Ok(self.get_proof_request_pages(request).await?.items)
    }

    async fn get_proof_request_pages(
        &self,
        request: ProofRequestListRequest,
    ) -> Result<PageResult<proto::ProofRequest>> {
        let mut listed = drain_pages(PROOF_REQUEST_PAGE_SIZE, MAX_PAGES, |offset| {
            let mut request = request.clone();
            request.limit = Some(PROOF_REQUEST_PAGE_SIZE);
            request.offset = Some(offset);
            self.get_proof_requests(request)
        })
        .await?;
        listed.items = dedup_by_id(listed.items);
        Ok(listed)
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

    pub async fn get_pending_proof_request_ids(
        &self,
        proof_ids: &[String],
        minimum_deadline: u64,
    ) -> Result<HashSet<String>> {
        if proof_ids.is_empty() {
            return Ok(HashSet::new());
        }
        let listed = self
            .get_proof_request_pages(ProofRequestListRequest {
                proof_status: vec![ProofRequestStatus::Pending.into()],
                minimum_deadline: Some(minimum_deadline),
                ..Default::default()
            })
            .await?;
        pending_request_ids(listed, proof_ids, |proof_id| {
            self.get_proof_request(ProofRequestGetRequest { proof_id })
        })
        .await
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

async fn pending_request_ids<F, Fut>(
    listed: PageResult<proto::ProofRequest>,
    proof_ids: &[String],
    fetch: F,
) -> Result<HashSet<String>>
where
    F: Fn(String) -> Fut,
    Fut: Future<Output = Result<Option<proto::ProofRequest>>>,
{
    let candidates: HashSet<_> = proof_ids.iter().collect();
    let mut pending: HashSet<_> = listed
        .items
        .into_iter()
        .filter(|request| candidates.contains(&request.id))
        .map(|request| request.id)
        .collect();
    if listed.is_complete {
        return Ok(pending);
    }

    // A capped listing can miss Pending requests, so read each missing candidate by ID.
    let missing: Vec<_> = proof_ids
        .iter()
        .filter(|id| !pending.contains(*id))
        .cloned()
        .collect();
    let mut checks =
        stream::iter(missing.into_iter().map(fetch)).buffer_unordered(STATUS_LOOKUP_FAN_OUT);
    while let Some(result) = checks.next().await {
        if let Some(request) = result? {
            if request.proof_status == ProofRequestStatus::Pending as i32 {
                pending.insert(request.id);
            }
        }
    }
    Ok(pending)
}

#[derive(Debug)]
pub struct PageResult<T> {
    pub items: Vec<T>,
    pub is_complete: bool,
}

/// Matches the server-side cap on `limit`, so each fetch is one full page.
const PROOF_REQUEST_PAGE_SIZE: u32 = 1000;

/// Stops a runaway walk, for example against a server that ignores `offset`,
/// and bounds memory at 100k rows. A healthy live set is far smaller.
const MAX_PAGES: u32 = 100;

const PAGE_WARN_THRESHOLD: u32 = 10;

pub async fn drain_pages<T, E, F, Fut>(
    page_size: u32,
    max_pages: u32,
    fetch: F,
) -> std::result::Result<PageResult<T>, E>
where
    F: Fn(u32) -> Fut,
    Fut: Future<Output = std::result::Result<Vec<T>, E>>,
{
    let mut all = Vec::new();
    for page_idx in 0..max_pages {
        let page = fetch(page_idx * page_size).await?;
        let full_page = page.len() as u32 == page_size;
        all.extend(page);
        if !full_page {
            return Ok(PageResult {
                items: all,
                is_complete: true,
            });
        }
        if page_idx + 1 == PAGE_WARN_THRESHOLD {
            tracing::warn!(
                "proof request listing past {PAGE_WARN_THRESHOLD} pages of {page_size}; \
                 match set is abnormally large"
            );
        }
    }
    tracing::error!(
        "proof request listing reached {max_pages} full pages of {page_size}; \
         returning an incomplete listing"
    );
    Ok(PageResult {
        items: all,
        is_complete: false,
    })
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

        let all = drain_pages(PROOF_REQUEST_PAGE_SIZE, MAX_PAGES, fetch)
            .await
            .unwrap();

        assert!(all.is_complete);
        assert_eq!(all.items, (0..total).collect::<Vec<_>>());
        assert_eq!(*offsets.lock().unwrap(), vec![0, 1000, 2000]);
    }

    #[tokio::test]
    async fn an_exact_page_boundary_costs_one_empty_confirmation_fetch() {
        let (fetch, offsets) = counting_fetch(PROOF_REQUEST_PAGE_SIZE);

        let all = drain_pages(PROOF_REQUEST_PAGE_SIZE, MAX_PAGES, fetch)
            .await
            .unwrap();

        assert!(all.is_complete);
        assert_eq!(all.items.len(), PROOF_REQUEST_PAGE_SIZE as usize);
        assert_eq!(*offsets.lock().unwrap(), vec![0, 1000]);
    }

    #[tokio::test]
    async fn hitting_the_page_cap_returns_a_partial_set() {
        // Always a full page regardless of offset.
        let all = drain_pages(PROOF_REQUEST_PAGE_SIZE, MAX_PAGES, |_offset| {
            std::future::ready(Ok::<_, eyre::Report>(vec![
                0u32;
                PROOF_REQUEST_PAGE_SIZE as usize
            ]))
        })
        .await
        .unwrap();

        assert!(!all.is_complete);
        assert_eq!(
            all.items.len(),
            (MAX_PAGES * PROOF_REQUEST_PAGE_SIZE) as usize
        );
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
        let err = drain_pages(PROOF_REQUEST_PAGE_SIZE, MAX_PAGES, |_offset| {
            std::future::ready(Err::<Vec<u32>, _>(eyre::eyre!("boom")))
        })
        .await
        .unwrap_err();

        assert_eq!(err.to_string(), "boom");
    }

    fn proof_request(id: impl Into<String>, status: ProofRequestStatus) -> proto::ProofRequest {
        proto::ProofRequest {
            id: id.into(),
            proof_status: status as i32,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn complete_pending_list_needs_no_individual_reads() {
        let listed = PageResult {
            items: vec![
                proof_request("pending", ProofRequestStatus::Pending),
                proof_request("unrelated", ProofRequestStatus::Pending),
            ],
            is_complete: true,
        };
        let ids = ["pending".into(), "absent".into()];
        let pending = pending_request_ids(listed, &ids, |_| async {
            panic!("a complete listing needs no individual reads")
        })
        .await
        .unwrap();
        assert_eq!(pending, HashSet::from(["pending".into()]));
    }

    #[tokio::test]
    async fn partial_pending_list_checks_missing_candidates() {
        let listed = PageResult {
            items: vec![proof_request("listed", ProofRequestStatus::Pending)],
            is_complete: false,
        };
        let ids: Vec<String> = [
            "listed",
            "pending",
            "completed",
            "failed",
            "cancelled",
            "missing",
        ]
        .into_iter()
        .map(String::from)
        .collect();
        let reads = std::cell::RefCell::new(HashSet::new());
        let pending = pending_request_ids(listed, &ids, |id| {
            assert!(reads.borrow_mut().insert(id.clone()));
            let status = match id.as_str() {
                "pending" => Some(ProofRequestStatus::Pending),
                "completed" => Some(ProofRequestStatus::Completed),
                "failed" => Some(ProofRequestStatus::Failed),
                "cancelled" => Some(ProofRequestStatus::Cancelled),
                "missing" => None,
                _ => panic!("listed requests need no individual read"),
            };
            std::future::ready(Ok(status.map(|status| proof_request(id, status))))
        })
        .await
        .unwrap();
        assert_eq!(pending, HashSet::from(["listed".into(), "pending".into()]));
        assert_eq!(reads.into_inner(), ids[1..].iter().cloned().collect());
    }
}
