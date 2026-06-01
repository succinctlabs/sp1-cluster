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
}
