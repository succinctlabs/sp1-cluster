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
use std::time::Duration;
use tonic::transport::{Channel, Endpoint};

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

        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        use tonic::transport::ClientTlsConfig;
        if addr.starts_with("https://") {
            builder = builder
                .tls_config(
                    ClientTlsConfig::new()
                        .with_enabled_roots()
                        .with_native_roots(),
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

    pub async fn create_proof_request(&self, request: ProofRequestCreateRequest) -> Result<()> {
        backoff_retry(self.backoff.clone(), || async {
            let response = self.rpc.clone().proof_request_create(request.clone()).await;
            response.map(|response| response.into_inner())
        })
        .await?;
        Ok(())
    }

    pub async fn cancel_proof_request(&self, request: ProofRequestCancelRequest) -> Result<()> {
        backoff_retry(self.backoff.clone(), || async {
            let response = self.rpc.clone().proof_request_cancel(request.clone()).await;
            response.map(|response| response.into_inner())
        })
        .await?;
        Ok(())
    }

    pub async fn get_proof_requests(
        &self,
        request: ProofRequestListRequest,
    ) -> Result<Vec<proto::ProofRequest>> {
        let result = backoff_retry(self.backoff.clone(), || async {
            let response = self.rpc.clone().proof_request_list(request.clone()).await;
            response.map(|response| response.into_inner().proof_requests)
        })
        .await?;
        Ok(result)
    }

    pub async fn update_proof_request(&self, request: ProofRequestUpdateRequest) -> Result<()> {
        backoff_retry(self.backoff.clone(), || async {
            let response = self.rpc.clone().proof_request_update(request.clone()).await;
            response.map(|response| response.into_inner())
        })
        .await?;
        Ok(())
    }

    pub async fn get_proof_request(
        &self,
        request: ProofRequestGetRequest,
    ) -> Result<Option<proto::ProofRequest>> {
        let result = backoff_retry(self.backoff.clone(), || async {
            let response = self.rpc.clone().proof_request_get(request.clone()).await;
            response.map(|response| response.into_inner().proof_request)
        })
        .await?;
        Ok(result)
    }
}
