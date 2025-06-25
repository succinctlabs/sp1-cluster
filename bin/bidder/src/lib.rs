use std::time::Duration;

use crate::metrics::BidderMetrics;
use alloy::primitives::Address;
use alloy_signer_local::PrivateKeySigner;
use anyhow::{Context, Result};
use futures::future::join_all;
use rand::Rng;
use tokio::time::sleep;
use tonic::transport::Channel;
use tracing::{error, info, instrument};

use spn_network_types::{
    prover_network_client::ProverNetworkClient, BidRequest, BidRequestBody, FulfillmentStatus,
    GetNonceRequest, GetOwnerRequest, MessageFormat, Signable, TransactionVariant,
};
use spn_utils::time_now;

pub mod config;
pub mod grpc;
pub mod metrics;

/// How long to wait between checking if there are any requested proofs to bid on.
///
/// Lower values will result in faster E2E latency for the user, but more outgoing requests to the
/// network.
const REFRESH_INTERVAL_SEC: u64 = 3;

/// The maximum number of requests to handle in a single refresh loop.
const REQUEST_LIMIT: u32 = 100;

#[derive(Clone)]
pub struct Bidder {
    network: ProverNetworkClient<Channel>,
    version: String,
    signer: PrivateKeySigner,
    metrics: BidderMetrics,
    domain_bytes: Vec<u8>,
}

impl Bidder {
    pub fn new(
        network: ProverNetworkClient<Channel>,
        version: String,
        signer: PrivateKeySigner,
        metrics: BidderMetrics,
        domain_bytes: Vec<u8>,
    ) -> Self {
        Self {
            network,
            version,
            signer,
            metrics,
            domain_bytes,
        }
    }

    /// Runs the bidder loop.
    pub async fn run(self) -> Result<()> {
        info!("starting the bidder");

        // Get the prover.
        let prover_bytes = self
            .network
            .clone()
            .get_owner(GetOwnerRequest {
                address: self.signer.address().to_vec(),
            })
            .await?
            .into_inner()
            .owner;
        let prover = Address::from_slice(&prover_bytes);

        loop {
            // Check for requests to bid on.
            if let Err(e) = self.bid_requests(prover).await {
                error!("bidding on requests: {:?}", e);
                self.metrics.main_loop_errors.increment(1);
            }
            // Wait for the next interval.
            sleep(Duration::from_secs(REFRESH_INTERVAL_SEC)).await;
        }
    }

    /// Checks for requested proof requests that are in the network and
    #[instrument(skip_all)]
    async fn bid_requests(&self, prover: Address) -> Result<()> {
        // Get all requested unexecutable requests from the network that are assigned to our
        // address.
        let request = spn_network_types::GetFilteredProofRequestsRequest {
            version: Some(self.version.clone()),
            fulfillment_status: Some(FulfillmentStatus::Requested.into()),
            execution_status: None,
            execute_fail_cause: None,
            minimum_deadline: Some(time_now()),
            vk_hash: None,
            requester: None,
            fulfiller: None,
            limit: Some(REQUEST_LIMIT),
            page: None,
            from: None,
            to: None,
            mode: None,
            not_bid_by: Some(prover.to_vec()),
            settlement_status: None,
        };
        let network_requests_resp = self
            .network
            .clone()
            .get_filtered_proof_requests(request)
            .await?;
        let requests = network_requests_resp.into_inner().requests;
        self.metrics.biddable_requests.set(requests.len() as f64);

        if requests.is_empty() {
            info!("found no biddable requests");
            return Ok(());
        }
        info!("found {} biddable requests", requests.len());

        let failure_tasks = requests.into_iter().map(|request| {
            let self_clone = self.clone();
            let request_id = hex::encode(request.request_id);

            tokio::spawn(async move {
                if request.cycle_limit > 10_000_000 {
                    info!(
                        "skipping request 0x{} with cycle limit {}",
                        request_id, request.cycle_limit
                    );
                    return;
                }
                match self_clone.bid_request(prover, &request_id).await {
                    Ok(_) => {
                        info!("bid on request 0x{}", request_id);
                        self_clone.metrics.requests_bid.increment(1);
                        self_clone.metrics.total_requests_processed.increment(1);
                    }
                    Err(e) => {
                        error!("failed to bid on request 0x{}: {:?}", request_id, e);
                        self_clone.metrics.request_bid_failures.increment(1);
                    }
                }
            })
        });

        join_all(failure_tasks).await;

        Ok(())
    }

    async fn bid_request(&self, prover: Address, request_id: &str) -> Result<()> {
        // Send the bid request to the network.
        let nonce = self
            .network
            .clone()
            .get_nonce(GetNonceRequest {
                address: self.signer.address().to_vec(),
            })
            .await?
            .into_inner()
            .nonce;
        // Generate a random bid amount from 90-110.
        let body = BidRequestBody {
            nonce,
            request_id: hex::decode(request_id).context("failed to decode request_id")?,
            amount: "1".to_string(),
            domain: self.domain_bytes.clone(),
            prover: prover.to_vec(),
            variant: TransactionVariant::BidVariant.into(),
        };
        let bid_request = BidRequest {
            format: MessageFormat::Binary.into(),
            signature: body.sign(&self.signer).into(),
            body: Some(body),
        };
        self.network.clone().bid(bid_request).await?;

        Ok(())
    }
}
