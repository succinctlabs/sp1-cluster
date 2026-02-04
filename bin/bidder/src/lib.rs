use std::time::Duration;

use crate::metrics::BidderMetrics;
use alloy::primitives::{Address, U256};
use alloy_signer_local::PrivateKeySigner;
use anyhow::{Context, Result};
use futures::future::join_all;
use tokio::time::sleep;
use tonic::transport::Channel;
use tracing::{error, info, instrument};

use spn_network_types::{
    prover_network_client::ProverNetworkClient, BidRequest, BidRequestBody, FulfillmentStatus,
    GetNonceRequest, GetOwnerRequest, MessageFormat, ProofMode, Signable, TransactionVariant,
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

// Per-instance configurable safety buffers are provided via Settings

/// The maximum number of requests to handle in a single refresh loop.
const REQUEST_LIMIT: u32 = 100;

#[derive(Clone)]
pub struct Bidder {
    network: ProverNetworkClient<Channel>,
    version: String,
    signer: PrivateKeySigner,
    metrics: BidderMetrics,
    domain_bytes: Vec<u8>,
    /// Total cluster throughput in million gas per second
    throughput_mgas: f64,
    /// Maximum number of concurrent proofs the cluster can handle
    max_concurrent_proofs: u32,
    /// Token bid amount per PGU in wei
    bid_amount: U256,
    /// Base safety buffer in seconds applied to all proofs
    buffer_sec: u64,
    /// Additional buffer for Groth16 proofs in seconds
    groth16_buffer_sec: u64,
    /// Additional buffer for Plonk proofs in seconds
    plonk_buffer_sec: u64,
    /// Whether to bid on Groth16 proofs
    groth16_enabled: bool,
    /// Whether to bid on Plonk proofs
    plonk_enabled: bool,
    /// Aggressive mode: bid on all requests without capacity/time checks
    aggressive_mode: bool,
    /// Minimum deadline in seconds to bid on (optional safety check, even in aggressive mode)
    min_deadline_secs: Option<u64>,
}

impl Bidder {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        network: ProverNetworkClient<Channel>,
        version: String,
        signer: PrivateKeySigner,
        metrics: BidderMetrics,
        domain_bytes: Vec<u8>,
        throughput_mgas: f64,
        max_concurrent_proofs: u32,
        bid_amount: U256,
        buffer_sec: u64,
        groth16_buffer_sec: u64,
        plonk_buffer_sec: u64,
        groth16_enabled: bool,
        plonk_enabled: bool,
        aggressive_mode: bool,
        min_deadline_secs: Option<u64>,
    ) -> Self {
        Self {
            network,
            version,
            signer,
            metrics,
            domain_bytes,
            throughput_mgas,
            max_concurrent_proofs,
            bid_amount,
            buffer_sec,
            groth16_buffer_sec,
            plonk_buffer_sec,
            groth16_enabled,
            plonk_enabled,
            aggressive_mode,
            min_deadline_secs,
        }
    }

    /// Calculate if we can fulfill a proof request within its deadline
    fn can_fulfill_proof(
        &self,
        active_proofs: u32,
        gas_limit: u64,
        deadline_secs: u64,
        mode: ProofMode,
    ) -> bool {
        // If the proof mode is disabled, we cannot fulfill it
        match mode {
            ProofMode::Groth16 if !self.groth16_enabled => return false,
            ProofMode::Plonk if !self.plonk_enabled => return false,
            _ => {}
        }
        // Calculate effective throughput per proof when at max capacity
        let effective_throughput = self.throughput_mgas / self.max_concurrent_proofs as f64;

        // Calculate time needed to complete this proof (in seconds)
        let completion_time_secs = (gas_limit as f64 / 1_000_000.0) / effective_throughput;

        // Add buffers for safety
        let mut total_time_needed = completion_time_secs + self.buffer_sec as f64;

        match mode {
            ProofMode::Groth16 => {
                total_time_needed += self.groth16_buffer_sec as f64;
            }
            ProofMode::Plonk => {
                total_time_needed += self.plonk_buffer_sec as f64;
            }
            _ => {}
        }

        // Check if we have enough time and capacity
        let has_capacity = active_proofs < self.max_concurrent_proofs;
        let has_time = total_time_needed <= deadline_secs as f64;

        has_capacity && has_time
    }

    /// Runs the bidder loop.
    pub async fn run(mut self) -> Result<()> {
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
    async fn bid_requests(&mut self, prover: Address) -> Result<()> {
        // Get all requests from the network that are biddable.
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
            error: None,
            settlement_status: None,
        };
        let network_requests_resp = self
            .network
            .clone()
            .get_filtered_proof_requests(request)
            .await?;
        let requests = network_requests_resp.into_inner().requests;
        self.metrics.biddable_requests.set(requests.len() as f64);

        // Get all requests from the network assigned to our address so we know how many additional
        // requests we can bid on.
        // Note this is done after the biddable requests query to ensure a proof is not ommitted
        // from both queries if it became assigned just after the assigned query.
        let assigned_requests = self
            .network
            .clone()
            .get_filtered_proof_requests(spn_network_types::GetFilteredProofRequestsRequest {
                version: Some(self.version.clone()),
                fulfillment_status: Some(FulfillmentStatus::Assigned.into()),
                execution_status: None,
                execute_fail_cause: None,
                minimum_deadline: Some(time_now()),
                vk_hash: None,
                requester: None,
                fulfiller: Some(prover.to_vec()),
                limit: Some(REQUEST_LIMIT),
                page: None,
                from: None,
                to: None,
                mode: None,
                not_bid_by: None,
                error: None,
                settlement_status: None,
            })
            .await?
            .into_inner()
            .requests;
        let mut active_proofs = assigned_requests.len() as u32;

        if requests.is_empty() {
            info!("found no biddable requests");
            return Ok(());
        }
        info!("found {} biddable requests", requests.len());

        let mut failure_tasks = Vec::new();
        for request in requests {
            let self_clone = self.clone();
            let mode = request.mode();
            let request_id = hex::encode(request.request_id);

            let request_duration = request.deadline.saturating_sub(time_now());

            // In aggressive mode, skip capacity/time checks but optionally enforce min deadline.
            if self.aggressive_mode {
                if let Some(min_deadline) = self.min_deadline_secs {
                    if request_duration < min_deadline {
                        info!(
                            "Skipping request 0x{} with deadline in {}s (below minimum {}s)",
                            request_id, request_duration, min_deadline
                        );
                        continue;
                    }
                }
            } else {
                // Normal mode: check capacity and time constraints.
                if !self.can_fulfill_proof(active_proofs, request.gas_limit, request_duration, mode)
                {
                    info!(
                        "Cannot fulfill request 0x{} with gas limit {} and deadline in {}s",
                        request_id, request.gas_limit, request_duration
                    );
                    continue;
                }
                active_proofs += 1;
            }
            failure_tasks.push(tokio::spawn(async move {
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
            }));
        }

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
        let amount = self.bid_amount;
        let body = BidRequestBody {
            nonce,
            request_id: hex::decode(request_id).context("failed to decode request_id")?,
            amount: amount.to_string(),
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
