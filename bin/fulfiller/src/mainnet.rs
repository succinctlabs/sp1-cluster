use alloy_primitives::Address;
use anyhow::Result;
use async_trait::async_trait;
use futures::future::join_all;
use prost::Message;
use sp1_cluster_artifact::ArtifactType;
use sp1_cluster_common::{
    client::{drain_pages, PageResult},
    proto::ProofRequest,
};
use sp1_cluster_fulfillment::{
    network::{CancelableKind, FulfillmentNetwork, NetworkRequest},
    prover_info::build_report_prover_info_body,
    request_error_from_extra_data,
};
use sp1_sdk::network::signer::NetworkSigner;
use spn_artifacts::Artifact;
use spn_network_types::error_trace::ErrorTrace;

/// SPN (SP1 Prover Network) implementation of FulfillmentNetwork.
#[derive(Clone)]
pub struct MainnetFulfiller {
    network:
        spn_network_types::prover_network_client::ProverNetworkClient<tonic::transport::Channel>,
}

impl MainnetFulfiller {
    pub fn new(
        network: spn_network_types::prover_network_client::ProverNetworkClient<
            tonic::transport::Channel,
        >,
    ) -> Self {
        Self { network }
    }
}

#[async_trait]
impl FulfillmentNetwork for MainnetFulfiller {
    type NetworkRequest = NetworkProofRequest;

    async fn init(&self, signer: &NetworkSigner) -> Result<Address> {
        let prover_bytes = self
            .network
            .clone()
            .get_owner(spn_network_types::GetOwnerRequest {
                address: signer.address().to_vec(),
            })
            .await?
            .into_inner()
            .owner;
        Ok(Address::from_slice(&prover_bytes))
    }

    async fn report_prover_info(
        &self,
        domain: &[u8],
        prover: Address,
        components: Vec<spn_network_types::ComponentInfo>,
        capacity: Option<spn_network_types::ClusterCapacitySnapshot>,
        signer: &NetworkSigner,
    ) -> Result<()> {
        // ReportProverInfo carries no nonce and writes no ledger
        // tx — it is signed, non-ledger telemetry: the body is signed and the
        // receiver verifies the signer, binding the report to the signer-resolved
        // prover. Build + sign the body exactly like fulfill_proof, minus GetNonce.
        // The signature covers the encoded body, so it covers the capacity snapshot too.
        let body = build_report_prover_info_body(domain, prover.as_slice(), components, capacity);

        let request = spn_network_types::ReportProverInfoRequest {
            format: spn_network_types::MessageFormat::Binary.into(),
            signature: signer.sign_message(&body.encode_to_vec()).await?.into(),
            body: Some(body),
        };

        self.network.clone().report_prover_info(request).await?;
        Ok(())
    }

    async fn submit_request(
        &self,
        request: &ProofRequest,
        proof_bytes: Option<Vec<u8>>,
        domain: &[u8],
        signer: &NetworkSigner,
    ) -> Result<()> {
        let nonce = self
            .network
            .clone()
            .get_nonce(spn_network_types::GetNonceRequest {
                address: signer.address().to_vec(),
            })
            .await?
            .into_inner()
            .nonce;

        let request_id_bytes = hex::decode(&request.id)
            .map_err(|e| anyhow::anyhow!("failed to decode request_id: {}", e))?;

        let body = spn_network_types::FulfillProofRequestBody {
            nonce,
            request_id: request_id_bytes,
            proof: proof_bytes.ok_or(anyhow::anyhow!("no proof bytes"))?,
            reserved_metadata: None,
            domain: domain.to_vec(),
            variant: spn_network_types::TransactionVariant::FulfillVariant.into(),
        };

        let fulfill_request = spn_network_types::FulfillProofRequest {
            format: spn_network_types::MessageFormat::Binary.into(),
            signature: signer.sign_message(&body.encode_to_vec()).await?.into(),
            body: Some(body),
        };

        self.network.clone().fulfill_proof(fulfill_request).await?;
        Ok(())
    }

    async fn fail_request(
        &self,
        request: &ProofRequest,
        _domain: &[u8],
        signer: &NetworkSigner,
    ) -> Result<()> {
        let error = request_error_from_extra_data(request.extra_data.as_deref());
        // Build a bounded, sanitized structured trace from the cluster's failure
        // `extra_data` so the network records *why* proving failed instead of only
        // the coarse `ProofRequestError` enum. Useless/empty extra_data yields
        // `None` (the network stores NULL rather than a junk trace).
        let error_trace = ErrorTrace::from_cluster_extra_data(request.extra_data.as_deref())
            .map(|t| t.with_request_id(request.id.clone()))
            .and_then(|t| t.finalize())
            .and_then(|t| t.to_wire_bytes());
        self.fail_request_with_error(&request.id, error, error_trace, &[], signer)
            .await
    }

    async fn cancel_request(&self, request_id: &str, signer: &NetworkSigner) -> Result<()> {
        // A cancellation carries no failure detail; send no trace.
        self.fail_request_with_error(request_id, None, None, &[], signer)
            .await
    }

    async fn fail_request_with_error(
        &self,
        request_id: &str,
        error: Option<i32>,
        error_trace: Option<Vec<u8>>,
        _domain: &[u8],
        signer: &NetworkSigner,
    ) -> Result<()> {
        let nonce = self
            .network
            .clone()
            .get_nonce(spn_network_types::GetNonceRequest {
                address: signer.address().to_vec(),
            })
            .await?
            .into_inner()
            .nonce;

        let request_id_bytes = hex::decode(request_id)
            .map_err(|e| anyhow::anyhow!("failed to decode request_id: {}", e))?;

        let body = spn_network_types::FailFulfillmentRequestBody {
            nonce,
            request_id: request_id_bytes,
            error,
            error_trace,
        };

        let fail_request = spn_network_types::FailFulfillmentRequest {
            format: spn_network_types::MessageFormat::Binary.into(),
            signature: signer.sign_message(&body.encode_to_vec()).await?.into(),
            body: Some(body),
        };

        if let Err(e) = self.network.clone().fail_fulfillment(fail_request).await {
            match e.code() {
                tonic::Code::PermissionDenied
                | tonic::Code::FailedPrecondition
                | tonic::Code::NotFound
                | tonic::Code::InvalidArgument => {
                    tracing::warn!("Fail fulfillment rejected: {:?}", e);
                }
                _ => {
                    return Err(e.into());
                }
            }
        }
        Ok(())
    }

    async fn get_schedulable_requests(
        &self,
        version: &str,
        fulfiller_addresses: Vec<Vec<u8>>,
        minimum_deadline: u64,
        limit: u32,
    ) -> Result<Vec<Self::NetworkRequest>> {
        let queries: Vec<_> = fulfiller_addresses
            .into_iter()
            .map(
                |address| spn_network_types::GetFilteredProofRequestsRequest {
                    version: Some(version.to_string()),
                    fulfillment_status: Some(spn_network_types::FulfillmentStatus::Assigned.into()),
                    execution_status: None,
                    execute_fail_cause: None,
                    minimum_deadline: Some(minimum_deadline),
                    vk_hash: None,
                    requester: None,
                    fulfiller: Some(address),
                    limit: Some(limit),
                    page: None,
                    from: None,
                    to: None,
                    mode: None,
                    not_bid_by: None,
                    settlement_status: None,
                    error: None,
                },
            )
            .collect();

        let responses = join_all(queries.into_iter().map(|request| {
            let mut network = self.network.clone();
            async move { network.get_filtered_proof_requests(request).await }
        }))
        .await;

        let mut requests = Vec::new();
        for response in responses {
            let response = response?;
            requests.extend(response.into_inner().requests);
        }

        Ok(requests.into_iter().map(NetworkProofRequest).collect())
    }

    async fn get_cancelable_requests(
        &self,
        version: &str,
        fulfiller_addresses: Vec<Vec<u8>>,
        minimum_deadline: u64,
        limit: u32,
    ) -> Result<Vec<(Self::NetworkRequest, CancelableKind)>> {
        // Two terminal shapes the network writes for an assigned request:
        //   1. `Assigned + Unexecutable`     — oracle reported an exec failure; the
        //                                      cluster still owes a `fail_fulfillment`.
        //   2. `Unfulfillable` (any exec)    — network already flipped it terminal
        //                                      (e.g. gas-limit, validation failure).
        // Mainnet never produces `CancelableKind::Fulfilled` candidates: fulfillers
        // are paid per fulfillment, so a fulfilled request is never ours to abort.
        let queries: Vec<_> = fulfiller_addresses
            .into_iter()
            .flat_map(|address| {
                let assigned = spn_network_types::GetFilteredProofRequestsRequest {
                    version: Some(version.to_string()),
                    fulfillment_status: Some(spn_network_types::FulfillmentStatus::Assigned.into()),
                    execution_status: Some(spn_network_types::ExecutionStatus::Unexecutable.into()),
                    minimum_deadline: Some(minimum_deadline),
                    fulfiller: Some(address.clone()),
                    limit: Some(limit),
                    ..Default::default()
                };
                let unfulfillable = spn_network_types::GetFilteredProofRequestsRequest {
                    version: Some(version.to_string()),
                    fulfillment_status: Some(
                        spn_network_types::FulfillmentStatus::Unfulfillable.into(),
                    ),
                    minimum_deadline: Some(minimum_deadline),
                    fulfiller: Some(address),
                    limit: Some(limit),
                    ..Default::default()
                };
                [
                    (assigned, CancelableKind::Unexecutable),
                    (unfulfillable, CancelableKind::Unfulfillable),
                ]
            })
            .collect();

        let max_pages = network_max_pages(limit);
        let responses = join_all(queries.into_iter().map(|(request, kind)| {
            let network = &self.network;
            async move {
                let pages = drain_pages(limit, max_pages, |offset| {
                    let mut network = network.clone();
                    let request = spn_network_types::GetFilteredProofRequestsRequest {
                        page: Some(offset / limit + 1),
                        ..request.clone()
                    };
                    async move {
                        network
                            .get_filtered_proof_requests(request)
                            .await
                            .map(|response| response.into_inner().requests)
                    }
                })
                .await;
                (pages, kind)
            }
        }))
        .await;

        merge_cancelable(responses)
    }

    async fn download_artifact(
        &self,
        id: &str,
        uri: &str,
        artifact_type: ArtifactType,
    ) -> Result<Vec<u8>> {
        let artifact = Artifact {
            id: id.to_string(),
            label: "".to_string(),
            expiry: None,
        };
        let network_artifact_type = (artifact_type as i32).try_into()?;
        let region = std::env::var("FULFILLER_NETWORK_S3_REGION")
            .unwrap_or_else(|_| "us-east-2".to_string());
        let bytes = artifact
            .download_raw_from_uri_par(
                uri,
                &region,
                network_artifact_type,
                Some(
                    std::env::var("FULFILLER_S3_CONCURRENCY")
                        .map(|s| s.parse().unwrap_or(32))
                        .unwrap_or(32),
                ),
            )
            .await?;
        Ok(bytes.into())
    }

    async fn fetch_stdin_uri(
        &self,
        request: &Self::NetworkRequest,
        signer: &NetworkSigner,
    ) -> Result<String> {
        if !request.stdin_private() {
            return Ok(request.stdin_public_uri().to_string());
        }

        let nonce = self
            .network
            .clone()
            .get_nonce(spn_network_types::GetNonceRequest {
                address: signer.address().to_vec(),
            })
            .await?
            .into_inner()
            .nonce;

        let request_id_bytes = hex::decode(request.request_id())
            .map_err(|e| anyhow::anyhow!("failed to decode request_id: {}", e))?;

        let body = spn_network_types::GetStdinUriRequestBody {
            nonce,
            request_id: request_id_bytes,
            domain: Vec::new(),
        };

        let get_stdin_uri_req = spn_network_types::GetStdinUriRequest {
            format: spn_network_types::MessageFormat::Binary.into(),
            signature: signer.sign_message(&body.encode_to_vec()).await?.into(),
            body: Some(body),
        };

        let response = self
            .network
            .clone()
            .get_stdin_uri(get_stdin_uri_req)
            .await?
            .into_inner();
        Ok(response.stdin_uri)
    }
}

// The network RPC rejects any page whose offset passes this bound
// (network-services `bin/rpc/src/pagination.rs`).
const NETWORK_MAX_OFFSET: u32 = 10_000;

fn network_max_pages(limit: u32) -> u32 {
    NETWORK_MAX_OFFSET / limit + 1
}

type CancelableListing = (
    std::result::Result<PageResult<spn_network_types::ProofRequest>, tonic::Status>,
    CancelableKind,
);

// Skip a failed query so the others still cancel.
// When every query fails, return the error. An empty list would hide the outage.
fn merge_cancelable(
    responses: Vec<CancelableListing>,
) -> Result<Vec<(NetworkProofRequest, CancelableKind)>> {
    let mut any_succeeded = false;
    let mut last_error = None;
    // A request can move from Assigned to Unfulfillable between the two query reads.
    // Both queries can then return its ID, so cancel it only once.
    let mut seen = std::collections::HashSet::new();
    let mut requests = Vec::new();
    for (pages, kind) in responses {
        let page_requests = match pages {
            Ok(page_requests) => page_requests.items,
            Err(error) => {
                tracing::warn!("failed to list {kind:?} cancelable requests: {error}");
                last_error = Some(error);
                continue;
            }
        };
        any_succeeded = true;
        for request in page_requests {
            if seen.insert(request.request_id.clone()) {
                requests.push((NetworkProofRequest(request), kind));
            }
        }
    }
    match last_error {
        Some(error) if !any_succeeded => Err(anyhow::anyhow!(
            "every cancelable request query failed: {error}"
        )),
        _ => Ok(requests),
    }
}

pub struct NetworkProofRequest(pub spn_network_types::ProofRequest);

impl NetworkRequest for NetworkProofRequest {
    fn request_id(&self) -> String {
        hex::encode(&self.0.request_id)
    }

    fn program_uri(&self) -> &str {
        &self.0.program_uri
    }

    fn stdin_uri(&self) -> &str {
        &self.0.stdin_uri
    }

    fn deadline(&self) -> u64 {
        self.0.deadline
    }

    fn cycle_limit(&self) -> u64 {
        self.0.cycle_limit
    }

    fn gas_limit(&self) -> u64 {
        self.0.gas_limit
    }

    fn requester(&self) -> &[u8] {
        &self.0.requester
    }

    fn mode(&self) -> i32 {
        self.0.mode
    }

    fn program_public_uri(&self) -> &str {
        &self.0.program_public_uri
    }

    fn stdin_public_uri(&self) -> &str {
        &self.0.stdin_public_uri
    }

    fn stdin_private(&self) -> bool {
        self.0.stdin_private
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn listing(
        ids: &[&[u8]],
    ) -> std::result::Result<PageResult<spn_network_types::ProofRequest>, tonic::Status> {
        Ok(PageResult {
            items: ids
                .iter()
                .map(|id| spn_network_types::ProofRequest {
                    request_id: id.to_vec(),
                    ..Default::default()
                })
                .collect(),
            is_complete: true,
        })
    }

    #[tokio::test]
    async fn network_paging_stops_inside_the_server_offset_bound() {
        let limit = 1000;
        // Fakes the network RPC's offset bound.
        let fetch = |offset: u32| {
            let page = offset / limit + 1;
            std::future::ready(if (page - 1) * limit > 10_000 {
                Err(tonic::Status::invalid_argument("page too deep"))
            } else {
                Ok(vec![0u8; limit as usize])
            })
        };

        let listed = drain_pages(limit, network_max_pages(limit), fetch)
            .await
            .unwrap();

        assert!(!listed.is_complete);
        assert_eq!(listed.items.len(), 11 * limit as usize);
    }

    #[test]
    fn a_failed_query_does_not_drop_the_others() {
        let merged = merge_cancelable(vec![
            (
                Err(tonic::Status::unavailable("down")),
                CancelableKind::Unexecutable,
            ),
            (listing(&[b"a"]), CancelableKind::Unfulfillable),
        ])
        .unwrap();

        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].0 .0.request_id, b"a");
    }

    #[test]
    fn every_query_failing_fails_the_tick() {
        let merged = merge_cancelable(vec![
            (
                Err(tonic::Status::unavailable("down")),
                CancelableKind::Unexecutable,
            ),
            (
                Err(tonic::Status::unavailable("down")),
                CancelableKind::Unfulfillable,
            ),
        ]);

        assert!(merged.is_err());
    }
}
