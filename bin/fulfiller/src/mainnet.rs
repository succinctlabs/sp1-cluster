use alloy_primitives::Address;
use anyhow::Result;
use async_trait::async_trait;
use futures::future::join_all;
use prost::Message;
use sp1_cluster_artifact::ArtifactType;
use sp1_cluster_common::proto::ProofRequest;
use sp1_cluster_fulfillment::network::{FulfillmentNetwork, NetworkRequest};
use sp1_sdk::network::signer::NetworkSigner;
use spn_artifacts::Artifact;

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
        self.cancel_request(&request.id, signer).await?;
        Ok(())
    }

    async fn cancel_request(&self, request_id: &str, signer: &NetworkSigner) -> Result<()> {
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
            error: None,
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
    ) -> Result<Vec<Self::NetworkRequest>> {
        let queries: Vec<_> = fulfiller_addresses
            .into_iter()
            .map(
                |address| spn_network_types::GetFilteredProofRequestsRequest {
                    version: Some(version.to_string()),
                    fulfillment_status: Some(spn_network_types::FulfillmentStatus::Assigned.into()),
                    execution_status: Some(spn_network_types::ExecutionStatus::Unexecutable.into()),
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
        let bytes = artifact
            .download_raw_from_uri_par(
                uri,
                "us-east-2", // TODO: region
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
}
