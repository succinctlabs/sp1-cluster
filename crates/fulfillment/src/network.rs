use alloy_primitives::Address;
use anyhow::Result;
use async_trait::async_trait;
use sp1_cluster_artifact::ArtifactType;
use sp1_cluster_common::proto::ProofRequest;
use sp1_sdk::network::signer::NetworkSigner;
pub trait NetworkRequest: Send + Sync + 'static {
    fn request_id(&self) -> String;

    fn program_uri(&self) -> &str;

    fn stdin_uri(&self) -> &str;

    fn deadline(&self) -> u64;

    fn cycle_limit(&self) -> u64;

    fn gas_limit(&self) -> u64;

    fn requester(&self) -> &[u8];

    fn mode(&self) -> i32;

    fn program_public_uri(&self) -> &str;

    fn stdin_public_uri(&self) -> &str;

    /// Whether the execution oracle has marked this request as unexecutable.
    fn is_unexecutable(&self) -> bool {
        false
    }
}

#[async_trait]
pub trait FulfillmentNetwork: Send + Sync + 'static {
    type NetworkRequest: NetworkRequest;

    /// Initialize the network client and return the prover address.
    async fn init(&self, signer: &NetworkSigner) -> Result<Address>;

    /// Submit a successful proof request with the given proof bytes.
    async fn submit_request(
        &self,
        request: &ProofRequest,
        proof_bytes: Option<Vec<u8>>,
        domain: &[u8],
        signer: &NetworkSigner,
    ) -> Result<()>;

    /// Fail a fulfillment request after it failed in the cluster.
    async fn fail_request(
        &self,
        request: &ProofRequest,
        domain: &[u8],
        signer: &NetworkSigner,
    ) -> Result<()>;

    /// Fail a fulfillment request with a specific error code (e.g., VK mismatch).
    async fn fail_request_with_error(
        &self,
        request_id: &str,
        error: Option<i32>,
        domain: &[u8],
        signer: &NetworkSigner,
    ) -> Result<()>;

    /// Cancel a fulfillment request after the network does not expect it anymore.
    async fn cancel_request(&self, request_id: &str, signer: &NetworkSigner) -> Result<()>;

    /// Get schedulable requests (assigned requests that should be scheduled).
    async fn get_schedulable_requests(
        &self,
        version: &str,
        fulfiller_addresses: Vec<Vec<u8>>,
        minimum_deadline: u64,
        limit: u32,
    ) -> Result<Vec<Self::NetworkRequest>>;

    /// Get cancelable requests (unexecutable assigned requests).
    async fn get_cancelable_requests(
        &self,
        version: &str,
        fulfiller_addresses: Vec<Vec<u8>>,
        minimum_deadline: u64,
        limit: u32,
    ) -> Result<Vec<Self::NetworkRequest>>;

    /// Download an artifact from the network.
    async fn download_artifact(
        &self,
        id: &str,
        uri: &str,
        artifact_type: ArtifactType,
    ) -> Result<Vec<u8>>;

    /// Whether to download proofs for fulfillment when submitting a request.
    ///
    /// This should be disabled for the executor binary.
    fn should_download_proofs() -> bool {
        true
    }
}
