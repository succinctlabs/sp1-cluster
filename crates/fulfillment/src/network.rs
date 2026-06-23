use alloy_primitives::Address;
use anyhow::Result;
use async_trait::async_trait;
use sp1_cluster_artifact::ArtifactType;
use sp1_cluster_common::proto::ProofRequest;
use sp1_sdk::network::signer::NetworkSigner;
use spn_network_types::ComponentInfo;
pub trait NetworkRequest: Send + Sync + 'static {
    fn request_id(&self) -> String;

    /// Whether the network already marked this request `Unfulfillable` (a terminal state).
    fn is_unfulfillable(&self) -> bool;

    /// Whether the network already marked this request `Fulfilled` (a terminal success).
    ///
    /// This happens when another path (e.g. the SPN proxy delivering a mainnet proof)
    /// satisfied the request while our cluster was still proving it. The request
    /// succeeded, so we must abort the wasted cluster work without failing fulfillment.
    fn is_fulfilled(&self) -> bool;

    fn program_uri(&self) -> &str;

    fn stdin_uri(&self) -> &str;

    fn deadline(&self) -> u64;

    fn cycle_limit(&self) -> u64;

    fn gas_limit(&self) -> u64;

    fn requester(&self) -> &[u8];

    fn mode(&self) -> i32;

    fn program_public_uri(&self) -> &str;

    fn stdin_public_uri(&self) -> &str;

    /// Whether the stdin for this request is private. When true,
    /// `stdin_public_uri` is empty and callers must fetch a presigned URL via
    /// [`FulfillmentNetwork::fetch_stdin_uri`].
    fn stdin_private(&self) -> bool;
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
    ///
    /// `error_trace` is an optional bounded, sanitized JSON trace (see
    /// `spn_network_types::error_trace`) describing why the request failed. Pass
    /// `None` when no structured detail is available (e.g. cancellations).
    async fn fail_request_with_error(
        &self,
        request_id: &str,
        error: Option<i32>,
        error_trace: Option<Vec<u8>>,
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

    /// Fetch a URI suitable for downloading the stdin of a request.
    ///
    /// Non-private requests simply return `stdin_public_uri`. Private requests
    /// go through the authenticated `GetStdinUri` RPC and receive a short-lived
    /// presigned URL.
    async fn fetch_stdin_uri(
        &self,
        request: &Self::NetworkRequest,
        signer: &NetworkSigner,
    ) -> Result<String>;

    /// Report build identity for one or more cluster components to the network
    /// (network#232 `ReportProverInfo`). The fulfiller passes its own component
    /// plus any coordinator/worker components it could collect, in ONE request.
    ///
    /// Best-effort debugging telemetry: implementations must never block or fail
    /// fulfillment on this. The default is a no-op for networks that don't
    /// support the RPC.
    async fn report_prover_info(
        &self,
        _domain: &[u8],
        _prover: Address,
        _components: Vec<ComponentInfo>,
        _signer: &NetworkSigner,
    ) -> Result<()> {
        Ok(())
    }

    /// Whether to download proofs for fulfillment when submitting a request.
    ///
    /// This should be disabled for the executor binary.
    fn should_download_proofs() -> bool {
        true
    }
}
