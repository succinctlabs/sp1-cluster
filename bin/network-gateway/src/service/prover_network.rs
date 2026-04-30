use dashmap::DashMap;
use prost::Message;
use sp1_cluster_artifact::{ArtifactClient, ArtifactType};
use sp1_cluster_common::{client::ClusterServiceClient, proto as cluster_pb};
use sp1_sdk::network::proto::base::network::prover_network_server::ProverNetwork;
use sp1_sdk::network::proto::base::types as pb;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};
use tracing::info;

use crate::auth::Auth;
use crate::ids::{
    artifact_id_from_uri, artifact_uri, mint_request_id, program_artifact_id,
    proof_id_from_request_id,
};
use crate::program_store::ProgramStore;
use crate::proof_events::ProofEventsHub;
use crate::status::{
    cluster_execution_filter, cluster_fulfillment_filter, execution_from_cluster,
    fulfillment_from_cluster,
};

/// Maximum time the gateway will hold a `get_proof_request_status` call
/// open while waiting for the next status transition.
///
/// Sized to fit comfortably under the SDK's 60s per-RPC tonic deadline
/// (`crates/sdk/src/network/grpc.rs` sets `Endpoint::timeout(60s)`), which
/// leaves margin for the round trip + the SDK's `process_proof_status`
/// post-processing. Mainnet clients return immediately as before — only
/// the gateway holds the call open.
const STATUS_LONG_POLL_MAX: Duration = Duration::from_secs(25);

pub struct ProverNetworkImpl<A> {
    client: A,
    cluster: ClusterServiceClient,
    public_http_url: String,
    balance_amount: String,
    auth: Auth,
    program_store: Arc<dyn ProgramStore>,
    proof_events: ProofEventsHub,
    nonces: DashMap<Vec<u8>, AtomicU64>,
}

impl<A> ProverNetworkImpl<A> {
    pub fn new(
        client: A,
        cluster: ClusterServiceClient,
        public_http_url: String,
        balance_amount: String,
        auth: Auth,
        program_store: Arc<dyn ProgramStore>,
        proof_events: ProofEventsHub,
    ) -> Self {
        Self {
            client,
            cluster,
            public_http_url,
            balance_amount,
            auth,
            program_store,
            proof_events,
            nonces: DashMap::new(),
        }
    }

    fn program_uri_for(&self, vk_hash: &[u8]) -> String {
        let base = self.public_http_url.trim_end_matches('/');
        format!("{base}/programs/{}", hex::encode(vk_hash))
    }

    fn next_nonce(&self, address: &[u8]) -> u64 {
        let current = self
            .nonces
            .entry(address.to_vec())
            .or_insert_with(|| AtomicU64::new(0));
        current.fetch_add(1, Ordering::Relaxed)
    }

    /// Build the SDK-shaped `GetProofRequestStatusResponse` from a cluster
    /// row. Used by both the unary fast-path and the long-poll wake-up
    /// paths in `get_proof_request_status`.
    fn build_status_response(
        &self,
        proof: cluster_pb::ProofRequest,
    ) -> pb::GetProofRequestStatusResponse {
        let fulfillment = fulfillment_from_cluster(proof.proof_status());
        let execution = proof
            .execution_result
            .as_ref()
            .map(|r| execution_from_cluster(r.status()))
            .unwrap_or(pb::ExecutionStatus::Unexecuted);

        let proof_uri = (fulfillment == pb::FulfillmentStatus::Fulfilled)
            .then(|| {
                proof
                    .proof_artifact_id
                    .as_ref()
                    .map(|id| artifact_uri(&self.public_http_url, ArtifactType::Proof, id))
            })
            .flatten();

        let public_values_hash = proof
            .execution_result
            .as_ref()
            .filter(|r| !r.public_values_hash.is_empty())
            .map(|r| r.public_values_hash.clone());

        pb::GetProofRequestStatusResponse {
            fulfillment_status: fulfillment as i32,
            execution_status: execution as i32,
            request_tx_hash: vec![0u8; 32],
            deadline: proof.deadline,
            fulfill_tx_hash: None,
            proof_uri,
            public_values_hash,
            proof_public_uri: None,
        }
    }

    async fn load_cluster_proof(&self, proof_id: &str) -> Result<cluster_pb::ProofRequest, Status> {
        self.cluster
            .get_proof_request(cluster_pb::ProofRequestGetRequest {
                proof_id: proof_id.to_string(),
            })
            .await
            .map_err(|e| Status::internal(format!("cluster get_proof_request failed: {e}")))?
            .ok_or_else(|| Status::not_found(format!("proof request {proof_id} not found")))
    }

    fn build_sdk_proof_request(
        &self,
        request_id: &[u8],
        proof: cluster_pb::ProofRequest,
    ) -> pb::ProofRequest {
        let fulfillment = fulfillment_from_cluster(proof.proof_status());
        let execution = proof
            .execution_result
            .as_ref()
            .map(|r| execution_from_cluster(r.status()))
            .unwrap_or(pb::ExecutionStatus::Unexecuted);

        let program_uri = artifact_uri(
            &self.public_http_url,
            ArtifactType::Program,
            &proof.program_artifact_id,
        );
        let stdin_uri = artifact_uri(
            &self.public_http_url,
            ArtifactType::Stdin,
            &proof.stdin_artifact_id,
        );

        let mode: i32 = proof
            .options_artifact_id
            .as_deref()
            .and_then(|s| s.parse().ok())
            .unwrap_or(pb::ProofMode::UnspecifiedProofMode as i32);

        let (cycles, public_values_hash) = proof
            .execution_result
            .as_ref()
            .map(|r| {
                (
                    Some(r.cycles),
                    (!r.public_values_hash.is_empty()).then(|| r.public_values_hash.clone()),
                )
            })
            .unwrap_or((None, None));

        pb::ProofRequest {
            request_id: request_id.to_vec(),
            vk_hash: vec![],
            version: String::new(),
            mode,
            strategy: pb::FulfillmentStrategy::Reserved as i32,
            program_uri,
            stdin_uri,
            deadline: proof.deadline,
            cycle_limit: proof.cycle_limit.unwrap_or(0),
            gas_price: None,
            fulfillment_status: fulfillment as i32,
            execution_status: execution as i32,
            requester: proof.requester,
            fulfiller: None,
            program_name: None,
            requester_name: None,
            fulfiller_name: None,
            created_at: proof.created_at,
            updated_at: proof.updated_at,
            fulfilled_at: None,
            tx_hash: vec![0u8; 32],
            cycles,
            public_values_hash,
            deduction_amount: None,
            refund_amount: None,
            gas_limit: proof.gas_limit.unwrap_or(0),
            gas_used: None,
            execute_fail_cause: 0,
            settlement_status: 0,
            program_public_uri: String::new(),
            stdin_public_uri: String::new(),
            min_auction_period: 0,
            whitelist: vec![],
            error: 0,
        }
    }
}

/// SDK terminal states: SDK's `process_proof_status` returns early on
/// `Fulfilled` (success) and `Unfulfillable` (`RequestUnfulfillable`).
/// We don't wait for further transitions past these.
fn is_terminal_fulfillment(fulfillment: i32) -> bool {
    matches!(
        pb::FulfillmentStatus::try_from(fulfillment),
        Ok(pb::FulfillmentStatus::Fulfilled) | Ok(pb::FulfillmentStatus::Unfulfillable)
    )
}

type StreamResult<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl<A> ProverNetwork for ProverNetworkImpl<A>
where
    A: ArtifactClient + 'static,
{
    type SubscribeProofRequestsStream = StreamResult<pb::ProofRequest>;

    async fn request_proof(
        &self,
        request: Request<pb::RequestProofRequest>,
    ) -> Result<Response<pb::RequestProofResponse>, Status> {
        let req = request.into_inner();
        let body = req
            .body
            .ok_or_else(|| Status::invalid_argument("request_proof: missing body"))?;

        let requester = self.auth.authorize(&body.encode_to_vec(), &req.signature)?;

        let stdin_artifact_id = artifact_id_from_uri(&body.stdin_uri)
            .ok_or_else(|| {
                Status::invalid_argument(format!("invalid stdin_uri: {}", body.stdin_uri))
            })?
            .to_string();

        // Hot path: address the ELF by a deterministic id derived from vk_hash.
        // While the cluster store still holds the bytes (Redis TTL = 4h), every
        // subsequent prove() is a single `exists()` ping — no upload at all.
        // Only on TTL miss do we re-upload from the durable `ProgramStore`.
        let program_artifact_id = program_artifact_id(&body.vk_hash);
        let warm = self
            .client
            .exists(&program_artifact_id, ArtifactType::Program)
            .await
            .map_err(|e| Status::internal(format!("artifact exists check failed: {e}")))?;
        if !warm {
            let (_vk_bytes, elf_bytes) = self
                .program_store
                .get(&body.vk_hash)
                .await
                .map_err(|e| Status::internal(format!("program store load failed: {e}")))?
                .ok_or_else(|| {
                    Status::failed_precondition(format!(
                        "program not registered for vk_hash {}",
                        hex::encode(&body.vk_hash)
                    ))
                })?;
            self.client
                .upload_raw(&program_artifact_id, ArtifactType::Program, elf_bytes)
                .await
                .map_err(|e| Status::internal(format!("upload program artifact failed: {e}")))?;
        }

        let request_id = mint_request_id();
        let proof_id = proof_id_from_request_id(&request_id);
        let proof_artifact = self
            .client
            .create_artifact()
            .map_err(|e| Status::internal(format!("create proof artifact failed: {e}")))?;
        let proof_artifact_id = proof_artifact.to_id();

        let create = cluster_pb::ProofRequestCreateRequest {
            proof_id: proof_id.clone(),
            program_artifact_id: program_artifact_id.clone(),
            stdin_artifact_id,
            options_artifact_id: Some(body.mode.to_string()),
            proof_artifact_id: Some(proof_artifact_id.clone()),
            requester: requester.to_vec(),
            deadline: body.deadline,
            cycle_limit: body.cycle_limit,
            gas_limit: body.gas_limit,
            scheduled_by: None,
        };
        self.cluster
            .create_proof_request(create)
            .await
            .map_err(|e| Status::internal(format!("cluster create_proof_request failed: {e}")))?;

        info!(
            proof_id,
            program_artifact_id,
            proof_artifact_id,
            %requester,
            "request_proof"
        );

        Ok(Response::new(pb::RequestProofResponse {
            tx_hash: vec![0u8; 32],
            body: Some(pb::RequestProofResponseBody { request_id }),
        }))
    }

    async fn fulfill_proof(
        &self,
        _request: Request<pb::FulfillProofRequest>,
    ) -> Result<Response<pb::FulfillProofResponse>, Status> {
        Err(Status::unimplemented(
            "fulfill_proof: not supported by self-hosted gateway",
        ))
    }

    async fn execute_proof(
        &self,
        _request: Request<pb::ExecuteProofRequest>,
    ) -> Result<Response<pb::ExecuteProofResponse>, Status> {
        Err(Status::unimplemented(
            "execute_proof: not supported by self-hosted gateway",
        ))
    }

    async fn fail_fulfillment(
        &self,
        _request: Request<pb::FailFulfillmentRequest>,
    ) -> Result<Response<pb::FailFulfillmentResponse>, Status> {
        Err(Status::unimplemented(
            "fail_fulfillment: not supported by self-hosted gateway",
        ))
    }

    async fn fail_execution(
        &self,
        _request: Request<pb::FailExecutionRequest>,
    ) -> Result<Response<pb::FailExecutionResponse>, Status> {
        Err(Status::unimplemented(
            "fail_execution: not supported by self-hosted gateway",
        ))
    }

    /// Long-polls. The handler holds the request open until the proof
    /// transitions to a terminal state (or any state at all), or
    /// `STATUS_LONG_POLL_MAX` elapses, whichever comes first. Mainnet's
    /// handler returns immediately as before; nothing in the SDK or proto
    /// changes — the *server's* response time is the only thing that
    /// shifts. The SDK's existing `wait_proof` loop just observes "this
    /// call took longer to return" and proceeds as before, so polling
    /// clients on mainnet are unaffected.
    async fn get_proof_request_status(
        &self,
        request: Request<pb::GetProofRequestStatusRequest>,
    ) -> Result<Response<pb::GetProofRequestStatusResponse>, Status> {
        let req = request.into_inner();
        let proof_id = proof_id_from_request_id(&req.request_id);

        // Subscribe BEFORE the initial fetch so we don't miss a transition
        // that fires between the SELECT and the recv() below.
        let mut events = self.proof_events.subscribe(&proof_id);

        let proof = self.load_cluster_proof(&proof_id).await?;
        let initial = self.build_status_response(proof);
        if is_terminal_fulfillment(initial.fulfillment_status) {
            return Ok(Response::new(initial));
        }

        match tokio::time::timeout(STATUS_LONG_POLL_MAX, events.recv()).await {
            // Status changed (or we lagged the channel): re-fetch the row
            // to pick up proof_uri / execution result / etc.
            Ok(Ok(_)) | Ok(Err(broadcast::error::RecvError::Lagged(_))) => {
                let proof = self.load_cluster_proof(&proof_id).await?;
                Ok(Response::new(self.build_status_response(proof)))
            }
            // Hub closed (shouldn't happen) or 25s elapsed without a
            // transition: return what we already have. The SDK's polling
            // loop will call again on its next iteration.
            Ok(Err(broadcast::error::RecvError::Closed)) | Err(_) => Ok(Response::new(initial)),
        }
    }

    async fn get_proof_request_details(
        &self,
        request: Request<pb::GetProofRequestDetailsRequest>,
    ) -> Result<Response<pb::GetProofRequestDetailsResponse>, Status> {
        let req = request.into_inner();
        let proof_id = proof_id_from_request_id(&req.request_id);
        let proof = self.load_cluster_proof(&proof_id).await?;

        let details = self.build_sdk_proof_request(&req.request_id, proof);
        Ok(Response::new(pb::GetProofRequestDetailsResponse {
            request: Some(details),
        }))
    }

    async fn get_filtered_proof_requests(
        &self,
        request: Request<pb::GetFilteredProofRequestsRequest>,
    ) -> Result<Response<pb::GetFilteredProofRequestsResponse>, Status> {
        let req = request.into_inner();

        // Best-effort: the cluster's ProofRequestListRequest doesn't carry
        // vk_hash/requester/fulfiller/from/to/version/mode filters, so those
        // are silently dropped. Status filters and pagination round-trip.
        let proof_status = req
            .fulfillment_status
            .and_then(|s| pb::FulfillmentStatus::try_from(s).ok())
            .map(cluster_fulfillment_filter)
            .unwrap_or_default()
            .into_iter()
            .map(|s| s as i32)
            .collect();
        let execution_status = req
            .execution_status
            .and_then(|s| pb::ExecutionStatus::try_from(s).ok())
            .map(cluster_execution_filter)
            .unwrap_or_default()
            .into_iter()
            .map(|s| s as i32)
            .collect();

        let limit = req.limit.map(|l| l.clamp(1, 100));
        let offset = match (req.page, limit) {
            (Some(page), Some(lim)) if page > 0 => Some(page.saturating_sub(1).saturating_mul(lim)),
            _ => None,
        };

        let list = cluster_pb::ProofRequestListRequest {
            proof_status,
            execution_status,
            minimum_deadline: req.minimum_deadline,
            handled: None,
            limit,
            offset,
            scheduled_by: None,
        };
        let proofs = self
            .cluster
            .get_proof_requests(list)
            .await
            .map_err(|e| Status::internal(format!("cluster get_proof_requests failed: {e}")))?;

        let requests = proofs
            .into_iter()
            .map(|p| {
                let request_id = p.id.as_bytes().to_vec();
                self.build_sdk_proof_request(&request_id, p)
            })
            .collect();
        Ok(Response::new(pb::GetFilteredProofRequestsResponse {
            requests,
        }))
    }

    async fn subscribe_proof_requests(
        &self,
        _request: Request<pb::GetFilteredProofRequestsRequest>,
    ) -> Result<Response<Self::SubscribeProofRequestsStream>, Status> {
        Err(Status::unimplemented(
            "subscribe_proof_requests: not supported by self-hosted gateway",
        ))
    }

    async fn get_search_results(
        &self,
        _request: Request<pb::GetSearchResultsRequest>,
    ) -> Result<Response<pb::GetSearchResultsResponse>, Status> {
        Err(Status::unimplemented(
            "get_search_results: not supported by self-hosted gateway",
        ))
    }

    async fn get_proof_request_metrics(
        &self,
        _request: Request<pb::GetProofRequestMetricsRequest>,
    ) -> Result<Response<pb::GetProofRequestMetricsResponse>, Status> {
        Err(Status::unimplemented(
            "get_proof_request_metrics: not supported by self-hosted gateway",
        ))
    }

    async fn get_proof_request_graph(
        &self,
        _request: Request<pb::GetProofRequestGraphRequest>,
    ) -> Result<Response<pb::GetProofRequestGraphResponse>, Status> {
        Err(Status::unimplemented(
            "get_proof_request_graph: not supported by self-hosted gateway",
        ))
    }

    async fn get_analytics_graphs(
        &self,
        _request: Request<pb::GetAnalyticsGraphsRequest>,
    ) -> Result<Response<pb::GetAnalyticsGraphsResponse>, Status> {
        Err(Status::unimplemented(
            "get_analytics_graphs: not supported by self-hosted gateway",
        ))
    }

    async fn get_overview_graphs(
        &self,
        _request: Request<pb::GetOverviewGraphsRequest>,
    ) -> Result<Response<pb::GetOverviewGraphsResponse>, Status> {
        Err(Status::unimplemented(
            "get_overview_graphs: not supported by self-hosted gateway",
        ))
    }

    async fn get_nonce(
        &self,
        request: Request<pb::GetNonceRequest>,
    ) -> Result<Response<pb::GetNonceResponse>, Status> {
        let addr = request.into_inner().address;
        let nonce = self.next_nonce(&addr);
        Ok(Response::new(pb::GetNonceResponse { nonce }))
    }

    async fn get_filtered_delegations(
        &self,
        _request: Request<pb::GetFilteredDelegationsRequest>,
    ) -> Result<Response<pb::GetFilteredDelegationsResponse>, Status> {
        Err(Status::unimplemented(
            "get_filtered_delegations: not supported by self-hosted gateway",
        ))
    }

    async fn add_delegation(
        &self,
        _request: Request<pb::AddDelegationRequest>,
    ) -> Result<Response<pb::AddDelegationResponse>, Status> {
        Err(Status::unimplemented(
            "add_delegation: not supported by self-hosted gateway",
        ))
    }

    async fn remove_delegation(
        &self,
        _request: Request<pb::RemoveDelegationRequest>,
    ) -> Result<Response<pb::RemoveDelegationResponse>, Status> {
        Err(Status::unimplemented(
            "remove_delegation: not supported by self-hosted gateway",
        ))
    }

    async fn terminate_delegation(
        &self,
        _request: Request<pb::TerminateDelegationRequest>,
    ) -> Result<Response<pb::TerminateDelegationResponse>, Status> {
        Err(Status::unimplemented(
            "terminate_delegation: not supported by self-hosted gateway",
        ))
    }

    async fn accept_delegation(
        &self,
        _request: Request<pb::AcceptDelegationRequest>,
    ) -> Result<Response<pb::AcceptDelegationResponse>, Status> {
        Err(Status::unimplemented(
            "accept_delegation: not supported by self-hosted gateway",
        ))
    }

    async fn set_account_name(
        &self,
        _request: Request<pb::SetAccountNameRequest>,
    ) -> Result<Response<pb::SetAccountNameResponse>, Status> {
        Err(Status::unimplemented(
            "set_account_name: not supported by self-hosted gateway",
        ))
    }

    async fn get_account_name(
        &self,
        _request: Request<pb::GetAccountNameRequest>,
    ) -> Result<Response<pb::GetAccountNameResponse>, Status> {
        Err(Status::unimplemented(
            "get_account_name: not supported by self-hosted gateway",
        ))
    }

    async fn get_terms_signature(
        &self,
        _request: Request<pb::GetTermsSignatureRequest>,
    ) -> Result<Response<pb::GetTermsSignatureResponse>, Status> {
        Err(Status::unimplemented(
            "get_terms_signature: not supported by self-hosted gateway",
        ))
    }

    async fn set_terms_signature(
        &self,
        _request: Request<pb::SetTermsSignatureRequest>,
    ) -> Result<Response<pb::SetTermsSignatureResponse>, Status> {
        Err(Status::unimplemented(
            "set_terms_signature: not supported by self-hosted gateway",
        ))
    }

    async fn get_account(
        &self,
        _request: Request<pb::GetAccountRequest>,
    ) -> Result<Response<pb::GetAccountResponse>, Status> {
        Err(Status::unimplemented(
            "get_account: not supported by self-hosted gateway",
        ))
    }

    async fn get_owner(
        &self,
        _request: Request<pb::GetOwnerRequest>,
    ) -> Result<Response<pb::GetOwnerResponse>, Status> {
        Err(Status::unimplemented(
            "get_owner: not supported by self-hosted gateway",
        ))
    }

    async fn get_program(
        &self,
        request: Request<pb::GetProgramRequest>,
    ) -> Result<Response<pb::GetProgramResponse>, Status> {
        let req = request.into_inner();
        let (vk_bytes, _elf_bytes) = self
            .program_store
            .get(&req.vk_hash)
            .await
            .map_err(|e| Status::internal(format!("program store load failed: {e}")))?
            .ok_or_else(|| {
                // The SDK's `NetworkClient::get_program` treats ANY Ok response as
                // "program already registered"; only a NotFound status routes it
                // into the `create_program` branch. Returning a bare None here
                // would cause the SDK to skip registration entirely and then fail
                // `request_proof` with FailedPrecondition.
                Status::not_found(format!(
                    "program not registered for vk_hash {}",
                    hex::encode(&req.vk_hash)
                ))
            })?;

        let program = pb::Program {
            vk_hash: req.vk_hash.clone(),
            vk: vk_bytes,
            program_uri: self.program_uri_for(&req.vk_hash),
            name: None,
            owner: vec![],
            created_at: 0,
        };
        Ok(Response::new(pb::GetProgramResponse {
            program: Some(program),
        }))
    }

    async fn create_program(
        &self,
        request: Request<pb::CreateProgramRequest>,
    ) -> Result<Response<pb::CreateProgramResponse>, Status> {
        let req = request.into_inner();
        let body = req
            .body
            .ok_or_else(|| Status::invalid_argument("create_program: missing body"))?;

        let requester = self.auth.authorize(&body.encode_to_vec(), &req.signature)?;

        // The SDK uploads ELF bytes to the gateway's artifact store immediately
        // before calling `create_program`. Pull those bytes out of the ephemeral
        // store and persist them in (1) the durable program store keyed by
        // `vk_hash`, and (2) the cluster artifact store under a deterministic
        // id so subsequent `request_proof` calls can skip the re-upload while
        // the ELF is still warm.
        let orphan_artifact_id = artifact_id_from_uri(&body.program_uri)
            .ok_or_else(|| {
                Status::invalid_argument(format!(
                    "create_program: invalid program_uri {}",
                    body.program_uri
                ))
            })?
            .to_string();

        let elf_bytes = self
            .client
            .download_raw(&orphan_artifact_id, ArtifactType::Program)
            .await
            .map_err(|e| {
                Status::failed_precondition(format!(
                    "create_program: ELF not present in artifact store at {} ({e})",
                    body.program_uri
                ))
            })?;
        let elf_len = elf_bytes.len();

        self.program_store
            .put(&body.vk_hash, &body.vk, &elf_bytes)
            .await
            .map_err(|e| Status::internal(format!("program store put failed: {e}")))?;

        let det_id = program_artifact_id(&body.vk_hash);
        self.client
            .upload_raw(&det_id, ArtifactType::Program, elf_bytes)
            .await
            .map_err(|e| Status::internal(format!("warm upload failed: {e}")))?;

        // The SDK-uploaded artifact is now redundant — the authoritative copy
        // lives in `ProgramStore` and the warm copy lives under `det_id`.
        self.client
            .try_delete(&orphan_artifact_id, ArtifactType::Program)
            .await
            .ok();

        info!(
            vk_hash = %hex::encode(&body.vk_hash),
            elf_len,
            program_artifact_id = det_id,
            %requester,
            "create_program"
        );
        Ok(Response::new(pb::CreateProgramResponse {
            tx_hash: vec![0u8; 32],
            body: Some(pb::CreateProgramResponseBody {}),
        }))
    }

    async fn set_program_name(
        &self,
        _request: Request<pb::SetProgramNameRequest>,
    ) -> Result<Response<pb::SetProgramNameResponse>, Status> {
        Err(Status::unimplemented(
            "set_program_name: not supported by self-hosted gateway",
        ))
    }

    async fn get_balance(
        &self,
        _request: Request<pb::GetBalanceRequest>,
    ) -> Result<Response<pb::GetBalanceResponse>, Status> {
        Ok(Response::new(pb::GetBalanceResponse {
            amount: self.balance_amount.clone(),
        }))
    }

    async fn get_filtered_balance_logs(
        &self,
        _request: Request<pb::GetFilteredBalanceLogsRequest>,
    ) -> Result<Response<pb::GetFilteredBalanceLogsResponse>, Status> {
        Err(Status::unimplemented(
            "get_filtered_balance_logs: not supported by self-hosted gateway",
        ))
    }

    async fn add_credit(
        &self,
        _request: Request<pb::AddCreditRequest>,
    ) -> Result<Response<pb::AddCreditResponse>, Status> {
        Err(Status::unimplemented(
            "add_credit: not supported by self-hosted gateway",
        ))
    }

    async fn get_latest_bridge_block(
        &self,
        _request: Request<pb::GetLatestBridgeBlockRequest>,
    ) -> Result<Response<pb::GetLatestBridgeBlockResponse>, Status> {
        Err(Status::unimplemented(
            "get_latest_bridge_block: not supported by self-hosted gateway",
        ))
    }

    async fn get_gas_price_estimate(
        &self,
        _request: Request<pb::GetGasPriceEstimateRequest>,
    ) -> Result<Response<pb::GetGasPriceEstimateResponse>, Status> {
        Err(Status::unimplemented(
            "get_gas_price_estimate: not supported by self-hosted gateway",
        ))
    }

    async fn get_transaction_details(
        &self,
        _request: Request<pb::GetTransactionDetailsRequest>,
    ) -> Result<Response<pb::GetTransactionDetailsResponse>, Status> {
        Err(Status::unimplemented(
            "get_transaction_details: not supported by self-hosted gateway",
        ))
    }

    async fn add_reserved_charge(
        &self,
        _request: Request<pb::AddReservedChargeRequest>,
    ) -> Result<Response<pb::AddReservedChargeResponse>, Status> {
        Err(Status::unimplemented(
            "add_reserved_charge: not supported by self-hosted gateway",
        ))
    }

    async fn get_billing_summary(
        &self,
        _request: Request<pb::GetBillingSummaryRequest>,
    ) -> Result<Response<pb::GetBillingSummaryResponse>, Status> {
        Err(Status::unimplemented(
            "get_billing_summary: not supported by self-hosted gateway",
        ))
    }

    async fn update_price(
        &self,
        _request: Request<pb::UpdatePriceRequest>,
    ) -> Result<Response<pb::UpdatePriceResponse>, Status> {
        Err(Status::unimplemented(
            "update_price: not supported by self-hosted gateway",
        ))
    }

    async fn get_filtered_clusters(
        &self,
        _request: Request<pb::GetFilteredClustersRequest>,
    ) -> Result<Response<pb::GetFilteredClustersResponse>, Status> {
        Err(Status::unimplemented(
            "get_filtered_clusters: not supported by self-hosted gateway",
        ))
    }

    async fn get_usage_summary(
        &self,
        _request: Request<pb::GetUsageSummaryRequest>,
    ) -> Result<Response<pb::GetUsageSummaryResponse>, Status> {
        Err(Status::unimplemented(
            "get_usage_summary: not supported by self-hosted gateway",
        ))
    }

    async fn get_filtered_charges(
        &self,
        _request: Request<pb::GetFilteredChargesRequest>,
    ) -> Result<Response<pb::GetFilteredChargesResponse>, Status> {
        Err(Status::unimplemented(
            "get_filtered_charges: not supported by self-hosted gateway",
        ))
    }

    async fn get_filtered_reservations(
        &self,
        _request: Request<pb::GetFilteredReservationsRequest>,
    ) -> Result<Response<pb::GetFilteredReservationsResponse>, Status> {
        Err(Status::unimplemented(
            "get_filtered_reservations: not supported by self-hosted gateway",
        ))
    }

    async fn add_reservation(
        &self,
        _request: Request<pb::AddReservationRequest>,
    ) -> Result<Response<pb::AddReservationResponse>, Status> {
        Err(Status::unimplemented(
            "add_reservation: not supported by self-hosted gateway",
        ))
    }

    async fn remove_reservation(
        &self,
        _request: Request<pb::RemoveReservationRequest>,
    ) -> Result<Response<pb::RemoveReservationResponse>, Status> {
        Err(Status::unimplemented(
            "remove_reservation: not supported by self-hosted gateway",
        ))
    }

    async fn bid(
        &self,
        _request: Request<pb::BidRequest>,
    ) -> Result<Response<pb::BidResponse>, Status> {
        Err(Status::unimplemented(
            "bid: not supported by self-hosted gateway",
        ))
    }

    async fn settle(
        &self,
        _request: Request<pb::SettleRequest>,
    ) -> Result<Response<pb::SettleResponse>, Status> {
        Err(Status::unimplemented(
            "settle: not supported by self-hosted gateway",
        ))
    }

    async fn sign_in(
        &self,
        _request: Request<pb::SignInRequest>,
    ) -> Result<Response<pb::SignInResponse>, Status> {
        Err(Status::unimplemented(
            "sign_in: not supported by self-hosted gateway",
        ))
    }

    async fn get_onboarded_accounts_count(
        &self,
        _request: Request<pb::GetOnboardedAccountsCountRequest>,
    ) -> Result<Response<pb::GetOnboardedAccountsCountResponse>, Status> {
        Err(Status::unimplemented(
            "get_onboarded_accounts_count: not supported by self-hosted gateway",
        ))
    }

    async fn get_filtered_onboarded_accounts(
        &self,
        _request: Request<pb::GetFilteredOnboardedAccountsRequest>,
    ) -> Result<Response<pb::GetFilteredOnboardedAccountsResponse>, Status> {
        Err(Status::unimplemented(
            "get_filtered_onboarded_accounts: not supported by self-hosted gateway",
        ))
    }

    async fn get_leaderboard(
        &self,
        _request: Request<pb::GetLeaderboardRequest>,
    ) -> Result<Response<pb::GetLeaderboardResponse>, Status> {
        Err(Status::unimplemented(
            "get_leaderboard: not supported by self-hosted gateway",
        ))
    }

    async fn get_leaderboard_stats(
        &self,
        _request: Request<pb::GetLeaderboardStatsRequest>,
    ) -> Result<Response<pb::GetLeaderboardStatsResponse>, Status> {
        Err(Status::unimplemented(
            "get_leaderboard_stats: not supported by self-hosted gateway",
        ))
    }

    async fn get_codes(
        &self,
        _request: Request<pb::GetCodesRequest>,
    ) -> Result<Response<pb::GetCodesResponse>, Status> {
        Err(Status::unimplemented(
            "get_codes: not supported by self-hosted gateway",
        ))
    }

    async fn redeem_code(
        &self,
        _request: Request<pb::RedeemCodeRequest>,
    ) -> Result<Response<pb::RedeemCodeResponse>, Status> {
        Err(Status::unimplemented(
            "redeem_code: not supported by self-hosted gateway",
        ))
    }

    async fn connect_twitter(
        &self,
        _request: Request<pb::ConnectTwitterRequest>,
    ) -> Result<Response<pb::ConnectTwitterResponse>, Status> {
        Err(Status::unimplemented(
            "connect_twitter: not supported by self-hosted gateway",
        ))
    }

    async fn complete_onboarding(
        &self,
        _request: Request<pb::CompleteOnboardingRequest>,
    ) -> Result<Response<pb::CompleteOnboardingResponse>, Status> {
        Err(Status::unimplemented(
            "complete_onboarding: not supported by self-hosted gateway",
        ))
    }

    async fn set_use_twitter_handle(
        &self,
        _request: Request<pb::SetUseTwitterHandleRequest>,
    ) -> Result<Response<pb::SetUseTwitterHandleResponse>, Status> {
        Err(Status::unimplemented(
            "set_use_twitter_handle: not supported by self-hosted gateway",
        ))
    }

    async fn set_use_twitter_image(
        &self,
        _request: Request<pb::SetUseTwitterImageRequest>,
    ) -> Result<Response<pb::SetUseTwitterImageResponse>, Status> {
        Err(Status::unimplemented(
            "set_use_twitter_image: not supported by self-hosted gateway",
        ))
    }

    async fn request_random_proof(
        &self,
        _request: Request<pb::RequestRandomProofRequest>,
    ) -> Result<Response<pb::RequestRandomProofResponse>, Status> {
        Err(Status::unimplemented(
            "request_random_proof: not supported by self-hosted gateway",
        ))
    }

    async fn submit_captcha_game(
        &self,
        _request: Request<pb::SubmitCaptchaGameRequest>,
    ) -> Result<Response<pb::SubmitCaptchaGameResponse>, Status> {
        Err(Status::unimplemented(
            "submit_captcha_game: not supported by self-hosted gateway",
        ))
    }

    async fn redeem_stars(
        &self,
        _request: Request<pb::RedeemStarsRequest>,
    ) -> Result<Response<pb::RedeemStarsResponse>, Status> {
        Err(Status::unimplemented(
            "redeem_stars: not supported by self-hosted gateway",
        ))
    }

    async fn get_flappy_leaderboard(
        &self,
        _request: Request<pb::GetFlappyLeaderboardRequest>,
    ) -> Result<Response<pb::GetFlappyLeaderboardResponse>, Status> {
        Err(Status::unimplemented(
            "get_flappy_leaderboard: not supported by self-hosted gateway",
        ))
    }

    async fn set_turbo_high_score(
        &self,
        _request: Request<pb::SetTurboHighScoreRequest>,
    ) -> Result<Response<pb::SetTurboHighScoreResponse>, Status> {
        Err(Status::unimplemented(
            "set_turbo_high_score: not supported by self-hosted gateway",
        ))
    }

    async fn submit_quiz_game(
        &self,
        _request: Request<pb::SubmitQuizGameRequest>,
    ) -> Result<Response<pb::SubmitQuizGameResponse>, Status> {
        Err(Status::unimplemented(
            "submit_quiz_game: not supported by self-hosted gateway",
        ))
    }

    async fn get_turbo_leaderboard(
        &self,
        _request: Request<pb::GetTurboLeaderboardRequest>,
    ) -> Result<Response<pb::GetTurboLeaderboardResponse>, Status> {
        Err(Status::unimplemented(
            "get_turbo_leaderboard: not supported by self-hosted gateway",
        ))
    }

    async fn submit_eth_block_metadata(
        &self,
        _request: Request<pb::SubmitEthBlockMetadataRequest>,
    ) -> Result<Response<pb::SubmitEthBlockMetadataResponse>, Status> {
        Err(Status::unimplemented(
            "submit_eth_block_metadata: not supported by self-hosted gateway",
        ))
    }

    async fn get_filtered_eth_block_requests(
        &self,
        _request: Request<pb::GetFilteredEthBlockRequestsRequest>,
    ) -> Result<Response<pb::GetFilteredEthBlockRequestsResponse>, Status> {
        Err(Status::unimplemented(
            "get_filtered_eth_block_requests: not supported by self-hosted gateway",
        ))
    }

    async fn set2048_high_score(
        &self,
        _request: Request<pb::Set2048HighScoreRequest>,
    ) -> Result<Response<pb::Set2048HighScoreResponse>, Status> {
        Err(Status::unimplemented(
            "set2048_high_score: not supported by self-hosted gateway",
        ))
    }

    async fn set_volleyball_high_score(
        &self,
        _request: Request<pb::SetVolleyballHighScoreRequest>,
    ) -> Result<Response<pb::SetVolleyballHighScoreResponse>, Status> {
        Err(Status::unimplemented(
            "set_volleyball_high_score: not supported by self-hosted gateway",
        ))
    }

    async fn get_eth_block_request_metrics(
        &self,
        _request: Request<pb::GetEthBlockRequestMetricsRequest>,
    ) -> Result<Response<pb::GetEthBlockRequestMetricsResponse>, Status> {
        Err(Status::unimplemented(
            "get_eth_block_request_metrics: not supported by self-hosted gateway",
        ))
    }

    async fn set_turbo_time_trial_high_score(
        &self,
        _request: Request<pb::SetTurboTimeTrialHighScoreRequest>,
    ) -> Result<Response<pb::SetTurboTimeTrialHighScoreResponse>, Status> {
        Err(Status::unimplemented(
            "set_turbo_time_trial_high_score: not supported by self-hosted gateway",
        ))
    }

    async fn set_coin_craze_high_score(
        &self,
        _request: Request<pb::SetCoinCrazeHighScoreRequest>,
    ) -> Result<Response<pb::SetCoinCrazeHighScoreResponse>, Status> {
        Err(Status::unimplemented(
            "set_coin_craze_high_score: not supported by self-hosted gateway",
        ))
    }

    async fn set_lean_high_score(
        &self,
        _request: Request<pb::SetLeanHighScoreRequest>,
    ) -> Result<Response<pb::SetLeanHighScoreResponse>, Status> {
        Err(Status::unimplemented(
            "set_lean_high_score: not supported by self-hosted gateway",
        ))
    }

    async fn set_flow_high_score(
        &self,
        _request: Request<pb::SetFlowHighScoreRequest>,
    ) -> Result<Response<pb::SetFlowHighScoreResponse>, Status> {
        Err(Status::unimplemented(
            "set_flow_high_score: not supported by self-hosted gateway",
        ))
    }

    async fn set_rollup_high_score(
        &self,
        _request: Request<pb::SetRollupHighScoreRequest>,
    ) -> Result<Response<pb::SetRollupHighScoreResponse>, Status> {
        Err(Status::unimplemented(
            "set_rollup_high_score: not supported by self-hosted gateway",
        ))
    }

    async fn get_pending_stars(
        &self,
        _request: Request<pb::GetPendingStarsRequest>,
    ) -> Result<Response<pb::GetPendingStarsResponse>, Status> {
        Err(Status::unimplemented(
            "get_pending_stars: not supported by self-hosted gateway",
        ))
    }

    async fn get_whitelist_status(
        &self,
        _request: Request<pb::GetWhitelistStatusRequest>,
    ) -> Result<Response<pb::GetWhitelistStatusResponse>, Status> {
        Err(Status::unimplemented(
            "get_whitelist_status: not supported by self-hosted gateway",
        ))
    }

    async fn set_gpu_delegate(
        &self,
        _request: Request<pb::SetGpuDelegateRequest>,
    ) -> Result<Response<pb::SetGpuDelegateResponse>, Status> {
        Err(Status::unimplemented(
            "set_gpu_delegate: not supported by self-hosted gateway",
        ))
    }

    async fn claim_gpu(
        &self,
        _request: Request<pb::ClaimGpuRequest>,
    ) -> Result<Response<pb::ClaimGpuResponse>, Status> {
        Err(Status::unimplemented(
            "claim_gpu: not supported by self-hosted gateway",
        ))
    }

    async fn set_gpu_variant(
        &self,
        _request: Request<pb::SetGpuVariantRequest>,
    ) -> Result<Response<pb::SetGpuVariantResponse>, Status> {
        Err(Status::unimplemented(
            "set_gpu_variant: not supported by self-hosted gateway",
        ))
    }

    async fn link_whitelisted_twitter(
        &self,
        _request: Request<pb::LinkWhitelistedTwitterRequest>,
    ) -> Result<Response<pb::LinkWhitelistedTwitterResponse>, Status> {
        Err(Status::unimplemented(
            "link_whitelisted_twitter: not supported by self-hosted gateway",
        ))
    }

    async fn retrieve_proving_key(
        &self,
        _request: Request<pb::RetrieveProvingKeyRequest>,
    ) -> Result<Response<pb::RetrieveProvingKeyResponse>, Status> {
        Err(Status::unimplemented(
            "retrieve_proving_key: not supported by self-hosted gateway",
        ))
    }

    async fn link_whitelisted_github(
        &self,
        _request: Request<pb::LinkWhitelistedGithubRequest>,
    ) -> Result<Response<pb::LinkWhitelistedGithubResponse>, Status> {
        Err(Status::unimplemented(
            "link_whitelisted_github: not supported by self-hosted gateway",
        ))
    }

    async fn link_whitelisted_discord(
        &self,
        _request: Request<pb::LinkWhitelistedDiscordRequest>,
    ) -> Result<Response<pb::LinkWhitelistedDiscordResponse>, Status> {
        Err(Status::unimplemented(
            "link_whitelisted_discord: not supported by self-hosted gateway",
        ))
    }

    async fn link_social_discord(
        &self,
        _request: Request<pb::LinkSocialDiscordRequest>,
    ) -> Result<Response<pb::LinkSocialDiscordResponse>, Status> {
        Err(Status::unimplemented(
            "link_social_discord: not supported by self-hosted gateway",
        ))
    }

    async fn link_social_twitter(
        &self,
        _request: Request<pb::LinkSocialTwitterRequest>,
    ) -> Result<Response<pb::LinkSocialTwitterResponse>, Status> {
        Err(Status::unimplemented(
            "link_social_twitter: not supported by self-hosted gateway",
        ))
    }

    async fn get_linked_social_accounts(
        &self,
        _request: Request<pb::GetLinkedSocialAccountsRequest>,
    ) -> Result<Response<pb::GetLinkedSocialAccountsResponse>, Status> {
        Err(Status::unimplemented(
            "get_linked_social_accounts: not supported by self-hosted gateway",
        ))
    }

    async fn get_prover_leaderboard(
        &self,
        _request: Request<pb::GetProverLeaderboardRequest>,
    ) -> Result<Response<pb::GetProverLeaderboardResponse>, Status> {
        Err(Status::unimplemented(
            "get_prover_leaderboard: not supported by self-hosted gateway",
        ))
    }

    async fn get_filtered_gpus(
        &self,
        _request: Request<pb::GetFilteredGpusRequest>,
    ) -> Result<Response<pb::GetFilteredGpusResponse>, Status> {
        Err(Status::unimplemented(
            "get_filtered_gpus: not supported by self-hosted gateway",
        ))
    }

    async fn set_gpu_coordinates(
        &self,
        _request: Request<pb::SetGpuCoordinatesRequest>,
    ) -> Result<Response<pb::SetGpuCoordinatesResponse>, Status> {
        Err(Status::unimplemented(
            "set_gpu_coordinates: not supported by self-hosted gateway",
        ))
    }

    async fn get_points(
        &self,
        _request: Request<pb::GetPointsRequest>,
    ) -> Result<Response<pb::GetPointsResponse>, Status> {
        Err(Status::unimplemented(
            "get_points: not supported by self-hosted gateway",
        ))
    }

    async fn process_clicks(
        &self,
        _request: Request<pb::ProcessClicksRequest>,
    ) -> Result<Response<pb::ProcessClicksResponse>, Status> {
        Err(Status::unimplemented(
            "process_clicks: not supported by self-hosted gateway",
        ))
    }

    async fn purchase_upgrade(
        &self,
        _request: Request<pb::PurchaseUpgradeRequest>,
    ) -> Result<Response<pb::PurchaseUpgradeResponse>, Status> {
        Err(Status::unimplemented(
            "purchase_upgrade: not supported by self-hosted gateway",
        ))
    }

    async fn bet(
        &self,
        _request: Request<pb::BetRequest>,
    ) -> Result<Response<pb::BetResponse>, Status> {
        Err(Status::unimplemented(
            "bet: not supported by self-hosted gateway",
        ))
    }

    async fn get_contest_details(
        &self,
        _request: Request<pb::GetContestDetailsRequest>,
    ) -> Result<Response<pb::GetContestDetailsResponse>, Status> {
        Err(Status::unimplemented(
            "get_contest_details: not supported by self-hosted gateway",
        ))
    }

    async fn get_latest_contest(
        &self,
        _request: Request<pb::GetLatestContestRequest>,
    ) -> Result<Response<pb::GetLatestContestResponse>, Status> {
        Err(Status::unimplemented(
            "get_latest_contest: not supported by self-hosted gateway",
        ))
    }

    async fn get_contest_bettors(
        &self,
        _request: Request<pb::GetContestBettorsRequest>,
    ) -> Result<Response<pb::GetContestBettorsResponse>, Status> {
        Err(Status::unimplemented(
            "get_contest_bettors: not supported by self-hosted gateway",
        ))
    }

    async fn get_gpu_metrics(
        &self,
        _request: Request<pb::GetGpuMetricsRequest>,
    ) -> Result<Response<pb::GetGpuMetricsResponse>, Status> {
        Err(Status::unimplemented(
            "get_gpu_metrics: not supported by self-hosted gateway",
        ))
    }

    async fn get_filtered_prover_activity(
        &self,
        _request: Request<pb::GetFilteredProverActivityRequest>,
    ) -> Result<Response<pb::GetFilteredProverActivityResponse>, Status> {
        Err(Status::unimplemented(
            "get_filtered_prover_activity: not supported by self-hosted gateway",
        ))
    }

    async fn get_prover_metrics(
        &self,
        _request: Request<pb::GetProverMetricsRequest>,
    ) -> Result<Response<pb::GetProverMetricsResponse>, Status> {
        Err(Status::unimplemented(
            "get_prover_metrics: not supported by self-hosted gateway",
        ))
    }

    async fn get_filtered_bet_history(
        &self,
        _request: Request<pb::GetFilteredBetHistoryRequest>,
    ) -> Result<Response<pb::GetFilteredBetHistoryResponse>, Status> {
        Err(Status::unimplemented(
            "get_filtered_bet_history: not supported by self-hosted gateway",
        ))
    }

    async fn get_gpu_team_stats(
        &self,
        _request: Request<pb::GetGpuTeamStatsRequest>,
    ) -> Result<Response<pb::GetGpuTeamStatsResponse>, Status> {
        Err(Status::unimplemented(
            "get_gpu_team_stats: not supported by self-hosted gateway",
        ))
    }

    async fn get_config_values(
        &self,
        _request: Request<pb::GetConfigValuesRequest>,
    ) -> Result<Response<pb::GetConfigValuesResponse>, Status> {
        Err(Status::unimplemented(
            "get_config_values: not supported by self-hosted gateway",
        ))
    }

    async fn get_prover_stats(
        &self,
        _request: Request<pb::GetProverStatsRequest>,
    ) -> Result<Response<pb::GetProverStatsResponse>, Status> {
        Err(Status::unimplemented(
            "get_prover_stats: not supported by self-hosted gateway",
        ))
    }

    async fn get_filtered_prover_stats(
        &self,
        _request: Request<pb::GetFilteredProverStatsRequest>,
    ) -> Result<Response<pb::GetFilteredProverStatsResponse>, Status> {
        Err(Status::unimplemented(
            "get_filtered_prover_stats: not supported by self-hosted gateway",
        ))
    }

    async fn get_prover_search_results(
        &self,
        _request: Request<pb::GetProverSearchResultsRequest>,
    ) -> Result<Response<pb::GetProverSearchResultsResponse>, Status> {
        Err(Status::unimplemented(
            "get_prover_search_results: not supported by self-hosted gateway",
        ))
    }

    async fn get_filtered_bid_history(
        &self,
        _request: Request<pb::GetFilteredBidHistoryRequest>,
    ) -> Result<Response<pb::GetFilteredBidHistoryResponse>, Status> {
        Err(Status::unimplemented(
            "get_filtered_bid_history: not supported by self-hosted gateway",
        ))
    }

    async fn get_tee_whitelist_status(
        &self,
        _request: Request<pb::GetTeeWhitelistStatusRequest>,
    ) -> Result<Response<pb::GetTeeWhitelistStatusResponse>, Status> {
        Err(Status::unimplemented(
            "get_tee_whitelist_status: not supported by self-hosted gateway",
        ))
    }

    async fn get_settlement_request(
        &self,
        _request: Request<pb::GetSettlementRequestRequest>,
    ) -> Result<Response<pb::GetSettlementRequestResponse>, Status> {
        Err(Status::unimplemented(
            "get_settlement_request: not supported by self-hosted gateway",
        ))
    }

    async fn get_filtered_settlement_requests(
        &self,
        _request: Request<pb::GetFilteredSettlementRequestsRequest>,
    ) -> Result<Response<pb::GetFilteredSettlementRequestsResponse>, Status> {
        Err(Status::unimplemented(
            "get_filtered_settlement_requests: not supported by self-hosted gateway",
        ))
    }

    async fn get_filtered_provers(
        &self,
        _request: Request<pb::GetFilteredProversRequest>,
    ) -> Result<Response<pb::GetFilteredProversResponse>, Status> {
        Err(Status::unimplemented(
            "get_filtered_provers: not supported by self-hosted gateway",
        ))
    }

    async fn get_staker_stake_balance(
        &self,
        _request: Request<pb::GetStakerStakeBalanceRequest>,
    ) -> Result<Response<pb::GetStakerStakeBalanceResponse>, Status> {
        Err(Status::unimplemented(
            "get_staker_stake_balance: not supported by self-hosted gateway",
        ))
    }

    async fn get_prover_stake_balance(
        &self,
        _request: Request<pb::GetProverStakeBalanceRequest>,
    ) -> Result<Response<pb::GetProverStakeBalanceResponse>, Status> {
        Err(Status::unimplemented(
            "get_prover_stake_balance: not supported by self-hosted gateway",
        ))
    }

    async fn get_filtered_staker_stake_balance_logs(
        &self,
        _request: Request<pb::GetFilteredStakerStakeBalanceLogsRequest>,
    ) -> Result<Response<pb::GetFilteredStakerStakeBalanceLogsResponse>, Status> {
        Err(Status::unimplemented(
            "get_filtered_staker_stake_balance_logs: not supported by self-hosted gateway",
        ))
    }

    async fn get_filtered_prover_stake_balance_logs(
        &self,
        _request: Request<pb::GetFilteredProverStakeBalanceLogsRequest>,
    ) -> Result<Response<pb::GetFilteredProverStakeBalanceLogsResponse>, Status> {
        Err(Status::unimplemented(
            "get_filtered_prover_stake_balance_logs: not supported by self-hosted gateway",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::program_store::InMemoryProgramStore;
    use crate::proof_events::ProofEventsHub;
    use sp1_cluster_artifact::InMemoryArtifactClient;
    use sp1_cluster_common::proto::cluster_service_client::ClusterServiceClient as InnerClusterClient;
    use tonic::transport::Endpoint;

    /// Cluster client that is never actually dialed — tests only exercise paths that
    /// don't touch the cluster RPC (program registration). If a test hits the network
    /// it'll fail connecting to 127.0.0.1:1, which is the behavior we want for coverage.
    fn dummy_cluster_client() -> ClusterServiceClient {
        let channel = Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        ClusterServiceClient {
            rpc: InnerClusterClient::new(channel.clone()),
            events: sp1_cluster_common::proto::events::cluster_events_service_client::ClusterEventsServiceClient::new(channel),
            backoff: Default::default(),
        }
    }

    fn mk() -> ProverNetworkImpl<InMemoryArtifactClient> {
        ProverNetworkImpl::new(
            InMemoryArtifactClient::new(),
            dummy_cluster_client(),
            "http://gw.test".into(),
            "42".into(),
            Auth::default(),
            Arc::new(InMemoryProgramStore::new()),
            ProofEventsHub::new(),
        )
    }

    fn mk_with_auth(auth: Auth) -> ProverNetworkImpl<InMemoryArtifactClient> {
        ProverNetworkImpl::new(
            InMemoryArtifactClient::new(),
            dummy_cluster_client(),
            "http://gw.test".into(),
            "42".into(),
            auth,
            Arc::new(InMemoryProgramStore::new()),
            ProofEventsHub::new(),
        )
    }

    #[tokio::test]
    async fn create_program_then_get_program() {
        let svc = mk();
        let vk_hash = vec![0xaa, 0xbb, 0xcc, 0xdd];
        let vk = b"vk-blob".to_vec();
        let elf = b"\x7fELF fake program bytes".to_vec();
        let orphan_artifact_id = "artifact_01abcdef".to_string();
        let program_uri = format!("http://gw.test/artifacts/program/{orphan_artifact_id}");

        // Mirror what the SDK does: stage the ELF in the gateway artifact store
        // before calling create_program, so the gateway can copy it into the
        // durable program store and warm the deterministic id.
        svc.client
            .upload_raw(&orphan_artifact_id, ArtifactType::Program, elf.clone())
            .await
            .unwrap();

        let req = pb::CreateProgramRequest {
            format: 0,
            signature: vec![],
            body: Some(pb::CreateProgramRequestBody {
                nonce: 0,
                vk_hash: vk_hash.clone(),
                vk: vk.clone(),
                program_uri,
            }),
        };
        svc.create_program(Request::new(req)).await.unwrap();

        let resp = svc
            .get_program(Request::new(pb::GetProgramRequest {
                vk_hash: vk_hash.clone(),
            }))
            .await
            .unwrap()
            .into_inner();
        let program = resp.program.expect("program should be present");
        assert_eq!(program.vk_hash, vk_hash);
        assert_eq!(program.vk, vk);
        assert_eq!(
            program.program_uri,
            format!("http://gw.test/programs/{}", hex::encode(&vk_hash))
        );

        // Durable copy in ProgramStore.
        let (got_vk, got_elf) = svc.program_store.get(&vk_hash).await.unwrap().unwrap();
        assert_eq!(got_vk, vk);
        assert_eq!(got_elf, elf);

        // Warm copy in cluster store under the deterministic id (so subsequent
        // request_proof calls can skip the upload).
        let det_id = program_artifact_id(&vk_hash);
        assert!(svc
            .client
            .exists(&det_id, ArtifactType::Program)
            .await
            .unwrap());
        let warm_bytes = svc
            .client
            .download_raw(&det_id, ArtifactType::Program)
            .await
            .unwrap();
        assert_eq!(warm_bytes, elf);

        // Orphaned SDK-uploaded artifact is cleaned up.
        assert!(!svc
            .client
            .exists(&orphan_artifact_id, ArtifactType::Program)
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn create_program_fails_if_elf_not_uploaded() {
        let svc = mk();
        let vk_hash = vec![0x01, 0x02];
        let req = pb::CreateProgramRequest {
            format: 0,
            signature: vec![],
            body: Some(pb::CreateProgramRequestBody {
                nonce: 0,
                vk_hash,
                vk: b"vk".to_vec(),
                program_uri: "http://gw.test/artifacts/program/artifact_missing".into(),
            }),
        };
        let err = svc.create_program(Request::new(req)).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn get_program_unknown_vk_hash_returns_not_found() {
        // The SDK's NetworkClient routes on NotFound to trigger create_program;
        // any Ok response is treated as "already registered". See
        // sp1/crates/sdk/src/network/client.rs:278.
        let svc = mk();
        let err = svc
            .get_program(Request::new(pb::GetProgramRequest {
                vk_hash: vec![9, 9, 9],
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn create_program_rejects_missing_body() {
        let svc = mk();
        let req = pb::CreateProgramRequest {
            format: 0,
            signature: vec![],
            body: None,
        };
        let err = svc.create_program(Request::new(req)).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn request_proof_rejects_missing_body() {
        let svc = mk();
        let req = pb::RequestProofRequest {
            format: 0,
            signature: vec![],
            body: None,
        };
        let err = svc.request_proof(Request::new(req)).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn get_nonce_is_monotonic_per_address() {
        let svc = mk();
        let addr_a = vec![0xaa; 20];
        let addr_b = vec![0xbb; 20];

        let call = |a: Vec<u8>| async {
            svc.get_nonce(Request::new(pb::GetNonceRequest { address: a }))
                .await
                .unwrap()
                .into_inner()
                .nonce
        };

        assert_eq!(call(addr_a.clone()).await, 0);
        assert_eq!(call(addr_a.clone()).await, 1);
        assert_eq!(call(addr_b.clone()).await, 0);
        assert_eq!(call(addr_a.clone()).await, 2);
        assert_eq!(call(addr_b.clone()).await, 1);
    }

    #[tokio::test]
    async fn get_balance_returns_configured_amount() {
        let svc = mk();
        let resp = svc
            .get_balance(Request::new(pb::GetBalanceRequest {
                address: vec![1, 2, 3],
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.amount, "42");
    }

    #[tokio::test]
    async fn create_program_rejects_bad_signature_in_verify_mode() {
        use crate::auth::AuthMode;
        let svc = mk_with_auth(Auth {
            mode: AuthMode::Verify,
            allowlist: Default::default(),
        });
        let req = pb::CreateProgramRequest {
            format: 0,
            signature: vec![],
            body: Some(pb::CreateProgramRequestBody {
                nonce: 0,
                vk_hash: vec![1, 2, 3],
                vk: vec![4, 5, 6],
                program_uri: "http://gw.test/artifacts/program/artifact_abc".into(),
            }),
        };
        let err = svc.create_program(Request::new(req)).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn request_proof_without_registered_program_fails_precondition() {
        let svc = mk();
        let body = pb::RequestProofRequestBody {
            nonce: 0,
            vk_hash: vec![1, 2, 3],
            version: "v0".into(),
            mode: pb::ProofMode::Core as i32,
            strategy: pb::FulfillmentStrategy::Reserved as i32,
            stdin_uri: "http://gw.test/artifacts/stdin/artifact_stdin".into(),
            deadline: 0,
            cycle_limit: 0,
            gas_limit: 0,
            min_auction_period: 0,
            whitelist: vec![],
        };
        let req = pb::RequestProofRequest {
            format: 0,
            signature: vec![],
            body: Some(body),
        };
        let err = svc.request_proof(Request::new(req)).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    }
}
