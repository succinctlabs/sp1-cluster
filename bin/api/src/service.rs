use std::sync::Arc;

use sp1_cluster_common::proto::{
    cluster_service_server::ClusterService, ExecutionResult, ProofRequest,
    ProofRequestCancelRequest, ProofRequestCreateRequest, ProofRequestGetRequest,
    ProofRequestGetResponse, ProofRequestListRequest, ProofRequestListResponse, ProofRequestStatus,
    ProofRequestUpdateRequest,
};
use sqlx::{PgPool, QueryBuilder};
use time::OffsetDateTime;
use tonic::{Request, Response, Status};
use tracing::{error, info, warn};

// Database models
#[derive(Debug, sqlx::FromRow)]
struct DbProofRequest {
    id: String,
    proof_status: i32,
    requester: Vec<u8>,
    execution_status: Option<i32>,
    execution_failure_cause: Option<i32>,
    execution_cycles: Option<i64>,
    execution_gas: Option<i64>,
    execution_public_values_hash: Option<Vec<u8>>,
    stdin_artifact_id: String,
    program_artifact_id: String,
    proof_artifact_id: Option<String>,
    options_artifact_id: Option<String>,
    cycle_limit: Option<i64>,
    gas_limit: Option<i64>,
    deadline: i64,
    handled: bool,
    metadata: String,
    created_at: OffsetDateTime,
    updated_at: OffsetDateTime,
}

impl DbProofRequest {
    pub(crate) fn into_proto(self) -> ProofRequest {
        ProofRequest {
            id: self.id,
            proof_status: self.proof_status,
            requester: self.requester,
            execution_result: self.execution_status.map(|status| ExecutionResult {
                status,
                failure_cause: self.execution_failure_cause.unwrap(),
                cycles: self.execution_cycles.unwrap() as u64,
                gas: self.execution_gas.unwrap() as u64,
                public_values_hash: self.execution_public_values_hash.unwrap(),
            }),
            stdin_artifact_id: self.stdin_artifact_id,
            program_artifact_id: self.program_artifact_id,
            proof_artifact_id: self.proof_artifact_id,
            options_artifact_id: self.options_artifact_id,
            cycle_limit: self.cycle_limit.map(|limit| limit as u64),
            gas_limit: self.gas_limit.map(|limit| limit as u64),
            deadline: self.deadline as u64,
            handled: self.handled,
            metadata: self.metadata,
            created_at: self.created_at.unix_timestamp() as u64,
            updated_at: self.updated_at.unix_timestamp() as u64,
        }
    }
}

/// Implementation of the ClusterService gRPC service
pub struct ClusterServiceImpl {
    db_pool: Arc<PgPool>,
}

impl ClusterServiceImpl {
    /// Create a new ClusterServiceImpl with the given database pool
    pub fn new(db_pool: Arc<PgPool>) -> Self {
        Self { db_pool }
    }
}

#[async_trait::async_trait]
impl ClusterService for ClusterServiceImpl {
    async fn proof_request_create(
        &self,
        request: Request<ProofRequestCreateRequest>,
    ) -> Result<Response<()>, Status> {
        let req = request.into_inner();
        info!("Creating proof request with ID: {}", req.proof_id);

        // Insert the proof request into the database
        let result = sqlx::query!("INSERT INTO proof_requests (id, proof_status, requester, stdin_artifact_id, program_artifact_id, options_artifact_id, proof_artifact_id, cycle_limit, deadline, gas_limit, handled, metadata) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
            req.proof_id,
            ProofRequestStatus::Pending as i16,
            &req.requester,
            &req.stdin_artifact_id,
            &req.program_artifact_id,
            req.options_artifact_id.as_ref(),
            req.proof_artifact_id.as_ref(),
            req.cycle_limit as i64,
            req.deadline as i64,
            req.gas_limit as i64,
            false,
            "null".to_string(),
        )
        .execute(&*self.db_pool)
        .await;

        match result {
            Ok(_) => {
                info!("Successfully created proof request: {}", req.proof_id);
                Ok(Response::new(()))
            }
            Err(e) => {
                error!("Failed to create proof request: {:?}", e);
                Err(Status::internal("Failed to create proof request"))
            }
        }
    }

    async fn proof_request_cancel(
        &self,
        request: Request<ProofRequestCancelRequest>,
    ) -> Result<Response<()>, Status> {
        let req = request.into_inner();
        info!("Cancelling proof request {}", req.proof_id);

        let result = sqlx::query!(
            "UPDATE proof_requests SET proof_status = $1 WHERE id = $2 and proof_status = $3",
            ProofRequestStatus::Cancelled as i16,
            req.proof_id,
            ProofRequestStatus::Pending as i16,
        )
        .execute(&*self.db_pool)
        .await;

        match result {
            Ok(result) => {
                if result.rows_affected() == 0 {
                    warn!("Pending proof request {} not found", req.proof_id);
                    return Err(Status::not_found(format!(
                        "Pending proof request {} not found",
                        req.proof_id
                    )));
                }
                info!("Successfully cancelled proof request: {}", req.proof_id);
                Ok(Response::new(()))
            }
            Err(e) => {
                error!("Failed to cancel proof request: {:?}", e);
                Err(Status::internal("Failed to cancel proof request"))
            }
        }
    }

    async fn proof_request_update(
        &self,
        request: Request<ProofRequestUpdateRequest>,
    ) -> Result<Response<()>, Status> {
        let req = request.into_inner();
        info!("Updating proof request with ID: {}", req.proof_id);

        // If we're not updating anything return an error.
        if req
            == (ProofRequestUpdateRequest {
                proof_id: req.proof_id.clone(),
                ..Default::default()
            })
        {
            return Err(Status::invalid_argument("No updates specified"));
        }

        let mut query = QueryBuilder::new("UPDATE proof_requests SET ");

        let mut separated = query.separated(", ");

        if let Some(status) = req.proof_status {
            separated.push("proof_status = ");
            separated.push_bind_unseparated(status);
        }

        if let Some(handled) = req.handled {
            separated.push("handled = ");
            separated.push_bind_unseparated(handled);
        }

        if let Some(deadline) = req.deadline {
            separated.push("deadline = ");
            separated.push_bind_unseparated(deadline as i64);
        }

        if let Some(execution_result) = req.execution_result {
            separated.push("execution_status = ");
            separated.push_bind_unseparated(execution_result.status);
            separated.push("execution_failure_cause = ");
            separated.push_bind_unseparated(execution_result.failure_cause);
            separated.push("execution_cycles = ");
            separated.push_bind_unseparated(execution_result.cycles as i64);
            separated.push("execution_gas = ");
            separated.push_bind_unseparated(execution_result.gas as i64);
            separated.push("execution_public_values_hash = ");
            separated.push_bind_unseparated(execution_result.public_values_hash);
        }

        if let Some(metadata) = req.metadata {
            separated.push("metadata = ");
            separated.push_bind_unseparated(metadata);
        }

        query.push(" WHERE id = ");
        query.push_bind(&req.proof_id);

        info!("query: {}", query.sql());
        let result = query.build().execute(&*self.db_pool).await;

        match result {
            Ok(result) => {
                if result.rows_affected() == 0 {
                    warn!("Proof request {} not found", req.proof_id);
                    return Err(Status::not_found(format!(
                        "Proof request {} not found",
                        req.proof_id
                    )));
                }
                info!("Successfully updated proof request: {}", req.proof_id);
                Ok(Response::new(()))
            }
            Err(e) => {
                error!("Failed to update proof request: {:?}", e);
                Err(Status::internal("Failed to update proof request"))
            }
        }
    }

    async fn proof_request_get(
        &self,
        request: Request<ProofRequestGetRequest>,
    ) -> Result<Response<ProofRequestGetResponse>, Status> {
        let req = request.into_inner();
        info!("Getting proof request with ID: {}", req.proof_id);

        let proof_request = sqlx::query_as!(
            DbProofRequest,
            "SELECT * FROM proof_requests WHERE id = $1",
            req.proof_id
        )
        .fetch_one(&*self.db_pool)
        .await
        .map_err(|e| Status::internal(format!("Failed to get proof request: {}", e)))?;

        let response = ProofRequestGetResponse {
            proof_request: Some(proof_request.into_proto()),
        };

        Ok(Response::new(response))
    }

    async fn proof_request_list(
        &self,
        request: Request<ProofRequestListRequest>,
    ) -> Result<Response<ProofRequestListResponse>, Status> {
        let req = request.into_inner();

        // Dynamic query based on request
        let mut query = QueryBuilder::new("SELECT * FROM proof_requests WHERE 1=1");

        if !req.proof_status.is_empty() {
            let mut separated = query.push(" AND proof_status IN (").separated(",");
            for status in req.proof_status {
                separated.push_bind(status);
            }
            query.push(")");
        }

        if !req.execution_status.is_empty() {
            let mut separated = query.push(" AND execution_status IN (").separated(",");
            for status in req.execution_status {
                separated.push_bind(status);
            }
            query.push(")");
        }

        if let Some(minimum_deadline) = req.minimum_deadline {
            query
                .push(" AND deadline >= ")
                .push_bind(minimum_deadline as i64);
        }

        if let Some(handled) = req.handled {
            query.push(" AND handled = ").push_bind(handled);
        }

        let limit = req.limit.unwrap_or(10).min(1000);
        query.push(" LIMIT ").push_bind(limit as i32);

        let offset = req.offset.unwrap_or(0);
        query.push(" OFFSET ").push_bind(offset as i32);

        let proof_requests = query
            .build_query_as::<DbProofRequest>()
            .fetch_all(&*self.db_pool)
            .await
            .map_err(|e| Status::internal(format!("Failed to list proof requests: {:?}", e)))?;

        let response = ProofRequestListResponse {
            proof_requests: proof_requests
                .into_iter()
                .map(|pr| pr.into_proto())
                .collect(),
        };

        Ok(Response::new(response))
    }

    async fn healthcheck(&self, _request: Request<()>) -> Result<Response<()>, Status> {
        Ok(Response::new(()))
    }
}
