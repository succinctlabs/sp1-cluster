use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use sp1_cluster_common::proto::{
    cluster_service_server::ClusterService, ClusterComponentManifest, ExecutionResult,
    ProofRequest, ProofRequestCancelRequest, ProofRequestCreateRequest, ProofRequestGetRequest,
    ProofRequestGetResponse, ProofRequestListRequest, ProofRequestListResponse, ProofRequestStatus,
    ProofRequestUpdateRequest, SetClusterComponentInfoRequest,
};
use sqlx::{PgPool, QueryBuilder};
use time::OffsetDateTime;
use tonic::{Request, Response, Status};
use tracing::{error, info, warn};

// Database models
#[derive(Debug, sqlx::FromRow)]
#[allow(dead_code)]
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
    extra_data: Option<String>,
    scheduled_by: Option<String>,
    stdin_private: bool,
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
            extra_data: self.extra_data,
            scheduled_by: self.scheduled_by,
            stdin_private: self.stdin_private,
        }
    }
}

/// Implementation of the ClusterService gRPC service
pub struct ClusterServiceImpl {
    db_pool: Arc<PgPool>,
    /// Latest cluster component build manifest pushed by the coordinator, held in
    /// memory only: it is periodically re-pushed telemetry, so it repopulates within
    /// one push interval after an API restart. `updated_at == 0` means no coordinator
    /// has pushed a manifest since startup.
    component_manifest: RwLock<ClusterComponentManifest>,
}

impl ClusterServiceImpl {
    /// Create a new ClusterServiceImpl with the given database pool
    pub fn new(db_pool: Arc<PgPool>) -> Self {
        Self {
            db_pool,
            component_manifest: RwLock::new(ClusterComponentManifest::default()),
        }
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
        let result = sqlx::query!("INSERT INTO proof_requests (id, proof_status, requester, stdin_artifact_id, program_artifact_id, options_artifact_id, proof_artifact_id, cycle_limit, deadline, gas_limit, handled, metadata, scheduled_by, stdin_private) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)",
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
            req.scheduled_by.as_ref(),
            req.stdin_private,
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
                // Idempotent: 0 rows means the row is missing or already terminal,
                // so the cancel goal (not Pending) is already met.
                if result.rows_affected() > 0 {
                    info!("Successfully cancelled proof request: {}", req.proof_id);
                }
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

        // A proof_status write is the Pending→terminal completion; guard the whole UPDATE (below) on
        // `proof_status = Pending` so a re-issue can't clobber a row that moved on (e.g. Cancelled).
        // Guards every field in the request — but only the completion writer sets proof_status today.
        let guard_pending = req.proof_status.is_some();

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

        if let Some(extra_data) = req.extra_data {
            separated.push("extra_data = ");
            separated.push_bind_unseparated(extra_data);
        }

        query.push(" WHERE id = ");
        query.push_bind(&req.proof_id);
        // Only land a completion write from Pending, so it can't clobber a row that's moved on
        // (e.g. Cancelled) or a gone row.
        if guard_pending {
            query.push(" AND proof_status = ");
            query.push_bind(ProofRequestStatus::Pending as i16);
        }

        let result = query.build().execute(&*self.db_pool).await;

        match result {
            Ok(result) => {
                if result.rows_affected() == 0 {
                    if guard_pending {
                        // Row is no longer Pending (already terminal / cancelled / gone) — the
                        // completion write is moot. Succeed idempotently so the caller stops tracking it.
                        info!(
                            "Proof request {} no longer Pending; completion write skipped",
                            req.proof_id
                        );
                        return Ok(Response::new(()));
                    }
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
                error!("Failed to update proof request {}: {:?}", req.proof_id, e);
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
        .map_err(|e| Status::internal(format!("Failed to get proof request: {e}")))?;

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

        if let Some(ref scheduled_by) = req.scheduled_by {
            query
                .push(" AND scheduled_by = ")
                .push_bind(scheduled_by.clone());
        }

        let limit = req.limit.unwrap_or(10).min(1000);
        query.push(" LIMIT ").push_bind(limit as i32);

        let offset = req.offset.unwrap_or(0);
        query.push(" OFFSET ").push_bind(offset as i32);

        let proof_requests = query
            .build_query_as::<DbProofRequest>()
            .fetch_all(&*self.db_pool)
            .await
            .map_err(|e| Status::internal(format!("Failed to list proof requests: {e:?}")))?;

        let response = ProofRequestListResponse {
            proof_requests: proof_requests
                .into_iter()
                .map(|pr| pr.into_proto())
                .collect(),
        };

        Ok(Response::new(response))
    }

    async fn set_cluster_component_info(
        &self,
        request: Request<SetClusterComponentInfoRequest>,
    ) -> Result<Response<()>, Status> {
        let components = request.into_inner().components;
        let updated_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| Status::internal(format!("system time before unix epoch: {e}")))?
            .as_secs();
        // Full-snapshot replace; updated_at is server-owned so readers can judge
        // freshness against a clock the coordinator doesn't control.
        *self
            .component_manifest
            .write()
            .unwrap_or_else(|e| e.into_inner()) = ClusterComponentManifest {
            components,
            updated_at,
        };
        Ok(Response::new(()))
    }

    async fn get_cluster_component_info(
        &self,
        _request: Request<()>,
    ) -> Result<Response<ClusterComponentManifest>, Status> {
        Ok(Response::new(
            self.component_manifest
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .clone(),
        ))
    }

    async fn healthcheck(&self, _request: Request<()>) -> Result<Response<()>, Status> {
        Ok(Response::new(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sp1_cluster_common::proto::ClusterComponentInfo;

    /// The manifest RPCs never touch the database, so a lazy (unconnected) pool is enough.
    fn service() -> ClusterServiceImpl {
        let pool = PgPool::connect_lazy("postgres://unused:unused@localhost:1/unused").unwrap();
        ClusterServiceImpl::new(Arc::new(pool))
    }

    fn entry(component: &str, git_sha: &str) -> ClusterComponentInfo {
        ClusterComponentInfo {
            component: component.to_string(),
            version: "2.5.3".to_string(),
            git_sha: git_sha.to_string(),
            image_tag: format!("base-{git_sha}"),
        }
    }

    #[tokio::test]
    async fn manifest_is_empty_with_zero_updated_at_before_any_push() {
        let svc = service();
        let manifest = svc
            .get_cluster_component_info(Request::new(()))
            .await
            .unwrap()
            .into_inner();
        assert!(manifest.components.is_empty());
        assert_eq!(manifest.updated_at, 0, "no push yet => updated_at 0");
    }

    #[tokio::test]
    async fn set_stores_manifest_and_stamps_server_owned_updated_at() {
        let svc = service();
        svc.set_cluster_component_info(Request::new(SetClusterComponentInfoRequest {
            components: vec![
                entry("coordinator", "abc1234"),
                entry("cpu-node", "abc1234"),
            ],
        }))
        .await
        .unwrap();

        let manifest = svc
            .get_cluster_component_info(Request::new(()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(manifest.components.len(), 2);
        assert_eq!(manifest.components[0].component, "coordinator");
        assert!(manifest.updated_at > 0, "server stamps updated_at");
    }

    #[tokio::test]
    async fn set_replaces_previous_manifest_full_snapshot() {
        let svc = service();
        svc.set_cluster_component_info(Request::new(SetClusterComponentInfoRequest {
            components: vec![entry("coordinator", "oldsha"), entry("gpu-node", "oldsha")],
        }))
        .await
        .unwrap();
        svc.set_cluster_component_info(Request::new(SetClusterComponentInfoRequest {
            components: vec![entry("coordinator", "newsha")],
        }))
        .await
        .unwrap();

        let manifest = svc
            .get_cluster_component_info(Request::new(()))
            .await
            .unwrap()
            .into_inner();
        // Replace, not merge: the gpu-node entry from the first push is gone.
        assert_eq!(manifest.components.len(), 1);
        assert_eq!(manifest.components[0].git_sha, "newsha");
    }
}
