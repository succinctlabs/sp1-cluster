use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::num::NonZeroU16;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sp1_cluster_artifact::{ArtifactClient, ArtifactType, InMemoryArtifactClient};
use sp1_cluster_common::proto::{
    self as cluster_pb,
    cluster_service_server::{ClusterService, ClusterServiceServer},
};
use sp1_cluster_utils::{
    request_proofs, ArtifactStoreConfig, ClusterElf, ProofRequestConfig, ProofRequestResults,
};
use sp1_sdk::{
    network::proto::types::ProofMode, ProofFromNetwork, SP1Proof, SP1PublicValues, SP1Stdin,
};
use tokio::sync::oneshot;

#[derive(Clone)]
struct BarrierCluster {
    artifacts: InMemoryArtifactClient,
    expected: usize,
    requests: Arc<Mutex<HashMap<String, cluster_pb::ProofRequest>>>,
    cancelled: Arc<Mutex<HashSet<String>>>,
    fail_create_at: Option<usize>,
    fail_get: bool,
}

impl BarrierCluster {
    fn new(expected: u16) -> Self {
        Self {
            artifacts: InMemoryArtifactClient::new(),
            expected: usize::from(expected),
            requests: Arc::default(),
            cancelled: Arc::default(),
            fail_create_at: None,
            fail_get: false,
        }
    }

    fn cancelled_count(&self) -> usize {
        self.cancelled.lock().unwrap().len()
    }
}

#[tonic::async_trait]
impl ClusterService for BarrierCluster {
    async fn proof_request_create(
        &self,
        request: tonic::Request<cluster_pb::ProofRequestCreateRequest>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        let req = request.into_inner();
        let mut requests = self.requests.lock().unwrap();
        if self.fail_create_at == Some(requests.len()) {
            return Err(tonic::Status::invalid_argument("simulated create failure"));
        }
        requests.insert(
            req.proof_id.clone(),
            cluster_pb::ProofRequest {
                id: req.proof_id,
                proof_status: cluster_pb::ProofRequestStatus::Pending as i32,
                requester: req.requester,
                execution_result: None,
                stdin_artifact_id: req.stdin_artifact_id,
                program_artifact_id: req.program_artifact_id,
                proof_artifact_id: req.proof_artifact_id,
                options_artifact_id: req.options_artifact_id,
                cycle_limit: Some(req.cycle_limit),
                gas_limit: Some(req.gas_limit),
                deadline: req.deadline,
                handled: true,
                metadata: String::new(),
                created_at: 0,
                updated_at: 0,
                extra_data: None,
                scheduled_by: req.scheduled_by,
                stdin_private: req.stdin_private,
            },
        );
        Ok(tonic::Response::new(()))
    }

    async fn proof_request_get(
        &self,
        request: tonic::Request<cluster_pb::ProofRequestGetRequest>,
    ) -> Result<tonic::Response<cluster_pb::ProofRequestGetResponse>, tonic::Status> {
        if self.fail_get {
            return Err(tonic::Status::invalid_argument("simulated poll failure"));
        }
        let id = request.into_inner().proof_id;
        let (mut proof_request, request_count) = {
            let requests = self.requests.lock().unwrap();
            let proof_request = requests
                .get(&id)
                .cloned()
                .ok_or_else(|| tonic::Status::not_found(id))?;
            (proof_request, requests.len())
        };

        if request_count >= self.expected {
            let proof_artifact_id = proof_request
                .proof_artifact_id
                .clone()
                .expect("proof_artifact_id set");
            let proof = ProofFromNetwork {
                proof: SP1Proof::Core(vec![]),
                public_values: SP1PublicValues::new(),
                sp1_version: String::new(),
            };
            self.artifacts
                .upload_with_type(&proof_artifact_id, ArtifactType::Proof, proof)
                .await
                .expect("upload canned proof");
            proof_request.proof_status = cluster_pb::ProofRequestStatus::Completed as i32;
        }

        Ok(tonic::Response::new(cluster_pb::ProofRequestGetResponse {
            proof_request: Some(proof_request),
        }))
    }

    async fn proof_request_cancel(
        &self,
        request: tonic::Request<cluster_pb::ProofRequestCancelRequest>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        self.cancelled
            .lock()
            .unwrap()
            .insert(request.into_inner().proof_id);
        Ok(tonic::Response::new(()))
    }

    async fn proof_request_update(
        &self,
        _request: tonic::Request<cluster_pb::ProofRequestUpdateRequest>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        Err(tonic::Status::unimplemented("update"))
    }

    async fn proof_request_list(
        &self,
        _request: tonic::Request<cluster_pb::ProofRequestListRequest>,
    ) -> Result<tonic::Response<cluster_pb::ProofRequestListResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("list"))
    }

    async fn set_cluster_component_info(
        &self,
        _request: tonic::Request<cluster_pb::SetClusterComponentInfoRequest>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        Err(tonic::Status::unimplemented("set_cluster_component_info"))
    }

    async fn get_cluster_component_info(
        &self,
        _request: tonic::Request<()>,
    ) -> Result<tonic::Response<cluster_pb::ClusterComponentManifest>, tonic::Status> {
        Err(tonic::Status::unimplemented("get_cluster_component_info"))
    }

    async fn healthcheck(
        &self,
        _request: tonic::Request<()>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        Ok(tonic::Response::new(()))
    }
}

async fn run_batch(cluster: BarrierCluster, count: u16) -> eyre::Result<Vec<ProofRequestResults>> {
    let artifacts = cluster.artifacts.clone();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    drop(listener);
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(ClusterServiceServer::new(cluster))
            .serve_with_shutdown(addr, async move {
                shutdown_rx.await.ok();
            })
            .await
            .unwrap();
    });

    let config = ProofRequestConfig {
        cluster_rpc: format!("http://{addr}"),
        mode: ProofMode::Compressed,
        timeout_hours: 1,
        artifact_store: ArtifactStoreConfig::Redis { nodes: vec![] },
    };

    let result = tokio::time::timeout(
        Duration::from_secs(30),
        request_proofs(
            artifacts,
            ClusterElf::NewElf(vec![0; 32]),
            SP1Stdin::new(),
            NonZeroU16::new(count).expect("test count is positive"),
            &config,
        ),
    )
    .await
    .expect("batch request timed out");

    shutdown_tx.send(()).ok();
    server.await.unwrap();

    result
}

async fn request_batch(count: u16) -> Vec<ProofRequestResults> {
    run_batch(BarrierCluster::new(count), count)
        .await
        .expect("request_proofs failed")
}

#[tokio::test]
async fn requests_are_created_before_any_is_awaited() {
    let results = request_batch(4).await;

    assert_eq!(results.len(), 4);
    let ids: std::collections::HashSet<_> = results.iter().map(|r| r.proof_id.clone()).collect();
    assert_eq!(ids.len(), 4, "proof ids must be unique");
}

#[tokio::test]
async fn single_request_still_works() {
    assert_eq!(request_batch(1).await.len(), 1);
}

#[tokio::test]
async fn create_failure_cancels_submitted_requests() {
    let mut cluster = BarrierCluster::new(4);
    cluster.fail_create_at = Some(2);
    let observed = cluster.clone();

    let error = run_batch(cluster, 4)
        .await
        .err()
        .expect("batch creation should fail");

    assert!(error.to_string().contains("simulated create failure"));
    assert_eq!(observed.cancelled_count(), 2);
}

#[tokio::test]
async fn poll_failure_cancels_pending_requests() {
    let mut cluster = BarrierCluster::new(4);
    cluster.fail_get = true;
    let observed = cluster.clone();

    let error = run_batch(cluster, 4)
        .await
        .err()
        .expect("batch polling should fail");

    assert!(error.to_string().contains("simulated poll failure"));
    assert_eq!(observed.cancelled_count(), 4);
}

#[tokio::test(start_paused = true)]
async fn unreachable_cluster_gives_up() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let config = ProofRequestConfig {
        cluster_rpc: format!("http://{addr}"),
        mode: ProofMode::Compressed,
        timeout_hours: 1,
        artifact_store: ArtifactStoreConfig::Redis { nodes: vec![] },
    };

    let err = tokio::time::timeout(
        Duration::from_secs(60),
        request_proofs(
            InMemoryArtifactClient::new(),
            ClusterElf::NewElf(vec![0; 32]),
            SP1Stdin::new(),
            NonZeroU16::MIN,
            &config,
        ),
    )
    .await
    .expect("kept retrying a refused connection")
    .map(|_| ())
    .expect_err("no cluster is listening");

    assert!(err.to_string().contains("CLI_CLUSTER_RPC"), "{err}");
}
