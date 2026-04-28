//! End-to-end test that drives the gateway's gRPC + HTTP surface exactly the way
//! sp1-sdk's `NetworkProver` would. Stops short of running `client.prove(...)`
//! (which requires a real ELF + heavy setup) — instead exercises the proto
//! contract directly with the SDK's generated tonic clients. That's enough to
//! cover: `create_artifact` → PUT → `create_program` → `create_artifact` → PUT
//! → `request_proof` → `get_proof_request_status` polling → proof download.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use sp1_cluster_artifact::{ArtifactClient, ArtifactType, InMemoryArtifactClient};
use sp1_cluster_common::{
    client::ClusterServiceClient,
    proto as cluster_pb,
    proto::{
        cluster_service_client::ClusterServiceClient as InnerClusterClient,
        cluster_service_server::{ClusterService, ClusterServiceServer},
    },
};
use sp1_cluster_network_gateway::{
    auth::Auth,
    config::Config,
    proof_events::ProofEventsHub,
    program_store::{InMemoryProgramStore, ProgramStore},
    serve,
};
use sp1_sdk::network::proto::{
    artifact::{
        artifact_store_client::ArtifactStoreClient, ArtifactType as SdkArtifactType,
        CreateArtifactRequest,
    },
    base::{
        network::prover_network_client::ProverNetworkClient,
        types::{
            CreateProgramRequest, CreateProgramRequestBody, FulfillmentStatus,
            GetProofRequestStatusRequest, MessageFormat, ProofMode, RequestProofRequest,
            RequestProofRequestBody,
        },
    },
};
use tokio::sync::oneshot;
use tonic::transport::{Channel, Endpoint};

/// In-memory fake of the cluster's API + coordinator + node. When a proof
/// request lands it pre-populates the shared artifact store with a canned
/// proof blob, then on the next `proof_request_get` reports Completed so the
/// SDK-style polling loop terminates.
#[derive(Clone)]
struct FakeCluster {
    artifacts: InMemoryArtifactClient,
    proof_bytes: Vec<u8>, // raw bincode bytes to serve as the proof (pre-zstd is the store's job)
    requests: Arc<dashmap::DashMap<String, cluster_pb::ProofRequest>>,
}

impl FakeCluster {
    fn new(artifacts: InMemoryArtifactClient, proof_bytes: Vec<u8>) -> Self {
        Self {
            artifacts,
            proof_bytes,
            requests: Arc::new(dashmap::DashMap::new()),
        }
    }
}

#[tonic::async_trait]
impl ClusterService for FakeCluster {
    async fn proof_request_create(
        &self,
        request: tonic::Request<cluster_pb::ProofRequestCreateRequest>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        let req = request.into_inner();

        // Pre-populate the proof artifact so a later GET on the proof_uri serves
        // what we canned. The gateway's HTTP `PUT` path zstd-wraps via
        // `upload_raw_compressed`; here we need the STORE-LEVEL bytes, which
        // `download_raw` will zstd-decode back to raw bincode on GET. For the
        // in-memory backend the two layers are identity, so just upload the
        // already-bincoded proof bytes.
        let proof_artifact_id = req
            .proof_artifact_id
            .clone()
            .expect("proof_artifact_id set");
        self.artifacts
            .upload_raw(
                &proof_artifact_id,
                ArtifactType::Proof,
                self.proof_bytes.clone(),
            )
            .await
            .expect("upload canned proof");

        self.requests.insert(
            req.proof_id.clone(),
            cluster_pb::ProofRequest {
                id: req.proof_id,
                proof_status: cluster_pb::ProofRequestStatus::Completed as i32,
                requester: req.requester,
                execution_result: Some(cluster_pb::ExecutionResult {
                    status: cluster_pb::ExecutionStatus::Executed as i32,
                    failure_cause: 0,
                    cycles: 0,
                    gas: 0,
                    public_values_hash: vec![],
                }),
                stdin_artifact_id: req.stdin_artifact_id,
                program_artifact_id: req.program_artifact_id,
                proof_artifact_id: Some(proof_artifact_id),
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
            },
        );
        Ok(tonic::Response::new(()))
    }

    async fn proof_request_cancel(
        &self,
        _request: tonic::Request<cluster_pb::ProofRequestCancelRequest>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        Err(tonic::Status::unimplemented("cancel"))
    }

    async fn proof_request_update(
        &self,
        _request: tonic::Request<cluster_pb::ProofRequestUpdateRequest>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        Err(tonic::Status::unimplemented("update"))
    }

    async fn proof_request_get(
        &self,
        request: tonic::Request<cluster_pb::ProofRequestGetRequest>,
    ) -> Result<tonic::Response<cluster_pb::ProofRequestGetResponse>, tonic::Status> {
        let id = request.into_inner().proof_id;
        let resp = cluster_pb::ProofRequestGetResponse {
            proof_request: self.requests.get(&id).map(|r| r.clone()),
        };
        Ok(tonic::Response::new(resp))
    }

    async fn proof_request_list(
        &self,
        _request: tonic::Request<cluster_pb::ProofRequestListRequest>,
    ) -> Result<tonic::Response<cluster_pb::ProofRequestListResponse>, tonic::Status> {
        Ok(tonic::Response::new(cluster_pb::ProofRequestListResponse {
            proof_requests: self.requests.iter().map(|r| r.clone()).collect(),
        }))
    }

    async fn healthcheck(
        &self,
        _request: tonic::Request<()>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        Ok(tonic::Response::new(()))
    }
}

/// Pick a free localhost port by briefly binding to 0.
fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

#[tokio::test]
async fn e2e_register_program_request_proof_download() {
    // ---- canned proof bytes (raw bincode, per SDK wire format) ----
    let proof_bytes: Vec<u8> = (0..1024u16).flat_map(|x| x.to_le_bytes()).collect();

    // ---- shared in-memory artifact store ----
    let artifacts = InMemoryArtifactClient::new();

    // ---- start the fake ClusterService on an ephemeral port ----
    let cluster_port = free_port();
    let cluster_addr: SocketAddr = format!("127.0.0.1:{cluster_port}").parse().unwrap();
    let fake = FakeCluster::new(artifacts.clone(), proof_bytes.clone());
    let (cluster_shutdown_tx, cluster_shutdown_rx) = oneshot::channel::<()>();
    let cluster_server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(ClusterServiceServer::new(fake))
            .serve_with_shutdown(cluster_addr, async move {
                cluster_shutdown_rx.await.ok();
            })
            .await
            .unwrap();
    });

    // ---- connect a real `ClusterServiceClient` to the fake ----
    // Use a lazy channel so we don't race the server's readiness.
    let cluster_rpc = format!("http://{cluster_addr}");
    let channel = Endpoint::from_shared(cluster_rpc.clone())
        .unwrap()
        .connect_lazy();
    let cluster = ClusterServiceClient {
        rpc: InnerClusterClient::new(channel.clone()),
        events: sp1_cluster_common::proto::events::cluster_events_service_client::ClusterEventsServiceClient::new(channel),
        backoff: Default::default(),
    };

    // ---- start the gateway ----
    let grpc_port = free_port();
    let http_port = free_port();
    let public_http_url = format!("http://127.0.0.1:{http_port}");
    let cfg = Config {
        grpc_addr: format!("127.0.0.1:{grpc_port}"),
        http_addr: format!("127.0.0.1:{http_port}"),
        public_http_url: public_http_url.clone(),
        cluster_rpc: cluster_rpc.clone(),
        artifact_store: "unused".into(),
        s3_bucket: None,
        s3_region: None,
        s3_concurrency: 0,
        redis_nodes: None,
        redis_pool_max_size: 0,
        balance_amount: None,
        auth_mode: sp1_cluster_network_gateway::auth::AuthMode::None,
        auth_allowlist: None,
        program_store: "memory".into(),
        program_store_dir: None,
    };
    let program_store: Arc<dyn ProgramStore> = Arc::new(InMemoryProgramStore::new());
    let (gw_grpc_shutdown_tx, gw_grpc_shutdown_rx) = oneshot::channel::<()>();
    let (gw_http_shutdown_tx, gw_http_shutdown_rx) = oneshot::channel::<()>();
    let gateway_artifacts = artifacts.clone();
    let gateway = tokio::spawn(async move {
        serve(
            cfg,
            gateway_artifacts,
            cluster,
            Auth::default(),
            program_store,
            ProofEventsHub::new(),
            async move {
                gw_grpc_shutdown_rx.await.ok();
            },
            async move {
                gw_http_shutdown_rx.await.ok();
            },
        )
        .await
        .unwrap();
    });

    // Give the gateway a moment to bind.
    wait_for_port(http_port).await;
    wait_for_port(grpc_port).await;

    // ---- run the SDK-side flow via the generated proto clients ----
    let gw_channel = Endpoint::from_shared(format!("http://127.0.0.1:{grpc_port}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut artifact_rpc = ArtifactStoreClient::new(gw_channel.clone());
    let mut network_rpc = ProverNetworkClient::new(gw_channel);
    let http = reqwest::Client::new();

    // 1) upload the ELF ("program")
    let elf_bytes = b"fake-elf-bytes".to_vec();
    let program_uri = create_artifact_put(
        &mut artifact_rpc,
        &http,
        SdkArtifactType::Program,
        &elf_bytes,
    )
    .await;

    // 2) register_program: record vk_hash → program_artifact_id sidecar
    let vk_hash = vec![0xaa; 32];
    let vk_bytes = b"fake-vk".to_vec();
    let body = CreateProgramRequestBody {
        nonce: 0,
        vk_hash: vk_hash.clone(),
        vk: vk_bytes.clone(),
        program_uri,
    };
    network_rpc
        .create_program(CreateProgramRequest {
            format: MessageFormat::Binary as i32,
            signature: vec![],
            body: Some(body),
        })
        .await
        .unwrap();

    // 3) upload stdin
    let stdin_bytes = b"fake-stdin".to_vec();
    let stdin_uri = create_artifact_put(
        &mut artifact_rpc,
        &http,
        SdkArtifactType::Stdin,
        &stdin_bytes,
    )
    .await;

    // 4) request_proof
    let body = RequestProofRequestBody {
        nonce: 0,
        vk_hash: vk_hash.clone(),
        version: "test".into(),
        mode: ProofMode::Core as i32,
        strategy: 2, // Reserved
        stdin_uri,
        deadline: u64::MAX,
        cycle_limit: 0,
        gas_limit: 0,
        min_auction_period: 0,
        whitelist: vec![],
    };
    let resp = network_rpc
        .request_proof(RequestProofRequest {
            format: MessageFormat::Binary as i32,
            signature: vec![],
            body: Some(body),
        })
        .await
        .unwrap()
        .into_inner();
    let request_id = resp.body.expect("body").request_id;
    assert!(!request_id.is_empty());

    // 5) poll get_proof_request_status (fake reports Completed immediately)
    let status = network_rpc
        .get_proof_request_status(GetProofRequestStatusRequest {
            request_id: request_id.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        status.fulfillment_status,
        FulfillmentStatus::Fulfilled as i32
    );
    let proof_uri = status.proof_uri.expect("proof_uri on Fulfilled");
    assert!(
        proof_uri.starts_with(&public_http_url),
        "expected gateway URL, got {proof_uri}"
    );

    // 6) GET proof_uri — gateway `download_raw` zstd-decodes; InMemoryArtifactClient
    // is identity, so bytes come back as what we uploaded (raw bincode of the "proof").
    let got = http.get(&proof_uri).send().await.unwrap();
    assert!(got.status().is_success());
    let got_bytes = got.bytes().await.unwrap().to_vec();
    assert_eq!(
        got_bytes, proof_bytes,
        "proof bytes must round-trip byte-for-byte"
    );

    // ---- shutdown ----
    gw_grpc_shutdown_tx.send(()).ok();
    gw_http_shutdown_tx.send(()).ok();
    cluster_shutdown_tx.send(()).ok();
    let _ = tokio::time::timeout(Duration::from_secs(5), gateway).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), cluster_server).await;
}

/// create_artifact → HTTP PUT with the SDK's zstd(bincode(...)) shape. Returns
/// the gateway-emitted artifact_uri.
async fn create_artifact_put(
    artifact_rpc: &mut ArtifactStoreClient<Channel>,
    http: &reqwest::Client,
    artifact_type: SdkArtifactType,
    raw: &[u8],
) -> String {
    let req = CreateArtifactRequest {
        artifact_type: artifact_type as i32,
        ..Default::default()
    };
    let resp = artifact_rpc
        .create_artifact(req)
        .await
        .unwrap()
        .into_inner();
    // Mirror SDK's encoding: bincode then zstd level 3.
    let bincoded = bincode::serialize(raw).unwrap();
    let compressed = zstd::encode_all(bincoded.as_slice(), 3).unwrap();
    let put = http
        .put(&resp.artifact_presigned_url)
        .body(compressed)
        .send()
        .await
        .unwrap();
    assert!(put.status().is_success(), "PUT failed: {}", put.status());
    resp.artifact_uri
}

async fn wait_for_port(port: u16) {
    for _ in 0..50 {
        if tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_ok()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("port {port} never became ready");
}
