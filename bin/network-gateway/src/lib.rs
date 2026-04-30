pub mod artifact_http;
pub mod auth;
pub mod config;
pub mod ids;
pub mod program_store;
pub mod proof_events;
pub mod service;
pub mod status;

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::{routing::get, Router};
use sp1_cluster_artifact::{ArtifactClient, CompressedUpload};
use sp1_cluster_common::client::ClusterServiceClient;
use sp1_sdk::network::proto::artifact::artifact_store_server::ArtifactStoreServer;
use sp1_sdk::network::proto::base::network::prover_network_server::ProverNetworkServer;
use tokio::signal;
use tonic::transport::Server;
use tracing::{info, warn};

use crate::artifact_http::ArtifactHttpState;
use crate::auth::{parse_allowlist, Auth, AuthMode};
use crate::config::Config;
use crate::program_store::{FilesystemProgramStore, InMemoryProgramStore, ProgramStore};
use crate::proof_events::{spawn as spawn_proof_events, ProofEventsHub};
use crate::service::artifact_store::ArtifactStoreImpl;
use crate::service::prover_network::ProverNetworkImpl;

/// Resolve the auth config, connect to the cluster, and serve both gRPC and HTTP endpoints.
/// Blocks until the shutdown signal fires.
pub async fn run<A>(cfg: Config, client: A) -> Result<()>
where
    A: ArtifactClient + CompressedUpload + 'static,
{
    let cluster = ClusterServiceClient::new(cfg.cluster_rpc.clone())
        .await
        .map_err(|e| {
            anyhow::anyhow!("failed to connect to cluster RPC {}: {e}", cfg.cluster_rpc)
        })?;
    let auth = build_auth(&cfg)?;
    let program_store = build_program_store(&cfg)?;
    let proof_events = spawn_proof_events(cluster.clone()).await?;
    serve(
        cfg,
        client,
        cluster,
        auth,
        program_store,
        proof_events,
        shutdown_signal(),
        shutdown_signal(),
    )
    .await
}

/// Serve both endpoints against pre-built cluster + auth state.
///
/// Broken out so integration tests can inject a fake ClusterServiceClient and
/// their own shutdown signals (e.g. `oneshot::Receiver`).
#[allow(clippy::too_many_arguments)]
pub async fn serve<A, FG, FH>(
    cfg: Config,
    client: A,
    cluster: ClusterServiceClient,
    auth: Auth,
    program_store: Arc<dyn ProgramStore>,
    proof_events: ProofEventsHub,
    grpc_shutdown: FG,
    http_shutdown: FH,
) -> Result<()>
where
    A: ArtifactClient + CompressedUpload + 'static,
    FG: std::future::Future<Output = ()> + Send + 'static,
    FH: std::future::Future<Output = ()> + Send + 'static,
{
    let grpc_addr: SocketAddr = cfg
        .grpc_addr
        .parse()
        .with_context(|| format!("invalid GATEWAY_GRPC_ADDR: {}", cfg.grpc_addr))?;

    let balance_amount = cfg
        .balance_amount
        .clone()
        .unwrap_or_else(|| alloy_primitives::U256::MAX.to_string());

    // Tonic's 4 MiB default is too tight for SP1 verifying keys carried in
    // `create_program`; bump to 64 MiB so the gRPC path is never the
    // bottleneck. Artifact bodies go through the HTTP proxy, not gRPC.
    const MAX_GRPC_MESSAGE_SIZE: usize = 64 * 1024 * 1024;

    let artifact_store = ArtifactStoreServer::new(ArtifactStoreImpl::new(
        client.clone(),
        cfg.public_http_url.clone(),
        auth.clone(),
    ))
    .max_decoding_message_size(MAX_GRPC_MESSAGE_SIZE)
    .max_encoding_message_size(MAX_GRPC_MESSAGE_SIZE);
    let prover_network = ProverNetworkServer::new(ProverNetworkImpl::new(
        client.clone(),
        cluster,
        cfg.public_http_url.clone(),
        balance_amount,
        auth,
        program_store,
        proof_events,
    ))
    .max_decoding_message_size(MAX_GRPC_MESSAGE_SIZE)
    .max_encoding_message_size(MAX_GRPC_MESSAGE_SIZE);

    let grpc_task = tokio::spawn(async move {
        info!("gRPC server listening on {grpc_addr}");
        Server::builder()
            .accept_http1(true)
            .add_service(tonic_web::enable(prover_network))
            .add_service(tonic_web::enable(artifact_store))
            .serve_with_shutdown(grpc_addr, grpc_shutdown)
            .await
            .unwrap_or_else(|e| warn!("gRPC server error: {e}"));
    });

    let http_state = Arc::new(ArtifactHttpState { client });
    let app = Router::new()
        .route("/", get(|| async { "OK" }))
        .route("/healthz", get(|| async { "OK" }))
        .merge(artifact_http::router(http_state));

    let http_listener = tokio::net::TcpListener::bind(&cfg.http_addr)
        .await
        .with_context(|| format!("bind {}", cfg.http_addr))?;
    info!("HTTP server listening on {}", cfg.http_addr);

    axum::serve(http_listener, app)
        .with_graceful_shutdown(http_shutdown)
        .await?;

    grpc_task.await.ok();
    info!("network-gateway shut down cleanly");
    Ok(())
}

pub fn build_program_store(cfg: &Config) -> Result<Arc<dyn ProgramStore>> {
    match cfg.program_store.as_str() {
        "memory" => {
            info!("program store: in-memory (programs lost on gateway restart)");
            Ok(Arc::new(InMemoryProgramStore::new()))
        }
        "fs" => {
            let dir = cfg
                .program_store_dir
                .clone()
                .context("GATEWAY_PROGRAM_STORE_DIR is required when program_store=fs")?;
            let store = FilesystemProgramStore::new(dir.clone())?;
            info!(dir = %dir.display(), "program store: filesystem");
            Ok(Arc::new(store))
        }
        other => anyhow::bail!("unknown GATEWAY_PROGRAM_STORE={other} (expected memory or fs)"),
    }
}

pub fn build_auth(cfg: &Config) -> Result<Auth> {
    let allowlist = cfg
        .auth_allowlist
        .as_deref()
        .map(parse_allowlist)
        .transpose()
        .map_err(|e| anyhow::anyhow!("GATEWAY_AUTH_ALLOWLIST: {e}"))?
        .unwrap_or_default();
    if cfg.auth_mode == AuthMode::Allowlist && allowlist.is_empty() {
        anyhow::bail!("GATEWAY_AUTH_MODE=allowlist requires GATEWAY_AUTH_ALLOWLIST");
    }
    let auth = Auth {
        mode: cfg.auth_mode,
        allowlist,
    };
    info!(mode = ?auth.mode, allowlist_size = auth.allowlist.len(), "auth configured");
    Ok(auth)
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c().await.ok();
    };
    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .ok()
            .map(|mut s| async move { s.recv().await })
            .unwrap()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    info!("shutdown signal received");
}
