pub mod service;

pub use service::ClusterServiceImpl;

use std::net::SocketAddr;
use std::sync::Arc;

use alloy::primitives::Bytes;
use axum::{routing::get, Router};
use serde::{Deserialize, Serialize};
use sp1_cluster_common::proto::cluster_service_server::ClusterServiceServer;
use sqlx::{prelude::FromRow, types::time::OffsetDateTime};
use tonic::transport::Server;
use tracing::{info, warn};

#[derive(Serialize, Deserialize, FromRow)]
#[allow(dead_code)]
struct Request {
    id: Bytes,
    // Note: Ensure these fields exist in your database schema
    created_at: OffsetDateTime,
    updated_at: OffsetDateTime,
}

#[derive(Debug, Clone)]
pub struct ApiConfig {
    pub http_addr: String,
    pub grpc_addr: String,
    pub postgres_uri: String,
    pub postgres_auto_migrate: bool,
}

pub async fn run(config: ApiConfig) -> anyhow::Result<()> {
    let pool = sqlx::postgres::PgPool::connect(&config.postgres_uri)
        .await
        .expect("Failed to connect to database");

    if config.postgres_auto_migrate {
        info!("Running database migrations");
        sqlx::migrate!("../../migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");
    }

    let pool = Arc::new(pool);

    // Create the gRPC service
    let cluster_service = ClusterServiceImpl::new(pool.clone());
    let grpc_service = ClusterServiceServer::new(cluster_service);

    // Set up the gRPC server
    let grpc_addr = config
        .grpc_addr
        .parse::<SocketAddr>()
        .expect("Invalid gRPC address");
    info!("Starting gRPC server on {grpc_addr}");

    // Start the gRPC server in a separate task
    tokio::spawn(async move {
        Server::builder()
            .accept_http1(true)
            .add_service(tonic_web::enable(grpc_service))
            .serve(grpc_addr)
            .await
            .unwrap_or_else(|e| {
                warn!("gRPC server error: {}", e);
            });
    });

    // Build the HTTP application with routes
    let app = Router::new()
        .route("/", get(|| async { "OK" }))
        .route("/healthz", get(|| async { "OK" }));

    info!("Starting HTTP server on {}", config.http_addr);
    let listener = tokio::net::TcpListener::bind(config.http_addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
