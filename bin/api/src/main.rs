mod service;

use std::net::SocketAddr;
use std::sync::Arc;

use alloy::primitives::Bytes;
use axum::{routing::get, Router};
use opentelemetry_sdk::Resource;
use serde::{Deserialize, Serialize};
use service::ClusterServiceImpl;
use sp1_cluster_common::{logger, proto::cluster_service_server::ClusterServiceServer};
use sqlx::{prelude::FromRow, types::time::OffsetDateTime};
use tonic::transport::Server;
use tracing::{info, warn};

#[derive(Serialize, Deserialize, FromRow)]
struct Request {
    id: Bytes,
    // Note: Ensure these fields exist in your database schema
    created_at: OffsetDateTime,
    updated_at: OffsetDateTime,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load environment variables
    if let Err(e) = dotenv::dotenv() {
        eprintln!("not loading .env file: {}", e);
    }
    logger::init(Resource::empty());
    info!("Loaded environment variables");

    // Connect to the database
    let database_url = std::env::var("API_DATABASE_URL").expect("API_DATABASE_URL must be set");
    let pool = sqlx::postgres::PgPool::connect(&database_url)
        .await
        .expect("Failed to connect to database");

    if std::env::var("API_AUTO_MIGRATE").unwrap_or("false".to_string()) == "true" {
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
    let grpc_addr = std::env::var("API_GRPC_ADDR").unwrap_or("127.0.0.1:50051".to_string());
    let grpc_addr = grpc_addr
        .parse::<SocketAddr>()
        .expect("Invalid gRPC address");
    info!("Starting gRPC server on {}", grpc_addr);

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

    // Run the HTTP server
    let http_addr = std::env::var("API_HTTP_ADDR").unwrap_or("127.0.0.1:3000".to_string());
    info!("Starting HTTP server on {}", http_addr);
    let listener = tokio::net::TcpListener::bind(http_addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
