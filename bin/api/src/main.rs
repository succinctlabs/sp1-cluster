use opentelemetry_sdk::Resource;
use sp1_cluster_api::ApiConfig;
use sp1_cluster_common::logger;
use tokio_util::sync::CancellationToken;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    logger::init(Resource::empty());
    if let Err(e) = dotenv::dotenv() {
        eprintln!("not loading .env file: {e}");
    }
    info!("Loaded environment variables");
    let config = ApiConfig {
        http_addr: std::env::var("API_HTTP_ADDR").unwrap_or("127.0.0.1:3000".to_string()),
        grpc_addr: std::env::var("API_GRPC_ADDR").unwrap_or("127.0.0.1:50051".to_string()),
        postgres_auto_migrate: std::env::var("API_AUTO_MIGRATE").unwrap_or("false".to_string())
            == "true",
        postgres_uri: std::env::var("API_DATABASE_URL").expect("API_DATABASE_URL must be set"),
    };
    let token = CancellationToken::new();
    sp1_cluster_api::run(config, token).await?;

    Ok(())
}
