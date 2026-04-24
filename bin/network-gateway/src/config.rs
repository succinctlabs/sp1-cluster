use clap::Parser;

use crate::auth::AuthMode;

#[derive(Parser, Debug, Clone)]
#[command(
    name = "network-gateway",
    about = "SDK-compatible gateway for sp1-cluster"
)]
pub struct Config {
    /// gRPC server bind address.
    #[arg(long, env = "GATEWAY_GRPC_ADDR", default_value = "0.0.0.0:50061")]
    pub grpc_addr: String,

    /// HTTP server bind address (artifact proxy + health).
    #[arg(long, env = "GATEWAY_HTTP_ADDR", default_value = "0.0.0.0:8081")]
    pub http_addr: String,

    /// Public base URL used to build artifact URIs returned to SDK clients.
    /// e.g. `http://gateway.internal:8081`.
    #[arg(
        long,
        env = "GATEWAY_PUBLIC_HTTP_URL",
        default_value = "http://localhost:8081"
    )]
    pub public_http_url: String,

    /// ClusterService gRPC URL (the `bin/api` endpoint).
    #[arg(long, env = "GATEWAY_CLUSTER_RPC")]
    pub cluster_rpc: String,

    /// Artifact store backend: "s3" or "redis".
    #[arg(long, env = "GATEWAY_ARTIFACT_STORE", default_value = "s3")]
    pub artifact_store: String,

    #[arg(long, env = "GATEWAY_S3_BUCKET")]
    pub s3_bucket: Option<String>,

    #[arg(long, env = "GATEWAY_S3_REGION")]
    pub s3_region: Option<String>,

    #[arg(long, env = "GATEWAY_S3_CONCURRENCY", default_value_t = 32)]
    pub s3_concurrency: usize,

    #[arg(long, env = "GATEWAY_REDIS_NODES", value_delimiter = ',')]
    pub redis_nodes: Option<Vec<String>>,

    #[arg(long, env = "GATEWAY_REDIS_POOL_MAX_SIZE", default_value_t = 16)]
    pub redis_pool_max_size: usize,

    /// Balance reported for any address by `get_balance`. Decimal string.
    /// Defaults to U256::MAX at startup when left unset.
    #[arg(long, env = "GATEWAY_BALANCE_AMOUNT")]
    pub balance_amount: Option<String>,

    /// Authentication mode for signed requests: `none` (default), `verify`, or `allowlist`.
    #[arg(long, env = "GATEWAY_AUTH_MODE", default_value = "none", value_enum)]
    pub auth_mode: AuthMode,

    /// Comma-separated list of 0x-prefixed addresses. Required when `auth_mode=allowlist`.
    #[arg(long, env = "GATEWAY_AUTH_ALLOWLIST")]
    pub auth_allowlist: Option<String>,
}
