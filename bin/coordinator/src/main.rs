use eyre::Result;
use sp1_cluster_coordinator::policy::balanced::BalancedPolicy;
use sp1_cluster_coordinator::server::run_coordinator_server;

#[tokio::main]
async fn main() -> Result<()> {
    run_coordinator_server::<BalancedPolicy>().await
}
