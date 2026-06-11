use std::time::Duration;

use sp1_sdk::SP1ProofMode;

use crate::assert::wait_stats;
use crate::cluster::{Cluster, API_GRPC_ADDR, COORDINATOR_ADDR, GATEWAY_GRPC_ADDR};
use crate::request::request_only;
use crate::scenario::{Scenario, ScenarioFuture};
use crate::scenarios::long_program;

pub fn scenario() -> Scenario {
    Scenario {
        name: "shutdown-drain",
        cpu_timeout: Duration::from_mins(45),
        gpu_timeout: Duration::from_mins(10),
        run: || -> ScenarioFuture { Box::pin(run()) },
    }
}

/// Whole-cluster shutdown while work is in flight: every component must exit within its
/// bound (no hang, no panic), and — the regression this exists for — nothing may keep the
/// fixed ports bound afterwards (detached-task zombies). Port re-bindability is the
/// observable proof that shutdown was actually clean.
async fn run() -> anyhow::Result<()> {
    let cluster = Cluster::standard().start().await?;
    let mut coordinator = cluster.coordinator_client().await?;

    let (elf, stdin) = long_program();
    let proof_id = request_only(
        &cluster.gateway_rpc_url(),
        elf,
        stdin,
        SP1ProofMode::Compressed,
    )
    .await?;
    wait_stats(
        &mut coordinator,
        "proof actively running",
        Duration::from_mins(5),
        |s| s.active_tasks > 0,
    )
    .await?;
    tracing::info!("work in flight ({proof_id}); shutting the whole cluster down");

    // shutdown() awaits every component with a 60s bound and logs stragglers.
    cluster.shutdown().await;

    // All fixed ports must be re-bindable: any zombie server still listening fails this.
    for (addr, what) in [
        (API_GRPC_ADDR, "api gRPC"),
        (COORDINATOR_ADDR, "coordinator gRPC"),
        (GATEWAY_GRPC_ADDR, "gateway gRPC"),
    ] {
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(|e| anyhow::anyhow!("port {addr} ({what}) still bound after shutdown: {e}"))?;
        drop(listener);
        tracing::info!("{what} port {addr} released cleanly");
    }

    Ok(())
}
