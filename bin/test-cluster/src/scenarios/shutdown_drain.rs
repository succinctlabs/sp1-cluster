use std::time::Duration;

use sp1_sdk::SP1ProofMode;

use crate::assert::wait_stats;
use crate::cluster::Cluster;
use crate::programs;
use crate::request::request_only;
use crate::scenario::{Scenario, ScenarioFuture, Tier};

pub fn scenario() -> Scenario {
    Scenario {
        name: "shutdown-drain",
        timeout: Duration::from_mins(10),
        tier: Tier::Full,
        run: || -> ScenarioFuture { Box::pin(run()) },
    }
}

/// Whole-cluster shutdown while work is in flight: every component must exit within its
/// bound (no hang, no panic), and — the regression this exists for — nothing may keep the
/// cluster's ports bound afterwards (detached-task zombies). Port re-bindability is the
/// observable proof that shutdown was actually clean.
async fn run() -> anyhow::Result<()> {
    let cluster = Cluster::standard().start().await?;
    let addrs = cluster.addrs.clone();
    let mut coordinator = cluster.coordinator_client().await?;

    let proof_id = request_only(
        &cluster.gateway_rpc_url(),
        programs::RSP_ELF.clone(),
        programs::RSP_STDIN.clone(),
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

    // All cluster ports must be re-bindable: any zombie server still listening fails this.
    for (addr, what) in [
        (addrs.api_grpc.as_str(), "api gRPC"),
        (addrs.api_http.as_str(), "api HTTP"),
        (addrs.coordinator.as_str(), "coordinator gRPC"),
        (addrs.coordinator_metrics.as_str(), "coordinator metrics"),
        (addrs.gateway_grpc.as_str(), "gateway gRPC"),
        (addrs.gateway_http.as_str(), "gateway HTTP"),
    ] {
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(|e| anyhow::anyhow!("port {addr} ({what}) still bound after shutdown: {e}"))?;
        drop(listener);
        tracing::info!("{what} port {addr} released cleanly");
    }

    Ok(())
}
