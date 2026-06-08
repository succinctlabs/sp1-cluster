# E2E Harness Foundation + Smoke Tier Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn `bin/test-cluster` into a scenario-based e2e harness (`run`/`suite`/`list` subcommands, composable `Cluster` builder, assertion pack) and implement the three smoke-tier scenarios: `quick`, `proof-modes`, `execute-only`.

**Architecture:** One binary, scenario-per-process. `suite` re-spawns `current_exe() run <scenario>` sequentially with per-scenario timeout and log capture. Cluster components (api, coordinator, gateway, nodes) run in-process via their lib `start` fns, each under its own child `CancellationToken` with a `JoinHandle`, so later plans can kill/stop/restart them individually. Spec: `docs/superpowers/specs/2026-06-09-cluster-e2e-coverage-matrix-design.md`.

**Tech Stack:** Rust, tokio, tonic (raw `WorkerServiceClient` for coordinator stats), testcontainers (redis + postgres), sp1-sdk hosted network mode through `sp1-cluster-network-gateway`.

**This is Plan 1 of 3.** Plan 2 (fault injection: scenarios 4–14, test-hooks feature, heartbeat setting) and Plan 3 (MinIO/S3 + CI workflows) build on this harness and are written after this plan lands.

---

## Build/run cheatsheet (used throughout)

```bash
# gpu flavor (the machine's default dev loop)
export RUSTFLAGS="-C linker=clang -C link-arg=-fuse-ld=mold"
export CARGO_TARGET_DIR=target_release

# type-check, gpu flavor
cargo check --release -p sp1-test-cluster --features gpu

# type-check, cpu-only flavor
SP1_CLUSTER_CPU_ONLY=1 cargo check --release -p sp1-test-cluster

# unit tests for the harness itself (no cluster boot; fast)
cargo test --release -p sp1-test-cluster --features gpu

# run one scenario, gpu flavor
RUST_LOG=info cargo run --release --bin sp1-test-cluster --features gpu -- run quick
```

Notes:
- `SP1_CLUSTER_CPU_ONLY` is compile-time (`option_env!`), hence env-on-the-build-command.
- CPU nodes download groth16+plonk circuit artifacts before connecting (`bin/node/src/lib.rs`, `download_artifacts_for_cpu_workers`). First run on a fresh machine is slow (~GB download, cached in `~/.sp1` afterwards). Builder readiness-wait and scenario timeouts are sized generously for this.
- Scenarios use docker via testcontainers (bitnami redis + postgres images), as the current `env.rs` already does.

## File structure

```
bin/test-cluster/src/
  main.rs            — CLI dispatch: run/suite/list (rewritten in Task 5)
  scenario.rs        — Flavor, Flavors, Tier, Scenario, registry resolution   (new, Task 1)
  cluster.rs         — Cluster + ClusterBuilder + ComponentHandle             (new, Task 2)
  request.rs         — SDK-through-gateway submit helpers                     (modified, Task 3)
  assert.rs          — poll_until + API/artifact/stats assertion pack         (new, Task 4)
  suite.rs           — child-process suite runner + report table              (new, Task 8)
  scenarios/
    mod.rs           — scenario registry                                      (new, Task 5)
    quick.rs         — fib core+compressed                                    (new, Task 5)
    proof_modes.rs   — fib × 4 modes                                          (new, Task 6)
    execute_only.rs  — standalone executor stack + failure variant           (new, Task 7)
  env.rs             — unchanged (testcontainers redis/postgres)
  programs.rs        — unchanged
  utils.rs           — unchanged
bin/test-cluster/README.md — run instructions                                 (new, Task 9)
```

Old `main.rs` content (dual prover+execute-only coordinators sharing one API, `assert_execute_only_gas`) is dissolved: bring-up moves to `cluster.rs`, the gas assert moves into the `execute-only` scenario, and the shared-API topology is deleted (spec: standalone executor stack).

---

### Task 1: Scenario registry types (`scenario.rs`)

**Files:**
- Create: `bin/test-cluster/src/scenario.rs`
- Modify: `bin/test-cluster/src/main.rs` (add `mod scenario;` only)

- [ ] **Step 1: Write `scenario.rs` with types, resolution logic, and failing-to-compile-without-impl unit tests**

```rust
use std::pin::Pin;
use std::time::Duration;

/// Which build flavor this binary is. `SP1_CLUSTER_CPU_ONLY` is compile-time, so the
/// flavor is fixed per build: gpu feature => Gpu, otherwise CpuOnly.
/// (main.rs keeps the existing panic if neither gpu feature nor SP1_CLUSTER_CPU_ONLY is set.)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Flavor {
    Gpu,
    CpuOnly,
}

impl Flavor {
    pub fn current() -> Self {
        if cfg!(feature = "gpu") {
            Flavor::Gpu
        } else {
            Flavor::CpuOnly
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Flavor::Gpu => "gpu",
            Flavor::CpuOnly => "cpu-only",
        }
    }
}

/// Which flavors a scenario can run under.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Flavors {
    Gpu,
    CpuOnly,
    Both,
}

impl Flavors {
    pub fn supports(self, flavor: Flavor) -> bool {
        match self {
            Flavors::Both => true,
            Flavors::Gpu => flavor == Flavor::Gpu,
            Flavors::CpuOnly => flavor == Flavor::CpuOnly,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tier {
    Smoke,
    Full,
}

pub type ScenarioFuture = Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send>>;

pub struct Scenario {
    pub name: &'static str,
    pub flavors: Flavors,
    /// Hard per-scenario timeout enforced by the suite runner (generous: first runs
    /// include circuit-artifact downloads).
    pub timeout: Duration,
    pub run: fn() -> ScenarioFuture,
}

/// Smoke tier is an explicit per-flavor list (spec Section 2 "Tiers").
pub fn smoke_names(flavor: Flavor) -> &'static [&'static str] {
    match flavor {
        Flavor::Gpu => &["proof-modes", "execute-only"],
        Flavor::CpuOnly => &["quick", "execute-only"],
    }
}

/// Resolve the scenario list for (tier, flavor). Panics if a smoke name is missing from
/// the registry — that is a programmer error, caught by the unit tests below.
pub fn resolve<'a>(all: &'a [Scenario], tier: Tier, flavor: Flavor) -> Vec<&'a Scenario> {
    match tier {
        Tier::Smoke => smoke_names(flavor)
            .iter()
            .map(|name| {
                all.iter()
                    .find(|s| s.name == *name)
                    .unwrap_or_else(|| panic!("smoke scenario {name} not in registry"))
            })
            .collect(),
        Tier::Full => all.iter().filter(|s| s.flavors.supports(flavor)).collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy(name: &'static str, flavors: Flavors) -> Scenario {
        Scenario {
            name,
            flavors,
            timeout: Duration::from_secs(1),
            run: || Box::pin(async { Ok(()) }),
        }
    }

    #[test]
    fn full_tier_filters_by_flavor() {
        let all = vec![
            dummy("a", Flavors::Both),
            dummy("b", Flavors::Gpu),
            dummy("c", Flavors::CpuOnly),
        ];
        let gpu: Vec<_> = resolve(&all, Tier::Full, Flavor::Gpu)
            .iter()
            .map(|s| s.name)
            .collect();
        assert_eq!(gpu, vec!["a", "b"]);
        let cpu: Vec<_> = resolve(&all, Tier::Full, Flavor::CpuOnly)
            .iter()
            .map(|s| s.name)
            .collect();
        assert_eq!(cpu, vec!["a", "c"]);
    }

    #[test]
    fn smoke_tier_is_explicit_per_flavor() {
        let all = vec![
            dummy("quick", Flavors::Both),
            dummy("proof-modes", Flavors::Both),
            dummy("execute-only", Flavors::Both),
        ];
        let gpu: Vec<_> = resolve(&all, Tier::Smoke, Flavor::Gpu)
            .iter()
            .map(|s| s.name)
            .collect();
        assert_eq!(gpu, vec!["proof-modes", "execute-only"]);
        let cpu: Vec<_> = resolve(&all, Tier::Smoke, Flavor::CpuOnly)
            .iter()
            .map(|s| s.name)
            .collect();
        assert_eq!(cpu, vec!["quick", "execute-only"]);
    }

    #[test]
    #[should_panic(expected = "not in registry")]
    fn smoke_panics_on_missing_scenario() {
        let all = vec![dummy("quick", Flavors::Both)];
        resolve(&all, Tier::Smoke, Flavor::Gpu);
    }
}
```

- [ ] **Step 2: Register the module in `main.rs`**

Add to the `mod` block at the top of `bin/test-cluster/src/main.rs`:

```rust
mod scenario;
```

- [ ] **Step 3: Run the unit tests**

Run: `cargo test --release -p sp1-test-cluster --features gpu scenario::`
Expected: 3 tests pass.

- [ ] **Step 4: Check the cpu-only flavor compiles too**

Run: `SP1_CLUSTER_CPU_ONLY=1 cargo check --release -p sp1-test-cluster`
Expected: success (warnings about unused items are fine at this stage).

- [ ] **Step 5: Commit**

```bash
git add bin/test-cluster/src/scenario.rs bin/test-cluster/src/main.rs
git commit -m "feat(test-cluster): add scenario registry types and tier resolution"
```

---

### Task 2: `Cluster` builder (`cluster.rs`)

**Files:**
- Create: `bin/test-cluster/src/cluster.rs`
- Modify: `bin/test-cluster/src/main.rs` (add `mod cluster;` only — old bring-up stays until Task 5)
- Modify: `bin/test-cluster/Cargo.toml` (add `tonic.workspace = true` — needed for `tonic::transport::Channel` and `tonic::Request` in `cluster.rs`/`assert.rs`)

This moves the bring-up currently inlined in `main.rs` into a reusable builder with per-component handles. Key behaviors:

- Every component runs under a **child token** of the cluster root token (root cancel ⇒ everything stops).
- A component that returns an **error** cancels the root token, so scenarios fail fast instead of hanging (the old `main.rs` silently ignored startup failures of spawned tiers).
- `kill` = `JoinHandle::abort()` (crash semantics: futures drop, TCP closes). `stop` = cancel child token + await (graceful). Restart comes in Plan 2.
- `start()` waits for worker registration via coordinator `GetStats` polling instead of sleeping.

- [ ] **Step 1: Write `cluster.rs`**

```rust
use std::collections::HashMap;
use std::future::Future;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use sp1_cluster_api::ApiConfig;
use sp1_cluster_artifact::redis::RedisArtifactClient;
use sp1_cluster_common::client::ClusterServiceClient;
use sp1_cluster_common::proto::worker_service_client::WorkerServiceClient as RawCoordinatorClient;
use sp1_cluster_common::proto::WorkerType;
use sp1_cluster_coordinator::config::Settings as CoordinatorSettings;
use sp1_cluster_coordinator::policy::balanced::BalancedPolicy;
use sp1_cluster_coordinator::server::start_coordinator_server_custom;
use sp1_cluster_network_gateway::auth::AuthMode;
use sp1_cluster_network_gateway::config::Config as GatewayConfig;
use tokio::task::JoinHandle;
use tokio_util::future::FutureExt as _;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;

use crate::{env, utils};

pub const REDIS_POOL_MAX_SIZE: usize = 1;
pub const GATEWAY_GRPC_ADDR: &str = "127.0.0.1:50061";
pub const GATEWAY_HTTP_ADDR: &str = "127.0.0.1:8081";
pub const API_HTTP_ADDR: &str = "127.0.0.1:3000";
pub const API_GRPC_ADDR: &str = "127.0.0.1:50051";
pub const COORDINATOR_ADDR: &str = "127.0.0.1:50052";
pub const COORDINATOR_METRICS_ADDR: &str = "0.0.0.0:9090";

/// How long `start()` waits for all requested workers to register with the coordinator.
/// Generous: CPU nodes download groth16+plonk circuit artifacts before connecting.
const WORKER_READY_TIMEOUT: Duration = Duration::from_secs(30 * 60);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoordinatorKind {
    Prover,
    ExecuteOnly,
}

pub struct ComponentHandle {
    pub token: CancellationToken,
    pub handle: JoinHandle<()>,
}

pub struct ClusterBuilder {
    coordinator: CoordinatorKind,
    cpu_nodes: usize,
    gpu_nodes: usize,
}

pub struct Cluster {
    pub root: CancellationToken,
    components: HashMap<String, ComponentHandle>,
    // Containers stop when dropped; keep them alive for the cluster lifetime.
    _redis: env::Redis,
    _postgres: env::Postgres,
    artifact_client: RedisArtifactClient,
}

impl Cluster {
    pub fn builder() -> ClusterBuilder {
        ClusterBuilder {
            coordinator: CoordinatorKind::Prover,
            cpu_nodes: 0,
            gpu_nodes: 0,
        }
    }

    /// Spec "standard" shape: 1 CPU + 1 GPU node on the gpu flavor, 1 CPU node on cpu-only.
    pub fn standard() -> ClusterBuilder {
        let gpu_nodes = if cfg!(feature = "gpu") { 1 } else { 0 };
        Self::builder().cpu_nodes(1).gpu_nodes(gpu_nodes)
    }

    pub fn gateway_rpc_url(&self) -> String {
        format!("http://{GATEWAY_GRPC_ADDR}")
    }

    pub fn artifact_client(&self) -> RedisArtifactClient {
        self.artifact_client.clone()
    }

    pub async fn api_client(&self) -> Result<ClusterServiceClient> {
        ClusterServiceClient::new(format!("http://{API_GRPC_ADDR}"))
            .await
            .map_err(|e| anyhow!("failed to connect to cluster api: {e}"))
    }

    pub async fn coordinator_client(&self) -> Result<RawCoordinatorClient<Channel>> {
        RawCoordinatorClient::connect(format!("http://{COORDINATOR_ADDR}"))
            .await
            .context("failed to connect to coordinator")
    }

    /// Crash a component: abort its task. Futures drop, TCP connections close, the
    /// coordinator's heartbeat cleanup eventually notices (worker components).
    pub fn kill(&mut self, name: &str) -> Result<()> {
        let c = self
            .components
            .get(name)
            .ok_or_else(|| anyhow!("unknown component {name}"))?;
        tracing::info!("killing component {name} (abort)");
        c.handle.abort();
        Ok(())
    }

    /// Gracefully stop a component: cancel its token and await its task.
    pub async fn stop(&mut self, name: &str) -> Result<()> {
        let c = self
            .components
            .remove(name)
            .ok_or_else(|| anyhow!("unknown component {name}"))?;
        tracing::info!("stopping component {name} (cancel + await)");
        c.token.cancel();
        let _ = c.handle.await;
        Ok(())
    }

    /// Shut the whole cluster down: cancel root, await every component (bounded).
    pub async fn shutdown(mut self) {
        self.root.cancel();
        for (name, c) in self.components.drain() {
            match tokio::time::timeout(Duration::from_secs(60), c.handle).await {
                Ok(_) => tracing::info!("component {name} exited"),
                Err(_) => tracing::warn!("component {name} did not exit within 60s"),
            }
        }
    }

    /// Poll coordinator GetStats until at least (cpu, gpu) workers are registered.
    pub async fn wait_for_workers(&self, cpu: u32, gpu: u32, timeout: Duration) -> Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if let Ok(mut client) = self.coordinator_client().await {
                if let Ok(stats) = client.get_stats(tonic::Request::new(())).await {
                    let stats = stats.into_inner();
                    if stats.cpu_workers >= cpu && stats.gpu_workers >= gpu {
                        tracing::info!(
                            "workers ready: {} cpu, {} gpu",
                            stats.cpu_workers,
                            stats.gpu_workers
                        );
                        return Ok(());
                    }
                    tracing::info!(
                        "waiting for workers: have {}/{} cpu, {}/{} gpu",
                        stats.cpu_workers,
                        cpu,
                        stats.gpu_workers,
                        gpu
                    );
                }
            }
            if tokio::time::Instant::now() >= deadline {
                anyhow::bail!("timed out waiting for {cpu} cpu / {gpu} gpu workers");
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }
}

impl ClusterBuilder {
    pub fn coordinator(mut self, kind: CoordinatorKind) -> Self {
        self.coordinator = kind;
        self
    }

    pub fn cpu_nodes(mut self, n: usize) -> Self {
        self.cpu_nodes = n;
        self
    }

    pub fn gpu_nodes(mut self, n: usize) -> Self {
        assert!(
            n == 0 || cfg!(feature = "gpu"),
            "gpu_nodes requires the gpu feature"
        );
        self.gpu_nodes = n;
        self
    }

    pub async fn start(self) -> Result<Cluster> {
        let root = CancellationToken::new();
        let mut components: HashMap<String, ComponentHandle> = HashMap::new();

        let (redis, postgres) = tokio::try_join!(
            env::Redis::start(root.clone()),
            env::Postgres::start(root.clone()),
        )?;
        let artifact_client =
            RedisArtifactClient::new(vec![redis.addr().into()], REDIS_POOL_MAX_SIZE);

        // api
        {
            let token = root.child_token();
            let fut = sp1_cluster_api::run(ApiConfig {
                http_addr: API_HTTP_ADDR.to_string(),
                grpc_addr: API_GRPC_ADDR.to_string(),
                postgres_uri: postgres.addr().to_string(),
                postgres_auto_migrate: true,
            })
            .with_cancellation_token_owned(token.clone());
            let handle = spawn_component("api", root.clone(), async move {
                match fut.await {
                    // None = cancelled (graceful), Some(r) = component finished on its own
                    None => Ok(()),
                    Some(r) => r.map_err(|e| anyhow!("api exited: {e:?}")),
                }
            });
            components.insert("api".to_string(), ComponentHandle { token, handle });
        }
        utils::wait_for_tcp(API_GRPC_ADDR, "api gRPC").await?;

        // coordinator
        {
            let token = root.child_token();
            let (_coordinator, fut) = start_coordinator_server_custom::<BalancedPolicy>(
                CoordinatorSettings {
                    addr: COORDINATOR_ADDR.to_string(),
                    cluster_rpc: format!("http://{API_GRPC_ADDR}"),
                    metrics_addr: COORDINATOR_METRICS_ADDR.to_string(),
                    disable_proof_status_update: false,
                    execute_only_mode: self.coordinator == CoordinatorKind::ExecuteOnly,
                },
                token.clone(),
            )
            .await
            .map_err(|e| anyhow!("coordinator startup failed: {e}"))?;
            let handle = spawn_component("coordinator", root.clone(), async move {
                fut.await;
                Ok(())
            });
            components.insert("coordinator".to_string(), ComponentHandle { token, handle });
        }
        utils::wait_for_tcp(COORDINATOR_ADDR, "coordinator gRPC").await?;

        // gateway
        {
            let token = root.child_token();
            let fut = sp1_cluster_network_gateway::run(
                GatewayConfig {
                    grpc_addr: GATEWAY_GRPC_ADDR.to_string(),
                    http_addr: GATEWAY_HTTP_ADDR.to_string(),
                    public_http_url: format!("http://{GATEWAY_HTTP_ADDR}"),
                    cluster_rpc: format!("http://{API_GRPC_ADDR}"),
                    artifact_store: "redis".to_string(),
                    s3_bucket: None,
                    s3_region: None,
                    s3_concurrency: 32,
                    redis_nodes: Some(vec![redis.addr().to_string()]),
                    redis_pool_max_size: REDIS_POOL_MAX_SIZE,
                    balance_amount: None,
                    auth_mode: AuthMode::None,
                    auth_allowlist: None,
                    program_store: "memory".to_string(),
                    program_store_dir: None,
                },
                artifact_client.clone(),
            )
            .with_cancellation_token_owned(token.clone());
            let handle = spawn_component("gateway", root.clone(), async move {
                match fut.await {
                    None => Ok(()),
                    Some(r) => r.map_err(|e| anyhow!("gateway exited: {e:?}")),
                }
            });
            components.insert("gateway".to_string(), ComponentHandle { token, handle });
        }
        utils::wait_for_tcp(GATEWAY_GRPC_ADDR, "gateway gRPC").await?;

        // nodes
        for i in 0..self.cpu_nodes {
            let name = format!("cpu-node-{i}");
            spawn_node(
                &mut components,
                &root,
                &artifact_client,
                name,
                WorkerType::Cpu,
            );
        }
        for i in 0..self.gpu_nodes {
            let name = format!("gpu-node-{i}");
            spawn_node(
                &mut components,
                &root,
                &artifact_client,
                name,
                WorkerType::Gpu,
            );
        }

        let cluster = Cluster {
            root,
            components,
            _redis: redis,
            _postgres: postgres,
            artifact_client,
        };
        cluster
            .wait_for_workers(
                self.cpu_nodes as u32,
                self.gpu_nodes as u32,
                WORKER_READY_TIMEOUT,
            )
            .await?;
        Ok(cluster)
    }
}

fn spawn_node(
    components: &mut HashMap<String, ComponentHandle>,
    root: &CancellationToken,
    artifact_client: &RedisArtifactClient,
    name: String,
    worker_type: WorkerType,
) {
    let token = root.child_token();
    let fut = sp1_cluster_node::run(
        sp1_cluster_node::config::NodeConfig {
            worker_id: format!("test-cluster-{name}"),
            worker_type,
            coordinator_rpc: format!("http://{COORDINATOR_ADDR}"),
            ..Default::default()
        },
        artifact_client.clone(),
        token.clone(),
        None,
    );
    let handle = spawn_component(&name.clone(), root.clone(), async move {
        fut.await.map_err(|e| anyhow!("node exited: {e:?}"))
    });
    components.insert(name, ComponentHandle { token, handle });
}

/// Spawn a component task. A component that returns Err cancels the root token so the
/// scenario fails fast instead of hanging on a dead cluster.
fn spawn_component(
    name: &str,
    root: CancellationToken,
    fut: impl Future<Output = Result<()>> + Send + 'static,
) -> JoinHandle<()> {
    let name = name.to_string();
    tokio::spawn(async move {
        match fut.await {
            Ok(()) => tracing::info!("component {name} exited cleanly"),
            Err(e) => {
                tracing::error!("component {name} failed: {e:?}");
                root.cancel();
            }
        }
    })
}
```

- [ ] **Step 2: Register the module in `main.rs`**

```rust
mod cluster;
```

- [ ] **Step 3: Type-check both flavors**

Run: `cargo check --release -p sp1-test-cluster --features gpu`
Run: `SP1_CLUSTER_CPU_ONLY=1 cargo check --release -p sp1-test-cluster`
Expected: both succeed. Likely friction points and their fixes:
- `sp1_cluster_api::run` / `sp1_cluster_network_gateway::run` result types: match on the actual `Option<Result<…>>` shape (current `main.rs` shows api/gateway are awaited via `with_cancellation_token_owned` and node returns `eyre::Result`); adjust the error-mapping closures, not the structure.
- `RawCoordinatorClient::connect` requires owned `String` URL — already passed.

- [ ] **Step 4: Commit**

```bash
git add bin/test-cluster/src/cluster.rs bin/test-cluster/src/main.rs
git commit -m "feat(test-cluster): add Cluster builder with per-component handles"
```

---

### Task 3: Submit helpers (`request.rs`)

**Files:**
- Modify: `bin/test-cluster/src/request.rs`

Existing `submit_proof_requests` (submit + wait + verify, returns proof ids) stays as-is — `quick` and `proof-modes` use it. Add `request_only` for the execute-only scenario: it must NOT `wait_proof` (an executor cluster never produces a proof; the SDK would poll forever or error), just submit and return the cluster proof id.

- [ ] **Step 1: Add `request_only` to `request.rs`**

Append:

```rust
/// Submit a proof request through the gateway WITHOUT waiting for a proof. Returns the
/// cluster proof_id. Used against execute-only clusters, where no proof is ever produced
/// and the terminal state is observed via the cluster API instead.
pub async fn request_only(
    rpc_url: &str,
    elf: Elf,
    stdin: SP1Stdin,
    mode: SP1ProofMode,
) -> Result<String> {
    let prover = ProverClient::builder()
        .network()
        .hosted()
        .rpc_url(rpc_url)
        .signer(random_local_signer())
        .build()
        .await;
    let pk = prover.setup(elf).await?;
    let request_id = prover.prove(&pk, stdin).mode(mode).request().await?;
    Ok(proof_id_from_request_id(request_id.as_slice()))
}
```

(`Elf`, `SP1Stdin`, `SP1ProofMode`, `ProverClient`, `random_local_signer`, `proof_id_from_request_id` are already imported/defined in this file.)

- [ ] **Step 2: Type-check**

Run: `cargo check --release -p sp1-test-cluster --features gpu`
Expected: success.

- [ ] **Step 3: Commit**

```bash
git add bin/test-cluster/src/request.rs
git commit -m "feat(test-cluster): add request_only submit helper for execute-only clusters"
```

---

### Task 4: Assertion pack (`assert.rs`)

**Files:**
- Create: `bin/test-cluster/src/assert.rs`
- Modify: `bin/test-cluster/src/main.rs` (add `mod assert;`)

- [ ] **Step 1: Write `assert.rs`**

```rust
use std::future::Future;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use sp1_cluster_artifact::{ArtifactClient, ArtifactType};
use sp1_cluster_common::client::ClusterServiceClient;
use sp1_cluster_common::proto::{
    ExecutionStatus, GetTaskStatusesRequest, ProofRequest, ProofRequestGetRequest,
    ProofRequestStatus, TaskStatus,
};
use sp1_cluster_common::proto::worker_service_client::WorkerServiceClient as RawCoordinatorClient;
use tonic::transport::Channel;

pub const POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Bounded poll. `f` returns Ok(Some(v)) when done, Ok(None) to keep polling, Err to abort.
pub async fn poll_until<T, F, Fut>(
    what: &str,
    timeout: Duration,
    mut f: F,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<Option<T>>>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(v) = f().await? {
            return Ok(v);
        }
        if tokio::time::Instant::now() >= deadline {
            bail!("timed out after {timeout:?} waiting for {what}");
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

pub async fn get_proof_request(
    api: &ClusterServiceClient,
    proof_id: &str,
) -> Result<ProofRequest> {
    api.get_proof_request(ProofRequestGetRequest {
        proof_id: proof_id.to_string(),
    })
    .await
    .map_err(|e| anyhow::anyhow!("get_proof_request({proof_id}): {e}"))?
    .with_context(|| format!("proof request {proof_id} not found"))
}

fn is_terminal(status: ProofRequestStatus) -> bool {
    matches!(
        status,
        ProofRequestStatus::Completed | ProofRequestStatus::Failed | ProofRequestStatus::Cancelled
    )
}

/// Poll the API until `proof_id` reaches `expected`. Errors immediately if it reaches a
/// DIFFERENT terminal status (no point waiting out the timeout).
pub async fn wait_proof_status(
    api: &ClusterServiceClient,
    proof_id: &str,
    expected: ProofRequestStatus,
    timeout: Duration,
) -> Result<ProofRequest> {
    poll_until(
        &format!("proof {proof_id} to reach {expected:?}"),
        timeout,
        || async {
            let pr = get_proof_request(api, proof_id).await?;
            let status = pr.proof_status();
            if status == expected {
                Ok(Some(pr))
            } else if is_terminal(status) {
                bail!("proof {proof_id} reached terminal {status:?}, expected {expected:?}")
            } else {
                Ok(None)
            }
        },
    )
    .await
}

pub struct ExpectedExecution {
    pub status: ExecutionStatus,
    pub min_cycles: u64,
    /// Exact gas regression value, when pinned (execute-only fibonacci oracle).
    pub gas: Option<u64>,
    pub require_pv_hash: bool,
}

pub fn assert_execution_result(pr: &ProofRequest, expected: &ExpectedExecution) -> Result<()> {
    let er = pr
        .execution_result
        .as_ref()
        .with_context(|| format!("proof {} has no execution_result", pr.id))?;
    if er.status() != expected.status {
        bail!(
            "proof {}: execution status {:?}, expected {:?} (failure_cause {:?})",
            pr.id,
            er.status(),
            expected.status,
            er.failure_cause()
        );
    }
    if er.cycles < expected.min_cycles {
        bail!("proof {}: cycles {} < expected min {}", pr.id, er.cycles, expected.min_cycles);
    }
    if let Some(gas) = expected.gas {
        if er.gas != gas {
            bail!("proof {}: gas {} != pinned {}", pr.id, er.gas, gas);
        }
    }
    if expected.require_pv_hash && er.public_values_hash.is_empty() {
        bail!("proof {}: empty public_values_hash", pr.id);
    }
    Ok(())
}

/// The proof artifact referenced by the API row must actually be downloadable and nonempty.
pub async fn assert_proof_artifact_downloadable(
    pr: &ProofRequest,
    artifact_client: &impl ArtifactClient,
) -> Result<()> {
    let id = pr
        .proof_artifact_id
        .clone()
        .with_context(|| format!("proof {} has no proof_artifact_id", pr.id))?;
    let bytes = artifact_client
        .download_raw(&id, ArtifactType::Proof)
        .await
        .map_err(|e| anyhow::anyhow!("download proof artifact {id}: {e:?}"))?;
    if bytes.is_empty() {
        bail!("proof artifact {id} is empty");
    }
    tracing::info!("proof artifact {id} downloadable ({} bytes)", bytes.len());
    Ok(())
}

/// Snapshot of task statuses for a proof: status -> task id list.
pub async fn task_statuses(
    coordinator: &mut RawCoordinatorClient<Channel>,
    proof_id: &str,
) -> Result<Vec<(TaskStatus, Vec<String>)>> {
    let resp = coordinator
        .get_task_statuses(GetTaskStatusesRequest {
            worker_id: String::new(), // unused by the handler (server.rs:198)
            proof_id: proof_id.to_string(),
            task_ids: vec![],
        })
        .await
        .context("get_task_statuses")?
        .into_inner();
    Ok(resp
        .statuses
        .into_iter()
        .map(|entry| {
            let status = entry.key();
            (status, entry.value.map(|l| l.ids).unwrap_or_default())
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn poll_until_returns_value() {
        let mut n = 0;
        let v = poll_until("test", Duration::from_secs(5), || {
            n += 1;
            let done = n >= 3;
            async move { Ok(if done { Some(42) } else { None }) }
        })
        .await
        .unwrap();
        assert_eq!(v, 42);
    }

    #[tokio::test]
    async fn poll_until_times_out() {
        let err = poll_until::<(), _, _>("never", Duration::from_millis(50), || async {
            Ok(None)
        })
        .await
        .unwrap_err();
        assert!(err.to_string().contains("timed out"), "{err}");
    }
}
```

Note on `entry.key()`: prost generates enum accessors for `TaskStatusMapEntry.key`. If the generated accessor differs, use `TaskStatus::try_from(entry.key).unwrap_or(TaskStatus::UnspecifiedStatus)`.

- [ ] **Step 2: Register module + run unit tests**

Add `mod assert;` to `main.rs`. Then:

Run: `cargo test --release -p sp1-test-cluster --features gpu assert::`
Expected: 2 tests pass.

- [ ] **Step 3: Commit**

```bash
git add bin/test-cluster/src/assert.rs bin/test-cluster/src/main.rs
git commit -m "feat(test-cluster): add assertion pack (poll, status, execution, artifact)"
```

---

### Task 5: `quick` scenario + CLI rewrite (`main.rs`, `scenarios/`)

**Files:**
- Create: `bin/test-cluster/src/scenarios/mod.rs`
- Create: `bin/test-cluster/src/scenarios/quick.rs`
- Rewrite: `bin/test-cluster/src/main.rs`

This is the task where the old dual-coordinator `main.rs` flow is deleted. The execute-only gas assert it contained is reinstated in Task 7 inside the `execute-only` scenario.

- [ ] **Step 1: Write `scenarios/quick.rs`**

```rust
use std::time::Duration;

use sp1_sdk::SP1ProofMode;

use crate::assert::{assert_proof_artifact_downloadable, wait_proof_status};
use crate::cluster::Cluster;
use crate::programs;
use crate::request::{submit_proof_requests, ClusterProofRequest, ClusterProofRequests};
use crate::scenario::{Flavors, Scenario, ScenarioFuture};
use sp1_cluster_common::proto::ProofRequestStatus;

pub fn scenario() -> Scenario {
    Scenario {
        name: "quick",
        flavors: Flavors::Both,
        timeout: Duration::from_secs(45 * 60),
        run: || -> ScenarioFuture { Box::pin(run()) },
    }
}

/// Cheap convenient run: fib core + compressed, verified locally. The default dev loop
/// and the cpu-only smoke proving scenario.
async fn run() -> anyhow::Result<()> {
    let cluster = Cluster::standard().start().await?;

    let proof_ids = submit_proof_requests(ClusterProofRequests {
        rpc_url: cluster.gateway_rpc_url(),
        requests: vec![ClusterProofRequest {
            elf: programs::FIBONACCI_ELF.clone(),
            stdin: programs::FIBONACCI_STDIN.clone(),
            modes: vec![SP1ProofMode::Core, SP1ProofMode::Compressed],
        }],
    })
    .await?;

    let api = cluster.api_client().await?;
    for proof_id in &proof_ids {
        let pr = wait_proof_status(
            &api,
            proof_id,
            ProofRequestStatus::Completed,
            Duration::from_secs(60),
        )
        .await?;
        assert_proof_artifact_downloadable(&pr, &cluster.artifact_client()).await?;
        tracing::info!("proof {proof_id} completed, artifact verified");
    }

    cluster.shutdown().await;
    Ok(())
}
```

- [ ] **Step 2: Write `scenarios/mod.rs`**

```rust
mod quick;

use crate::scenario::Scenario;

pub fn all() -> Vec<Scenario> {
    vec![quick::scenario()]
}
```

- [ ] **Step 3: Rewrite `main.rs`**

```rust
mod assert;
mod cluster;
mod env;
mod programs;
mod request;
mod scenario;
mod scenarios;
mod utils;
// (`mod suite;` is added in Task 8)

use scenario::{Flavor, Tier};

#[derive(Debug)]
enum Cmd {
    Run(String),
    Suite(Tier),
    List,
}

fn parse_args(args: &[String]) -> Result<Cmd, String> {
    match args {
        [] => Ok(Cmd::Suite(Tier::Smoke)),
        [cmd, name] if cmd == "run" => Ok(Cmd::Run(name.clone())),
        [cmd, tier] if cmd == "suite" => match tier.as_str() {
            "smoke" => Ok(Cmd::Suite(Tier::Smoke)),
            "full" => Ok(Cmd::Suite(Tier::Full)),
            other => Err(format!("unknown tier {other:?} (smoke|full)")),
        },
        [cmd] if cmd == "list" => Ok(Cmd::List),
        other => Err(format!(
            "usage: sp1-test-cluster [run <scenario> | suite <smoke|full> | list], got {other:?}"
        )),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    #[cfg(not(feature = "gpu"))]
    if option_env!("SP1_CLUSTER_CPU_ONLY").is_none() {
        panic!(
            "You need to either enable the \"gpu\" feature or set the \"SP1_CLUSTER_CPU_ONLY\" env"
        )
    }

    sp1_cluster_common::logger::init(opentelemetry_sdk::Resource::empty());

    let args: Vec<String> = std::env::args().skip(1).collect();
    let cmd = parse_args(&args).map_err(|e| anyhow::anyhow!(e))?;
    let flavor = Flavor::current();

    match cmd {
        Cmd::List => {
            for s in scenarios::all() {
                println!(
                    "{:<24} flavors={:?} timeout={:?}",
                    s.name, s.flavors, s.timeout
                );
            }
            Ok(())
        }
        Cmd::Run(name) => {
            let all = scenarios::all();
            let s = all
                .iter()
                .find(|s| s.name == name)
                .ok_or_else(|| anyhow::anyhow!("unknown scenario {name:?} (see `list`)"))?;
            if !s.flavors.supports(flavor) {
                anyhow::bail!(
                    "scenario {name} does not support the {} flavor of this binary",
                    flavor.as_str()
                );
            }
            tracing::info!("running scenario {name} (flavor {})", flavor.as_str());
            (s.run)().await?;
            tracing::info!("scenario {name} PASSED");
            Ok(())
        }
        Cmd::Suite(_tier) => {
            anyhow::bail!("suite runner lands in a later task; use `run <scenario>`")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    #[test]
    fn parses_commands() {
        assert!(matches!(parse_args(&s(&["run", "quick"])), Ok(Cmd::Run(n)) if n == "quick"));
        assert!(matches!(parse_args(&s(&["suite", "smoke"])), Ok(Cmd::Suite(Tier::Smoke))));
        assert!(matches!(parse_args(&s(&["suite", "full"])), Ok(Cmd::Suite(Tier::Full))));
        assert!(matches!(parse_args(&s(&["list"])), Ok(Cmd::List)));
        assert!(matches!(parse_args(&s(&[])), Ok(Cmd::Suite(Tier::Smoke))));
        assert!(parse_args(&s(&["bogus"])).is_err());
        assert!(parse_args(&s(&["suite", "bogus"])).is_err());
    }
}
```

(Omit the `mod suite;` line until Task 8. Delete the now-unused `EXPECTED_FIBONACCI_GAS`, `assert_execute_only_gas`, the dual-coordinator constants and bring-up — all of it is superseded by `cluster.rs` + scenarios.)

- [ ] **Step 4: Unit tests + both-flavor check**

Run: `cargo test --release -p sp1-test-cluster --features gpu`
Expected: scenario, assert, and parse tests pass.
Run: `SP1_CLUSTER_CPU_ONLY=1 cargo check --release -p sp1-test-cluster`
Expected: success.

- [ ] **Step 5: Run the `quick` scenario for real (gpu flavor)**

Run: `RUST_LOG=info cargo run --release --bin sp1-test-cluster --features gpu -- run quick`
Expected: redis+postgres containers start, api/coordinator/gateway come up, both nodes register (`workers ready: 1 cpu, 1 gpu`), two proofs complete and verify, artifact downloads logged, `scenario quick PASSED`, clean exit 0. First run may spend a long time in circuit-artifact download before the CPU node connects.

- [ ] **Step 6: Commit**

```bash
git add bin/test-cluster/src/
git commit -m "feat(test-cluster): scenario-based CLI with quick scenario; drop dual-coordinator main"
```

---

### Task 6: `proof-modes` scenario

**Files:**
- Create: `bin/test-cluster/src/scenarios/proof_modes.rs`
- Modify: `bin/test-cluster/src/scenarios/mod.rs`

- [ ] **Step 1: Write `scenarios/proof_modes.rs`**

```rust
use std::time::Duration;

use sp1_sdk::SP1ProofMode;

use crate::assert::{assert_proof_artifact_downloadable, wait_proof_status};
use crate::cluster::Cluster;
use crate::programs;
use crate::request::{submit_proof_requests, ClusterProofRequest, ClusterProofRequests};
use crate::scenario::{Flavors, Scenario, ScenarioFuture};
use sp1_cluster_common::proto::ProofRequestStatus;

pub fn scenario() -> Scenario {
    Scenario {
        name: "proof-modes",
        flavors: Flavors::Both,
        // plonk+groth16 wraps; on cpu-only this is the slow explicit CPU wrap coverage.
        timeout: Duration::from_secs(120 * 60),
        run: || -> ScenarioFuture { Box::pin(run()) },
    }
}

/// All four proof modes concurrently, each verified locally (smoke tier on gpu flavor).
async fn run() -> anyhow::Result<()> {
    let cluster = Cluster::standard().start().await?;

    let proof_ids = submit_proof_requests(ClusterProofRequests {
        rpc_url: cluster.gateway_rpc_url(),
        requests: vec![ClusterProofRequest {
            elf: programs::FIBONACCI_ELF.clone(),
            stdin: programs::FIBONACCI_STDIN.clone(),
            modes: vec![
                SP1ProofMode::Core,
                SP1ProofMode::Compressed,
                SP1ProofMode::Plonk,
                SP1ProofMode::Groth16,
            ],
        }],
    })
    .await?;

    let api = cluster.api_client().await?;
    for proof_id in &proof_ids {
        let pr = wait_proof_status(
            &api,
            proof_id,
            ProofRequestStatus::Completed,
            Duration::from_secs(60),
        )
        .await?;
        assert_proof_artifact_downloadable(&pr, &cluster.artifact_client()).await?;
    }

    cluster.shutdown().await;
    Ok(())
}
```

- [ ] **Step 2: Register in `scenarios/mod.rs`**

```rust
mod proof_modes;
mod quick;

use crate::scenario::Scenario;

pub fn all() -> Vec<Scenario> {
    vec![quick::scenario(), proof_modes::scenario()]
}
```

- [ ] **Step 3: Run it (gpu flavor)**

Run: `RUST_LOG=info cargo run --release --bin sp1-test-cluster --features gpu -- run proof-modes`
Expected: four proofs (core, compressed, plonk, groth16) all complete and verify locally; `scenario proof-modes PASSED`; exit 0. First run downloads plonk+groth16 circuit artifacts.

- [ ] **Step 4: Commit**

```bash
git add bin/test-cluster/src/scenarios/
git commit -m "feat(test-cluster): add proof-modes scenario covering all four proof modes"
```

---

### Task 7: `execute-only` scenario (standalone executor stack)

**Files:**
- Create: `bin/test-cluster/src/scenarios/execute_only.rs`
- Modify: `bin/test-cluster/src/scenarios/mod.rs`

Semantics pinned from code: with `execute_only_mode=true` the coordinator creates an `ExecuteOnly` task instead of `Controller` (`bin/coordinator/src/lib.rs:364`); the worker's ExecuteOnly task completes the proof with `ProofRequestStatus::Completed` (execution OK) or `Failed` (execution error), writing the JSON-serialized `ExecutionResult` as `extra_data` (`crates/worker/src/tasks/execute_only.rs:143-157`). The coordinator prunes proof/task state after completion, so task-status sampling is best-effort observation during the run; the durable evidence that the ExecuteOnly path ran is the terminal status + `execution_result`/`extra_data`.

The deterministic execution failure is fibonacci with an **empty stdin** — the program's first `sp1_zkvm::io::read` fails, no new test program needed.

- [ ] **Step 1: Write `scenarios/execute_only.rs`**

```rust
use std::time::Duration;

use anyhow::Context;
use sp1_sdk::{SP1ProofMode, SP1Stdin};

use crate::assert::{
    assert_execution_result, task_statuses, wait_proof_status, ExpectedExecution,
};
use crate::cluster::{Cluster, CoordinatorKind};
use crate::programs;
use crate::request::request_only;
use crate::scenario::{Flavors, Scenario, ScenarioFuture};
use sp1_cluster_common::proto::{ExecutionResult, ExecutionStatus, ProofRequestStatus};

/// Pinned gas-oracle regression value for the committed fibonacci elf+stdin
/// (cycles=18320, gas=20173 — carried over from the pre-scenario test).
const EXPECTED_FIBONACCI_GAS: u64 = 20173;

pub fn scenario() -> Scenario {
    Scenario {
        name: "execute-only",
        flavors: Flavors::Both,
        timeout: Duration::from_secs(45 * 60),
        run: || -> ScenarioFuture { Box::pin(run()) },
    }
}

/// Standalone executor cluster (prod is_executor_cluster shape): api + execute-only
/// coordinator + gateway + 1 CPU node. No prover, no proof artifacts — terminal state
/// and execution metadata come from the cluster API.
async fn run() -> anyhow::Result<()> {
    let cluster = Cluster::builder()
        .coordinator(CoordinatorKind::ExecuteOnly)
        .cpu_nodes(1)
        .start()
        .await?;
    let api = cluster.api_client().await?;
    let mut coordinator = cluster.coordinator_client().await?;

    // --- success case ---------------------------------------------------------------
    let proof_id = request_only(
        &cluster.gateway_rpc_url(),
        programs::FIBONACCI_ELF.clone(),
        programs::FIBONACCI_STDIN.clone(),
        SP1ProofMode::Compressed,
    )
    .await?;
    tracing::info!("submitted execute-only request {proof_id}");

    // Best-effort observation of the ExecuteOnly task while it runs (state is pruned
    // after completion, so this may legitimately see nothing for fast executions).
    if let Ok(statuses) = task_statuses(&mut coordinator, &proof_id).await {
        tracing::info!("task statuses during run: {statuses:?}");
    }

    let pr = wait_proof_status(
        &api,
        &proof_id,
        ProofRequestStatus::Completed,
        Duration::from_secs(10 * 60),
    )
    .await?;
    assert_execution_result(
        &pr,
        &ExpectedExecution {
            status: ExecutionStatus::Executed,
            min_cycles: 1,
            gas: Some(EXPECTED_FIBONACCI_GAS),
            require_pv_hash: true,
        },
    )?;

    // extra_data carries the same ExecutionResult, written by the ExecuteOnly task.
    let extra_data = pr.extra_data.clone().context("no extra_data on completed request")?;
    let from_extra: ExecutionResult = serde_json::from_str(&extra_data)
        .with_context(|| format!("extra_data not an ExecutionResult: {extra_data:?}"))?;
    anyhow::ensure!(
        from_extra.gas == EXPECTED_FIBONACCI_GAS,
        "extra_data gas {} != pinned {}",
        from_extra.gas,
        EXPECTED_FIBONACCI_GAS
    );
    tracing::info!(
        "execute-only success: cycles={} gas={} pv_hash={} bytes",
        from_extra.cycles,
        from_extra.gas,
        from_extra.public_values_hash.len()
    );

    // --- failure case: empty stdin makes fibonacci's io::read fail -------------------
    let fail_id = request_only(
        &cluster.gateway_rpc_url(),
        programs::FIBONACCI_ELF.clone(),
        SP1Stdin::new(),
        SP1ProofMode::Compressed,
    )
    .await?;
    tracing::info!("submitted execute-only failure request {fail_id}");

    let pr = wait_proof_status(
        &api,
        &fail_id,
        ProofRequestStatus::Failed,
        Duration::from_secs(10 * 60),
    )
    .await?;
    let er = pr
        .execution_result
        .as_ref()
        .context("failed request has no execution_result")?;
    anyhow::ensure!(
        er.status() == ExecutionStatus::Failed,
        "expected execution Failed, got {:?}",
        er.status()
    );
    // Observe-and-pin: log the cause on the first real run, then pin the exact variant
    // below (Step 3). Until pinned, only Failed status is asserted.
    tracing::info!("execution failure cause: {:?}", er.failure_cause());

    cluster.shutdown().await;
    Ok(())
}
```

- [ ] **Step 2: Register in `scenarios/mod.rs`**

```rust
mod execute_only;
mod proof_modes;
mod quick;

use crate::scenario::Scenario;

pub fn all() -> Vec<Scenario> {
    vec![
        quick::scenario(),
        proof_modes::scenario(),
        execute_only::scenario(),
    ]
}
```

- [ ] **Step 3: Run, observe the failure cause, pin it**

Run: `RUST_LOG=info cargo run --release --bin sp1-test-cluster --features gpu -- run execute-only`
Expected: success case completes with gas=20173; failure case reaches `Failed`; log line `execution failure cause: <Variant>`.

Then pin the observed variant — replace the `tracing::info!("execution failure cause: …")` line with (using the observed variant in place of the example):

```rust
anyhow::ensure!(
    er.failure_cause() == ExecutionFailureCause::HaltWithNonZeroExitCode,
    "expected pinned failure cause HaltWithNonZeroExitCode, got {:?}",
    er.failure_cause()
);
```

and add `ExecutionFailureCause` to the `sp1_cluster_common::proto` import list. (Likely variants given the executor's mapping in `crates/worker/src/tasks/execute_only.rs`: `HaltWithNonZeroExitCode` if the read panics, `UnspecifiedExecutionFailureCause` if it surfaces as `InvalidMemoryAccessUntrustedProgram`. Pin whatever is actually observed.)

- [ ] **Step 4: Re-run to confirm the pinned assert**

Run: `RUST_LOG=info cargo run --release --bin sp1-test-cluster --features gpu -- run execute-only`
Expected: `scenario execute-only PASSED`, exit 0.

- [ ] **Step 5: Commit**

```bash
git add bin/test-cluster/src/scenarios/
git commit -m "feat(test-cluster): add execute-only scenario with standalone executor stack"
```

---

### Task 8: Suite runner (`suite.rs`)

**Files:**
- Create: `bin/test-cluster/src/suite.rs`
- Modify: `bin/test-cluster/src/main.rs` (add `mod suite;`, wire `Cmd::Suite`)

- [ ] **Step 1: Write `suite.rs`**

```rust
use std::process::Stdio;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};

use crate::scenario::{resolve, Flavor, Scenario, Tier};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Pass,
    Fail,
    Timeout,
}

pub struct ScenarioResult {
    pub name: &'static str,
    pub outcome: Outcome,
    pub duration: Duration,
    pub log_path: std::path::PathBuf,
}

/// Run every (tier, flavor) scenario as a child process of this same binary.
/// A failure does not stop the suite. Returns true iff everything passed.
pub async fn run_suite(all: &[Scenario], tier: Tier, flavor: Flavor) -> Result<bool> {
    let scenarios = resolve(all, tier, flavor);
    let logs_dir = std::path::PathBuf::from("target/test-cluster-logs");
    std::fs::create_dir_all(&logs_dir).context("create logs dir")?;
    let exe = std::env::current_exe().context("current_exe")?;

    let mut results = Vec::new();
    for s in scenarios {
        let log_path = logs_dir.join(format!("{}.log", s.name));
        let log = std::fs::File::create(&log_path)
            .with_context(|| format!("create {}", log_path.display()))?;
        tracing::info!("=== running scenario {} (timeout {:?}) ===", s.name, s.timeout);
        let started = Instant::now();
        let mut child = tokio::process::Command::new(&exe)
            .arg("run")
            .arg(s.name)
            .stdout(Stdio::from(log.try_clone().context("clone log handle")?))
            .stderr(Stdio::from(log))
            .kill_on_drop(true)
            .spawn()
            .with_context(|| format!("spawn scenario {}", s.name))?;

        let outcome = match tokio::time::timeout(s.timeout, child.wait()).await {
            Ok(Ok(status)) if status.success() => Outcome::Pass,
            Ok(Ok(_)) => Outcome::Fail,
            Ok(Err(e)) => return Err(e).context("wait for scenario child"),
            Err(_elapsed) => {
                // Kill the scenario process; its testcontainers are reaped by ryuk
                // once the connection drops.
                let _ = child.kill().await;
                Outcome::Timeout
            }
        };
        let duration = started.elapsed();
        tracing::info!("=== scenario {} -> {:?} in {:?} ===", s.name, outcome, duration);
        results.push(ScenarioResult { name: s.name, outcome, duration, log_path });
    }

    println!("{}", render_table(&results));
    Ok(results.iter().all(|r| r.outcome == Outcome::Pass))
}

pub fn render_table(results: &[ScenarioResult]) -> String {
    let mut out = String::from(
        "\n  scenario                  result    duration    log\n  \
         ------------------------------------------------------------\n",
    );
    for r in results {
        out.push_str(&format!(
            "  {:<25} {:<9} {:>8.0?}    {}\n",
            r.name,
            format!("{:?}", r.outcome).to_uppercase(),
            r.duration,
            r.log_path.display()
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_renders_all_outcomes() {
        let results = vec![
            ScenarioResult {
                name: "quick",
                outcome: Outcome::Pass,
                duration: Duration::from_secs(61),
                log_path: "target/test-cluster-logs/quick.log".into(),
            },
            ScenarioResult {
                name: "execute-only",
                outcome: Outcome::Timeout,
                duration: Duration::from_secs(2700),
                log_path: "target/test-cluster-logs/execute-only.log".into(),
            },
        ];
        let table = render_table(&results);
        assert!(table.contains("quick"));
        assert!(table.contains("PASS"));
        assert!(table.contains("TIMEOUT"));
        assert!(table.contains("execute-only.log"));
    }
}
```

- [ ] **Step 2: Wire into `main.rs`**

Add `mod suite;` to the mod block, and replace the `Cmd::Suite` arm:

```rust
        Cmd::Suite(tier) => {
            let all = scenarios::all();
            let ok = suite::run_suite(&all, tier, flavor).await?;
            if !ok {
                std::process::exit(1);
            }
            Ok(())
        }
```

- [ ] **Step 3: Unit tests**

Run: `cargo test --release -p sp1-test-cluster --features gpu`
Expected: all unit tests pass (scenario, assert, parse, table).

- [ ] **Step 4: Run the smoke suite end-to-end (gpu flavor)**

Run: `RUST_LOG=info cargo run --release --bin sp1-test-cluster --features gpu -- suite smoke`
Expected: runs `proof-modes` then `execute-only` as child processes, prints the table with both PASS, exit 0. Logs land in `target/test-cluster-logs/`. (Run from the repo root; with `CARGO_TARGET_DIR=target_release` the logs path is still relative to the working directory.)

- [ ] **Step 5: If a CPU-only machine or spare time: cpu-only smoke**

Run: `SP1_CLUSTER_CPU_ONLY=1 RUST_LOG=info cargo run --release --bin sp1-test-cluster -- suite smoke`
Expected: runs `quick` then `execute-only`, both PASS. (Optional on the GPU dev box; mandatory before wiring CI in Plan 3.)

- [ ] **Step 6: Commit**

```bash
git add bin/test-cluster/src/
git commit -m "feat(test-cluster): add suite runner with per-scenario child processes"
```

---

### Task 9: README + spec touch-up

**Files:**
- Create: `bin/test-cluster/README.md`
- Modify: `docs/superpowers/specs/2026-06-09-cluster-e2e-coverage-matrix-design.md` (two wording fixes)

- [ ] **Step 1: Write `bin/test-cluster/README.md`**

```markdown
# sp1-test-cluster

Scenario-based e2e harness for sp1-cluster. Boots the whole cluster in-process
(api, coordinator, gateway, worker nodes) against dockerized redis + postgres
(testcontainers), submits proofs through the network gateway in hosted SDK mode,
and asserts terminal API status, execution metadata, artifact availability, and
local proof verification.

Design: `docs/superpowers/specs/2026-06-09-cluster-e2e-coverage-matrix-design.md`

## Build flavors

`SP1_CLUSTER_CPU_ONLY` is compile-time, so the flavor is chosen at build time:

- gpu flavor: `--features gpu` (needs an Nvidia GPU)
- cpu-only flavor: `SP1_CLUSTER_CPU_ONLY=1`, no gpu feature (runs anywhere)

## Usage

```bash
export RUSTFLAGS="-C linker=clang -C link-arg=-fuse-ld=mold"
export CARGO_TARGET_DIR=target_release

# list scenarios
cargo run --release --bin sp1-test-cluster --features gpu -- list

# run one scenario
RUST_LOG=info cargo run --release --bin sp1-test-cluster --features gpu -- run quick

# smoke suite (default when no args are given)
RUST_LOG=info cargo run --release --bin sp1-test-cluster --features gpu -- suite smoke

# cpu-only flavor (machines without a GPU)
SP1_CLUSTER_CPU_ONLY=1 RUST_LOG=info cargo run --release --bin sp1-test-cluster -- run quick
```

Suite logs: `target/test-cluster-logs/<scenario>.log`.

Requirements: docker (testcontainers), network access on first run (circuit
artifacts for CPU workers, container images).
```

- [ ] **Step 2: Spec touch-up**

In `docs/superpowers/specs/2026-06-09-cluster-e2e-coverage-matrix-design.md`:

1. Replace the `panic` test-program bullet:
   - Old: "`panic` — new tiny program that panics during execution, for deterministic execution-failure / fatal paths. Committed as `.elf.zst` like the others."
   - New: "deterministic execution failure = fibonacci with an **empty stdin** (the program's first `io::read` fails). No extra committed program needed."
2. In the `Cluster` builder responsibilities, replace "Owns embedded redis + postgres (and minio when selected); fresh data dirs per scenario process." with "Owns dockerized redis + postgres via testcontainers (and minio when selected); fresh containers per scenario process."

- [ ] **Step 3: Final check + commit**

Run: `cargo test --release -p sp1-test-cluster --features gpu`
Expected: all green.

```bash
git add bin/test-cluster/README.md docs/superpowers/specs/2026-06-09-cluster-e2e-coverage-matrix-design.md
git commit -m "docs(test-cluster): add harness README; align spec with implementation"
```

---

## Self-review notes (already applied)

- **Spec coverage (Plan 1 scope):** harness subcommands ✓ (Tasks 1, 5, 8), Cluster builder + per-component tokens + kill/stop ✓ (Task 2; `restart` deliberately deferred to Plan 2 where it's first used — YAGNI), scenarios 1–3 ✓ (Tasks 5–7), assertion pack ✓ (Task 4), gateway-ingress asserts ride on `submit_proof_requests`/`request_only` ✓. Scenarios 4–15, test-hooks, heartbeat setting, MinIO, CI = Plans 2–3.
- **Execution-result asserts for prover scenarios** (`quick`/`proof-modes`): plan asserts Completed + artifact + local verify; `execution_result` population on the **prover** path is not pinned by code reading yet — Plan 2 adds it after observing real payloads (the spec's "pinned during implementation" risk (a) covers the executor path, pinned in Task 7).
- **Type consistency:** `Scenario`/`Flavors`/`Tier` (Task 1) match usage in Tasks 5–8; `task_statuses` returns `Vec<(TaskStatus, Vec<String>)>` and is only consumed for logging in Task 7; `RawCoordinatorClient` alias used in both `cluster.rs` and `assert.rs`.
- **No placeholders:** the only observe-then-pin step (failure cause, Task 7 Step 3) includes the exact replacement code and candidate variants.
