# sp1-cluster e2e coverage matrix — design

Date: 2026-06-09
Status: approved (design review with Tobias)

## Goal

The current e2e test (`bin/test-cluster`) proves a single Fibonacci compressed proof. That is not
enough signal for provers upgrading sp1-cluster in production. This design defines an explicit
coverage matrix — worker topologies, proof modes, execute-only mode, concurrency, failure/requeue,
retries, cancellation, artifact stores, restarts, and gateway ingress — plus a cheap smoke tier
that runs per-PR and a comprehensive full tier that runs post-merge.

Every scenario asserts: API terminal status, task/proof completion behavior, artifact
availability, and local proof verification where applicable.

## Decisions (from design review)

| Decision | Choice |
|---|---|
| Target environments | CI on a GPU runner. Per-PR = smoke tier (all four proof modes). Full matrix = post-merge on main + `workflow_dispatch`. |
| CPU-only axis | `SP1_CLUSTER_CPU_ONLY` is a suite-wide build axis, not a scenario: it exists so the whole suite can run on machines without Nvidia GPUs. No dedicated "cpu-only" scenario; instead a cheap `quick` scenario (core+compressed) is the convenient CPU smoke. |
| Multi-GPU topology | Multiple GPU nodes share one physical GPU in-process — risk of GPU memory conflict. Any >1-GPU-node topology is gated behind a memory-footprint verification; until verified, capacity scenarios use multiple CPU nodes + 1 GPU node. |
| Harness shape | Scenario-per-process, one binary. `suite` drives scenarios as child processes of itself. |
| Fault injection | In-process: `JoinHandle::abort()` = crash, per-component `CancellationToken` = graceful. No subprocess components. |
| Execute-only topology | Standalone scenario with its own stack (api + executor coordinator + cpu node + gateway), matching prod `is_executor_cluster` separation. No shared-API race. |
| Retryable-failure injection | `#[cfg(feature = "test-hooks")]` hook in node/worker, compiled out of prod builds. |
| S3 coverage | MinIO child process in the full tier (managed like embedded redis/postgres). |

## Section 1: Harness architecture

One binary (`sp1-test-cluster`), three subcommands (clap):

- `run <scenario>` — boot a fresh environment, run one scenario, exit 0/1.
- `suite <smoke|full>` — resolve the scenario list for the tier and current build flavor, spawn
  `current_exe() run <scenario>` sequentially as child processes, enforce a per-scenario hard
  timeout (kill the child process tree on expiry), capture each child's stdout/stderr to a
  per-scenario log file, print a pass/fail summary table, exit nonzero if any scenario failed.
  A scenario failure does not stop the suite; remaining scenarios still run.
- `list` — print scenarios with tier and flavor metadata.

### Build flavors

`SP1_CLUSTER_CPU_ONLY` is compile-time (`option_env!`), so the flavor is a separate build:

- **gpu flavor**: `cargo build --features gpu`
- **cpu-only flavor**: `SP1_CLUSTER_CPU_ONLY=1 cargo build` (no gpu feature)

The cpu-only flavor is a **suite-wide axis**, not a coverage row: it exists so the whole suite can
run on machines without Nvidia GPUs. Nearly every scenario runs under both flavors; the cluster
shape and workload adapt per flavor (see "standard" shape and workload parametrization below).
The binary knows its own flavor (same `option_env!`/`cfg(feature)` checks); `suite` resolves the
scenario list from (tier, flavor).

### Workload parametrization per flavor

Long-running and heavy workloads differ by flavor so the cpu-only suite stays functional-coverage
focused rather than a performance test:

- **gpu flavor**: rsp is the "large/long" program; proof-modes covers all four modes.
- **cpu-only flavor**: the "large/long" program is fibonacci with a large `n` stdin (generated at
  runtime — many cycles, no new committed artifacts; rsp on CPU is impractically slow).
  plonk/groth16 run on CPU only where the scenario explicitly tests them (proof-modes in the full
  tier); everything else uses core/compressed.

### `Cluster` builder

Refactor the current `main.rs` bring-up into a composable builder:

```rust
let cluster = Cluster::builder()
    .api()
    .coordinator(CoordinatorKind::Prover)      // or CoordinatorKind::ExecuteOnly
    .gateway()
    .cpu_nodes(2)
    .gpu_nodes(1)
    .artifact_store(ArtifactStore::Redis)       // or ArtifactStore::S3Minio
    .start()
    .await?;
```

Responsibilities:

- Owns dockerized redis + postgres via testcontainers (and minio when selected); fresh
  containers per scenario process.
- Fixed ports (process-per-scenario + sequential suite ⇒ no conflicts).
- `wait_for_tcp` gating between tiers, as today.
- Each component (api, coordinator(s), gateway, each node) gets its **own** `CancellationToken`
  and `JoinHandle`, recorded in a component map keyed by name (e.g. `cpu-node-0`).

Cluster methods:

- `kill(component)` — `JoinHandle::abort()`. Simulates a crash: futures drop, TCP connections
  close, coordinator heartbeat cleanup fires.
- `stop(component)` — cancel that component's token and await the handle. Graceful shutdown.
- `restart(component)` — `stop` (or after `kill`) then re-invoke the component's lib `start` fn
  with the same config/ports; postgres/redis state persists across the restart.
- `submit(...)` — SDK-through-gateway helper (today's `request.rs`), returning proof ids and
  verified proofs.
- `api_client()` — `ClusterServiceClient` against the api gRPC addr.
- `coordinator_client()` — `WorkerServiceClient` (or raw tonic client) for `GetStats` /
  `GetTaskStatuses`.
- `artifact_client()` — handle to the active artifact store.

### Scenario registry

```rust
struct Scenario {
    name: &'static str,
    flavors: Flavors,              // Gpu | CpuOnly | Both
    run: fn() -> BoxFuture<'static, anyhow::Result<()>>,
}
```

Static registry array; each scenario builds exactly the cluster shape it needs (shape and workload
may differ per flavor — see workload parametrization). Tier membership is resolved per
(tier, flavor) — explicit smoke lists per flavor, full = everything runnable under the flavor
(see Section 2 "Tiers").

### Test programs

Committed pre-built artifacts under `bin/test-cluster/programs/` (as today):

- `fibonacci` — small program (exists).
- `rsp` — large program for long-running proofs and large artifacts (exists).
- deterministic execution failure = fibonacci with an **empty stdin** (the program's first
  `io::read` fails, halting with a non-zero exit code). No extra committed program needed.

## Section 2: Coverage matrix

Conventions:

- Cluster shape "standard" = api + prover coordinator + gateway + 1 CPU node + 1 GPU node on the
  gpu flavor; api + prover coordinator + gateway + 1 CPU node on the cpu-only flavor.
- "long" program = rsp (gpu flavor) / fibonacci with large `n` (cpu-only flavor) — see workload
  parametrization in Section 1.
- Flavor column = which builds the scenario can run under. Nearly everything is "both"; the
  cpu-only build is how the suite runs on GPU-less machines.

| # | Scenario | Flavor | Cluster shape | Asserts (beyond terminal status + local verify) |
|---|----------|--------|---------------|--------------------------------------------------|
| 1 | `proof-mode-{core,compressed,plonk,groth16}` | both | standard | one scenario per mode, one request per process (see implementation findings); each: `Completed`, proof artifact downloadable, local verify |
| 2 | `quick` | both | standard | a single fib compressed request — the cheap, convenient run for dev loops and CPU machines; also exercises the `SP1_CLUSTER_CPU_ONLY` remap paths (`coordinator/src/util.rs:102`, balanced policy) when built cpu-only |
| 3 | `execute-only` | both | standalone executor stack: api + execute-only coord + gateway + 1 CPU node (no prover) | ExecuteOnly task created → `Succeeded`; `ExecutionResult`: `Executed`, cycles > 0, gas == expected (oracle regression, e.g. fib gas 20173), `public_values_hash` nonempty; `extra_data` parses as `ExecutionResult`. Variant: panic program → `ExecutionStatus=Failed` + `failure_cause` set |
| 4 | `multi-worker` | both | gpu: 2 CPU + 1 GPU (see multi-GPU gate below); cpu-only: 4 CPU nodes | 4 concurrent fib proofs (2 core, 2 compressed); all `Completed`; every worker completed ≥ 1 task (`GetTaskStatuses`) — exercises assignment under capacity |
| 5 | `mixed-load` | both | standard | long-program groth16 (gpu) / compressed (cpu-only) + 4 fib compressed submitted back-to-back while workers busy; all `Completed` within per-request deadline (no starvation) |
| 6 | `worker-death-requeue` | both | standard + 1 extra CPU node | kill the CPU node owning a `Running` task; worker disappears from `GetStats` within heartbeat window; task requeued with `retries` **unchanged** (`coordinator/src/lib.rs:843` contract); surviving CPU node completes; verify proof |
| 7 | `retryable-then-success` | both | standard | test-hooks: fail first attempt of a prove task type; observe `FailedRetryable` → retried (`retries == 1`) → `Succeeded`; proof `Completed` |
| 8 | `fatal-failure` | both | standard | panic program prove request → proof `Failed`; `ExecutionFailureCause` set; `ProvingFailure`/`extra_data` populated; SDK call surfaces the failure |
| 9 | `cancel-pending` | both | api + coord + gateway, **no nodes** | submit; request stays `Pending`; cancel via API; `Cancelled`; no tasks `Running` |
| 10 | `cancel-active` | both | standard | cancel while long-program proof `Running`; `CancelTask` reaches workers; coordinator active tasks → 0; `Cancelled`; no stuck tasks |
| 11 | `coordinator-restart` | both | standard | kill + restart coordinator on the same port against the same api; workers rolled and re-registered; a NEW proof completes. (Mid-flight proofs do not survive a coordinator crash today — the claimer only claims unhandled requests; that recovery gap is documented product work, pinned in the scenario comment.) |
| 12 | `worker-restart` | both | standard | graceful `stop` (Close/drain) + restart of the CPU node between proofs; worker de/re-registers; next proof `Completed`. (Mid-task death is scenario 6's job.) |
| 13 | `api-outage` | both | standard | stop api while proving (graceful, port released); 20s outage; restart api on same postgres; terminal status eventually written (covers the #126 re-issue path); `Completed` |
| 14 | `shutdown-drain` | both | standard | root-token shutdown mid-proof; all components exit within bounds; ALL fixed ports re-bindable afterwards (detached-task-zombie regression guard) |
| 15 | `s3-artifacts` | both | standard + MinIO, S3 artifact store | long-program proof (large artifacts → upload/download/chunking); `Completed` + verify + proof artifact retrievable from S3 store |

**Multi-GPU gate**: multiple GPU nodes in one process share one physical GPU; GPU memory conflict
is likely. Before any topology with >1 GPU node ships, measure a single GPU node's peak GPU memory
under proving load on the target runner and confirm two fit (contexts + headroom). Until then,
`multi-worker` exercises capacity with multiple CPU nodes + 1 GPU node; the GPU-side capacity
queueing is still exercised (more GPU tasks than the single GPU worker's slots).

**Gateway/SDK ingress** is covered by every scenario rather than a dedicated one: all submissions
go through `sp1-cluster-network-gateway` in hosted SDK mode. The shared submit helper asserts the
full ingress path: program setup/registration, stdin artifact upload, proof request creation,
status polling transitions, and final proof retrieval.

**Tiers** (resolved per flavor; updated 2026-06-10 from implementation findings):

- `proof-modes` is implemented as FOUR scenarios (`proof-mode-{core,compressed,plonk,groth16}`),
  one request per scenario per process — see "Implementation findings" below.
- smoke @ gpu = {`proof-mode-core`, `proof-mode-compressed`, `execute-only`}. plonk/groth16 are
  full-tier only: their SHRINK_WRAP stage needs roughly >=16GB free GPU memory, which shared
  desktop GPUs lack (supersedes the earlier all-four-modes-per-PR decision, per review 2026-06-10).
- smoke @ cpu-only = {`quick`, `execute-only`} — cheap, no wrap circuits.
- full @ gpu = all scenarios (includes the wrap modes; targets dedicated/headless GPUs).
- full @ cpu-only = all scenarios with per-flavor workload parametrization (wrap modes on CPU —
  slow but explicit coverage of the CPU wrap path).

**Implementation findings (2026-06-10)** — constraints discovered while building Plan 1:

1. **Per-request GPU memory growth**: the in-process GPU worker's device-memory high-water mark
   grows per proof request (retained core + lazily-initialized recursion arenas). On a 24GB
   desktop card with ~11GB of graphics, a second request's ProveShard OOMs (`AllocError`,
   observed at 23.9/24.5GB). Hence: one request per scenario process; `quick` = a single
   compressed request; concurrency/capacity coverage is full-tier on dedicated GPUs.
2. **Components leak detached tasks on token-cancel**: api's gRPC server is a detached
   `tokio::spawn` (`bin/api/src/lib.rs`), coordinator periodic pollers and the gateway behave the
   same — cancelling a component's token leaves zombie servers bound to the fixed ports. A second
   `Cluster` in one process talks to those zombies (dead postgres/redis behind them). Hence: one
   cluster per process, suite runner spawns a child process per scenario. The prod-grade fix
   (graceful shutdown wired to the token everywhere) is a Plan-2 prerequisite for the restart
   scenarios (11–14).

## Section 3: Assertion pack

Shared helpers used by every scenario:

- **API**: `ProofRequestGet` → `proof_status`, `execution_result`
  (status/failure_cause/cycles/gas/public_values_hash), `proof_artifact_id`, `extra_data`
  (parsed as `ExecutionResult` or `ProvingFailure` depending on scenario).
- **Coordinator**: `GetStats` (worker presence, queue depths), `GetTaskStatuses` (task states +
  `retries` counters for a proof).
- **Artifacts**: proof artifact downloadable via the active artifact client, nonzero size.
- **Proof**: `sp1_sdk` local verification per mode (existing `request.rs` flow).
- **Polling discipline**: every wait is a bounded poll with a deadline and a descriptive timeout
  error (which assertion, last observed state). No unbounded `wait_proof` calls.

## Section 4: Production-code changes (explicit, minimal)

All four landed on 2026-06-10:

1. **`test-hooks` feature** (DONE) in `crates/worker` (forwarded through `bin/node`): env
   `TEST_FAIL_TASK=<TASK_TYPE_NAME>:<n>` → the first `n` attempts of that task type return a
   retryable error. `#[cfg(feature = "test-hooks")]`, compiled out of prod builds; only the
   test-cluster dependency enables it.
2. **`worker_heartbeat_timeout_secs` in coordinator `Settings`** (DONE; default stays 30s via
   `DEFAULT_WORKER_HEARTBEAT_TIMEOUT`, env `COORDINATOR_WORKER_HEARTBEAT_TIMEOUT_SECS`).
   worker-death-requeue sets 5s so the requeue test doesn't idle for 30s+.
3. **MinIO endpoint** (DONE): the S3 client picks up `AWS_ENDPOINT_URL` via aws-config; an
   `AWS_S3_FORCE_PATH_STYLE` env opt-in enables path-style addressing for MinIO.
4. **Token-correct graceful shutdown** (DONE): api gained `run_with_shutdown` (tonic
   `serve_with_shutdown` + axum `with_graceful_shutdown`), the gateway gained a
   `run_with_shutdown` wrapper over its already-token-ready `serve`, and the coordinator's
   heartbeat/periodic/claimer/status tasks now stop on token-cancel. This unblocked the
   restart scenarios (11–14) and `Cluster::restart`; the shutdown-drain scenario guards the
   regression by asserting all fixed ports are re-bindable after shutdown.

## Section 5: CI wiring

- **PR + main push (smoke)**:
  - GPU runner job: gpu build, `suite smoke` (`proof-modes`, `execute-only`).
  - 16cpu runner job: cpu-only build, `suite smoke` (`quick`, `execute-only`).
  - Caches: cargo, circuit artifacts dir (plonk/groth16, ~GB, hot after first run), embedded
    postgres/redis/minio binaries.
- **main post-merge + `workflow_dispatch` (full)**:
  - GPU runner job: gpu build, `suite full`.
  - 16cpu runner job: cpu-only build, `suite full` (per-flavor workload parametrization keeps
    this functional-coverage focused; runtime monitored and trimmed if it grows unreasonable).
- Per-scenario log files uploaded as CI artifacts on failure.

## Section 6: Error handling & risks

- Suite runner enforces a hard per-scenario timeout and kills the child process tree on expiry;
  the suite continues with remaining scenarios and reports the timeout as a failure.
- Isolation: fresh embedded postgres/redis data dirs + tmp dirs per scenario process; no state
  can leak between scenarios.
- **Risks to pin during implementation**:
  - (a) Exact terminal-status semantics of an `execute_only_mode` coordinator in a standalone
    stack (what it writes to `proof_status` when there is no prover). Scenario 3 asserts the
    verified-correct behavior once confirmed against the code.
  - (b) GPU runner availability/queueing on runs-on.
  - (c) plonk/groth16 first-run circuit download time (cached afterwards).
  - (d) Multi-GPU-node topology: blocked on the GPU memory-footprint verification (Section 2,
    multi-GPU gate). Single-GPU-node assumption holds everywhere else in this design.
  - (e) cpu-only full-tier runtime (proof-modes wrap on CPU, long fib runs) — measure on first
    runs; trim scenario workloads if the job exceeds a reasonable post-merge budget.

## Out of scope

- SPN / network-services integration (fulfiller-driven ingress). Gateway hosted-SDK mode is the
  ingress under test.
- Real S3 (MinIO stands in).
- Subprocess/SIGKILL-level crash realism; `JoinHandle::abort()` is the crash model.
- Multi-machine topology; everything runs on one machine.
