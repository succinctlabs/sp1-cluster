# sp1-test-cluster

Scenario-based e2e harness for sp1-cluster. Boots the whole cluster in-process
(api, coordinator, gateway, worker nodes) against dockerized redis + postgres
(+ MinIO for the S3 scenario) via testcontainers, submits proofs through the
network gateway in hosted SDK mode, and asserts terminal API status, execution
metadata, artifact availability, and local proof verification.

CI: `.github/workflows/e2e.yml` (smoke per-PR; full post-merge/manual).

## Build flavors

`SP1_CLUSTER_CPU_ONLY` is compile-time, so the flavor is chosen at build time:

- gpu flavor: `--features gpu` (needs an Nvidia GPU)
- cpu-only flavor: `SP1_CLUSTER_CPU_ONLY=1`, no gpu feature (runs anywhere)

The cpu-only flavor runs the same suite on machines without GPUs; workloads do
not change (long/large scenarios prove rsp on CPU, which is slow).

## Usage

```bash
export RUSTFLAGS="-C linker=clang -C link-arg=-fuse-ld=mold"
export CARGO_TARGET_DIR=target_release

# list scenarios
cargo run --release --bin sp1-test-cluster --features gpu -- list

# run one scenario
RUST_LOG=info cargo run --release --bin sp1-test-cluster --features gpu -- run proof-mode-compressed

# smoke suite (default when no args are given)
RUST_LOG=info cargo run --release --bin sp1-test-cluster --features gpu -- suite smoke

# the whole matrix
RUST_LOG=info cargo run --release --bin sp1-test-cluster --features gpu -- suite full

# cpu-only flavor (machines without a GPU)
SP1_CLUSTER_CPU_ONLY=1 RUST_LOG=info cargo run --release --bin sp1-test-cluster -- suite full
```

Suite logs: `target/test-cluster-logs/<scenario>.log` (relative to the working
directory). Requirements: docker (testcontainers), network access on first run
(circuit artifacts for CPU workers, container images).

## Scenarios

| Scenario | What it pins down |
|---|---|
| `proof-mode-{core,compressed,plonk,groth16}` | every proof mode, verified locally |
| `execute-only` | standalone executor stack (prod `is_executor_cluster` shape): ExecuteOnly task, gas oracle (fib gas pinned), pv hash, empty-stdin failure cause |
| `multi-worker` | assignment under capacity (2 CPU + 1 GPU / 4 CPU), 4 concurrent requests |
| `mixed-load` | long + 4 small requests back-to-back; no starvation |
| `worker-death-requeue` | kill the worker owning active work; 5s heartbeat cleanup; requeued work completes after restart |
| `retryable-then-success` | TEST_FAIL_TASK fails the first Controller attempt; retry completes the proof |
| `fatal-failure` | empty-stdin execution failure on the PROVING path → Failed + failure metadata |
| `cancel-pending` | cancel with no workers: API row Cancelled, queue dropped, status holds |
| `cancel-active` | cancel mid-proving (api + coordinator, mirroring the fulfiller): work drained, status holds |
| `coordinator-restart` | kill+restart coordinator on the same port; workers rolled; new proof completes |
| `worker-restart` | graceful stop/drain + restart of a worker between proofs |
| `api-outage` | stop API mid-proof, restore; terminal status recovered (#126 re-issue path) |
| `shutdown-drain` | whole-cluster shutdown with work in flight; all fixed ports re-bindable (zombie regression guard) |
| `s3-artifacts` | full pipeline on MinIO-backed S3 store (endpoint + path-style) |

Known product gap (documented, not asserted): a proof claimed by a coordinator
that crashes mid-flight is orphaned — the claimer only claims unhandled
requests. `coordinator-restart` pins restart/rebind/reconnect, not mid-flight
recovery.

## Tiers

Each scenario declares its own tier (the `tier` field in its `Scenario`
definition, next to the constraints that motivate the choice); the smoke tier
runs the scenarios that declare `Smoke`, the full tier runs everything.

- smoke: `proof-mode-core`, `proof-mode-compressed`, `proof-mode-groth16`,
  `execute-only`
- full: every scenario

## GPU memory requirements

Scenarios run against fresh clusters in fresh processes; the in-process GPU
worker's device-memory high-water mark grows per request (retained
core/recursion arenas):

- single-request scenarios fit a 24GB desktop GPU even with ~11GB of desktop
  graphics in use;
- `proof-mode-plonk`/`-groth16` (SHRINK_WRAP) and the multi-request scenarios
  need a dedicated/headless GPU (~>=16GB free) — that's what the full tier's CI
  GPU runner provides (prod wrap runs on 24GB g6/g5 workers).
