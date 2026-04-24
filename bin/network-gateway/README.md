# network-gateway

An SDK-compatible gRPC + HTTP gateway that terminates the same network contract
`sp1-sdk`'s `NetworkProver` uses against `api.prover.network`, but fans the
requests out to a self-hosted `sp1-cluster`. It replaces the prover network for
deployments that want to run their own cluster.

## Why it exists

`sp1-sdk` callers talk to a remote `ProverNetwork` + `ArtifactStore` pair. The
cluster's own `ClusterService` gRPC is a different shape entirely, so today
direct callers either hit the Succinct prover network or bypass the SDK (see
op-succinct's `multi.rs`). The gateway lets any existing SDK caller be
re-pointed at a self-hosted cluster with only an RPC URL change:

```rust
let prover = ProverClient::builder()
    .network()
    .rpc_url("http://gateway.internal:50061")
    .build();
```

## Where it sits

```
           sp1-sdk
              │
              │ gRPC (ProverNetwork.Base + ArtifactStore)
              │ HTTP PUT/GET artifacts
              ▼
        network-gateway
              │
              │ ClusterService gRPC
              ▼
           bin/api                coordinator + nodes
              │                         │
              ▼                         ▼
          Postgres             artifact store (S3 / Redis)
```

Only this binary is new. The coordinator, nodes, API, and artifact store are
untouched; `bin/fulfiller` stays in the repo for mainnet deployments and is
simply not run in self-hosted mode.

## Running locally

```bash
# Requires a bin/api (ClusterService) reachable at GATEWAY_CLUSTER_RPC.
export GATEWAY_CLUSTER_RPC=http://localhost:50051
export GATEWAY_ARTIFACT_STORE=redis
export GATEWAY_REDIS_NODES=redis://localhost:6379
export GATEWAY_PUBLIC_HTTP_URL=http://localhost:8081

cargo run -p sp1-cluster-network-gateway
```

The gateway binds two ports: gRPC on `GATEWAY_GRPC_ADDR` (default `0.0.0.0:50061`)
and HTTP on `GATEWAY_HTTP_ADDR` (default `0.0.0.0:8081`). SDK clients hit both —
make sure both are reachable from the caller.

## Testing against the local docker-compose stack

The repo ships [`infra/docker-compose.local.yml`](../../infra/docker-compose.local.yml)
which stands up the full cluster (Postgres + Redis + `api` + coordinator + one
CPU and GPU node, plus Grafana Alloy for traces) on a shared Docker network. The
`api`'s gRPC port is published on `127.0.0.1:50051` and Redis on
`127.0.0.1:6379` — exactly what the gateway needs.

1. Bring up the stack. Drop `gpu0` if you don't have an NVIDIA runtime; the
   gateway only needs `redis`, `postgresql`, `api`, and optionally a
   coordinator + worker for a full prove round-trip:

   ```bash
   cd infra
   # minimum to exercise the gateway's request-submission path:
   docker compose -f docker-compose.local.yml up -d redis postgresql api coordinator cpu-node
   # or, with GPU, the full stack:
   # docker compose -f docker-compose.local.yml up -d
   ```

2. Wait for `api` to finish migrations (a few seconds on first boot):

   ```bash
   docker compose -f docker-compose.local.yml logs -f api
   ```

3. Run the gateway natively against the stack:

   ```bash
   GATEWAY_CLUSTER_RPC=http://127.0.0.1:50051 \
   GATEWAY_ARTIFACT_STORE=redis \
   GATEWAY_REDIS_NODES=redis://:redispassword@127.0.0.1:6379/0 \
   GATEWAY_PUBLIC_HTTP_URL=http://127.0.0.1:8081 \
   cargo run -p sp1-cluster-network-gateway
   ```

4. Smoke-check:

   ```bash
   curl http://127.0.0.1:8081/healthz                       # -> OK
   grpcurl -plaintext 127.0.0.1:50061 list                  # lists ProverNetwork + ArtifactStore
   ```

5. Drive it with `sp1-sdk` — point `ProverClient` at the gateway, **not** the
   api's `:50051`:

   ```rust
   let client = ProverClient::builder()
       .network()
       .rpc_url("http://127.0.0.1:50061")
       .network_mode(NetworkMode::Reserved)
       .build();
   ```

Iteration tip: keep the docker-compose stack up and only restart `cargo run
-p sp1-cluster-network-gateway` between changes. Teardown when done:

```bash
docker compose -f infra/docker-compose.local.yml down
```

With the default `auth_mode=none`, the SDK's private key only needs to be
parseable by `NetworkSigner`; the gateway doesn't check it. Without a GPU
worker, proof requests land in `Pending` and stay there — that's enough to
verify the wire contract end-to-end but you'll need a worker (CPU or GPU) in
the compose stack for a full prove round-trip.

## Configuration

All flags are also environment variables with the `GATEWAY_` prefix.

| Variable | Default | Purpose |
|---|---|---|
| `GATEWAY_GRPC_ADDR` | `0.0.0.0:50061` | gRPC listen address. |
| `GATEWAY_HTTP_ADDR` | `0.0.0.0:8081` | HTTP listen address (artifact proxy + `/healthz`). |
| `GATEWAY_PUBLIC_HTTP_URL` | `http://localhost:8081` | Base URL embedded in `artifact_uri`/`program_uri`/`proof_uri` handed back to SDK callers. Must be reachable *from* the caller. |
| `GATEWAY_CLUSTER_RPC` | *(required)* | `bin/api` gRPC endpoint. |
| `GATEWAY_ARTIFACT_STORE` | `s3` | Backend: `s3` or `redis`. |
| `GATEWAY_S3_BUCKET` / `GATEWAY_S3_REGION` / `GATEWAY_S3_CONCURRENCY` | — | Required when `s3`. |
| `GATEWAY_REDIS_NODES` (comma-separated) / `GATEWAY_REDIS_POOL_MAX_SIZE` | — | Required when `redis`. |
| `GATEWAY_BALANCE_AMOUNT` | `U256::MAX` | Decimal string returned by `get_balance`. |
| `GATEWAY_AUTH_MODE` | `none` | `none`, `verify`, or `allowlist` (see below). |
| `GATEWAY_AUTH_ALLOWLIST` | — | Comma-separated `0x`-addresses. Required when `auth_mode=allowlist`. |

## Auth modes

Applies only to the three RPCs the SDK signs — `create_artifact`,
`create_program`, `request_proof`. Read-only RPCs
(`get_nonce`, `get_balance`, `get_proof_request_status`,
`get_proof_request_details`, `get_filtered_proof_requests`) are unauthenticated
because the SDK doesn't sign them.

- **`none` (default)** — accept everything; `requester` is reported as the zero
  address. Intended for trusted-VPC deployments or when an external auth proxy
  sits in front of the gateway.
- **`verify`** — parse the 65-byte `[r‖s‖v]` signature (alloy `Signature::from_raw`
  handles both `v∈{0,1}` and `v∈{27,28}`), EIP-191 recover the signer, and set
  the cluster-side `requester` field to the recovered address. No nonce replay
  check — auth is scoped to the lifetime of the process.
- **`allowlist`** — `verify` + the recovered address must be in
  `GATEWAY_AUTH_ALLOWLIST`.

## Design notes

A few things worth knowing when reading the code or debugging wire issues:

- **Artifact traffic is always proxied through the gateway's HTTP endpoint**,
  never raw S3 presigned URLs. The SDK zstd-compresses on `PUT` but does
  **not** zstd-decode on `GET`, while the cluster artifact store stores
  `zstd(bincode(...))` under the hood. The gateway uses
  `ArtifactClient::upload_raw_compressed` on `PUT` (preserve the SDK's
  zstd bytes verbatim) and `ArtifactClient::download_raw` on `GET` (zstd-decode,
  hand the SDK raw bincode). Costs a proxy hop but keeps the wire format
  correct across both backends.
- **Program registry lives in the artifact store, not Postgres.** `create_program`
  writes a bincode sidecar at `program_sidecar_<hex(vk_hash)>` containing
  `{vk_bytes, program_artifact_id}`. `get_program` and `request_proof` read
  that sidecar. No schema changes, no migration, no `bin/api` bump.
- **Request IDs use the cluster's `"req".create_type_id::<V7>()` typeid
  convention.** The `request_id` bytes returned to the SDK are the UTF-8 of
  e.g. `req_01hxyz...`, which doubles as the `proof_id` string the cluster
  stores — so cluster rows carry typeid-style IDs consistent with the rest of
  the system.
- **Reserved mode only.** The gateway implements the `base` proto variant
  (Reserved/self-hosted). Auction methods (`bid`, `settle`, `cancel_request`,
  etc.) return `Unimplemented`.
- **`get_filtered_proof_requests` honors a subset of filters.** The cluster's
  `ProofRequestListRequest` doesn't carry `vk_hash`, `requester`, `fulfiller`,
  `from`, `to`, `version`, or `mode` — those fields on the SDK request are
  silently dropped. Status filters, `minimum_deadline`, and pagination
  (`limit`, `page`) round-trip.

## Status mapping

| Cluster `ProofRequestStatus` | SDK `FulfillmentStatus` |
|---|---|
| `Pending` | `Requested` |
| `Completed` | `Fulfilled` (`proof_uri` populated) |
| `Failed` \| `Cancelled` | `Unfulfillable` |

| Cluster `ExecutionStatus` | SDK `ExecutionStatus` |
|---|---|
| `Unexecuted` | `Unexecuted` |
| `Executed` | `Executed` |
| `Failed` (any cause) \| `Cancelled` | `Unexecutable` |

The `Unexecutable` mapping is what drives the SDK's `process_proof_status` to
bail out of `wait_proof` early on cycle-limit / invalid-memory / etc. failures.
