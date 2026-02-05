# Requester Utils

This crate provides a set of utilities for requesting proofs from the SP1 Cluster, bypassing the network / sdk layer. Most users should request proofs using the `sp1-sdk` crate.

## Quick start

Your machine should have 64 GB of RAM, and an NVIDIA GPU. 4090, 5090, or L4 are recommended. 

Put the following variables in your .env

```bash
CLI_CLUSTER_RPC=http://localhost:50051
CLI_REDIS_NODES=redis://:redispassword@localhost:6379/0
```

Start a local docker compose in the background. This uses a local redis artifact store.
```bash
docker compose -f infra/docker-compose.local.yml up -d
```

Then run the following command to request a proof:
```bash
cargo run --bin sp1-cluster-cli bench input v6-fib-program.bin v6-fib-stdin.bin --mode compressed
```

## Requester env vars

`CLI_CLUSTER_RPC`: The cluster API gRPC endpoint.
`CLI_S3_BUCKET`: The S3 bucket the cluster artifact store is using.
`CLI_S3_REGION`: The S3 region the cluster artifact store is using.
`CLI_REDIS_NODES`: The Redis nodes the cluster artifact store is using.

Use either the S3 env vars or the Redis env vars, but not both. 

## Usage

The basic usage of this crate is to use the `request_proof_from_env` function, which reads environment variables, requests a proof, and waits for its completion.