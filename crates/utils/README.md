# Requester Utils

This crate provides a set of utilities for requesting proofs from the SP1 Cluster, bypassing the network / sdk layer. Most users should request proofs using the `sp1-sdk` crate.

## Quick start

See the project [benchmarking workflow](../../README.md#benchmarking) for the local cluster and CLI setup.

## Requester env vars

`CLI_CLUSTER_RPC`: The cluster API gRPC endpoint.
`CLI_S3_BUCKET`: The S3 bucket the cluster artifact store is using.
`CLI_S3_REGION`: The S3 region the cluster artifact store is using.
`CLI_REDIS_NODES`: The Redis nodes the cluster artifact store is using.

Use either the S3 env vars or the Redis env vars, but not both. 

## Usage

Call `request_proof_from_env` to read the environment, request a proof, and wait for it.
