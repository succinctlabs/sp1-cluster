#!/bin/sh

export COORDINATOR_CLUSTER_RPC=http://127.0.0.1:50051
export COORDINATOR_METRICS_ADDR=127.0.0.1:9090
export COORDINATOR_ADDR=127.0.0.1:50053
export RUST_LOG=info
export OTLP_ENDPOINT=http://127.0.0.1:4318

./target/release/sp1-cluster-coordinator
