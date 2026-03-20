#!/bin/sh
export CLI_CLUSTER_RPC=http://127.0.0.1:50051
export CLI_REDIS_NODES=redis://:redispassword@127.0.0.1:6379/0
export RUST_LOG=info

mcycles=${1:-5}
./target/release/sp1-cluster-cli bench fibonacci "$mcycles" --mode core
