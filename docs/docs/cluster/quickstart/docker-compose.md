---
title: "Docker Compose"
sidebar_position: 0
toc_min_heading_level: 2
toc_max_heading_level: 3
---

# Docker Compose

*Get started with a single-machine prover that can support up to 8 GPUs. Easy to setup.*

This page explains how to setup the SP1 Cluster using Docker Compose and generate some basic proofs using command line tools. There are several ways to deploy SP1 Cluster, with Docker Compose being the simplest method for single-machine deployments supporting up to 8 GPUs. For larger deployments, please refer to the [Kubernetes installation guide](./kubernetes.md).

## Prerequisites

SP1 Cluster runs on Linux and has the following software requirements:

- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/linux/)
- [CUDA 12](https://developer.nvidia.com/cuda-12-0-0-download-archive?target_os=Linux&target_arch=x86_64&Distribution=Ubuntu&target_version=22.04&target_type=deb_local)
- [NVIDIA Container Toolkit](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/install-guide.html)

## Hardware Requirements

The hardware requirements for running SP1 Cluster depend on the node configuration and can change over time as the prover changes or new features are implemented.

| Component | Minimum Requirements | Recommended Requirements | Notes |
|-----------|---------------------|-------------------------|--------|
| RAM | 64 GB | 64 GB DDR5 | DDR5 RAM is recommended for better proving performance |
| Storage | 30 GB | >30 GB | - |
| CPU | High single clock speed | High single clock speed + High core count | - High single clock speed is important for optimal VM emulation<br/>- High core count helps reduce Groth16/Plonk proving latency |
| GPU | 1-8x NVIDIA GPU with:<br/>- ≥24 GB VRAM<br/>- CUDA Compute Capability ≥8.6 | Multiple supported GPUs:<br/>- GeForce RTX 5090/4090 (Best performance)<br/>- NVIDIA L4<br/>- NVIDIA A10G<br/>- NVIDIA A5000/A6000 | - Multiple GPUs are supported on a single machine<br/>- Each GPU can run a separate instance of the GPU node binary<br/>- If you have no GPU, consider using the single-node prover reference implementation |

## Setup

In this section, we'll walk through the steps needed to setup a basic proving service.

### 1. Clone the repo

```bash
git clone https://github.com/succinctlabs/cluster.git
cd cluster/infra
```

### 2. Setup image registry

Create a [personal access token](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry#authenticating-with-a-personal-access-token-classic) with the `read:packages` scope to access the private images.

```bash
docker login ghcr.io -u $GITHUB_USERNAME -p $GITHUB_TOKEN
```

### 3. Run the cluster

You can specify `gpu{0..n}` to run multiple GPU workers and a CPU worker, or specify `mixed` if you don't have any GPUs.

```bash
# Run only 1 GPU + 1 CPU worker
docker compose up -d gpu0

# Run 8 GPUs + 1 CPU worker
docker compose up -d gpu{0..7}

# Run only 1 CPU worker (no GPUs)
docker compose up -d mixed

# Run from source
docker compose -f docker-compose.local.yml up --build -d gpu0
```

View the logs with:
```
docker logs -f infra-gpu0-1
```

### 4. Submit a test 5m cycle Fibonacci proof

```bash
docker run --rm -it --network=infra_default ghcr.io/succinctlabs/cluster:base-latest \
  /cli bench fibonacci 5 \
  --cluster-rpc http://api:50051 \
  --redis-nodes redis://:redispassword@redis:6379/0
```

When you run this command, it'll request a simple Fibonacci proof to the cluster and wait for the response:

```
INFO crates/common/src/logger.rs:111: logging initialized    
INFO bin/cli/src/commands/bench.rs:68: Running Fibonacci Compressed for 5 million cycles...
INFO crates/common/src/client.rs:22: connecting to http://127.0.0.1:50051
INFO bin/cli/src/commands/bench.rs:113: using redis artifact store
INFO crates/artifact/src/redis.rs:38: initializing redis pool
INFO serialize: crates/artifact/src/lib.rs:126: close time.busy=62.9µs time.idle=1.55ms
INFO upload: crates/artifact/src/redis.rs:196: close time.busy=2.20ms time.idle=2.15ms artifact_type=Program id="artifact_01jxzhe54bf28v7qk3qr9yedqd"
INFO upload: crates/artifact/src/redis.rs:196: close time.busy=734µs time.idle=442µs artifact_type=Stdin id="artifact_01jxzhe54hfg1bhewm129hw58j"
INFO bin/cli/src/commands/bench.rs:146: proof_id: cli_1750183908498
INFO bin/cli/src/commands/bench.rs:185: Proof request completed after 6.531405385s
INFO bin/cli/src/commands/bench.rs:187: Aggregate MHz: 0.77
```

Congratulations! You've now succesfully generated a SP1 proof end-to-end across your cluster of GPUs.

## Prover Network Integration

This section walks through the steps necessary to integrate with the [Succinct Prover Network](https://docs.succinct.xyz/docs/network/introduction) enabling you to fulfill proofs on behalf of requesters and earn fees.

### 1. Create a prover

To create a prover, you need to call `createProver(uint256 _stakerFeeBips)` on the [SuccinctStaking](https://github.com/succinctlabs/network/blob/main/contracts/src/SuccinctStaking.sol) contract. We suggest doing this through our frontend [here](https://staking.sepolia.succinct.xyz/prover). 

By default, the wallet you use to create the prover will be the owner of the prover and also the delegated signer used to sign transactions on behalf of the prover. 

### 2. Stake $PROVE to your prover

The next step is to stake to your prover. You can do this by calling the `stake(address _prover, uint256 _amount)` function on the [SuccinctStaking](https://github.com/succinctlabs/network/blob/main/contracts/src/SuccinctStaking.sol) contract.

We also suggest doing this through our frontend [here](https://staking.sepolia.succinct.xyz/prover). 

### 3. Run the fulfiller

```
export RPC_GRPC_ADDR=https://rpc.sepolia.succinct.xyz
export SP1_PRIVATE_KEY=<key>

docker compose up -d fulfiller
```

### 4. Run the bidder

```
export RPC_GRPC_ADDR=https://rpc.sepolia.succinct.xyz
export SP1_PRIVATE_KEY=<key>

docker compose up -d bidder
```