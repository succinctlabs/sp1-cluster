---
title: "Kubernetes"
sidebar_position: 1
---

# Kubernetes

*Get started with a multi-machine prover that can run with as many GPUs as you can provision.*

This page explains how to setup the SP1 Cluster using Kubernetes and generate some basic proofs using command line tools. There are several ways to deploy SP1 Cluster, with Kubernetes being the best option for practical, production-ready deployments that need the best performance. For a simpler deployment workflow, please refer to the [Docker Compose installation guide](./docker-compose.md).

## Prerequisites

SP1 Cluster runs on Linux and has the following software requirements for each worker on the cluster:

- [Kubernetes](https://kubernetes.io/) or [RKE2](https://docs.rke2.io/install/quickstart) (Recommended)
- [CUDA 12](https://developer.nvidia.com/cuda-12-0-0-download-archive?target_os=Linux&target_arch=x86_64&Distribution=Ubuntu&target_version=22.04&target_type=deb_local)
- [NVIDIA Container Toolkit](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/install-guide.html)

For the machine you're using to connect to the kubernetes cluster, you'll need:

- [kubectl](https://kubernetes.io/docs/tasks/tools/#kubectl)
- [helm](https://helm.sh/docs/intro/install/)

## Hardware Requirements


The hardware requirements for running SP1 Cluster depend on the node configuration and can change over time as the prover changes or new features are implemented.

| Component | Minimum Requirements | Recommended Requirements | Notes |
|-----------|---------------------|-------------------------|--------|
| 1x+ CPU Machines | • ≥40 GB RAM<br/>• >30 GB disk space<br/>• High single clock speed | • ≥64 GB DDR5 RAM<br/>• >30 GB disk space<br/>• High single clock speed<br/>• High core count | • High single clock speed is important for optimal VM emulation<br/>• High core count helps reduce Groth16/Plonk proving latency<br/>• DDR5 RAM is recommended for better proving performance |
| 1x+ GPU Machines | • ≥16 GB RAM<br/>• 1x NVIDIA GPU with:<br/>&nbsp;&nbsp;&nbsp;&nbsp;• ≥24 GB VRAM<br/>&nbsp;&nbsp;&nbsp;&nbsp;• CUDA Compute Capability ≥8.6 | • ≥64 GB DDR5 RAM<br/>• Multiple supported GPUs:<br/>&nbsp;&nbsp;&nbsp;&nbsp;• GeForce RTX 5090/4090 (Best performance)<br/>&nbsp;&nbsp;&nbsp;&nbsp;• NVIDIA L4<br/>&nbsp;&nbsp;&nbsp;&nbsp;• NVIDIA A10G<br/>&nbsp;&nbsp;&nbsp;&nbsp;• NVIDIA A5000/A6000 | • Multiple GPUs are supported on a single machine<br/>• Each GPU can run a separate instance of the GPU node binary<br/>• If you have no GPU, consider using the single-node prover reference implementation<br/>• DDR5 RAM is recommended for proving performance |

## Helm Chart

We use a [Helm Chart](https://helm.sh/) to manage the Kubernetes applications. The chart is defined in [`infra/charts/sp1-cluster/values-example.yml`](https://github.com/succinctlabs/cluster/blob/main/infra/charts/sp1-cluster/values-example.yaml).

There are several services that the Helm chart orcheastrates:

- CPU Workers
- GPU Workers
- API
- Postgres
- Redis
- Coordinator
- Fulfiller (Optional)
- Bidder (Optional)

The chart can be used to configure the hardware requirements and the number of replicas you want per service.

## Setup

In this section, we'll walk through the steps needed to setup a basic proving service using [Helm](https://helm.sh/).

### 1. Clone the repo

```bash
git clone https://github.com/succinctlabs/cluster.git
cd cluster
```

### 2. Ensure kubectl is connected to your cluster

```
kubectl cluster-info
kubectl get nodes
```

### 3. Create a k8s namespace

```bash
kubectl create namespace sp1-cluster-test
```

### 4. Create k8s secrets

Create a secret called `cluster-secrets` that configures the postgres database, the redis database, and the private key used for the prover.
```bash
kubectl create secret generic cluster-secrets \
  --from-literal=DATABASE_URL=postgresql://postgres:postgrespassword@postgresql:5432/postgres \
  --from-literal=REDIS_NODES=redis://:redispassword@redis-master:6379/0 \
  --from-literal=PRIVATE_KEY=<PROVER_SIGNER_KEY> \
  -n sp1-cluster-test
```

Create a secret called `ghcr-secret` that can allow you to pull private images from [ghcr.io](https://ghcr.io).
```bash
kubectl create secret docker-registry ghcr-secret \
  --docker-server=ghcr.io \
  --docker-username=$GITHUB_USERNAME \
  --docker-password=$GITHUB_TOKEN \
  -n sp1-cluster-test
```

### 5. Configure the Helm chart

The template helm chart for the cluster exists at `infra/charts/sp1-cluster/values-example.yml`. Copy it and configure it to your liking.

```bash
cp infra/charts/sp1-cluster/values-example.yaml infra/charts/sp1-cluster/values-test.yaml
```

You can configure the Helm chart to your liking. In particular, you may want to configure the `resources` and node placement values based on your cluster hardware. We recommend following the following configuration constraints:

* Redis: placed on a non-worker machine with >= 20 GB RAM.
* API, Postgres, Fulfiller: placed on non-worker machine.
* CPU Workers: allocated >= 32 GB RAM, 10 GB disk, and powerful CPU with as many cores as possible.
* GPU Workers: allocated 1 GPU, >= 24 GB RAM, and as many cores as possible.

### 6. Setup Helm chart dependencies

```bash
helm dependency update infra/charts/redis-store
helm dependency update infra/charts/sp1-cluster
```

### 7. Create (or redeploy) the cluster

```bash
helm upgrade --install my-sp1-cluster infra/charts/sp1-cluster \
  -f infra/charts/sp1-cluster/values-test.yaml \
  -n sp1-cluster-test \
  --debug
```

### 8. Send a test 5M cycle Fibonacci proof

We'll first create a temporary pod within the k8s namespace so we can access the cluster network.

```bash
kubectl run cli --image=ghcr.io/succinctlabs/cluster:base-latest \
  --rm -it -n sp1-cluster-test /bin/bash
```

Once you're in the pod, you can use the SP1 Cluster CLI to send a test 5M cycle Fibonacci proof:

```bash
/cli bench fibonacci 5 \
  --cluster-rpc http://api-grpc:50051 \
  --redis-nodes redis://:redispassword@redis-master:6379/0
```

This should output something like:
```
INFO crates/common/src/logger.rs:110: logging initialized    
INFO bin/cli/src/commands/bench.rs:68: Running Fibonacci Compressed for 5 million cycles...
INFO crates/common/src/client.rs:22: connecting to http://api-grpc:50051
INFO bin/cli/src/commands/bench.rs:113: using redis artifact store
INFO crates/artifact/src/redis.rs:38: initializing redis pool
INFO serialize: crates/artifact/src/lib.rs:126: close time.busy=15.5µs time.idle=267µs
INFO upload: crates/artifact/src/redis.rs:196: close time.busy=1.56ms time.idle=3.03ms artifact_type=Program id="artifact_01jxzm994ke78shjk272egp5vt"
INFO upload: crates/artifact/src/redis.rs:196: close time.busy=355µs time.idle=492µs artifact_type=Stdin id="artifact_01jxzm994rf3hve7yrfgg43t0w"
INFO bin/cli/src/commands/bench.rs:146: proof_id: cli_1750186894489
INFO bin/cli/src/commands/bench.rs:185: Proof request completed after 3.016538313s
INFO bin/cli/src/commands/bench.rs:187: Aggregate MHz: 1.66
```

## Prover Network Integration

This section walks through the steps necessary to integrate with the [Succinct Prover Network](https://docs.succinct.xyz/docs/network/introduction) enabling you to fulfill proofs on behalf of requesters and earn fees.

### 1. Create a prover

To create a prover, you need to call `createProver(uint256 _stakerFeeBips)` on the [SuccinctStaking](https://github.com/succinctlabs/network/blob/main/contracts/src/SuccinctStaking.sol) contract. We suggest doing this through our frontend [here](https://staking.sepolia.succinct.xyz/prover). 

By default, the wallet you use to create the prover will be the owner of the prover and also the delegated signer used to sign transactions on behalf of the prover. 

### 2. Stake $PROVE to your prover

The next step is to stake to your prover. You can do this by calling the `stake(address _prover, uint256 _amount)` function on the [SuccinctStaking](https://github.com/succinctlabs/network/blob/main/contracts/src/SuccinctStaking.sol) contract.

We also suggest doing this through our frontend [here](https://staking.sepolia.succinct.xyz/prover). 

### 3. Run the fulfiller

Inside the Helm chart, update the `fulfiller` service's `enabled` attribute to `true`.

```
fulfiller:
  enabled: true
  ...
```

Then upgrade the deployment:
```
helm upgrade --install my-sp1-cluster infra/charts/sp1-cluster \
  -f infra/charts/sp1-cluster/values-test.yaml \
  -n sp1-cluster-test \
  --debug
```

### 4. Run the bidder

Inside the Helm chart, update the `bidder` service's `enabled` attribute to `true`.
```
bidder:
  enabled: true
  ...
```

Then upgrade the deployment:
```
helm upgrade --install my-sp1-cluster infra/charts/sp1-cluster \
  -f infra/charts/sp1-cluster/values-test.yaml \
  -n sp1-cluster-test \
  --debug
```