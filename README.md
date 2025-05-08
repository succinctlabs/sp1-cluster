![SP1 Cluster](./.github/assets/header.png)

A distributed task coordination system for generating SP1 proofs.

## Overview

This project implements a distributed task coordination system designed to manage proof generation workloads across multiple workers. The system consists of several key components:

- **API**: REST API for interacting with the cluster backed by Postgres
- **Coordinator**: Central service that manages task and worker coordination
- **CPU/GPU Node**: Distributed worker nodes that execute proof generation tasks
- **Fulfiller**: Service that syncs the cluster API with assigned proofs from the SP1 Prover Network

## Getting Started

To get started, you will need to have the following installed:

* [Rust](https://www.rust-lang.org/tools/install)
* [SP1](https://docs.succinct.xyz/docs/sp1/getting-started/install)

```bash
git clone https://github.com/succinctlabs/cluster.git
cd cluster

# Build
cargo build --release
```

## Deploying

To deploy to a Kubernetes cluster, you can use the provided Helm chart in the `infra/charts/sp1-cluster` directory.

```bash
cd infra/charts/sp1-cluster
cp values-example.yaml values-testnet.yaml
```

Edit the values-testnet.yaml file to configure your cluster.

```bash
helm install sp1-cluster . -f values-testnet.yaml
```
