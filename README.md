<div align="center">

![SP1 Cluster](./.github/assets/header.png)

[![Docker CI][docker-badge]][docker-url] [![Lint CI][lint-badge]][lint-url] [![Telegram Chat][tg-badge]][tg-url]

The official multi-GPU proving service implementation for SP1, designed for the cloud and bare metal deployment.

[docker-badge]: https://img.shields.io/github/actions/workflow/status/succinctlabs/sp1-cluster/docker.yml?branch=main
[docker-url]: https://github.com/succinctlabs/sp1-cluster/actions/workflows/docker.yml
[lint-badge]: https://img.shields.io/github/actions/workflow/status/succinctlabs/sp1-cluster/lint.yml?branch=main&label=lint
[lint-url]: https://github.com/succinctlabs/sp1-cluster/actions/workflows/lint.yml
[tg-badge]: https://img.shields.io/endpoint?color=neon&logo=telegram&label=chat&url=https%3A%2F%2Ftg.sumanjay.workers.dev%2F%2B9jkmKpjkJ1U5MTc5
[tg-url]: https://t.me/+9jkmKpjkJ1U5MTc5
[docs-badge]: https://img.shields.io/github/deployments/succinctlabs/sp1-cluster/Production?label=docs
[docs-url]: https://cluster-docs.succinct.xyz
</div>

## Overview

This project implements a high performance SP1 proving cluster that can scale to thousands of GPUs. The system consists of several key components:

- **API**: REST API for interfacing with the cluster backed by Postgres
- **Coordinator**: Central service that manages task and worker coordination
- **CPU/GPU Node**: Distributed worker nodes that execute proof generation tasks
- **Fulfiller**: Service that syncs the cluster API with assigned proofs from the SP1 Prover Network
- **Bidder**: Service that bids on proofs from the SP1 Prover Network
- **Artifact Store**: Redis or S3 store used for storing ephemeral proving data

For detailed information on the architecture and deployment instructions, see the [docs](https://docs.succinct.xyz/docs/provers/introduction).

## Development

To get started, you will need to have the following installed:

* [Rust](https://www.rust-lang.org/tools/install)
* [SP1](https://docs.succinct.xyz/docs/sp1/getting-started/install)

```bash
git clone https://github.com/succinctlabs/sp1-cluster.git
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
