---
title: "Local Development"
sidebar_position: 0
toc_min_heading_level: 2
toc_max_heading_level: 3
---

# Local Development

*Develop and test the SP1 Cluster locally for faster iteration and debugging.*

This page walks through how to set up and run the SP1 Cluster locally. While Docker Compose is recommended for deployment, running locally (outside Docker) offers faster compile times and easier iteration, especially when working on GPU code.

## Prerequisites

To get started with local development, ensure you have the following installed:

- [Rust](https://www.rust-lang.org/tools/install)
- [CUDA](https://developer.nvidia.com/cuda-downloads) ≥ 12.5.1

If using Docker Compose:
- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)
- [NVIDIA Container Toolkit](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/install-guide.html)

If not using Docker Compose:
- [Go](https://golang.org/dl/) ≥ 1.22

:::info
Local development is fastest when done on a GPU machine. GPU proving is significantly faster and allows for efficient debugging of GPU-specific code paths.
:::

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/succinctlabs/cluster.git
cd cluster
```

### 2. Build the workspace

Use Cargo to build the project in debug mode for faster rebuilds during development:

```bash
cargo build
```

> Note: Even in debug mode, dependencies are built with `opt-level=3`, which preserves decent proving performance.

## Running Locally

You can run the cluster services directly from source without Docker. This setup is ideal for development environments where compile time and iteration speed are critical.

To get started:

- Run the desired binaries from `target/debug/`
- Ensure environment variables match `.env.example` or customize as needed
- Services include the coordinator, GPU nodes, CPU nodes, etc.

## Testing Proofs

You can use the provided CLI tool to send test proof requests to your running cluster:

```bash
./bin/cli
```

See the [Testing Guide](./deployment/testing) for detailed usage examples.

## Troubleshooting

When debugging, the fastest approach is to search the logs for failure keywords. Check the logs of worker nodes and the coordinator using terms like:

- `panicked`
- `WARN`
- `ERROR`
- failed proof/task ID

The coordinator also logs periodic summaries of its state, which can offer useful context.

> **Note:** You can also manually run specific task instances (TODO: link once available) to isolate and debug performance or correctness issues.
