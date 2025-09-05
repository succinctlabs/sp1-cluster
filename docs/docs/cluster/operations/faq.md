---
title: "FAQ"
sidebar_position: 0
---

# Frequently Asked Questions

### 1. I have >50 GPUs in my cluster and I am adding even more, how come a single Fibonacci proof is not getting any faster? {#faq-1}

SP1 proof generation is bottlenecked by the executor that executes the VM program and creates checkpoints to prove from. Therefore, there is an upper limit on how many GPUs can be utilized by a single proof. In other words, the executor is only able to create work for GPU nodes at a certain rate. This upper limit on GPUs is around 50 from our testing. This bottleneck can be alleviated by running the CPU node on a CPU machine with better single-core clock speed. Future SP1 upgrades will also improve upon this bottleneck by optimizing the executor code.

Adding additional GPUs should still improve the total throughput of your cluster, but it may not be noticeable without multiple concurrent proofs. Therefore, if you are trying to test the throughput of your cluster with more than 50 GPUs, you should run multiple proofs at once in order to measure the overall throughput.

### 2. Why is my GPU VRAM usage so low?

SP1 proof generation on GPUs is mainly compute bound rather than memory bound, so low VRAM usage does not always mean the GPU is underutilized. As of SP1 v5, at least 24 GB of VRAM is recommended to prevent GPU OOM errors. Significantly more VRAM does not necessarily improve performance.

### 3. How can I tell if I am getting good performance out of my cluster? The CPU/GPU utilization seems low. {#faq-3}

#### Measuring performance

Firstly, it's important to get an accurate measurement of the resulting performance from your cluster. You can do this by measuring the total gas proven by your cluster over a large enough time period on the prover network. You can also do this by requesting several Fibonacci proofs at once using the cluster CLI and computing the total throughput. You may want to try various combinations of proof size and count depending on your cluster size to get an accurate throughput measurement. Note the above answer means a single proof can only use up to 30-50 GPUs depending on your CPU node performance.

In our experience, we tend to observe ~2 million PGU/s of proving throughput per GPU node for higher performance GPUs (ex. RTX 4090/5090) and closer to 800k PGU/s for less powerful GPUs (ex. L4). Other factors such as GPU bandwidth / PCIe lane count, CPU cores/clock speed, CPU memory speed and network bandwidth can also affect overall performance.

#### Potential bottlenecks

There are a few likely bottlenecks that can affect performance:

- Uploading/downloading circuit artifacts takes too much time (>100ms)
    - You can verify whether this is the case by inspecting the CPU/GPU node logs for download/upload span times (ex. `download: close time.busy=10ms time.idle=1200ms`) or by setting up [distributed tracing](../development/tracing) and inspecting the traces for unusually large spans.
    - Redis nodes could be bound by compute, memory, or networking. You can scale the artifact store load horizontally by simply setting up additional Redis instances and adding them (comma-separated) to the `REDIS_NODES` env var. (ex. `REDIS_NODES=redis://:redispassword@redis1:6379/0,redis://:redispassword@redis2:6379/0`). Also ensure that persistence is disabled for Redis (`persistence.enabled: false` or `REDIS_AOF_ENABLED=no`) as it is not needed. You can also try tuning other Redis parameters such as `io-threads` and `hz`.
    - We do not recommend using S3 as the artifact store unless you are running in AWS due to the increased data transfer cost and latency.
- GPU nodes are not tuned properly, meaning the node is not able to provide enough work to the GPU itself to keep it busy.
    - You can increase the max weight override to run more tasks in parallel. (ex. `WORKER_MAX_WEIGHT_OVERRIDE=32`). Weight is measured in such that 1 weight is equivalent to 1 GB of RAM (not VRAM) available to the node. Setting the parameter too high can cause the node to run too many tasks concurrently, run out of memory, and crash.
    - You can measure this by setting up [Prometheus metrics and the provided Grafana dashboard](../development/prometheus-metrics) and inspecting the "Active GPUs" chart while a proof runs for several minutes. This chart has "Estimated Active GPUs" which reflects total GPU utilization over time. A low value relative to # of total GPUs means some GPUs were idle for a majority of the interval. You can also measure GPU utilization using `nvidia-smi` or similar tools.
- Network operations between the coordinator and nodes has too much latency.
    - These should all be running on the same internal network, ideally with at least 10Gbps bandwidth.
- There are not enough CPU nodes to run proofs in parallel.
    - If running many proofs at once (ex. `BIDDER_MAX_CONCURRENT_PROOFS` > 10), you should have up to 1 CPU node per proof, ideally with at least 32 worker weight per CPU node. You can get away with fewer CPU nodes (as little as 4 worker weight per concurrent proof), but you may notice decreased cluster utilization when there are Plonk or Groth16 tasks running.

#### Miscellaneous issues

* We do not recommend using `WORKER_TYPE=ALL` on CPU machines if using GPUs as well, since doing so will cause the node to run all task types, and the CPU prover is much less performant than GPU.

### 4. Why are my Groth16/Plonk proofs timing out / failing?

#### Artifacts setup

Check your CPU node logs for messages like the following:

```
2025-08-12T00:44:40.415405Z  INFO task: /app/crates/worker/src/tasks/finalize.rs:55: Waiting for circuit artifacts to be ready     proof_id="28cf80a36990e85a064da2d9ccf82a7dfc7aeecd57a95aeb3353143f14b5102c" task_id="task_01k2dtppn8fe9v7az1mek4rnvg" otel.name="PLONK_WRAP"
2025-08-12T00:44:45.415810Z  INFO task: /app/crates/worker/src/tasks/finalize.rs:55: Waiting for circuit artifacts to be ready     proof_id="28cf80a36990e85a064da2d9ccf82a7dfc7aeecd57a95aeb3353143f14b5102c" task_id="task_01k2dtppn8fe9v7az1mek4rnvg" otel.name="PLONK_WRAP"
2025-08-12T00:44:50.417412Z  INFO task: /app/crates/worker/src/tasks/finalize.rs:55: Waiting for circuit artifacts to be ready     proof_id="28cf80a36990e85a064da2d9ccf82a7dfc7aeecd57a95aeb3353143f14b5102c" task_id="task_01k2dtppn8fe9v7az1mek4rnvg" otel.name="PLONK_WRAP"
```

This suggests the CPU node was unable to download artifacts properly, likely due to directory permissions if you are mounting the artifacts directory as a volume. By default, the artifacts are downloaded to `/root/.sp1/circuits` in the CPU node image. Look for logs like this when the CPU node starts:

```
2025-08-12T00:54:29.242040Z  INFO bin/node/src/main.rs:207: worker type: Cpu
2025-08-12T00:54:29.242178Z  INFO bin/node/src/main.rs:215: downloading circuit artifacts
thread 'tokio-runtime-worker' panicked at /usr/local/cargo/git/checkouts/sp1-9091391fc1cd5ab7/6544380/crates/sdk/src/install.rs:85:41:
failed to create build directory: Os { code: 13, kind: PermissionDenied, message: "Permission denied" }
note: run with RUST_BACKTRACE=1 environment variable to display a backtrace
2025-08-12T00:54:29.244128Z  INFO bin/node/src/main.rs:123: Not creating circuits dir: Os { code: 13, kind: PermissionDenied, message: "Permission denied" }
2025-08-12T00:54:29.244158Z  INFO bin/node/src/main.rs:133: Not creating temp dir: Os { code: 13, kind: PermissionDenied, message: "Permission denied" }
```

When the artifacts are correctly setup, you'll see a message like `circuit artifacts ready after 3 min`.

#### Wrap is too slow

If you do not see circuit artifact issues but are still seeing Groth16/Plonk proofs timing out, it's likely that your CPU node does not have enough RAM or CPU resources. Having more cores and at least 32 GB RAM should result in better wrapping performance. If this is not possible, you could customize the bidder logic to avoid bidding on Plonk/Groth16 proof requests or require a longer deadline.

### 5. Why is my Redis node using so much disk space?

Make sure persistence is disabled for Redis (`persistence.enabled: false` for Helmchart or `REDIS_AOF_ENABLED=no` for docker image) as it is not needed. You can also try tuning other Redis parameters such as `io-threads` and `hz`.

### 6. What's the optimal configuration for my cluster hardware?

All services should be in the same internal network for minimal latency, with ideally at least 10 Gbps networking for efficient artifact data transfer.

We recommend at least 1 CPU node per proof, ideally with at least 32 worker weight per CPU node. You can get away with fewer CPU nodes (as little as 4 worker weight per concurrent proof), but you may notice decreased cluster utilization when there are Plonk or Groth16 tasks running.

The primary bottleneck for proving throughput is GPUs. High performant GPUs (ex. RTX 4090/5090) are recommended and are generally the most cost effective option. GPUs with high VRAM (much more than 24 GB) may not be as cost effective because the extra VRAM is not necessary and the GPU prover is generally compute bound. Adding more GPUs will allow your cluster to prove more gas per second. Ensure that your GPU nodes have enough RAM (base RAM not VRAM) and CPU cores for CPU work that happens in parallel (ex. program execution, event/trace generation, artifact downloading, recursion program compilation) to supply work to the GPU. DDR5 RAM is recommended.

As mentioned in [FAQ #3](#faq-3), Redis node performance could be a bottleneck. Ensure they have enough RAM and CPU cores, and add more if necessary.

### 7. How should I configure the bidder?

The bidder has three main configuration env vars: `BIDDER_MAX_CONCURRENT_PROOFS`, `BIDDER_THROUGHPUT_MGAS`, and `BIDDER_BID_AMOUNT`.

- `BIDDER_THROUGHPUT_MGAS` should be set to the estimated total throughput of the cluster in million PGUs per second. This should be configured to the observed total throughput from running a large enough Fibonacci test on the cluster. See [FAQ #1](#faq-1).
- `BIDDER_MAX_CONCURRENT_PROOFS` controls the maximum number of assigned proofs the bidder will try to maintain.
    - The bidder determines how much throughput each proof gets based on `BIDDER_THROUGHPUT_MGAS / BIDDER_MAX_CONCURRENT_PROOFS`, and uses this per-proof throughput to estimate whether or not it has enough time to fulfill open proof requests. (The cluster coordinator assigns tasks evenly by proof, so each proof gets a roughly equal amount of throughput at any given time.) Setting `BIDDER_MAX_CONCURRENT_PROOFS` to a lower value will allow the bidder to bid on proofs that have more aggressive deadlines, but if the value is too low, it may not fully utilize the cluster effectively.
    - Depending on how aggressive proof deadlines of proofs being requested are, this value could for example be set to a number in the range of `BIDDER_THROUGHPUT_MGAS / 10` to `BIDDER_THROUGHPUT_MGAS / 2`, allowing the bidder to bid on proofs with deadlines of up to 10 MGas/sec to 2 MGas/sec.
- `BIDDER_BID_AMOUNT` should be set to the amount of PROVE (in wei) per PGU the bidder will bid at. For example, if this is set to `200_000_000`, that would be equivalent to 0.20 PROVE per billion PGUs.

