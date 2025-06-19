---
title: "Distributed Tracing"
sidebar_position: 1
toc_min_heading_level: 2
toc_max_heading_level: 3
---

SP1 Cluster utilizes [OpenTelemetry distributed tracing](https://opentelemetry.io/docs/concepts/signals/traces/) to instrument the entire proving system. Tracing can be helpful to identify latency/throughput bottlenecks across the distributed cluster and find issues (e.g. artifact data transfer, trace generation, coordinator API latency) in the lifetime of a particular proof.

## Setup

To enable tracing, you will need to set the following environment variables on CPU and GPU workers:
* `TRACING_ENABLED=true`
* `OTLP_ENDPOINT` to the endpoint of your tracing collector/exporter (e.g. [Grafana Alloy](https://grafana.com/docs/grafana-cloud/send-data/traces/set-up/traces-with-alloy/#configure-grafana-alloy-to-send-traces-to-grafana-cloud)).
* `RUST_LOG=debug` for maximum trace visibility.

## Navigating Traces

Once you can successfully gather traces and view them in your tracing backend, you should see a root trace with name `Prove <proof ID>` for each proof request. In this root trace, you should see a span for every single task executed during the duration of the proof. Each task span depicts how long that task took to complete once it began running on a worker node.

:::info
We recommend using "Collapse all" and then "Expand +1" to see the spans better.
:::

## Inspecting Task Details

Each task span should be linked to another root span for the task, so you should be able to click a link icon on the span and then "View linked span". The task root span is also linked in the reverse direction.

Each task span should also contain span attribute `task_id` and resource attribute `node.id` to aid in debugging.

## Instrumentation Code

To add more tracing detail in the worker code, you can simply add `tracing::info!` spans to the code. To hide detail, you can change spans to be `trace!` level.

Note that span context does not automatically propagate into async (Tokio) tasks. You can create a span outside of the task and move it into the task to ensure child spans in the task are under the parent span.

Note that adding too many spans can cause performance issues and make traces hard to read.
