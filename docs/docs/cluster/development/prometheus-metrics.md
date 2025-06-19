---
title: "Prometheus Metrics"
sidebar_position: 2
toc_min_heading_level: 2
toc_max_heading_level: 3
---

# Prometheus Metrics

The cluster binaries are instrumented to export Prometheus metrics at /metrics on port 9090. We also provide a default Grafana dashboard for cluster metrics at infra/grafana-dashboard.json which you can import into your Grafana instance to visualize some of the metrics collected.