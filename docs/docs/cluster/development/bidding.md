---
title: "Bidding"
sidebar_position: 1
toc_min_heading_level: 2
toc_max_heading_level: 3
---
SP1 Cluster uses an automated bidder that constantly listens for proof requests from the network and places bids. When scaling up your cluster, you have the option to tune relevant parameters. You can set these parameters in your [`docker-compose.yml`](https://github.com/succinctlabs/cluster-private/blob/main/infra/docker-compose.yml) file. 

## Throughput
The parameter `BIDDER_THROUGHPUT_MGAS` sets your cluster's maximum throughput capacity in millions of prover gas units (MPGUs) per second. This value represents the maximum computational capacity your cluster can handle per second. Higher values mean your cluster can process work.

This value should be set based on your hardware capabilities and your desired resource utilization. Setting it too high may lead to resource contention and slower individual proof times
whereas setting it too low may underutilize your cluster's capacity.

## Concurrent Proofs
The parameter `BIDDER_MAX_CONCURRENT_PROOFS` controls the maximum number of proof requests your cluster can bid on simultaneously. It controls parallelism and resource allocation across multiple proof requests. Too many concurrent proofs can lead to memory exhaustion or context switching overhead, whereas too few may leave computational resources idle.

## Bid
The parameter `BIDDER_BID_AMOUNT` controls your bid price in PROVE tokens per MPGUs. The Succinct Prover Network uses a reverse auction mechanism to assign proof requests; lower bids are more competitive. This parameter represents your cost per MPGUs of computational work. Lower values increase your chances of winning bids but reduce profit margins. Higher values decrease competitiveness but increase potential profits per proof. Market dynamics will determine the optimal bid level.
