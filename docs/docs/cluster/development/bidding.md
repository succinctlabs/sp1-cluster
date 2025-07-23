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
The parameter `BIDDER_MAX_CONCURRENT_PROOFS` limits the maximum number of proof requests your cluster can work on simultaneously. Once the cluster is working on `BIDDER_MAX_CONCURRENT_PROOFS` proofs, the bidder will not bid on additional proofs until the number goes down.

The bidder assumes that each proof the cluster is working on will have `BIDDER_THROUGHPUT_MGAS / BIDDER_MAX_CONCURRENT_PROOFS` throughput, and will use this value to calculate whether there is enough time to fulfill new proofs. If there isn't enough time, it won't bid on these potential proofs.

## Bid
The parameter `BIDDER_BID_AMOUNT` controls your bid price in PROVE tokens per MPGUs. The Succinct Prover Network uses a reverse auction mechanism to assign proof requests; lower bids are more competitive. This parameter represents your cost per MPGUs of computational work. Lower values increase your chances of winning bids but reduce profit margins. Higher values decrease competitiveness but increase potential profits per proof. Market dynamics will determine the optimal bid level.
