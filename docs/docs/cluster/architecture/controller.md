---
title: "Controller Task"
sidebar_position: 1
---

The controller task is created at the beginning of a proof request and orchestrates the entire proving process of a single proof request. Several Tokio tasks are spawned in parallel to handle data movement and task creation in order to minimize the end to end proving latency and maximize cluster throughput.

## Setup Vkey

The first thing that happens in the controller is a Setup Vkey task being created which will run on a GPU worker. The task generates the vkey and uploads it to the artifact store. Then, the vkey is downloaded in the controller where some small computation happens to create the `CommonProveShardInput` which is uploaded and provided to every Prove Shard task.

## Execute Thread

In another thread, the controller immediately begins executing the program and emitting **checkpoints** at every ~2 million VM cycles. These checkpoints are sent to another thread to be uploaded to the artifact store and made into **Prove Shard tasks**.

At the end of the execution, all unique touched memory addresses must be proven in memory shards, so these are split into memory shards and sent out to be proven in their own Prove Shard tasks.

### Checkpoints

Each checkpoint contains just enough data to re-execute the shard, including the program counter, touched memory, and any other relevant state such as how many proofs have been verified in the VM.

## Spawn Prove Tasks Thread

Given inputs for a Prove Shard task, this thread uploads them and requests the coordinator to create a Prove Shard task.

The input for a specific shard (`ShardEventData` in the code) consists of public values data and one of the following:

* Execution Shard: Checkpoint state which contains just enough data to re-execute the shard and generate a trace.
* Precompile Shard: Precompile events data.
* Memory Shard: Memory initialize/finalize data.

Additionally, a thread is spawned to process each shard serially in order to setup the "public values" for that shard which are verified sequentially in recursion in order to connect each shard to the next and form the full proof of the program execution. The public values for a shard is essentially a commitment to the state of what has already been proven before the shard and what is being proven in the shard.

## Precompile Data Processing (Download and Pack)

In Prove Shard tasks for execution shards, any precompile events are taken out of the shard trace and uploaded to the artifact store. A record is also uploaded of the counts of each precompile.

As this happens, the controller downloads these records and packs them into precompile shards based on the precompile counts. When there's enough precompile events of a certain type or there's no more pending execution shards to wait on, a new precompile shard is created and a Prove Shard task for it is sent out.

## Deferred Leaves

If the program is verifying any SP1 proofs using proof aggregation, these can be processed immediately and put into a Recursion Deferred task for each proof. These tasks then are sent to the recursion thread where they're awaited and compressed into a final recursion proof.

## Recursion Thread

The recursion thread waits for any Prove Shard, Recursion Compress, or Recursion Deferred tasks to complete and creates new tasks to recursively compress the proofs in batches until there is only one final proof. 

## Groth16/Plonk

Finally, once a final compressed proof is achieved, it is wrapped into a Groth16/Plonk proof (in a new task) if applicable. 