#![allow(clippy::too_many_arguments)]
#![allow(clippy::type_complexity)]

use std::collections::BTreeMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::shards::{DeferredEvents, GlobalMemoryEvents, ShardEventData, ShardType};
use super::CommonTaskInput;
use crate::error::TaskError;
use crate::utils::{conditional_future, get_tree_layer_size, with_parent};
use crate::SP1Worker;
use crate::{client::WorkerService, FOLD_LAYER};
use anyhow::{anyhow, Result};
use hashbrown::HashMap;
use opentelemetry::Context;
use p3_field::PrimeField32;
use sp1_cluster_artifact::{Artifact, ArtifactClient};
use sp1_cluster_common::proto::{TaskStatus, TaskType};
use sp1_core_executor::{DeferredProofVerification, SP1ReduceProof};
use sp1_core_executor::{ExecutionRecord, Executor, Program};
use sp1_prover::{CoreSC, InnerSC};
use sp1_sdk::SP1Stdin;
use sp1_stark::air::PublicValues;
use sp1_stark::SP1CoreOpts;
use sp1_stark::{SplitOpts, StarkVerifyingKey};
use tokio::sync::mpsc::{Receiver, Sender, UnboundedReceiver, UnboundedSender};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::{info_span, Instrument};

/// The maximum number of memory records to be be uploading at once (per proof). This is done as
/// each upload requires serializing the entire record which effectively doubles the memory usage.
pub(crate) const MEMORY_RECORD_CAPACITY: usize = 40;

/// The number of workers to download precompile records.
pub(crate) const PRECOMPILE_RECORD_DOWNLOAD_WORKERS: usize = 32;

impl<W: WorkerService, A: ArtifactClient> SP1Worker<W, A> {
    /// Execute the program, sending a checkpoint every shard to the checkpoint_tx channel.
    /// This thread will error if the program is invalid or the user is out of cycle quota.
    pub(crate) async fn execute_thread(
        self: Arc<Self>,
        program: Program,
        stdin: SP1Stdin,
        opts: SP1CoreOpts,
        checkpoint_tx: Sender<(ShardEventData, bool)>,
        final_record_tx: tokio::sync::oneshot::Sender<ExecutionRecord>,
        cycle_limit: Option<u64>,
        deferred_proof_verification: DeferredProofVerification,
    ) -> Result<Vec<u8>, TaskError> {
        let start = Instant::now();
        let cancelled = Arc::new(AtomicBool::new(false));
        let cancelled_clone = cancelled.clone();
        let span = info_span!("execute_blocking_thread");
        let blocking_handle = tokio::task::spawn_blocking({
            let this = self.clone();
            move || {
                let _guard = span.enter();
                let mut executor = Executor::new(program, opts);
                let maximal_shapes = this.prover.core_shape_config.as_ref().map(|config| {
                    config
                        .maximal_core_shapes(opts.shard_size.ilog2() as usize)
                        .into_iter()
                        .collect()
                });
                executor.maximal_shapes = maximal_shapes;
                executor.write_vecs(&stdin.buffer);
                for (reduce_proof, vk) in stdin.proofs.clone() {
                    executor.write_proof(reduce_proof, vk);
                }
                // Stop execution if the cycle limit is reached, + 1 to account for >= executor max_cycles check
                if let Some(cycle_limit) = cycle_limit {
                    if cycle_limit > 0 {
                        executor.max_cycles = Some(cycle_limit + 1);
                    }
                }

                executor.deferred_proof_verification = deferred_proof_verification;

                executor.subproof_verifier = Some(this.prover.as_ref());

                loop {
                    let (checkpoint, pv, done) = executor.execute_state(true)?;

                    // Handle edge case where digest COMMIT happen across the last two shards. We
                    // assume it cannot be more than two shards.
                    if !done
                        && (pv.committed_value_digest.iter().any(|v| v != &0)
                            || pv.deferred_proofs_digest.iter().any(|v| v != &0))
                    {
                        tracing::warn!("digest commit happens across multiple shards");
                        let (last_checkpoint, last_pv, last_done) = executor.execute_state(true)?;
                        let mut pv = pv;
                        pv.committed_value_digest = last_pv.committed_value_digest;
                        pv.deferred_proofs_digest = last_pv.deferred_proofs_digest;
                        if !last_done {
                            tracing::error!("digest commit happens across >2 last shards");
                        }
                        checkpoint_tx
                            .blocking_send((ShardEventData::state(checkpoint, pv), done))
                            .unwrap();
                        checkpoint_tx
                            .blocking_send((
                                ShardEventData::state(last_checkpoint, last_pv),
                                last_done,
                            ))
                            .unwrap();
                        break;
                    }

                    // Send checkpoint to handler thread.
                    checkpoint_tx
                        .blocking_send((ShardEventData::state(checkpoint, pv), done))
                        .unwrap();

                    if done {
                        break;
                    }

                    if cancelled_clone.load(std::sync::atomic::Ordering::Relaxed) {
                        return Err(TaskError::Fatal(anyhow!("Execution cancelled")));
                    }
                }
                drop(checkpoint_tx);

                tracing::info!(
                    "execution time: {:?}, MHz: {:.2}, cycles: {}",
                    start.elapsed(),
                    executor.state.global_clk as f64 / start.elapsed().as_secs_f64() / 1_000_000.0,
                    executor.state.global_clk
                );

                let pv = executor.state.public_values_stream;
                let final_record = executor.records.pop().unwrap();
                let cycles = executor.state.global_clk;
                anyhow::Result::<_, TaskError>::Ok((pv, final_record, cycles))
            }
        });

        // TODO: We could handle cancellation and cancel execute thread if this thread is aborted.

        // Get results from blocking task.
        let (public_values, final_record, _cycles) = blocking_handle.await.unwrap()?;

        final_record_tx.send(*final_record).unwrap();
        Ok(public_values)
    }

    /// Given a channel of consecutive 0-indexed items, send them out in order.
    async fn process_ordered<T>(
        self: Arc<Self>,
        mut receiver: UnboundedReceiver<(usize, T)>,
        sender: UnboundedSender<(usize, T)>,
    ) -> Result<(), TaskError> {
        let mut received = Vec::new();
        let mut next_index = 0;
        let mut done = false;
        while !done {
            if let Some(task) = receiver.recv().await {
                received.push(task);
            } else {
                done = true;
            }
            received.sort_by_key(|(index, _)| *index);
            while !received.is_empty() && received[0].0 == next_index {
                next_index += 1;
                let item = received.remove(0);
                sender.send(item).unwrap();
            }
        }
        if !received.is_empty() {
            tracing::error!("Received elements are not contiguous (this should never happen)");
            return Err(TaskError::Fatal(anyhow!(
                "Received elements are not contiguous (this should never happen)"
            )));
        }
        Ok(())
    }

    /// Upload shards and return artifacts. They may become out of order.
    async fn upload_data(
        self: Arc<Self>,
        mut receiver: Receiver<(ShardEventData, bool)>,
        sender: Sender<(usize, (Artifact, ShardType, bool))>,
        final_tx: UnboundedSender<Result<Artifact, TaskError>>,
    ) -> Result<(), TaskError> {
        let mut sent = 0;
        // Limit the number of memory records that can be uploaded at once. This is done because
        // the bottleneck is in the network part of upload, and with no lock too many records are
        // serialized at once which greatly increases peak memory.
        let memory_record_lock = Arc::new(Semaphore::new(MEMORY_RECORD_CAPACITY));
        let mut set = JoinSet::new();
        while let Some((item, last)) = receiver.recv().await {
            let sender = sender.clone();
            let final_sender = final_tx.clone();
            let self_arc = self.clone();
            let lock = memory_record_lock.clone();
            set.spawn(
                async move {
                    // Upload shard
                    let shard_type = match &item {
                        ShardEventData::Checkpoint(_, _) => ShardType::Execution,
                        ShardEventData::GlobalMemory(_, _, _) => ShardType::Precompile,
                        ShardEventData::PrecompileRemote(_, _, _) => ShardType::Precompile,
                    };
                    let result = async move {
                        let artifact = self_arc.artifact_client.create_artifact()?;
                        let permit = match &item {
                            ShardEventData::GlobalMemory(_, _, _) => {
                                Some(lock.acquire_owned().await.unwrap())
                            }
                            _ => None,
                        };
                        self_arc.artifact_client.upload(&artifact, item).await?;
                        drop(permit);
                        Ok(artifact)
                    }
                    .await;

                    match result {
                        Ok(artifact) => sender
                            .send((sent, (artifact, shard_type, last)))
                            .await
                            .unwrap(),
                        Err(e) => final_sender.send(Err(e)).unwrap(),
                    }
                }
                .instrument(info_span!("upload shard")),
            );
            sent += 1;
        }
        set.join_all().await;

        Ok(())
    }

    /// Given a channel of items, apply a function to each and send the result to a new channel.
    async fn map<I, F>(self: Arc<Self>, mut receiver: Receiver<I>, f: F) -> Result<(), TaskError>
    where
        F: Fn(Arc<Self>, I) -> Result<(), TaskError>,
        I: Send + 'static,
    {
        while let Some(item) = receiver.recv().await {
            f(self.clone(), item)?;
        }
        Ok(())
    }

    /// Given a stream of `ShardEventData` artifacts, create prove tasks for each shard.
    /// The tasks are then sent to next_sender channel and precompile_sender channel. (The latter
    /// only if it's an execution shard.)
    ///
    /// `leaves_tx` is used to tell the recursion thread the total number of shards after
    /// the input channels are closed.
    pub(crate) async fn spawn_prove_tasks(
        self: Arc<Self>,
        mut receiver: Receiver<(ShardEventData, bool)>,
        mut common_rx: tokio::sync::watch::Receiver<
            Option<(StarkVerifyingKey<CoreSC>, CommonTaskInput, Artifact)>,
        >,
        next_sender: UnboundedSender<(usize, (String, Artifact, Option<(Artifact, String)>))>,
        precompile_sender: UnboundedSender<(usize, (String, Artifact, Option<(Artifact, String)>))>,
        final_tx: UnboundedSender<Result<Artifact, TaskError>>,
        leaves_tx: tokio::sync::watch::Sender<Option<u32>>,
        proof_id: String,
        elf_artifact_id: String,
        parent: Context,
        num_deferred_leaves: usize,
        requester: String,
    ) -> Result<(), TaskError> {
        // Span with root span as the parent for tracing visibility.
        let task_span = with_parent(info_span!("prove tasks"), parent);

        // Spawn child tasks in a join set so if this thread dies, they will also be aborted.
        let requester = requester.clone();
        let mut join_set = tokio::task::JoinSet::<Result<(), TaskError>>::new();
        let total_shards = {
            let mut precompile_sender = Some(precompile_sender);

            let (upload_tx, upload_rx) = tokio::sync::mpsc::channel::<(ShardEventData, bool)>(1);
            let (map_tx, map_rx) =
                tokio::sync::mpsc::channel::<(usize, (Artifact, ShardType, bool))>(1);
            // prove_task, proof_artifact, (deferred_artifact, deferred_marker_task), last
            let (task_tx, task_rx) = tokio::sync::mpsc::unbounded_channel::<(
                usize,
                (String, Artifact, Option<(Artifact, String)>, bool),
            )>();
            let (multi_tx, mut multi_rx) = tokio::sync::mpsc::unbounded_channel::<(
                usize,
                (String, Artifact, Option<(Artifact, String)>, bool),
            )>();

            // Upload inputs
            join_set.spawn(
                self.clone()
                    .upload_data(upload_rx, map_tx, final_tx.clone())
                    .instrument(info_span!("upload_data")),
            );

            // Create prove tasks
            let self_clone = self.clone();
            let common_rx_clone = common_rx.clone();
            let requester = requester.clone();
            let span = info_span!("create tasks");
            let inner_span = info_span!(parent: span.clone(), "create tasks inner");
            join_set.spawn(self.clone().map(map_rx, move |_, item| {
                let _guard = span.enter();
                let elf_artifact_id = elf_artifact_id.clone();
                let proof_id = proof_id.clone();
                let task_tx = task_tx.clone();
                let self_clone = self_clone.clone();
                let mut common_rx = common_rx_clone.clone();
                let tasks_span = task_span.clone();
                let inner_span = inner_span.clone();
                let requester = requester.clone();
                tokio::spawn(
                    async move {
                        // Get common input which has SP1 challenger and vkey. (It may take a few seconds for CPU `prover.setup`.)
                        let common_input_id = common_rx
                            .wait_for(|i| i.is_some())
                            .await
                            .unwrap()
                            .as_ref()
                            .map(|i| i.2.clone().to_id())
                            .unwrap();

                        let output_artifact = self_clone.artifact_client.create_artifact().unwrap();

                        // Create artifact for execution shards to upload their deferred events to.
                        let precompile_shard_artifact = match item.1 .1 {
                            ShardType::Precompile => None,
                            ShardType::Execution => {
                                Some(self_clone.artifact_client.create_artifact().unwrap())
                            }
                        };

                        let task_artifact = item.1 .0.to_id();
                        let mut input_artifacts =
                            vec![elf_artifact_id, common_input_id, task_artifact];
                        let mut output_artifacts = vec![output_artifact.clone()];
                        let marker_task =
                            if let Some(precompile_shard_artifact) = &precompile_shard_artifact {
                                // Create marker task for deferred record upload.
                                let marker_task = self_clone
                                    .worker_client
                                    .create_task(
                                        TaskType::MarkerDeferredRecord,
                                        &[] as &[&Artifact],
                                        &[] as &[&Artifact],
                                        proof_id.clone(),
                                        None,
                                        None,
                                        requester.clone(),
                                    )
                                    .await
                                    .unwrap();
                                input_artifacts.push(marker_task.clone());

                                output_artifacts.push(precompile_shard_artifact.clone());

                                Some(marker_task)
                            } else {
                                None
                            };

                        // Launch task.
                        // OPT: we could create tasks in batches here, may reduce db/API load.
                        let task = self_clone
                            .worker_client
                            .create_task(
                                TaskType::ProveShard,
                                &input_artifacts.iter().collect::<Vec<_>>(),
                                &output_artifacts.iter().collect::<Vec<_>>(),
                                proof_id,
                                None,
                                None,
                                requester.clone(),
                            )
                            .instrument(tasks_span)
                            .await
                            .unwrap();

                        // Send task to next channel.
                        task_tx
                            .send((
                                item.0,
                                (
                                    task,
                                    output_artifact,
                                    precompile_shard_artifact.map(|a| (a, marker_task.unwrap())),
                                    item.1 .2,
                                ),
                            ))
                            .unwrap();
                        Ok::<(), anyhow::Error>(())
                    }
                    .instrument(inner_span),
                );
                Ok(())
            }));

            // Sort tasks after spawning them, since they can come slightly out of order.
            join_set.spawn(
                self.clone()
                    .process_ordered(task_rx, multi_tx)
                    .instrument(info_span!("process_ordered")),
            );

            // Send task and output artifacts to multiple channels.
            join_set.spawn(
                async move {
                    while let Some(item) = multi_rx.recv().await {
                        if let Some(precompile_sender) = &precompile_sender {
                            // This is an execution shard (has deferred events artifact) so we send it to the precompile thread.
                            if item.1 .2.is_some() {
                                let item = item.clone();
                                precompile_sender
                                    .send((item.0, (item.1 .0, item.1 .1, item.1 .2)))
                                    .unwrap();
                            }
                        }
                        // If this is the last execution shard, drop the precompile thread sender.
                        if item.1 .3 {
                            precompile_sender.take();
                        }
                        // Also send to the thread that will wait for these tasks to finish.
                        next_sender
                            .send((
                                num_deferred_leaves + item.0,
                                (item.1 .0, item.1 .1, item.1 .2),
                            ))
                            .unwrap();
                    }
                    Ok(())
                }
                .instrument(info_span!("map")),
            );

            // Handle each shard input and send to the above threads.
            let mut state = PublicValues::<u32, u32>::default().reset();
            // Get common input because we need start_pc.
            let start_pc = common_rx
                .wait_for(|i| i.is_some())
                .await
                .unwrap()
                .as_ref()
                .map(|i| i.0.pc_start.as_canonical_u32())
                .unwrap();
            state.start_pc = start_pc;
            while let Some((mut data, last)) = receiver.recv().await {
                // For each shard, modify `state` depending on shard type, set the shard's public
                // values, then update `state` for future shards.
                // This is similar to logic in local prover prove_with_context.
                state.shard += 1;
                match &mut data {
                    ShardEventData::Checkpoint(_, pv) => {
                        // Set the checkpoint's shard number which depends on precompile shards that
                        // have been inserted.
                        // Increment execution shard since this is an execution shard.
                        state.execution_shard += 1;
                        // Set next_pc and digests which are only known from the executor public values.
                        state.next_pc = pv.next_pc;
                        state.committed_value_digest = pv.committed_value_digest;
                        state.deferred_proofs_digest = pv.deferred_proofs_digest;
                        // All other values can be set from current state.
                        **pv = state;
                        log::info!("[pv] checkpoint {} {:?}", state.shard, state);
                        // Next shard's start_pc should be current shard's next_pc.
                        state.start_pc = state.next_pc;
                    }
                    ShardEventData::GlobalMemory(_, _, pv) => {
                        // Update memory public values. Everything else is set from current state.
                        state.previous_init_addr_bits = pv.previous_init_addr_bits;
                        state.last_init_addr_bits = pv.last_init_addr_bits;
                        state.previous_finalize_addr_bits = pv.previous_finalize_addr_bits;
                        state.last_finalize_addr_bits = pv.last_finalize_addr_bits;
                        // Use current controller state.
                        *pv = state;
                        log::info!("[pv] memory {} {:?}", state.shard, state);
                        // For later shards, previous memory bits should be the current last bits.
                        state.previous_init_addr_bits = state.last_init_addr_bits;
                        state.previous_finalize_addr_bits = state.last_finalize_addr_bits;
                    }
                    ShardEventData::PrecompileRemote(artifacts, code, pv) => {
                        // Use current controller state.
                        *pv = state;
                        let debug_info = artifacts
                            .iter()
                            .map(|(artifact, start, end)| {
                                format!("{:?} {} {}", artifact, start, end)
                            })
                            .collect::<Vec<_>>();
                        log::info!(
                            "[pv] precompile_remote {} {} {:?}",
                            state.shard,
                            code,
                            debug_info
                        );
                    }
                }

                // Send to upload thread.
                upload_tx.send((data, last)).await.unwrap();
            }
            state.shard + num_deferred_leaves as u32
        };
        tracing::info!("total shards: {}", total_shards);
        // Send out total shard count. It's okay if the other side is closed so we ignore the result.
        let _ = leaves_tx.send(Some(total_shards));

        join_set.join_all().await;

        Ok(())
    }

    /// Download deferred precompile records and send them to the precompile packing thread.
    pub(crate) async fn download_precompile_events(
        self: Arc<Self>,
        proof_id: String,
        mut receiver: UnboundedReceiver<(usize, (String, Artifact, Option<(Artifact, String)>))>,
        sender: UnboundedSender<(usize, DeferredEvents)>,
    ) -> Result<(), TaskError> {
        // Spawn child tasks in a join set so if this thread dies, they will also be aborted.
        let mut join_set = tokio::task::JoinSet::<Result<(), TaskError>>::new();
        {
            // mpmc channel for completed tasks -> download workers
            let (completed_tasks_tx, completed_tasks_rx) =
                async_channel::unbounded::<(usize, Artifact)>();
            // Downloaded records -> sort thread
            let (downloaded_records_tx, mut downloaded_records_rx) =
                tokio::sync::mpsc::unbounded_channel::<(usize, DeferredEvents)>();

            // Spawn workers to download records.
            let download_span = info_span!("download records");
            let (artifacts_tx, mut artifacts_rx) =
                tokio::sync::mpsc::unbounded_channel::<Artifact>();
            for _ in 0..PRECOMPILE_RECORD_DOWNLOAD_WORKERS {
                let completed_tasks_rx = completed_tasks_rx.clone();
                let downloaded_records_tx = downloaded_records_tx.clone();
                let client = self.artifact_client.clone();
                let span = download_span.clone();
                let artifacts_tx = artifacts_tx.clone();
                join_set.spawn(
                    async move {
                        while let Ok((index, artifact)) = completed_tasks_rx.recv().await {
                            let record =
                                client.download::<DeferredEvents>(&artifact).await.unwrap();

                            // Send artifact to cleanup collector
                            artifacts_tx.send(artifact).unwrap();
                            downloaded_records_tx.send((index, record)).unwrap();
                        }
                        Ok(())
                    }
                    .instrument(span),
                );
            }
            drop(artifacts_tx); // Close the channel when all workers are done

            // Collect artifacts for batch deletion
            let artifact_client = self.artifact_client.clone();
            join_set.spawn(
                async move {
                    let mut artifacts_to_delete = Vec::new();
                    while let Some(artifact) = artifacts_rx.recv().await {
                        artifacts_to_delete.push(artifact);
                    }

                    // Batch delete all artifacts
                    if !artifacts_to_delete.is_empty() {
                        artifact_client
                            .try_delete_batch(
                                &artifacts_to_delete,
                                sp1_cluster_artifact::ArtifactType::UnspecifiedArtifactType,
                            )
                            .await;
                    }
                    Ok(())
                }
                .instrument(info_span!("cleanup artifacts")),
            );

            // Send records to next channel in order
            join_set.spawn(
                async move {
                    let mut next_index = 0;
                    let mut downloaded_records = HashMap::new();
                    while let Some((index, artifact)) = downloaded_records_rx.recv().await {
                        downloaded_records.insert(index, artifact);

                        // Process any consecutive records we can now send
                        while downloaded_records.contains_key(&next_index) {
                            let record = downloaded_records.remove(&next_index).unwrap();
                            sender.send((next_index, record)).unwrap();
                            next_index += 1;
                        }
                    }
                    Ok(())
                }
                .instrument(info_span!("order records")),
            );

            // Wait for tasks to complete and then send to the above download thread.
            let mut current_checkpoint = 0;
            let mut completed_tasks: HashMap<usize, Artifact> = HashMap::new();
            // Map from task ID to (artifact, last, checkpoint)
            let mut pending_events: HashMap<String, (Artifact, usize)> = HashMap::new();
            // Whether all items have been received from precompile_shard_rx.
            let mut no_more_shards = false;
            // We may receive precompile prove tasks which we'll ignore, so we need to keep track of
            // how many we've received so we know what index to send out.
            let mut received = 0;
            let (sub_tx, mut sub_rx) = self
                .worker_client
                .create_subscriber(proof_id.clone())
                .await?;
            let mut ticker = tokio::time::interval(Duration::from_secs(2));
            while !no_more_shards || !pending_events.is_empty() {
                // let recv_future = receiver.map(|mut r| r.recv());
                tokio::select! {
                    res = receiver.recv(), if !no_more_shards => {
                        match res {
                            Some((_, (_, _, deferred))) => {
                                // Filter for the execution shards which have an artifact that they upload
                                // deferred events to.
                                if let Some((artifact, task)) = deferred {
                                    pending_events.insert(task.clone(), (artifact, received));
                                    sub_tx.send(task).unwrap();
                                    received += 1;
                                }
                            }
                            None => {
                                no_more_shards = true;
                            }
                        }
                    }
                    Some(res) = sub_rx.recv() => {
                        if res.1 == TaskStatus::FailedFatal {
                            return Err(TaskError::Fatal(anyhow!("Tasks {:?} failed", res.1)));
                        }
                        if let Some((artifact, checkpoint)) = pending_events.remove(&res.0) {
                            completed_tasks.insert(checkpoint, artifact);

                            while completed_tasks.contains_key(&current_checkpoint) {
                                let artifact = completed_tasks.remove(&current_checkpoint).unwrap();
                                completed_tasks_tx
                                    .send((current_checkpoint, artifact))
                                    .await
                                    .unwrap();
                                current_checkpoint += 1;
                            }
                        }
                    }
                    _ = ticker.tick() => {
                        log::info!("waiting on {} tasks", pending_events.len());
                    }
                }
            }
            if !completed_tasks.is_empty() {
                log::error!(
                    "completed tasks is not empty (this should never happen): {:?}",
                    completed_tasks
                );
            }
            log::info!(
                "received {} checkpoints, {} completed",
                received,
                current_checkpoint
            );
        }

        join_set.join_all().await;

        Ok(())
    }

    /// Receive deferred precompile records and pack them into records to be proven. Once there are
    /// no more incoming records, send out all remaining precompiles.
    pub(crate) async fn pack_precompiles(
        self: Arc<Self>,
        mut receiver: UnboundedReceiver<(usize, DeferredEvents)>,
        sender: Sender<(ShardEventData, bool)>,
        final_record_receiver: tokio::sync::oneshot::Receiver<ExecutionRecord>,
        opts: SplitOpts,
    ) -> Result<(), TaskError> {
        let mut record = DeferredEvents::empty();
        let mut final_receiver = Some(final_record_receiver);

        loop {
            tokio::select! {
                Some((_, deferred)) = receiver.recv() => {
                    record.append(deferred, &self.artifact_client).await;

                    let new_shards = record.split(false, opts, &self.artifact_client).instrument(info_span!("split")).await;
                    for shard in new_shards {
                        sender.send((shard, false)).await.unwrap();
                    }
                }
                Some(res) = conditional_future(final_receiver.as_mut()) => {
                    async {
                        if let Err(e) = &res {
                            log::error!("final record error: {:?}", e);
                            return Err(TaskError::Fatal(anyhow!("final record error: {}", e)));
                        }
                        let record = res.unwrap();

                        let mut init_addr_bits = [0; 32];
                        let mut finalize_addr_bits = [0; 32];

                        let init_records = Arc::new(record.global_memory_initialize_events);
                        let finalize_records = Arc::new(record.global_memory_finalize_events);
                        let num_chunks = std::cmp::max(init_records.len().div_ceil(opts.memory), finalize_records.len().div_ceil(opts.memory));

                        // Get each chunk of events, handle pubilc values, and send to be uploaded and proven.
                        // Each GlobalMemoryEvents is a view of the Arc<Vec<MemoryInitializeFinalizeEvent>> to avoid copying.
                        for chunk in 0..num_chunks {
                            let start = chunk * opts.memory;
                            let init_end = std::cmp::min(start + opts.memory, init_records.len());
                            let finalize_end = std::cmp::min(start + opts.memory, finalize_records.len());

                            // This logic mirrors `ExecutionRecord::split`.
                            let mut pv = PublicValues {
                                previous_init_addr_bits: init_addr_bits,
                                ..Default::default()
                            };
                            let init_chunk = if init_end <= start {
                                GlobalMemoryEvents::empty()
                            } else {
                                let last_event = &init_records[init_end - 1];
                                let last_init_addr_bits = core::array::from_fn(|i| (last_event.addr >> i) & 1);
                                init_addr_bits = last_init_addr_bits;
                                GlobalMemoryEvents::new(init_records.clone(), start, init_end)
                            };
                            pv.last_init_addr_bits = init_addr_bits;

                            pv.previous_finalize_addr_bits = finalize_addr_bits;
                            let finalize_chunk = if finalize_end <= start {
                                GlobalMemoryEvents::empty()
                            } else {
                                let last_event = &finalize_records[finalize_end - 1];
                                let last_finalize_addr_bits =
                                    core::array::from_fn(|i| (last_event.addr >> i) & 1);
                                finalize_addr_bits = last_finalize_addr_bits;
                                GlobalMemoryEvents::new(finalize_records.clone(), start, finalize_end)
                            };
                            pv.last_finalize_addr_bits = finalize_addr_bits;

                            let shard = ShardEventData::memory(init_chunk, finalize_chunk, pv);
                            sender.send((shard, false)).await.unwrap();
                        }
                        final_receiver = None;
                        Ok(())
                    }.instrument(info_span!("split memory")).await?
                }
                else => {
                    break;
                }
            }
        }

        // Send out remaining records.
        let final_shards = record
            .split(true, opts, &self.artifact_client)
            .instrument(info_span!("split last"))
            .await;
        for shard in final_shards {
            sender.send((shard, false)).await.unwrap();
        }

        Ok(())
    }

    /// Prove the deferred leaves for a proof.
    pub(crate) async fn deferred_leaves_thread(
        self: Arc<Self>,
        mut common_rx: tokio::sync::watch::Receiver<
            Option<(StarkVerifyingKey<CoreSC>, CommonTaskInput, Artifact)>,
        >,
        deferred_proofs: Vec<(SP1ReduceProof<InnerSC>, StarkVerifyingKey<CoreSC>)>,
        proof_id: String,
        task_id: String,
        parent: Context,
        prove_tasks_tx: UnboundedSender<(usize, (String, Artifact, Option<(Artifact, String)>))>,
        requester: String,
    ) -> Result<(), TaskError> {
        if deferred_proofs.is_empty() {
            return Ok(());
        }

        let vk = common_rx
            .wait_for(|i| i.is_some())
            .await
            .unwrap()
            .as_ref()
            .map(|i| i.0.clone())
            .unwrap();

        let (deferred_tasks, deferred_artifacts, _) = self
            .prove_deferred_leaves(
                &proof_id,
                &task_id,
                &vk,
                deferred_proofs
                    .into_iter()
                    .map(|p| (p.0.vk, p.0.proof))
                    .collect::<Vec<_>>(),
                parent.clone(),
                requester.clone(),
            )
            .await?;

        for (i, (task, artifact)) in deferred_tasks
            .into_iter()
            .zip(deferred_artifacts.into_iter())
            .enumerate()
        {
            prove_tasks_tx.send((i, (task, artifact, None))).unwrap();
        }
        Ok(())
    }

    /// Join leaves into trees of at most `2^FOLD_LAYER`, then fold them backwards into a single proof.
    pub async fn stream_recursion(
        self: Arc<Self>,
        mut open_rx: UnboundedReceiver<(usize, (String, Artifact, Option<(Artifact, String)>))>,
        result_tx: UnboundedSender<Result<Artifact, TaskError>>,
        proof_id: String,
        task_id: String,
        leaves_rx: tokio::sync::watch::Receiver<Option<u32>>,
        parent: Context,
        requester: String,
    ) -> Result<(), TaskError> {
        let (new_tasks_tx, mut new_tasks_rx) = tokio::sync::mpsc::unbounded_channel::<
            Result<((usize, usize), String, Artifact), TaskError>,
        >();

        let mut ready_map = BTreeMap::<(usize, usize), (String, Artifact)>::new();
        let mut running_map = HashMap::<String, (usize, usize, Artifact)>::new();

        // Channel for recursion join thread to send proofs to the folding thread, where it will
        // fold them backwards.
        let (fold_tx, mut fold_rx) =
            tokio::sync::mpsc::unbounded_channel::<Result<(usize, Artifact), TaskError>>();

        // Spans for tracing where the parent is the root span.
        let tasks_span = with_parent(info_span!("recursion tasks"), parent.clone());
        let fold_tasks_span = with_parent(info_span!("recursion fold tasks"), parent);

        /// Insert a task into the tree, where it will be next to its "neighbor", or the proof it should
        /// be joined into. If it has no neighbor (fold layer) or its neighbor is already in the tree, it won't be
        /// inserted and the neighbor will be returned.
        fn insert_task(
            start_layer: usize,
            mut index: usize,
            task_id: &str,
            artifact: Artifact,
            ready_map: &mut BTreeMap<(usize, usize), (String, Artifact)>,
            leaf_count: Option<u32>,
        ) -> (usize, usize, Option<(usize, usize, (String, Artifact))>) {
            for layer in start_layer..=(*FOLD_LAYER) {
                // Get correct num of nodes in this layer so we can see if this task belongs in the
                // current layer or not. If it's the odd one out, it will be deferred to the next
                // layer with an odd number of nodes.
                // If the total leaf count is not known yet (shards are not done being generated),
                // we can just assume we're not close to the end of the layer yet.
                let layer_size = leaf_count
                    .map(|c| get_tree_layer_size(c, layer))
                    .unwrap_or(usize::MAX);
                if (layer_size > 1 && index < layer_size) || layer == *FOLD_LAYER {
                    let neighbor_key = index / 2 * 2 + (1 - index % 2);
                    let neighbor = ready_map
                        .remove(&(layer, neighbor_key))
                        .map(|artifact| (layer, neighbor_key, artifact));

                    // Insert into ready_map if the neighbor is not ready yet
                    if neighbor.is_none() && layer != *FOLD_LAYER {
                        ready_map.insert((layer, index), (task_id.to_string(), artifact));
                    }

                    return (layer, index, neighbor);
                } else {
                    index /= 2;
                }
            }
            unreachable!()
        }

        let artifact_client = self.artifact_client.clone();
        let worker_client = self.worker_client.clone();
        let proof_id_clone = proof_id.clone();
        let task_id_clone = task_id.clone();
        let requester = requester.clone();
        let requester_clone = requester.clone();
        let mut gotten_leaves_count = false;

        let (sub_tx, mut sub_rx) = self
            .worker_client
            .create_subscriber(proof_id_clone.clone())
            .await?;
        let mut ticker = tokio::time::interval(Duration::from_secs(2));
        let mut set = JoinSet::new();
        let span = info_span!("wait_tasks");
        set.spawn(async move {
            // Counter for tasks that are about to be created on another thread from previous iteration.
            // Used to know when to break the loop.
            let mut tasks_being_created = 0;
            let requester = requester_clone.clone();
            loop {
                // Track all tasks that are updated in this iteration.
                let mut updated_tasks = Vec::<(
                    usize,
                    usize,
                    Artifact,
                    Option<(usize, usize, (String, Artifact))>,
                )>::new();

                // Check if total shards is known yet.
                let leaves = *leaves_rx.borrow();
                // We clone and use the same value when we insert into the tree later in the same
                // loop iteration. We don't want to call insert_task with the leaves_count value
                // until we've migrated all the tasks that could be past the fold layer.
                let leaves_count_clone = leaves;
                if !gotten_leaves_count && leaves.is_some() {
                    // Now that we know total shards, there may be odd-one-out tasks at the end of
                    // each layer that need to be pushed to the next layer.
                    // Easy way to do this is to make a new map and push all the tasks to it.
                    gotten_leaves_count = true;
                    let old_ready_map = std::mem::take(&mut ready_map);
                    for ((layer, index), (task, artifact)) in old_ready_map.into_iter() {
                        let (tree_layer, layer_index, neighbor) = insert_task(
                            layer,
                            index,
                            &task,
                            artifact.clone(),
                            &mut ready_map,
                            leaves,
                        );
                        updated_tasks.push((tree_layer, layer_index, artifact, neighbor));
                    }
                }

                tokio::select! {
                    Some((index, (task_id, artifact, _))) = open_rx.recv() => {
                        // Track this Phase1+Lift task in the running map.
                        sub_tx.send(task_id.clone()).unwrap();
                        running_map.insert(task_id, (0, index, artifact));
                    }
                    Some(res) = new_tasks_rx.recv() => {
                        match res {
                            Ok((index, task_id, artifact)) => {
                                // Track this Compress task in the running map.
                                tasks_being_created -= 1;
                                sub_tx.send(task_id.clone()).unwrap();
                                running_map.insert(task_id, (index.0, index.1, artifact));
                            }
                            Err(e) => {
                                panic!("Failed to create task: {:?}", e);
                            }
                        }
                    }
                    res = sub_rx.recv() => {
                        match res {
                            Some((task_id, status)) => {
                                if status == TaskStatus::FailedFatal {
                                    fold_tx
                                        .send(Err(TaskError::Fatal(anyhow::anyhow!(
                                            "Task failed {:?}",
                                            task_id
                                        ))))
                                        .unwrap();
                                    break;
                                }

                                if let Some((proven_recursion_level, proven_layer_index, artifact)) =
                                    running_map.remove(&task_id) {
                                    let (tree_level, layer_index, neighbor) = insert_task(
                                        proven_recursion_level,
                                        proven_layer_index,
                                        &task_id,
                                        artifact.clone(),
                                        &mut ready_map,
                                        leaves_count_clone,
                                    );
                                    updated_tasks.push((tree_level, layer_index, artifact, neighbor));
                                }
                            }
                            None => {
                                tracing::warn!("subscriber closed");
                                break;
                            }
                        }
                    }
                    _ = ticker.tick() => {
                        log::info!("waiting on {} tasks", running_map.len());
                    }
                }

                let requester = requester.clone();
                for (tree_level, layer_index, artifact, neighbor) in updated_tasks {
                    // If the completed task belongs in the fold level, send it to the folding thread.
                    // If its neighbor is ready, create a task to join the two and put into running map.
                    if tree_level == *FOLD_LAYER {
                        // Send to folding thread.
                        fold_tx.send(Ok((layer_index, artifact))).unwrap();
                    } else if let Some(neighbor) = neighbor {
                        let (first, second) = if layer_index % 2 == 0 {
                            (artifact, neighbor.2 .1)
                        } else {
                            (neighbor.2 .1, artifact)
                        };
                        let sender = new_tasks_tx.clone();
                        let artifact_client = artifact_client.clone();
                        let worker_client = worker_client.clone();
                        let proof_id_clone = proof_id_clone.clone();
                        let task_id_clone = task_id_clone.clone();
                        tasks_being_created += 1;
                        let input_artifacts = [first, second];
                        let tasks_span = tasks_span.clone();
                        let requester = requester.clone();
                        let span = info_span!("create_task");
                        tokio::spawn(async move {
                            let output = artifact_client
                                .create_artifact()
                                .unwrap();
                            match worker_client
                                .create_task(
                                    TaskType::RecursionReduce,
                                    &input_artifacts.iter().collect::<Vec<_>>(),
                                    &[&output],
                                    proof_id_clone,
                                    Some(task_id_clone),
                                    None,
                                    requester.clone(),
                                )
                                .instrument(tasks_span)
                                .await
                            {
                                Ok(task) => {
                                    sender
                                        .send(Ok(((tree_level + 1, layer_index / 2), task, output)))
                                        .unwrap();
                                }
                                Err(e) => {
                                    tracing::error!("Failed to create task: {:?}", e);
                                    sender.send(Err(TaskError::Fatal(anyhow!(e)))).unwrap();
                                }
                            }
                            Ok::<(), TaskError>(())
                        }.instrument(span));
                    }
                }

                // Break the loop when there's no more incoming leaves, there's no tasks running,
                // and there's no tasks about to be created. (or if subscriber is closed)
                if sub_rx.is_closed()
                    || (open_rx.is_closed()
                        && open_rx.is_empty()
                        && running_map.is_empty()
                        && tasks_being_created == 0
                        && gotten_leaves_count)
                {
                    if !ready_map.is_empty() {
                        log::error!(
                            "ready map is not empty (this should never happen): {:?}",
                            ready_map
                        );
                    }
                    tracing::debug!("breaking stream loop");
                    break;
                }
            }
            anyhow::Result::<_, TaskError>::Ok(())
        }.instrument(span));

        // Spawn a thread to fold proofs backwards.
        let artifact_client = self.artifact_client.clone();
        let worker_client = self.worker_client.clone();
        let proof_id_clone = proof_id.clone();
        let span = info_span!("fold_thread_handle");
        let requester = requester.clone();

        let fold_thread = async move {
            // First proof doesn't need to be folded.
            let mut current_fold: Option<(usize, Option<String>, Artifact)> = None;
            let mut fold_queue = Vec::new();
            let mut next_index = 0;

            loop {
                if let Some(result) = fold_rx.recv().await {
                    if let Err(e) = result {
                        log::error!("recursion join thread panicked: {:?}", e);
                        return Err(e);
                    }
                    fold_queue.push(result.unwrap());
                }
                // Process proofs in order.
                fold_queue.sort_by_key(|(index, _)| *index);

                while !fold_queue.is_empty() && fold_queue[0].0 == next_index {
                    let (index, fold_artifact) = fold_queue.remove(0);
                    next_index += 1;
                    if let Some((index, prev_task_id, prev_artifact)) = &current_fold {
                        // Wait for previous fold task to complete.
                        if let Some(task_id) = prev_task_id {
                            worker_client.wait_tasks(proof_id_clone.clone(), &[task_id.clone()]).await?;
                        }

                        // Create a task to fold the two proofs.
                        let output = artifact_client.create_artifact().unwrap();

                        let input_artifacts = vec![&prev_artifact, &fold_artifact];

                        // Create task in span for tracing visibility.
                        let fold_tasks_span = fold_tasks_span.clone();
                        let new_task = worker_client
                            .create_task(
                                TaskType::RecursionReduce,
                                &input_artifacts,
                                &[&output],
                                proof_id.clone(),
                                Some(task_id.clone()),
                                None,
                                requester.clone(),
                            )
                            .instrument(fold_tasks_span)
                            .await?;
                        current_fold.replace((*index + 1, Some(new_task), output));
                    } else {
                        current_fold.replace((index, None, fold_artifact));
                    }
                }

                if fold_rx.is_closed() && fold_rx.is_empty() {
                    if !fold_queue.is_empty() {
                        log::error!(
                            "fold queue is not contiguous (this should never happen): {:?} {}",
                            fold_queue,
                            next_index
                        );
                    }
                    if current_fold.is_none() {
                        log::error!("current_fold is none after fold receiver is closed (this should never happen)");
                    }
                    log::debug!("recursion join thread is done");
                    break;
                }
            }

            // Wait for last task to complete.
            let (_, task_id, artifact) = current_fold.unwrap();
            if let Some(task_id) = task_id {
                worker_client.wait_tasks(proof_id_clone, &[task_id.clone()]).await?;
            }

            anyhow::Result::<_, TaskError>::Ok(artifact)
        }.instrument(span);

        let final_proof = fold_thread.await?;

        result_tx.send(Ok(final_proof)).unwrap();

        Ok(())
    }
}
