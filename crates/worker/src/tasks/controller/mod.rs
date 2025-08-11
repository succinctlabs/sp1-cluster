#![allow(clippy::too_many_arguments)]
#![allow(clippy::type_complexity)]

pub mod shards;
pub mod stream;

use super::TaskMetadata;
use crate::error::TaskError;
use crate::utils::conditional_future;
use crate::{client::WorkerService, ClusterProverComponents, SP1Worker, POLLING_INTERVAL_MS};
use anyhow::{anyhow, Result};
use hashbrown::HashMap;
use opentelemetry::Context;
use p3_baby_bear::BabyBear;
use p3_field::AbstractField;
use serde::{Deserialize, Serialize};
use shards::{DeferredEvents, ShardEventData};
use sp1_cluster_artifact::{Artifact, ArtifactBatch};
use sp1_cluster_artifact::{ArtifactClient, ArtifactType};
use sp1_cluster_common::proto::{ProofRequestStatus, TaskStatus, TaskType, WorkerTask};
use sp1_core_executor::ExecutionRecord;
use sp1_core_executor::{DeferredProofVerification, SP1ReduceProof};
use sp1_prover::{CoreSC, InnerSC, SP1_CIRCUIT_VERSION};
use sp1_recursion_core::DIGEST_SIZE;
use sp1_sdk::network::proto::types::ProofMode;
use sp1_sdk::proof::ProofFromNetwork;
use sp1_sdk::{SP1Proof, SP1Prover, SP1Stdin};
use sp1_sdk::{SP1PublicValues, SP1VerifyingKey};
use sp1_stark::{Challenger, StarkVerifyingKey};
use sp1_stark::{MachineProver, StarkGenericConfig};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::error::TryRecvError;
use tracing::{info_span, Instrument};

/// Max number of ShardEventData to keep in the channel. This channel should only have backpressure
/// when waiting for setup vkey to finish.
///
/// 150 means with 30 MHz executor, this shouldn't fill up for >7.5 seconds. Setup should take <6s.
/// If each checkpoint is ~50MB, this should be ~7.5 GB.
pub(crate) const PROVE_INPUTS_CHANNEL_CAPACITY: usize = 150;

/// Common data used in all ProveShard tasks and ShrinkWrap task for a proof.
#[derive(Clone, Serialize, Deserialize)]
pub struct CommonTaskInput {
    pub challenger: Challenger<CoreSC>,
    pub mode: ProofMode,
    pub vk: StarkVerifyingKey<CoreSC>,
    /// The final deferred digest. This is used in prove shard to generate the leaf proof, since
    /// the leaf program takes in `reconstruct_deferred_digest`.
    pub deferred_digest: [BabyBear; DIGEST_SIZE],
}

impl<W: WorkerService, A: ArtifactClient> SP1Worker<W, A> {
    /// The controller task for an SP1 proof. This task does all of the coordination for a full
    /// SP1 proof which can have mode core, compressed, plonk, and groth16.
    pub async fn process_sp1_controller(
        self: &Arc<Self>,
        parent: Context,
        task: &WorkerTask,
    ) -> Result<TaskMetadata, TaskError> {
        let data = task.data()?;

        if data.inputs.is_empty() {
            log::info!("no inputs for task");
            return Err(TaskError::Fatal(anyhow::anyhow!("no inputs for task")));
        }

        let parsed = data.inputs[2]
            .parse::<i32>()
            .map_err(|e| TaskError::Fatal(anyhow::anyhow!(e)))?;
        let mode = ProofMode::try_from(parsed).map_err(|e| TaskError::Fatal(anyhow::anyhow!(e)))?;
        let (elf, stdin) = tokio::try_join!(
            self.artifact_client.download_program(&data.inputs[0]),
            self.artifact_client
                .download_stdin::<SP1Stdin>(&data.inputs[1]),
        )?;
        let output_artifact_id = data.outputs[0].clone();

        let cycle_limit = if data.inputs.len() > 3 {
            data.inputs[3].parse::<u64>().ok()
        } else {
            None
        };

        let (common_tx, common_rx) = tokio::sync::watch::channel::<
            Option<(StarkVerifyingKey<CoreSC>, CommonTaskInput, Artifact)>,
        >(None);

        let (proof, pv, vk) = {
            // Setup channels. Some channels are bounded to limit memory usage.
            let (prove_inputs_tx, prove_inputs_rx) =
                tokio::sync::mpsc::channel::<(ShardEventData, bool)>(PROVE_INPUTS_CHANNEL_CAPACITY);
            let (prove_tasks_tx, mut prove_tasks_rx) = tokio::sync::mpsc::unbounded_channel::<(
                usize,
                (String, Artifact, Option<(Artifact, String)>),
            )>();
            let (precompile_wait_tx, precompile_wait_rx) = tokio::sync::mpsc::unbounded_channel::<(
                usize,
                (String, Artifact, Option<(Artifact, String)>),
            )>();
            let (precompile_record_tx, precompile_record_rx) =
                tokio::sync::mpsc::unbounded_channel::<(usize, DeferredEvents)>();
            let (final_tx, mut final_rx) =
                tokio::sync::mpsc::unbounded_channel::<Result<Artifact, TaskError>>();
            let (leaves_tx, leaves_count_rx) = tokio::sync::watch::channel::<Option<u32>>(None);
            let (final_record_tx, final_record_rx) =
                tokio::sync::oneshot::channel::<ExecutionRecord>();

            // Generate checkpoints and send to checkpoint channel.
            let program = self.prover.get_program(&elf)?;

            let mut join_set = tokio::task::JoinSet::<Result<(), TaskError>>::new();
            let prover = self.prover.clone();
            let worker_client = self.worker_client.clone();
            let artifact_client = self.artifact_client.clone();

            let deferred_proofs = stdin.proofs.clone();
            let deferred_proofs_clone = deferred_proofs.clone();
            let num_deferred_leaves = deferred_proofs.len();

            let task_id = task.task_id.clone();
            let requester = data.requester.clone();
            let proof_id = data.proof_id.clone();
            let parent_clone = parent.clone();
            let elf_artifact_id = data.inputs[0].clone();

            let vkey_cache = self.vkey_cache.clone();
            let requester_clone = requester.clone();
            // Spawn a task to get the vkey from cache or from a worker that will do the setup.
            join_set.spawn(
                async move {
                    let (input, vk) = {
                        let mut lock = vkey_cache.lock().await;
                        let res = lock.get(&elf_artifact_id).cloned();
                        drop(lock);
                        let vk = if let Some(vk) = res {
                            vk.clone()
                        } else {
                            // Create setup task on GPU.
                            let start_time = Instant::now();
                            let vk_artifact = artifact_client.create_artifact().unwrap();
                            let setup_task = worker_client
                                .create_task(
                                    TaskType::SetupVkey,
                                    &[&elf_artifact_id],
                                    &[&vk_artifact],
                                    proof_id.clone(),
                                    Some(task_id),
                                    Some(parent_clone),
                                    requester_clone.clone(),
                                )
                                .await
                                .unwrap();

                            worker_client
                                .wait_tasks(proof_id, &[setup_task])
                                .await
                                .unwrap_or_else(|e| panic!("failed to wait for task: {:?}", e));

                            if start_time.elapsed() > Duration::from_secs(10) {
                                log::warn!(
                                    "setup task took {:?} to complete",
                                    start_time.elapsed()
                                );
                            }

                            let vk: StarkVerifyingKey<CoreSC> =
                                artifact_client.download(&vk_artifact).await.unwrap();

                            // Clean up VKey setup artifact
                            artifact_client
                                .try_delete(&vk_artifact, ArtifactType::UnspecifiedArtifactType)
                                .await;

                            vkey_cache.lock().await.put(elf_artifact_id, vk.clone());
                            vk
                        };

                        let mut challenger = prover.core_prover.machine().config().challenger();
                        vk.observe_into(&mut challenger);

                        // Hash deferred digest since every leaf proof's public values needs it.
                        let mut deferred_digest = [BabyBear::zero(); DIGEST_SIZE];
                        deferred_digest =
                            SP1Prover::<ClusterProverComponents>::hash_deferred_proofs(
                                deferred_digest,
                                &deferred_proofs_clone
                                    .into_iter()
                                    .map(|p| p.0)
                                    .collect::<Vec<_>>(),
                            );

                        (
                            CommonTaskInput {
                                challenger,
                                mode,
                                deferred_digest,
                                vk: vk.clone(),
                            },
                            vk,
                        )
                    };

                    let artifact = artifact_client.create_artifact().unwrap();
                    artifact_client.upload(&artifact, &input).await.unwrap();
                    common_tx.send(Some((vk, input, artifact))).unwrap();
                    Ok(())
                }
                .instrument(info_span!("setup")),
            );

            // Spawn thread that will execute the program and send checkpoints to prove_inputs channel.
            let execute_handle = tokio::spawn(
                self.clone()
                    .execute_thread(
                        program.clone(),
                        stdin,
                        self.prover_opts.core_opts,
                        prove_inputs_tx.clone(),
                        final_record_tx,
                        cycle_limit,
                        DeferredProofVerification::Enabled,
                    )
                    .instrument(info_span!("execute_thread")),
            );

            // Launch prove tasks and send the task info to recursion streaming thread and precompile record download thread.
            join_set.spawn(
                self.clone()
                    .spawn_prove_tasks(
                        prove_inputs_rx,
                        common_rx.clone(),
                        prove_tasks_tx.clone(),
                        precompile_wait_tx,
                        final_tx.clone(),
                        leaves_tx,
                        data.proof_id.clone(),
                        data.inputs[0].clone(),
                        parent.clone(),
                        num_deferred_leaves,
                        requester.clone(),
                    )
                    .instrument(info_span!("spawn_prove_tasks")),
            );

            // Download precompile records.
            join_set.spawn(
                self.clone()
                    .download_precompile_events(
                        data.proof_id.clone(),
                        precompile_wait_rx,
                        precompile_record_tx,
                    )
                    .instrument(info_span!("download_precompile_events")),
            );

            // Pack precompile records and emit records as they are full or when there's no more shards coming in.
            join_set.spawn(
                self.clone()
                    .pack_precompiles(
                        precompile_record_rx,
                        prove_inputs_tx,
                        final_record_rx,
                        self.prover_opts.core_opts.split_opts,
                    )
                    .instrument(info_span!("pack_precompiles")),
            );

            let requester_clone = requester.clone();
            if mode == ProofMode::Core {
                drop(prove_tasks_tx);

                // Just download shard proofs and combine into a single SP1Proof.
                let mut running_tasks = HashMap::new();
                let mut shard_proofs = Vec::new();
                let mut completed_proofs = HashMap::new();
                let proof_id_clone = data.proof_id.clone();
                // Wait for all prove tasks to complete.
                async {
                    loop {
                        loop {
                            match prove_tasks_rx.try_recv() {
                                Ok(task) => {
                                    running_tasks.insert(task.1 .0, (task.0, task.1 .1));
                                }
                                Err(TryRecvError::Empty) => break,
                                Err(TryRecvError::Disconnected) => {
                                    break;
                                }
                            }
                        }

                        if !running_tasks.is_empty() {
                            let statuses = self
                                .worker_client
                                .get_task_statuses(
                                    proof_id_clone.clone(),
                                    &running_tasks.keys().cloned().collect::<Vec<_>>(),
                                )
                                .await
                                .unwrap();

                            if statuses.contains_key(&TaskStatus::FailedFatal)
                                && !statuses[&TaskStatus::FailedFatal].is_empty()
                            {
                                // Join set will auto abort on drop.
                                return Err(TaskError::Fatal(anyhow!(
                                    "shard proof tasks {:?} failed",
                                    statuses[&TaskStatus::FailedFatal]
                                )));
                            }
                            for completed_task in
                                statuses.get(&TaskStatus::Succeeded).unwrap_or(&vec![])
                            {
                                let (idx, artifact) = running_tasks.remove(completed_task).unwrap();
                                completed_proofs.insert(idx, artifact);
                            }

                            tokio::time::sleep(std::time::Duration::from_millis(
                                *POLLING_INTERVAL_MS * 2,
                            ))
                            .await;
                        }

                        while completed_proofs.contains_key(&shard_proofs.len()) {
                            let artifact = completed_proofs.remove(&shard_proofs.len()).unwrap();
                            shard_proofs.push(artifact);
                        }

                        // If all tasks are done, break out of the loop.
                        if prove_tasks_rx.is_closed()
                            && prove_tasks_rx.is_empty()
                            && running_tasks.is_empty()
                        {
                            break;
                        }

                        tokio::time::sleep(std::time::Duration::from_millis(
                            *POLLING_INTERVAL_MS * 2,
                        ))
                        .await;
                    }
                    Ok(())
                }
                .instrument(info_span!("wait core shards"))
                .await?;

                // OPT: we could download these as they come in, could help for large proofs.
                let pv = execute_handle.await.unwrap()?;
                let shard_proofs = ArtifactBatch::from(shard_proofs)
                    .download(&self.artifact_client)
                    .await?;

                let vk = common_rx.borrow().as_ref().unwrap().0.clone();

                (SP1Proof::Core(shard_proofs), pv, vk)
            } else {
                // After vk + initial challenger is created, launch all deferred leaves, and then
                // send those into prove_tasks channel so recursion thread can merge them.
                let requester_clone_2 = requester.clone();
                join_set.spawn(self.clone().deferred_leaves_thread(
                    common_rx.clone(),
                    deferred_proofs,
                    data.proof_id.clone(),
                    task.task_id.clone(),
                    parent.clone(),
                    prove_tasks_tx,
                    requester_clone_2.clone(),
                ));

                // Stream recursion.
                join_set.spawn(
                    self.clone()
                        .stream_recursion(
                            prove_tasks_rx,
                            final_tx,
                            data.proof_id.clone(),
                            task.task_id.clone(),
                            leaves_count_rx,
                            parent.clone(),
                            requester_clone.clone(),
                        )
                        .instrument(info_span!("stream_recursion")),
                );

                // Wait for any task in join_set to fail, or final_rx and execute_handle to complete.
                let mut final_result = None;
                let mut pv = None;
                let mut execute_handle = Some(execute_handle);
                loop {
                    tokio::select! {
                        Some(result) = join_set.join_next() => {
                            // Join set will auto abort on drop.
                            result.map_err(|e| TaskError::Fatal(anyhow!("Thread error: {}", e)))??;
                        }
                        result = final_rx.recv() => {
                            final_result = Some(result.expect("final channel closed")?);
                            if pv.is_some() {
                                break;
                            }
                        }
                        Some(result) = conditional_future(execute_handle.as_mut()) => {
                            execute_handle = None;
                            pv = Some(result.map_err(|e| TaskError::Fatal(anyhow!("Thread error: {}", e)))??);
                            if final_result.is_some() {
                                break;
                            }
                        }
                    };
                }

                let final_result = final_result.unwrap();
                let pv = pv.unwrap();
                let (vk, _, common_artifact) = common_rx.borrow().as_ref().unwrap().clone();

                if mode == ProofMode::Compressed {
                    // If user just requested a compressed proof, stop here.
                    let proof: SP1ReduceProof<InnerSC> =
                        self.artifact_client.download(&final_result).await?;
                    (SP1Proof::Compressed(Box::new(proof)), pv, vk)
                } else {
                    // Otherwise, shrink and wrap (maybe on a GPU), then wrap bn254.
                    let shrink_artifact = self.artifact_client.create_artifact()?;
                    let shrink_task = self
                        .worker_client
                        .create_task(
                            TaskType::ShrinkWrap,
                            &[&final_result, &common_artifact],
                            &[&shrink_artifact],
                            data.proof_id.clone(),
                            Some(task.task_id.clone()),
                            Some(parent.clone()),
                            requester.clone(),
                        )
                        .await?;

                    self.worker_client
                        .wait_tasks(data.proof_id.clone(), &[shrink_task])
                        .await?;

                    // Create an artifact for the public values.
                    let pv_artifact = self.artifact_client.create_artifact()?;

                    self.artifact_client
                        .upload(&pv_artifact, SP1PublicValues::from(&pv))
                        .await?;

                    let wrap_task_type = match mode {
                        ProofMode::Plonk => TaskType::PlonkWrap,
                        ProofMode::Groth16 => TaskType::Groth16Wrap,
                        _ => unreachable!(),
                    };

                    // Completes the proof in process_sp1_finalize
                    self.worker_client
                        .create_task(
                            wrap_task_type,
                            &[&shrink_artifact, &pv_artifact],
                            &[&output_artifact_id], // Pass the original output artifact directly
                            data.proof_id.clone(),
                            Some(task.task_id.clone()),
                            Some(parent.clone()),
                            requester.clone(),
                        )
                        .await?;

                    // Returns early
                    return Ok(Default::default());
                }
            }
        };

        // Verify compressed proof in another thread.
        let maybe_verify_future = if let SP1Proof::Compressed(proof) = &proof {
            let proof_clone = proof.clone();
            let prover_clone = self.prover.clone();
            let span = info_span!("verify compressed proof");
            Some(tokio::task::spawn_blocking(move || {
                let _guard = span.enter();
                if let Err(e) =
                    prover_clone.verify_compressed(&proof_clone, &SP1VerifyingKey { vk })
                {
                    return Err(TaskError::Fatal(anyhow!(
                        "verify_compressed failed: {:?}",
                        e
                    )));
                }
                Ok(())
            }))
        } else {
            None
        };

        // Upload final result.
        let result = ProofFromNetwork {
            proof,
            public_values: SP1PublicValues::from(&pv),
            sp1_version: SP1_CIRCUIT_VERSION.to_string(),
        };

        if std::env::var("WORKER_NO_UPLOAD").unwrap_or("false".to_string()) == "true" {
            tracing::info!("skipping upload");
        } else {
            self.artifact_client
                .upload_proof(&data.outputs[0], result)
                .await?;
        }

        // Clean up stdin since it's no longer needed
        let mut artifacts_to_cleanup = vec![data.inputs[1].clone()];

        // Add common prove shard input artifact
        if let Some((_, _, common_artifact)) = common_rx.borrow().as_ref() {
            artifacts_to_cleanup.push(common_artifact.0.clone());
        }

        self.artifact_client
            .try_delete_batch(&artifacts_to_cleanup, ArtifactType::UnspecifiedArtifactType)
            .await;

        // Ensure final proof verifies.
        if let Some(verify_future) = maybe_verify_future {
            verify_future.await.unwrap()?;
        }

        // Mark proof as completed.
        self.worker_client
            .complete_proof(
                data.proof_id.clone(),
                Some(task.task_id.clone()),
                ProofRequestStatus::Completed,
            )
            .await?;

        Ok(Default::default())
    }
}
