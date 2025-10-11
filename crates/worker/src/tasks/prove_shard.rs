use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use futures::TryFutureExt;
use p3_baby_bear::BabyBear;
use p3_challenger::CanObserve;
use sp1_cluster_artifact::{ArtifactClient, ArtifactType};
use sp1_cluster_common::proto::WorkerTask;
use sp1_core_executor::{RiscvAirId, SP1ReduceProof};
use sp1_prover::{shapes::SP1CompressProgramShape, CoreSC, SP1CircuitWitness};
use sp1_recursion_circuit::machine::{SP1RecursionShape, SP1RecursionWitnessValues};
use sp1_recursion_core::air::RecursionPublicValues;
use sp1_recursion_core::RecursionProgram;
use sp1_sdk::network::proto::types::ProofMode;
use sp1_sdk::HashableKey;
use sp1_stark::air::MachineAir;
use sp1_stark::shape::OrderedShape;
use sp1_stark::{MachineProof, MachineRecord, MachineVerificationError};
use sp1_stark::{MachineProver, StarkGenericConfig, Verifier};
use std::borrow::Borrow;
use std::iter::once;
use std::slice::from_mut;
use tokio::try_join;

use crate::client::WorkerService;
use crate::tasks::controller::shards::DeferredEvents;
use crate::{acquire_gpu, ShardEventData};
use crate::{error::TaskError, SP1Worker};

use super::controller::CommonTaskInput;
use super::TaskMetadata;

impl<W: WorkerService, A: ArtifactClient> SP1Worker<W, A> {
    /// Prove a single shard.
    pub async fn process_sp1_prove_shard(
        self: &Arc<Self>,
        task: &WorkerTask,
    ) -> Result<TaskMetadata, TaskError> {
        let data = task.data()?;
        let client = &self.artifact_client;
        let (elf, common_input, input) = tokio::try_join!(
            client.download_program(&data.inputs[0]),
            client.download::<CommonTaskInput>(&data.inputs[1]),
            client.download::<ShardEventData>(&data.inputs[2]),
        )?;
        let marker_task = data.inputs.get(3).cloned();
        let program = self.prover.get_program(&elf)?;
        let program_clone = program.clone();
        let opts_clone = self.prover_opts;

        let is_precompile = match input {
            ShardEventData::Checkpoint(_, _) => false,
            ShardEventData::GlobalMemory(_, _, _) => true,
            ShardEventData::PrecompileRemote(_, _, _) => true,
        };

        // Extract precompile artifacts before moving input
        let precompile_artifacts =
            if let ShardEventData::PrecompileRemote(ref artifacts, _, _) = input {
                Some(artifacts.clone())
            } else {
                None
            };

        let (mut shard, deferred) = input.into_record(program_clone, self.clone()).await?;

        let prover = self.prover.clone();
        let dep_span = tracing::info_span!("generate_dependencies");
        let trace_span = tracing::info_span!("generate_traces");
        let (traces, shard) = tokio::task::spawn_blocking(move || {
            dep_span.in_scope(|| {
                prover.core_prover.machine().generate_dependencies(
                    from_mut(&mut shard),
                    &opts_clone.core_opts,
                    None,
                )
            });
            if let Some(shape_config) = &prover.core_shape_config {
                shape_config.fix_shape(&mut shard).unwrap();
            }

            let traces = trace_span.in_scope(|| prover.core_prover.generate_traces(&shard));
            (traces, shard)
        })
        .await
        .unwrap();

        // Oneshot channel to send recursion program back to main thread
        let (tx, rx) = tokio::sync::oneshot::channel::<
            Arc<RecursionProgram<<CoreSC as StarkGenericConfig>::Val>>,
        >();
        let shard_number = shard.public_values.shard;

        // CPU tracegen leaves some chips' traces missing, so we need to get heights from chips and then get shape.
        // TODO: coalesce with sp1
        let proof_shape = {
            let mut heights = vec![];
            let chips = self
                .prover
                .core_prover
                .shard_chips(&shard)
                .collect::<Vec<_>>();
            let shape = shard.shape.as_ref().expect("shape not set");
            for chip in chips.iter() {
                let id = RiscvAirId::from_str(&chip.name()).unwrap();
                let height = shape.log2_height(&id).unwrap();
                heights.push((chip.name().clone(), height));
            }
            OrderedShape::from_log2_heights(&heights)
        };
        log::debug!("stats {} {:?}", shard_number, shard.stats());
        let compile_thread = if common_input.mode != ProofMode::Core {
            let prover_clone = self.prover.clone();
            let span = tracing::info_span!("compile recursion program");
            Some(tokio::task::spawn_blocking(move || {
                span.in_scope(|| {
                    log::debug!("threaded shape {}: {:?}", shard_number, proof_shape);
                    let compress_shape = SP1CompressProgramShape::Recursion(SP1RecursionShape {
                        proof_shapes: vec![proof_shape],
                        is_complete: false,
                    });
                    let program = prover_clone.program_from_shape(compress_shape, None);
                    if tx.send(program).is_err() {
                        log::error!("failed to send recursion program");
                    }
                });
            }))
        } else {
            None
        };

        // For execution shards, upload the precompile events so the controller can pack them.
        let deferred_promise = if !is_precompile {
            if let Some(marker_task) = marker_task {
                let client = self.artifact_client.clone();
                let worker_client = self.worker_client.clone();
                let output = data.outputs[1].clone();
                let opts = self.prover_opts.core_opts.split_opts;
                let proof_id = data.proof_id.clone();
                Some(
                    tokio::spawn(async move {
                        let deferred_data =
                            DeferredEvents::defer_record(deferred.unwrap(), client.clone(), opts)
                                .await?;
                        client.upload(&output, deferred_data).await?;
                        worker_client
                            .complete_task(proof_id, marker_task, Default::default())
                            .await?;
                        Ok::<(), TaskError>(())
                    })
                    .map_err(|e| anyhow!(e)),
                )
            } else {
                tracing::warn!("no marker task");
                None
            }
        } else {
            None
        };

        let gpu_time = Arc::new(AtomicU32::new(0));
        let permit = acquire_gpu!(self, gpu_time);

        // Generate commit and proof.
        let CommonTaskInput {
            vk,
            mode,
            challenger,
            deferred_digest,
        } = common_input;
        let prover_clone = self.prover.clone();
        let mut challenger_clone = challenger.clone();
        let span = tracing::info_span!("compute");
        let (permit, shard_proof, vk, shard) = tokio::task::spawn_blocking(move || {
            let permit = permit;
            let _guard = span.enter();

            let pk = tracing::info_span!("pk_from_vk")
                .in_scope(|| prover_clone.core_prover.pk_from_vk(&program, &vk));

            let data = tracing::info_span!("commit_main")
                .in_scope(|| prover_clone.core_prover.commit(&shard, traces));

            let shard_proof = tracing::info_span!("prove_shard").in_scope(|| {
                prover_clone
                    .core_prover
                    .open(&pk, data, &mut challenger_clone)
                    .unwrap()
            });

            (permit, shard_proof, vk, shard)
        })
        .await
        .unwrap();

        drop(permit);

        // Verify shard
        let mut challenger_clone = challenger.clone();
        let prover_clone = self.prover.clone();
        let shard_proof_clone = shard_proof.clone();
        let vk_clone = vk.clone();
        let span = tracing::info_span!("verify_shard");
        let verify_future = tokio::task::spawn_blocking(move || {
            let _guard = span.enter();
            let machine = prover_clone.core_prover.machine();
            let config = machine.config();
            let chips = machine
                .shard_chips_ordered(&shard_proof_clone.chip_ordering)
                .collect::<Vec<_>>();
            challenger_clone.observe_slice(
                &shard_proof_clone.public_values[0..prover_clone.core_prover.num_pv_elts()],
            );
            let result = Verifier::verify_shard(
                config,
                &vk_clone,
                &chips,
                &mut challenger_clone,
                &shard_proof_clone,
            );
            match &result {
                Ok(_) => {
                    log::debug!("Core shard proof verification succeeded");
                }
                Err(e) => {
                    log::error!("Core shard proof verification failed: {:?}", e);
                }
            }
            let mut expected_global_cumulative_sum = shard_proof_clone.global_cumulative_sum();
            if shard_number == 1 {
                expected_global_cumulative_sum = once(expected_global_cumulative_sum)
                    .chain(once(vk_clone.initial_global_cumulative_sum))
                    .sum();
            }
            log::info!(
                "Expected cumulative sum from core: {:?}",
                expected_global_cumulative_sum
            );
            result.map(|_| expected_global_cumulative_sum)
        });

        let maybe_reduce_proof = if mode != ProofMode::Core {
            // Get program from channel
            let thread_future = compile_thread.unwrap();
            let (program, _) = tokio::try_join!(
                rx.map_err(|_| TaskError::Fatal(anyhow::anyhow!(
                    "failed to receive recursion program"
                ))),
                thread_future.map_err(|_| TaskError::Fatal(anyhow::anyhow!(
                    "failed to compile recursion program"
                )))
            )?;
            log::debug!("proof shape {}: {:?}", shard_number, shard_proof.shape());
            let is_first_shard = shard.public_values.shard == 1;
            let witness = SP1CircuitWitness::Core(SP1RecursionWitnessValues {
                vk: vk.clone(),
                shard_proofs: vec![shard_proof.clone()],
                reconstruct_deferred_digest: deferred_digest,
                is_complete: false,
                is_first_shard,
                vk_root: self.prover.recursion_vk_root,
            });
            let opts = self.prover_opts;
            let reduce_proof = self
                .full_recursion(program, witness, opts, None, gpu_time.clone())
                .await
                .map_err(|e| TaskError::Retryable(e.into()))?;
            Some(reduce_proof)
        } else {
            None
        };

        let reduce_verify_future = if let Some(reduce_proof) = &maybe_reduce_proof {
            let proof = reduce_proof.clone();
            let span = tracing::info_span!("verify reduce shards");
            let prover_clone = self.prover.clone();
            Some(tokio::task::spawn_blocking(move || {
                let _guard = span.enter();
                let compress_prover = &prover_clone.compress_prover;
                let machine = compress_prover.machine();
                let config = compress_prover.config();
                let SP1ReduceProof {
                    vk,
                    proof: shard_proof,
                } = proof;
                let mut challenger = config.challenger();
                let machine_proof = MachineProof {
                    shard_proofs: vec![shard_proof.clone()],
                };
                let pv: &RecursionPublicValues<BabyBear> =
                    shard_proof.public_values.as_slice().borrow();
                log::info!("Reduce proof shard pv: {:?}", pv);
                let result = machine.verify(&vk, &machine_proof, &mut challenger);
                match &result {
                    Ok(_) => {
                        log::debug!("Reduce leaf proof verification succeeded");
                        let vkey_hash = vk.hash_babybear();
                        if !prover_clone.recursion_vk_map.contains_key(&vkey_hash) {
                            tracing::error!("vkey {:?} not found in map", vkey_hash);
                            return Err(MachineVerificationError::InvalidVerificationKey);
                        }
                    }
                    Err(e) => {
                        log::error!("Reduce leaf proof verification failed: {:?}", e);
                    }
                }
                result.map(|_| pv.global_cumulative_sum)
            }))
        } else {
            None
        };

        // Need to use Box::pin so the Future types are the same due to different captured vars.
        let result_upload: Pin<Box<dyn Future<Output = Result<()>> + Send>> = match common_input
            .mode
        {
            ProofMode::Core => Box::pin(self.artifact_client.upload(&data.outputs[0], shard_proof)),
            _ => Box::pin(
                self.artifact_client
                    .upload(&data.outputs[0], maybe_reduce_proof.unwrap()),
            ),
        };

        if let Some(deferred_promise) = deferred_promise {
            let (_, deferred_result) = try_join!(result_upload, deferred_promise)?;
            deferred_result?;
        } else {
            result_upload.await?;
        }

        let expected_global_cumulative_sum = verify_future
            .await
            .unwrap()
            .map_err(|e| TaskError::Retryable(anyhow!("failed to verify shard proof: {}", e)))?;

        if let Some(verify_future) = reduce_verify_future {
            let final_sum = verify_future.await.unwrap().map_err(|e| {
                TaskError::Retryable(anyhow!("failed to verify reduce proof: {}", e))
            })?;
            if expected_global_cumulative_sum != final_sum {
                return Err(TaskError::Retryable(anyhow!(
                    "expected global cumulative sum {:?} != final sum {:?}",
                    expected_global_cumulative_sum,
                    final_sum
                )));
            }
        }

        // Clean up input shard since it's no longer needed
        client
            .try_delete(
                &data.inputs[2],
                sp1_cluster_artifact::ArtifactType::UnspecifiedArtifactType,
            )
            .await;

        // Remove task reference for precompile artifacts only at successful completion
        if let Some(artifacts) = precompile_artifacts {
            for (artifact, start, end) in artifacts {
                let _ = client
                    .remove_ref(
                        &artifact,
                        ArtifactType::UnspecifiedArtifactType,
                        &format!("{}_{}", start, end),
                    )
                    .await;
            }
        }

        Ok(TaskMetadata::new(
            gpu_time.load(std::sync::atomic::Ordering::Relaxed),
        ))
    }
}
