use std::borrow::Borrow;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;

use crate::acquire_gpu;
use crate::client::WorkerService;
use crate::error::TaskError;
use crate::tasks::TaskMetadata;
use crate::ClusterProverComponents;
use anyhow::{anyhow, Result};
use futures::executor::block_on;
use futures::TryFutureExt;
use opentelemetry::Context;
use p3_baby_bear::BabyBear;
use p3_field::AbstractField;
use p3_matrix::dense::DenseMatrix;
use sp1_cluster_artifact::Artifact;
use sp1_cluster_artifact::ArtifactBatch;
use sp1_cluster_artifact::ArtifactClient;
use sp1_cluster_artifact::ArtifactType;
use sp1_cluster_common::proto::WorkerTask;
use sp1_core_executor::SP1ReduceProof;
use sp1_prover::{DeviceProvingKey, InnerSC, SP1CircuitWitness, SP1RecursionProverError};
use sp1_recursion_circuit::machine::SP1CompressWithVkeyShape;
use sp1_recursion_circuit::machine::SP1CompressWitnessValues;
use sp1_recursion_circuit::machine::SP1DeferredWitnessValues;
use sp1_recursion_circuit::witness::Witnessable;
use sp1_recursion_compiler::config::InnerConfig;
use sp1_recursion_core::ExecutionRecord;
use sp1_recursion_core::RecursionProgram;
use sp1_recursion_core::DIGEST_SIZE;
use sp1_recursion_core::{air::RecursionPublicValues, runtime::Runtime as RecursionRuntime};
use sp1_sdk::HashableKey;
use sp1_stark::baby_bear_poseidon2::BabyBearPoseidon2;
use sp1_stark::septic_digest::SepticDigest;
use sp1_stark::MachineProof;
#[cfg(feature = "gpu")]
use sp1_stark::MachineProvingKey;
use sp1_stark::{Challenge, SP1ProverOpts, Val};
use sp1_stark::{MachineProver, ShardProof, StarkGenericConfig, StarkVerifyingKey};
use tokio::task::JoinHandle;
use tokio::try_join;
use tracing::{instrument, Instrument};

use crate::utils::with_parent;
use crate::SP1Worker;
use sp1_cluster_common::proto::TaskType;

pub type Traces = Vec<(String, DenseMatrix<Val<InnerSC>>)>;

impl<W: WorkerService, A: ArtifactClient> SP1Worker<W, A> {
    /// Get the compress keys for a given shape. If the shape is not in the cache, the keys will be
    /// generated and cached. This should only be called with the GPU lock held.
    pub fn get_compress_keys(
        self: &Arc<Self>,
        shape: SP1CompressWithVkeyShape,
        program: Arc<RecursionProgram<Val<InnerSC>>>,
    ) -> Arc<(
        DeviceProvingKey<ClusterProverComponents>,
        StarkVerifyingKey<InnerSC>,
    )> {
        let read = self.compress_keys.read().unwrap();
        if let Some(keys) = read.get(&shape) {
            keys.clone()
        } else {
            log::error!("missing keys for shape: {:?}", shape);
            drop(read);
            let keys = self.prover.compress_prover.setup(&program);
            // Arc::new(keys)
            let keys = Arc::new(keys);
            let mut write = self.compress_keys.write().unwrap();
            write.insert(shape, keys.clone());
            keys
        }
    }

    #[allow(clippy::type_complexity)]
    pub fn prepare_recursion(
        self: &Arc<Self>,
        program: Arc<RecursionProgram<Val<InnerSC>>>,
        input: SP1CircuitWitness,
    ) -> Result<ExecutionRecord<Val<InnerSC>>, SP1RecursionProverError> {
        let witness_stream = tracing::info_span!("Get witness stream").in_scope(|| match input {
            SP1CircuitWitness::Core(input) => {
                tracing::debug!("core shape: {:?}", input.shape().proof_shapes);
                let mut witness_stream = Vec::new();
                Witnessable::<InnerConfig>::write(&input, &mut witness_stream);
                witness_stream
            }
            SP1CircuitWitness::Deferred(input) => {
                let mut witness_stream = Vec::new();
                Witnessable::<InnerConfig>::write(&input, &mut witness_stream);
                witness_stream
            }
            SP1CircuitWitness::Compress(input) => {
                let mut witness_stream = Vec::new();
                let input_with_merkle = self.prover.make_merkle_proofs(input);
                Witnessable::<InnerConfig>::write(&input_with_merkle, &mut witness_stream);
                witness_stream
            }
        });

        // Execute the runtime
        let record = tracing::info_span!("execute runtime").in_scope(|| {
            tracing::debug!("recursion program shape: {:?}", program.shape);
            let mut runtime = RecursionRuntime::<Val<InnerSC>, Challenge<InnerSC>, _>::new(
                program,
                self.prover.compress_prover.config().perm.clone(),
            );
            runtime.witness_stream = witness_stream.into();
            match runtime.run() {
                Ok(_) => {
                    runtime.print_stats();
                    Ok(runtime.record)
                }
                Err(e) => Err(SP1RecursionProverError::RuntimeError(e.to_string())),
            }
        })?;

        Ok(record)
    }

    pub fn prove_recursion(
        self: &Arc<Self>,
        program: Arc<RecursionProgram<Val<InnerSC>>>,
        traces_handle: JoinHandle<(ExecutionRecord<Val<InnerSC>>, Traces)>,
        cache_shape: Option<SP1CompressWithVkeyShape>,
    ) -> Result<SP1ReduceProof<InnerSC>, SP1RecursionProverError> {
        // Setup the program
        let (ref pk, ref vk) = *cache_shape
            .map(|shape| self.get_compress_keys(shape, program.clone()))
            .unwrap_or_else(|| {
                let (pk, vk) = tracing::info_span!("Setup compress program")
                    .in_scope(|| self.prover.compress_prover.setup(&program));
                Arc::new((pk, vk))
            });

        // Initialize the challenger
        let mut challenger = self.prover.compress_prover.config().challenger();
        pk.observe_into(&mut challenger);

        let (record, traces) = block_on(traces_handle).unwrap();

        // Commit to the record and traces
        let data = tracing::info_span!("commit")
            .in_scope(|| self.prover.compress_prover.commit(&record, traces));

        tokio::task::spawn_blocking(move || {
            drop(record);
        });

        let proof = tracing::info_span!("open").in_scope(|| {
            self.prover
                .compress_prover
                .open(pk, data, &mut challenger)
                .map_err(|e| SP1RecursionProverError::RuntimeError(e.to_string()))
        })?;
        let reduce_proof = SP1ReduceProof {
            proof,
            vk: vk.clone(),
        };
        Ok(reduce_proof)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn full_recursion(
        self: &Arc<Self>,
        program: Arc<RecursionProgram<Val<InnerSC>>>,
        input: SP1CircuitWitness,
        opts: SP1ProverOpts,
        cached_keys: Option<SP1CompressWithVkeyShape>,
        gpu_time: Arc<AtomicU32>,
    ) -> Result<SP1ReduceProof<InnerSC>, SP1RecursionProverError> {
        let self_clone = self.clone();
        let program_clone = program.clone();
        let span = tracing::info_span!("prepare_recursion");
        let record = tokio::task::spawn_blocking(move || {
            let _guard = span.enter();
            self_clone.prepare_recursion(program_clone, input)
        })
        .await
        .unwrap()?;

        let span = tracing::info_span!("thread");
        let prover_clone = self.prover.clone();
        let traces_handle = tokio::task::spawn_blocking(move || {
            let _guard = span.enter();
            // Generate the dependencies
            let mut records = vec![record];
            tracing::info_span!("generate dependencies").in_scope(|| {
                prover_clone
                    .compress_prover
                    .machine()
                    .generate_dependencies(&mut records, &opts.recursion_opts, None)
            });
            let record = records.into_iter().next().unwrap();

            // Generate the traces
            let traces = tracing::info_span!("generate traces")
                .in_scope(|| prover_clone.compress_prover.generate_traces(&record));

            (record, traces)
        });

        // Generate the proof
        let self_clone = self.clone();
        let span = tracing::info_span!("prove_recursion");
        let permit = acquire_gpu!(self, gpu_time);
        let (permit, reduce_proof) = tokio::task::spawn_blocking(move || {
            let permit = permit;
            let _guard = span.enter();

            let res = self_clone.prove_recursion(program, traces_handle, cached_keys)?;
            Ok::<_, SP1RecursionProverError>((permit, res))
        })
        .await
        .unwrap()?;

        drop(permit);

        Ok(reduce_proof)
    }

    /// Generate a proof with the compress machine. Uses GPU semaphore around prover call.
    #[instrument(name = "setup_and_prove_compress", level = "debug", skip_all)]
    pub async fn setup_and_prove_compress(
        self: &Arc<Self>,
        input: SP1CircuitWitness,
        opts: SP1ProverOpts,
        gpu_time: Arc<AtomicU32>,
    ) -> Result<SP1ReduceProof<InnerSC>, SP1RecursionProverError> {
        // Witness stream
        let prover = self.prover.clone();
        let span = tracing::info_span!("Get program");
        let (program, input, cache_shape) = tokio::task::spawn_blocking(move || {
            let _guard = span.enter();
            let (program, cache_shape) = match &input {
                SP1CircuitWitness::Core(input) => (prover.recursion_program(input), None),
                SP1CircuitWitness::Deferred(input) => (prover.deferred_program(input), None),
                SP1CircuitWitness::Compress(input) => {
                    let input_with_merkle = prover.make_merkle_proofs(input.clone());
                    let cache_shape = input_with_merkle.shape();
                    (
                        prover.compress_program(&input_with_merkle),
                        Some(cache_shape),
                    )
                }
            };
            (program, input, cache_shape)
        })
        .await
        .unwrap();
        log::debug!("program shape: {:?}", program.shape);

        let reduce_proof = self
            .full_recursion(program, input, opts, cache_shape, gpu_time)
            .await?;

        Ok(reduce_proof)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn prove_deferred_leaves(
        self: &Arc<Self>,
        proof_id: &str,
        parent_id: &str,
        vk: &StarkVerifyingKey<InnerSC>,
        vks_and_proofs: Vec<(
            StarkVerifyingKey<BabyBearPoseidon2>,
            ShardProof<BabyBearPoseidon2>,
        )>,
        parent: Context,
        requester: String,
    ) -> Result<(Vec<String>, Vec<Artifact>, [BabyBear; DIGEST_SIZE]), TaskError> {
        let sp1_reduce_proofs: Vec<SP1ReduceProof<BabyBearPoseidon2>> = vks_and_proofs
            .iter()
            .map(|(vk, shard_proof)| SP1ReduceProof {
                vk: vk.clone(),
                proof: shard_proof.clone(),
            })
            .collect();

        let (deferred_inputs, deferred_digest) =
            self.prover
                .get_recursion_deferred_inputs(vk, &sp1_reduce_proofs, 1);

        // Note this differs a bit from sp1-prover because we have a RecursionPublicValues vs core PublicValues.
        let num_batches = deferred_inputs.len();
        let deferred_data_artifacts = self.artifact_client.create_artifacts(num_batches)?;
        deferred_data_artifacts
            .upload(&self.artifact_client, &deferred_inputs)
            .await?;
        let deferred_data_artifacts = deferred_data_artifacts.to_vec();

        let proof_artifacts = self.artifact_client.create_artifacts(num_batches)?.to_vec();

        let input_artifact_ids = deferred_data_artifacts
            .iter()
            .map(|id| vec![id])
            .collect::<Vec<Vec<&Artifact>>>();
        let output_artifact_ids = proof_artifacts
            .iter()
            .map(|id| vec![id])
            .collect::<Vec<Vec<&Artifact>>>();
        let (_, tasks) = self
            .worker_client
            .create_tasks(
                TaskType::RecursionDeferred,
                &input_artifact_ids,
                &output_artifact_ids,
                proof_id.to_string(),
                Some(parent_id.to_string()),
                None,
                requester.clone(),
            )
            .instrument(with_parent(
                tracing::info_span!("recursion_deferred tasks"),
                parent,
            ))
            .await?;

        Ok((tasks, proof_artifacts, deferred_digest))
    }

    /// Creates `Sp1RecursionDeferredBatch` tasks to lift deferred proofs.
    ///
    /// Returns:
    /// - A vector of task IDs
    /// - An artifact batch containing the deferred proofs
    #[allow(clippy::too_many_arguments)]
    pub async fn prove_subblock_deferred_leaves(
        self: &Arc<Self>,
        proof_id: &str,
        parent_id: &str,
        agg_vk: &StarkVerifyingKey<InnerSC>,
        deferred_proof: SP1ReduceProof<BabyBearPoseidon2>,
        initial_deferred_digest: [BabyBear; DIGEST_SIZE],
        parent: Context,
        requester: String,
    ) -> Result<(Vec<String>, ArtifactBatch), TaskError> {
        let sp1_reduce_proofs: Vec<SP1ReduceProof<BabyBearPoseidon2>> = vec![deferred_proof];

        let (deferred_inputs, _deferred_digest) = self
            .prover
            .get_recursion_deferred_inputs_with_initial_digest(
                agg_vk,
                &sp1_reduce_proofs,
                initial_deferred_digest,
                1,
            );

        // Note this differs a bit from sp1-prover because we have a RecursionPublicValues vs core PublicValues.
        let num_batches = deferred_inputs.len();
        let deferred_data_artifacts = self.artifact_client.create_artifacts(num_batches)?;
        deferred_data_artifacts
            .upload(&self.artifact_client, &deferred_inputs)
            .await?;
        let deferred_data_artifacts = deferred_data_artifacts.to_vec();

        let proof_artifacts = self.artifact_client.create_artifacts(num_batches)?.to_vec();

        let input_artifact_ids = deferred_data_artifacts
            .iter()
            .map(|id| vec![id])
            .collect::<Vec<Vec<&Artifact>>>();
        let output_artifact_ids = proof_artifacts
            .iter()
            .map(|id| vec![id])
            .collect::<Vec<Vec<&Artifact>>>();
        let (_, tasks) = self
            .worker_client
            .create_tasks(
                TaskType::RecursionDeferred,
                &input_artifact_ids,
                &output_artifact_ids,
                proof_id.to_string(),
                Some(parent_id.to_string()),
                None,
                requester,
            )
            .instrument(with_parent(
                tracing::info_span!("recursion_deferred tasks"),
                parent,
            ))
            .await?;

        Ok((tasks, proof_artifacts.into()))
    }

    /// Generate a proof that verifies a batch of deferred proofs.
    pub async fn process_sp1_recursion_deferred_batch(
        self: &Arc<Self>,
        task: &WorkerTask,
    ) -> Result<TaskMetadata, TaskError> {
        let data = task.data()?;
        let deferred_input = self
            .artifact_client
            .download::<SP1DeferredWitnessValues<InnerSC>>(&data.inputs[0])
            .await?;

        let opts = self.prover_opts;
        let gpu_time = Arc::new(AtomicU32::new(0));
        let reduce_proof = self
            .setup_and_prove_compress(
                SP1CircuitWitness::Deferred(deferred_input),
                opts,
                gpu_time.clone(),
            )
            .await?;

        // verify
        let reduce_proof_clone = reduce_proof.clone();
        let prover = self.prover.clone();
        let span = tracing::info_span!("verify deferred batch");
        let verify_future = tokio::task::spawn_blocking(move || {
            let _guard = span.enter();
            let config = prover.compress_prover.machine().config();
            let mut challenger = config.challenger();
            if let Err(err) = prover.compress_prover.machine().verify(
                &reduce_proof_clone.vk,
                &MachineProof {
                    shard_proofs: vec![reduce_proof_clone.proof.clone()],
                },
                &mut challenger,
            ) {
                tracing::error!("failed to verify deferred batch result: {}", err);
                Err(TaskError::Retryable(anyhow!(
                    "failed to verify deferred batch result: {}",
                    err
                )))
            } else {
                tracing::debug!("deferred batch result verified");
                Ok(())
            }
        })
        .map_err(|e| anyhow!("failed to verify deferred batch result: {}", e));

        let upload_promise = self.artifact_client.upload(&data.outputs[0], reduce_proof);

        let (verify_result, _) = try_join!(verify_future, upload_promise)?;
        verify_result?;

        // Clean up input artifact since it's processed into a reduce proof
        self.artifact_client
            .try_delete(
                &data.inputs[0],
                sp1_cluster_artifact::ArtifactType::UnspecifiedArtifactType,
            )
            .await;

        Ok(TaskMetadata::new(
            gpu_time.load(std::sync::atomic::Ordering::Relaxed),
        ))
    }

    /// Generate a proof that verifies a batch of recursive proofs (which can be verify-core,
    /// verify-deferred, or this proof itself)
    pub async fn process_sp1_recursion_reduce_batch(
        self: &Arc<Self>,
        task: &WorkerTask,
    ) -> Result<TaskMetadata, TaskError> {
        let data = task.data()?;
        let (proof1, proof2) = try_join!(
            self.artifact_client
                .download::<SP1ReduceProof<InnerSC>>(&data.inputs[0]),
            self.artifact_client
                .download::<SP1ReduceProof<InnerSC>>(&data.inputs[1]),
        )?;
        let proofs = vec![proof1, proof2];

        {
            let proofs_clone = proofs.clone();
            let span = tracing::info_span!("verify reduce shards");
            let prover_clone = self.prover.clone();
            tokio::task::spawn_blocking(move || {
                let _guard = span.enter();
                let compress_prover = &prover_clone.compress_prover;
                let machine = compress_prover.machine();
                let config = compress_prover.config();
                for (i, proof) in proofs_clone.iter().enumerate() {
                    let SP1ReduceProof {
                        vk,
                        proof: shard_proof,
                    } = proof;
                    let pv: &RecursionPublicValues<BabyBear> =
                        shard_proof.public_values.as_slice().borrow();
                    log::debug!("Reduce proof shard {} pv: {:?}", i, pv);
                    let mut challenger = config.challenger();
                    let machine_proof = MachineProof {
                        shard_proofs: vec![shard_proof.clone()],
                    };
                    match machine.verify(vk, &machine_proof, &mut challenger) {
                        Ok(_) => {
                            log::debug!("Reduce shard proof {} verification succeeded", i);
                        }
                        Err(e) => {
                            log::error!("Reduce shard proof {} verification failed: {:?}", i, e);
                        }
                    }
                }
            });
        }

        let first_pv: &RecursionPublicValues<BabyBear> =
            proofs[0].proof.public_values.as_slice().borrow();
        let last_pv: &RecursionPublicValues<BabyBear> = proofs[proofs.len() - 1]
            .proof
            .public_values
            .as_slice()
            .borrow();

        let zero_sum = [
            first_pv.global_cumulative_sum,
            last_pv.global_cumulative_sum,
        ]
        .into_iter()
        .sum::<SepticDigest<BabyBear>>()
        .is_zero();
        let is_complete = first_pv.start_shard == BabyBear::one()
            && last_pv.next_pc == BabyBear::zero()
            && first_pv.start_reconstruct_deferred_digest == [BabyBear::zero(); DIGEST_SIZE]
            && zero_sum;

        tracing::debug!(
            "start shard {} next shard {} is_complete {} zero_sum {}",
            first_pv.start_shard,
            last_pv.next_shard,
            is_complete,
            zero_sum
        );

        let reduce_input = SP1CompressWitnessValues {
            vks_and_proofs: proofs.into_iter().map(|p| (p.vk, p.proof)).collect(),
            is_complete,
        };
        let gpu_time = Arc::new(AtomicU32::new(0));
        let opts = self.prover_opts;
        let reduce_proof = self
            .setup_and_prove_compress(
                SP1CircuitWitness::Compress(reduce_input),
                opts,
                gpu_time.clone(),
            )
            .await?;

        // verify
        let reduce_proof_clone = reduce_proof.clone();
        let prover_clone = self.prover.clone();
        let span = tracing::info_span!("verify reduce batch");
        let verify_handle = tokio::task::spawn_blocking(move || {
            let _guard = span.enter();
            let config = prover_clone.compress_prover.machine().config();
            let mut challenger = config.challenger();
            if let Err(err) = prover_clone.compress_prover.machine().verify(
                &reduce_proof_clone.vk,
                &MachineProof {
                    shard_proofs: vec![reduce_proof_clone.proof.clone()],
                },
                &mut challenger,
            ) {
                tracing::error!("failed to verify reduce batch result: {}", err);
                Err(TaskError::Retryable(anyhow!(
                    "failed to verify reduce batch result: {}",
                    err
                )))
            } else {
                tracing::debug!("reduce batch result verified");
                let vkey_hash = reduce_proof_clone.vk.hash_babybear();
                if !prover_clone.recursion_vk_map.contains_key(&vkey_hash) {
                    tracing::error!("vkey {:?} not found in map", vkey_hash);
                    return Err(TaskError::Retryable(anyhow!(
                        "vkey {:?} not found in map",
                        vkey_hash
                    )));
                }
                Ok(())
            }
        })
        .map_err(|e| anyhow!("failed to verify reduce batch result: {}", e));

        let upload_future = self
            .artifact_client
            .upload::<SP1ReduceProof<InnerSC>>(&data.outputs[0], reduce_proof);

        let (verify_result, _) = try_join!(verify_handle, upload_future)?;
        verify_result?;

        // Clean up input artifacts since they're now combined into a single proof
        self.artifact_client
            .try_delete_batch(&data.inputs[0..2], ArtifactType::UnspecifiedArtifactType)
            .await;

        Ok(TaskMetadata::new(
            gpu_time.load(std::sync::atomic::Ordering::Relaxed),
        ))
    }
}
