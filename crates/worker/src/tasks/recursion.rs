use crate::error::TaskError;
use crate::utils::worker_task_to_raw_task_request;
use anyhow::Result;
use sp1_cluster_artifact::ArtifactClient;
use sp1_cluster_common::proto::WorkerTask;
use sp1_prover::worker::TaskMetadata;
use sp1_prover::worker::WorkerClient;
use std::sync::Arc;

use crate::SP1ClusterWorker;

impl<W: WorkerClient, A: ArtifactClient> SP1ClusterWorker<W, A> {
    // #[allow(clippy::too_many_arguments)]
    // pub async fn prove_deferred_leaves(
    //     self: &Arc<Self>,
    //     proof_id: &str,
    //     parent_id: &str,
    //     vk: &StarkVerifyingKey<InnerSC>,
    //     vks_and_proofs: Vec<(
    //         StarkVerifyingKey<BabyBearPoseidon2>,
    //         ShardProof<BabyBearPoseidon2>,
    //     )>,
    //     parent: Context,
    //     requester: String,
    // ) -> Result<(Vec<String>, Vec<Artifact>, [BabyBear; DIGEST_SIZE]), TaskError> {
    //     let sp1_reduce_proofs: Vec<SP1ReduceProof<BabyBearPoseidon2>> = vks_and_proofs
    //         .iter()
    //         .map(|(vk, shard_proof)| SP1ReduceProof {
    //             vk: vk.clone(),
    //             proof: shard_proof.clone(),
    //         })
    //         .collect();

    //     let mut initial_challenger = self.prover.core_prover.config().challenger();
    //     vk.observe_into(&mut initial_challenger);

    //     let (deferred_inputs, deferred_digest) =
    //         self.prover
    //             .get_recursion_deferred_inputs(vk, &sp1_reduce_proofs, 1);

    //     // Note this differs a bit from sp1-prover because we have a RecursionPublicValues vs core PublicValues.
    //     let num_batches = deferred_inputs.len();
    //     let deferred_data_artifacts = self.artifact_client.create_artifacts(num_batches)?;
    //     deferred_data_artifacts
    //         .upload(&self.artifact_client, &deferred_inputs)
    //         .await?;
    //     let deferred_data_artifacts = deferred_data_artifacts.to_vec();

    //     let proof_artifacts = self.artifact_client.create_artifacts(num_batches)?.to_vec();

    //     let input_artifact_ids = deferred_data_artifacts
    //         .iter()
    //         .map(|id| vec![id])
    //         .collect::<Vec<Vec<&Artifact>>>();
    //     let output_artifact_ids = proof_artifacts
    //         .iter()
    //         .map(|id| vec![id])
    //         .collect::<Vec<Vec<&Artifact>>>();
    //     let (_, tasks) = self
    //         .worker_client
    //         .create_tasks(
    //             TaskType::RecursionDeferred,
    //             &input_artifact_ids,
    //             &output_artifact_ids,
    //             proof_id.to_string(),
    //             Some(parent_id.to_string()),
    //             None,
    //             requester.clone(),
    //         )
    //         .instrument(with_parent(
    //             tracing::info_span!("recursion_deferred tasks"),
    //             parent,
    //         ))
    //         .await?;

    //     Ok((tasks, proof_artifacts, deferred_digest))
    // }

    // /// Creates `Sp1RecursionDeferredBatch` tasks to lift deferred proofs.
    // ///
    // /// Returns:
    // /// - A vector of task IDs
    // /// - An artifact batch containing the deferred proofs
    // #[allow(clippy::too_many_arguments)]
    // pub async fn prove_subblock_deferred_leaves(
    //     self: &Arc<Self>,
    //     proof_id: &str,
    //     parent_id: &str,
    //     agg_vk: &StarkVerifyingKey<InnerSC>,
    //     deferred_proof: SP1ReduceProof<BabyBearPoseidon2>,
    //     initial_deferred_digest: [BabyBear; DIGEST_SIZE],
    //     parent: Context,
    //     requester: String,
    // ) -> Result<(Vec<String>, ArtifactBatch), TaskError> {
    //     let sp1_reduce_proofs: Vec<SP1ReduceProof<BabyBearPoseidon2>> = vec![deferred_proof];

    //     let mut initial_challenger = self.prover.core_prover.config().challenger();
    //     agg_vk.observe_into(&mut initial_challenger);

    //     let (deferred_inputs, _deferred_digest) = self
    //         .prover
    //         .get_recursion_deferred_inputs_with_initial_digest(
    //             agg_vk,
    //             &sp1_reduce_proofs,
    //             initial_deferred_digest,
    //             1,
    //         );

    //     // Note this differs a bit from sp1-prover because we have a RecursionPublicValues vs core PublicValues.
    //     let num_batches = deferred_inputs.len();
    //     let deferred_data_artifacts = self.artifact_client.create_artifacts(num_batches)?;
    //     deferred_data_artifacts
    //         .upload(&self.artifact_client, &deferred_inputs)
    //         .await?;
    //     let deferred_data_artifacts = deferred_data_artifacts.to_vec();

    //     let proof_artifacts = self.artifact_client.create_artifacts(num_batches)?.to_vec();

    //     let input_artifact_ids = deferred_data_artifacts
    //         .iter()
    //         .map(|id| vec![id])
    //         .collect::<Vec<Vec<&Artifact>>>();
    //     let output_artifact_ids = proof_artifacts
    //         .iter()
    //         .map(|id| vec![id])
    //         .collect::<Vec<Vec<&Artifact>>>();
    //     let (_, tasks) = self
    //         .worker_client
    //         .create_tasks(
    //             TaskType::RecursionDeferred,
    //             &input_artifact_ids,
    //             &output_artifact_ids,
    //             proof_id.to_string(),
    //             Some(parent_id.to_string()),
    //             None,
    //             requester,
    //         )
    //         .instrument(with_parent(
    //             tracing::info_span!("recursion_deferred tasks"),
    //             parent,
    //         ))
    //         .await?;

    //     Ok((tasks, proof_artifacts.into()))
    // }

    /// Generate a proof that verifies a batch of deferred proofs.
    pub async fn process_sp1_recursion_deferred_batch(
        self: &Arc<Self>,
        _task: &WorkerTask,
    ) -> Result<TaskMetadata, TaskError> {
        todo!()
        // let data = task.data()?;
        // let deferred_input = self
        //     .artifact_client
        //     .download::<SP1DeferredWitnessValues<InnerSC>>(&data.inputs[0])
        //     .await?;

        // let opts = self.prover_opts;
        // let gpu_time = Arc::new(AtomicU32::new(0));
        // let reduce_proof = self
        //     .setup_and_prove_compress(
        //         SP1CircuitWitness::Deferred(deferred_input),
        //         opts,
        //         gpu_time.clone(),
        //     )
        //     .await?;

        // // verify
        // let reduce_proof_clone = reduce_proof.clone();
        // let prover = self.prover.clone();
        // let span = tracing::info_span!("verify deferred batch");
        // let verify_future = tokio::task::spawn_blocking(move || {
        //     let _guard = span.enter();
        //     let config = prover.compress_prover.machine().config();
        //     let mut challenger = config.challenger();
        //     if let Err(err) = prover.compress_prover.machine().verify(
        //         &reduce_proof_clone.vk,
        //         &MachineProof {
        //             shard_proofs: vec![reduce_proof_clone.proof.clone()],
        //         },
        //         &mut challenger,
        //     ) {
        //         tracing::error!("failed to verify deferred batch result: {}", err);
        //         Err(TaskError::Retryable(anyhow!(
        //             "failed to verify deferred batch result: {}",
        //             err
        //         )))
        //     } else {
        //         tracing::debug!("deferred batch result verified");
        //         Ok(())
        //     }
        // })
        // .map_err(|e| anyhow!("failed to verify deferred batch result: {}", e));

        // let upload_promise = self.artifact_client.upload(&data.outputs[0], reduce_proof);

        // let (verify_result, _) = try_join!(verify_future, upload_promise)?;
        // verify_result?;

        // // Clean up input artifact since it's processed into a reduce proof
        // self.artifact_client
        //     .try_delete(
        //         &data.inputs[0],
        //         sp1_cluster_artifact::ArtifactType::UnspecifiedArtifactType,
        //     )
        //     .await;

        // Ok(TaskMetadata::new(
        //     gpu_time.load(std::sync::atomic::Ordering::Relaxed),
        // ))
    }

    /// Generate a proof that verifies a batch of recursive proofs (which can be verify-core,
    /// verify-deferred, or this proof itself)
    pub async fn process_sp1_recursion_reduce_batch(
        self: &Arc<Self>,
        task: &WorkerTask,
    ) -> Result<TaskMetadata, TaskError> {
        let data = task.data()?;

        let raw_task_request = worker_task_to_raw_task_request(&data, None);

        let metadata = self
            .worker
            .prover_engine()
            .submit_recursion_reduce(raw_task_request)
            .await?
            .await
            .expect("failed to join recursion reduce")?;

        Ok(metadata)
    }
}
